package utils

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/microsoft/durabletask-go/backend/azure/durabletaskservice/internal/backend/v1"
)

var UserAgent = "dev/1"

// Set to true to enable debug logging
const ConnectWorkerDebug = false

func ConnectWorker(
	ctx context.Context,
	taskHub string,
	worker backend.TaskHubWorkerClient,
	orchestratorFns OrchestratorFnList,
	activityFns ActivityFnList,
	serverMsgChan chan<- *backend.ConnectWorkerServerMessage,
	clientMsgChan <-chan *backend.ConnectWorkerClientMessage,
	readyCb func(),
) error {
	retryClientMsgChan := make(chan *backend.ConnectWorkerClientMessage, 1)
	defer close(retryClientMsgChan)
	defer close(serverMsgChan)

	// Stream ID used for correlating debug logs
	var streamID string
	if ConnectWorkerDebug {
		streamIDBytes := make([]byte, 4)
		_, err := io.ReadFull(rand.Reader, streamIDBytes)
		if err != nil {
			return fmt.Errorf("failed to generate stream ID: %w", err)
		}
		streamID = hex.EncodeToString(streamIDBytes)
	}

	// Define the establishWorkerConnection message
	establishWorkerConnectionMsg := &backend.EstablishWorkerConnectionMessage{
		Version:              UserAgent,
		ActivityFunction:     activityFns.ToProto(),
		OrchestratorFunction: orchestratorFns.ToProto(),
	}

	// readyCb can only be invoked once
	var readyCbInvoked *atomic.Bool
	if readyCb != nil {
		readyCbInvoked = &atomic.Bool{}
		readyCbOrig := readyCb
		readyCb = func() {
			readyCbInvoked.Store(true)
			readyCbOrig()
		}
	}

	// We retry connecting until we get a persistent error
	for {
		log.Printf("[%s] Establishing connection with service", streamID)

		// Invoke the connectWorkerInternal method to perform the rest of the work
		workerConfig, err := connectWorkerInternal(
			ctx, streamID,
			taskHub, worker,
			establishWorkerConnectionMsg,
			serverMsgChan, clientMsgChan, retryClientMsgChan,
			readyCb,
		)

		// Check if we have a persistent error
		switch {
		case ctx.Err() != nil:
			// Our context has been canceled, this is persistent
			return fmt.Errorf("closing stream: %w", ctx.Err())

		case errors.Is(err, io.EOF):
			// Indicates that the server closed the connection gracefully
			// The instance of the service was shutting down gracefully
			log.Printf("[%s] Connection closed by the server", streamID)

		case err != nil:
			log.Printf("[%s] Error from ConnectWorker stream: %v", streamID, err)

		default:
			log.Printf("[%s] ConnectWorker returned with no error", streamID)
		}

		// If we have a workerConfig, update the worker ID in the EstablishWorkerConnection message
		if workerConfig != nil && workerConfig.WorkerId != "" {
			establishWorkerConnectionMsg.WorkerId = workerConfig.WorkerId
		}

		// readyCb cannot be invoked more than once
		if readyCb != nil && readyCbInvoked.Load() {
			readyCb = nil
		}

		// Sleep before retrying
		select {
		case <-time.After(time.Second):
			// All good
		case <-ctx.Done():
			return fmt.Errorf("closing stream: %w", ctx.Err())
		}
	}
}

func connectWorkerInternal(
	ctx context.Context,
	streamID string,
	taskHub string,
	worker backend.TaskHubWorkerClient,
	establishWorkerConnectionMsg *backend.EstablishWorkerConnectionMessage,
	serverMsgChan chan<- *backend.ConnectWorkerServerMessage,
	clientMsgChan <-chan *backend.ConnectWorkerClientMessage,
	retryClientMsgChan chan *backend.ConnectWorkerClientMessage,
	readyCb func(),
) (wc *backend.WorkerConfiguration, err error) {
	// Replace the context with one that we can cancel when this method returns
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Establish the ConnectWorker stream
	// streamCtx := metadata.AppendToOutgoingContext(ctx,
	// 	"taskhub", taskHub,
	// )
	stream, err := worker.ConnectWorker(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting ConnectWorker: %w", err)
	}

	// Send the initial message
	err = stream.Send(&backend.ConnectWorkerClientMessage{
		Message: &backend.ConnectWorkerClientMessage_EstablishWorkerConnection{
			EstablishWorkerConnection: establishWorkerConnectionMsg,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("error sending EstablishWorkerConnectionMessage message: %w", err)
	}

	// Wait for the first message
	timeout := time.NewTimer(5 * time.Second)
	configReceived := make(chan error)
	go func() {
		msg, err := stream.Recv()
		if err != nil {
			configReceived <- fmt.Errorf("error receiving initial message: %w", err)
			return
		}

		wc = msg.GetWorkerConfiguration()
		if wc == nil {
			configReceived <- errors.New("received unexpected message")
			return
		}

		if ConnectWorkerDebug {
			log.Printf("[%s] Received configuration message: %v", streamID, wc)
		}
		close(configReceived)
	}()

	select {
	case err = <-configReceived:
		if !timeout.Stop() {
			<-timeout.C
		}
		if err != nil {
			return nil, fmt.Errorf("error receiving configuration: %w", err)
		}
	case <-timeout.C:
		return nil, errors.New("timed out waiting for configuration message")
	}

	// We are ready
	if readyCb != nil {
		readyCb()
	}

	// Channel for errors
	errChan := make(chan error, 1)
	sendErr := func(err error) {
		select {
		case errChan <- err:
			// Error sent
		default:
			// Channel is full (so there's another error)
		}
	}

	// In background, send messages from the client and periodic healthchecks
	go func() {
		// Do pings 2s before the deadline
		tickInterval := wc.HealthCheckInterval.AsDuration() - (2 * time.Second)
		tick := time.NewTicker(tickInterval)
		defer tick.Stop()

		var err error
		for {
			select {
			case <-ctx.Done():
				// Stop
				return

			case <-tick.C:
				// Send an empty message as ping
				err = stream.Send(&backend.ConnectWorkerClientMessage{})
				if err != nil {
					sendErr(fmt.Errorf("error sending ping to server: %w", err))
					return
				}

			case msg, ok := <-clientMsgChan:
				// If the channel is closing, then we are returning
				if !ok {
					err = stream.CloseSend()
					if err != nil {
						sendErr(fmt.Errorf("failed to close send stream: %w", err))
						return
					}

					// Send an EOF to indicate the stream is over
					sendErr(io.EOF)
					return
				}

				// Send another message
				err = stream.Send(msg)
				if err != nil {
					sendErr(fmt.Errorf("error sending message to server: %w", err))
					return
				}

				// Reset the ping ticker
				tick.Reset(tickInterval)
			}
		}
	}()

	// Process other messages in a background goroutine
	msgChan := make(chan *backend.ConnectWorkerServerMessage)
	go func() {
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) || errors.Is(ctx.Err(), context.Canceled) {
				sendErr(io.EOF)
				return
			} else if err != nil {
				sendErr(fmt.Errorf("error receiving messages: %w", err))
				return
			}

			msgChan <- msg
		}
	}()

	// Process all channels
	healthCheckDuration := wc.HealthCheckInterval.AsDuration()
	healthCheckTick := time.NewTimer(healthCheckDuration)
	defer healthCheckTick.Stop()

	for {
		select {
		case err = <-errChan:
			// We have an error; return
			return wc, err

		case msg := <-msgChan:
			// Reset healthCheckTick
			healthCheckTick.Reset(healthCheckDuration)

			// Do not send pings
			if msg.GetMessage() != nil {
				if ConnectWorkerDebug {
					log.Printf("[%s] Received message: %v", streamID, msg)
				}
				serverMsgChan <- msg
			} else if ConnectWorkerDebug {
				log.Printf("[%s] Received ping from server", streamID)
			}

		case <-healthCheckTick.C:
			// A signal on healthCheckTick indicates that we haven't received a message from the server in an amount of time
			// Assume the server is dead
			return wc, fmt.Errorf("did not receive a message from the server in %v; closing connection", healthCheckDuration)
		}
	}
}

type ActivityFnList []string

func (l ActivityFnList) ToProto() []*backend.EstablishWorkerConnectionMessage_ActivityFunctionType {
	res := make([]*backend.EstablishWorkerConnectionMessage_ActivityFunctionType, len(l))
	for i := 0; i < len(l); i++ {
		res[i] = &backend.EstablishWorkerConnectionMessage_ActivityFunctionType{
			Name:            l[i],
			ConcurrentLimit: 20, // TODO: Make this configurable
		}
	}
	return res
}

type OrchestratorFnList []string

func (l OrchestratorFnList) ToProto() []*backend.EstablishWorkerConnectionMessage_OrchestratorFunctionType {
	res := make([]*backend.EstablishWorkerConnectionMessage_OrchestratorFunctionType, len(l))
	for i := 0; i < len(l); i++ {
		name, version, _ := strings.Cut(l[i], "|")
		res[i] = &backend.EstablishWorkerConnectionMessage_OrchestratorFunctionType{
			Name:            name,
			Version:         version,
			ConcurrentLimit: 20, // TODO: Make this configurable
		}
	}
	return res
}
