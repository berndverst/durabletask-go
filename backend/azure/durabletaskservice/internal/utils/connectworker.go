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

	// Establish the ConnectWorker stream
	// streamCtx := metadata.AppendToOutgoingContext(ctx,
	// 	"taskhub", taskHub,
	// )
	stream, err := worker.ConnectWorker(ctx)
	if err != nil {
		return fmt.Errorf("error starting ConnectWorker: %w", err)
	}

	// Send the initial message
	err = stream.Send(&backend.ConnectWorkerClientMessage{
		Message: &backend.ConnectWorkerClientMessage_EstablishWorkerConnection{
			EstablishWorkerConnection: &backend.EstablishWorkerConnectionMessage{
				Version:              UserAgent,
				ActivityFunction:     activityFns.ToProto(),
				OrchestratorFunction: orchestratorFns.ToProto(),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("error sending EstablishWorkerConnectionMessage message: %w", err)
	}

	// Wait for the first message
	timeout := time.NewTimer(5 * time.Second)
	configReceived := make(chan error)
	var wc *backend.WorkerConfiguration
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
			return fmt.Errorf("error receiving configuration: %w", err)
		}
	case <-timeout.C:
		return errors.New("timed out waiting for configuration message")
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
			// io.EOF means the stream was closed by the server, so we can return cleanly
			if errors.Is(err, io.EOF) {
				log.Printf("[%s] Stream ended…", streamID)
				return nil
			}

			// We have an error; return
			return err

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
			return fmt.Errorf("did not receive a message from the server in %v; closing connection", healthCheckDuration)
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
