package dtmb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dtmbprotos "github.com/microsoft/durabletask-go/backend/azure/dtmb/internal/backend/v1"
)

type dtmb struct {
	logger             backend.Logger
	endpoint           string
	options            *DTMBOptions
	OrchestrationQueue [0]int // use lock to protect this
	ActivityQueue      [0]int // use lock to protect this
}

type DTMBOptions struct {
	Endpoint string
}

func NewDTMBOptions(endpoint string) *DTMBOptions {
	return &DTMBOptions{
		Endpoint: endpoint,
	}
}

func NewDTMB(opts *DTMBOptions, logger backend.Logger) *dtmb {
	be := &dtmb{
		logger: logger,
	}

	var defaultEndpoint string = "localhost:50051"

	if opts == nil {
		opts = NewDTMBOptions(defaultEndpoint)
	}
	be.options = opts
	be.endpoint = opts.Endpoint

	return be
}

// CreateTaskHub creates a new task hub for the current backend. Task hub creation must be idempotent.
//
// If the task hub for this backend already exists, an error of type [ErrTaskHubExists] is returned.
func (d dtmb) CreateTaskHub(context.Context) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// DeleteTaskHub deletes an existing task hub configured for the current backend. It's up to the backend
// implementation to determine how the task hub data is deleted.
//
// If the task hub for this backend doesn't exist, an error of type [ErrTaskHubNotFound] is returned.
func (d dtmb) DeleteTaskHub(context.Context) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// Start starts any background processing done by this backend.
func (d dtmb) Start(ctx context.Context, filterOptions *backend.FilterOptions) error {
	// TODO: is the context provided a background context or what?
	// ctx := context.Background()

	connCtx, connCancel := context.WithTimeout(ctx, 15*time.Second) // TODO: make this a configurable timeout
	conn, err := grpc.DialContext(connCtx, d.endpoint, grpc.WithBlock(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	connCancel()

	if err != nil {
		return fmt.Errorf("failed to connect to dtmb: %v", err)
	}
	defer conn.Close()

	client := dtmbprotos.NewTaskHubClientClient(conn)
	worker := dtmbprotos.NewTaskHubWorkerClient(conn)

	{
		res, err := client.Metadata(ctx, &dtmbprotos.MetadataRequest{})
		if err != nil {
			return fmt.Errorf("failed to get metadata: %v", err)
		}
		log.Println("Response from client:", res)
	}

	{
		res, err := worker.Metadata(ctx, &dtmbprotos.MetadataRequest{})
		if err != nil {
			panic(err)
		}
		log.Println("Response from worker:", res)
	}

	err = connectWorker(ctx, filterOptions, worker)
	if err != nil {
		panic(err)
	}

	return nil
}

func connectWorker(ctx context.Context, filterOptions *backend.FilterOptions, worker dtmbprotos.TaskHubWorkerClient) error {
	// Establish the ConnectWorker stream

	var stream dtmbprotos.TaskHubWorker_ConnectWorkerClient
	var err error

	if filterOptions != nil {
		// map filter options here
		stream, err = worker.ConnectWorker(ctx, &dtmbprotos.ConnectWorkerRequest{
			Version: "dev/1",
			// ActivityFunction:     d.options.ActivityFunction,
			// OrchestratorFunction: d.options.OrchestratorFunction,
		})
	} else {
		stream, err = worker.ConnectWorker(ctx, &dtmbprotos.ConnectWorkerRequest{
			Version:              "dev/1",
			ActivityFunction:     nil,
			OrchestratorFunction: nil,
		})
	}

	if err != nil {
		return fmt.Errorf("error starting ConnectWorker: %w", err)
	}

	// Wait for the first message
	timeout := time.NewTimer(5 * time.Second)
	configReceived := make(chan error, 1)
	var wc *dtmbprotos.WorkerConfiguration
	go func() {
		msg, err := stream.Recv()
		if err != nil {
			configReceived <- err
			return
		}

		wc = msg.GetWorkerConfiguration()
		if wc == nil {
			configReceived <- errors.New("received unexpected message")
			return
		}

		log.Println("Received configuration message", wc)
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

	// In background, start periodic pings as health checks
	errChan := make(chan error, 1)
	go func() {
		// Do the ping 2s before the deadline
		tick := time.NewTicker(wc.HealthCheckInterval.AsDuration() - (2 * time.Second))
		defer tick.Stop()

		pingRequest := &dtmbprotos.PingRequest{
			InstanceId: wc.InstanceId,
		}
		for {
			select {
			case <-ctx.Done():
				// Stop
				return

			case <-tick.C:
				log.Println("Sending ping…")
				pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
				_, err := worker.Ping(pingCtx, pingRequest)
				pingCancel()
				if err != nil {
					err = fmt.Errorf("error sending ping: %w", err)
					select {
					case errChan <- err:
						// All good
					default:
						// Channel is full (so there's another error)
					}
				}
			}
		}
	}()

	// Process other messages in a background goroutine
	msgChan := make(chan *dtmbprotos.ConnectWorkerMessage)
	go func() {
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				errChan <- io.EOF
				return
			} else if err != nil {
				errChan <- fmt.Errorf("error receiving messages: %w", err)
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
			// io.EOF means the stream ended, so we can return cleanly
			if errors.Is(err, io.EOF) {
				log.Println("Stream ended")
				return nil
			}

			// We have an error; return
			return err

		case msg := <-msgChan:
			if msg.GetMessage() == nil {
				log.Println("Received ping from server")
			} else {
				log.Println("Received message", msg)
			}

			// Reset healthCheckTick
			healthCheckTick.Reset(healthCheckDuration)

		case <-healthCheckTick.C:
			// A signal on healthCheckTick indicates that we haven't received a message from the server in an amount of time
			// Assume the server is dead
			return fmt.Errorf("did not receive a message from the server in %v; closing connection", healthCheckDuration)
		}
	}
}

// Stop stops any background processing done by this backend.
func (d dtmb) Stop(context.Context) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// CreateOrchestrationInstance creates a new orchestration instance with a history event that
// wraps a ExecutionStarted event.
func (d dtmb) CreateOrchestrationInstance(context.Context, *backend.HistoryEvent) error {
	// return not implemented error
	return fmt.Errorf("IMPLEMENTED")
}

// AddNewEvent adds a new orchestration event to the specified orchestration instance.
func (d dtmb) AddNewOrchestrationEvent(context.Context, api.InstanceID, *backend.HistoryEvent) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// GetOrchestrationWorkItem gets a pending work item from the task hub or returns [ErrNoOrchWorkItems]
// if there are no pending work items.
func (d dtmb) GetOrchestrationWorkItem(context.Context) (*backend.OrchestrationWorkItem, error) {
	// return not implemented error
	return nil, fmt.Errorf("not implemented")
}

// GetOrchestrationRuntimeState gets the runtime state of an orchestration instance.
func (d dtmb) GetOrchestrationRuntimeState(context.Context, *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, error) {
	// return not implemented error
	return nil, fmt.Errorf("Get metadata")
}

// GetOrchestrationMetadata gets the metadata associated with the given orchestration instance ID.
//
// Returns [api.ErrInstanceNotFound] if the orchestration instance doesn't exist.
func (d dtmb) GetOrchestrationMetadata(context.Context, api.InstanceID) (*api.OrchestrationMetadata, error) {
	// return not implemented error
	return nil, fmt.Errorf("Get metadata")
}

// CompleteOrchestrationWorkItem completes a work item by saving the updated runtime state to durable storage.
//
// Returns [ErrWorkItemLockLost] if the work-item couldn't be completed due to a lock-lost conflict (e.g., split-brain).
func (d dtmb) CompleteOrchestrationWorkItem(context.Context, *backend.OrchestrationWorkItem) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// AbandonOrchestrationWorkItem undoes any state changes and returns the work item to the work item queue.
//
// This is called if an internal failure happens in the processing of an orchestration work item. It is
// not called if the orchestration work item is processed successfully (note that an orchestration that
// completes with a failure is still considered a successfully processed work item).
func (d dtmb) AbandonOrchestrationWorkItem(context.Context, *backend.OrchestrationWorkItem) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// GetActivityWorkItem gets a pending activity work item from the task hub or returns [ErrNoWorkItems]
// if there are no pending activity work items.
func (d dtmb) GetActivityWorkItem(context.Context) (*backend.ActivityWorkItem, error) {
	// return not implemented error
	return nil, fmt.Errorf("not implemented")
}

// CompleteActivityWorkItem sends a message to the parent orchestration indicating activity completion.
//
// Returns [ErrWorkItemLockLost] if the work-item couldn't be completed due to a lock-lost conflict (e.g., split-brain).
func (d dtmb) CompleteActivityWorkItem(context.Context, *backend.ActivityWorkItem) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// AbandonActivityWorkItem returns the work-item back to the queue without committing any other chances.
//
// This is called when an internal failure occurs during activity work-item processing.
func (d dtmb) AbandonActivityWorkItem(context.Context, *backend.ActivityWorkItem) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// PurgeOrchestrationState deletes all saved state for the specified orchestration instance.
//
// [api.ErrInstanceNotFound] is returned if the specified orchestration instance doesn't exist.
// [api.ErrNotCompleted] is returned if the specified orchestration instance is still running.
func (d dtmb) PurgeOrchestrationState(context.Context, api.InstanceID) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}
