package dtmb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dtmbprotos "github.com/microsoft/durabletask-go/backend/azure/dtmb/internal/backend/v1"
)

const (
	// DefaultEndpoint is the default endpoint for the DTMB service.
	defaultEndpoint = "localhost:50051"
)

type dtmb struct {
	logger             backend.Logger
	endpoint           string
	options            *DTMBOptions
	orchestrationQueue syncQueue[dtmbprotos.ExecuteOrchestrationMessage]
	activityQueue      syncQueue[dtmbprotos.ExecuteActivityMessage]
	// connection         *grpc.ClientConn
	clientClient dtmbprotos.TaskHubClientClient
	workerClient dtmbprotos.TaskHubWorkerClient
}

type DTMBOptions struct {
	Endpoint string
}

func NewDTMBOptions(endpoint string) *DTMBOptions {
	if endpoint != "" {
		return &DTMBOptions{
			Endpoint: endpoint,
		}
	} else {
		return &DTMBOptions{
			Endpoint: defaultEndpoint,
		}
	}
}

func NewDTMB(opts *DTMBOptions, logger backend.Logger) (*dtmb, error) {
	be := &dtmb{
		logger: logger,
	}

	if opts == nil {
		opts = NewDTMBOptions(defaultEndpoint)
	}
	be.options = opts
	be.endpoint = opts.Endpoint

	// The following queues are used to store messages received from the server
	be.orchestrationQueue = NewSyncQueue[dtmbprotos.ExecuteOrchestrationMessage]()
	be.activityQueue = NewSyncQueue[dtmbprotos.ExecuteActivityMessage]()

	ctx := context.Background()
	connCtx, connCancel := context.WithTimeout(ctx, 15*time.Second) // TODO: make this a configurable timeout
	conn, err := grpc.DialContext(connCtx, be.endpoint, grpc.WithBlock(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	connCancel()

	if err != nil {
		return nil, fmt.Errorf("failed to connect to dtmb: %v", err)
	}

	be.clientClient = dtmbprotos.NewTaskHubClientClient(conn)
	be.workerClient = dtmbprotos.NewTaskHubWorkerClient(conn)

	return be, nil
}

type syncQueue[T dtmbprotos.ExecuteOrchestrationMessage | dtmbprotos.ExecuteActivityMessage] struct {
	lock  *sync.Mutex
	items []*T
}

func NewSyncQueue[T dtmbprotos.ExecuteOrchestrationMessage | dtmbprotos.ExecuteActivityMessage]() syncQueue[T] {
	return syncQueue[T]{
		lock:  &sync.Mutex{},
		items: []*T{},
	}
}

func (q *syncQueue[T]) Enqueue(item *T) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.items = append(q.items, item)
}

func (q *syncQueue[T]) Dequeue() *T {
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.items) == 0 {
		return nil
	}

	item := q.items[0]
	q.items = q.items[1:]
	return item
}

// CreateTaskHub creates a new task hub for the current backend. Task hub creation must be idempotent.
//
// If the task hub for this backend already exists, an error of type [ErrTaskHubExists] is returned.
func (d dtmb) CreateTaskHub(ctx context.Context) error {
	_, err := d.clientClient.Metadata(ctx, &dtmbprotos.MetadataRequest{})
	if err != nil {
		return backend.ErrTaskHubNotFound
	}
	return nil
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
func (d dtmb) Start(ctx context.Context, orchestrators *[]string, activities *[]string) error {
	// TODO: is the context provided a background context or what?

	err := d.connectWorker(ctx, orchestrators, activities)
	if err != nil {
		return err
	}

	return nil
}

func (d dtmb) connectWorker(ctx context.Context, orchestrators *[]string, activities *[]string) error {
	// Establish the ConnectWorker stream
	client := d.workerClient

	var stream dtmbprotos.TaskHubWorker_ConnectWorkerClient
	var err error

	var activityFunctionTypes []*dtmbprotos.ConnectWorkerRequest_ActivityFunctionType = []*dtmbprotos.ConnectWorkerRequest_ActivityFunctionType{}
	var orchestratorFunctionTypes []*dtmbprotos.ConnectWorkerRequest_OrchestratorFunctionType = []*dtmbprotos.ConnectWorkerRequest_OrchestratorFunctionType{}

	if orchestrators != nil {
		// populate orchestratorFunctionTypes
		for _, orchestrator := range *orchestrators {
			orchestratorFunctionTypes = append(orchestratorFunctionTypes, &dtmbprotos.ConnectWorkerRequest_OrchestratorFunctionType{
				OrchestrationName: orchestrator,
				ConcurrentLimit:   0, // TODO: make this configurable
			})
		}

		// populate activityFunctionTypes
		for _, activity := range *activities {
			activityFunctionTypes = append(activityFunctionTypes, &dtmbprotos.ConnectWorkerRequest_ActivityFunctionType{
				ActivityName: activity,
			})
		}
	}

	stream, err = client.ConnectWorker(ctx, &dtmbprotos.ConnectWorkerRequest{
		Version:              "dev/1",
		ActivityFunction:     activityFunctionTypes,
		OrchestratorFunction: orchestratorFunctionTypes,
	})
	if err != nil {
		return fmt.Errorf("error starting ConnectWorker: %w", err)
	}

	// Wait for the first message
	timeout := time.NewTimer(5 * time.Second) // should this be configurable?
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
				_, err := client.Ping(pingCtx, pingRequest)
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

				switch msg.GetMessage().(type) {
				case *dtmbprotos.ConnectWorkerMessage_ExecuteActivity:
					d.activityQueue.Enqueue(msg.GetExecuteActivity())
				case *dtmbprotos.ConnectWorkerMessage_ExecuteOrchestration:
					d.orchestrationQueue.Enqueue(msg.GetExecuteOrchestration())
				case *dtmbprotos.ConnectWorkerMessage_AbandonWorkItems:
					// not implemented
				default:
					log.Println("Received unknown message type")
				}
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
func (d dtmb) GetOrchestrationWorkItem(ctx context.Context) (*backend.OrchestrationWorkItem, error) {
	// return not implemented error
	var ret *backend.OrchestrationWorkItem = nil
	item := d.orchestrationQueue.Dequeue()
	if item == nil {
		return nil, backend.ErrNoWorkItems
	}

	historyRequest := &dtmbprotos.GetOrchestrationHistoryRequest{
		OrchestrationId: item.GetOrchestrationId(),
		// LastItem --
		// TODO: Do I need to implement caching of orchestration history?
	}

	historyResponse, err := d.workerClient.GetOrchestrationHistory(ctx, historyRequest)
	if err != nil {
		return nil, fmt.Errorf("error fetching orchestration history for orchestration id %s: %w", historyRequest.OrchestrationId, err)
	}
	historyResponse.Event[0].GetHistoryState()

	ret = &backend.OrchestrationWorkItem{
		InstanceID: api.InstanceID(item.OrchestrationId.InstanceId),
		// State:      state,
		// NewEvents: ,
		// NewEvents
		// LockedBy:   "",
		// RetryCount: 0,
		// State
		// Properties: map[string]interface{}{},
	}
	return ret, nil
}

// GetOrchestrationRuntimeState gets the runtime state of an orchestration instance.
func (d dtmb) GetOrchestrationRuntimeState(context.Context, *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, error) {
	// return not implemented error
	return nil, fmt.Errorf("not implemented: can be done via get metadata")
}

// GetOrchestrationMetadata gets the metadata associated with the given orchestration instance ID.
//
// Returns [api.ErrInstanceNotFound] if the orchestration instance doesn't exist.
func (d dtmb) GetOrchestrationMetadata(context.Context, api.InstanceID) (*api.OrchestrationMetadata, error) {
	// return not implemented error
	return nil, fmt.Errorf("not implemented: can be done via get metadata")
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
	var ret *backend.ActivityWorkItem = nil
	item := d.activityQueue.Dequeue()
	if item == nil {
		return nil, backend.ErrNoWorkItems
	}

	ret = &backend.ActivityWorkItem{
		InstanceID:     api.InstanceID(item.OrchestrationId.InstanceId),
		SequenceNumber: int64(item.GetSequenceNumber()),
		// NewEvent: ,
		// Result: ,
		// LockedBy: ,
		// Properties: ,
	}
	return ret, nil
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
