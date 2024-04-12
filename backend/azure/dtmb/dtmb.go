package dtmb

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	dtmbprotos "github.com/microsoft/durabletask-go/backend/azure/dtmb/internal/backend/v1"
	"github.com/microsoft/durabletask-go/backend/azure/dtmb/internal/utils"
)

const (
	// DefaultEndpoint is the default endpoint for the DTMB service.
	defaultEndpoint = "localhost:50051"
)

type dtmb struct {
	logger                    backend.Logger
	endpoint                  string
	options                   *DTMBOptions
	orchestrationQueue        utils.SyncQueue[dtmbprotos.ExecuteOrchestrationMessage]
	activityQueue             utils.SyncQueue[dtmbprotos.ExecuteActivityMessage]
	orchestrationHistoryCache utils.OrchestrationHistoryCache
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
	be.orchestrationQueue = utils.NewSyncQueue[dtmbprotos.ExecuteOrchestrationMessage]()
	be.activityQueue = utils.NewSyncQueue[dtmbprotos.ExecuteActivityMessage]()

	be.orchestrationHistoryCache = utils.NewOrchestrationHistoryCache(nil) // TODO: make capacity configurable

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

// CreateTaskHub creates a new task hub for the current backend. Task hub creation must be idempotent.
//
// If the task hub for this backend already exists, an error of type [ErrTaskHubExists] is returned.
func (d *dtmb) CreateTaskHub(ctx context.Context) error {
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
func (d *dtmb) DeleteTaskHub(context.Context) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// Start starts any background processing done by this backend.
func (d *dtmb) Start(ctx context.Context, orchestrators *[]string, activities *[]string) error {
	// TODO: is the context provided a background context or what?

	err := d.connectWorker(ctx, orchestrators, activities)
	if err != nil {
		return err
	}

	return nil
}

func (d *dtmb) connectWorker(ctx context.Context, orchestrators *[]string, activities *[]string) error {
	// Establish the ConnectWorker stream
	worker := d.workerClient
	var taskHubName string = "MAKE THIS CONFIGURABLE"
	var testID string = "MAKE THIS CONFIGURABLE"

	readyCh := make(chan struct{})
	bgErrCh := make(chan error)
	clientMessageChan := make(chan *dtmbprotos.ConnectWorkerClientMessage)
	serverMessageChan := make(chan *dtmbprotos.ConnectWorkerServerMessage)

	var orchestratorFnList utils.OrchestratorFnList = *orchestrators
	var activityFnList utils.ActivityFnList = *activities

	// start the bidirectional stream
	go func() {
		bgErrCh <- utils.ConnectWorker(
			ctx,
			testID,
			taskHubName,
			worker,
			orchestratorFnList,
			activityFnList,
			serverMessageChan,
			clientMessageChan,
			func() {
				close(readyCh)
			},
			true,
		)
	}()

	// Wait for readiness
	select {
	case err := <-bgErrCh: // This includes a timeout too
		log.Printf("[%s] Error starting test: %v", testID, err)
		return err
	case <-readyCh:
		// All good
	}

	// Do a ping as last warm-up
	pingCtx, pingCancel := context.WithTimeout(ctx, 15*time.Second)
	pingCtx = metadata.AppendToOutgoingContext(pingCtx,
		"taskhub", taskHubName,
	)
	_, err := worker.Ping(pingCtx, &dtmbprotos.PingRequest{})
	pingCancel()
	if err != nil {
		log.Printf("[%s] Ping error: %v", testID, err)
		return fmt.Errorf("ping error: %w", err)
	}

	// Execute messages in background
	go func() {
		for msg := range serverMessageChan {
			switch m := msg.Message.(type) {

			case *dtmbprotos.ConnectWorkerServerMessage_ExecuteActivity:
				go func() {
					d.activityQueue.Enqueue(m.ExecuteActivity)
				}()

			case *dtmbprotos.ConnectWorkerServerMessage_ExecuteOrchestration:
				go func() {
					d.orchestrationQueue.Enqueue(m.ExecuteOrchestration)
				}()
			}
		}
	}()

	// Wait for stop (or an error)
	select {
	case err = <-bgErrCh:
		log.Printf("[%s] Error from ConnectWorker: %v", testID, err)
		return fmt.Errorf("error from ConnectWorker: %w", err)
	case <-ctx.Done():
		// We stopped, so all good
		return nil
	}
}

// Stop stops any background processing done by this backend.
func (d *dtmb) Stop(context.Context) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// CreateOrchestrationInstance creates a new orchestration instance with a history event that
// wraps a ExecutionStarted event.
func (d *dtmb) CreateOrchestrationInstance(context.Context, *backend.HistoryEvent) error {
	// return not implemented error
	return fmt.Errorf("IMPLEMENTED")
}

// AddNewEvent adds a new orchestration event to the specified orchestration instance.
func (d *dtmb) AddNewOrchestrationEvent(context.Context, api.InstanceID, *backend.HistoryEvent) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

func (d *dtmb) getOrchestrationHistory(ctx context.Context, orchestrationID string) ([]*dtmbprotos.Event, error) {
	// look up cached history events and request the rest from the server
	cachedEvents := d.orchestrationHistoryCache.GetCachedHistoryEventsForOrchestrationID(orchestrationID)

	var historyRequest dtmbprotos.GetOrchestrationHistoryRequest
	if cachedEvents == nil {
		historyRequest = dtmbprotos.GetOrchestrationHistoryRequest{
			OrchestrationId: orchestrationID,
		}
	} else {
		historyRequest = dtmbprotos.GetOrchestrationHistoryRequest{
			OrchestrationId:        orchestrationID,
			LastItemSequenceNumber: cachedEvents[len(cachedEvents)-1].GetSequenceNumber(),
			LastItemEventHash:      cachedEvents[len(cachedEvents)-1].GetEventHash(),
		}
	}

	res, err := d.workerClient.GetOrchestrationHistory(
		ctx,
		&historyRequest,
	)
	if err != nil {
		return nil, err
	}

	events := res.GetEvent()

	// cache the newly received events
	d.orchestrationHistoryCache.AddHistoryEventsForOrchestrationID(orchestrationID, events)

	if cachedEvents != nil {
		// merge the sever events with the cached events
		events = append(cachedEvents, events...)
	}
	return events, err
}

// GetOrchestrationWorkItem gets a pending work item from the task hub or returns [ErrNoOrchWorkItems]
// if there are no pending work items.
func (d *dtmb) GetOrchestrationWorkItem(ctx context.Context) (*backend.OrchestrationWorkItem, error) {
	// return not implemented error
	var ret *backend.OrchestrationWorkItem = nil
	item := d.orchestrationQueue.Dequeue()
	if item == nil {
		return nil, backend.ErrNoWorkItems
	}

	cachedEvents, err := d.getOrchestrationHistory(ctx, item.OrchestrationId)
	if err != nil {
		return nil, err
	}

	// combine cached events with new events yet to be executed
	allEvents := append(cachedEvents, item.GetNewEvents()...)

	convertedEvents, err := utils.ConvertEvents(allEvents)
	if err != nil {
		return nil, err
	}

	ret = &backend.OrchestrationWorkItem{
		InstanceID: api.InstanceID(item.OrchestrationId),
		NewEvents:  convertedEvents,
		// RetryCount: TODO: Alessandro to implement
	}
	return ret, nil
}

// GetOrchestrationRuntimeState gets the runtime state of an orchestration instance.
func (d *dtmb) GetOrchestrationRuntimeState(context.Context, *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, error) {
	// return not implemented error
	return nil, fmt.Errorf("not implemented: can be done via get metadata")
}

// GetOrchestrationMetadata gets the metadata associated with the given orchestration instance ID.
//
// Returns [api.ErrInstanceNotFound] if the orchestration instance doesn't exist.
func (d *dtmb) GetOrchestrationMetadata(context.Context, api.InstanceID) (*api.OrchestrationMetadata, error) {
	// return not implemented error
	return nil, fmt.Errorf("not implemented: can be done via get metadata")
}

// CompleteOrchestrationWorkItem completes a work item by saving the updated runtime state to durable storage.
//
// Returns [ErrWorkItemLockLost] if the work-item couldn't be completed due to a lock-lost conflict (e.g., split-brain).
func (d *dtmb) CompleteOrchestrationWorkItem(context.Context, *backend.OrchestrationWorkItem) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// AbandonOrchestrationWorkItem undoes any state changes and returns the work item to the work item queue.
//
// This is called if an internal failure happens in the processing of an orchestration work item. It is
// not called if the orchestration work item is processed successfully (note that an orchestration that
// completes with a failure is still considered a successfully processed work item).
func (d *dtmb) AbandonOrchestrationWorkItem(context.Context, *backend.OrchestrationWorkItem) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// GetActivityWorkItem gets a pending activity work item from the task hub or returns [ErrNoWorkItems]
// if there are no pending activity work items.
func (d *dtmb) GetActivityWorkItem(context.Context) (*backend.ActivityWorkItem, error) {
	// return not implemented error
	var ret *backend.ActivityWorkItem = nil
	item := d.activityQueue.Dequeue()
	if item == nil {
		return nil, backend.ErrNoWorkItems
	}

	inputEvent := protos.HistoryEvent{
		EventType: &protos.HistoryEvent_TaskScheduled{
			TaskScheduled: &protos.TaskScheduledEvent{
				Name:  item.Name,
				Input: wrapperspb.String(string(item.Input)), // TODO: What if this is not a string?
				// ParentTraceContext: TODO: Alessandro to implement
			},
		},
	}

	ret = &backend.ActivityWorkItem{
		InstanceID: api.InstanceID(item.OrchestrationId),
		NewEvent:   &inputEvent,
	}
	return ret, nil
}

// CompleteActivityWorkItem sends a message to the parent orchestration indicating activity completion.
//
// Returns [ErrWorkItemLockLost] if the work-item couldn't be completed due to a lock-lost conflict (e.g., split-brain).
func (d *dtmb) CompleteActivityWorkItem(context.Context, *backend.ActivityWorkItem) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// AbandonActivityWorkItem returns the work-item back to the queue without committing any other chances.
//
// This is called when an internal failure occurs during activity work-item processing.
func (d *dtmb) AbandonActivityWorkItem(context.Context, *backend.ActivityWorkItem) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}

// PurgeOrchestrationState deletes all saved state for the specified orchestration instance.
//
// [api.ErrInstanceNotFound] is returned if the specified orchestration instance doesn't exist.
// [api.ErrNotCompleted] is returned if the specified orchestration instance is still running.
func (d *dtmb) PurgeOrchestrationState(context.Context, api.InstanceID) error {
	// return not implemented error
	return fmt.Errorf("not implemented")
}
