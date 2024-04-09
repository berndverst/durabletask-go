package dtmb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	dtmbprotos "github.com/microsoft/durabletask-go/backend/azure/dtmb/internal/backend/v1"
	"github.com/microsoft/durabletask-go/backend/azure/dtmb/internal/utils"
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

	genActivityAction := func(idx int) *dtmbprotos.OrchestratorAction {
		return &dtmbprotos.OrchestratorAction{
			OrchestratorActionType: &dtmbprotos.OrchestratorAction_ScheduleActivity{
				ScheduleActivity: &dtmbprotos.ScheduleActivityOrchestratorAction{
					Name:  "count",
					Input: []byte(strconv.Itoa(idx)),
				},
			},
		}
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

	switch {
	case item.Name == "counter" && item.Version == "v1":
		switch item.Action.InboxActionType.(type) {
		case *dtmbprotos.InboxAction_CreateOrchestration:

			// what to do here?
			_ = &dtmbprotos.ConnectWorkerClientMessage_CompleteOrchestration{
				CompleteOrchestration: &dtmbprotos.CompleteOrchestrationMessage{
					OrchestrationId: item.OrchestrationId,
					Name:            item.Name,
					Version:         item.Version,
					CompletionToken: item.CompletionToken,
					CustomStatus:    "",
					Actions: []*dtmbprotos.OrchestratorAction{
						{
							OrchestratorActionType: &dtmbprotos.OrchestratorAction_ScheduleActivity{
								ScheduleActivity: &dtmbprotos.ScheduleActivityOrchestratorAction{
									Name:  item.Name,
									Input: []byte(strconv.Itoa(0)),
								},
							},
						},
					},
				},
			}

		case *dtmbprotos.InboxAction_ActivityCompleted:
			if len(item.Input) == 0 {
				bgErrCh <- errors.New("orchestration has empty input")
				return
			}
			idx, err := strconv.Atoi(string(item.Input))
			if err != nil {
				bgErrCh <- fmt.Errorf("failed parsing orchestration input '%s': %w", string(item.Input), err)
				return
			}

			var action *dtmbprotos.OrchestratorAction
			switch {
			case idx >= 0 && idx < 5:
				action = genActivityAction(idx + 1)
			case idx == 5:
				action = &dtmbprotos.OrchestratorAction{
					OrchestratorActionType: &dtmbprotos.OrchestratorAction_CompleteOrchestration{
						CompleteOrchestration: &dtmbprotos.CompleteOrchestrationOrchestratorAction{
							OrchestrationStatus: dtmbprotos.OrchestrationStatus_COMPLETED,
							Result:              item.Input,
						},
					},
				}
			default:
				bgErrCh <- fmt.Errorf("invalid orchestration input index: %d", idx)
				return
			}

			clientMessageChan <- &dtmbprotos.ConnectWorkerClientMessage{
				Message: &dtmbprotos.ConnectWorkerClientMessage_CompleteOrchestration{
					CompleteOrchestration: &dtmbprotos.CompleteOrchestrationMessage{
						OrchestrationId: item.OrchestrationId,
						Name:            item.Name,
						Version:         item.Version,
						CompletionToken: item.CompletionToken,
						Actions:         []*dtmbprotos.OrchestratorAction{action},
					},
				},
			}

			if idx == 5 {
				log.Printf("[%s] Orchestration completed with count: %d", testID, idx)
			}
		}
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

	// how do I map this to the backend.ActivityWorkItem?
	_ = &dtmbprotos.ConnectWorkerClientMessage_CompleteActivity{
		CompleteActivity: &dtmbprotos.CompleteActivityMessage{
			OrchestrationId: item.OrchestrationId,
			Name:            item.Name,
			CompletionToken: item.CompletionToken,
			Result:          item.Input,
		},
	}

	ret = &backend.ActivityWorkItem{
		SequenceNumber: 0,
		InstanceID:     "",
		NewEvent:       &protos.HistoryEvent{},
		Result:         &protos.HistoryEvent{},
		LockedBy:       "",
		Properties:     map[string]interface{}{},
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
