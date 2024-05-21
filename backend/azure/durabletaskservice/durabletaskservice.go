package durabletaskservice

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	dtmbprotos "github.com/microsoft/durabletask-go/backend/azure/durabletaskservice/internal/backend/v1"
	"github.com/microsoft/durabletask-go/backend/azure/durabletaskservice/internal/utils"
)

type TaskHubClient struct {
	dtmbprotos.TaskHubClientClient
	dtmbprotos.TaskHubWorkerClient
}

type durableTaskService struct {
	logger                     backend.Logger
	options                    *durableTaskServiceBackendOptions
	orchestrationQueue         utils.SyncQueue[dtmbprotos.ExecuteOrchestrationMessage]
	activityQueue              utils.SyncQueue[dtmbprotos.ExecuteActivityMessage]
	orchestrationHistoryCache  utils.OrchestrationHistoryCache
	connectWorkerClientStream  chan *dtmbprotos.ConnectWorkerClientMessage
	client                     TaskHubClient
	workerCancelFunc           context.CancelFunc
	running                    atomic.Bool
	orchestrationTaskIDManager utils.OrchestrationTaskCounter
}

func NewDurableTaskServiceBackend(ctx context.Context, logger backend.Logger, endpoint string, taskHubName string, options ...DurableTaskServiceBackendConfigurationOption) (backend.Backend, error) {
	be := &durableTaskService{
		logger: logger,
	}

	var err error
	be.options, err = newDurableTaskServiceBackendConfiguration(endpoint, taskHubName, options...)
	if err != nil {
		return nil, err
	}

	grpcDialOptions, optionErr := utils.CreateGrpcDialOptions(
		ctx, logger, be.options.Insecure, be.options.DisableAuth, be.options.TaskHubName, be.options.UserAgent, &be.options.AzureCredential, be.options.ResourceScopes, be.options.TenantID)
	if optionErr != nil {
		return nil, optionErr
	}

	connCtx, connCancel := context.WithTimeout(ctx, 15*time.Second) // TODO: make this a configurable timeout
	conn, err := grpc.DialContext(connCtx, be.options.Endpoint, grpcDialOptions...)
	connCancel()

	if err != nil {
		return nil, fmt.Errorf("failed to connect to dtmb: %v", err)
	}

	be.client = TaskHubClient{
		TaskHubClientClient: dtmbprotos.NewTaskHubClientClient(conn),
		TaskHubWorkerClient: dtmbprotos.NewTaskHubWorkerClient(conn),
	}

	return be, nil
}

// CreateTaskHub checks whether a task hub for the current backend has already been created.
//
// Task hub creation is not performed here. Instead it must be created on the durable task service.
//
// If the task hub for this backend is not found, an error of type [ErrTaskHubNotFound] is returned.
func (d *durableTaskService) CreateTaskHub(ctx context.Context) error {
	_, err := d.client.TaskHubClientClient.Metadata(ctx, &dtmbprotos.MetadataRequest{})
	if err != nil {
		return backend.ErrTaskHubNotFound
	}
	return nil
}

// DeleteTaskHub deletes an existing task hub configured for the current backend. It's up to the backend
// implementation to determine how the task hub data is deleted.
//
// If the task hub for this backend doesn't exist, an error of type [ErrTaskHubNotFound] is returned.
func (d *durableTaskService) DeleteTaskHub(context.Context) error {
	return errors.New("the TaskHub cannot be deleted using this SDK; please perform this operation on the service")
}

// Start starts any background processing done by this backend.
func (d *durableTaskService) Start(ctx context.Context, orchestrators []string, activities []string) error {
	if !d.running.CompareAndSwap(false, true) {
		// return errors.New("backend is already running")
		return nil
	}

	var ctxWithCancel context.Context
	ctxWithCancel, d.workerCancelFunc = context.WithCancel(ctx)

	if len(orchestrators) == 0 {
		orchestrators = d.options.Orchestrators
	}

	if len(activities) == 0 {
		activities = d.options.Activities
	}

	d.logger.Debug("Starting DurableTaskServiceBackend")
	d.logger.Debugf("===== Orchestrators: %v", orchestrators)
	d.logger.Debugf("===== Activities: %v", activities)

	// The following queues are used to store messages received from the server
	d.orchestrationQueue = utils.NewSyncQueue[dtmbprotos.ExecuteOrchestrationMessage]()
	d.activityQueue = utils.NewSyncQueue[dtmbprotos.ExecuteActivityMessage]()

	d.orchestrationHistoryCache = utils.NewOrchestrationHistoryCache(d.options.OrchestrationHistoryCacheSize) // TODO: make capacity configurable
	d.orchestrationTaskIDManager = utils.NewOrchestrationTaskIDManager()

	err := d.connectWorker(ctxWithCancel, orchestrators, activities)
	if err != nil {
		return err
	}

	return nil
}

func (d *durableTaskService) connectWorker(ctx context.Context, orchestrators []string, activities []string) error {
	// Establish the ConnectWorker stream
	var taskHubName string = "default" // make this configurable

	readyCh := make(chan struct{})
	bgErrCh := make(chan error)
	d.connectWorkerClientStream = make(chan *dtmbprotos.ConnectWorkerClientMessage)
	serverMessageChan := make(chan *dtmbprotos.ConnectWorkerServerMessage)

	// start the bidirectional stream
	go func() {
		bgErrCh <- utils.ConnectWorker(
			ctx,
			taskHubName,
			d.client.TaskHubWorkerClient,
			orchestrators,
			activities,
			serverMessageChan,
			d.connectWorkerClientStream,
			func() {
				close(readyCh)
			},
		)
	}()

	// Wait for readiness
	select {
	case err := <-bgErrCh: // This includes a timeout too
		d.logger.Errorf("Error starting worker stream: %v", err)
		return err
	case <-readyCh:
		// All good
	}

	// Execute messages in background
	go func() {
		for msg := range serverMessageChan {
			switch m := msg.Message.(type) {

			case *dtmbprotos.ConnectWorkerServerMessage_ExecuteActivity:
				d.activityQueue.Enqueue(m.ExecuteActivity)
			case *dtmbprotos.ConnectWorkerServerMessage_ExecuteOrchestration:
				d.orchestrationQueue.Enqueue(m.ExecuteOrchestration)
			}
		}
	}()

	// Wait for stop (or an error)
	// select {
	// case err = <-bgErrCh:
	// 	d.logger.Errorf("[%s] Error from ConnectWorker: %v", testID, err)
	// 	return fmt.Errorf("error from ConnectWorker: %w", err)
	// case <-ctx.Done():
	// 	// We stopped, so all good
	// 	return nil
	// }
	return nil
}

// Stop stops any background processing done by this backend.
func (d *durableTaskService) Stop(ctx context.Context) error {
	// new messages are no longer received from the server, but existing received messages are still available to be processed from memory
	d.workerCancelFunc()
	return nil
}

// CreateOrchestrationInstance creates a new orchestration instance with a history event that
// wraps a ExecutionStarted event.
func (d *durableTaskService) CreateOrchestrationInstance(ctx context.Context, event *backend.HistoryEvent, IdReusePolicy ...backend.OrchestrationIdReusePolicyOptions) error {
	executionStartedEvent := event.GetExecutionStarted()
	if executionStartedEvent == nil {
		return fmt.Errorf("expected an ExecutionStarted event, but got %v", event.GetEventType())
	}

	var startTime *dtmbprotos.Delay
	if executionStartedEvent.GetScheduledStartTimestamp() == nil {
		startTime = nil
	} else {
		startTime = &dtmbprotos.Delay{
			Delayed: &dtmbprotos.Delay_Time{
				Time: executionStartedEvent.GetScheduledStartTimestamp(),
			},
		}
	}

	_, err := d.client.CreateOrchestration(ctx, &dtmbprotos.CreateOrchestrationRequest{
		OrchestrationId: executionStartedEvent.GetOrchestrationInstance().GetInstanceId(),
		Name:            executionStartedEvent.GetName(),
		Version:         executionStartedEvent.GetVersion().GetValue(),
		Input:           []byte(executionStartedEvent.GetInput().GetValue()),
		StartAt:         startTime,
		// IdReusePolicy: // this is not implemented on the server
	})

	return err
}

// AddNewEvent adds a new orchestration event to the specified orchestration instance.
func (d *durableTaskService) AddNewOrchestrationEvent(ctx context.Context, id api.InstanceID, event *backend.HistoryEvent) error {
	var err error
	switch typedEvent := event.GetEventType().(type) {
	case *protos.HistoryEvent_EventRaised:

		req := dtmbprotos.RaiseEventRequest{
			OrchestrationId: string(id),
			Name:            typedEvent.EventRaised.GetName(),
			Input:           []byte(typedEvent.EventRaised.GetInput().GetValue()),
		}

		_, err = d.client.RaiseEvent(ctx, &req)

	case *protos.HistoryEvent_ExecutionTerminated:
		// this is a terminal event, so we can evict the cache
		d.orchestrationHistoryCache.EvictCacheForOrchestrationID(string(id))
		d.orchestrationTaskIDManager.PurgeOrchestration(string(id))
		err = fmt.Errorf("not implemented in protos")
	case *protos.HistoryEvent_ExecutionResumed:
		err = fmt.Errorf("not implemented in protos")
	case *protos.HistoryEvent_ExecutionSuspended:
		err = fmt.Errorf("not implemented in protos")
	default:
		err = fmt.Errorf("unsupported event type in AddNewOrchestrationEvent: %v", event.GetEventType())
	}

	return err
}

func (d *durableTaskService) getOrchestrationHistory(ctx context.Context, orchestrationID string) ([]*dtmbprotos.Event, error) {
	// look up cached history events and request the rest from the server
	cachedEvents := d.orchestrationHistoryCache.GetCachedHistoryEventsForOrchestrationID(orchestrationID)

	var historyRequest dtmbprotos.GetOrchestrationHistoryRequest
	if len(cachedEvents) == 0 {
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

	res, err := d.client.GetOrchestrationHistory(
		ctx,
		&historyRequest,
	)
	if err != nil {
		return nil, err
	}

	events := res.GetEvents()

	if len(events) != 0 {
		if events[0].SequenceNumber == 0 && historyRequest.LastItemSequenceNumber != 0 {
			// the server has reset the history
			d.orchestrationHistoryCache.EvictCacheForOrchestrationID(orchestrationID)
			d.orchestrationTaskIDManager.PurgeOrchestration(orchestrationID)
			cachedEvents = nil
		}
	}

	// cache the newly received events
	d.orchestrationHistoryCache.AddHistoryEventsForOrchestrationID(orchestrationID, events)

	if cachedEvents != nil {
		// merge the sever events with the cached events
		events = append(cachedEvents, events...)
	}
	return events, err
}

// GetOrchestrationWorkItem gets a pending work item from the task hub or returns [ErrNoWorkItems]
// if there are no pending work items.
func (d *durableTaskService) GetOrchestrationWorkItem(ctx context.Context) (*backend.OrchestrationWorkItem, error) {
	var ret *backend.OrchestrationWorkItem
	item := d.orchestrationQueue.Dequeue()
	if item == nil || len(item.GetNewEvents()) == 0 {
		return nil, backend.ErrNoWorkItems
	}

	actualHistoryEvents, err := d.getOrchestrationHistory(ctx, item.GetOrchestrationId())
	if err != nil {
		return nil, err
	}

	convertedHistoryEvents, err := utils.ConvertEvents(item.GetOrchestrationId(), &d.orchestrationTaskIDManager, actualHistoryEvents)
	if err != nil {
		return nil, err
	}

	orchestationRuntimeState := backend.NewOrchestrationRuntimeState(api.InstanceID(item.GetOrchestrationId()), convertedHistoryEvents)

	convertedNewEvents, err := utils.ConvertEvents(item.GetOrchestrationId(), &d.orchestrationTaskIDManager, item.GetNewEvents())
	if err != nil {
		return nil, err
	}

	ret = &backend.OrchestrationWorkItem{
		InstanceID: api.InstanceID(item.GetOrchestrationId()),
		NewEvents:  convertedNewEvents,
		State:      orchestationRuntimeState,
		RetryCount: int32(item.GetRetryCount()),
		Properties: map[string]interface{}{
			"CompletionToken":   item.GetCompletionToken(),
			"OrchestrationName": item.GetName(),
			"Version":           item.GetVersion(),
			"ExecutionId":       item.GetExecutionId(),
		},
	}
	return ret, nil
}

// GetOrchestrationRuntimeState gets the runtime state of an orchestration instance.
func (d *durableTaskService) GetOrchestrationRuntimeState(ctx context.Context, workitem *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, error) {
	events, err := d.getOrchestrationHistory(ctx, string(workitem.InstanceID))
	if err != nil {
		return nil, err
	}
	historyEvents, err := utils.ConvertEvents(string(workitem.InstanceID), &d.orchestrationTaskIDManager, events)
	if err != nil {
		return nil, err
	}

	state := backend.NewOrchestrationRuntimeState(workitem.InstanceID, historyEvents)
	return state, nil
}

// GetOrchestrationMetadata gets the metadata associated with the given orchestration instance ID.
//
// Returns [api.ErrInstanceNotFound] if the orchestration instance doesn't exist.
func (d *durableTaskService) GetOrchestrationMetadata(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	resp, err := d.client.GetOrchestration(ctx, &dtmbprotos.GetOrchestrationRequest{
		OrchestrationId: string(id),
		NoPayloads:      false,
	})
	if err != nil {
		code := status.Code(err)
		if code == codes.NotFound {
			return nil, api.ErrInstanceNotFound
		}

		return nil, err
	}

	ret := &api.OrchestrationMetadata{
		InstanceID:             id,
		Name:                   resp.Name,
		RuntimeStatus:          utils.ConvertOrchestrationStatusToDurableTaskServiceBackend(resp.GetOrchestrationStatus()),
		CreatedAt:              resp.GetCreatedAt().AsTime(),
		LastUpdatedAt:          resp.GetLastUpdatedAt().AsTime(),
		SerializedInput:        string(resp.GetInput()),
		SerializedOutput:       string(resp.GetOutput()),
		SerializedCustomStatus: string(resp.GetCustomStatus()),
		FailureDetails:         utils.ConvertFailureDetails(resp.GetFailureDetails()),
	}

	return ret, nil
}

func extractActionsFromOrchestrationState(state *backend.OrchestrationRuntimeState) []*dtmbprotos.OrchestratorAction {
	actions := make([]*dtmbprotos.OrchestratorAction, 0)
	pendingMessages := state.PendingMessages()
	pendingTasks := state.PendingTasks()
	pendingTimers := state.PendingTimers()
	newEvents := state.NewEvents()

	for _, task := range pendingTasks {
		action := &dtmbprotos.OrchestratorAction{
			OrchestratorActionType: &dtmbprotos.OrchestratorAction_ScheduleActivity{
				ScheduleActivity: &dtmbprotos.ScheduleActivityOrchestratorAction{
					Name:  task.GetTaskScheduled().GetName(),
					Input: []byte(task.GetTaskScheduled().GetInput().GetValue()),
					// StartAt:
				},
			},
		}
		actions = append(actions, action)
	}

	for _, timer := range pendingTimers {
		if timer.GetTimerFired().GetFireAt() == nil {
			continue
		}
		action := &dtmbprotos.OrchestratorAction{
			OrchestratorActionType: &dtmbprotos.OrchestratorAction_CreateTimer{
				CreateTimer: &dtmbprotos.CreateTimerOrchestratorAction{
					StartAt: &dtmbprotos.Delay{
						Delayed: &dtmbprotos.Delay_Time{
							Time: timer.GetTimerFired().GetFireAt(),
						},
					},
				},
			},
		}
		actions = append(actions, action)
	}

	for _, msg := range pendingMessages {
		if createdEvent := msg.HistoryEvent.GetSubOrchestrationInstanceCreated(); createdEvent != nil {
			action := &dtmbprotos.OrchestratorAction{
				OrchestratorActionType: &dtmbprotos.OrchestratorAction_CreateSubOrchestration{
					CreateSubOrchestration: &dtmbprotos.CreateSubOrchestrationOrchestratorAction{
						OrchestrationId: createdEvent.GetInstanceId(),
						Name:            createdEvent.GetName(),
						Version:         createdEvent.GetVersion().GetValue(),
						Input:           []byte(createdEvent.GetInput().GetValue()),
						// StartAt:
					},
				},
			}
			actions = append(actions, action)
		}
	}

	for _, msg := range newEvents {
		var action *dtmbprotos.OrchestratorAction = nil
		switch typedEvent := msg.GetEventType().(type) {
		case *protos.HistoryEvent_EventSent:
			action = &dtmbprotos.OrchestratorAction{
				OrchestratorActionType: &dtmbprotos.OrchestratorAction_SendEvent{
					SendEvent: &dtmbprotos.SendEventOrchestratorAction{
						InstanceId: typedEvent.EventSent.GetInstanceId(), // or shoudl this be targetInstanceId?
						Name:       typedEvent.EventSent.GetName(),
						Data:       []byte(typedEvent.EventSent.GetInput().GetValue()),
					},
				},
			}
		case *protos.HistoryEvent_ExecutionTerminated:
			action = &dtmbprotos.OrchestratorAction{
				OrchestratorActionType: &dtmbprotos.OrchestratorAction_TerminateOrchestration{
					TerminateOrchestration: &dtmbprotos.TerminateOrchestrationOrchestratorAction{
						NonRecursive: !typedEvent.ExecutionTerminated.GetRecurse(),
						Reason:       []byte(typedEvent.ExecutionTerminated.GetInput().GetValue()),
					},
				},
			}

		case *protos.HistoryEvent_ExecutionCompleted:
			var result []byte = nil
			if typedEvent.ExecutionCompleted.GetResult() != nil {
				result = []byte(typedEvent.ExecutionCompleted.GetResult().GetValue())
			}
			var failureDetails *dtmbprotos.FailureDetails = nil
			if typedEvent.ExecutionCompleted.GetFailureDetails() != nil {
				failureDetails = utils.ConvertTaskFailureDetails(typedEvent.ExecutionCompleted.GetFailureDetails())
			}
			action = &dtmbprotos.OrchestratorAction{
				OrchestratorActionType: &dtmbprotos.OrchestratorAction_CompleteOrchestration{
					CompleteOrchestration: &dtmbprotos.CompleteOrchestrationOrchestratorAction{
						OrchestrationStatus: utils.ConvertOrchestrationStatusFromDurableTaskServiceBackend(typedEvent.ExecutionCompleted.GetOrchestrationStatus()),
						FailureDetails:      failureDetails,
						Result:              result,
					},
				},
			}
			// Do we need these types??
			// =============================
			// case *protos.HistoryEvent_SubOrchestrationInstanceCompleted:
			// 	result := []byte(typedEvent.SubOrchestrationInstanceCompleted.GetResult().GetValue())
			// 	action = &dtmbprotos.OrchestratorAction{
			// 		OrchestratorActionType: &dtmbprotos.OrchestratorAction_CompleteOrchestration{
			// 			CompleteOrchestration: &dtmbprotos.CompleteOrchestrationOrchestratorAction{
			// 				OrchestrationStatus: dtmbprotos.OrchestrationStatus_COMPLETED,
			// 				FailureDetails:      nil,
			// 				Result:              result,
			// 			},
			// 		},
			// 	}
			// case *protos.HistoryEvent_SubOrchestrationInstanceFailed:
			// 	failureDetails := utils.ConvertTaskFailureDetails(typedEvent.SubOrchestrationInstanceFailed.GetFailureDetails())
			// 	action = &dtmbprotos.OrchestratorAction{
			// 		OrchestratorActionType: &dtmbprotos.OrchestratorAction_CompleteOrchestration{
			// 			CompleteOrchestration: &dtmbprotos.CompleteOrchestrationOrchestratorAction{
			// 				OrchestrationStatus: dtmbprotos.OrchestrationStatus_FAILED,
			// 				FailureDetails:      failureDetails,
			// 				Result:              nil,
			// 			},
			// 		},
			// 	}
		}
		if action != nil {
			actions = append(actions, action)
		}
	}
	return actions
}

// CompleteOrchestrationWorkItem completes a work item by saving the updated runtime state to durable storage.
func (d *durableTaskService) CompleteOrchestrationWorkItem(_ context.Context, item *backend.OrchestrationWorkItem) error {
	completionToken := item.Properties["CompletionToken"].(string)
	orchestrationName := item.Properties["OrchestrationName"].(string)
	version := item.Properties["Version"].(string)

	completionMessage := &dtmbprotos.ConnectWorkerClientMessage{
		Message: &dtmbprotos.ConnectWorkerClientMessage_CompleteOrchestration{
			CompleteOrchestration: &dtmbprotos.CompleteOrchestrationMessage{
				OrchestrationId: string(item.InstanceID),
				Name:            orchestrationName,
				Version:         version,
				CompletionToken: completionToken,
				CustomStatus:    []byte(item.State.CustomStatus.GetValue()),
				Actions:         extractActionsFromOrchestrationState(item.State),
			},
		},
	}

	d.connectWorkerClientStream <- completionMessage
	if item.State.IsCompleted() {
		d.orchestrationHistoryCache.EvictCacheForOrchestrationID(string(item.InstanceID))
		d.orchestrationTaskIDManager.PurgeOrchestration((string(item.InstanceID)))
	}

	d.logger.Debugf("=============== CompleteOrchestrationWorkItem: %v\n", completionMessage)

	return nil
}

// AbandonOrchestrationWorkItem undoes any state changes and returns the work item to the work item queue.
//
// This is called if an internal failure happens in the processing of an orchestration work item. It is
// not called if the orchestration work item is processed successfully (note that an orchestration that
// completes with a failure is still considered a successfully processed work item).
func (d *durableTaskService) AbandonOrchestrationWorkItem(_ context.Context, item *backend.OrchestrationWorkItem) error {
	completionToken := item.Properties["CompletionToken"].(string)
	orchestrationName := item.Properties["OrchestrationName"].(string)
	version := item.Properties["Version"].(string)

	d.connectWorkerClientStream <- &dtmbprotos.ConnectWorkerClientMessage{
		Message: &dtmbprotos.ConnectWorkerClientMessage_AbandonOrchestration{
			AbandonOrchestration: &dtmbprotos.AbandonOrchestrationMessage{
				OrchestrationId: string(item.InstanceID),
				Name:            orchestrationName,
				Version:         version,
				CompletionToken: completionToken,
			},
		},
	}
	return nil
}

// GetActivityWorkItem gets a pending activity work item from the task hub or returns [ErrNoWorkItems]
// if there are no pending activity work items.
func (d *durableTaskService) GetActivityWorkItem(context.Context) (*backend.ActivityWorkItem, error) {
	var ret *backend.ActivityWorkItem
	item := d.activityQueue.Dequeue()
	if item == nil {
		return nil, backend.ErrNoWorkItems
	}

	event := protos.HistoryEvent{
		EventType: &protos.HistoryEvent_TaskScheduled{
			TaskScheduled: &protos.TaskScheduledEvent{
				Name: item.GetName(),
				// Version:          // We don't use versions for activities
				Input: &wrapperspb.StringValue{Value: string(item.GetInput())},
				// ParentTraceContext: &protos.TraceContext{} // TODO: Add support for this if required
			},
		},
	}

	ret = &backend.ActivityWorkItem{
		// SequenceNumber: 0, // TODO: Determine if this is required and how to obtain it
		InstanceID: api.InstanceID(item.GetOrchestrationId()),
		NewEvent:   &event,
		Properties: map[string]interface{}{
			"CompletionToken": item.GetCompletionToken(),
			"ActivityName":    item.GetName(),
			"ExecutionId":     item.GetExecutionId(), // probably not needed
		},
	}
	return ret, nil
}

// CompleteActivityWorkItem sends a message to the parent orchestration indicating activity completion.
func (d *durableTaskService) CompleteActivityWorkItem(_ context.Context, item *backend.ActivityWorkItem) error {
	var result []byte = nil

	if item.Result.GetTaskCompleted() != nil {
		result = []byte(item.Result.GetTaskCompleted().GetResult().GetValue())
	}
	var failureDetails *dtmbprotos.FailureDetails = nil
	if item.NewEvent.GetTaskFailed() != nil {
		failureDetails = utils.ConvertTaskFailureDetails(item.Result.GetTaskFailed().GetFailureDetails())
	}

	completionToken := item.Properties["CompletionToken"].(string)
	activityName := item.Properties["ActivityName"].(string)

	d.connectWorkerClientStream <- &dtmbprotos.ConnectWorkerClientMessage{
		Message: &dtmbprotos.ConnectWorkerClientMessage_CompleteActivity{
			CompleteActivity: &dtmbprotos.CompleteActivityMessage{
				OrchestrationId: string(item.InstanceID),
				Name:            activityName,
				CompletionToken: completionToken,
				Result:          result,
				FailureDetails:  failureDetails,
			},
		},
	}
	d.logger.Debug("=============== CompleteActivityWorkItem \n")
	return nil
}

// AbandonActivityWorkItem returns the work-item back to the queue without committing any other chances.
//
// This is called when an internal failure occurs during activity work-item processing.
func (d *durableTaskService) AbandonActivityWorkItem(_ context.Context, item *backend.ActivityWorkItem) error {
	completionToken := item.Properties["CompletionToken"].(string)
	activityName := item.Properties["ActivityName"].(string)

	d.connectWorkerClientStream <- &dtmbprotos.ConnectWorkerClientMessage{
		Message: &dtmbprotos.ConnectWorkerClientMessage_AbandonActivity{
			AbandonActivity: &dtmbprotos.AbandonActivityMessage{
				OrchestrationId: string(item.InstanceID),
				Name:            activityName,
				CompletionToken: completionToken,
			},
		},
	}
	return nil
}

// PurgeOrchestrationState deletes all saved state for the specified orchestration instance.
//
// [api.ErrInstanceNotFound] is returned if the specified orchestration instance doesn't exist.
// [api.ErrNotCompleted] is returned if the specified orchestration instance is still running.
func (d *durableTaskService) PurgeOrchestrationState(ctx context.Context, id api.InstanceID) error {
	_, err := d.client.PurgeOrchestration(ctx, &dtmbprotos.PurgeOrchestrationRequest{
		Request: &dtmbprotos.PurgeOrchestrationRequest_OrchestrationId{
			OrchestrationId: string(id),
		},
		NonRecursive: false,
	})
	if err != nil {
		d.logger.Debug("PurgeOrchestrationState error: %v", err)
		if status.Code(err) == codes.NotFound {
			return api.ErrInstanceNotFound
		} else {
			return api.ErrNotCompleted
		}
	}
	d.orchestrationHistoryCache.EvictCacheForOrchestrationID(string(id))
	d.orchestrationTaskIDManager.PurgeOrchestration((string(id)))
	return nil
}
