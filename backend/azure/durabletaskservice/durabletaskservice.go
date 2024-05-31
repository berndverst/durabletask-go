package durabletaskservice

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/grpc"

	dtmbprotos "github.com/microsoft/durabletask-go/backend/azure/durabletaskservice/internal/backend/v1"
	"github.com/microsoft/durabletask-go/backend/azure/durabletaskservice/internal/utils"
)

type TaskHubClient struct {
	dtmbprotos.TaskHubClientClient
	dtmbprotos.TaskHubWorkerClient
}

type durableTaskService struct {
	logger                     backend.Logger
	config                     *durableTaskServiceBackendOptions
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
	be.config, err = newDurableTaskServiceBackendConfiguration(endpoint, taskHubName, options...)
	if err != nil {
		return nil, err
	}

	grpcDialOptions, optionErr := utils.CreateGrpcDialOptions(
		ctx, logger, be.config.Insecure, be.config.DisableAuth, be.config.TaskHubName, be.config.UserAgent, &be.config.AzureCredential, be.config.ResourceScopes, be.config.TenantID)
	if optionErr != nil {
		return nil, optionErr
	}

	connCtx, connCancel := context.WithTimeout(ctx, 15*time.Second) // TODO: make this a configurable timeout
	conn, err := grpc.DialContext(connCtx, be.config.Endpoint, grpcDialOptions...)
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
			d.config.UserAgent,
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
		case *protos.HistoryEvent_SubOrchestrationInstanceCompleted:
			var result []byte = nil
			if typedEvent.SubOrchestrationInstanceCompleted.GetResult() != nil {
				result = []byte(typedEvent.SubOrchestrationInstanceCompleted.GetResult().GetValue())
			}
			action = &dtmbprotos.OrchestratorAction{
				OrchestratorActionType: &dtmbprotos.OrchestratorAction_CompleteOrchestration{
					CompleteOrchestration: &dtmbprotos.CompleteOrchestrationOrchestratorAction{
						OrchestrationStatus: dtmbprotos.OrchestrationStatus_COMPLETED,
						FailureDetails:      nil,
						Result:              result,
					},
				},
			}
		case *protos.HistoryEvent_SubOrchestrationInstanceFailed:
			var failureDetails *dtmbprotos.FailureDetails = nil
			if typedEvent.SubOrchestrationInstanceFailed.GetFailureDetails() != nil {
				failureDetails = utils.ConvertTaskFailureDetails(typedEvent.SubOrchestrationInstanceFailed.GetFailureDetails())
			}
			action = &dtmbprotos.OrchestratorAction{
				OrchestratorActionType: &dtmbprotos.OrchestratorAction_CompleteOrchestration{
					CompleteOrchestration: &dtmbprotos.CompleteOrchestrationOrchestratorAction{
						OrchestrationStatus: dtmbprotos.OrchestrationStatus_FAILED,
						FailureDetails:      failureDetails,
						Result:              nil,
					},
				},
			}
		}
		if action != nil {
			actions = append(actions, action)
		}
	}
	return actions
}
