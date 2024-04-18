package utils

import (
	"fmt"

	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"

	dtmbprotos "github.com/microsoft/durabletask-go/backend/azure/dtmb/internal/backend/v1"
)

func ConvertFailureDetails(failureDetails *dtmbprotos.FailureDetails) *protos.TaskFailureDetails {
	if failureDetails == nil {
		return nil
	}

	return &protos.TaskFailureDetails{
		ErrorType:      failureDetails.GetErrorType(),
		ErrorMessage:   failureDetails.GetErrorMessage(),
		StackTrace:     &wrapperspb.StringValue{Value: failureDetails.GetStackTrace()}, // what if this is not string
		InnerFailure:   ConvertFailureDetails(failureDetails.GetInnerFailure()),
		IsNonRetriable: !failureDetails.GetRetriable(), // need to negate this
	}
}

func ConvertTaskFailureDetails(taskFailureDetails *protos.TaskFailureDetails) *dtmbprotos.FailureDetails {
	if taskFailureDetails == nil {
		return nil
	}

	return &dtmbprotos.FailureDetails{
		ErrorType:    taskFailureDetails.GetErrorType(),
		ErrorMessage: taskFailureDetails.GetErrorMessage(),
		StackTrace:   taskFailureDetails.GetStackTrace().GetValue(),
		InnerFailure: ConvertTaskFailureDetails(taskFailureDetails.GetInnerFailure()),
		Retriable:    !taskFailureDetails.GetIsNonRetriable(),
	}
}

func ConvertEvent(event *dtmbprotos.Event) (*protos.HistoryEvent, error) {
	switch typedEvent := event.GetEventType().(type) {
	case *dtmbprotos.Event_ExecutionStarted:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()), // is this correct?
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name:    typedEvent.ExecutionStarted.GetName(),
					Version: &wrapperspb.StringValue{Value: typedEvent.ExecutionStarted.GetVersion()},
					Input:   &wrapperspb.StringValue{Value: string(typedEvent.ExecutionStarted.GetInput())}, // what if this is not string
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  typedEvent.ExecutionStarted.GetOrchestrationId(),
						ExecutionId: wrapperspb.String(typedEvent.ExecutionStarted.GetExecutionId()),
					},
					ParentInstance: &protos.ParentInstanceInfo{
						TaskScheduledId: int32(typedEvent.ExecutionStarted.GetParent().GetSequenceNumber()),
						Name:            &wrapperspb.StringValue{Value: typedEvent.ExecutionStarted.GetParent().GetName()},
						Version:         &wrapperspb.StringValue{Value: typedEvent.ExecutionStarted.GetParent().GetVersion()},
						OrchestrationInstance: &protos.OrchestrationInstance{
							InstanceId: typedEvent.ExecutionStarted.GetParent().GetOrchestrationId(),
						},
					},
					ScheduledStartTimestamp: typedEvent.ExecutionStarted.GetScheduledTime(),
					ParentTraceContext: &protos.TraceContext{
						TraceParent: typedEvent.ExecutionStarted.GetTraceContext().GetTraceParent(),
						SpanID:      typedEvent.ExecutionStarted.GetTraceContext().GetSpanId(),
						TraceState:  &wrapperspb.StringValue{Value: typedEvent.ExecutionStarted.GetTraceContext().GetTraceState()},
					},
					// TODO: Alessandro to implement tracing
					// OrchestrationSpanID: &wrapperspb.StringValue{},
				},
			},
		}, nil
	case *dtmbprotos.Event_ExecutionCompleted:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_ExecutionCompleted{
				ExecutionCompleted: &protos.ExecutionCompletedEvent{
					OrchestrationStatus: protos.OrchestrationStatus(typedEvent.ExecutionCompleted.GetOrchestrationStatus()),
					Result:              &wrapperspb.StringValue{},
					FailureDetails:      ConvertFailureDetails(typedEvent.ExecutionCompleted.GetFailureDetails()),
				},
			},
		}, nil
	case *dtmbprotos.Event_ExecutionTerminated:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_ExecutionTerminated{
				ExecutionTerminated: &protos.ExecutionTerminatedEvent{
					Input:   &wrapperspb.StringValue{Value: string(typedEvent.ExecutionTerminated.GetReason())}, // what if this is not string
					Recurse: !typedEvent.ExecutionTerminated.GetNonRecursive(),
				},
			},
		}, nil
	case *dtmbprotos.Event_ExecutionResumed:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()), // is this correct?
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_ExecutionResumed{
				ExecutionResumed: &protos.ExecutionResumedEvent{
					Input: &wrapperspb.StringValue{Value: string(typedEvent.ExecutionResumed.GetInput())}, // what if this is not string
				},
			},
		}, nil
	case *dtmbprotos.Event_ExecutionSuspended:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()), // is this correct?
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_ExecutionSuspended{
				ExecutionSuspended: &protos.ExecutionSuspendedEvent{
					Input: &wrapperspb.StringValue{Value: string(typedEvent.ExecutionSuspended.GetInput())}, // what if this is not string
				},
			},
		}, nil
	case *dtmbprotos.Event_ContinueAsNew:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()), // is this correct?
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_ContinueAsNew{
				ContinueAsNew: &protos.ContinueAsNewEvent{
					Input: &wrapperspb.StringValue{Value: string(typedEvent.ContinueAsNew.GetInput())}, // what if this is not string
				},
			},
		}, nil
	case *dtmbprotos.Event_TimerCreated:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()), // is this correct?
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_TimerCreated{
				TimerCreated: &protos.TimerCreatedEvent{
					FireAt: typedEvent.TimerCreated.GetExecutionTime(),
				},
			},
		}, nil

	case *dtmbprotos.Event_TimerExecuted:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_TimerFired{
				TimerFired: &protos.TimerFiredEvent{},
			},
		}, nil

	case *dtmbprotos.Event_ActivityScheduled:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_TaskScheduled{
				TaskScheduled: &protos.TaskScheduledEvent{
					Name:  typedEvent.ActivityScheduled.GetName(),
					Input: &wrapperspb.StringValue{Value: string(typedEvent.ActivityScheduled.GetInput())}, // what if this is not string
					// Version  // Activities are not versioned in DTMB
					ParentTraceContext: &protos.TraceContext{
						TraceParent: typedEvent.ActivityScheduled.GetParentTraceContext().GetTraceParent(),
						SpanID:      typedEvent.ActivityScheduled.GetParentTraceContext().GetSpanId(),
						TraceState:  &wrapperspb.StringValue{Value: typedEvent.ActivityScheduled.GetParentTraceContext().GetTraceState()},
					},
				},
			},
		}, nil

	case *dtmbprotos.Event_ActivityCompleted:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_TaskCompleted{
				TaskCompleted: &protos.TaskCompletedEvent{
					TaskScheduledId: int32(typedEvent.ActivityCompleted.GetRelatedSequenceNumber()),
					Result:          &wrapperspb.StringValue{Value: string(typedEvent.ActivityCompleted.GetResult())}, // what if this is not string
				},
			},
		}, nil

	case *dtmbprotos.Event_ActivityFailed:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_TaskFailed{
				TaskFailed: &protos.TaskFailedEvent{
					TaskScheduledId: int32(typedEvent.ActivityFailed.GetRelatedSequenceNumber()),
					FailureDetails:  ConvertFailureDetails(typedEvent.ActivityFailed.GetFailureDetails()),
				},
			},
		}, nil

	case *dtmbprotos.Event_EventRaised:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_EventRaised{
				EventRaised: &protos.EventRaisedEvent{
					Name:  typedEvent.EventRaised.GetName(),
					Input: &wrapperspb.StringValue{Value: string(typedEvent.EventRaised.GetInput())}, // what if this is not string
				},
			},
		}, nil

	case *dtmbprotos.Event_EventSent:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_EventSent{
				EventSent: &protos.EventSentEvent{
					InstanceId: typedEvent.EventSent.GetOrchestrationId(),
					Name:       typedEvent.EventSent.GetName(),
					Input:      &wrapperspb.StringValue{Value: string(typedEvent.EventSent.GetInput())}, // what if this is not string
				},
			},
		}, nil

	case *dtmbprotos.Event_HistoryState:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_HistoryState{
				HistoryState: &protos.HistoryStateEvent{
					OrchestrationState: &protos.OrchestrationState{
						OrchestrationStatus: protos.OrchestrationStatus(typedEvent.HistoryState.GetOrchestrationStatus()),
					},
				},
			},
		}, nil

	case *dtmbprotos.Event_OrchestratorStarted:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_OrchestratorStarted{
				OrchestratorStarted: &protos.OrchestratorStartedEvent{},
			},
		}, nil

	case *dtmbprotos.Event_OrchestratorCompleted:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_OrchestratorCompleted{
				OrchestratorCompleted: &protos.OrchestratorCompletedEvent{},
			},
		}, nil

	case *dtmbprotos.Event_SubOrchestrationInstanceCreated:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_SubOrchestrationInstanceCreated{
				SubOrchestrationInstanceCreated: &protos.SubOrchestrationInstanceCreatedEvent{
					InstanceId: typedEvent.SubOrchestrationInstanceCreated.GetOrchestrationId(),
					Name:       typedEvent.SubOrchestrationInstanceCreated.GetName(),
					Version:    &wrapperspb.StringValue{Value: typedEvent.SubOrchestrationInstanceCreated.GetVersion()},
					Input:      &wrapperspb.StringValue{Value: string(typedEvent.SubOrchestrationInstanceCreated.GetInput())}, // what if this is not string
					ParentTraceContext: &protos.TraceContext{
						TraceParent: typedEvent.SubOrchestrationInstanceCreated.GetParentTraceContext().GetTraceParent(),
						SpanID:      typedEvent.SubOrchestrationInstanceCreated.GetParentTraceContext().GetSpanId(),
						TraceState:  &wrapperspb.StringValue{Value: typedEvent.SubOrchestrationInstanceCreated.GetParentTraceContext().GetTraceState()},
					},
				},
			},
		}, nil

	case *dtmbprotos.Event_SubOrchestrationInstanceCompleted:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
				SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
					TaskScheduledId: int32(typedEvent.SubOrchestrationInstanceCompleted.GetRelatedSequenceNumber()),
					Result:          &wrapperspb.StringValue{Value: string(typedEvent.SubOrchestrationInstanceCompleted.GetResult())}, // what if this is not string
				},
			},
		}, nil

	case *dtmbprotos.Event_SubOrchestrationInstanceFailed:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_SubOrchestrationInstanceFailed{
				SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{
					TaskScheduledId: int32(typedEvent.SubOrchestrationInstanceFailed.GetRelatedSequenceNumber()),
					FailureDetails:  ConvertFailureDetails(typedEvent.SubOrchestrationInstanceFailed.GetFailureDetails()),
				},
			},
		}, nil
	case *dtmbprotos.Event_GenericEvent:
		return &protos.HistoryEvent{
			EventId:   int32(event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: &protos.HistoryEvent_GenericEvent{
				GenericEvent: &protos.GenericEvent{
					Data: &wrapperspb.StringValue{Value: string(typedEvent.GenericEvent.GetData())}, // what if this is not string
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown event type: %T", typedEvent)
	}
}

func ConvertEvents(events []*dtmbprotos.Event) ([]*protos.HistoryEvent, error) {
	// convert events from dtmbprotos.Event to protos.HistoryEvent

	historyEvents := make([]*protos.HistoryEvent, len(events))

	for i, item := range events {
		convertedItem, err := ConvertEvent(item)
		if err != nil {
			return nil, err
		}
		historyEvents[i] = convertedItem
	}
	return historyEvents, nil
}
