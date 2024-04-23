package utils

import (
	"fmt"

	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"

	dtmbprotos "github.com/microsoft/durabletask-go/backend/azure/dtmb/internal/backend/v1"
)

func ToStringWrapper(data []byte) *wrapperspb.StringValue {
	return &wrapperspb.StringValue{Value: string(data)}
}

func WrapString(data string) *wrapperspb.StringValue {
	return &wrapperspb.StringValue{Value: data}
}

func ConvertFailureDetails(failureDetails *dtmbprotos.FailureDetails) *protos.TaskFailureDetails {
	if failureDetails == nil {
		return nil
	}

	return &protos.TaskFailureDetails{
		ErrorType:      failureDetails.GetErrorType(),
		ErrorMessage:   failureDetails.GetErrorMessage(),
		StackTrace:     WrapString(failureDetails.GetStackTrace()),
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

func convertExecutionStartedEvent(typedEvent *dtmbprotos.Event_ExecutionStarted) *protos.HistoryEvent_ExecutionStarted {
	return &protos.HistoryEvent_ExecutionStarted{
		ExecutionStarted: &protos.ExecutionStartedEvent{
			Name:    typedEvent.ExecutionStarted.GetName(),
			Version: WrapString(typedEvent.ExecutionStarted.GetVersion()),
			Input:   ToStringWrapper(typedEvent.ExecutionStarted.GetInput()), // what if this is not string
			OrchestrationInstance: &protos.OrchestrationInstance{
				InstanceId:  typedEvent.ExecutionStarted.GetOrchestrationId(),
				ExecutionId: wrapperspb.String(typedEvent.ExecutionStarted.GetExecutionId()),
			},
			ParentInstance: &protos.ParentInstanceInfo{
				TaskScheduledId: -1, // int32(typedEvent.ExecutionStarted.GetParent().GetSequenceNumber()),
				Name:            WrapString(typedEvent.ExecutionStarted.GetParent().GetName()),
				Version:         WrapString(typedEvent.ExecutionStarted.GetParent().GetVersion()),
				OrchestrationInstance: &protos.OrchestrationInstance{
					InstanceId: typedEvent.ExecutionStarted.GetParent().GetOrchestrationId(),
				},
			},
			ScheduledStartTimestamp: typedEvent.ExecutionStarted.GetScheduledTime(),
			ParentTraceContext: &protos.TraceContext{
				TraceParent: typedEvent.ExecutionStarted.GetTraceContext().GetTraceParent(),
				SpanID:      typedEvent.ExecutionStarted.GetTraceContext().GetSpanId(),
				TraceState:  WrapString(typedEvent.ExecutionStarted.GetTraceContext().GetTraceState()),
			},
			// TODO (ItalyPaleAle): implement tracing
			// OrchestrationSpanID: &wrapperspb.StringValue{},
		},
	}
}

func convertExecutionCompletedEvent(typedEvent *dtmbprotos.Event_ExecutionCompleted) *protos.HistoryEvent_ExecutionCompleted {
	return &protos.HistoryEvent_ExecutionCompleted{
		ExecutionCompleted: &protos.ExecutionCompletedEvent{
			OrchestrationStatus: protos.OrchestrationStatus(typedEvent.ExecutionCompleted.GetOrchestrationStatus()),
			Result:              ToStringWrapper(typedEvent.ExecutionCompleted.GetResult()),
			FailureDetails:      ConvertFailureDetails(typedEvent.ExecutionCompleted.GetFailureDetails()),
		},
	}
}

func convertExecutionTerminatedEvent(typedEvent *dtmbprotos.Event_ExecutionTerminated) *protos.HistoryEvent_ExecutionTerminated {
	return &protos.HistoryEvent_ExecutionTerminated{
		ExecutionTerminated: &protos.ExecutionTerminatedEvent{
			Input:   ToStringWrapper(typedEvent.ExecutionTerminated.GetReason()), // what if this is not string
			Recurse: !typedEvent.ExecutionTerminated.GetNonRecursive(),
		},
	}
}

func convertExecutionResumedEvent(typedEvent *dtmbprotos.Event_ExecutionResumed) *protos.HistoryEvent_ExecutionResumed {
	return &protos.HistoryEvent_ExecutionResumed{
		ExecutionResumed: &protos.ExecutionResumedEvent{
			Input: ToStringWrapper(typedEvent.ExecutionResumed.GetInput()), // what if this is not string
		},
	}
}

func convertExecutionSuspendedEvent(typedEvent *dtmbprotos.Event_ExecutionSuspended) *protos.HistoryEvent_ExecutionSuspended {
	return &protos.HistoryEvent_ExecutionSuspended{
		ExecutionSuspended: &protos.ExecutionSuspendedEvent{
			Input: ToStringWrapper(typedEvent.ExecutionSuspended.GetInput()), // what if this is not string
		},
	}
}

func convertContinueAsNewEvent(typedEvent *dtmbprotos.Event_ContinueAsNew) *protos.HistoryEvent_ContinueAsNew {
	return &protos.HistoryEvent_ContinueAsNew{
		ContinueAsNew: &protos.ContinueAsNewEvent{
			Input: ToStringWrapper(typedEvent.ContinueAsNew.GetInput()), // what if this is not string
		},
	}
}

func convertTimerCreatedEvent(typedEvent *dtmbprotos.Event_TimerCreated) *protos.HistoryEvent_TimerCreated {
	return &protos.HistoryEvent_TimerCreated{
		TimerCreated: &protos.TimerCreatedEvent{
			FireAt: typedEvent.TimerCreated.GetExecutionTime(),
		},
	}
}

func convertTimerFiredEvent(_ *dtmbprotos.Event_TimerExecuted) *protos.HistoryEvent_TimerFired {
	return &protos.HistoryEvent_TimerFired{
		TimerFired: &protos.TimerFiredEvent{},
	}
}

func convertActivityScheduledEvent(typedEvent *dtmbprotos.Event_ActivityScheduled) *protos.HistoryEvent_TaskScheduled {
	return &protos.HistoryEvent_TaskScheduled{
		TaskScheduled: &protos.TaskScheduledEvent{
			Name:  typedEvent.ActivityScheduled.GetName(),
			Input: ToStringWrapper(typedEvent.ActivityScheduled.GetInput()), // what if this is not string
			// Version  // Activities are not versioned in DTMB
			ParentTraceContext: &protos.TraceContext{
				TraceParent: typedEvent.ActivityScheduled.GetParentTraceContext().GetTraceParent(),
				SpanID:      typedEvent.ActivityScheduled.GetParentTraceContext().GetSpanId(),
				TraceState:  WrapString(typedEvent.ActivityScheduled.GetParentTraceContext().GetTraceState()),
			},
		},
	}
}

func convertActivityCompletedEvent(taskIDManager *OrchestrationTaskCounter, orchestrationID string, typedEvent *dtmbprotos.Event_ActivityCompleted) *protos.HistoryEvent_TaskCompleted {
	return &protos.HistoryEvent_TaskCompleted{
		TaskCompleted: &protos.TaskCompletedEvent{
			TaskScheduledId: taskIDManager.GetTaskNumber(orchestrationID, typedEvent.ActivityCompleted.GetRelatedSequenceNumber()),
			Result:          ToStringWrapper(typedEvent.ActivityCompleted.GetResult()), // what if this is not string
		},
	}
}

func convertActivityFailedEvent(taskIDManager *OrchestrationTaskCounter, orchestrationID string, typedEvent *dtmbprotos.Event_ActivityFailed) *protos.HistoryEvent_TaskFailed {
	return &protos.HistoryEvent_TaskFailed{
		TaskFailed: &protos.TaskFailedEvent{
			TaskScheduledId: taskIDManager.GetTaskNumber(orchestrationID, typedEvent.ActivityFailed.GetRelatedSequenceNumber()),
			FailureDetails:  ConvertFailureDetails(typedEvent.ActivityFailed.GetFailureDetails()),
		},
	}
}

func convertEventRaisedEvent(typedEvent *dtmbprotos.Event_EventRaised) *protos.HistoryEvent_EventRaised {
	return &protos.HistoryEvent_EventRaised{
		EventRaised: &protos.EventRaisedEvent{
			Name:  typedEvent.EventRaised.GetName(),
			Input: ToStringWrapper(typedEvent.EventRaised.GetInput()), // what if this is not string
		},
	}
}

func convertEventSentEvent(typedEvent *dtmbprotos.Event_EventSent) *protos.HistoryEvent_EventSent {
	return &protos.HistoryEvent_EventSent{
		EventSent: &protos.EventSentEvent{
			InstanceId: typedEvent.EventSent.GetOrchestrationId(),
			Name:       typedEvent.EventSent.GetName(),
			Input:      ToStringWrapper(typedEvent.EventSent.GetInput()), // what if this is not string
		},
	}
}

func convertHistoryStateEvent(typedEvent *dtmbprotos.Event_HistoryState) *protos.HistoryEvent_HistoryState {
	return &protos.HistoryEvent_HistoryState{
		HistoryState: &protos.HistoryStateEvent{
			OrchestrationState: &protos.OrchestrationState{
				OrchestrationStatus: protos.OrchestrationStatus(typedEvent.HistoryState.GetOrchestrationStatus()),
			},
		},
	}
}

func convertOrchestratorStartedEvent(_ *dtmbprotos.Event_OrchestratorStarted) *protos.HistoryEvent_OrchestratorStarted {
	return &protos.HistoryEvent_OrchestratorStarted{
		OrchestratorStarted: &protos.OrchestratorStartedEvent{},
	}
}

func convertOrchestratorCompletedEvent(_ *dtmbprotos.Event_OrchestratorCompleted) *protos.HistoryEvent_OrchestratorCompleted {
	return &protos.HistoryEvent_OrchestratorCompleted{
		OrchestratorCompleted: &protos.OrchestratorCompletedEvent{},
	}
}

func convertSuborchestrationInstanceCreatedEvent(typedEvent *dtmbprotos.Event_SubOrchestrationInstanceCreated) *protos.HistoryEvent_SubOrchestrationInstanceCreated {
	return &protos.HistoryEvent_SubOrchestrationInstanceCreated{
		SubOrchestrationInstanceCreated: &protos.SubOrchestrationInstanceCreatedEvent{
			InstanceId: typedEvent.SubOrchestrationInstanceCreated.GetOrchestrationId(),
			Name:       typedEvent.SubOrchestrationInstanceCreated.GetName(),
			Version:    WrapString(typedEvent.SubOrchestrationInstanceCreated.GetVersion()),
			Input:      ToStringWrapper(typedEvent.SubOrchestrationInstanceCreated.GetInput()),
			ParentTraceContext: &protos.TraceContext{
				TraceParent: typedEvent.SubOrchestrationInstanceCreated.GetParentTraceContext().GetTraceParent(),
				SpanID:      typedEvent.SubOrchestrationInstanceCreated.GetParentTraceContext().GetSpanId(),
				TraceState:  WrapString(typedEvent.SubOrchestrationInstanceCreated.GetParentTraceContext().GetTraceState()),
			},
		},
	}
}

func convertSuborchestrationInstanceCompletedEvent(taskIDManager *OrchestrationTaskCounter, orchestrationID string, typedEvent *dtmbprotos.Event_SubOrchestrationInstanceCompleted) *protos.HistoryEvent_SubOrchestrationInstanceCompleted {
	return &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
		SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
			TaskScheduledId: taskIDManager.GetTaskNumber(orchestrationID, typedEvent.SubOrchestrationInstanceCompleted.GetRelatedSequenceNumber()),
			Result:          ToStringWrapper(typedEvent.SubOrchestrationInstanceCompleted.GetResult()), // what if this is not string
		},
	}
}

func convertSuborchestrationInstanceFailedEvent(taskIDManager *OrchestrationTaskCounter, orchestrationID string, typedEvent *dtmbprotos.Event_SubOrchestrationInstanceFailed) *protos.HistoryEvent_SubOrchestrationInstanceFailed {
	return &protos.HistoryEvent_SubOrchestrationInstanceFailed{
		SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{
			TaskScheduledId: taskIDManager.GetTaskNumber(orchestrationID, typedEvent.SubOrchestrationInstanceFailed.GetRelatedSequenceNumber()),
			FailureDetails:  ConvertFailureDetails(typedEvent.SubOrchestrationInstanceFailed.GetFailureDetails()),
		},
	}
}

func convertGenericEvent(typedEvent *dtmbprotos.Event_GenericEvent) *protos.HistoryEvent_GenericEvent {
	return &protos.HistoryEvent_GenericEvent{
		GenericEvent: &protos.GenericEvent{
			Data: ToStringWrapper(typedEvent.GenericEvent.GetData()), // what if this is not string
		},
	}
}

func ConvertEvent(orchestrationID string, taskIDManager *OrchestrationTaskCounter, event *dtmbprotos.Event) (*protos.HistoryEvent, error) {
	switch typedEvent := event.GetEventType().(type) {
	case *dtmbprotos.Event_ExecutionStarted:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertExecutionStartedEvent(typedEvent),
		}, nil
	case *dtmbprotos.Event_ExecutionCompleted:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertExecutionCompletedEvent(typedEvent),
		}, nil
	case *dtmbprotos.Event_ExecutionTerminated:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertExecutionTerminatedEvent(typedEvent),
		}, nil
	case *dtmbprotos.Event_ExecutionResumed:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertExecutionResumedEvent(typedEvent),
		}, nil
	case *dtmbprotos.Event_ExecutionSuspended:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertExecutionSuspendedEvent(typedEvent),
		}, nil
	case *dtmbprotos.Event_ContinueAsNew:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertContinueAsNewEvent(typedEvent),
		}, nil
	case *dtmbprotos.Event_TimerCreated:
		return &protos.HistoryEvent{
			EventId:   taskIDManager.GetTaskNumber(orchestrationID, event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: convertTimerCreatedEvent(typedEvent),
		}, nil
	case *dtmbprotos.Event_TimerExecuted:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertTimerFiredEvent(typedEvent),
		}, nil

	case *dtmbprotos.Event_ActivityScheduled:
		return &protos.HistoryEvent{
			EventId:   taskIDManager.GetTaskNumber(orchestrationID, event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: convertActivityScheduledEvent(typedEvent),
		}, nil

	case *dtmbprotos.Event_ActivityCompleted:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertActivityCompletedEvent(taskIDManager, orchestrationID, typedEvent),
		}, nil

	case *dtmbprotos.Event_ActivityFailed:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertActivityFailedEvent(taskIDManager, orchestrationID, typedEvent),
		}, nil

	case *dtmbprotos.Event_EventRaised:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertEventRaisedEvent(typedEvent),
		}, nil

	case *dtmbprotos.Event_EventSent:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertEventSentEvent(typedEvent),
		}, nil

	case *dtmbprotos.Event_HistoryState:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertHistoryStateEvent(typedEvent),
		}, nil

	case *dtmbprotos.Event_OrchestratorStarted:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertOrchestratorStartedEvent(typedEvent),
		}, nil

	case *dtmbprotos.Event_OrchestratorCompleted:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertOrchestratorCompletedEvent(typedEvent),
		}, nil

	case *dtmbprotos.Event_SubOrchestrationInstanceCreated:
		return &protos.HistoryEvent{
			EventId:   taskIDManager.GetTaskNumber(typedEvent.SubOrchestrationInstanceCreated.GetOrchestrationId(), event.GetSequenceNumber()),
			Timestamp: event.GetTimestamp(),
			EventType: convertSuborchestrationInstanceCreatedEvent(typedEvent),
		}, nil

	case *dtmbprotos.Event_SubOrchestrationInstanceCompleted:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertSuborchestrationInstanceCompletedEvent(taskIDManager, orchestrationID, typedEvent),
		}, nil

	case *dtmbprotos.Event_SubOrchestrationInstanceFailed:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertSuborchestrationInstanceFailedEvent(taskIDManager, orchestrationID, typedEvent),
		}, nil
	case *dtmbprotos.Event_GenericEvent:
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: event.GetTimestamp(),
			EventType: convertGenericEvent(typedEvent),
		}, nil
	default:
		return nil, fmt.Errorf("unknown event type: %T", typedEvent)
	}
}

func ConvertEvents(orchestrationID string, taskIDManager *OrchestrationTaskCounter, events []*dtmbprotos.Event) ([]*protos.HistoryEvent, error) {
	// convert events from dtmbprotos.Event to protos.HistoryEvent

	historyEvents := make([]*protos.HistoryEvent, len(events))

	for i, item := range events {
		convertedItem, err := ConvertEvent(orchestrationID, taskIDManager, item)
		if err != nil {
			return nil, err
		}
		historyEvents[i] = convertedItem
	}
	return historyEvents, nil
}
