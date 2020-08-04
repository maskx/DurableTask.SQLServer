﻿using DurableTask.Core;
using DurableTask.Core.History;
using System.Text;

namespace maskx.DurableTask.SQLServer.Utils
{
    public static class DTUtils
    {
        public static string GetEventTypeString(TaskMessage message)
    => message.Event.EventType.ToString();

        public static int GetTaskEventId(TaskMessage message)
            => GetTaskEventId(message.Event);

        public static int GetTaskEventId(HistoryEvent historyEvent)
        {
            if (TryGetTaskScheduledId(historyEvent, out int taskScheduledId))
            {
                return taskScheduledId;
            }

            return historyEvent.EventId;
        }

        private static bool TryGetTaskScheduledId(HistoryEvent historyEvent, out int taskScheduledId)
        {
            switch (historyEvent.EventType)
            {
                case EventType.TaskCompleted:
                    taskScheduledId = ((TaskCompletedEvent)historyEvent).TaskScheduledId;
                    return true;

                case EventType.TaskFailed:
                    taskScheduledId = ((TaskFailedEvent)historyEvent).TaskScheduledId;
                    return true;

                case EventType.SubOrchestrationInstanceCompleted:
                    taskScheduledId = ((SubOrchestrationInstanceCompletedEvent)historyEvent).TaskScheduledId;
                    return true;

                case EventType.SubOrchestrationInstanceFailed:
                    taskScheduledId = ((SubOrchestrationInstanceFailedEvent)historyEvent).TaskScheduledId;
                    return true;

                case EventType.TimerFired:
                    taskScheduledId = ((TimerFiredEvent)historyEvent).TimerId;
                    return true;

                default:
                    taskScheduledId = -1;
                    return false;
            }
        }

        public static int GetPayloadSizeInBytes(TaskMessage message)
        {
            if (TryGetPayloadText(message.Event, out string? payloadText) && payloadText != null)
            {
                return Encoding.UTF8.GetByteCount(payloadText);
            }

            return -1;
        }

        public static bool TryGetPayloadText(HistoryEvent e, out string? payloadText)
        {
            payloadText = e.EventType switch
            {
                EventType.ContinueAsNew => ((ContinueAsNewEvent)e).Result,
                EventType.EventRaised => ((EventRaisedEvent)e).Input,
                EventType.EventSent => ((EventSentEvent)e).Input,
                EventType.ExecutionCompleted => ((ExecutionCompletedEvent)e).Result,
                EventType.ExecutionFailed => ((ExecutionCompletedEvent)e).Result,
                EventType.ExecutionStarted => ((ExecutionStartedEvent)e).Input,
                EventType.ExecutionTerminated => ((ExecutionTerminatedEvent)e).Input,
                EventType.GenericEvent => ((GenericEvent)e).Data,
                EventType.SubOrchestrationInstanceCompleted => ((SubOrchestrationInstanceCompletedEvent)e).Result,
                EventType.SubOrchestrationInstanceCreated => ((SubOrchestrationInstanceCreatedEvent)e).Input,
                EventType.SubOrchestrationInstanceFailed => ((SubOrchestrationInstanceFailedEvent)e).Details,
                EventType.TaskCompleted => ((TaskCompletedEvent)e).Result,
                EventType.TaskFailed => ((TaskFailedEvent)e).Details,
                EventType.TaskScheduled => ((TaskScheduledEvent)e).Input,
                _ => null,
            };

            if (payloadText != null)
            {
                // DTFx serializes null values as "null", which we don't want to save in the DB
                payloadText = StripJsonNulls(payloadText);
                return payloadText != null;
            }

            return false;
        }

        public static string? GetName(HistoryEvent historyEvent)
        {
            return historyEvent.EventType switch
            {
                EventType.EventRaised => ((EventRaisedEvent)historyEvent).Name,
                EventType.EventSent => ((EventSentEvent)historyEvent).Name,
                EventType.ExecutionStarted => ((ExecutionStartedEvent)historyEvent).Name,
                EventType.SubOrchestrationInstanceCreated => ((SubOrchestrationInstanceCreatedEvent)historyEvent).Name,
                EventType.TaskScheduled => ((TaskScheduledEvent)historyEvent).Name,
                _ => null,
            };
        }

        public static OrchestrationStatus? GetRuntimeStatus(HistoryEvent historyEvent)
        {
            switch (historyEvent.EventType)
            {
                case EventType.ExecutionCompleted:
                case EventType.ExecutionFailed:
                    return ((ExecutionCompletedEvent)historyEvent).OrchestrationStatus;

                default:
                    return null;
            }
        }

        private static string? StripJsonNulls(string value)
        {
            if (value == "null")
            {
                return null;
            }

            return value;
        }
    }
}