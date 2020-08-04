using DurableTask.Core;
using DTCommon = DurableTask.Core.Common;
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;
using DurableTask.Core.Serializing;
using DurableTask.Core.Tracing;
using DurableTask.Core.Tracking;
using maskx.DurableTask.SQLServer.Settings;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using maskx.DurableTask.SQLServer.Utils;

namespace maskx.DurableTask.SQLServer
{
    public class SQLServerOrchestrationService : IOrchestrationService, IOrchestrationServiceClient, IDisposable
    {
        private static readonly DataConverter DataConverter = new JsonDataConverter();

        private readonly SessionManager sessionManager;
        private readonly MessageManager messageMagager;
        private readonly SQLServerOrchestrationServiceSettings settings;
        private readonly SQLServerSettings sqlSettings;
        private const int StatusPollingIntervalInSeconds = 2;

        private readonly CancellationTokenSource cancellationTokenSource;

        private readonly IOrchestrationServiceInstanceStore instanceStore;

        /// <summary>
        /// Creates a new instance of the SQLServerOrchestrationService
        /// </summary>
        public SQLServerOrchestrationService(string connectionString,
            string hubName,
            IOrchestrationServiceInstanceStore instanceStore,
            SQLServerOrchestrationServiceSettings settings)
        {
            this.settings = settings;
            this.instanceStore = instanceStore;
            sqlSettings = new SQLServerSettings()
            {
                SchemaName = settings.SchemaName,
                HubName = hubName,
                MessageLockedSeconds = settings.MessageLockedSeconds,
                SessionLockedSeconds = settings.SessionLockedSeconds,
                ConnectionString = connectionString
            };
            this.sessionManager = new SessionManager(sqlSettings);
            this.messageMagager = new MessageManager(sqlSettings);
            this.cancellationTokenSource = new CancellationTokenSource();
        }

        /// <inheritdoc />
        public Task CreateAsync()
        {
            return CreateAsync(true);
        }

        /// <inheritdoc />
        public async Task CreateAsync(bool recreateInstanceStore)
        {
            await SqlUtils.InitializeDatabase(true, sqlSettings);
        }

        /// <inheritdoc />
        public async Task CreateIfNotExistsAsync()
        {
            await SqlUtils.InitializeDatabase(false, sqlSettings);
        }

        /// <inheritdoc />
        public Task DeleteAsync()
        {
            return DeleteAsync(true);
        }

        /// <inheritdoc />
        public async Task DeleteAsync(bool deleteInstanceStore)
        {
            await SqlUtils.ClearDatabaseAsync(sqlSettings);
        }

        /// <inheritdoc />
        public Task StartAsync()
        {
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task StopAsync(bool isForced)
        {
            this.cancellationTokenSource.Cancel();
            TraceHelper.Trace(TraceEventType.Information, "SQLServerOrchestrationService-Stop", () => string.Empty);
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task StopAsync()
        {
            return StopAsync(false);
        }

        /// <summary>
        /// Determines whether is a transient or not.
        /// </summary>
        /// <param name="exception">The exception.</param>
        /// <returns>
        ///   <c>true</c> if is transient exception; otherwise, <c>false</c>.
        /// </returns>
        public bool IsTransientException(Exception exception)
        {
            return false;
        }

        /// <inheritdoc />
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            return CreateTaskOrchestrationAsync(creationMessage, null);
        }

        /// <inheritdoc />
        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            if (!(creationMessage.Event is ExecutionStartedEvent))
            {
                throw new InvalidOperationException("Invalid creation task message");
            }
            try
            {
                await this.sessionManager.CreateSessionAsync(creationMessage);
            }
            catch (Exception ex)
            {
                TraceHelper.Trace(TraceEventType.Warning, "SQLServerOrchestrationService-CreateTaskOrchestrationAsync", $"Error while adding message to ServiceBus: {ex}");
            }
        }

        /// <inheritdoc />
        public async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            await SendTaskOrchestrationMessageBatchAsync(message);
        }

        /// <inheritdoc />
        public async Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            if (messages.Length == 0)
            {
                return;
            }
            await this.sessionManager.SendMessageAsync(messages);
        }

        /// <inheritdoc />
        public async Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            ThrowIfInstanceStoreNotConfigured();
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException("instanceId");
            }
            double timeoutSeconds = timeout.TotalSeconds;
            while (!cancellationToken.IsCancellationRequested && timeoutSeconds > 0)
            {
                OrchestrationState state;
                if (string.IsNullOrEmpty(executionId))
                {
                    state = (await GetOrchestrationStateAsync(instanceId, false))?.FirstOrDefault();
                }
                else
                {
                    state = (await GetOrchestrationStateAsync(instanceId, executionId));
                }
                if (state == null
                    || (state.OrchestrationStatus == OrchestrationStatus.Running)
                    || (state.OrchestrationStatus == OrchestrationStatus.Pending))
                {
                    await Task.Delay(StatusPollingIntervalInSeconds * 1000, cancellationToken);
                    timeoutSeconds -= StatusPollingIntervalInSeconds;
                }
                else
                {
                    return state;
                }
            }

            return null;
        }

        /// <inheritdoc />
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            ThrowIfInstanceStoreNotConfigured();
            OrchestrationStateInstanceEntity state = await this.instanceStore.GetOrchestrationStateAsync(instanceId, executionId);
            return state?.State;
        }

        /// <inheritdoc />
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            ThrowIfInstanceStoreNotConfigured();
            IEnumerable<OrchestrationStateInstanceEntity> states = await this.instanceStore.GetOrchestrationStateAsync(instanceId, allExecutions);
            return states?.Select(s => s.State).ToList() ?? new List<OrchestrationState>();
        }

        /// <inheritdoc />
        public async Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            ThrowIfInstanceStoreNotConfigured();
            IEnumerable<OrchestrationWorkItemInstanceEntity> historyEvents =
                await this.instanceStore.GetOrchestrationHistoryEventsAsync(instanceId, executionId);

            return DataConverter.Serialize(historyEvents.Select(historyEventEntity => historyEventEntity.HistoryEvent));
        }

        /// <inheritdoc />
        public async Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            TraceHelper.Trace(TraceEventType.Information, "SQLServerOrchestrationService-PurgeOrchestrationHistory-Start", $"Purging orchestration instances before: {thresholdDateTimeUtc}, Type: {timeRangeFilterType}");

            if (this.instanceStore != null)
            {
                int purgedEvents = await this.instanceStore.PurgeOrchestrationHistoryEventsAsync(thresholdDateTimeUtc, timeRangeFilterType);
                TraceHelper.Trace(TraceEventType.Information, "SQLServerOrchestrationService-HistoryPurged", $"Purged {purgedEvents} orchestration histories");
            }
        }

        /// <inheritdoc />
        public int MaxConcurrentTaskOrchestrationWorkItems => this.settings.TaskOrchestrationDispatcherSettings.MaxConcurrentOrchestrations;

        /// <inheritdoc />
        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            using var cts1 = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.cancellationTokenSource.Token);

            SQLServerOrchestrationSession taskSession = await this.sessionManager.AcceptSessionAsync(receiveTimeout, cts1.Token);

            if (taskSession == null)
            {
                using var cts2 = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.cancellationTokenSource.Token);
                await Task.Delay(this.settings.IdleSleepMilliSeconds, cts2.Token);
                return null;
            }
            long maxSequenceNumber = taskSession.Messages.Max(message => message.SequenceNumber);
            if (this.instanceStore != null)
            {
                TaskMessage executionStartedMessage = taskSession.Messages.FirstOrDefault(m => m.Event is ExecutionStartedEvent);

                if (executionStartedMessage != null)
                {
                    await UpdateInstanceStoreAsync(executionStartedMessage.Event as ExecutionStartedEvent, maxSequenceNumber);
                }
            }
            var wi = new TaskOrchestrationWorkItem
            {
                NewMessages = taskSession.Messages.ToList(),
                InstanceId = taskSession.Id,
                LockedUntilUtc = taskSession.LockedUntilUtc,
                OrchestrationRuntimeState = taskSession.SessionState
            };

            return wi;
        }

        /// <inheritdoc />
        public async Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> workItemTimerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState state)
        {
            OrchestrationRuntimeState runtimeState = workItem.OrchestrationRuntimeState;
            if (await this.TrySetSessionStateAsync(workItem, newOrchestrationRuntimeState))
            {
                if (outboundMessages?.Count > 0)
                {
                    await this.messageMagager.SendMessageAsync(outboundMessages.ToArray());
                }

                if (workItemTimerMessages?.Count > 0)
                {
                    await this.sessionManager.SendMessageAsync(workItemTimerMessages.ToArray());
                }
                if (orchestratorMessages?.Count > 0)
                {
                    await this.sessionManager.SendMessageAsync(orchestratorMessages.ToArray());
                }
                if (continuedAsNewMessage != null)
                {
                    await this.sessionManager.SendMessageAsync(continuedAsNewMessage);
                }
                if (this.instanceStore != null)
                {
                    var trackingMessages = CreateTrackingMessagesAsync(runtimeState, 1);
                    await ProcessTrackingWorkItemAsync(trackingMessages);
                }
            }
        }

        /// <inheritdoc />
        public async Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            TraceHelper.Trace(TraceEventType.Critical, "SQLServerOrchestrationService-AbandonTaskOrchestrationWorkItemAsync", $"Orchestration()");

            await this.sessionManager.AbandonSessionAsync(workItem.InstanceId);
        }

        /// <inheritdoc />
        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public int TaskActivityDispatcherCount => this.settings.TaskActivityDispatcherSettings.DispatcherCount;

        /// <summary>
        ///  Should we carry over unexecuted raised events to the next iteration of an orchestration on ContinueAsNew
        /// </summary>
        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

        /// <inheritdoc />
        public int MaxConcurrentTaskActivityWorkItems => this.settings.TaskActivityDispatcherSettings.MaxConcurrentActivities;

        /// <inheritdoc />
        public async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string message)
        {
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                Event = new ExecutionTerminatedEvent(-1, message)
            };

            await SendTaskOrchestrationMessageAsync(taskMessage);
        }

        /// <inheritdoc />
        public async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            workItem.LockedUntilUtc = await this.sessionManager.RenewLock(workItem.InstanceId);
        }

        /// <inheritdoc />
        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }

        /// <inheritdoc />
        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            return 1;
        }

        /// <inheritdoc />
        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            return 1;
        }

        /// <inheritdoc />
        public int TaskOrchestrationDispatcherCount => this.settings.TaskOrchestrationDispatcherSettings.DispatcherCount;

        /******************************/
        // Task activity methods
        /******************************/

        /// <inheritdoc />
        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            using var cts1 = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.cancellationTokenSource.Token);
            TaskActivityWorkItem workItem = await this.messageMagager.ReceiveMessageAsync(receiveTimeout, cts1.Token);

            if (workItem == null)
            {
                using var cts2 = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.cancellationTokenSource.Token);
                await Task.Delay(this.settings.IdleSleepMilliSeconds, cts2.Token);
                return null;
            }

            return workItem;
        }

        /// <inheritdoc />
        public async Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            await this.messageMagager.AbandonMessageAsync(workItem);
        }

        /// <inheritdoc />
        public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            var t1 = this.messageMagager.CompleteMessageAsync(workItem);
            var t2 = this.sessionManager.SendMessageAsync(responseMessage);
            await Task.WhenAll(t1, t2);
        }

        /// <inheritdoc />
        public async Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            workItem.LockedUntilUtc = await this.messageMagager.RenewLock(workItem);
            return workItem;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.cancellationTokenSource.Cancel();
                this.cancellationTokenSource.Dispose();
            }
        }

        #region support unit test

        public async Task<int> GetPendingOrchestrationsCount()
        {
            return await this.sessionManager.GetPendingOrchestrationsCount();
        }

        #endregion support unit test

        private void ThrowIfInstanceStoreNotConfigured()
        {
            if (this.instanceStore == null)
            {
                throw new InvalidOperationException("Instance store is not configured");
            }
        }

        private async Task<bool> TrySetSessionStateAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState
            )
        {
            if (newOrchestrationRuntimeState == null ||
              newOrchestrationRuntimeState.ExecutionStartedEvent == null ||
              newOrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.Running)
            {
                await this.sessionManager.SetStateAsync(workItem.InstanceId, null);
                return true;
            }
            await this.sessionManager.SetStateAsync(workItem.InstanceId, newOrchestrationRuntimeState);
            return true;
        }

        private async Task ProcessTrackingWorkItemAsync(TrackingWorkItem workItem, OrchestrationRuntimeState state = null)
        {
            if (workItem == null)
                return;
            var historyEntities = new List<OrchestrationWorkItemInstanceEntity>();
            var stateEntities = new List<OrchestrationStateInstanceEntity>();

            foreach (TaskMessage taskMessage in workItem.NewMessages)
            {
                if (taskMessage.Event.EventType == EventType.HistoryState)
                {
                    var s = (taskMessage.Event as HistoryStateEvent)?.State;
                    if (string.IsNullOrEmpty(s.OrchestrationInstance.ExecutionId))
                    {
                        s.OrchestrationInstance.ExecutionId = state.OrchestrationInstance.ExecutionId;
                    }
                    stateEntities.Add(new OrchestrationStateInstanceEntity
                    {
                        State = s,
                        SequenceNumber = taskMessage.SequenceNumber
                    });
                }
                else
                {
                    if (string.IsNullOrEmpty(taskMessage.OrchestrationInstance.ExecutionId))
                    {
                        taskMessage.OrchestrationInstance.ExecutionId = state.OrchestrationInstance.ExecutionId;
                    }
                    historyEntities.Add(new OrchestrationWorkItemInstanceEntity
                    {
                        InstanceId = taskMessage.OrchestrationInstance.InstanceId,
                        ExecutionId = taskMessage.OrchestrationInstance.ExecutionId,
                        SequenceNumber = taskMessage.SequenceNumber,
                        EventTimestamp = DateTime.UtcNow,
                        HistoryEvent = taskMessage.Event
                    });
                }
            }

            TraceEntities(TraceEventType.Verbose, "Writing tracking history event", historyEntities, GetNormalizedWorkItemEvent);
            TraceEntities(TraceEventType.Verbose, "Writing tracking state event", stateEntities, GetNormalizedStateEvent);

            try
            {
                await this.instanceStore.WriteEntitiesAsync(historyEntities);
            }
            catch (Exception e) when (!DTCommon.Utils.IsFatal(e))
            {
                TraceEntities(TraceEventType.Critical, $"Failed to write history entity: {e}", historyEntities, GetNormalizedWorkItemEvent);
                throw;
            }

            try
            {
                // TODO : send batch to instance store, it can write it as individual if it chooses
                foreach (OrchestrationStateInstanceEntity stateEntity in stateEntities)
                {
                    if (String.IsNullOrEmpty(stateEntity.State.OrchestrationInstance.ExecutionId))
                    {
                        break;
                    }
                    await this.instanceStore.WriteEntitiesAsync(new List<OrchestrationStateInstanceEntity> { stateEntity });
                }
            }
            catch (Exception e) when (!DTCommon.Utils.IsFatal(e))
            {
                TraceEntities(TraceEventType.Critical, $"Failed to write state entity: {e}", stateEntities, GetNormalizedStateEvent);
                throw;
            }
        }

        private void TraceEntities<T>(TraceEventType eventType, string message, IEnumerable<T> entities, Func<int, string, T, string> traceGenerator)
        {
            var index = 0;
            foreach (T entry in entities)
            {
                int idx = index;
                TraceHelper.Trace(eventType, "SQLServerOrchestrationService-Entities", () => traceGenerator(idx, message, entry));
                index++;
            }
        }

        private string GetNormalizedWorkItemEvent(int index, string message, OrchestrationWorkItemInstanceEntity entity)
        {
            string serializedHistoryEvent = DTCommon.Utils.EscapeJson(DataConverter.Serialize(entity.HistoryEvent));
            int historyEventLength = serializedHistoryEvent.Length;
            int maxLen = this.instanceStore?.MaxHistoryEntryLength ?? int.MaxValue;

            if (historyEventLength > maxLen)
            {
                serializedHistoryEvent = serializedHistoryEvent.Substring(0, maxLen) + " ....(truncated)..]";
            }

            return GetFormattedLog(
                $"{message} - #{index} - Instance Id: {entity.InstanceId}, Execution Id: {entity.ExecutionId}, HistoryEvent Length: {historyEventLength}\n{serializedHistoryEvent}");
        }

        private string GetFormattedLog(string input)
        {
            // TODO : take context from the dispatcher and use that to format logs
            return input;
        }

        private string GetNormalizedStateEvent(int index, string message, OrchestrationStateInstanceEntity stateEntity)
        {
            string serializedHistoryEvent = DTCommon.Utils.EscapeJson(DataConverter.Serialize(stateEntity.State));
            int historyEventLength = serializedHistoryEvent.Length;

            int maxLen = this.instanceStore?.MaxHistoryEntryLength ?? int.MaxValue;

            if (historyEventLength > maxLen)
            {
                serializedHistoryEvent = serializedHistoryEvent.Substring(0, maxLen) + " ....(truncated)..]";
            }

            return GetFormattedLog(
                $"{message} - #{index} - Instance Id: {stateEntity.State?.OrchestrationInstance?.InstanceId},"
                + $" Execution Id: {stateEntity.State?.OrchestrationInstance?.ExecutionId},"
                + $" State Length: {historyEventLength}\n{serializedHistoryEvent}");
        }

        private Task UpdateInstanceStoreAsync(ExecutionStartedEvent executionStartedEvent, long sequenceNumber)
        {
            var orchestrationState = new OrchestrationState()
            {
                Name = executionStartedEvent.Name,
                Version = executionStartedEvent.Version,
                OrchestrationInstance = executionStartedEvent.OrchestrationInstance,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Input = executionStartedEvent.Input,
                Tags = executionStartedEvent.Tags,
                CreatedTime = executionStartedEvent.Timestamp,
                LastUpdatedTime = DateTime.UtcNow,
                CompletedTime = DTCommon.DateTimeUtils.MinDateTime,
                ParentInstance = executionStartedEvent.ParentInstance
            };

            var orchestrationStateEntity = new OrchestrationStateInstanceEntity
            {
                State = orchestrationState,
                SequenceNumber = sequenceNumber
            };

            return this.instanceStore.WriteEntitiesAsync(new[] { orchestrationStateEntity });
        }

        private TrackingWorkItem CreateTrackingMessagesAsync(OrchestrationRuntimeState runtimeState, long sequenceNumber)
        {
            if (string.IsNullOrWhiteSpace(runtimeState?.OrchestrationInstance?.InstanceId))
            {
                return null;
            }
            List<TaskMessage> newMessages = new List<TaskMessage>();
            int historyEventIndex = runtimeState.Events.Count - runtimeState.NewEvents.Count;
            foreach (HistoryEvent he in runtimeState.NewEvents)
            {
                newMessages.Add(new TaskMessage
                {
                    Event = he,
                    SequenceNumber = historyEventIndex++,
                    OrchestrationInstance = runtimeState.OrchestrationInstance
                });
            }
            try
            {
                newMessages.Add(new TaskMessage
                {
                    Event = new HistoryStateEvent(-1, DTCommon.Utils.BuildOrchestrationState(runtimeState)),
                    SequenceNumber = 999,
                    OrchestrationInstance = runtimeState.OrchestrationInstance
                });
            }
            catch (Exception ex)
            {
                throw ex;
            }
            // TODO: LockedUntilUtc need to be set
            return new TrackingWorkItem
            {
                InstanceId = runtimeState.OrchestrationInstance.InstanceId,
                LockedUntilUtc = DateTime.UtcNow,
                NewMessages = newMessages,
                SessionInstance = null//TODO: need set to session
            };
        }
    }
}