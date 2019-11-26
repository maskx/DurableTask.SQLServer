using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.Core.Serializing;
using DurableTask.Core.Tracking;
using maskx.DurableTask.SQLServer.SQL;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer.Tracking
{
    /// <summary>
    /// SQL Server Instance store provider to allow storage and lookup for orchestration state event history with query support
    /// </summary>
    public class SqlServerInstanceStore : IOrchestrationServiceInstanceStore
    {
        private readonly DataConverter dataConverter = new JsonDataConverter();
        private readonly SqlServerInstanceStoreSettings settings;

        /// <summary>
        /// Creates a new SqlServerInstanceStore using the supplied settings
        /// </summary>
        /// <param name="settings">Configuration values for the Instnace Store</param>
        public SqlServerInstanceStore(SqlServerInstanceStoreSettings settings)
        {
            ValidateSettings(settings);

            this.settings = settings;
        }

        /// <inheritdoc />
        public int MaxHistoryEntryLength => int.MaxValue;

        /// <inheritdoc />
        public async Task<object> DeleteEntitiesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            using (var db = new DbAccess(settings.ConnectionString))
            {
                foreach (var entity in entities)
                {
                    if (entity is OrchestrationStateInstanceEntity state)
                    {
                        db.AddStatement($"DELETE FROM {settings.OrchestrationStateTableName} WHERE InstanceId = @instanceId AND ExecutionId = @executionId",
                            new { instanceId = state.State.OrchestrationInstance.InstanceId, executionId = state.State.OrchestrationInstance.ExecutionId });
                    }
                    else if (entity is OrchestrationWorkItemInstanceEntity workItem)
                    {
                        db.AddStatement($"DELETE FROM {settings.WorkItemTableName} WHERE InstanceId = @instanceId AND ExecutionId = @executionId AND SequenceNumber = @sequenceNumber",
                            new { instanceId = workItem.InstanceId, executionId = workItem.ExecutionId, sequenceNumber = workItem.SequenceNumber });
                    }
                    else
                        throw new InvalidOperationException($"Invalid history event type: {entity.GetType()}");
                }
                await db.ExecuteNonQueryAsync();
            }

            return null;
        }

        /// <inheritdoc />
        public async Task<object> DeleteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
        {
            using (var db = new DbAccess(settings.ConnectionString))
            {
                var sql = String.Format("DELETE FROM {0} WHERE InstanceId = @InstanceId AND ExecutionId = @ExecutionId;", settings.JumpStartTableWithSchema);

                foreach (var entity in entities)
                {
                    db.AddStatement(sql, new
                    {
                        InstanceId = entity.State.OrchestrationInstance.InstanceId,
                        ExecutionId = entity.State.OrchestrationInstance.ExecutionId
                    });
                }
                await db.ExecuteNonQueryAsync();
            }
            return null;
        }

        /// <inheritdoc />
        public async Task DeleteStoreAsync()
        {
            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement($"DROP TABLE IF EXISTS {settings.WorkItemTableName}");
                db.AddStatement($"DROP TABLE IF EXISTS {settings.OrchestrationStateTableName}");
                await db.ExecuteNonQueryAsync();
            }
        }

        /// <inheritdoc />
        public async Task<IEnumerable<OrchestrationStateInstanceEntity>> GetEntitiesAsync(string instanceId, string executionId)
        {
            var entities = new List<OrchestrationStateInstanceEntity>();
            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement($"SELECT StateData FROM {settings.OrchestrationStateTableName} WHERE InstanceId = @instanceId AND ExecutionId = @executionId", new
                {
                    instanceId,
                    executionId
                });

                await db.ExecuteReaderAsync((reader, index) =>
                {
                    entities.Add(new OrchestrationStateInstanceEntity { State = dataConverter.Deserialize<OrchestrationState>(reader.GetString(0)) });
                });
            }
            return entities;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<OrchestrationJumpStartInstanceEntity>> GetJumpStartEntitiesAsync(int top)
        {
            var entities = new List<OrchestrationJumpStartInstanceEntity>();

            using (var db = new DbAccess(settings.ConnectionString))
            {
                string sql = String.Format("SELECT TOP({0}) * FROM {1} ", top, settings.JumpStartTableWithSchema);
                db.AddStatement(sql);

                await db.ExecuteReaderAsync((reader, index) =>
                {
                    var jumpStartEntity = new OrchestrationJumpStartInstanceEntity();
                    jumpStartEntity.SequenceNumber = reader.GetValue<long>("SequenceNumber");
                    jumpStartEntity.JumpStartTime = reader.GetValue<DateTime>("JumpStartTime");

                    var state = new OrchestrationState();
                    state.CompletedTime = reader.GetValue<DateTime>("CompletedTime");
                    state.CompressedSize = reader.GetValue<long>("CompressedSize");
                    state.CreatedTime = reader.GetValue<DateTime>("CreatedTime");
                    state.Input = reader.GetValue<string>("Input");
                    state.LastUpdatedTime = reader.GetValue<DateTime>("LastUpdatedTime");
                    state.Name = reader.GetValue<string>("Name");

                    state.OrchestrationInstance = dataConverter.Deserialize<OrchestrationInstance>(reader.GetValue<string>("OrchestrationInstance"));
                    state.OrchestrationStatus = reader.GetValue<OrchestrationStatus>("OrchestrationStatus");
                    state.Output = reader.GetValue<string>("Output");
                    state.ParentInstance = dataConverter.Deserialize<ParentInstance>(reader.GetValue<string>("ParentInstance"));
                    state.Size = reader.GetValue<long>("Size");
                    state.Status = reader.GetValue<string>("Status");
                    state.Tags = dataConverter.Deserialize<Dictionary<string, string>>(reader.GetValue<string>("Tags"));
                    state.Version = reader.GetValue<string>("Version");

                    jumpStartEntity.State = state;

                    entities.Add(jumpStartEntity);
                });
            }
            return entities;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<OrchestrationWorkItemInstanceEntity>> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId)
        {
            var entities = new List<OrchestrationWorkItemInstanceEntity>();
            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement($"SELECT InstanceId, ExecutionId, EventTimestamp, SequenceNumber, HistoryEvent FROM {settings.WorkItemTableName} WHERE InstanceId = @instanceId AND ExecutionId = @executionId ORDER BY SequenceNumber", new
                {
                    instanceId,
                    executionId
                });

                await db.ExecuteReaderAsync((reader, index) =>
                {
                    entities.Add(new OrchestrationWorkItemInstanceEntity
                    {
                        InstanceId = reader.GetFieldValue<string>(0),
                        ExecutionId = reader.GetFieldValue<string>(1),
                        EventTimestamp = reader.GetFieldValue<DateTime>(2),
                        SequenceNumber = reader.GetFieldValue<long>(3),
                        HistoryEvent = dataConverter.Deserialize<HistoryEvent>(reader.GetFieldValue<string>(4))
                    });
                });
            }
            return entities;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<OrchestrationStateInstanceEntity>> GetOrchestrationStateAsync(string instanceId, bool allInstances)
        {
            using (var db = new DbAccess(settings.ConnectionString))
            {
                string sql = string.Empty;
                if (allInstances == false)
                {
                    sql = $"SELECT top(1) StateData FROM {settings.OrchestrationStateTableName} WHERE InstanceId = @instanceId  AND OrchestrationStatus != @status ORDER BY LastUpdatedTime";
                    db.AddStatement(sql, new
                    {
                        instanceId,
                        status = OrchestrationStatus.ContinuedAsNew.ToString()
                    });
                }
                else
                {
                    sql = $"SELECT StateData FROM {settings.OrchestrationStateTableName} WHERE InstanceId = @instanceId  ORDER BY LastUpdatedTime";
                    db.AddStatement(sql, new
                    {
                        instanceId
                    });
                }
                var entities = new List<OrchestrationStateInstanceEntity>();
                await db.ExecuteReaderAsync((reader, index) =>
                {
                    entities.Add(new OrchestrationStateInstanceEntity { State = dataConverter.Deserialize<OrchestrationState>(reader.GetFieldValue<string>(0)) });
                });
                return entities;
            }
        }

        /// <inheritdoc />
        public async Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement($"SELECT TOP 1 StateData FROM {settings.OrchestrationStateTableName} WHERE InstanceId = @instanceId AND ExecutionId = @executionId", new
                {
                    instanceId,
                    executionId
                });
                var value = await db.ExecuteScalarAsync();
                if (value == null)
                    return null;
                return new OrchestrationStateInstanceEntity { State = dataConverter.Deserialize<OrchestrationState>(value.ToString()) };
            }
        }

        /// <inheritdoc />
        public async Task InitializeStoreAsync(bool recreate)
        {
            if (recreate) await DeleteStoreAsync();
            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement($@"IF(SCHEMA_ID(@schema) IS NULL)
                    BEGIN
                        EXEC sp_executesql N'CREATE SCHEMA [{settings.SchemaName}]'
                    END", new { schema = settings.SchemaName });

                db.AddStatement($@"IF(OBJECT_ID(@table) IS NULL)
                    BEGIN
                        CREATE TABLE {settings.OrchestrationStateTableName} (
	                        [InstanceId] NVARCHAR(50) NOT NULL,
	                        [ExecutionId] NVARCHAR(50) NOT NULL,
	                        [Name] NVARCHAR(MAX) NOT NULL,
	                        [Version] NVARCHAR(MAX) NOT NULL,
	                        [OrchestrationStatus] NVARCHAR(50) NOT NULL,
	                        [CreatedTime]  DATETIME2 NOT NULL,
	                        [CompletedTime] DATETIME2 NOT NULL,
	                        [LastUpdatedTime] DATETIME2 NOT NULL,
	                        [StateData] NVARCHAR(MAX) NOT NULL,
                            CONSTRAINT [PK_{settings.SchemaName}_{settings.HubName}{SqlServerInstanceStoreSettings.OrchestrationTable}_InstanceId_ExecutionId] PRIMARY KEY CLUSTERED ([InstanceId], [ExecutionId]))
                    END", new { table = settings.OrchestrationStateTableName });

                db.AddStatement($@"IF(OBJECT_ID(@table) IS NULL)
                    BEGIN
                        CREATE TABLE {settings.WorkItemTableName} (
	                        [InstanceId] NVARCHAR(50) NOT NULL,
	                        [ExecutionId] NVARCHAR(50) NOT NULL,
	                        [SequenceNumber] BIGINT NOT NULL,
	                        [EventTimestamp] DATETIME2 NOT NULL,
	                        [HistoryEvent] NVARCHAR(MAX) NOT NULL,
                            CONSTRAINT [PK_{settings.SchemaName}_{settings.HubName}{SqlServerInstanceStoreSettings.WorkitemTable}_InstanceId_ExecutionId_SequenceNumber] PRIMARY KEY CLUSTERED ([InstanceId], [ExecutionId], [SequenceNumber]))
                    END", new { table = settings.WorkItemTableName });
                await db.ExecuteNonQueryAsync();
            }
        }

        /// <inheritdoc />
        public async Task<int> PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            var deleteStatement = $@"DELETE h FROM {settings.WorkItemTableName} h JOIN {settings.OrchestrationStateTableName} e ON e.InstanceId = h.InstanceId AND e.ExecutionId = h.ExecutionId ";

            switch (timeRangeFilterType)
            {
                case OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter:
                    deleteStatement += "WHERE e.CompletedTime <= @thresholdDateTimeUtc";
                    break;

                case OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter:
                    deleteStatement += "WHERE e.CreatedTime <= @thresholdDateTimeUtc";
                    break;

                case OrchestrationStateTimeRangeFilterType.OrchestrationLastUpdatedTimeFilter:
                    deleteStatement += "WHERE e.LastUpdatedTime <= @thresholdDateTimeUtc";
                    break;

                default:
                    throw new ArgumentOutOfRangeException($"Unknown {nameof(timeRangeFilterType)} value: {timeRangeFilterType}");
            }

            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement(deleteStatement, new
                {
                    thresholdDateTimeUtc
                });

                return await db.ExecuteNonQueryAsync();
            }
        }

        /// <inheritdoc />
        public async Task<object> WriteEntitiesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            using (var db = new DbAccess(settings.ConnectionString))
            {
                foreach (var entity in entities)
                {
                    if (entity is OrchestrationStateInstanceEntity orchestration)
                    {
                        OrchestrationState state = orchestration.State;
                        db.AddStatement(string.Format(MergeOrchestrationStateInstanceEntityQuery, settings.OrchestrationStateTableName),
                            new
                            {
                                instanceId = state.OrchestrationInstance.InstanceId,
                                executionId = state.OrchestrationInstance.ExecutionId,
                                name = state.Name,
                                version = state.Version,
                                orchestrationStatus = state.OrchestrationStatus.ToString(),
                                createdTime = state.CreatedTime,
                                completedTime = state.CompletedTime,
                                lastUpdatedTime = state.LastUpdatedTime,
                                stateData = dataConverter.Serialize(state)
                            });
                    }
                    else if (entity is OrchestrationWorkItemInstanceEntity workItem)
                    {
                        db.AddStatement(string.Format(MergeOrchestrationWorkItemInstanceEntityQuery, settings.WorkItemTableName),
                            new
                            {
                                instanceId = workItem.InstanceId,
                                executionId = workItem.ExecutionId,
                                sequenceNumber = workItem.SequenceNumber,
                                eventTimestamp = workItem.EventTimestamp,
                                historyEvent = dataConverter.Serialize(workItem.HistoryEvent)
                            });
                    }
                    else
                        throw new InvalidOperationException($"Invalid history event type: {entity.GetType()}");
                }
                await db.ExecuteNonQueryAsync();
            }

            return null;
        }

        /// <inheritdoc />
        public async Task<object> WriteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
        {
            if (entities == null || entities.Count() == 0)
                return null;

            try
            {
                using (var db = new DbAccess(settings.ConnectionString))
                {
                    var sql = String.Format(MergeJumpStartEntitiesQuery, settings.JumpStartTableWithSchema);

                    foreach (var entity in entities)
                    {
                        var state = entity.State;
                        db.AddStatement(sql, new
                        {
                            state.OrchestrationInstance.InstanceId,
                            state.OrchestrationInstance.ExecutionId,
                            entity.SequenceNumber,
                            entity.JumpStartTime,
                            state.CompletedTime,
                            state.CompressedSize,
                            state.CreatedTime,
                            state.Input,
                            state.LastUpdatedTime,
                            state.Name,
                            state.Output,
                            state.Size,
                            state.Status,
                            state.Version,
                            OrchestrationInstance = dataConverter.Serialize(state.OrchestrationInstance),
                            OrchestrationStatus = state.OrchestrationStatus.ToString(),
                            ParentInstance = dataConverter.Serialize(state.ParentInstance),
                            Tags = dataConverter.Serialize(state.Tags)
                        });
                    }
                    await db.ExecuteNonQueryAsync();
                }
            }
            catch (Exception)
            {
                throw;
            }

            return null;
        }

        private void ValidateSettings(SqlServerInstanceStoreSettings settings)
        {
            if (settings.HubName == null) throw new ArgumentException($"{nameof(settings.HubName)} cannot be null.");
            if (settings.SchemaName == null) throw new ArgumentException($"{nameof(settings.SchemaName)} cannot be null.");

            //Validate schema and hubnames are valid SQL Identifiers
            var sqlIdentifierRegex = new Regex(@"^[\p{L}_][\p{L}\p{N}@$#_]{0,127}$");
            if (sqlIdentifierRegex.IsMatch(settings.SchemaName) == false) throw new ArgumentException($"{nameof(settings.SchemaName)} must be a valid SQL Identifier");
            if (sqlIdentifierRegex.IsMatch(settings.HubName) == false) throw new ArgumentException($"{nameof(settings.HubName)} must be a valid SQL Identifier");
        }

        private const string MergeOrchestrationStateInstanceEntityQuery =
            @"MERGE {0} [Target] USING (VALUES (@instanceId,@executionId,@name,@version,@orchestrationStatus,@createdTime,@completedTime,@lastUpdatedTime,@stateData)) as [Source](InstanceId,ExecutionId,[Name],[Version],OrchestrationStatus,CreatedTime,CompletedTime,LastUpdatedTime,StateData)
                ON [Target].InstanceId = [Source].InstanceId AND [Target].ExecutionId = [Source].ExecutionId
              WHEN NOT MATCHED THEN INSERT (InstanceId,ExecutionId,[Name],[Version],OrchestrationStatus,CreatedTime,CompletedTime,LastUpdatedTime,StateData) VALUES (InstanceId,ExecutionId,[Name],[Version],OrchestrationStatus,CreatedTime,CompletedTime,LastUpdatedTime,StateData)
              WHEN MATCHED THEN UPDATE SET InstanceId = [Source].InstanceId,ExecutionId = [Source].ExecutionId,[Name] = [Source].[Name],[Version] = [Source].[Version],OrchestrationStatus = [Source].OrchestrationStatus,CreatedTime = [Source].CreatedTime,CompletedTime = [Source].CompletedTime,LastUpdatedTime = [Source].LastUpdatedTime,StateData = [Source].StateData;";

        private const string MergeOrchestrationWorkItemInstanceEntityQuery =
            @"MERGE {0} [Target] USING (VALUES (@instanceId,@executionId,@sequenceNumber,@eventTimestamp,@historyEvent)) as [Source](InstanceId,ExecutionId,SequenceNumber,EventTimestamp,HistoryEvent)
                ON [Target].InstanceId = [Source].InstanceId AND [Target].ExecutionId = [Source].ExecutionId AND [Target].SequenceNumber = [Source].SequenceNumber
              WHEN NOT MATCHED THEN INSERT (InstanceId, ExecutionId, SequenceNumber, EventTimestamp, HistoryEvent) VALUES (InstanceId, ExecutionId, SequenceNumber, EventTimestamp, HistoryEvent)
              WHEN MATCHED THEN UPDATE SET EventTimestamp = [Source].EventTimestamp, HistoryEvent = [Source].HistoryEvent;";

        private const string MergeJumpStartEntitiesQuery = @"
MERGE {0} [Target] USING (VALUES(@InstanceId, @ExecutionId, @SequenceNumber, @JumpStartTime, @CompletedTime, @CompressedSize, @CreatedTime, @Input, @LastUpdatedTime, @Name, @OrchestrationInstance, @OrchestrationStatus, @Output, @ParentInstance, @Size, @Status, @Tags, @Version)) AS SOURCE (InstanceId, ExecutionId, SequenceNumber, JumpStartTime, CompletedTime, CompressedSize, CreatedTime, Input, LastUpdatedTime, Name, OrchestrationInstance, OrchestrationStatus, Output, ParentInstance, Size, Status, Tags, Version)
ON [Target].InstanceId = [Source].InstanceId AND [Target].ExecutionId = [Source].ExecutionId AND [Target].SequenceNumber = [Source].SequenceNumber
WHEN NOT MATCHED THEN THEN Insert (InstanceId, ExecutionId, SequenceNumber, JumpStartTime, CompletedTime, CompressedSize, CreatedTime, Input, LastUpdatedTime, Name, OrchestrationInstance, OrchestrationStatus, Output, ParentInstance, Size, Status, Tags, Version) VALUES (@InstanceId, @ExecutionId, @SequenceNumber, @JumpStartTime, @CompletedTime, @CompressedSize, @CreatedTime, @Input, @LastUpdatedTime, @Name, @OrchestrationInstance, @OrchestrationStatus, @Output, @ParentInstance, @Size, @Status, @Tags, @Version)
WHEN MATCHED THEN UPDATE SET JumpStartTime=[Source].JumpStartTime;
";
    }
}