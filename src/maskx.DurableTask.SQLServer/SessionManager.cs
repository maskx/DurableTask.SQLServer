using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.Core.Serializing;
using maskx.DurableTask.SQLServer.Database;
using maskx.DurableTask.SQLServer.Extensions;
using maskx.DurableTask.SQLServer.Settings;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer
{
    internal class SessionManager
    {
        private readonly SQLServerSettings settings;
        private readonly DataConverter dataConverter = new JsonDataConverter();

        public SessionManager(SQLServerSettings settings)
        {
            this.settings = settings;
        }

        public async Task CreateSessionAsync(TaskMessage message, OrchestrationStatus[] dedupeStatuses)
        {
            using var db = new SQLServerAccess(settings.ConnectionString);

            await db.ExecuteStoredProcedureASync(settings.CreateSessionCommandName, new
            {
                ExecutionId = message.OrchestrationInstance.ExecutionId,
                InstanceId = message.OrchestrationInstance.InstanceId,
                SequenceNumber = message.SequenceNumber,
                FireAt = GetFireTime(message),
                Event = dataConverter.Serialize(message.Event).CompressString(),
                ExtensionData = dataConverter.Serialize(message.ExtensionData).CompressString()
            });
        }

        public async Task SendMessageAsync(params TaskMessage[] messages)
        {
            if (messages.Length == 0)
                return;
            string sql = string.Format(SendMessageSQL, settings.SessionTableName, settings.SessionMessageTableName);
            using var db = new SQLServerAccess(settings.ConnectionString);
            foreach (var msg in messages)
            {
                db.AddStatement(sql, new
                {
                    msg.OrchestrationInstance.ExecutionId,
                    msg.OrchestrationInstance.InstanceId,
                    msg.SequenceNumber,
                    LockedUntilUtc = DBNull.Value,
                    Status = "Pending",
                    FireAt = GetFireTime(msg),
                    OrchestrationInstance = dataConverter.Serialize(msg.OrchestrationInstance),
                    Event = dataConverter.Serialize(msg.Event).CompressString(),
                    ExtensionData = dataConverter.Serialize(msg.ExtensionData).CompressString()
                });
            }
            await db.ExecuteNonQueryAsync();
        }

        public async Task<SQLServerOrchestrationSession> AcceptSessionAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            SQLServerOrchestrationSession t = null;
            using DbAccess db = new SQLServerAccess(settings.ConnectionString);

            db.AddStatement(string.Format(AcceptSessionSql, settings.SessionTableName, settings.SessionMessageTableName), new
            {
                LockedUntilUtc = DateTime.UtcNow.AddSeconds(this.settings.SessionLockedSeconds)
            });
            try
            {
                await db.ExecuteReaderAsync((reader) =>
                {
                    int readerCount = 0;

                    while (!reader.Read() && readerCount < 3)
                    {
                        reader.NextResult();
                        readerCount++;
                    }
                    if (readerCount < 3)
                    {
                        t = new SQLServerOrchestrationSession()
                        {
                            InstanceId = reader["InstanceId"].ToString(),
                            LockedUntilUtc = reader.GetDateTime(1),
                            SessionState = reader.IsDBNull(3) ? new OrchestrationRuntimeState() : DeserializeOrchestrationRuntimeState((byte[])reader["SessionState"])
                        };
                        if (!reader.NextResult())
                        {
                            throw new Exception("get a session without message");
                        }

                        while (reader.Read())
                        {
                            var m = new TaskMessage()
                            {
                                SequenceNumber = reader.GetInt64(0),
                                OrchestrationInstance = reader.IsDBNull(1) ? null : dataConverter.Deserialize<OrchestrationInstance>(reader.GetString(1)),
                                Event = reader.IsDBNull(2) ? null : JsonConvert.DeserializeObject<HistoryEvent>(((byte[])reader[2]).DecompressString(), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto }),
                                ExtensionData = reader.IsDBNull(3) ? null : dataConverter.Deserialize<ExtensionDataObject>(((byte[])reader[3]).DecompressString())
                            };
                            t.Messages.Add(m);
                        }
                        if (reader.NextResult())
                        {
                            while (reader.Read())
                            {
                                var m = new TaskMessage()
                                {
                                    SequenceNumber = reader.GetInt64(0),
                                    OrchestrationInstance = reader.IsDBNull(1) ? null : dataConverter.Deserialize<OrchestrationInstance>(reader.GetString(1)),
                                    Event = reader.IsDBNull(2) ? null : JsonConvert.DeserializeObject<HistoryEvent>(((byte[])reader[2]).DecompressString(), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto }),
                                    ExtensionData = reader.IsDBNull(3) ? null : dataConverter.Deserialize<ExtensionDataObject>(((byte[])reader[3]).DecompressString())
                                };
                                if (string.IsNullOrEmpty(m.OrchestrationInstance.ExecutionId))
                                    m.OrchestrationInstance.ExecutionId = t.SessionState.OrchestrationInstance.ExecutionId;
                                t.Messages.Add(m);
                            }
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return t;
        }

        public async Task AbandonSessionAsync(string id)
        {
            //TODO: abandon session
            using var db = new SQLServerAccess(settings.ConnectionString);
            db.AddStatement(string.Format(AbandonSessionSql, settings.SessionTableName, settings.SessionMessageTableName), new
            {
                InstanceId = id
            });
            await db.ExecuteNonQueryAsync();
        }

        public async Task SetStateAsync(string id, OrchestrationRuntimeState? newState)
        {
            using var db = new SQLServerAccess(settings.ConnectionString);
            if (newState == null)
            {
                db.AddStatement(string.Format(RemoveSessionSql, settings.SessionTableName, settings.SessionMessageTableName), new
                {
                    InstanceId = id
                });
            }
            else
            {
                db.AddStatement(string.Format(CompleteSessionStateSql, settings.SessionTableName, settings.SessionMessageTableName), new
                {
                    InstanceId = id,
                    SessionState = SerializeOrchestrationRuntimeState(newState),
                    LockedUntilUtc = DBNull.Value
                });
            }
            await db.ExecuteNonQueryAsync();
        }

        public async Task<int> GetPendingOrchestrationsCount()
        {
            int count = 0;
            using (var db = new SQLServerAccess(settings.ConnectionString))
            {
                db.AddStatement(string.Format(GetPendingOrchestrationsCountSQL, settings.SessionTableName));
                var rtv = await db.ExecuteScalarAsync();
                count = rtv == null ? 0 : (int)rtv;
            }
            return count;
        }

        public async Task<DateTime> RenewLock(string id)
        {
            string sql = $@"
UPDATE {this.settings.SessionTableName}
SET LockedUntilUtc=@LockedUntilUtc
WHERE InstanceId=@InstanceId
";

            DateTime dt = DateTime.UtcNow.AddSeconds(this.settings.MessageLockedSeconds);
            using (var db = new SQLServerAccess(settings.ConnectionString))
            {
                db.AddStatement(sql, new
                {
                    InstanceId = id,
                    LockedUntilUtc = dt
                });
                await db.ExecuteNonQueryAsync();
            }
            return dt;
        }

        private DateTime? GetFireTime(TaskMessage msg)
        {
            DateTime? dt = null;
            if (msg.Event is TimerFiredEvent fe)
            {
                dt = fe.FireAt;
            }
            else if (msg.Event is TimerCreatedEvent ce)
            {
                dt = ce.FireAt;
            }
            return dt;
        }

        private byte[] SerializeOrchestrationRuntimeState(OrchestrationRuntimeState runtimeState)
        {
            if (runtimeState == null)
            {
                return null;
            }

            return JsonConvert.SerializeObject(runtimeState.Events,
                new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto }).CompressString();
        }

        private OrchestrationRuntimeState DeserializeOrchestrationRuntimeState(byte[] serializedState)
        {
            var events = JsonConvert.DeserializeObject<IList<HistoryEvent>>(serializedState.DecompressString(), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto });
            return new OrchestrationRuntimeState(events);
        }

        private const string GetPendingOrchestrationsCountSQL = @"select count(0) from {0} where [Status]='Pending'";

        private const string CreateSessionSQL = @"
BEGIN TRANSACTION
     INSERT INTO {0}
     ([InstanceId],[LockedUntilUtc],[Status])
     VALUES
     (@InstanceId,@LockedUntilUtc,@Status);

     INSERT INTO  {1}
     ([InstanceId],[ExecutionId],[SequenceNumber],[OrchestrationInstance],[FireAt], [LockedUntilUtc],[Status],[Event],[ExtensionData])
     VALUES
     ( @InstanceId,@ExecutionId,@SequenceNumber,@OrchestrationInstance,@FireAt,@LockedUntilUtc,@Status,@Event,@ExtensionData);
COMMIT TRANSACTION
";

        private const string SendMessageSQL = @"
BEGIN TRANSACTION
    if @ExecutionId is not null
    begin
        MERGE {0} with (serializable) TARGET
        USING (VALUES (@InstanceId)) AS SOURCE ([InstanceId])
        ON [Target].InstanceId = [Source].InstanceId
        WHEN NOT MATCHED THEN INSERT ([InstanceId],[LockedUntilUtc],[Status]) VALUES (@InstanceId,@LockedUntilUtc,@Status);
    end

    INSERT INTO {1}
    ([InstanceId],[ExecutionId],[SequenceNumber],[OrchestrationInstance],[FireAt], [LockedUntilUtc],[Status],[Event],[ExtensionData])
    VALUES
    ( @InstanceId,@ExecutionId,@SequenceNumber,@OrchestrationInstance,@FireAt,@LockedUntilUtc,@Status,@Event,@ExtensionData);
COMMIT TRANSACTION
";

        private const string RemoveSessionSql = @"
BEGIN TRANSACTION
    delete {0} where InstanceId=@InstanceId-- and ExecutionId=@ExecutionId
    delete {1} where InstanceId=@InstanceId-- and ExecutionId=@ExecutionId
COMMIT TRANSACTION
";

        private const string CompleteSessionStateSql = @"
BEGIN TRANSACTION
    delete {1} where InstanceId=@InstanceId  and Status='Locked';
    update {0}
    set SessionState=@SessionState,[Status]='Pending',LockedUntilUtc=@LockedUntilUtc
    where InstanceId=@InstanceId
COMMIT TRANSACTION
";

        private const string AcceptSessionSql = @"
declare @InstanceId nvarchar(50)
declare @ExecutionId nvarchar(50)

BEGIN TRANSACTION
    update top(1) {0}
    set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc
    output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState
    where [Status]='Pending'
    and [SessionState] is not null
    and (select min(FireAt) from {1} where {0}.InstanceId={1}.InstanceId and FireAt is not null and [Status]='Pending') <=getutcdate()

    if @InstanceId is not null
    begin
        update {1}
        set [Status]='Locked',LockedUntilUtc=@LockedUntilUtc
        output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData
        where InstanceId=@InstanceId
        and FireAt<=getutcdate()
        and [Status]='Pending'
COMMIT TRANSACTION
        return
    end

    if @InstanceId is null
    begin
        update top(1) {0}
        set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc
        output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState
        where [Status]='Pending'
            and (select count(0) from {1} where {1}.InstanceId={0}.InstanceId
                and FireAt is null
                and [Status]='Pending')>0
    end

    if @InstanceId is null
    begin
        update top(1) {0}
        set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc
        output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState--,INSERTED.ExecutionId
        where [Status]='Locked'
        and LockedUntilUtc<getutcdate()
    end

    if @InstanceId is not null
    begin
        update {1}
        set [Status]='Locked',LockedUntilUtc=@LockedUntilUtc,@ExecutionId=ExecutionId
        output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData
        where InstanceId=@InstanceId
            and FireAt is null
            and ExecutionId is not null
            and [Status]='Pending'
    end

    if @ExecutionId is null
    begin
        update top(1) {1}
        set [Status]='Locked',LockedUntilUtc=@LockedUntilUtc,@ExecutionId=ExecutionId
        output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData
        where InstanceId=@InstanceId
            and FireAt is null
            and ExecutionId is  null
    end
COMMIT TRANSACTION
";

        private const string AbandonSessionSql = @"
BEGIN TRANSACTION
    update {1} set Status=N'Abandon' where InstanceId=@InstanceId and Status=N'Locked'
    update {0} set Status=N'Pending' where InstanceId=@InstanceId
COMMIT TRANSACTION
";
    }
}