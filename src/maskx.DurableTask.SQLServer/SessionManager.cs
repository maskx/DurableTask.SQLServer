﻿using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.Core.Serializing;
using maskx.DurableTask.SQLServer.Settings;
using maskx.DurableTask.SQLServer.Tracking;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer
{
    internal class SessionManager
    {
        private readonly SQLServerSettings settings;
        private readonly DataConverter dataConverter = new JsonDataConverter();

        private const string CreateSessionSQL = @"
 INSERT INTO {0}
 ([InstanceId],[LockedUntilUtc],[Status],[FireAt])
 VALUES
 (@InstanceId,@LockedUntilUtc,@Status,@FireAt);

 INSERT INTO  {1}
 ([InstanceId],[ExecutionId],[SequenceNumber],[OrchestrationInstance],[FireAt], [LockedUntilUtc],[Status],[Event],[ExtensionData])
 VALUES
 ( @InstanceId,@ExecutionId,@SequenceNumber,@OrchestrationInstance,@FireAt,@LockedUntilUtc,@Status,@Event,@ExtensionData);
";

        private const string SendMessageSQL = @"
MERGE {0} TARGET
USING (VALUES (@InstanceId)) AS SOURCE ([InstanceId])
ON [Target].InstanceId = [Source].InstanceId
WHEN NOT MATCHED THEN INSERT ([InstanceId],[LockedUntilUtc],[Status],[FireAt]) VALUES (@InstanceId,@LockedUntilUtc,@Status,@FireAt)
WHEN MATCHED THEN UPDATE SET [FireAt]=IIF(FireAt<@FireAt,FireAt,@FireAt);

MERGE {1} TARGET
USING (VALUES(@InstanceId,@ExecutionId,@SequenceNumber)) AS SOURCE ([InstanceId],[ExecutionId],[SequenceNumber])
ON [Target].InstanceId = [Source].InstanceId AND [Target].ExecutionId = [Source].ExecutionId AND [Target].SequenceNumber = [Source].SequenceNumber
WHEN NOT MATCHED THEN
    INSERT
    ([InstanceId],[ExecutionId],[SequenceNumber],[OrchestrationInstance],[FireAt], [LockedUntilUtc],[Status],[Event],[ExtensionData])
    VALUES
    ( @InstanceId,@ExecutionId,@SequenceNumber,@OrchestrationInstance,@FireAt,@LockedUntilUtc,@Status,@Event,@ExtensionData)
WHEN MATCHED THEN UPDATE SET [OrchestrationInstance]=@OrchestrationInstance,FireAt=@FireAt,LockedUntilUtc=@LockedUntilUtc,Status=@Status,Event=@Event,ExtensionData=@ExtensionData;
";

        private const string GetPendingSessionSQL = @"
declare @InstanceId nvarchar(50)

update top(1) {0}
set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState
where [Status]='Pending'
and (select count(0) from {1} where {1}.InstanceId={0}.InstanceId)>0

update {1}
set [Status]='Locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData
where InstanceId=@InstanceId
";

        private const string GetTimeoutSessionSQL = @"
declare @InstanceId nvarchar(50)

update top(1) {0}
set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState
where [Status]='Locked'
and LockedUntilUtc<getutcdate()

update {1}
set [Status]='Locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData
where InstanceId=@InstanceId
";

        private const string RemoveSessionSql = @"
delete {0} where InstanceId=@InstanceId
delete {1} where InstanceId=@InstanceId
";

        private const string CompleteSessionStateSql = @"
delete {1}
where InstanceId=@InstanceId and Status='Locked'

update {0}
set SessionState=@SessionState,[Status]='Pending',LockedUntilUtc=@LockedUntilUtc
where InstanceId=@InstanceId

";

        private const string AcceptSessionSql = @"
declare @InstanceId nvarchar(50)

update top(1) {0}
set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState
where [Status]='Pending' AND FireAt<=getutcdate()

if @InstanceId is null
begin
    update top(1) {0}
    set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc
    output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState
    where [Status]='Pending'
    and FireAt is null
    and (select count(0) from {1} where {1}.InstanceId={0}.InstanceId)>0
end

if @InstanceId is null
begin
    update top(1) {0}
    set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc
    output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState
    where [Status]='Locked'
    and LockedUntilUtc<getutcdate()
end

update {1}
set [Status]='Locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData
where InstanceId=@InstanceId
";

        private const string SetSessionStateSql = @"
update {0}
set SessionState=@SessionState
where InstanceId=@InstanceId
";

        public SessionManager(SQLServerSettings settings)
        {
            this.settings = settings;
        }

        public async Task CreateSessionAsync(TaskMessage message)
        {
            string sql = string.Format(CreateSessionSQL, settings.SessionTableName, settings.SessionMessageTableName);
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(sql, new
                {
                    ExecutionId = message.OrchestrationInstance.ExecutionId,
                    InstanceId = message.OrchestrationInstance.InstanceId,
                    LockedUntilUtc = DBNull.Value,
                    Status = "Pending",
                    FireAt = GetFireTime(message),
                    SequenceNumber = message.SequenceNumber,
                    OrchestrationInstance = dataConverter.Serialize(message.OrchestrationInstance),
                    Event = dataConverter.Serialize(message.Event),
                    ExtensionData = dataConverter.Serialize(message.ExtensionData)
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
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

        public async Task SendMessageAsync(params TaskMessage[] messages)
        {
            if (messages.Length == 0)
                return;
            string sql = string.Format(SendMessageSQL, settings.SessionTableName, settings.SessionMessageTableName);
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                foreach (var msg in messages)
                {
                    cmd.AddStatement(sql, new
                    {
                        ExecutionId = msg.OrchestrationInstance.ExecutionId,
                        InstanceId = msg.OrchestrationInstance.InstanceId,
                        LockedUntilUtc = DBNull.Value,
                        Status = "Pending",
                        FireAt = GetFireTime(msg),
                        SequenceNumber = msg.Event.EventId,
                        OrchestrationInstance = dataConverter.Serialize(msg.OrchestrationInstance),
                        Event = dataConverter.Serialize(msg.Event),
                        ExtensionData = dataConverter.Serialize(msg.ExtensionData)
                    });
                }
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        private async Task<SQLServerOrchestrationSession> GetSessionAsync(string cmdText, CancellationToken cancellationToken)
        {
            SQLServerOrchestrationSession t = null;
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(cmdText, new
                {
                    LockedUntilUtc = DateTime.UtcNow.AddSeconds(this.settings.SessionLockedSeconds)
                });
                await con.OpenAsync();
                var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                if (reader.Read())
                {
                    t = new SQLServerOrchestrationSession()
                    {
                        Id = reader["InstanceId"].ToString(),
                        LockedUntilUtc = reader.GetDateTime(1),
                        SessionState = reader.IsDBNull(3) ? string.Empty : reader["SessionState"].ToString()
                    };
                    if (!reader.NextResult())
                    {
                        throw new Exception("get a session without message");
                    }
                    while (reader.Read())
                    {
                        t.Messages.Add(new TaskMessage()
                        {
                            SequenceNumber = reader.GetInt64(0),
                            OrchestrationInstance = reader.IsDBNull(1) ? null : dataConverter.Deserialize<OrchestrationInstance>(reader.GetString(1)),
                            Event = reader.IsDBNull(2) ? null : JsonConvert.DeserializeObject<HistoryEvent>(reader.GetString(2), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto }),
                            ExtensionData = reader.IsDBNull(3) ? null : dataConverter.Deserialize<ExtensionDataObject>(reader.GetString(3))
                        });
                    }
                }
            }
            return t;
        }

        public async Task<SQLServerOrchestrationSession> AcceptSessionAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            //SQLServerOrchestrationSession t = await GetSessionAsync(GetPendingSessionSQL, cancellationToken);
            //if (t == null)
            //{
            //    t = await GetSessionAsync(this.GetTimeoutSessionSQL, cancellationToken);
            //}
            //return t;
            SQLServerOrchestrationSession t = null;
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(string.Format(AcceptSessionSql, settings.SessionTableName, settings.SessionMessageTableName), new
                {
                    LockedUntilUtc = DateTime.UtcNow.AddSeconds(this.settings.SessionLockedSeconds)
                });
                await con.OpenAsync();
                var reader = await cmd.ExecuteReaderAsync(cancellationToken);
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
                        Id = reader["InstanceId"].ToString(),
                        LockedUntilUtc = reader.GetDateTime(1),
                        SessionState = reader.IsDBNull(3) ? string.Empty : reader["SessionState"].ToString()
                    };
                    if (!reader.NextResult())
                    {
                        throw new Exception("get a session without message");
                    }
                    while (reader.Read())
                    {
                        t.Messages.Add(new TaskMessage()
                        {
                            SequenceNumber = reader.GetInt64(0),
                            OrchestrationInstance = reader.IsDBNull(1) ? null : dataConverter.Deserialize<OrchestrationInstance>(reader.GetString(1)),
                            Event = reader.IsDBNull(2) ? null : JsonConvert.DeserializeObject<HistoryEvent>(reader.GetString(2), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto }),
                            ExtensionData = reader.IsDBNull(3) ? null : dataConverter.Deserialize<ExtensionDataObject>(reader.GetString(3))
                        });
                    }
                }
            }
            return t;
        }

        public async Task AbandonSessionAsync(string id)
        {
            return;
        }

        public async Task CompleteSessionAsync(string id, string newState)
        {
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                if (string.IsNullOrEmpty(newState))
                {
                    cmd.AddStatement(string.Format(RemoveSessionSql, settings.SessionTableName, settings.SessionMessageTableName), new
                    {
                        InstanceId = id
                    });
                }
                else
                {
                    cmd.AddStatement(string.Format(CompleteSessionStateSql, settings.SessionTableName, settings.SessionMessageTableName), new
                    {
                        InstanceId = id,
                        SessionState = newState,
                        LockedUntilUtc = DateTime.MinValue
                    });
                }
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task SetStateAsync(string id, string sessinoState)
        {
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(string.Format(SetSessionStateSql, settings.SessionTableName), new
                {
                    InstanceId = id,
                    SessionState = sessinoState
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task<int> GetPendingOrchestrationsCount()
        {
            int count = 0;
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(string.Format(GetPendingOrchestrationsCountSQL, settings.SessionTableName));
                await con.OpenAsync();
                count = (int)await cmd.ExecuteScalarAsync();
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
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(sql, new
                {
                    InstanceId = id,
                    LockedUntilUtc = dt
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
            return dt;
        }

        private const string GetPendingOrchestrationsCountSQL = @"
return select count(0) from {0} where [Status]='Pending'
";

        public async Task DeleteSessionManagerAsync()
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                command.AddStatement($"DROP TABLE IF EXISTS {settings.SessionMessageTableName}");
                command.AddStatement($"DROP TABLE IF EXISTS {settings.SessionTableName}");

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }
        }

        public async Task InitializeSessionManagerAsync(bool recreate)
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                if (recreate) await DeleteSessionManagerAsync();

                command.AddStatement($@"IF(SCHEMA_ID(@schema) IS NULL)
                    BEGIN
                        EXEC sp_executesql N'CREATE SCHEMA [{settings.SchemaName}]'
                    END", new { schema = settings.SchemaName });

                command.AddStatement($@"
IF(OBJECT_ID(@table) IS NULL)
BEGIN
    CREATE TABLE {settings.SessionTableName} (
        [InstanceId] [nvarchar](50) NOT NULL,
	    [LockedUntilUtc] [datetime2](7)  NULL,
        [FireAt] [datetime2](7)  NULL,
	    [Status] [nvarchar](50) NOT NULL,
	    [SessionState] [nvarchar](max) NULL,
    CONSTRAINT [PK_{settings.SchemaName}_{settings.HubName}_{SQLServerSettings.SessionTable}] PRIMARY KEY CLUSTERED
    (
	    [InstanceId] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END", new { table = settings.SessionTableName });

                command.AddStatement($@"
IF(OBJECT_ID(@table) IS NULL)
BEGIN
    CREATE TABLE {settings.SessionMessageTableName} (
	    [InstanceId] [nvarchar](50) NOT NULL,
	    [ExecutionId] [nvarchar](50) NOT NULL,
	    [SequenceNumber] [bigint] NOT NULL,
        [FireAt] [datetime2](7)  NULL,
	    [LockedUntilUtc] [datetime2](7)  NULL,
	    [Status] [nvarchar](50) NOT NULL,
        [OrchestrationInstance] [nvarchar](max) NULL,
	    [Event] [nvarchar](max) NULL,
	    [ExtensionData] [nvarchar](max) NULL,
     CONSTRAINT [PK_{settings.SchemaName}_{settings.HubName}_{SQLServerSettings.SessionMessageTable}] PRIMARY KEY CLUSTERED
    (
	    [InstanceId] ASC,
	    [ExecutionId] ASC,
	    [SequenceNumber] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END", new { table = settings.SessionMessageTableName });

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }
        }
    }
}