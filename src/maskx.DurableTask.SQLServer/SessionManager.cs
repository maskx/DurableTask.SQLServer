using DurableTask.Core;
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

        private readonly string CreateSessionSQL = @"
 INSERT INTO DTF_TaskSession
 ([InstanceId],[LockedUntilUtc],[Status])
 VALUES
 (@InstanceId,@LockedUntilUtc,@Status);

 INSERT INTO  DTF_TaskMessage
 ([InstanceId],[ExecutionId],[SequenceNumber],[OrchestrationInstance], [LockedUntilUtc],[Status],[Event],[ExtensionData])
 VALUES
 ( @InstanceId,@ExecutionId,@SequenceNumber,@OrchestrationInstance,@LockedUntilUtc,@Status,@Event,@ExtensionData);
";

        private readonly string SendMessageSQL = @"
INSERT INTO  DTF_TaskMessage
 ([InstanceId],[ExecutionId],[SequenceNumber],[OrchestrationInstance], [LockedUntilUtc],[Status],[Event],[ExtensionData])
 VALUES
 ( @InstanceId,@ExecutionId,@SequenceNumber,@OrchestrationInstance,@LockedUntilUtc,@Status,@Event,@ExtensionData);
";

        private readonly string GetPendingSessionSQL = @"
declare @InstanceId nvarchar(50)

update top(1) DTF_TaskSession
set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState
where [Status]='Pending'
and (select count(0) from DTF_TaskMessage where DTF_TaskMessage.InstanceId=DTF_TaskSession.InstanceId)>0

update DTF_TaskMessage
set [Status]='Locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData
where InstanceId=@InstanceId
";

        private readonly string GetTimeoutSessionSQL = @"
declare @InstanceId nvarchar(50)

update top(1) DTF_TaskSession
set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState
where [Status]='Locked'
and LockedUntilUtc<getutcdate()

update DTF_TaskMessage
set [Status]='Locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData
where InstanceId=@InstanceId
";

        private readonly string RemoveSessionSql = @"
delete DTF_TaskMessage where InstanceId=@InstanceId
delete DTF_TaskSession where InstanceId=@InstanceId
";

        private readonly string CompleteSessionStateSql = @"
delete DTF_TaskMessage
where InstanceId=@InstanceId and Status='Locked'

update DTF_TaskSession
set SessionState=@SessionState,[Status]='Pending',LockedUntilUtc=null
where InstanceId=@InstanceId

";

        private readonly string SetSessionStateSql = @"
update DTF_TaskSession
set SessionState=@SessionState
where InstanceId=@InstanceId
";

        public SessionManager(SQLServerSettings settings)
        {
            this.settings = settings;
        }

        public async Task CreateSessionAsync(TaskMessage message)
        {
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(this.CreateSessionSQL, new
                {
                    ExecutionId = message.OrchestrationInstance.ExecutionId,
                    InstanceId = message.OrchestrationInstance.InstanceId,
                    LockedUntilUtc = DBNull.Value,
                    Status = "Pending",
                    SequenceNumber = message.SequenceNumber,
                    OrchestrationInstance = dataConverter.Serialize(message.OrchestrationInstance),
                    Event = dataConverter.Serialize(message.Event),
                    ExtensionData = dataConverter.Serialize(message.ExtensionData)
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task SendMessageBatchAsync(params TaskMessage[] messages)
        {
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();

                foreach (var msg in messages)
                {
                    cmd.AddStatement(this.SendMessageSQL, new
                    {
                        ExecutionId = msg.OrchestrationInstance.ExecutionId,
                        InstanceId = msg.OrchestrationInstance.InstanceId,
                        LockedUntilUtc = DBNull.Value,
                        Status = "Pending",
                        SequenceNumber = msg.SequenceNumber,
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
            SQLServerOrchestrationSession t = await GetSessionAsync(GetPendingSessionSQL, cancellationToken);
            if (t == null)
            {
                t = await GetSessionAsync(this.GetTimeoutSessionSQL, cancellationToken);
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
                    cmd.AddStatement(this.RemoveSessionSql, new
                    {
                        InstanceId = id
                    });
                }
                else
                {
                    cmd.AddStatement(this.CompleteSessionStateSql, new
                    {
                        InstanceId = id,
                        SessionState = newState
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
                cmd.AddStatement(this.SetSessionStateSql, new
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
                cmd.AddStatement(this.GetPendingOrchestrationsCountSQL);
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

        private readonly string GetPendingOrchestrationsCountSQL = @"
return select count(0) from DTF_TaskSession where [Status]='Pending'
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
	    [LockedUntilUtc] [datetime2](7) NULL,
	    [Status] [nvarchar](50) NULL,
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
	    [OrchestrationInstance] [nvarchar](max) NULL,
	    [LockedUntilUtc] [datetime2](7) NULL,
	    [Status] [nvarchar](50) NULL,
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