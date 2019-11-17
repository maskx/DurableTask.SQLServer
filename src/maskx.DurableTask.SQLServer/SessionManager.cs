using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.Core.Serializing;
using maskx.DurableTask.SQLServer.Settings;
using maskx.DurableTask.SQLServer.Tracking;
using Newtonsoft.Json;
using System;
using System.Reflection.Metadata;
using System.Runtime.Serialization;
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
 ([InstanceId],[ExecutionId],[LockedUntilUtc],[Status])
 VALUES
 (@InstanceId,@ExecutionId,@LockedUntilUtc,@Status);

 INSERT INTO  {1}
 ([InstanceId],[ExecutionId],[SequenceNumber],[OrchestrationInstance],[FireAt], [LockedUntilUtc],[Status],[Event],[ExtensionData])
 VALUES
 ( @InstanceId,@ExecutionId,@SequenceNumber,@OrchestrationInstance,@FireAt,@LockedUntilUtc,@Status,@Event,@ExtensionData);
";

        private const string SendMessageSQL = @"
if @ExecutionId is not null
begin
    MERGE {0} TARGET
    USING (VALUES (@InstanceId,@ExecutionId)) AS SOURCE ([InstanceId],[ExecutionId])
    ON [Target].InstanceId = [Source].InstanceId and [Target].ExecutionId = [Source].ExecutionId
    WHEN NOT MATCHED THEN INSERT ([InstanceId],[ExecutionId],[LockedUntilUtc],[Status]) VALUES (@InstanceId,@ExecutionId,@LockedUntilUtc,@Status);

end

INSERT INTO {1}
([InstanceId],[ExecutionId],[SequenceNumber],[OrchestrationInstance],[FireAt], [LockedUntilUtc],[Status],[Event],[ExtensionData])
VALUES
( @InstanceId,@ExecutionId,@SequenceNumber,@OrchestrationInstance,@FireAt,@LockedUntilUtc,@Status,@Event,@ExtensionData);
";

        private const string RemoveSessionSql = @"
delete {0} where InstanceId=@InstanceId and ExecutionId=@ExecutionId
delete {1} where InstanceId=@InstanceId and ExecutionId=@ExecutionId
";

        private const string CompleteSessionStateSql = @"
delete {1}
where InstanceId=@InstanceId and ExecutionId=@ExecutionId and Status='Locked'

update {0}
set SessionState=@SessionState,[Status]='Pending',LockedUntilUtc=@LockedUntilUtc
where InstanceId=@InstanceId and ExecutionId=@ExecutionId
";

        private const string AcceptSessionSql = @"
declare @InstanceId nvarchar(50)
declare @ExecutionId nvarchar(50)

update top(1) {0}
set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc,@ExecutionId=ExecutionId
output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState,INSERTED.ExecutionId
where [Status]='Pending'
and (select min(FireAt) from {1} where {0}.InstanceId={1}.InstanceId and {0}.ExecutionId={1}.ExecutionId and FireAt is not null) <=getutcdate()

if @InstanceId is not null
begin
    update {1}
    set [Status]='Locked',LockedUntilUtc=@LockedUntilUtc
    output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData
    where InstanceId=@InstanceId and ExecutionId=@ExecutionId
    and FireAt<=getutcdate()
    return
end

if @InstanceId is null
begin
    update top(1) {0}
    set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc,@ExecutionId=ExecutionId
    output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState,INSERTED.ExecutionId
    where [Status]='Pending'
    and (select count(0) from {1} where {1}.InstanceId={0}.InstanceId
            and ({1}.ExecutionId={0}.ExecutionId or {1}.ExecutionId is null)
            and FireAt is null)>0
end

if @InstanceId is null
begin
    update top(1) {0}
    set @InstanceId=InstanceId,[Status]='Locked',LockedUntilUtc=@LockedUntilUtc,@ExecutionId=ExecutionId
    output INSERTED.InstanceId,INSERTED.LockedUntilUtc,INSERTED.[Status],INSERTED.SessionState,INSERTED.ExecutionId
    where [Status]='Locked'
    and LockedUntilUtc<getutcdate()
end

if @InstanceId is not null
begin
    update {1}
    set [Status]='Locked',LockedUntilUtc=@LockedUntilUtc,ExecutionId=@ExecutionId
    output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData
    where InstanceId=@InstanceId
        and (ExecutionId=@ExecutionId or ExecutionId is null)
        and FireAt is null
end
";

        private const string SetSessionStateSql = @"
update {0}
set SessionState=@SessionState
where InstanceId=@InstanceId
and ExecutionId=@ExecutionId
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

        public async Task<SQLServerOrchestrationSession> AcceptSessionAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
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
                        InstanceId = reader["InstanceId"].ToString(),
                        ExecutionId = reader["ExecutionId"].ToString(),
                        LockedUntilUtc = reader.GetDateTime(1),
                        SessionState = reader.IsDBNull(3) ? string.Empty : reader["SessionState"].ToString()
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
                            Event = reader.IsDBNull(2) ? null : JsonConvert.DeserializeObject<HistoryEvent>(reader.GetString(2), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto }),
                            ExtensionData = reader.IsDBNull(3) ? null : dataConverter.Deserialize<ExtensionDataObject>(reader.GetString(3))
                        };
                        if (string.IsNullOrEmpty(m.OrchestrationInstance.ExecutionId))
                            m.OrchestrationInstance.ExecutionId = t.ExecutionId;
                        t.Messages.Add(m);
                    }
                }
            }
            return t;
        }

        public async Task AbandonSessionAsync(string id)
        {
            //TODO: abandon session
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(string.Format(AcceptSessionSql, settings.SessionTableName, settings.SessionMessageTableName), new
                {
                    LockedUntilUtc = DateTime.UtcNow.AddSeconds(this.settings.SessionLockedSeconds)
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task CompleteSessionAsync(string id, string newState)
        {
            var ids = ExtractId(id);
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                if (string.IsNullOrEmpty(newState))
                {
                    cmd.AddStatement(string.Format(RemoveSessionSql, settings.SessionTableName, settings.SessionMessageTableName), new
                    {
                        InstanceId = ids.InstanceId,
                        ExecutionId = ids.ExecutionId
                    });
                }
                else
                {
                    cmd.AddStatement(string.Format(CompleteSessionStateSql, settings.SessionTableName, settings.SessionMessageTableName), new
                    {
                        InstanceId = ids.InstanceId,
                        ExecutionId = ids.ExecutionId,
                        SessionState = newState,
                        LockedUntilUtc = DBNull.Value
                    });
                }
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task SetStateAsync(string id, string sessinoState)
        {
            var ids = ExtractId(id);
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(string.Format(SetSessionStateSql, settings.SessionTableName), new
                {
                    InstanceId = ids.InstanceId,
                    ExecutionId = ids.ExecutionId,
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

        private (string InstanceId, string ExecutionId) ExtractId(string id)
        {
            var s = id.Split('_');
            return (s[0], s[1]);
        }

        public async Task<DateTime> RenewLock(string id)
        {
            string sql = $@"
UPDATE {this.settings.SessionTableName}
SET LockedUntilUtc=@LockedUntilUtc
WHERE InstanceId=@InstanceId
AND ExecutionId=@ExecutionId
";
            var ids = ExtractId(id);
            DateTime dt = DateTime.UtcNow.AddSeconds(this.settings.MessageLockedSeconds);
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(sql, new
                {
                    InstanceId = ids.InstanceId,
                    ExecutionId = ids.ExecutionId,
                    LockedUntilUtc = dt
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
            return dt;
        }

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
        [ExecutionId] [nvarchar](50) NOT NULL,
	    [LockedUntilUtc] [datetime2](7)  NULL,
	    [Status] [nvarchar](50) NOT NULL,
	    [SessionState] [nvarchar](max) NULL,
    CONSTRAINT [PK_{settings.SchemaName}_{settings.HubName}_{SQLServerSettings.SessionTable}] PRIMARY KEY CLUSTERED
    (
	    [InstanceId] ASC,
        [ExecutionId] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END", new { table = settings.SessionTableName });

                command.AddStatement($@"
IF(OBJECT_ID(@table) IS NULL)
BEGIN
    CREATE TABLE {settings.SessionMessageTableName} (
	    [InstanceId] [nvarchar](50) NOT NULL,
	    [ExecutionId] [nvarchar](50)  NULL,
	    [SequenceNumber] [bigint] NOT NULL,
	    [FireAt] [datetime2](7) NULL,
	    [LockedUntilUtc] [datetime2](7) NULL,
	    [Status] [nvarchar](50) NOT NULL,
	    [OrchestrationInstance] [nvarchar](max) NULL,
	    [Event] [nvarchar](max) NULL,
	    [ExtensionData] [nvarchar](max) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END", new { table = settings.SessionMessageTableName });

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }
        }

        private const string GetPendingOrchestrationsCountSQL = @"return select count(0) from {0} where [Status]='Pending'";
    }
}