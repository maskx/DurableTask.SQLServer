using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.Core.Serializing;
using maskx.DurableTask.SQLServer.Settings;
using maskx.DurableTask.SQLServer.Tracking;
using Newtonsoft.Json;
using System;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer
{
    internal class MessageMagager
    {
        private readonly SQLServerSettings settings;
        private readonly DataConverter dataConverter = new JsonDataConverter();

        public MessageMagager(SQLServerSettings settings)
        {
            this.settings = settings;
        }

        public async Task<TaskActivityWorkItem> GetMessageAsync(string sqlText, TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            TaskActivityWorkItem t = null;
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(sqlText, new
                {
                    LockedUntilUtc = DateTime.UtcNow.AddSeconds(this.settings.MessageLockedSeconds)
                });
                await con.OpenAsync(cancellationToken);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                if (reader.Read())
                {
                    t = new TaskActivityWorkItem
                    {
                        Id = reader["Id"].ToString(),
                        LockedUntilUtc = reader.GetDateTime(4),
                        TaskMessage = new TaskMessage
                        {
                            SequenceNumber = reader.GetInt64(0),
                            OrchestrationInstance = reader.IsDBNull(1) ? null : dataConverter.Deserialize<OrchestrationInstance>(reader.GetString(1)),
                            Event = reader.IsDBNull(2) ? null : JsonConvert.DeserializeObject<HistoryEvent>(reader.GetString(2), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto }),
                            ExtensionData = reader.IsDBNull(3) ? null : dataConverter.Deserialize<ExtensionDataObject>(reader.GetString(3))
                        }
                    };
                    //  t.Id = $"{t.TaskMessage.OrchestrationInstance.InstanceId}_{t.TaskMessage.OrchestrationInstance.ExecutionId}_{t.TaskMessage.SequenceNumber}";
                }
            }
            return t;
        }

        public async Task<TaskActivityWorkItem> ReceiveMessageAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            string sqlGetPending = $@"
update top(1) {settings.MessageTableName}
set [Status]='locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData,INSERTED.LockedUntilUtc,INSERTED.Id
where [Status]='Pending'
";
            string sqlGetTimeout = $@"
update top(1) {settings.MessageTableName}
set [Status]='locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData,INSERTED.LockedUntilUtc,INSERTED.Id
where [Status]='Locked'
and LockedUntilUtc<getutcdate()
";
            TaskActivityWorkItem t = await GetMessageAsync(sqlGetPending, receiveTimeout, cancellationToken);
            if (t == null)
                t = await GetMessageAsync(sqlGetTimeout, receiveTimeout, cancellationToken);
            return t;
        }

        public async Task SendMessageAsync(params TaskMessage[] message)
        {
            string sql = $@"
INSERT INTO {this.settings.MessageTableName}
(Id,[InstanceId],[ExecutionId],[SequenceNumber],[OrchestrationInstance], [LockedUntilUtc],[Status],[Event],[ExtensionData])
VALUES
(newid(),@InstanceId,@ExecutionId,@SequenceNumber,@OrchestrationInstance,@LockedUntilUtc,@Status,@Event,@ExtensionData);
";
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                foreach (var msg in message)
                {
                    cmd.AddStatement(sql, new
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

        public async Task CompleteMessageAsync(TaskActivityWorkItem workItem)
        {
            string sql = $"DELETE {this.settings.MessageTableName} WHERE Id=@Id";
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(sql, new
                {
                    Id = workItem.Id
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task AbandonMessageAsync(TaskActivityWorkItem workItem)
        {
            string sql = $@"
UPDATE {this.settings.MessageTableName}
SET [Status]=N'Abandon',LockedUntilUtc=@LockedUntilUtc
WHERE Id=@Id
";
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(sql, new
                {
                    Id = workItem.Id,
                    LockedUntilUtc = DateTime.MaxValue
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task<DateTime> RenewLock(TaskActivityWorkItem workItem)
        {
            string sql = $@"
UPDATE {this.settings.MessageTableName}
SET LockedUntilUtc=@LockedUntilUtc
WHERE Id=@Id
";
            DateTime dt = DateTime.UtcNow.AddSeconds(this.settings.MessageLockedSeconds);
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(sql, new
                {
                    Id = workItem.Id,
                    LockedUntilUtc = dt
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
            return dt;
        }

        public async Task DeleteMessageManagerAsync()
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                command.AddStatement($"DROP TABLE IF EXISTS {settings.MessageTableName}");

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }
        }

        public async Task InitializeMessageManagerAsync(bool recreate)
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                if (recreate) await DeleteMessageManagerAsync();

                command.AddStatement($@"IF(SCHEMA_ID(@schema) IS NULL)
                    BEGIN
                        EXEC sp_executesql N'CREATE SCHEMA [{settings.SchemaName}]'
                    END", new { schema = settings.SchemaName });

                command.AddStatement($@"
IF(OBJECT_ID(@table) IS NULL)
BEGIN
    CREATE TABLE {settings.MessageTableName} (
        [Id] [nvarchar](50) NOT NULL,
	    [InstanceId] [nvarchar](50) NOT NULL,
	    [ExecutionId] [nvarchar](50) NOT NULL,
	    [SequenceNumber] [bigint] NOT NULL,
	    [OrchestrationInstance] [nvarchar](max) NULL,
	    [LockedUntilUtc] [datetime2](7) NULL,
	    [Status] [nvarchar](50) NULL,
	    [Event] [nvarchar](max) NULL,
	    [ExtensionData] [nvarchar](max) NULL,
     CONSTRAINT [PK_{settings.SchemaName}_{settings.HubName}_{SQLServerSettings.MessageTable}] PRIMARY KEY CLUSTERED
    (
	    [Id] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END", new { table = settings.MessageTableName });

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }
        }
    }
}