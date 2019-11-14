using DurableTask.Core;
using DurableTask.Core.Serializing;
using maskx.DurableTask.SQLServer.Settings;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using maskx.DurableTask.SQLServer.Tracking;
using DurableTask.Core.History;
using Newtonsoft.Json;
using System.Runtime.Serialization;

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
                        LockedUntilUtc = reader.GetDateTime(4),
                        TaskMessage = new TaskMessage
                        {
                            SequenceNumber = reader.GetInt64(0),
                            OrchestrationInstance = reader.IsDBNull(1) ? null : dataConverter.Deserialize<OrchestrationInstance>(reader.GetString(1)),
                            Event = reader.IsDBNull(2) ? null : JsonConvert.DeserializeObject<HistoryEvent>(reader.GetString(2), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto }),
                            ExtensionData = reader.IsDBNull(3) ? null : dataConverter.Deserialize<ExtensionDataObject>(reader.GetString(3))
                        }
                    };
                    t.Id = $"{t.TaskMessage.OrchestrationInstance.InstanceId}_{t.TaskMessage.OrchestrationInstance.ExecutionId}_{t.TaskMessage.SequenceNumber}";
                }
            }
            return t;
        }

        public async Task<TaskActivityWorkItem> ReceiveMessageAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            string sqlGetPending = $@"
update top(1) {settings.MessageTableName}
set [Status]='locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData,INSERTED.LockedUntilUtc
where [Status]='Pending'
";
            string sqlGetTimeout = $@"
update top(1) {settings.MessageTableName}
set [Status]='locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData
where [Status]='Locked'
and LockedUntilUtc<getutcdate()
";
            TaskActivityWorkItem t = await GetMessageAsync(sqlGetPending, receiveTimeout, cancellationToken);
            if (t == null)
                t = await GetMessageAsync(sqlGetTimeout, receiveTimeout, cancellationToken);
            return t;
        }

        public async Task SendMessageAsync(TaskMessage message)
        {
            string sql = $@"
INSERT INTO {this.settings.MessageTableName}
([InstanceId],[ExecutionId],[SequenceNumber],[OrchestrationInstance], [LockedUntilUtc],[Status],[Event],[ExtensionData])
VALUES
( @InstanceId,@ExecutionId,@SequenceNumber,@OrchestrationInstance,@LockedUntilUtc,@Status,@Event,@ExtensionData);
";
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(sql, new
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

        public async Task CompleteMessageAsync(TaskMessage message)
        {
            string sql = $@"
DELETE {this.settings.MessageTableName}
WHERE InstanceId=@InstanceId
AND ExecutionId=@ExecutionId
AND SequenceNumber=@SequenceNumber
";
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(sql, new
                {
                    ExecutionId = message.OrchestrationInstance.ExecutionId,
                    InstanceId = message.OrchestrationInstance.InstanceId,
                    SequenceNumber = message.SequenceNumber,
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task AbandonMessageAsync(TaskMessage message)
        {
            string sql = $@"
UPDATE {this.settings.MessageTableName}
SET [Status]=N'Abandon',LockedUntilUtc=@LockedUntilUtc
WHERE InstanceId=@InstanceId
AND ExecutionId=@ExecutionId
AND SequenceNumber=@SequenceNumber
";
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(sql, new
                {
                    ExecutionId = message.OrchestrationInstance.ExecutionId,
                    InstanceId = message.OrchestrationInstance.InstanceId,
                    SequenceNumber = message.SequenceNumber,
                    LockedUntilUtc = DateTime.MaxValue
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task<DateTime> RenewLock(TaskMessage message)
        {
            string sql = $@"
UPDATE {this.settings.MessageTableName}
SET LockedUntilUtc=@LockedUntilUtc
WHERE InstanceId=@InstanceId
AND ExecutionId=@ExecutionId
AND SequenceNumber=@SequenceNumber
";
            DateTime dt = DateTime.UtcNow.AddSeconds(this.settings.MessageLockedSeconds);
            using (var con = await this.settings.GetDatabaseConnection())
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(sql, new
                {
                    ExecutionId = message.OrchestrationInstance.ExecutionId,
                    InstanceId = message.OrchestrationInstance.InstanceId,
                    SequenceNumber = message.SequenceNumber,
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
	    [InstanceId] ASC,
	    [ExecutionId] ASC,
	    [SequenceNumber] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END", new { table = settings.MessageTableName });

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }
        }
    }
}