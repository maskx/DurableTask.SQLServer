using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.Core.Serializing;
using maskx.DurableTask.SQLServer.Settings;
using maskx.DurableTask.SQLServer.SQL;
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

        public async Task<TaskActivityWorkItem> ReceiveMessageAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            TaskActivityWorkItem t = null;
            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement(string.Format(ReceiveMessageCommand, settings.MessageTableName),
                    new
                    {
                        LockedUntilUtc = DateTime.UtcNow.AddSeconds(this.settings.MessageLockedSeconds)
                    });
                await db.ExecuteReaderAsync((reader, index) =>
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
                 });
            }
            return t;
        }

        public async Task SendMessageAsync(params TaskMessage[] message)
        {
            string sql = $@"
INSERT INTO {this.settings.MessageTableName}
([Id],[InstanceId],[ExecutionId],[SequenceNumber],[OrchestrationInstance], [LockedUntilUtc],[Status],[Event],[ExtensionData])
VALUES
(newid(),@InstanceId,@ExecutionId,@SequenceNumber,@OrchestrationInstance,@LockedUntilUtc,@Status,@Event,@ExtensionData);
";
            using (var db = new DbAccess(settings.ConnectionString))
            {
                foreach (var msg in message)
                {
                    db.AddStatement(sql, new
                    {
                        msg.OrchestrationInstance.ExecutionId,
                        msg.OrchestrationInstance.InstanceId,
                        msg.SequenceNumber,
                        LockedUntilUtc = DBNull.Value,
                        Status = "Pending",
                        OrchestrationInstance = dataConverter.Serialize(msg.OrchestrationInstance),
                        Event = dataConverter.Serialize(msg.Event),
                        ExtensionData = dataConverter.Serialize(msg.ExtensionData)
                    });
                }
                await db.ExecuteNonQueryAsync();
            }
        }

        public async Task CompleteMessageAsync(TaskActivityWorkItem workItem)
        {
            string sql = $"DELETE {this.settings.MessageTableName} WHERE Id=@Id";
            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement(sql, new
                {
                    workItem.Id
                });
                await db.ExecuteNonQueryAsync();
            }
        }

        public async Task AbandonMessageAsync(TaskActivityWorkItem workItem)
        {
            string sql = $@"
UPDATE {this.settings.MessageTableName}
SET [Status]=N'Abandon',LockedUntilUtc=@LockedUntilUtc
WHERE Id=@Id
";
            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement(sql, new
                {
                    workItem.Id,
                    LockedUntilUtc = DBNull.Value
                });
                await db.ExecuteNonQueryAsync();
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
            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement(sql, new
                {
                    workItem.Id,
                    LockedUntilUtc = dt
                });
                await db.ExecuteNonQueryAsync();
            }
            return dt;
        }

        public async Task DeleteMessageManagerAsync()
        {
            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement($"DROP TABLE IF EXISTS {settings.MessageTableName}");
                await db.ExecuteNonQueryAsync();
            }
        }

        public async Task InitializeMessageManagerAsync(bool recreate)
        {
            if (recreate) await DeleteMessageManagerAsync();
            using (var db = new DbAccess(settings.ConnectionString))
            {
                db.AddStatement($@"IF(SCHEMA_ID(@schema) IS NULL)
                    BEGIN
                        EXEC sp_executesql N'CREATE SCHEMA [{settings.SchemaName}]'
                    END", new { schema = settings.SchemaName });

                db.AddStatement($@"
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
                await db.ExecuteNonQueryAsync();
            }
        }

        //{0} Message TableName
        private const string ReceiveMessageCommand = @"
declare @Id nvarchar(50)

update top(1) {0}
set @Id=Id,[Status]='locked',LockedUntilUtc=@LockedUntilUtc
output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData,INSERTED.LockedUntilUtc,INSERTED.Id
where [Status]='Locked'
and LockedUntilUtc<getutcdate()

if @Id is null
begin
    update top(1) {0}
    set [Status]='locked',LockedUntilUtc=@LockedUntilUtc
    output INSERTED.SequenceNumber,INSERTED.OrchestrationInstance,INSERTED.[Event],INSERTED.ExtensionData,INSERTED.LockedUntilUtc,INSERTED.Id
    where [Status]='Pending'
end
";
    }
}