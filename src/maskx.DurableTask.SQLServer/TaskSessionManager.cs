using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.Core.Serializing;
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
    internal class TaskSessionManager
    {
        private readonly string connectionString;
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

        private readonly string AcceptSessionAsyncSQL = @"
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
and [Status]='Pending'
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

        public TaskSessionManager(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public async Task CreateSession(TaskMessage message)
        {
            using (var con = new SqlConnection(this.connectionString))
            {
                var cmd = con.CreateCommand();
                cmd.AddStatement(this.CreateSessionSQL, new
                {
                    ExecutionId = message.OrchestrationInstance.ExecutionId,
                    InstanceId = message.OrchestrationInstance.InstanceId,
                    LockedUntilUtc = DBNull.Value,
                    Status = "Pending",
                    SequenceNumber = message.SequenceNumber,
                    OrchestrationInstance = SQLSerializer.SerializeToJson(message.OrchestrationInstance),
                    Event = SQLSerializer.SerializeToJson(message.Event),
                    ExtensionData = SQLSerializer.SerializeToJson(message.ExtensionData)
                });
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task SendMessageBatch(params TaskMessage[] messages)
        {
            using (var con = new SqlConnection(this.connectionString))
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
                        OrchestrationInstance = SQLSerializer.SerializeToJson(msg.OrchestrationInstance),
                        Event = SQLSerializer.SerializeToJson(msg.Event),
                        ExtensionData = SQLSerializer.SerializeToJson(msg.ExtensionData)
                    });
                }
                await con.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task<TaskSession> AcceptSessionAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            TaskSession t = null;
            using (DbAccess db = new DbAccess(this.connectionString))
            {
                await db.ExecuteReaderAsync(this.AcceptSessionAsyncSQL, (reader, index) =>
                {
                    if (index == 0)
                    {
                        t = new TaskSession()
                        {
                            Id = reader["InstanceId"].ToString(),
                            Messages = new List<TaskMessage>(),
                            SessionState = reader.IsDBNull(3) ? string.Empty : reader["SessionState"].ToString()
                        };
                    }
                    else if (index == 1)
                    {
                        t.Messages.Add(new TaskMessage()
                        {
                            SequenceNumber = reader.GetInt64(0),
                            OrchestrationInstance = reader.IsDBNull(1) ? null : SQLSerializer.DeserializeJson<OrchestrationInstance>(reader.GetString(1)),
                            Event = reader.IsDBNull(2) ? null : JsonConvert.DeserializeObject<HistoryEvent>(reader.GetString(2), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto }),
                            ExtensionData = reader.IsDBNull(3) ? null : SQLSerializer.DeserializeJson<ExtensionDataObject>(reader.GetString(3))
                        }); ;
                    }
                }, (pars) =>
                {
                    pars.AddWithValue("LockedUntilUtc", DateTime.UtcNow.AddMinutes(5));
                });
            }

            if (t == null)

                await Task.Delay(2000);

            return t;
        }

        public async Task AbandonSession(string id)
        {
            return;
        }

        public async Task CompleteSession(string id, string newState)
        {
            using (DbAccess db = new DbAccess(this.connectionString))
            {
                if (string.IsNullOrEmpty(newState))
                {
                    await db.ExecuteNonQueryAsync(this.RemoveSessionSql, (pars) =>
                    {
                        pars.AddWithValue("InstanceId", id);
                    });
                }
                else
                {
                    await db.ExecuteNonQueryAsync(this.CompleteSessionStateSql, (pars) =>
                    {
                        pars.AddWithValue("InstanceId", id);
                        pars.AddWithValue("SessionState", newState);
                    });
                }
            }
        }

        public async Task SetStateAsync(string id, string sessinoState)
        {
            using (DbAccess db = new DbAccess(this.connectionString))
            {
                await db.ExecuteNonQueryAsync(this.SetSessionStateSql, (pars) =>
                {
                    pars.AddWithValue("InstanceId", id);
                    pars.AddWithValue("SessionState", sessinoState);
                });
            }
        }
    }
}