using DurableTask.Core;
using DurableTask.Core.History;
using maskx.DurableTask.SQLServer.Database;
using maskx.DurableTask.SQLServer.Settings;
using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer.Utils
{
    public static class SqlUtils
    {
        private static readonly SqlMetaData[] TaskEventSchema = new SqlMetaData[]
       {
            new SqlMetaData("SequenceNumber", SqlDbType.BigInt),
            new SqlMetaData("VisibleTime", SqlDbType.DateTime),
            new SqlMetaData("InstanceID", SqlDbType.NVarChar, 100),
            new SqlMetaData("ExecutionID", SqlDbType.NVarChar, 100),
            new SqlMetaData("EventType", SqlDbType.VarChar, 30),
            new SqlMetaData("Name", SqlDbType.NVarChar, 256),
            new SqlMetaData("RuntimeStatus", SqlDbType.VarChar, 30),
            new SqlMetaData("TaskID", SqlDbType.Int),
            new SqlMetaData("Reason", SqlDbType.NVarChar, -1 /* max */),
            new SqlMetaData("PayloadText", SqlDbType.NVarChar, -1 /* max */),
            new SqlMetaData("CustomStatusText", SqlDbType.NVarChar, -1 /* max */),
            new SqlMetaData("IsPlayed", SqlDbType.Bit),
            new SqlMetaData("LockedBy", SqlDbType.NVarChar, 100),
            new SqlMetaData("LockExpiration", SqlDbType.DateTime2),
            new SqlMetaData("CompletedTime", SqlDbType.DateTime2),
       };

        private static class TaskEventFields
        {
            // NOTE: These numbers must be kept in sync with the TaskEvent SQL type
            // NOTE: Also, these must be kept in sync with the static TaskEventSchema above
            public const int SequenceNumber = 0;

            public const int VisibleTime = 1;
            public const int InstanceID = 2;
            public const int ExecutionID = 3;
            public const int EventType = 4;
            public const int Name = 5;
            public const int RuntimeStatus = 6;
            public const int TaskID = 7;
            public const int Reason = 8;
            public const int PayloadText = 9;
            public const int CustomStatusText = 10;
            public const int IsPlayed = 11;
            public const int LockedBy = 12;
            public const int LockExpiration = 13;
            public const int CompletedTime = 14;
        }

        private static SqlDateTime GetVisibleTime(HistoryEvent historyEvent)
        {
            return historyEvent.EventType switch
            {
                EventType.TimerCreated => ((TimerCreatedEvent)historyEvent).FireAt,
                EventType.TimerFired => ((TimerFiredEvent)historyEvent).FireAt,
                _ => SqlDateTime.Null,
            };
        }

        private static SqlString GetRuntimeStatus(HistoryEvent historyEvent)
        {
            return DTUtils.GetRuntimeStatus(historyEvent)?.ToString() ?? SqlString.Null;
        }

        private static SqlString GetName(HistoryEvent historyEvent)
        {
            return DTUtils.GetName(historyEvent) ?? SqlString.Null;
        }

        private static string? GetName(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("Name");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        private static SqlInt32 GetTaskId(HistoryEvent historyEvent)
        {
            int taskEventId = DTUtils.GetTaskEventId(historyEvent);
            return taskEventId >= 0 ? new SqlInt32(taskEventId) : SqlInt32.Null;
        }

        public static int GetTaskId(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("TaskID");
            return reader.IsDBNull(ordinal) ? -1 : reader.GetInt32(ordinal);
        }

        public static long GetSequenceNumber(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("SequenceNumber");
            return reader.IsDBNull(ordinal) ? -1 : reader.GetInt64(ordinal);
        }

        private static SqlString GetReason(HistoryEvent historyEvent)
        {
            return historyEvent.EventType switch
            {
                EventType.SubOrchestrationInstanceFailed => ((SubOrchestrationInstanceFailedEvent)historyEvent).Reason,
                EventType.TaskFailed => ((TaskFailedEvent)historyEvent).Reason,
                _ => SqlString.Null,
            };
        }

        private static string? GetReason(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("Reason");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        private static SqlString GetPayloadText(HistoryEvent e)
        {
            DTUtils.TryGetPayloadText(e, out string? payloadText);
            return payloadText ?? SqlString.Null;
        }

        private static string? GetPayloadText(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("PayloadText");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        private static string GetInstanceId(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("InstanceID");
            return reader.GetString(ordinal);
        }

        private static string? GetExecutionId(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("ExecutionID");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        public static SqlDataRecord Populate(
            this TaskMessage taskMessage,
            SqlDataRecord record,
            bool onlyMessageId = false)
        {
            record.SetSqlInt64(TaskEventFields.SequenceNumber, taskMessage.SequenceNumber);
            record.SetSqlString(TaskEventFields.InstanceID, taskMessage.OrchestrationInstance.InstanceId);

            if (!onlyMessageId)
            {
                record.SetSqlDateTime(TaskEventFields.VisibleTime, GetVisibleTime(taskMessage.Event));
                record.SetSqlString(TaskEventFields.ExecutionID, taskMessage.OrchestrationInstance.ExecutionId);
                record.SetSqlString(TaskEventFields.EventType, taskMessage.Event.EventType.ToString());
                record.SetSqlString(TaskEventFields.Name, GetName(taskMessage.Event));
                record.SetSqlString(TaskEventFields.RuntimeStatus, GetRuntimeStatus(taskMessage.Event));
                record.SetSqlInt32(TaskEventFields.TaskID, GetTaskId(taskMessage.Event));
                record.SetSqlString(TaskEventFields.Reason, GetReason(taskMessage.Event));
                record.SetSqlString(TaskEventFields.PayloadText, GetPayloadText(taskMessage.Event));
            }

            return record;
        }

        public static IEnumerable<SqlDataRecord> ToTableValueParameter(
           this TaskMessage taskMessage,
           bool onlyMessageId = false)
        {
            return new[] { taskMessage.Populate(new SqlDataRecord(TaskEventSchema), onlyMessageId) };
        }

        internal static async Task<string> GetScriptTextAsync(string scriptName, string schemaName, string hubName, Assembly? assembly = null)
        {
            if (assembly == null)
            {
                assembly = typeof(SQLServerOrchestrationService).Assembly;
            }

            string assemblyName = assembly.GetName().Name;
            if (!scriptName.StartsWith(assemblyName))
            {
                scriptName = $"{assembly.GetName().Name}.Scripts.{scriptName}.sql";
            }

            using Stream resourceStream = assembly.GetManifestResourceStream(scriptName);
            if (resourceStream == null)
            {
                throw new ArgumentException($"Could not find assembly resource named '{scriptName}'.");
            }

            using var reader = new StreamReader(resourceStream);
            var str = await reader.ReadToEndAsync();
            return string.Format(str, schemaName, hubName);
        }

        internal static async Task<List<string>> GetStoredProcedureScriptTextAsync(string schemaName, string hubName, Assembly? assembly = null)
        {
            List<string> text = new List<string>();
            if (assembly == null)
            {
                assembly = typeof(SQLServerOrchestrationService).Assembly;
            }
            IEnumerable<string> createSchemaFiles = assembly.GetManifestResourceNames()
                .Where(name => name.Contains(".sproces.") && name.EndsWith(".sql"));
            foreach (string name in createSchemaFiles)
            {
                using Stream resourceStream = assembly.GetManifestResourceStream(name);
                if (resourceStream == null)
                {
                    throw new ArgumentException($"Could not find assembly resource named '{name}'.");
                }

                using var reader = new StreamReader(resourceStream);
                var str = await reader.ReadToEndAsync();
                text.Add(string.Format(str, schemaName, hubName));
            }
            return text;
        }

        internal static async Task ClearDatabaseAsync(SQLServerSettings settings)
        {
            using (var db = new SQLServerAccess(settings.ConnectionString))
            {
                var str = await SqlUtils.GetScriptTextAsync("drop-schema", settings.SchemaName, settings.HubName);
                await db.ExecuteNonQueryAsync(str, null, CommandType.Text);
            }
        }

        internal static async Task InitializeDatabase(bool recreate, SQLServerSettings settings)
        {
            if (recreate) await ClearDatabaseAsync(settings);
            var str = await SqlUtils.GetScriptTextAsync("create-schema", settings.SchemaName, settings.HubName);
            var sp = await SqlUtils.GetStoredProcedureScriptTextAsync(settings.SchemaName, settings.HubName);
            using (var db = new SQLServerAccess(settings.ConnectionString))
            {
                await db.ExecuteNonQueryAsync(str, null, CommandType.Text);
                foreach (var item in sp)
                {
                    await db.ExecuteNonQueryAsync(item, null, CommandType.Text);
                }
            }
        }
    }
}