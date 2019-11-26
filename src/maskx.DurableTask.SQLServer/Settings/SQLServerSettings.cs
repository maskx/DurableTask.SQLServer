using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer.Settings
{
    internal class SQLServerSettings
    {
        internal const string SessionTable = "_Session";
        internal const string SessionMessageTable = "_SessionMessage";
        internal const string MessageTable = "_Message";

        public string HubName { get; set; }

        public string SchemaName { get; set; } = "dbo";

        public string SessionTableName => $"[{SchemaName}].[{HubName}{SessionTable}]";
        public string SessionMessageTableName => $"[{SchemaName}].[{HubName}{SessionMessageTable}]";
        public string MessageTableName => $"[{SchemaName}].[{HubName}{MessageTable}]";

        /// <summary>
        /// Orchestration session locked time
        /// </summary>
        public double SessionLockedSeconds { get; set; } = 300;

        /// <summary>
        /// Message locked time
        /// </summary>
        public double MessageLockedSeconds { get; set; } = 300;

        public string ConnectionString { get; set; }
    }
}