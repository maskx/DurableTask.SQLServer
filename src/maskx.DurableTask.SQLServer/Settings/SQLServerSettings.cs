namespace maskx.DurableTask.SQLServer.Settings
{
    internal class SQLServerSettings
    {
        internal const string CreateSessionSP = "_CreateSession";
        internal const string SessionTable = "_Session";
        internal const string SessionMessageTable = "_SessionMessage";
        internal const string MessageTable = "_Message";
        internal const string LockNextOrchestrationCommand = "_LockNextOrchestration";

        public string HubName { get; set; }

        public string SchemaName { get; set; } = "dbo";

        public string SessionTableName => $"[{SchemaName}].[{HubName}{SessionTable}]";
        public string SessionMessageTableName => $"[{SchemaName}].[{HubName}{SessionMessageTable}]";
        public string MessageTableName => $"[{SchemaName}].[{HubName}{MessageTable}]";
        public string LockNextOrchestrationCommandName => $"[{SchemaName}].[{HubName}{LockNextOrchestrationCommand}]";
        public string CreateSessionSPName => $"[{SchemaName}].[{HubName}{CreateSessionSP}]";

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