namespace maskx.DurableTask.SQLServer.Tracking
{
    public class SqlServerInstanceStoreSettings
    {
        internal const string OrchestrationTable = "_OrchestrationState";
        internal const string WorkitemTable = "_WorkItem";
        internal const string JumpStartTable = "_JumpStart";

        /// <summary>
        /// Gets or sets the hub name for the database instance store.
        /// </summary>
        public string HubName { get; set; }

        /// <summary>
        /// Gets or sets the schema name to which the tables will be added.
        /// </summary>
        public string SchemaName { get; set; } = "dbo";

        /// <summary>
        /// The schema and name of the Orchestration State table.
        /// </summary>
        public string OrchestrationStateTableName => $"[{SchemaName}].[{HubName}{OrchestrationTable}]";

        /// <summary>
        /// The schema and name of the Work Item table.
        /// </summary>
        public string WorkItemTableName => $"[{SchemaName}].[{HubName}{WorkitemTable}]";

        public string JumpStartTableWithSchema => $"[{SchemaName}].[{HubName}{JumpStartTable}]";

        public string ConnectionString { get; set; }
    }
}