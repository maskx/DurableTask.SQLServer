using DurableTask.Core.Settings;

namespace maskx.DurableTask.SQLServer.Settings
{
    public class SQLServerOrchestrationServiceSettings
    {
        public SQLServerOrchestrationServiceSettings()
        {
            TaskOrchestrationDispatcherSettings = new TaskOrchestrationDispatcherSettings();
            TaskActivityDispatcherSettings = new TaskActivityDispatcherSettings();
            TrackingDispatcherSettings = new TrackingDispatcherSettings();
        }

        /// <summary>
        /// default is false
        /// </summary>
        public bool JumpStartEnabled { get; set; } = false;

        /// <summary>
        /// Orchestration session locked time
        /// </summary>
        public double SessionLockedSeconds { get; set; } = 300;

        /// <summary>
        /// Message locked time
        /// </summary>
        public double MessageLockedSeconds { get; set; } = 300;

        /// <summary>
        /// when no data fetch from SQL Server, the system will sleep the setting time before next try
        /// </summary>
        public int IdleSleepMilliSeconds { get; set; } = 1;

        /// <summary>
        /// the schema name of the SQLServerOrchestratorService tables
        /// </summary>
        public string SchemaName { get; set; } = "dbo";

        /// <summary>
        ///     Settings to configure the Task Orchestration Dispatcher
        /// </summary>
        public TaskOrchestrationDispatcherSettings TaskOrchestrationDispatcherSettings { get; private set; }

        /// <summary>
        ///     Settings to configure the Task Activity Dispatcher
        /// </summary>
        public TaskActivityDispatcherSettings TaskActivityDispatcherSettings { get; private set; }

        /// <summary>
        ///     Settings to configure the Tracking Dispatcher
        /// </summary>
        public TrackingDispatcherSettings TrackingDispatcherSettings { get; private set; }
    }
}