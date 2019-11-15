using DurableTask.Core;
using DurableTask.Core.Common;
using DurableTask.Core.Settings;
using DurableTask.Core.Tracking;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer.Settings
{
    public class SQLServerOrchestrationServiceSettings
    {
        /// <summary>
        ///     Create a TaskHubWorkerSettings object with default settings
        /// </summary>
        public SQLServerOrchestrationServiceSettings()
        {
            TaskOrchestrationDispatcherSettings = new TaskOrchestrationDispatcherSettings();
            TaskActivityDispatcherSettings = new TaskActivityDispatcherSettings();
            TrackingDispatcherSettings = new TrackingDispatcherSettings();
            MessageCompressionSettings = new CompressionSettings
            {
                Style = CompressionStyle.Never,
                ThresholdInBytes = 0
            };
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
        public int IdleSleepSeconds { get; set; } = 1;

        /// <summary>
        /// the schema name of the SQLServerOrchestratorService tables
        /// </summary>
        public string SchemaName { get; set; } = "dbo";

        /// <summary>
        ///     Maximum number of times the task orchestration dispatcher will try to
        ///     process an orchestration message before giving up
        /// </summary>
        public int MaxTaskOrchestrationDeliveryCount { get; set; }

        /// <summary>
        ///     Maximum number of times the task activity dispatcher will try to
        ///     process an orchestration message before giving up
        /// </summary>
        public int MaxTaskActivityDeliveryCount { get; set; }

        /// <summary>
        ///     Maximum number of times the tracking dispatcher will try to
        ///     process an orchestration message before giving up
        /// </summary>
        public int MaxTrackingDeliveryCount { get; set; }

        /// <summary>
        ///     Maximum queue size, in megabytes, for the service bus queues
        /// </summary>
        public long MaxQueueSizeInMegabytes { get; set; } = 1024L;

        /// <summary>
        /// Gets the message prefetch count
        /// </summary>
        public int PrefetchCount { get; } = 50;

        /// <summary>
        /// Gets the default interval in settings between retries
        /// </summary>
        public int IntervalBetweenRetriesSecs { get; } = 5;

        /// <summary>
        /// Gets the max retries
        /// </summary>
        public int MaxRetries { get; } = 5;

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

        /// <summary>
        ///     Enable compression of messages. Allows exchange of larger parameters and return values with activities at the cost
        ///     of additional CPU.
        ///     Default is false.
        ///     TODO: move this setting into ServiceBusSessionSettings and ServiceBusMessageSettings.
        /// </summary>
        public CompressionSettings MessageCompressionSettings { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of orchestrations that can be processed concurrently on a single node.
        /// The default value is 100.
        /// </summary>
        public int MaxConcurrentTaskOrchestrationWorkItems { get; set; } = 100;

        /// <summary>
        /// Gets or sets the maximum number of work items that can be processed concurrently on a single node.
        /// The default value is 100.
        /// </summary>
        public int MaxConcurrentTaskActivityWorkItems { get; set; } = 100;
    }
}