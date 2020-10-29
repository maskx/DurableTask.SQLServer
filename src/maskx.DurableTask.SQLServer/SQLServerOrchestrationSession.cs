using DurableTask.Core;
using System;
using System.Collections.Generic;

namespace maskx.DurableTask.SQLServer
{
    internal class SQLServerOrchestrationSession
    {
        public string Id { get { return InstanceId; } }
        public string InstanceId { get; set; }
        public string ExecutionId { get; set; }
        public OrchestrationRuntimeState SessionState { get; set; }
        public List<TaskMessage> Messages { get; set; }= new List<TaskMessage>();

        /// <summary>
        /// for locked message Id
        /// </summary>
        public HashSet<string> LockTable { get; set; }= new HashSet<string>();

        public DateTime LockedUntilUtc { get; set; }

    }
}