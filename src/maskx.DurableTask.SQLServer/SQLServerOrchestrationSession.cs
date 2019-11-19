using DurableTask.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer
{
    internal class SQLServerOrchestrationSession
    {
        public string Id { get { return InstanceId; } }
        public string InstanceId { get; set; }
        public string ExecutionId { get; set; }
        public OrchestrationRuntimeState SessionState { get; set; }
        public List<TaskMessage> Messages { get; set; }

        /// <summary>
        /// for locked message Id
        /// </summary>
        public HashSet<string> LockTable { get; set; }

        public DateTime LockedUntilUtc { get; set; }

        public SQLServerOrchestrationSession()
        {
            this.SessionState = null;
            this.Messages = new List<TaskMessage>();
            this.LockTable = new HashSet<string>();
        }
    }
}