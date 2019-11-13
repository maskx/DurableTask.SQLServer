using DurableTask.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer
{
    internal class TaskSession
    {
        public string Id { get; set; }
        public string SessionState { get; set; }
        public List<TaskMessage> Messages { get; set; }
        public HashSet<TaskMessage> LockTable { get; set; }
        public DateTime LockedUntilUtc { get; set; }
        public string ConnectionString { get; private set; }

        public TaskSession()
        {
            this.SessionState = string.Empty;
            this.Messages = new List<TaskMessage>();
            this.LockTable = new HashSet<TaskMessage>();
        }
    }
}