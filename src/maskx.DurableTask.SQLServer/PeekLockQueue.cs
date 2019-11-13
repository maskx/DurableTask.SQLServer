using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;

namespace maskx.DurableTask.SQLServer
{
    internal class PeekLockQueue
    {
        private readonly List<TaskMessage> messages;
        private readonly HashSet<TaskMessage> lockTable;

        private readonly object thisLock = new object();
        private readonly string connectionString;

        public PeekLockQueue(string connectionString)
        {
            this.messages = new List<TaskMessage>();
            this.lockTable = new HashSet<TaskMessage>();
            this.connectionString = connectionString;
        }

        public async Task<TaskMessage> ReceiveMessageAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !cancellationToken.IsCancellationRequested)
            {
                lock (this.thisLock)
                {
                    foreach (TaskMessage tm in this.messages)
                    {
                        if (!this.lockTable.Contains(tm))
                        {
                            this.lockTable.Add(tm);
                            return tm;
                        }
                    }
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                throw new TaskCanceledException();
            }

            return null;
        }

        public void SendMessageAsync(TaskMessage message)
        {
            lock (this.thisLock)
            {
                this.messages.Add(message);
            }
        }

        public void CompleteMessageAsync(TaskMessage message)
        {
            lock (this.thisLock)
            {
                if (!this.lockTable.Contains(message))
                {
                    throw new InvalidOperationException("Message Lock Lost");
                }

                this.lockTable.Remove(message);
                this.messages.Remove(message);
            }
        }

        public void AbandonMessageAsync(TaskMessage message)
        {
            lock (this.thisLock)
            {
                if (!this.lockTable.Contains(message))
                {
                    return;
                }

                this.lockTable.Remove(message);
            }
        }
    }
}