using DurableTask.Core.Tracing;
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace maskx.DurableTask.SQLServer.Tests
{
    public class TraceHelperTests
    {
        [Fact]
        public async Task Test()
        {
            var _ = new TraceActivityEventListener(()=> { 
            
            });
            var eft = new EmulatorFunctionalTests();
            await eft.MockSubOrchestrationTest();
        }
        public class TraceActivityEventListener : EventListener
        {
            public Action OnTracing { get; set; }

            public TraceActivityEventListener(Action onTracing)
            {
                this.OnTracing = onTracing;
                this.EnableEvents(DefaultEventSource.Log, EventLevel.Informational);
            }

            protected override void OnEventWritten(EventWrittenEventArgs eventData)
            {
                if(eventData.Level!=EventLevel.Informational)
                {
                    var s = "";
                }    
                var payload = eventData.Payload;
                this.OnTracing();
            }
        }
    }

}
