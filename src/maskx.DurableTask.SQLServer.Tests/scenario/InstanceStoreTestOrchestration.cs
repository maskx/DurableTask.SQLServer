using DurableTask.Core;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer.Tests.scenario
{
    public sealed class Activity1 : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string input)
        {
            return "Spartacus";
        }
    }

    public class InstanceStoreTestOrchestration : TaskOrchestration<string, string>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static string Result;

        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string result = await context.ScheduleTask<string>(typeof(Activity1));
            if (string.Equals(input, "THROW", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("BADFOOD");
            }

            if (string.Equals(input, "WAIT", StringComparison.OrdinalIgnoreCase))
            {
                await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(15), null);
            }
            else if (string.Equals(input, "WAIT_THROW", StringComparison.OrdinalIgnoreCase))
            {
                await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(15), null);
                throw new InvalidOperationException("BADFOOD");
            }
            else if (string.Equals(input, "WAIT_DONTTHROW", StringComparison.OrdinalIgnoreCase))
            {
                await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(15), null);
            }

            return result;
        }
    }
}