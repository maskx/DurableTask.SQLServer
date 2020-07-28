using DurableTask.Core;
using System;
using System.Threading.Tasks;
using Xunit;

namespace maskx.DurableTask.SQLServer.Tests
{
    public class OrchestrationHubTableClientTests : IDisposable
    {
        private readonly TaskHubClient client;
        private readonly TaskHubWorker taskHub;

        public OrchestrationHubTableClientTests()
        {
            this.client = TestHelpers.CreateTaskHubClient();

            this.taskHub = TestHelpers.CreateTaskHub();

            this.taskHub.orchestrationService.CreateAsync(true).Wait();
        }

        public void Dispose()
        {
            this.taskHub.StopAsync(true).Wait();
            this.taskHub.orchestrationService.DeleteAsync(true).Wait();
        }

        [Fact]
        public async Task BasicInstanceStoreTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(InstanceStoreTestOrchestration),
                "DONT_THROW");

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            OrchestrationState runtimeState = await this.client.GetOrchestrationStateAsync(id);
            Assert.Equal(OrchestrationStatus.Completed, runtimeState.OrchestrationStatus);
            Assert.Equal(runtimeState.OrchestrationInstance.InstanceId, id.InstanceId);
            Assert.Equal(runtimeState.OrchestrationInstance.ExecutionId, id.ExecutionId);
            Assert.Equal("maskx.DurableTask.SQLServer.Tests.OrchestrationHubTableClientTests+InstanceStoreTestOrchestration", runtimeState.Name);
            Assert.Equal(runtimeState.Version, string.Empty);
            Assert.Equal("\"DONT_THROW\"", runtimeState.Input);
            Assert.Equal("\"Spartacus\"", runtimeState.Output);

            string history = await this.client.GetOrchestrationHistoryAsync(id);
            Assert.True(!string.IsNullOrWhiteSpace(history));
            Assert.Contains("ExecutionStartedEvent", history);
        }

        [Fact]
        public async Task MultipleInstanceStoreTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .StartAsync();

            OrchestrationInstance id1 = await this.client.CreateOrchestrationInstanceAsync(typeof(InstanceStoreTestOrchestration),
                "WAIT_THROW");
            OrchestrationInstance id2 = await this.client.CreateOrchestrationInstanceAsync(typeof(InstanceStoreTestOrchestration),
                "WAIT_DONTTHROW");

            await TestHelpers.WaitForInstanceAsync(this.client, id1, 60, false);
            await TestHelpers.WaitForInstanceAsync(this.client, id2, 60, false);

            OrchestrationState runtimeState1 = await this.client.GetOrchestrationStateAsync(id1);
            OrchestrationState runtimeState2 = await this.client.GetOrchestrationStateAsync(id2);
            Assert.Equal(OrchestrationStatus.Pending, runtimeState1.OrchestrationStatus);
            Assert.Equal(OrchestrationStatus.Pending, runtimeState2.OrchestrationStatus);

            await TestHelpers.WaitForInstanceAsync(this.client, id1, 60);
            await TestHelpers.WaitForInstanceAsync(this.client, id2, 60);

            runtimeState1 = await this.client.GetOrchestrationStateAsync(id1);
            runtimeState2 = await this.client.GetOrchestrationStateAsync(id2);
            Assert.Equal(OrchestrationStatus.Failed, runtimeState1.OrchestrationStatus);
            Assert.Equal(OrchestrationStatus.Completed, runtimeState2.OrchestrationStatus);
        }

        [Fact]
        public async Task TerminateInstanceStoreTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(InstanceStoreTestOrchestration),
                "WAIT");

            await TestHelpers.WaitForInstanceAsync(this.client, id, 60, false);
            OrchestrationState runtimeState = await this.client.GetOrchestrationStateAsync(id);
            Assert.Equal(OrchestrationStatus.Pending, runtimeState.OrchestrationStatus);

            await this.client.TerminateInstanceAsync(id);
            await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            runtimeState = await this.client.GetOrchestrationStateAsync(id);
            Assert.Equal(OrchestrationStatus.Terminated, runtimeState.OrchestrationStatus);
        }

        [Fact]
        public async Task IntermediateStateInstanceStoreTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(InstanceStoreTestOrchestration),
                "WAIT");

            await TestHelpers.WaitForInstanceAsync(this.client, id, 60, false);

            OrchestrationState runtimeState = await this.client.GetOrchestrationStateAsync(id);
            Assert.NotNull(runtimeState);
            Assert.Equal(OrchestrationStatus.Pending, runtimeState.OrchestrationStatus);
            Assert.Equal(id.InstanceId, runtimeState.OrchestrationInstance.InstanceId);
            Assert.Equal(id.ExecutionId, runtimeState.OrchestrationInstance.ExecutionId);
            Assert.Equal("maskx.DurableTask.SQLServer.Tests.OrchestrationHubTableClientTests+InstanceStoreTestOrchestration", runtimeState.Name);
            Assert.Equal(runtimeState.Version, string.Empty);
            Assert.Equal("\"WAIT\"", runtimeState.Input);
            Assert.Null(runtimeState.Output);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            runtimeState = await this.client.GetOrchestrationStateAsync(id);
            Assert.Equal(OrchestrationStatus.Completed, runtimeState.OrchestrationStatus);
        }

        [Fact]
        public async Task FailingInstanceStoreTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(InstanceStoreTestOrchestration),
                "THROW");

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            OrchestrationState status = await this.client.GetOrchestrationStateAsync(id);
            Assert.True(status.OrchestrationStatus == OrchestrationStatus.Failed);
        }

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
}