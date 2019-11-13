using DurableTask.Core;
using maskx.DurableTask.SQLServer.Tests.scenario;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace maskx.DurableTask.SQLServer.Tests
{
    public class OrchestrationHubTableClientTests : IDisposable
    {
        private TaskHubClient client;
        private TaskHubWorker taskHub;

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
            Assert.Equal("maskx.DurableTask.SQLServer.Tests.scenario.InstanceStoreTestOrchestration", runtimeState.Name);
            Assert.Equal(runtimeState.Version, string.Empty);
            Assert.Equal("\"DONT_THROW\"", runtimeState.Input);
            Assert.Equal("\"Spartacus\"", runtimeState.Output);

            string history = await this.client.GetOrchestrationHistoryAsync(id);
            Assert.True(!string.IsNullOrWhiteSpace(history));
            Assert.Contains("ExecutionStartedEvent", history);
        }
    }
}