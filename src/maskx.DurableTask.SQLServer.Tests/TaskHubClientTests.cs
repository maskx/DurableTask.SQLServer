using DurableTask.Core;
using System;
using System.Threading.Tasks;
using Xunit;

namespace maskx.DurableTask.SQLServer.Tests
{
    public class TaskHubClientTests
    {
        private TaskHubClient client;
        private TaskHubWorker taskHub;

        [Fact]
        public async Task TestOrchestrationCount()
        {
            this.taskHub = TestHelpers.CreateTaskHub();
            this.client = TestHelpers.CreateTaskHubClient();
            var service = this.taskHub.orchestrationService as SQLServerOrchestrationService;
            Assert.NotNull(service);
            await service.CreateAsync();
            await this.client.CreateOrchestrationInstanceAsync("foo", "1.0", null);
            await this.client.CreateOrchestrationInstanceAsync("foo1", "1.0", null);
            await this.client.CreateOrchestrationInstanceAsync("foo2", "1.0", null);
            Assert.Equal(3, service.GetPendingOrchestrationsCount().Result);
            await service.DeleteAsync();
        }
    }
}