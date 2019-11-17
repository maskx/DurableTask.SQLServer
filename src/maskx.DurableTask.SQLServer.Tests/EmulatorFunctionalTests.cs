using DurableTask.Core;
using DurableTask.Core.Exceptions;
using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace maskx.DurableTask.SQLServer.Tests
{
    public class EmulatorFunctionalTests
    {
        [Fact]
        public async Task MockOrchestrationTest()
        {
            var worker = TestHelpers.CreateTaskHub();
            await worker.orchestrationService.CreateAsync(true);
            await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = TestHelpers.CreateTaskHubClient();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(30), new CancellationToken());
            Assert.Equal(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.Equal("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result);

            await worker.StopAsync(true);
        }

        [Fact]
        public async Task MockRecreateOrchestrationTest()
        {
            var worker = TestHelpers.CreateTaskHub();
            await worker.orchestrationService.CreateAsync(true);
            await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = TestHelpers.CreateTaskHubClient();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(30), new CancellationToken());
            Assert.Equal(OrchestrationStatus.Completed, result.OrchestrationStatus);

            await Assert.ThrowsAsync<OrchestrationAlreadyExistsException>(() => client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null));

            await Assert.ThrowsAsync<OrchestrationAlreadyExistsException>(() => client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null, new[] { OrchestrationStatus.Completed }));

            SimplestGreetingsOrchestration.Result = String.Empty;

            OrchestrationInstance id2 = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null, new OrchestrationStatus[] { });
            result = await client.WaitForOrchestrationAsync(id2, TimeSpan.FromSeconds(30), new CancellationToken());
            Assert.Equal(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.Equal("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result);

            await worker.StopAsync(true);
        }

        [Fact]
        public async Task MockTimerTest()
        {
            var worker = TestHelpers.CreateTaskHub();
            await worker.orchestrationService.CreateAsync(true);
            await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = TestHelpers.CreateTaskHubClient();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), "6");

            Stopwatch sw = Stopwatch.StartNew();
            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(40), new CancellationToken());
            Assert.Equal(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.True(sw.Elapsed.Seconds > 6);

            Assert.Equal("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result);

            await worker.StopAsync(true);
        }

        [Fact]
        public async Task MockRepeatTimerTest()
        {
            var worker = TestHelpers.CreateTaskHub();
            await worker.orchestrationService.CreateAsync(true);
            await worker.AddTaskOrchestrations(typeof(GreetingsRepeatWaitOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = TestHelpers.CreateTaskHubClient();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(GreetingsRepeatWaitOrchestration), "1");

            Stopwatch sw = Stopwatch.StartNew();
            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(40), new CancellationToken());
            Assert.Equal(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.True(sw.Elapsed.Seconds > 3);

            Assert.Equal("Greeting send to Gabbar", GreetingsRepeatWaitOrchestration.Result);

            await worker.StopAsync(true);
        }

        [Fact]
        public async Task MockGenerationTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            var worker = TestHelpers.CreateTaskHub();
            var client = TestHelpers.CreateTaskHubClient();
            await worker.orchestrationService.CreateAsync(true);

            await worker.AddTaskOrchestrations(typeof(GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(GenerationBasicOrchestration), 4);

            // strip out the eid so we wait for the latest one always
            var masterId = new OrchestrationInstance { InstanceId = id.InstanceId };

            OrchestrationState result1 = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(20), CancellationToken.None);

            OrchestrationState result2 = await client.WaitForOrchestrationAsync(masterId, TimeSpan.FromSeconds(20), CancellationToken.None);

            Assert.Equal(OrchestrationStatus.ContinuedAsNew, result1.OrchestrationStatus);
            Assert.Equal(OrchestrationStatus.Completed, result2.OrchestrationStatus);

            Assert.Equal(4, GenerationBasicOrchestration.Result);
        }

        [Fact]
        public async Task MockSubOrchestrationTest()
        {
            var worker = TestHelpers.CreateTaskHub();
            var client = TestHelpers.CreateTaskHubClient();
            await worker.orchestrationService.CreateAsync(true);
            await worker.AddTaskOrchestrations(typeof(ParentWorkflow), typeof(ChildWorkflow))
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(ParentWorkflow), true);

            OrchestrationState result = await client.WaitForOrchestrationAsync(id,
                TimeSpan.FromSeconds(40), CancellationToken.None);

            Assert.Equal(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.Equal(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result);

            ParentWorkflow.Result = string.Empty;

            id = await client.CreateOrchestrationInstanceAsync(typeof(ParentWorkflow), false);

            result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(40), CancellationToken.None);

            Assert.Equal(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.Equal(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result);

            await worker.StopAsync(true);
        }

        [Fact]
        public async Task MockRaiseEventTest()
        {
            var worker = TestHelpers.CreateTaskHub();
            var client = TestHelpers.CreateTaskHubClient();
            await worker.orchestrationService.CreateAsync(true);
            await worker.AddTaskOrchestrations(typeof(GenerationSignalOrchestration))
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(
                typeof(GenerationSignalOrchestration), 5);

            var signalId = new OrchestrationInstance { InstanceId = id.InstanceId };

            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "1");
            GenerationSignalOrchestration.Signal.Set();

            await Task.Delay(2 * 500);
            GenerationSignalOrchestration.Signal.Reset();
            await client.RaiseEventAsync(signalId, "Count", "2");
            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "3"); // will be received by next generation
            GenerationSignalOrchestration.Signal.Set();

            await Task.Delay(2 * 500);
            GenerationSignalOrchestration.Signal.Reset();
            await client.RaiseEventAsync(signalId, "Count", "4");
            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "5"); // will be received by next generation
            await client.RaiseEventAsync(signalId, "Count", "6"); // lost
            await client.RaiseEventAsync(signalId, "Count", "7"); // lost
            GenerationSignalOrchestration.Signal.Set();

            OrchestrationState result = await client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = id.InstanceId },
                TimeSpan.FromSeconds(40), CancellationToken.None);

            Assert.Equal(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.Equal("5", GenerationSignalOrchestration.Result);
        }
    }
}