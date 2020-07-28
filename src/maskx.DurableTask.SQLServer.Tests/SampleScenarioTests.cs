using DurableTask.Core;
using DurableTask.Core.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace maskx.DurableTask.SQLServer.Tests
{
    public class SampleScenarioTests : IDisposable
    {
        private readonly TaskHubClient client;
        private readonly TaskHubWorker fakeTaskHub;
        private readonly TaskHubWorker taskHub;
        private readonly TaskHubWorker taskHubNoCompression;

        public SampleScenarioTests()
        {
            this.client = TestHelpers.CreateTaskHubClient();

            this.taskHub = TestHelpers.CreateTaskHub();
            this.fakeTaskHub = TestHelpers.CreateTaskHub();

            this.taskHubNoCompression = TestHelpers.CreateTaskHubNoCompression();
            this.taskHub.orchestrationService.CreateAsync(true).Wait();
        }

        public void Dispose()
        {
            this.taskHub.StopAsync(true).Wait();
            this.taskHubNoCompression.StopAsync().Wait();
            this.fakeTaskHub.StopAsync(true).Wait();
            this.taskHub.orchestrationService.DeleteAsync(true).Wait();
        }

        #region Common TaskActivities

        public sealed class SendGreetingTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string user)
            {
                return "Greeting send to " + user;
            }
        }

        #endregion Common TaskActivities

        #region Simplest Greetings Test

        [Fact]
        public async Task SimplestGreetingsTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result);
        }

        [Fact]
        public async Task SimplestGreetingsRecreationTest()
        {
            SimplestGreetingsOrchestration.Result = string.Empty;

            await this.taskHub.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60, true, true);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result);

            await Assert.ThrowsAsync<OrchestrationAlreadyExistsException>(() => this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null));

            await Assert.ThrowsAsync<OrchestrationAlreadyExistsException>(() => this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null, null));

            await Assert.ThrowsAsync<OrchestrationAlreadyExistsException>(() => this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null, new[] { OrchestrationStatus.Completed, OrchestrationStatus.Terminated }));

            SimplestGreetingsOrchestration.Result = string.Empty;

            OrchestrationInstance id2 = await this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null, new[] { OrchestrationStatus.Terminated });

            isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id2, 60, true, true);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id2, 60));
            Assert.Equal("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result);

            SimplestGreetingsOrchestration.Result = string.Empty;

            OrchestrationInstance id3 = await this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null, new OrchestrationStatus[] { });

            isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id3, 60, true, true);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id3, 60));
            Assert.Equal("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result);
        }

        [Fact]
        public async Task SimplestGreetingsNoCompressionTest()
        {
            await this.taskHubNoCompression.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result);
        }

        public sealed class SimplestGetUserTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "Gabbar";
            }
        }

        public class SimplestGreetingsOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string user = await context.ScheduleTask<string>(typeof(SimplestGetUserTask));
                string greeting = await context.ScheduleTask<string>(typeof(SimplestSendGreetingTask), user);
                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = greeting;

                return greeting;
            }
        }

        public sealed class SimplestSendGreetingTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string user)
            {
                return "Greeting send to " + user;
            }
        }

        #endregion Simplest Greetings Test

        #region Greetings Test

        [Fact]
        public async Task GreetingsTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(GreetingsOrchestration))
                .AddTaskActivities(typeof(GetUserTask), typeof(SendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(GreetingsOrchestration), null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal("Greeting send to Gabbar", GreetingsOrchestration.Result);
        }

        public sealed class GetUserTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "Gabbar";
            }
        }

        public class GreetingsOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string user = await context.ScheduleTask<string>(typeof(GetUserTask));
                string greeting = await context.ScheduleTask<string>(typeof(SendGreetingTask), user);
                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = greeting;

                return greeting;
            }
        }

        #endregion Greetings Test

        #region Greetings2 Test

        [Fact]
        public async Task Greetings2Test()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(GreetingsOrchestration2))
                .AddTaskActivities(typeof(GetUserTask2), typeof(SendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(GreetingsOrchestration2), 40);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal("Greeting send to Gabbar", GreetingsOrchestration2.Result);

            id = this.client.CreateOrchestrationInstanceAsync(typeof(GreetingsOrchestration2), 2).Result;

            isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal("Greeting send to TimedOut", GreetingsOrchestration2.Result);
        }

        public sealed class GetUserTask2 : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                Thread.Sleep(15 * 1000);
                return "Gabbar";
            }
        }

        public class GreetingsOrchestration2 : TaskOrchestration<string, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, int secondsToWait)
            {
                Task<string> user = context.ScheduleTask<string>(typeof(GetUserTask2));
                Task<string> timer = context.CreateTimer(context.CurrentUtcDateTime.AddSeconds(secondsToWait),
                    "TimedOut");

                Task<string> u = await Task.WhenAny(user, timer);
                string greeting = await context.ScheduleTask<string>(typeof(SendGreetingTask), u.Result);
                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = greeting;

                return greeting;
            }
        }

        #endregion Greetings2 Test

        #region EventConversation

        [Fact]
        public async Task EventConversation()
        {
            await this.taskHub
                .AddTaskOrchestrations(typeof(EventConversationOrchestration),
                                       typeof(EventConversationOrchestration.Responder))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(EventConversationOrchestration), "false");
            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.True(EventConversationOrchestration.OkResult, "Orchestration did not finish ok!!!");
        }

        #endregion EventConversation

        #region Message Overflow Test for Large Orchestration Input Output

        [Fact]
        public async Task MessageOverflowTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(LargeInputOutputOrchestration)).StartAsync();

            // generate a large string as the orchestration input;
            // make it random so that it won't be compressed too much.
            string largeInput = TestUtils.GenerateRandomString(1000 * 1024);
            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(LargeInputOutputOrchestration), largeInput);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal($"output-{largeInput}", LargeInputOutputOrchestration.Result);
        }

        public class LargeInputOutputOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                string output = $"output-{input}";
                Result = output;
                return Task.FromResult(output);
            }
        }

        #endregion Message Overflow Test for Large Orchestration Input Output

        #region AverageCalculator Test

        [Fact]
        public async Task AverageCalculatorTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(AverageCalculatorOrchestration))
                .AddTaskActivities(typeof(ComputeSumTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(
                typeof(AverageCalculatorOrchestration),
                new[] { 1, 50, 10 });

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 120);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 120));
            Assert.Equal(25.5, AverageCalculatorOrchestration.Result);
        }

        private class AverageCalculatorOrchestration : TaskOrchestration<double, int[]>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static double Result;

            public override async Task<double> RunTask(OrchestrationContext context, int[] input)
            {
                if (input == null || input.Length != 3)
                {
                    throw new ArgumentException("input");
                }

                int start = input[0];
                int end = input[1];
                int step = input[2];
                int total = end - start + 1;

                var chunks = new List<Task<int>>();
                while (start < end)
                {
                    int current = start + step - 1;
                    if (current > end)
                    {
                        current = end;
                    }

                    Task<int> chunk = context.ScheduleTask<int>(typeof(ComputeSumTask), new[] { start, current });
                    chunks.Add(chunk);

                    start = current + 1;
                }

                var sum = 0;
                int[] allChunks = await Task.WhenAll(chunks.ToArray());
                foreach (int result in allChunks)
                {
                    sum += result;
                }

                double r = sum / (double)total;
                Result = r;
                return r;
            }
        }

        public sealed class ComputeSumTask : TaskActivity<int[], int>
        {
            protected override int Execute(TaskContext context, int[] chunk)
            {
                if (chunk == null || chunk.Length != 2)
                {
                    throw new ArgumentException("chunk");
                }

                Console.WriteLine("Compute Sum for " + chunk[0] + "," + chunk[1]);
                var sum = 0;
                int start = chunk[0];
                int end = chunk[1];
                for (int i = start; i <= end; i++)
                {
                    sum += i;
                }

                Console.WriteLine("Total Sum for Chunk '" + chunk[0] + "," + chunk[1] + "' is " + sum);

                return sum;
            }
        }

        #endregion AverageCalculator Test

        #region Signal Test

        [Fact]
        public async Task SignalTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(SignalOrchestration))
                .AddTaskActivities(typeof(SendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(SignalOrchestration), null);

            await Task.Delay(2 * 1000);
            await this.client.RaiseEventAsync(id, "GetUser", "Gabbar");

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal("Greeting send to Gabbar", SignalOrchestration.Result);
        }

        public class SignalOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            private TaskCompletionSource<string> resumeHandle;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string user = await WaitForSignal();
                string greeting = await context.ScheduleTask<string>(typeof(SendGreetingTask), user);
                Result = greeting;
                return greeting;
            }

            private async Task<string> WaitForSignal()
            {
                this.resumeHandle = new TaskCompletionSource<string>();
                string data = await this.resumeHandle.Task;
                this.resumeHandle = null;
                return data;
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                Assert.Equal("GetUser", name);
                this.resumeHandle?.SetResult(input);
            }
        }

        #endregion Signal Test

        #region ErrorHandling Test

        [Fact]
        public async Task ErrorHandlingTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(ErrorHandlingOrchestration))
                .AddTaskActivities(typeof(GoodTask), typeof(BadTask), typeof(CleanupTask))
                .StartAsync();
            this.taskHub.TaskActivityDispatcher.IncludeDetails = true;

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(ErrorHandlingOrchestration), null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal("CleanupResult", ErrorHandlingOrchestration.Result);
        }

        public sealed class BadTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                throw new InvalidOperationException("BadTask failed.");
            }
        }

        public sealed class CleanupTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "CleanupResult";
            }
        }

        public class ErrorHandlingOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string goodResult = null;
                string result = null;
                var hasError = false;
                try
                {
                    goodResult = await context.ScheduleTask<string>(typeof(GoodTask));
                    string badResult = await context.ScheduleTask<string>(typeof(BadTask));
                    result = goodResult + badResult;
                }
                catch (TaskFailedException e)
                {
                    Assert.IsType<InvalidOperationException>(e.InnerException);
                    Assert.Equal("BadTask failed.", e.Message);
                    hasError = true;
                }

                if (hasError && !string.IsNullOrWhiteSpace(goodResult))
                {
                    result = await context.ScheduleTask<string>(typeof(CleanupTask));
                }

                Result = result;
                return result;
            }
        }

        public sealed class GoodTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "GoodResult";
            }
        }

        #endregion ErrorHandling Test

        #region Cron Test

        public enum RecurrenceFrequency
        {
            Second,
            Minute,
            Hour,
            Day,
            Week,
            Month,
            Year
        }

        [Fact]
        public async Task CronTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(CronOrchestration))
                .AddTaskActivities(typeof(CronTask))
                .StartAsync();

            CronOrchestration.Tasks.Clear();
            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(CronOrchestration), new CronJob
            {
                Frequency = RecurrenceFrequency.Second,
                Count = 5,
                Interval = 3,
            });

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 120);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 120));
            Assert.Equal(5, CronTask.Result);
            Assert.Equal(5, CronOrchestration.Result);
            int taskExceptions = CronOrchestration.Tasks.Count(task => task.Exception != null);
            Assert.Equal(0, taskExceptions);
        }

        public class CronJob
        {
            public RecurrenceFrequency Frequency { get; set; }

            public int Interval { get; set; }

            public int Count { get; set; }
        }

        public class CronOrchestration : TaskOrchestration<string, CronJob>
        {
            public static int Result;
            public static List<Task<string>> Tasks = new List<Task<string>>();

            public override async Task<string> RunTask(OrchestrationContext context, CronJob job)
            {
                int runAfterEverySeconds;
                if (job.Frequency == RecurrenceFrequency.Second)
                {
                    runAfterEverySeconds = job.Interval;
                }
                else
                {
                    throw new NotSupportedException("Job Frequency '" + job.Frequency + "' not supported...");
                }

                int i;
                for (i = 1; i <= job.Count; i++)
                {
                    DateTime currentTime = context.CurrentUtcDateTime;
                    DateTime fireAt = currentTime.AddSeconds(runAfterEverySeconds);

                    string attempt = await context.CreateTimer(fireAt, i.ToString());

                    Tasks.Add(context.ScheduleTask<string>(typeof(CronTask), attempt));
                }

                Result = i - 1;
                return "Done";
            }
        }

        private sealed class CronTask : TaskActivity<string, string>
        {
            public static int Result;

            protected override string Execute(TaskContext context, string input)
            {
                Result++;
                Thread.Sleep(2 * 1000);
                string completed = "Cron Job '" + input + "' Completed...";
                return completed;
            }
        }

        #endregion Cron Test

        #region SubOrchestrationInstance Test

        [Fact]
        public async Task SubOrchestrationTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(ParentWorkflow), typeof(ChildWorkflow))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(ParentWorkflow), true);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result);

            id = await this.client.CreateOrchestrationInstanceAsync(typeof(ParentWorkflow), false);

            isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result);
        }

        public class ChildWorkflow : TaskOrchestration<string, int>
        {
            public override Task<string> RunTask(OrchestrationContext context, int input)
            {
                return Task.FromResult($"Child '{input}' completed.");
            }
        }

        public class ParentWorkflow : TaskOrchestration<string, bool>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
            {
                var results = new Task<string>[5];
                for (var i = 0; i < 5; i++)
                {
                    Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(ChildWorkflow), i);
                    if (waitForCompletion)
                    {
                        await r;
                    }

                    results[i] = r;
                }

                string[] data = await Task.WhenAll(results);
                Result = string.Concat(data);
                return Result;
            }
        }

        #endregion SubOrchestrationInstance Test

        #region SubOrchestrationInstance Failure Test

        [Fact]
        public async Task SubOrchestrationFailedTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(ParentWorkflow2), typeof(ChildWorkflow2))
                .StartAsync();
            this.taskHub.TaskOrchestrationDispatcher.IncludeDetails = true;

            ChildWorkflow2.Count = 0;
            ParentWorkflow2.Result = null;
            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(ParentWorkflow2), true);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));

            Assert.Equal("Test", ParentWorkflow2.Result);
            Assert.Equal(1, ChildWorkflow2.Count);

            ChildWorkflow2.Count = 0;
            ParentWorkflow2.Result = null;
            id = await this.client.CreateOrchestrationInstanceAsync(typeof(ParentWorkflow2), false);

            isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal("Test", ParentWorkflow2.Result);
            Assert.Equal(5, ChildWorkflow2.Count);
        }

        public class ChildWorkflow2 : TaskOrchestration<string, int>
        {
            public static int Count;

            public override Task<string> RunTask(OrchestrationContext context, int input)
            {
                Count++;
                throw new InvalidOperationException("Test");
            }
        }

        public class ParentWorkflow2 : TaskOrchestration<string, bool>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
            {
                var results = new Task<string>[5];
                try
                {
                    for (var i = 0; i < 5; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(ChildWorkflow2), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }

                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                }
                catch (SubOrchestrationFailedException e)
                {
                    Assert.IsType<InvalidOperationException>(e.InnerException);
                    Result = e.Message;
                }

                return Result;
            }
        }

        #endregion SubOrchestrationInstance Failure Test

        #region BadOrchestration Test

        [Fact]
        public async Task BadOrchestrationTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(BadOrchestration))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(BadOrchestration), null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
        }

        public class BadOrchestration : TaskOrchestration<string, string>
        {
#pragma warning disable 1998

            public override async Task<string> RunTask(OrchestrationContext context, string input)
#pragma warning restore 1998
            {
                throw new Exception("something very bad happened");
            }
        }

        #endregion BadOrchestration Test

        #region SubOrchestrationInstance Explicit InstanceId Test

        [Fact]
        public async Task SubOrchestrationExplicitIdTest()
        {
            SimpleChildWorkflow.ChildInstanceId = null;
            await this.taskHub.AddTaskOrchestrations(typeof(SimpleParentWorkflow), typeof(SimpleChildWorkflow))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(SimpleParentWorkflow), null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.True(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.Equal("foo_instance", SimpleChildWorkflow.ChildInstanceId);
        }

        public class SimpleChildWorkflow : TaskOrchestration<string, object>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string ChildInstanceId;

            public override Task<string> RunTask(OrchestrationContext context, object input)
            {
                ChildInstanceId = context.OrchestrationInstance.InstanceId;
                return Task.FromResult<string>("123");
            }
        }

        public class SimpleParentWorkflow : TaskOrchestration<string, object>
        {
            public override async Task<string> RunTask(OrchestrationContext context, object input)
            {
                var r = await
                       context.CreateSubOrchestrationInstanceWithRetry<string>(typeof(SimpleChildWorkflow), "foo_instance",
                           new RetryOptions(TimeSpan.FromSeconds(5), 3), null);
                return null;
            }
        }

        #endregion SubOrchestrationInstance Explicit InstanceId Test
    }
}