using DurableTask.Core;
using DurableTask.Core.Common;
using DurableTask.Core.History;
using DurableTask.Core.Settings;
using maskx.DurableTask.SQLServer.Settings;
using maskx.DurableTask.SQLServer.Tracking;
using Microsoft.Extensions.Configuration;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer.Tests
{
    public static class TestHelpers
    {
        private static IConfigurationRoot _ConfigurationRoot;

        private static string SQLServerConnectionString
        {
            get
            {
                return _ConfigurationRoot.GetConnectionString("dbConnection");
            }
        }

        private static string TaskHubName
        {
            get
            {
                return _ConfigurationRoot["TaskHubName"];
            }
        }

        static TestHelpers()
        {
            _ConfigurationRoot = GetIConfigurationRoot(AppContext.BaseDirectory);
        }

        public static IConfigurationRoot GetIConfigurationRoot(string outputPath)
        {
            return new ConfigurationBuilder()
                .SetBasePath(outputPath)
                .AddJsonFile("appsettings.json", optional: true)
                .AddUserSecrets("3f1758ab-b9bd-4af0-a067-ecd5b65241c7")
                .Build();
        }

        public static SQLServerOrchestrationServiceSettings CreateTestWorkerSettings(CompressionStyle style = CompressionStyle.Threshold)
        {
            var settings = new SQLServerOrchestrationServiceSettings
            {
                TaskOrchestrationDispatcherSettings = { CompressOrchestrationState = true },
                MessageCompressionSettings = new CompressionSettings { Style = style, ThresholdInBytes = 1024 }
            };

            return settings;
        }

        public static SQLServerOrchestrationServiceSettings CreateTestClientSettings()
        {
            var settings = new SQLServerOrchestrationServiceSettings
            {
                MessageCompressionSettings = new CompressionSettings
                {
                    Style = CompressionStyle.Threshold,
                    ThresholdInBytes = 1024
                }
            };

            return settings;
        }

        private static IOrchestrationService CreateOrchestrationServiceWorker(
            SQLServerOrchestrationServiceSettings settings,
            TimeSpan jumpStartAttemptInterval)
        {
            var service = new SQLServerOrchestrationService(
                SQLServerConnectionString,
                TaskHubName,
               CreateSQLServerInstanceStore(),
                settings);
            return service;
        }

        private static IOrchestrationServiceClient CreateOrchestrationServiceClient(
            SQLServerOrchestrationServiceSettings settings)
        {
            var service = new SQLServerOrchestrationService(
                SQLServerConnectionString,
                TaskHubName,
               CreateSQLServerInstanceStore(),
                settings);
            return service;
        }

        public static SqlServerInstanceStore CreateSQLServerInstanceStore()
        {
            return new SqlServerInstanceStore(new SqlServerInstanceStoreSettings()
            {
                SchemaName = "dbo",
                HubName = "DTF",
                ConnectionString = SQLServerConnectionString
            });
        }

        public static TaskHubClient CreateTaskHubClient()
        {
            return new TaskHubClient(CreateOrchestrationServiceClient(CreateTestClientSettings()));
        }

        public static TaskHubWorker CreateTaskHubNoCompression()
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestClientSettings(), TimeSpan.FromMinutes(1)));
        }

        public static TaskHubWorker CreateTaskHubLegacyCompression()
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestWorkerSettings(CompressionStyle.Legacy), TimeSpan.FromMinutes(1)));
        }

        public static TaskHubWorker CreateTaskHubAlwaysCompression()
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestWorkerSettings(CompressionStyle.Always), TimeSpan.FromMinutes(1)));
        }

        public static TaskHubWorker CreateTaskHub()
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestWorkerSettings(), TimeSpan.FromMinutes(1)));
        }

        public static TaskHubWorker CreateTaskHub(TimeSpan jumpStartAttemptInterval)
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestWorkerSettings(), jumpStartAttemptInterval));
        }

        public static TaskHubWorker CreateTaskHub(SQLServerOrchestrationServiceSettings settings)
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(settings, TimeSpan.FromMinutes(10)));
        }

        public static async Task<bool> WaitForInstanceAsync(TaskHubClient taskHubClient, OrchestrationInstance instance,
            int timeoutSeconds,
            bool waitForCompletion = true,
            bool exactExecution = false)
        {
            if (string.IsNullOrWhiteSpace(instance?.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            var sleepForSeconds = 5;

            while (timeoutSeconds > 0)
            {
                OrchestrationState state;
                if (exactExecution)
                {
                    state = await taskHubClient.GetOrchestrationStateAsync(instance);
                }
                else
                {
                    state = await taskHubClient.GetOrchestrationStateAsync(instance.InstanceId);
                }

                if (waitForCompletion && state == null)
                {
                    await Task.Delay(sleepForSeconds * 1000);
                    timeoutSeconds -= sleepForSeconds;
                    continue;
                    // throw new ArgumentException("OrchestrationState is expected but NULL value returned");
                }

                if (waitForCompletion &&
                    (state.OrchestrationStatus == OrchestrationStatus.Running ||
                     state.OrchestrationStatus == OrchestrationStatus.Pending))
                {
                    await Task.Delay(sleepForSeconds * 1000);
                    timeoutSeconds -= sleepForSeconds;
                }
                else
                {
                    // Session state deleted after completion
                    return true;
                }
            }

            return false;
        }

        public static string PrintHistory(TaskHubClient taskHubClient, OrchestrationInstance instance)
        {
            return taskHubClient.GetOrchestrationHistoryAsync(instance).Result;
        }

        public static string GetInstanceNotCompletedMessage(
            TaskHubClient taskHubClient,
            OrchestrationInstance instance,
            int timeWaited)
        {
            if (string.IsNullOrWhiteSpace(instance?.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            string history = PrintHistory(taskHubClient, instance);
            string message = $"Instance '{instance}' not completed within {timeWaited} seconds.\n History: {history}";

            return message;
        }

        //TODO  NotImplementedException
        public static string GetTestSetting(string name)
        {
            string value = Environment.GetEnvironmentVariable("DurableTaskTest" + name);
            if (string.IsNullOrWhiteSpace(value))
            {
                value = name;
            }

            return value;
        }

        public static async Task<OrchestrationInstance> CreateOrchestrationInstanceAsync(
            SQLServerOrchestrationService sboService,
            string name,
            string version,
            string instanceId,
            string executionId,

            bool serviceBusOnly)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                instanceId = Guid.NewGuid().ToString("N");
            }

            if (string.IsNullOrWhiteSpace(executionId))
            {
                executionId = Guid.NewGuid().ToString("N");
            }

            var orchestrationInstance = new OrchestrationInstance
            {
                InstanceId = instanceId,
                ExecutionId = executionId,
            };

            var startedEvent = new ExecutionStartedEvent(-1, null)
            {
                Tags = null,
                Name = name,
                Version = version,
                OrchestrationInstance = orchestrationInstance
            };

            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = orchestrationInstance,
                Event = startedEvent
            };

            if (serviceBusOnly)
            {
                await sboService.SendTaskOrchestrationMessageAsync(taskMessage);
            }
            else
            {
                await sboService.CreateTaskOrchestrationAsync(taskMessage);
            }

            return orchestrationInstance;
        }

        public static async Task<TException> ThrowsAsync<TException>(Func<Task> action, string errorMessage = null) where TException : Exception
        {
            errorMessage = errorMessage ?? "Failed";
            try
            {
                await action();
            }
            catch (TException ex)
            {
                return ex;
            }
            catch (Exception ex)
            {
                throw new AssertFailedException(
                    $"{errorMessage}. Expected:<{typeof(TException).ToString()}> Actual<{ex.GetType().ToString()}>", ex);
            }

            throw new AssertFailedException($"{errorMessage}. Expected {typeof(TException).ToString()} exception but no exception is thrown");
        }
    }
}