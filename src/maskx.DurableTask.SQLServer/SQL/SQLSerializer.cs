using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.Core.Serializing;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace maskx.DurableTask.SQLServer
{
    internal class SQLSerializer
    {
        private static readonly JsonSerializerSettings serializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All,
            SerializationBinder = new PackageUpgradeSerializationBinder()
        };

        public static string SerializeObject(object obj)
        {
            return JsonConvert.SerializeObject(obj, serializerSettings);
        }

        public static T DeserializeObject<T>(string serializedObj)
        {
            return JsonConvert.DeserializeObject<T>(serializedObj, serializerSettings);
        }

        public static OrchestrationRuntimeState DeserializeRuntimeState(string serializedRuntimeState)
        {
            // OrchestrationRuntimeEvent builds its internal state with it's constructor and the AddEvent() method.
            // Must emulate that when deserializing
            IList<HistoryEvent> events = JsonConvert.DeserializeObject<IList<HistoryEvent>>(serializedRuntimeState, serializerSettings);
            return new OrchestrationRuntimeState(events);
        }

        /// <summary>
        /// Deserializes a Json String to specified Type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="jsonString"></param>
        /// <returns></returns>
        public static T DeserializeJson<T>(string jsonString)
        {
            if (String.IsNullOrEmpty(jsonString) || String.IsNullOrWhiteSpace(jsonString))
                return default(T);

            object obj = JsonConvert.DeserializeObject(jsonString, typeof(T));

            return (T)obj;
        }

        /// <summary>
        /// Serialize History Event to json string
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string SerializeToJson(object obj)
        {
            if (obj == null)
                return String.Empty;

            string serializedHistoryEvent = JsonConvert.SerializeObject(obj,
                new JsonSerializerSettings
                {
                    Formatting = Formatting.Indented,
                    TypeNameHandling = TypeNameHandling.Objects
                });

            return serializedHistoryEvent;
        }
    }
}