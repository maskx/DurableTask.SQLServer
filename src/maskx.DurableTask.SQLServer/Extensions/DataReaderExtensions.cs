using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;

namespace maskx.DurableTask.SQLServer
{
    public static class DataReaderExtensions
    {
        public static T GetValue<T>(this IDataReader reader, string column)
        {
            int ordinal = reader.GetOrdinal(column);
            return GetValue<T>(reader, ordinal);
        }

        public static T GetValue<T>(this IDataReader reader, int columnNo)
        {
            if (reader.IsDBNull(columnNo))
                return default(T);
            var typeInfo = typeof(T).GetTypeInfo();
            if (typeInfo.IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                Type[] source = typeInfo.IsGenericTypeDefinition ? typeInfo.GenericTypeParameters : typeInfo.GenericTypeArguments;
                Type type = source.FirstOrDefault<Type>();
                bool isEnum = type.GetTypeInfo().IsEnum;
                object value;
                if (isEnum)
                {
                    value = Enum.Parse(type, reader.GetValue(columnNo).ToString());
                }
                else
                {
                    value = Convert.ChangeType(reader.GetValue(columnNo), type, CultureInfo.InvariantCulture);
                }
                NullableConverter nullableConverter = new NullableConverter(typeof(T));
                return (T)((object)nullableConverter.ConvertFrom(value));
            }
            if (typeInfo.IsEnum)
                return (T)Enum.Parse(typeof(T), reader.GetValue(columnNo).ToString());
            return (T)Convert.ChangeType(reader.GetValue(columnNo), typeof(T), CultureInfo.InvariantCulture);
        }
    }
}