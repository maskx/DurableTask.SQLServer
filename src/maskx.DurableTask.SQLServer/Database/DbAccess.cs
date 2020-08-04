using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer.Database
{
    public class DbAccess : IDisposable
    {
        protected DbConnection connection;
        protected readonly DbCommand command;
        protected const int _MaxRetryCount = 2;
        protected const int _IncreasingDelayRetry = 500;

        protected enum RetryAction
        {
            None = 0,
            Reconnect = 1,
            RefreshParameters = 2
        }

        public DbAccess(DbProviderFactory dbProviderFactory, string connectionString)
        {
            connection = dbProviderFactory.CreateConnection();
            connection.ConnectionString = connectionString;
            command = connection.CreateCommand();
        }

        public DbAccess AddStatement(string sql, IDictionary<string, object> parameters = null)
        {
            command.AddStatement(sql, parameters);
            return this;
        }

        public DbAccess AddStatement(string sql, object parameters)
        {
            command.AddStatement(sql, parameters);
            return this;
        }

        public async Task<object> ExecuteScalarAsync()
        {
            if (string.IsNullOrEmpty(command.CommandText))
                throw new Exception("commandText cannot be empty");
            object obj;
            obj = await ExcuteWithRetry(() => command.ExecuteScalarAsync());
            return obj;
        }

        public async Task ExecuteReaderAsync(Action<DbDataReader> dataReader)
        {
            if (string.IsNullOrEmpty(command.CommandText))
                return;
            using (DbDataReader reader = await ExcuteWithRetry(() => command.ExecuteReaderAsync()))
            {
                dataReader?.Invoke(reader);
            }
        }

        public async Task ExecuteReaderAsync(Action<DbDataReader, int> dataReaders, bool bulkRead = false)
        {
            if (string.IsNullOrEmpty(command.CommandText))
                return;

            using (DbDataReader reader = await ExcuteWithRetry(() => command.ExecuteReaderAsync()))
            {
                if (dataReaders != null)
                {
                    int resultSet = 0;
                    do
                    {
                        if (dataReaders != null)
                            if (bulkRead)
                                dataReaders(reader, resultSet);
                            else
                                while (await reader.ReadAsync())
                                    dataReaders(reader, resultSet);

                        resultSet++;
                    } while (await reader.NextResultAsync());
                }
            }
        }

        private DbCommand CreateCommand(string commandText, int commandTimeout, CommandType commandType, object parameters)
        {
            var dictionary = new Dictionary<string, object>();
            if (parameters != null)
            {
                //convert object to dictionary
                foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(parameters))
                {
                    dictionary.Add(descriptor.Name, descriptor.GetValue(parameters));
                }
            }
            return CreateCommand(commandText, commandTimeout, commandType, dictionary);
        }

        private DbCommand CreateCommand(string commandText, int commandTimeout, CommandType commandType, Dictionary<string, object> parameters)
        {
            command.CommandType = commandType;
            command.CommandText = commandText;

            if (commandTimeout > 0)
                command.CommandTimeout = commandTimeout;

            foreach (var par in parameters)
            {
                command.AddParameter(par.Key, par.Value);
            }
            return command;
        }

        public async Task<DbDataReader> CreateReaderAsync(string commandText
             , int commandTimeout
             , CommandType commandType
             , object parameters = null)
        {
            return await ExcuteWithRetry(() =>
           {
               var dbCmd = CreateCommand(commandText, commandTimeout, commandType, parameters);
               return dbCmd.ExecuteReaderAsync(CommandBehavior.CloseConnection);
           });
        }

        public async Task<DbParameterCollection> ExecuteReaderAsync(string commandText, Action<DbDataReader, int> dataReader, object parameters = null, CommandType commandType = CommandType.StoredProcedure, int commandTimeout = 0)
        {
            using (DbDataReader reader = await CreateReaderAsync(commandText, commandTimeout, commandType, parameters))
            {
                if (dataReader == null)
                    return null;
                int resultSet = 0;
                do
                {
                    while (reader.Read())
                        dataReader(reader, resultSet);
                    resultSet++;
                } while (reader.NextResult());
                reader.Close();
                return command.Parameters;
            }
        }

        private void ReConnect()
        {
            if (connection == null)
                return;
            if (connection.State == ConnectionState.Closed)
                connection.Open();
            else
            {
                OnReconnecting();
                connection.Close();
                connection.Open();
            }
        }

        public async Task<int> ExecuteNonQueryAsync()
        {
            if (string.IsNullOrEmpty(command.CommandText))
                return 0;
            int recordsAffected;
            recordsAffected = await ExcuteWithRetry(() => command.ExecuteNonQueryAsync());
            return recordsAffected;
        }

        public async Task<int> ExecuteNonQueryAsync(string commandText, object parameters = null, CommandType commandType = CommandType.StoredProcedure, int commandTimeout = 0)
        {
            var dbCmd = CreateCommand(commandText, commandTimeout, commandType, parameters);
            var recordsAffected = await ExcuteWithRetry(() => dbCmd.ExecuteNonQueryAsync());
            return recordsAffected;
        }

        private async Task<T> ExcuteWithRetry<T>(Func<Task<T>> func)
        {
            try
            {
                await connection.OpenAsync();
                for (int retry = 0; ; retry++)
                {
                    try
                    {
                        return await func();
                    }
                    catch (Exception e)
                    {
                        if (retry >= _MaxRetryCount)
                            throw;
                        switch (OnContextLost(e))
                        {
                            case RetryAction.Reconnect:
                                await Task.Delay(retry * _IncreasingDelayRetry);
                                ReConnect();
                                break;

                            case RetryAction.RefreshParameters:
                                throw;
                            default:
                                throw;
                        }
                    }
                }
            }
            finally
            {
                try { connection.Close(); } catch { }
            }
        }

        protected virtual RetryAction OnContextLost(Exception dbException)
        {
            return RetryAction.None;
        }

        protected virtual void OnReconnecting()
        {
        }

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing && connection != null)
            {
                if (connection.State != ConnectionState.Closed)
                {
                    connection.Dispose();          // Close()
                }

                connection = null;
            }
        }

        #endregion IDisposable Members
    }
}