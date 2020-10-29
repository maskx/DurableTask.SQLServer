using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer.Database
{
    public class DbAccess : IDisposable
    {
        private DbCommand _Command;
        public DbCommand Command
        {
            get
            {
                if (_Command == null)
                {
                    _Command = connection.CreateCommand();
                    _Command.CommandType = CommandType.Text;
                }

                return _Command;
            }
            set { _Command = value; }
        }
        protected DbConnection connection;
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
        }

        #region CommandType.Text

        #region AddStatement
        public DbAccess AddStatement(string sql)
        {
            Command.AddStatement(sql, null);
            return this;
        }
        public DbAccess AddStatement(string sql, IDictionary<string, object> parameters)
        {
            Command.AddStatement(sql, parameters);
            return this;
        }

        public DbAccess AddStatement(string sql, object parameters)
        {
            Command.AddStatement(sql, parameters);
            return this;
        }
        #endregion

        #region ExecuteScalarAsync
        public async Task<object> ExecuteScalarAsync()
        {
            if (string.IsNullOrEmpty(Command.CommandText))
                throw new Exception("commandText cannot be empty");
            object obj;
            obj = await ExcuteWithRetry(() => Command.ExecuteScalarAsync());
            return obj;
        }
        #endregion

        #region ExecuteReaderAsync
        public async Task ExecuteReaderAsync(Action<DbDataReader> dataReader)
        {
            if (string.IsNullOrEmpty(Command.CommandText))
                return;
            using DbDataReader reader = await ExcuteWithRetry(() => Command.ExecuteReaderAsync(), false);
            dataReader?.Invoke(reader);
        }

        public async Task ExecuteReaderAsync(Action<DbDataReader, int> dataReaders, bool bulkRead = false)
        {
            if (string.IsNullOrEmpty(Command.CommandText))
                return;
            using DbDataReader reader = await ExcuteWithRetry(() => Command.ExecuteReaderAsync(), false);
            int resultSet = 0;
            do
            {
                if (bulkRead)
                    dataReaders(reader, resultSet);
                else
                    while (await reader.ReadAsync())
                        dataReaders(reader, resultSet);

                resultSet++;
            } while (await reader.NextResultAsync());
        }

        #endregion

        #region ExecuteNonQueryAsync
        public async Task<int> ExecuteNonQueryAsync()
        {
            if (string.IsNullOrEmpty(Command.CommandText))
                return 0;
            int recordsAffected;
            recordsAffected = await ExcuteWithRetry(() => Command.ExecuteNonQueryAsync());
            return recordsAffected;
        }
        #endregion

        #endregion

        #region CommandType.StoredProcedure

        #region ExecuteNonQueryAsync
        public async Task<int> ExecuteStoredProcedureASync(string spnmae, int commandTimeout = 0)
        {
            Command = CreateStoredProcedureCommand(spnmae, commandTimeout);
            var recordsAffected = await ExcuteWithRetry(() => Command.ExecuteNonQueryAsync());
            return recordsAffected;
        }
        public async Task<int> ExecuteStoredProcedureASync(string spnmae, object parameters, int commandTimeout = 0)
        {
            Command = CreateStoredProcedureCommand(spnmae, commandTimeout, parameters);
            var recordsAffected = await ExcuteWithRetry(() => Command.ExecuteNonQueryAsync());
            return recordsAffected;
        }
        public async Task<int> ExecuteStoredProcedureASync(string spnmae, Dictionary<string, object> parameters, int commandTimeout = 0)
        {
            Command = CreateStoredProcedureCommand(spnmae, commandTimeout, parameters);
            var recordsAffected = await ExcuteWithRetry(() => Command.ExecuteNonQueryAsync());
            return recordsAffected;
        }
        public async Task<int> ExecuteStoredProcedureASync(string commandText, DbParameter[] parameters, int commandTimeout = 0)
        {
            Command = CreateStoredProcedureCommand(commandText, commandTimeout, parameters);
            var recordsAffected = await ExcuteWithRetry(() => Command.ExecuteNonQueryAsync());
            return recordsAffected;
        }
        #endregion ExecuteNonQueryAsync

        #region ExecuteStoredProcedure
        public async Task ExecuteStoredProcedureASync(string spname, Action<DbDataReader, int> dataReader, int commandTimeout = 0)
        {
            using DbDataReader reader = await CreateStoredProcedureReaderASync(spname, commandTimeout);
            int resultSet = 0;
            do
            {
                while (reader.Read())
                    dataReader(reader, resultSet);
                resultSet++;
            } while (reader.NextResult());
            reader.Close();
        }
        public async Task ExecuteStoredProcedureASync(string spname, Action<DbDataReader, int> dataReader, Dictionary<string, object> parameters, int commandTimeout = 0)
        {
            using DbDataReader reader = await CreateStoredProcedureReaderASync(spname, commandTimeout, parameters);
            int resultSet = 0;
            do
            {
                while (reader.Read())
                    dataReader(reader, resultSet);
                resultSet++;
            } while (reader.NextResult());
            reader.Close();
        }
        public async Task ExecuteStoredProcedureASync(string spname, Action<DbDataReader, int> dataReader, object parameters, int commandTimeout = 0)
        {
            using DbDataReader reader = await CreateStoredProcedureReaderASync(spname, commandTimeout, parameters);
            int resultSet = 0;
            do
            {
                while (reader.Read())
                    dataReader(reader, resultSet);
                resultSet++;
            } while (reader.NextResult());
            reader.Close();
        }
        public async Task ExecuteStoredProcedureASync(string spname, Action<DbDataReader, int> dataReader, DbParameter[] parameters, int commandTimeout = 0)
        {
            using DbDataReader reader = await CreateStoredProcedureReaderASync(spname, commandTimeout, parameters);
            int resultSet = 0;
            do
            {
                while (reader.Read())
                    dataReader(reader, resultSet);
                resultSet++;
            } while (reader.NextResult());
            reader.Close();
        }
       
        #endregion

        #region CreateStoredProcedureCommand
        private DbCommand CreateStoredProcedureCommand(string spname, int commandTimeout)
        {
            if (connection == null)
                throw new ObjectDisposedException("DbAccess");

            var dbCommand = connection.CreateCommand();
            dbCommand.CommandType = CommandType.StoredProcedure;
            dbCommand.CommandText = spname;

            if (commandTimeout > 0)
                dbCommand.CommandTimeout = commandTimeout;

            return dbCommand;
        }
        private DbCommand CreateStoredProcedureCommand(string commandText, int commandTimeout, object parameters)
        {
            var dbCommand = CreateStoredProcedureCommand(commandText, commandTimeout);
            if (parameters != null)
            {
                foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(parameters))
                {
                    dbCommand.AddParameter(descriptor.Name, descriptor.GetValue(parameters));
                }
            }
            return dbCommand;
        }
        private DbCommand CreateStoredProcedureCommand(string commandText, int commandTimeout, Dictionary<string, object> parameters)
        {
            var dbCommand = CreateStoredProcedureCommand(commandText, commandTimeout);
            foreach (var par in parameters)
            {
                dbCommand.AddParameter(par.Key, par.Value);
            }
            return dbCommand;
        }
        private DbCommand CreateStoredProcedureCommand(string commandText, int commandTimeout, DbParameter[] parameters)
        {
            var dbCommand = CreateStoredProcedureCommand(commandText, commandTimeout);
            dbCommand.Parameters.AddRange(parameters);
            return dbCommand;
        }
        #endregion

        #region CreateStoredProcedureReaderASync
        private async Task<DbDataReader> CreateStoredProcedureReaderASync(string spname, int commandTimeout)
        {
            return await ExcuteWithRetry(() =>
            {
                Command = CreateStoredProcedureCommand(spname, commandTimeout);
                return Command.ExecuteReaderAsync(CommandBehavior.CloseConnection);
            }, false);
        }
        private async Task<DbDataReader> CreateStoredProcedureReaderASync(string spname, int commandTimeout, Dictionary<string, object> parameters)
        {
            return await ExcuteWithRetry(() =>
            {
                Command = CreateStoredProcedureCommand(spname, commandTimeout, parameters);
                return Command.ExecuteReaderAsync(CommandBehavior.CloseConnection);
            }, false);
        }
        private async Task<DbDataReader> CreateStoredProcedureReaderASync(string spname, int commandTimeout, object parameters)
        {
            return await ExcuteWithRetry(() =>
            {
                Command = CreateStoredProcedureCommand(spname, commandTimeout, parameters);
                return Command.ExecuteReaderAsync(CommandBehavior.CloseConnection);
            }, false);
        }
        private async Task<DbDataReader> CreateStoredProcedureReaderASync(string spname, int commandTimeout, DbParameter[] parameters)
        {
            return await ExcuteWithRetry(() =>
            {
                Command = CreateStoredProcedureCommand(spname, commandTimeout, parameters);
                return Command.ExecuteReaderAsync(CommandBehavior.CloseConnection);
            }, false);
        }
        #endregion

        #endregion

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
        private async Task<T> ExcuteWithRetry<T>(Func<Task<T>> func, bool closeConnection = true)
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
                try { if (closeConnection) connection.Close(); } catch { }
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
                    connection.Dispose();
                }
            }
        }

        #endregion IDisposable Members
    }
}