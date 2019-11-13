using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace maskx.DurableTask.SQLServer
{
    internal class DbAccess : IDisposable
    {
        SqlConnection _Connection;
        const int _MaxRetryCount = 2;
        const int _IncreasingDelayRetry = 500;

        public DbAccess(string connectionString)
        {
            _Connection = new SqlConnection(connectionString);
            _Connection.Open();

        }
        public void Dispose()
        {
            if (_Connection != null)
            {
                if (_Connection.State != ConnectionState.Closed)
                {
                    _Connection.Dispose();
                }

                _Connection = null;
            }
        }
        public async Task<SqlParameterCollection> ExecuteReaderAsync(string commandText, Action<SqlDataReader, int> dataReader, Action<SqlParameterCollection> parametersBuilder = null, CommandType commandType = CommandType.Text, int commandTimeout = 0)
        {
            using (SqlDataReader reader = CreateReader(commandText, parametersBuilder, out SqlParameterCollection pars, commandType, commandTimeout))
            {
                if (dataReader == null)
                    return pars;
                int resultSet = 0;
                do
                {
                    while (await reader.ReadAsync())
                        dataReader(reader, resultSet);
                    resultSet++;
                } while (await reader.NextResultAsync());
                reader.Close();
                return pars;
            }
        }
        public async Task<int> ExecuteNonQueryAsync(string commandText, Action<SqlParameterCollection> parametersBuilder,
              CommandType commandType = CommandType.Text, int commandTimeout = 0)
        {
            int nAffectedRows = 0;

            for (int retry = 0; ; retry++)
            {
                try
                {
                    nAffectedRows = await CreateCommand(commandText, parametersBuilder, commandType, commandTimeout).ExecuteNonQueryAsync();
                    break;
                }
                catch (Exception e)
                {
                    if (retry < _MaxRetryCount && OnConnectionLost(e))
                        ReConnect(retry);
                    else
                        throw;
                }
            }

            return nAffectedRows;
        }
        bool OnConnectionLost(Exception dbException)
        {
            bool canRetry = false;

            SqlException e = dbException as SqlException;

            if (e == null)
                canRetry = false;
            else
                switch (e.Number)
                {
                    case 233:
                    case -2: canRetry = true; break;
                    default: canRetry = false; break;
                }
            return canRetry;
        }
        void ReConnect(int retrying)
        {
            if (_Connection != null)
                if (_Connection.State != ConnectionState.Closed)
                {
                    _Connection.Close();

                    if (retrying > 0)
                        Thread.Sleep(retrying * _IncreasingDelayRetry);	// retrying starts at 0, increases delay time for every retry.

                    _Connection.Open();
                }
        }
        SqlCommand CreateCommand(string commandText, Action<SqlParameterCollection> parametersBuilder, CommandType commandType = CommandType.StoredProcedure, int commandTimeout = 0)
        {
            if (_Connection == null)
                throw new ObjectDisposedException("DbAccess");

            var dbCommand = _Connection.CreateCommand();
            dbCommand.CommandType = commandType;
            dbCommand.CommandText = commandText;

            if (commandTimeout > 0)
                dbCommand.CommandTimeout = commandTimeout;

            parametersBuilder?.Invoke(dbCommand.Parameters);

            return dbCommand;
        }
        SqlDataReader CreateReader(string commandText
             , Action<SqlParameterCollection> parametersBuilder
             , out SqlParameterCollection pars
             , CommandType commandType = CommandType.StoredProcedure
        , int commandTimeout = 0)
        {
            for (int retry = 0; ; retry++)
            {
                try
                {
                    var dbCmd = CreateCommand(commandText, parametersBuilder, commandType, commandTimeout);
                    pars = dbCmd.Parameters;
                    return dbCmd.ExecuteReader(CommandBehavior.CloseConnection);
                }
                catch (Exception e)
                {
                    if (retry < _MaxRetryCount && OnConnectionLost(e))
                        ReConnect(retry);
                    else
                        throw;
                }
            }
        }

    }
}
