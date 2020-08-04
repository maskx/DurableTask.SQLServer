using System;
using System.Data.SqlClient;

namespace maskx.DurableTask.SQLServer.Database
{
    public class SQLServerAccess : DbAccess
    {
        public SQLServerAccess(string connectionString) : base(SqlClientFactory.Instance, connectionString)
        {
        }

        protected override void OnReconnecting()
        {
            if (!(connection is SqlConnection conn))
                return;
            SqlConnection.ClearPool(conn);
        }

        protected override RetryAction OnContextLost(Exception dbException)
        {
            var retryAction = RetryAction.None;
            if (connection is SqlConnection)
            {
                if (!(dbException is SqlException e))
                    retryAction = RetryAction.None;
                else
                    switch (e.Number)   // sys.messages
                    {
                        case 233:
                        case -2:
                        case 10054:
                            retryAction = RetryAction.Reconnect;
                            break;

                        case 201:       // Procedure or function '%.*ls' expects parameter '%.*ls', which was not supplied.
                        case 206:       // Operand type clash: %ls is incompatible with %ls.
                        case 257:       // Implicit conversion from data type %ls to %ls is not allowed. Use the CONVERT function to run this query.
                        case 8144:      // Procedure or function %.*ls has too many arguments specified.
                            retryAction = RetryAction.RefreshParameters;
                            break;
                        // To add other cases
                        default:
                            retryAction = RetryAction.None;
                            break;
                    }
            }
            return retryAction;
        }
    }
}