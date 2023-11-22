using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Ruzzie.Common.Threading;

namespace Ruzzie.Azure.Storage
{
    [Obsolete]
    public interface ITablePool<TTable> : IDisposable
    {
        string              TableName { get; }

        [Obsolete]
        IObjectPool<TTable> Pool      { get; }


        /// Executed the given function in the configured pool <see cref="IObjectPool{T}.ExecuteOnAvailableObject{TResult}"/>, for easy access.
        [Obsolete]
        public TResult Execute<TResult>(Func<TTable, TResult> funcToExecute);

        /// Executed the given function async in the configured pool <see cref="IObjectPool{T}.ExecuteOnAvailableObject{TResult}"/>, for easy access.
        [Obsolete]
        public Task<TResult> ExecuteAsync<TResult>(Func<TTable, Task<TResult>> funcToExecute);
    }

    /// <summary>
    /// If the table does not exist, it will automatically be created.
    /// </summary>
    public class CloudTablePool //: ITablePool<CloudTable>
    {
        private readonly Task _createTableIfNotExistsTask;

        private readonly CloudTable _table;

        public string TableName { get; }

        /// <summary>
        /// Create access to CloudTables. If the table does not exist, it will automatically be created.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="cloudTableClient"></param>
        /// <remarks>As of the new azure.cosmos.table client sdk v2, pooling and locking isn't necessary anymore. So we provide easy access to tables and reuse a single instance.
        /// https://github.com/Azure/azure-cosmos-dotnet-v2/issues/629
        /// </remarks>
        public CloudTablePool(string tableName, CloudTableClient cloudTableClient)
        {
            TableName                   = tableName;
            _table                      = cloudTableClient.GetTableReference(tableName);

            _createTableIfNotExistsTask = _table.CreateIfNotExistsAsync();

            _createTableIfNotExistsTask.ContinueWith(_ => _createTableIfNotExistsTask.Dispose());
        }

        internal CloudTablePool(string tableName, CloudTable table)
        {
            TableName                   = tableName;
            _table                      = table;

            _createTableIfNotExistsTask = table.CreateIfNotExistsAsync().ContinueWith(task => task.Dispose());
        }

        /// Executed the given function in the configured pool <see cref="IObjectPool{T}.ExecuteOnAvailableObject{TResult}"/>, for easy access.
        [Obsolete("The Table can be accessed directly via the Table property")]
        public TResult Execute<TResult>(Func<CloudTable, TResult> funcToExecute)
        {
            return funcToExecute(Table);
        }

        /// Executed the given function async in the configured pool <see cref="IObjectPool{T}.ExecuteOnAvailableObject{TResult}"/>, for easy access.
        [Obsolete("The Table can be accessed directly via the Table property")]
        public async Task<TResult> ExecuteAsync<TResult>(Func<CloudTable, Task<TResult>> funcToExecute)
        {
            return await funcToExecute(Table);
        }

        public CloudTable Table
        {
            get
            {
                _createTableIfNotExistsTask?.Wait();
                return _table;
            }
        }

        public void Dispose()
        {
            _createTableIfNotExistsTask?.Dispose();
        }
    }
}