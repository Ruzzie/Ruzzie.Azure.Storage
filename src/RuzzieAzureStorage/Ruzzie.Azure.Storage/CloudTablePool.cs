using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Ruzzie.Common.Threading;

namespace Ruzzie.Azure.Storage
{
    public interface ITablePool<TTable> : IDisposable
    {
        string              TableName { get; }
        IObjectPool<TTable> Pool      { get; }

        /// Executed the given function in the configured pool <see cref="IObjectPool{T}.ExecuteOnAvailableObject{TResult}"/>, for easy access.
        public TResult Execute<TResult>(Func<TTable, TResult> funcToExecute);

        /// Executed the given function async in the configured pool <see cref="IObjectPool{T}.ExecuteOnAvailableObject{TResult}"/>, for easy access.
        public Task<TResult> ExecuteAsync<TResult>(Func<TTable, Task<TResult>> funcToExecute);
    }

    /// <summary>
    /// Pool of CloudTables. If the table does not exist, it will automatically be created.
    /// </summary>
    public class CloudTablePool : ITablePool<CloudTable>
    {
        private readonly Task                         _createTableIfNotExistsTask;
        private readonly SingleObjectPool<CloudTable> _pool;

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
            _pool                       = new SingleObjectPool<CloudTable>(GetNewTableReference());
            _createTableIfNotExistsTask = Pool.ExecuteOnAvailableObject(table => table.CreateIfNotExistsAsync());
            _createTableIfNotExistsTask.ContinueWith(task =>
            {
                _createTableIfNotExistsTask.Dispose();
            });

            CloudTable GetNewTableReference()
            {
                return cloudTableClient.GetTableReference(tableName);
            }
        }

        internal CloudTablePool(string tableName, CloudTable table)
        {
            TableName                   = tableName;
            _pool                       = new SingleObjectPool<CloudTable>(table);
            _createTableIfNotExistsTask = table.CreateIfNotExistsAsync().ContinueWith(t => t.Dispose());
        }

        /// Executed the given function in the configured pool <see cref="IObjectPool{T}.ExecuteOnAvailableObject{TResult}"/>, for easy access.
        public TResult Execute<TResult>(Func<CloudTable,TResult> funcToExecute)
        {
            return Pool.ExecuteOnAvailableObject(funcToExecute);
        }

        /// Executed the given function async in the configured pool <see cref="IObjectPool{T}.ExecuteOnAvailableObject{TResult}"/>, for easy access.
        public async Task<TResult> ExecuteAsync<TResult>(Func<CloudTable, Task<TResult>> funcToExecute)
        {
            return await Pool.ExecuteOnAvailableObject(funcToExecute);
        }

        public IObjectPool<CloudTable> Pool
        {
            get
            {
                _createTableIfNotExistsTask?.Wait();
                return _pool;
            }
        }

        public void Dispose()
        {
            _createTableIfNotExistsTask?.Dispose();
            _pool?.Dispose();
        }
    }
}