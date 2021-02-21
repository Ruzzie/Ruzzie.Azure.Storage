using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Ruzzie.Common.Threading;

namespace Ruzzie.Azure.Storage
{
    /// <summary>
    /// Pool of CloudTables. If the table does not exist, it will automatically be created.
    /// </summary>
    public class CloudTablePool
    {
        public string TableName { get; }
        private readonly Task _createTableIfNotExistsTask;
        private readonly ThreadSafeObjectPool<CloudTable> _pool;

        /// <summary>
        /// Create a new Pool of CloudTables. If the table does not exist, it will automatically be created.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="cloudStorageAccount"></param>
        /// <param name="poolSize">Size of the pool.</param>
        public CloudTablePool(string tableName, CloudStorageAccount cloudStorageAccount, int poolSize = 16)
        {
            TableName = tableName;
            _pool = new ThreadSafeObjectPool<CloudTable>(GetNewTableReference, poolSize);
            _createTableIfNotExistsTask = Pool.ExecuteOnAvailableObject(table => table.CreateIfNotExistsAsync());
            _createTableIfNotExistsTask.ContinueWith(task =>
            {
                _createTableIfNotExistsTask.Dispose();
            });

            CloudTable GetNewTableReference()
            {
                return cloudStorageAccount.CreateCloudTableClient().GetTableReference(tableName);
            }
        }

        public ThreadSafeObjectPool<CloudTable> Pool
        {
            get
            {
                _createTableIfNotExistsTask?.Wait();
                return _pool;
            }
        }
    }
}