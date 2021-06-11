using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace Ruzzie.Azure.Storage
{
    /// <summary>
    /// Can load all rows from an Azure Table, it does this in proper batches
    /// </summary>
    /// <typeparam name="TIn">The type of the entity to use for reading</typeparam>
    /// <typeparam name="TOut">The desired output type.</typeparam>
    /// <seealso cref="IDisposable" />
    public class AzureStorageTableLoader<TIn, TOut> : IDisposable where TIn :  ITableEntity, new()
    {
        private readonly Func<TIn, TOut> _mapEntityFunc;
        private readonly ITablePool<CloudTable> _tablePool;
        private readonly Task _readAllEntitiesTask;
        private ReadOnlyCollection<TOut> _allEntities;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureStorageTableLoader{TIn, TOut}"/> class.
        /// </summary>
        /// <param name="table">The table to load entities from.</param>
        /// <param name="mapEntityFunc">The map entity function, this maps the TIn to TOut</param>
        /// <param name="allPartitionKeys">All partition keys to load</param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <remarks>The table will start to load as soon as this type is constructed.</remarks>
        public AzureStorageTableLoader(CloudTable table, Func<TIn, TOut> mapEntityFunc, IEnumerable<string> allPartitionKeys)
        {
            if (mapEntityFunc == null)
            {
                throw new ArgumentNullException(nameof(mapEntityFunc));
            }
            _mapEntityFunc = mapEntityFunc;

            _tablePool = new CloudTablePool(table.Name, table.ServiceClient);

            _readAllEntitiesTask = Task.Run(async () =>
            {
                _allEntities = await ReadAllEntitiesFromTableStorage(allPartitionKeys);
                //_readAllCardsInitTask = null;
            });
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_tablePool != null)
                {
                    _tablePool.Dispose();
                }
                if (_readAllEntitiesTask != null)
                {
                    _readAllEntitiesTask.Dispose();
                }
            }
        }

        /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureStorageTableLoader{TIn, TOut}"/> class.
        /// </summary>
        /// <param name="tableName">The name of the table to load.</param>
        /// <param name="mapEntityFunc">The map entity function, this maps the TIn to TOut</param>
        /// <param name="allPartitionKeys">All partition keys to load</param>
        /// <param name="connectionString">The connectionstring to the storage account.</param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <remarks>The table will start to load as soon as this type is constructed.</remarks>
        /// <remarks>The table will start to load as soon as this type is constructed.</remarks>
        public AzureStorageTableLoader(string connectionString, string tableName, Func<TIn, TOut> mapEntityFunc, IEnumerable<string> allPartitionKeys)
        {
            if (mapEntityFunc == null)
            {
                throw new ArgumentNullException(nameof(mapEntityFunc));
            }
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(connectionString));
            }
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            }

            _mapEntityFunc = mapEntityFunc;
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            _tablePool = new CloudTablePool(tableName, storageAccount.CreateCloudTableClient());

            _readAllEntitiesTask = Task.Run(async () =>
            {
                _allEntities = await ReadAllEntitiesFromTableStorage(allPartitionKeys);
            });
        }

        private async Task<ReadOnlyCollection<TOut>> ReadAllEntitiesFromTableStorage(IEnumerable<string> allPartitionKeys)
        {
            var res = Task.Run(async () =>
            {
                ConcurrentBag<TOut> allItems = new ConcurrentBag<TOut>();
                var allTasks = new ConcurrentBag<Task>();
                Parallel.ForEach(allPartitionKeys, partitionKey =>
                {
                    allTasks.Add(ReadAllEntitiesForPartitionKey(partitionKey, allItems));
                });

                await Task.WhenAll(allTasks);
                // no more concurrent writes needed so create a List
                return allItems.ToList().AsReadOnly();
            });
            return await res;
        }

        private Task ReadAllEntitiesForPartitionKey(string partitionKey, ConcurrentBag<TOut> listToAddTo)
        {
            return _tablePool.Execute(async table =>
            {
                await CloudTableHelpers.LoopResultSetAndMap(partitionKey, listToAddTo, table, _mapEntityFunc);
            });
        }

        /// <summary>
        /// Gets all cards from the table. The table is read once in the background at the creation of the repository. The data is not refreshed during the lifecycle of this object.
        /// </summary>
        /// <value>
        /// All cards.
        /// </value>
        public ReadOnlyCollection<TOut> AllEntities
        {
            get
            {
                _readAllEntitiesTask?.Wait();
                return _allEntities;
            }
        }
    }
}