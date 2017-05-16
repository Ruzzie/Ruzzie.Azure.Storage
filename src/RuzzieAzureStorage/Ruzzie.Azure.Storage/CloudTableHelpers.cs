using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Ruzzie.Common.Threading;

namespace Ruzzie.Azure.Storage
{
    public static class CloudTableHelpers
    {
        public static readonly int DefaultDegreeOfParallelismForFunctions = Environment.ProcessorCount > 4 ? Environment.ProcessorCount - 2 : 2;

        public static async Task<IList<T>> ExecuteQueryAsync<T>(this CloudTable table, TableQuery<T> query,
            CancellationToken cancellationToken = default(CancellationToken)) where T : ITableEntity, new()
        {
            List<T> items = new List<T>();
            TableContinuationToken continuationToken = null;
            
            do
            {
                TableQuerySegment<T> querySegment = await table.ExecuteQuerySegmentedAsync(query, continuationToken, cancellationToken);
                continuationToken = querySegment.ContinuationToken;
                items.AddRange(querySegment);
            } while (continuationToken != null && !cancellationToken.IsCancellationRequested);

            return items;
        }

        public static async Task LoopResultSetAndMap<TOut, TIn>(string partitionKey, ConcurrentBag<TOut> listToAddTo, CloudTable table,
            Func<TIn, TOut> mapEntityFunc) where TIn : ITableEntity, new()
        {
            var query = new TableQuery<TIn>().Where(TableQuery.GenerateFilterCondition("PartitionKey", "eq", partitionKey));
            
            TableContinuationToken continuationToken = null;
            CancellationToken cancellationToken = default(CancellationToken);

            do
            {
                TableQuerySegment<TIn> querySegment = await table.ExecuteQuerySegmentedAsync(query, continuationToken, cancellationToken);
                continuationToken = querySegment.ContinuationToken;
                MapItemsAndAddToList(querySegment.Results, listToAddTo, mapEntityFunc);
            } while (continuationToken != null && !cancellationToken.IsCancellationRequested);
        }

        private static void MapItemsAndAddToList<TIn, TOut>(IReadOnlyList<TIn> itemsToMap, ConcurrentBag<TOut> listToAddTo, Func<TIn, TOut> mapEntityFunc)
            where TIn : ITableEntity, new()
        {
            int numberOfItems = itemsToMap.Count;
            Parallel.For(0, numberOfItems, i => { listToAddTo.Add(mapEntityFunc(itemsToMap[i])); });
        }

        public static int ExecuteInsertOrMergeInBatches<TEntity>(List<TEntity> allEntitiesToInsert, CloudTable tableToInsertTo) where TEntity : ITableEntity
        {
            int batchSize = 100;

            int numberOfItems = allEntitiesToInsert.Count;

            int numberOfBatches = (int) Math.Ceiling((double) numberOfItems / batchSize);
            int resultCount = 0;
            for (int i = 0; i < numberOfBatches; i++)
            {
                var batch = allEntitiesToInsert.Skip(i * batchSize).Take(batchSize);

                TableBatchOperation op = new TableBatchOperation();
                batch.ToList().ForEach(entity => op.InsertOrMerge(entity));
                resultCount += tableToInsertTo.ExecuteBatch(op).Count;
            }
            return resultCount;
        }

        public static int ExecuteDeleteInBatches<TEntity>(List<TEntity> allEntitiesToDelete, CloudTable tableToDeleteFrom) where TEntity : ITableEntity
        {
            int batchSize = 100;

            int numberOfItems = allEntitiesToDelete.Count;

            int numberOfBatches = (int)Math.Ceiling((double)numberOfItems / batchSize);
            int resultCount = 0;
            for (int i = 0; i < numberOfBatches; i++)
            {
                var batch = allEntitiesToDelete.Skip(i * batchSize).Take(batchSize);

                TableBatchOperation op = new TableBatchOperation();
                batch.ToList().ForEach(entity => op.Delete(entity));
                resultCount += tableToDeleteFrom.ExecuteBatch(op).Count;
            }
            return resultCount;
        }

        /// <summary>
        /// Executes the insert or merge in batches asynchronous and will automatically group inserts by partitionkey.
        /// </summary>
        /// <typeparam name="TEntityIn">The type of the entity in.</typeparam>
        /// <typeparam name="TEntityOut">The type of the entity out.</typeparam>
        /// <param name="allEntitiesToInsert">All entities to insert.</param>
        /// <param name="map">The map function.</param>
        /// <param name="tableToInsertTo">The table to insert to.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The total number of operations.</returns>
        public static async Task<int> ExecuteInsertOrMergeInBatchesAsync<TEntityIn, TEntityOut>(
            List<TEntityIn> allEntitiesToInsert,
            Func<TEntityIn, TEntityOut> map,
            CloudTable tableToInsertTo,
            CancellationToken cancellationToken = default(CancellationToken)) where TEntityOut : ITableEntity
        {
            var cloudTablePoolTask = Task.Run(() =>
            {
                CloudStorageAccount storageAccount = new CloudStorageAccount(tableToInsertTo.ServiceClient.Credentials, true);
                var currentTableName = tableToInsertTo.Name;
                return new ThreadSafeObjectPool<CloudTable>(() => storageAccount.CreateCloudTableClient().GetTableReference(currentTableName), DefaultDegreeOfParallelismForFunctions);
            }, cancellationToken);

            //Group and map
            var partitionKeyGroups = GroupByPartitionKeyAndMap(allEntitiesToInsert, map);

            int totalNumberOfOperations = await ExecuteParallelInBatches(cloudTablePoolTask, partitionKeyGroups, cancellationToken, DefaultDegreeOfParallelismForFunctions);

            return totalNumberOfOperations;
        }

        private static async Task<int> ExecuteParallelInBatches<TEntityOut>(
            Task<ThreadSafeObjectPool<CloudTable>> cloudTablePoolTask,
            ConcurrentDictionary<string, List<TEntityOut>> partitionKeyGroups,
            CancellationToken cancellationToken,
            int poolSize) where TEntityOut : ITableEntity
        {
            int totalNumberOfOperations = 0;

            //Execute each partition in batches
            var cloudTablePool = await cloudTablePoolTask;
            ParallelOptions options = new ParallelOptions {MaxDegreeOfParallelism = poolSize, CancellationToken = cancellationToken};

            Parallel.ForEach(partitionKeyGroups, options, grp =>
            {
                //Yes i know, this isnt async, doesnt need to , needs to be parallel.
                var count = cloudTablePool.ExecuteOnAvailableObject(
                    table => ExecuteInsertOrMergeInBatchesAsyncInSinglePartition(grp.Value, table, cancellationToken, 100).Result);
                // ReSharper disable once AccessToModifiedClosure
                Interlocked.Add(ref totalNumberOfOperations, count);
            });
            return totalNumberOfOperations;
        }

        private static ConcurrentDictionary<string, List<TEntityOut>> GroupByPartitionKeyAndMap<TEntityIn, TEntityOut>(IReadOnlyList<TEntityIn> allEntitiesToInsert, Func<TEntityIn, TEntityOut> map) where TEntityOut : ITableEntity
        {
            ConcurrentDictionary<string, List<TEntityOut>> partitionKeyGroups = new ConcurrentDictionary<string, List<TEntityOut>>();
            int totalNumberOfEntititiesToInsert = allEntitiesToInsert.Count;

            for (int i = 0; i < totalNumberOfEntititiesToInsert; i++)
            {
                var entityToInsert = map(allEntitiesToInsert[i]);

                partitionKeyGroups.AddOrUpdate(entityToInsert.PartitionKey,
                    //Add
                    new List<TEntityOut> { entityToInsert },
                    //Update
                    (key, partitionList) =>
                    {
                        partitionList.Add(entityToInsert);
                        return partitionList;
                    });
            }
           
            return partitionKeyGroups;
        }

        /// <summary>
        /// Executes the insert or merge in batches asynchronous and will automatically group inserts by partitionkey.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="allEntitiesToInsert">All entities to insert.</param>
        /// <param name="tableToInsertTo">The table to insert to.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The number of executed operations.</returns>
        public static async Task<int> ExecuteInsertOrMergeInBatchesAsync<TEntity>(List<TEntity> allEntitiesToInsert, CloudTable tableToInsertTo,
            CancellationToken cancellationToken = default(CancellationToken)) where TEntity : ITableEntity
        {
            return await ExecuteInsertOrMergeInBatchesAsync(allEntitiesToInsert,entity => entity, tableToInsertTo, cancellationToken);         
        }

        private static async Task<int> ExecuteInsertOrMergeInBatchesAsyncInSinglePartition<TEntity>(
            IList<TEntity> allEntitiesToInsertInPartition,
            CloudTable tableToInsertTo,
            CancellationToken cancellationToken, int batchSize = 100) where TEntity : ITableEntity
        {
            int totalCount = 0;
            await allEntitiesToInsertInPartition.ExecuteInBatchesAsync
                (async itemsInBatch =>
                {
                    var r = await ExecuteBatchAsync(tableToInsertTo, itemsInBatch, cancellationToken);
                    totalCount += r.Count;
                },
                CreateInsertOrMergeOperationForEntity, cancellationToken,
                batchSize);

            return totalCount;
        }

        private static async Task<IList<TableResult>> ExecuteBatchAsync(CloudTable tableToInsertTo, IReadOnlyList<TableOperation> allOperations, CancellationToken cancellationToken)
        {
            TableBatchOperation op = new TableBatchOperation();
            int allOperationsCount = allOperations.Count;

            for (int i = 0; i < allOperationsCount; i++)
            {
                op.Add(allOperations[i]);
            }
            return await tableToInsertTo.ExecuteBatchAsync(op, cancellationToken);
        }

        private static TableOperation CreateInsertOrMergeOperationForEntity<TEntity>(TEntity entity) where TEntity : ITableEntity
        {
            return TableOperation.InsertOrMerge(entity);
        }
    }
}