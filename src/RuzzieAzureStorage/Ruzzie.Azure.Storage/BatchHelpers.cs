using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Ruzzie.Azure.Storage
{
    /// <summary>
    /// Helper class for batchoperations.
    /// </summary>
    public static class BatchHelpers
    {
        /// <summary>
        /// Executes a function in batches.
        /// </summary>
        /// <typeparam name="TIn">The type of the in.</typeparam>
        /// <typeparam name="TOut">The type of the out.</typeparam>
        /// <param name="allItems">All items.</param>
        /// <param name="executeOnBatch">The function to execute for each batch..</param>
        /// <param name="mapEachFunc">The map unction to map TIn to TOut. When TOut is null it will be skipped in the batch.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <param name="batchSize">Size of the batch.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">
        /// </exception>
        /// <exception cref="Exception">currentItem is null on index: {i}. numberOfItemsSoFarInCurrentBatch: {entitiesBatch.Count}, allitemsSize: {allItems.Count}-{allItemsCount}");</exception>
        public static async Task ExecuteInBatchesAsync<TIn, TOut>(this IList<TIn> allItems, Func<List<TOut>, Task> executeOnBatch, Func<TIn, TOut> mapEachFunc, CancellationToken cancellationToken = default(CancellationToken), int batchSize = 100)
        {
            if (allItems == null)
            {
                throw new ArgumentNullException(nameof(allItems));
            }
            if (executeOnBatch == null)
            {
                throw new ArgumentNullException(nameof(executeOnBatch));
            }
            if (mapEachFunc == null)
            {
                throw new ArgumentNullException(nameof(mapEachFunc));
            }

            if (batchSize < 1)
            {
                batchSize = 1;
            }

            var entitiesBatch = new List<TOut>(batchSize);
            int allItemsCount = allItems.Count;

            for (int i = 0; i < allItemsCount; i++)
            {
                var currentItem = allItems[i];

                //batch items per batchsize;
                if (i > 0 && i % batchSize == 0)
                {
                    //When batchsize is reached, call execute and create new batch
                    await executeOnBatch(entitiesBatch.ToList());
                    entitiesBatch = new List<TOut>(batchSize);
                }

                if (currentItem == null)
                {
                    throw new Exception($"currentItem is null on index: {i}. numberOfItemsSoFarInCurrentBatch: {entitiesBatch.Count}, allitemsSize: {allItems.Count}-{allItemsCount}");
                }

                var mappedItem = mapEachFunc(currentItem);

                if (mappedItem != null)
                {
                    entitiesBatch.Add(mappedItem);
                }
            }

            //The last batch
            if (entitiesBatch.Count > 0)
            {
                await executeOnBatch(entitiesBatch.ToList());
            }
        }
    }
}