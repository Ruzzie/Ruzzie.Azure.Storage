using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace Ruzzie.Azure.Storage
{
    public static class TableStorageHelpers
    {
        private static readonly IDictionary<string, EntityProperty> EmptyProperties =
            new ReadOnlyDictionary<string, EntityProperty>(new Dictionary<string, EntityProperty>(0));

        public static T InsertEntity<T>(this CloudTablePool tablePool, T entity) where T : class, ITableEntity
        {
            var insertOp    = TableOperation.Insert(entity, true);
            var tableResult = tablePool.Table.Execute(insertOp);
            return (T)tableResult.Result;
        }

        public static T InsertOrMergeEntity<T>(this CloudTablePool tablePool, T entity) where T : class, ITableEntity
        {
            var insertOp    = TableOperation.InsertOrMerge(entity);
            var tableResult = tablePool.Table.Execute(insertOp);
            return (T)tableResult.Result;
        }

        public static T InsertOrReplaceEntity<T>(this CloudTablePool tablePool, T entity) where T : class, ITableEntity
        {
            var insertOp    = TableOperation.InsertOrReplace(entity);
            var tableResult = tablePool.Table.Execute(insertOp);
            return (T)tableResult.Result;
        }

        public static async Task<T> InsertOrMergeEntityAsync<T>(this CloudTablePool tablePool, T entity)
            where T : class, ITableEntity
        {
            var insertOp    = TableOperation.InsertOrMerge(entity);
            var tableResult = await tablePool.Table.ExecuteAsync(insertOp);

            return (T)tableResult.Result;
        }


        /// Performs a plain merge operation for a given entity where the caller must set the e-tag on the entity
        public static T UpdateEntity<T>(this CloudTablePool tablePool, T entity) where T : class, ITableEntity
        {
            var updateOp = TableOperation.Merge(entity);

            var tableResult = tablePool.Table.Execute(updateOp);
            return (T)tableResult.Result;
        }

        public static T? GetEntity<T>(this CloudTablePool tablePool, string partitionKey, string rowKey)
            where T : ITableEntity, new()
        {
            var filter =
                TableQueryHelpers.CreatePointQueryFilterForPartitionAndRowKey(partitionKey
                                                                            , rowKey);

            var entity = tablePool.Table.ExecuteQuery(new TableQuery<T>().Where(filter))
                                  .FirstOrDefault();

            return entity;
        }

        public static bool TryGetEntity<T>(this CloudTablePool tablePool
                                         , string              partitionKey
                                         , string              rowKey
                                         , out T               entity)
            where T : ITableEntity, new()
        {
            var e = GetEntity<T>(tablePool, partitionKey, rowKey);
            if (e == null)
            {
                entity = new T(); // default
                return false;
            }

            entity = e;
            return true;
        }

        /// Performs a 'hard' delete operation, the e-tag will be set to * (so always delete)
        public static void Delete(this CloudTablePool tablePool, string partitionKey, string rowKey)
        {
            tablePool.Table.Execute(TableOperation.Delete(new DynamicTableEntity(partitionKey
                                                                               , rowKey
                                                                               , "*"
                                                                               , EmptyProperties)));
        }

        public static ReadOnlyCollection<T> GetAllEntitiesInPartition<T>(
            this CloudTablePool tablePool
          , string              partitionKey) where T : ITableEntity, new()
        {
            using var loader = new AzureStorageTableLoader<T, T>(
                                                                 tablePool.Table
                                                               , DefaultMap
                                                               , new[] { partitionKey }
                                                                );
            return loader.AllEntities;


            static T DefaultMap(T val)
            {
                return val;
            }
        }
    }
}