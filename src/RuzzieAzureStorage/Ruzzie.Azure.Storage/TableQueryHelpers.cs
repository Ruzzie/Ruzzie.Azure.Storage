using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;
using Ruzzie.Common.Threading;

namespace Ruzzie.Azure.Storage
{
    public static class TableQueryHelpers
    {
        public const string PartitionKeyField = "PartitionKey";
        public const string RowKeyField = "RowKey";
        public const string OpEquals = "eq";
        public const string OpAnd = "and";
        private static readonly IList<string> QuerySelectColumnsPartitionKeyRowKeyOnly = new List<string>(new[] { PartitionKeyField, RowKeyField });

        [Obsolete("This method is obsolete. Please use the new RowExistsForPartitionKey(ITablePool<CloudTable> tablePool, ..) method",true)]
        public static bool RowExistsForPartitionKey(this IObjectPool<CloudTable> tablePool, string partitionKey)
        {
            var queryFilter = TableQuery.GenerateFilterCondition(PartitionKeyField, OpEquals, partitionKey);
            TableQuery query = new TableQuery();
            query.SelectColumns = QuerySelectColumnsPartitionKeyRowKeyOnly;
            query.FilterString = queryFilter;

            return tablePool.ExecuteOnAvailableObject(table =>
            {
                var result = table.ExecuteQuery(query);
                return result.Any();
            });
        }

        public static bool RowExistsForPartitionKey(this ITablePool<CloudTable> tablePool, string partitionKey)
        {
            var        queryFilter = TableQuery.GenerateFilterCondition(PartitionKeyField, OpEquals, partitionKey);
            TableQuery query       = new TableQuery();
            query.SelectColumns = QuerySelectColumnsPartitionKeyRowKeyOnly;
            query.FilterString  = queryFilter;

            return tablePool.Execute(table =>
            {
                var result = table.ExecuteQuery(query);
                return result.Any();
            });
        }

        [Obsolete("This method is obsolete. Please use the new RowExistsForRowKey(ITablePool<CloudTable> tablePool, ..) method",true)]
        public static bool RowExistsForRowKey(this IObjectPool<CloudTable> tablePool, string rowKey)
        {
            var queryFilter = TableQuery.GenerateFilterCondition(RowKeyField, OpEquals, rowKey);
            TableQuery query = new TableQuery();
            query.SelectColumns = QuerySelectColumnsPartitionKeyRowKeyOnly;
            query.FilterString = queryFilter;

            return tablePool.ExecuteOnAvailableObject(table =>
            {
                var result = table.ExecuteQuery(query);
                return result.Any();
            });
        }

        public static bool RowExistsForRowKey(this ITablePool<CloudTable> tablePool, string rowKey)
        {
            var        queryFilter = TableQuery.GenerateFilterCondition(RowKeyField, OpEquals, rowKey);
            TableQuery query       = new TableQuery();
            query.SelectColumns = QuerySelectColumnsPartitionKeyRowKeyOnly;
            query.FilterString  = queryFilter;

            return tablePool.Execute(table =>
            {
                var result = table.ExecuteQuery(query);
                return result.Any();
            });
        }

        [Obsolete("This method is obsolete. Please use the new RowExistsForRowKey(ITablePool<CloudTable> tablePool, ..) method",true)]
        public static bool RowExistsForPartitionKeyAndRowKey(this IObjectPool<CloudTable> tablePool, string partitionKey, string rowKey)
        {
            var queryFilter = TableQuery.GenerateFilterCondition(PartitionKeyField, OpEquals, partitionKey);
            queryFilter = TableQuery.CombineFilters(queryFilter,OpAnd,TableQuery.GenerateFilterCondition(RowKeyField, OpEquals, rowKey));

            TableQuery query = new TableQuery();
            query.SelectColumns = QuerySelectColumnsPartitionKeyRowKeyOnly;
            query.FilterString = queryFilter;

            return tablePool.ExecuteOnAvailableObject(table =>
            {
                var result = table.ExecuteQuery(query);
                return result.Any();
            });
        }

        public static bool RowExistsForPartitionKeyAndRowKey(this ITablePool<CloudTable> tablePool, string partitionKey, string rowKey)
        {
            var queryFilter = TableQuery.GenerateFilterCondition(PartitionKeyField, OpEquals, partitionKey);
            queryFilter = TableQuery.CombineFilters(queryFilter,OpAnd,TableQuery.GenerateFilterCondition(RowKeyField, OpEquals, rowKey));

            TableQuery query = new TableQuery();
            query.SelectColumns = QuerySelectColumnsPartitionKeyRowKeyOnly;
            query.FilterString  = queryFilter;

            return tablePool.Execute(table =>
            {
                var result = table.ExecuteQuery(query);
                return result.Any();
            });
        }

        public static string CreatePointQueryFilterForPartitionAndRowKey(string partitionKey, string rowKey)
        {
            var queryFilter = TableQuery.GenerateFilterCondition(PartitionKeyField, OpEquals, partitionKey);
            queryFilter = TableQuery.CombineFilters(
                queryFilter,
                OpAnd,
                TableQuery.GenerateFilterCondition(RowKeyField, OpEquals, rowKey));
            return queryFilter;
        }
    }
}
