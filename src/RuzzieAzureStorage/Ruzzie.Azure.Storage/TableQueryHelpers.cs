using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;

namespace Ruzzie.Azure.Storage
{
    public static class TableQueryHelpers
    {
        public const string PartitionKeyField = "PartitionKey";
        public const string RowKeyField       = "RowKey";
        public const string OpEquals          = "eq";
        public const string OpAnd             = "and";

        private static readonly IList<string> QuerySelectColumnsPartitionKeyRowKeyOnly =
            new List<string>(new[] { PartitionKeyField, RowKeyField });


        public static bool RowExistsForPartitionKey(this CloudTablePool tablePool, string partitionKey)
        {
            var        queryFilter = TableQuery.GenerateFilterCondition(PartitionKeyField, OpEquals, partitionKey);
            TableQuery query       = new TableQuery();
            query.SelectColumns = QuerySelectColumnsPartitionKeyRowKeyOnly;
            query.FilterString  = queryFilter;


            var result = tablePool.Table.ExecuteQuery(query);
            return result.Any();
        }


        public static bool RowExistsForRowKey(this CloudTablePool tablePool, string rowKey)
        {
            var        queryFilter = TableQuery.GenerateFilterCondition(RowKeyField, OpEquals, rowKey);
            TableQuery query       = new TableQuery();
            query.SelectColumns = QuerySelectColumnsPartitionKeyRowKeyOnly;
            query.FilterString  = queryFilter;

            var result = tablePool.Table.ExecuteQuery(query);
            return result.Any();
        }


        /// <summary>
        /// Executes a point query on the given table and returns true when a record was found, false otherwise.
        /// </summary>
        /// <param name="tablePool">The table to execute the point query against.</param>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>true when a record exists, false otherwise</returns>
        // ReSharper disable once UnusedMember.Global
        public static bool RowExistsForPartitionKeyAndRowKey(this CloudTablePool tablePool
                                                           , string              partitionKey
                                                           , string              rowKey)
        {
            var queryFilter = TableQuery.GenerateFilterCondition(PartitionKeyField, OpEquals, partitionKey);
            queryFilter = TableQuery.CombineFilters(queryFilter
                                                  , OpAnd
                                                  , TableQuery.GenerateFilterCondition(RowKeyField, OpEquals, rowKey));

            TableQuery query = new TableQuery();
            query.SelectColumns = QuerySelectColumnsPartitionKeyRowKeyOnly;
            query.FilterString  = queryFilter;


            var result = tablePool.Table.ExecuteQuery(query);
            return result.Any();
        }

        public static string CreatePointQueryFilterForPartitionAndRowKey(string partitionKey, string rowKey)
        {
            var queryFilter = TableQuery.GenerateFilterCondition(PartitionKeyField, OpEquals, partitionKey);
            queryFilter = TableQuery.CombineFilters(
                                                    queryFilter
                                                  , OpAnd
                                                  , TableQuery.GenerateFilterCondition(RowKeyField, OpEquals, rowKey));
            return queryFilter;
        }
    }
}