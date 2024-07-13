using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using FsCheck.Xunit;
using Microsoft.Azure.Cosmos.Table;
using Moq;
using Xunit;
#pragma warning disable xUnit1031
// ReSharper disable InvokeAsExtensionMethod

namespace Ruzzie.Azure.Storage.UnitTests
{
    public class TableQueryHelpersTests
    {
        [Property]
        public void CreatePointQueryFilterForPartitionAndRowKey_PropertyTests(string partitionKey, string rowKey)
        {
           TableQueryHelpers.CreatePointQueryFilterForPartitionAndRowKey(partitionKey, rowKey).Should().NotBeNull();
        }
    }

    public class TableStorageHelpersTests
    {

        [Fact]
        public void InsertEntity_Ok()
        {
            //Arrange
            var tableEntity    = new DynamicTableEntity();
            var cloudTableMock = new Mock<CloudTable>(new Uri("https://nothing.net"), new TableClientConfiguration());
            SetupExecute(cloudTableMock, tableEntity, TableOperationType.Insert);
            var pool = new CloudTablePool("UnitTestTable", cloudTableMock.Object);

            //Act & Assert
            TableStorageHelpers.InsertEntity(pool, tableEntity).Should().Be(tableEntity);
            cloudTableMock.Verify();
        }

        [Fact]
        public void InsertOrMergeEntity_Ok()
        {
            //Arrange
            var tableEntity = new DynamicTableEntity();
            var (cloudTableMock, pool) =
                CreateTablePoolWithMockForExecute(tableEntity, TableOperationType.InsertOrMerge, SetupExecute);

            //Act & Assert
            TableStorageHelpers.InsertOrMergeEntity(pool, tableEntity).Should().Be(tableEntity);
            cloudTableMock.Verify();
        }

        [Fact]
        public void InsertOrReplaceEntity_Ok()
        {
            //Arrange
            var tableEntity = new DynamicTableEntity();
            var (cloudTableMock, pool) =
                CreateTablePoolWithMockForExecute(tableEntity, TableOperationType.InsertOrReplace, SetupExecute);

            //Act & Assert
            TableStorageHelpers.InsertOrReplaceEntity(pool, tableEntity).Should().Be(tableEntity);
            cloudTableMock.Verify();
        }

        [Fact]
        public void InsertOrMergeEntityAsync_Ok()
        {
            //Arrange
            var tableEntity = new DynamicTableEntity();
            var (cloudTableMock, pool) =
                CreateTablePoolWithMockForExecute(tableEntity, TableOperationType.InsertOrMerge, SetupExecuteAsync);

            //Act & Assert
            cloudTableMock.Verify();
            TableStorageHelpers.InsertOrMergeEntityAsync(pool, tableEntity).GetAwaiter().GetResult()
                               .Should()
                               .Be(tableEntity);
        }

        [Fact]
        public void UpdateEntity_Ok()
        {
            //Arrange
            var tableEntity = new DynamicTableEntity();
            tableEntity.ETag = "*";
            var (cloudTableMock, pool) =
                CreateTablePoolWithMockForExecute(tableEntity, TableOperationType.Merge, SetupExecute);

            //Act & Assert
            cloudTableMock.Verify();
            TableStorageHelpers.UpdateEntity(pool, tableEntity)
                               .Should()
                               .Be(tableEntity);
        }

        [Fact]
        public void DeleteEntity_Ok()
        {
            //Arrange
            var tableEntity = new DynamicTableEntity();
            var (cloudTableMock, pool) =
                CreateTablePoolWithMockForExecute(tableEntity, TableOperationType.Delete, SetupExecute);

            //Act & Assert
            cloudTableMock.Verify();
            TableStorageHelpers.Delete(pool, "partitionKey", "rowKey"); // no exceptions
        }

        [Fact]
        public void GetEntity_Ok()
        {
            //Arrange
            var tableEntity = new DynamicTableEntity();
            var (cloudTableMock, pool) =
                CreateTablePoolWithMockForExecuteQuery(tableEntity, SetupExecuteQuery);

            //Act & Assert
            cloudTableMock.Verify();
            TableStorageHelpers.GetEntity<DynamicTableEntity>(pool, "partitionKey", "rowKey")
                               .Should()
                               .Be(tableEntity);
        }

        [Fact(Skip = "Ignore, this is pretty hard to test against the sdk")]
        public void GetAllEntitiesInPartition_Ok()
        {
            //Arrange
            var tableEntity = new DynamicTableEntity();

            var tableQuerySegmentMock = new Mock<TableQuerySegment<DynamicTableEntity>>(new List<DynamicTableEntity> {tableEntity});
            //tableQuerySegmentMock.Setup(s => s.Results).Returns(new List<DynamicTableEntity> {tableEntity});

            var (cloudTableMock, pool) =
                CreateTablePoolWithMockForExecuteQuery(tableEntity, SetupExecuteQuery);
            cloudTableMock.Setup(t => t.ExecuteQuerySegmentedAsync(It.IsAny<TableQuery<DynamicTableEntity>>(),
                                                                   It.IsAny<TableContinuationToken>(),
                                                                   It.IsAny<TableRequestOptions>(),
                                                                   It.IsAny<OperationContext>(),
                                                                   It.IsAny<CancellationToken>()))
                          .Returns(Task.FromResult(tableQuerySegmentMock.Object));

            //Act & Assert
            cloudTableMock.Verify();
            TableStorageHelpers.GetAllEntitiesInPartition<DynamicTableEntity>(pool, "partitionKey")
                               .Should()
                               .Contain(tableEntity);
        }

        private static (Mock<CloudTable> cloudTableMock, CloudTablePool pool ) CreateTablePoolWithMockForExecute(
            DynamicTableEntity                                               entityToReturnInTableResult,
            TableOperationType                                               expectedTableOperation,
            Action<Mock<CloudTable>, DynamicTableEntity, TableOperationType> setupMethod)
        {
            const string tableName = "UnitTestTable";
            var cloudTableMock = new Mock<CloudTable>(new Uri($"https://nothing.net/{tableName}"), new TableClientConfiguration());


            setupMethod(cloudTableMock, entityToReturnInTableResult, expectedTableOperation);

            var pool = new CloudTablePool(tableName, cloudTableMock.Object);
            return (cloudTableMock, pool);
        }

        private static (Mock<CloudTable> cloudTableMock, CloudTablePool pool ) CreateTablePoolWithMockForExecuteQuery(
            DynamicTableEntity                           entityToReturnInTableResult,
            Action<Mock<CloudTable>, DynamicTableEntity> setupMethod)
        {
            const string tableName = "UnitTestTable";
            var cloudTableMock = new Mock<CloudTable>(new Uri($"https://nothing.net/{tableName}"), new TableClientConfiguration());

            setupMethod(cloudTableMock, entityToReturnInTableResult);

            var pool = new CloudTablePool(tableName, cloudTableMock.Object);
            return (cloudTableMock, pool);
        }

        private static void SetupExecute(Mock<CloudTable> cloudTableMock, DynamicTableEntity entityToReturn, TableOperationType expectedTableOperation)
        {
            cloudTableMock.Setup(t =>
                                     t.Execute(
                                               It.Is<TableOperation>(o => o.OperationType == expectedTableOperation),
                                               It.IsAny<TableRequestOptions>(),
                                               It.IsAny<OperationContext>()))
                          .Returns(new TableResult {Result = entityToReturn});
        }

        private static void SetupExecuteQuery(Mock<CloudTable> cloudTableMock, DynamicTableEntity entityToReturn)
        {
            cloudTableMock.Setup(t =>
                                     t.ExecuteQuery(
                                                    It.IsAny<TableQuery<DynamicTableEntity>>(),
                                                    It.IsAny<TableRequestOptions>(),
                                                    It.IsAny<OperationContext>()))
                          .Returns(new[] {entityToReturn});
        }

        private static void SetupExecuteAsync(Mock<CloudTable>   cloudTableMock,
                                              DynamicTableEntity entityToReturn,
                                              TableOperationType expectedTableOperation)
        {
            cloudTableMock.Setup(t =>
                                     t.ExecuteAsync(It.Is<TableOperation>(o => o.OperationType == expectedTableOperation)))
                          .Returns(Task.FromResult(new TableResult {Result = entityToReturn}));
        }
    }


}
