using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using FluentAssertions;
#pragma warning disable xUnit1031

namespace Ruzzie.Azure.Storage.UnitTests
{
    public class BatchHelpersTests
    {
        private static async Task TestDoSomethingForBatch(IReadOnlyCollection<string> batch,
                                                          int                         batchSize,
                                                          List<string>                allItemsFromBatches)
        {
            await Task.Run(() =>
            {
                allItemsFromBatches.AddRange(batch);
                batch.Count.Should().BeGreaterThan(0).And.BeLessOrEqualTo(batchSize);
            });
        }

        [Fact]
        public async Task SmokeTest()
        {
            var allItems            = new List<string> {"1", "2", "3", "4", "5"};
            var batchSize           = 2;
            var allItemsFromBatches = new List<string>();

            await
                allItems.ExecuteInBatchesAsync(batch => TestDoSomethingForBatch(batch, batchSize, allItemsFromBatches),
                                               s => s,
                                               batchSize: batchSize);

            allItemsFromBatches.Should().BeEquivalentTo(allItems);
        }

        [Fact]
        public void LargeSetTest()
        {
            //Arrange
            var totalItemCount = 19997;
            var allItems       = new List<int>(totalItemCount);
            for (var i = 0; i < totalItemCount; i++)
            {
                allItems.Add(i);
            }

            var itemCount = 0;
            //Act
            allItems.ExecuteInBatchesAsync(batch =>
            {
                return Task.Run(() =>
                {
                    batch.Count.Should().BeLessOrEqualTo(100);
                    itemCount = itemCount + batch.Count;
                });
            }, i => i).Wait();

            //Assert
            itemCount.Should().Be(19997);
        }

        [Fact]
        public void AllItemsSmallerThanBatchSize()
        {
            var allItems            = new List<string> {"1", "2", "3", "4", "5"};
            var batchSize           = 100;
            var allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestDoSomethingForBatch(batch, batchSize, allItemsFromBatches),
                                           s => "mapped" + s,
                                           batchSize: batchSize).Wait();

            allItemsFromBatches.Count.Should().Be(allItems.Count);
        }

        [Fact]
        public void MapFunctionIsCalled()
        {
            var allItems            = new List<string> {"1", "2", "3", "4", "5"};
            var batchSize           = 100;
            var allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestDoSomethingForBatch(batch, batchSize, allItemsFromBatches),
                                           s => "mapped" + s, batchSize: batchSize).Wait();

            foreach (var mappedString in allItemsFromBatches)
            {
                mappedString.StartsWith("mapped").Should().BeTrue();
            }
        }

        [Fact]
        public void BatchSizeOfOneShouldSucceed()
        {
            var allItems            = new List<string> {"1", "2", "3", "4", "5"};
            var batchSize           = 1;
            var allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestDoSomethingForBatch(batch, batchSize, allItemsFromBatches),
                                           s => "mapped" + s, batchSize: batchSize).Wait();

            foreach (var mappedString in allItemsFromBatches)
            {
                mappedString.StartsWith("mapped").Should().BeTrue();
            }
        }

        [Fact]
        public void WhenMapFunctionRetursNullSkipItem()
        {
            var allItems            = new List<string> {"1", "2", "3", "4", "5"};
            var batchSize           = 5;
            var allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestDoSomethingForBatch(batch, batchSize, allItemsFromBatches), s =>
            {
                if (s == "2")
                {
                    return null;
                }

                return "mapped" + s;
            }, batchSize: batchSize).Wait();

            foreach (var mappedString in allItemsFromBatches)
            {
                mappedString.StartsWith("mapped").Should().BeTrue();
            }

            allItemsFromBatches.Count.Should().Be(4);
        }
    }
}