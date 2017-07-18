using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using FluentAssertions;

namespace Ruzzie.Azure.Storage.UnitTests
{
    public class BatchHelpersTests
    {
        private static async Task TestBatch(IReadOnlyCollection<string> batch, int batchSize, List<string> allItemsFromBatches)
        {
            await Task.Run(() =>
            {
                allItemsFromBatches.AddRange(batch);
                batch.Count.Should().BeGreaterThan(0).And.BeLessOrEqualTo(batchSize);
            });
        }

        [Fact]
        public void SmokeTest()
        {
            List<string> allItems = new List<string> {"1", "2", "3", "4", "5"};
            var batchSize = 2;
            List<string> allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestBatch(batch, batchSize, allItemsFromBatches), s => "mapped" + s, batchSize: batchSize).Wait();

            allItemsFromBatches.Count.Should().Be(allItems.Count);
        }

        [Fact]
        public void AllItemsSmallerThanBatchSize()
        {
            List<string> allItems = new List<string> { "1", "2", "3", "4", "5" };
            var batchSize = 100;
            List<string> allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestBatch(batch, batchSize, allItemsFromBatches), s => "mapped" + s, batchSize: batchSize).Wait();

            allItemsFromBatches.Count.Should().Be(allItems.Count);
        }

        [Fact]
        public void MapFunctionIsCalled()
        {
            List<string> allItems = new List<string> { "1", "2", "3", "4", "5" };
            var batchSize = 100;
            List<string> allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestBatch(batch, batchSize, allItemsFromBatches), s => "mapped" + s, batchSize: batchSize).Wait();

            foreach (string mappedString in allItemsFromBatches)
            {
                mappedString.StartsWith("mapped").Should().BeTrue();
            }
        }

        [Fact]
        public void BatchSizeOfOneShouldSucceeed()
        {
            List<string> allItems = new List<string> { "1", "2", "3", "4", "5" };
            var batchSize = 1;
            List<string> allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestBatch(batch, batchSize, allItemsFromBatches), s => "mapped" + s, batchSize: batchSize).Wait();

            foreach (string mappedString in allItemsFromBatches)
            {
                mappedString.StartsWith("mapped").Should().BeTrue();
            }
        }

        [Fact]
        public void WhenMapFunctionRetursNullSkipItem()
        {
            List<string> allItems = new List<string> { "1", "2", "3", "4", "5" };
            var batchSize = 5;
            List<string> allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestBatch(batch, batchSize, allItemsFromBatches), s =>
            {
                if (s == "2")
                {
                    return null;
                }
                return "mapped" + s;
            }, batchSize: batchSize).Wait();

            foreach (string mappedString in allItemsFromBatches)
            {
                mappedString.StartsWith("mapped").Should().BeTrue();
            }
            allItemsFromBatches.Count.Should().Be(4);
        }
    }
}