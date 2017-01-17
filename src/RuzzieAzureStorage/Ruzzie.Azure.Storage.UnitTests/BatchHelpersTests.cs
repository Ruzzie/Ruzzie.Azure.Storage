using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Ruzzie.Azure.Storage.UnitTests
{
    [TestFixture]
    public class BatchHelpersTests
    {
        private async Task TestBatch(IReadOnlyCollection<string> batch, int batchSize, List<string> allItemsFromBatches)
        {
            await Task.Run(() =>
            {
                allItemsFromBatches.AddRange(batch);
                Assert.That(batch.Count, Is.LessThanOrEqualTo(batchSize).And.GreaterThan(0));
            });
        }

        [Test]
        public void SmokeTest()
        {
            List<string> allItems = new List<string> {"1", "2", "3", "4", "5"};
            var batchSize = 2;
            List<string> allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestBatch(batch, batchSize, allItemsFromBatches), s => "mapped" + s, batchSize: batchSize).Wait();

            Assert.That(allItemsFromBatches.Count, Is.EqualTo(allItems.Count));
        }

        [Test]
        public void AllItemsSmallerThanBatchSize()
        {
            List<string> allItems = new List<string> { "1", "2", "3", "4", "5" };
            var batchSize = 100;
            List<string> allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestBatch(batch, batchSize, allItemsFromBatches), s => "mapped" + s, batchSize: batchSize).Wait();

            Assert.That(allItemsFromBatches.Count, Is.EqualTo(allItems.Count));
        }

        [Test]
        public void MapFunctionIsCalled()
        {
            List<string> allItems = new List<string> { "1", "2", "3", "4", "5" };
            var batchSize = 100;
            List<string> allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestBatch(batch, batchSize, allItemsFromBatches), s => "mapped" + s, batchSize: batchSize).Wait();

            foreach (string mappedString in allItemsFromBatches)
            {
                Assert.That(mappedString.StartsWith("mapped"), Is.True);
            }
        }

        [Test]
        public void BatchSizeOfOneShouldSucceeed()
        {
            List<string> allItems = new List<string> { "1", "2", "3", "4", "5" };
            var batchSize = 1;
            List<string> allItemsFromBatches = new List<string>();

            allItems.ExecuteInBatchesAsync(batch => TestBatch(batch, batchSize, allItemsFromBatches), s => "mapped" + s, batchSize: batchSize).Wait();

            foreach (string mappedString in allItemsFromBatches)
            {
                Assert.That(mappedString.StartsWith("mapped"), Is.True);
            }
        }
    }
}