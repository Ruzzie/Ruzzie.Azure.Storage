using FluentAssertions;
using FsCheck.Xunit;

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
}
