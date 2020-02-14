using System;
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


    public class KeyHelperTests
    {
        [Property]
        public void CreateKeyForDateTimeWithPaddingAndReverseOrder_PropertyTests(DateTime dateTime)
        {
            dateTime.CreateKeyForDateTimeWithPaddingAndReverseOrder().Should().NotBeNullOrWhiteSpace();
        }

        [Property]
        public void EncodeInvalidKeyChars_PropertyTests(string input)
        {
            input.EncodeInvalidKeyChars().Should().NotContain("/");
        }

        [Property]
        public void DecodeInvalidKeyChars_PropertyTests(string input)
        {
            input.DecodeInvalidKeyChars().Should().NotContain("_");
        }
    }
}
