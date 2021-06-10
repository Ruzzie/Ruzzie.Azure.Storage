using System;
using FluentAssertions;
using FsCheck;
using Xunit;

namespace Ruzzie.Azure.Storage.UnitTests
{

    public class KeyGeneratorsTests
    {
        public class CreateAlphaNumericKeyTests
        {
            [FsCheck.Xunit.Property]
            public void PropertySmokeTest(NonEmptyString input)
            {
                try
                {
                    var result = input.Item.CreateAlphaNumericPartitionKey();
                    (char.IsLetterOrDigit(result) || result == '-').Should().BeTrue();
                }
                catch (ArgumentException e)
                    when( e.Message.StartsWith("Stripped value has a length of 0. Provide a input string with at least 1 ASCII character"))
                {
                    //This is a valid case: for example: "%^%^#$#$    "
                }
                catch (ArgumentException e)
                    when( e.Message.StartsWith("Value cannot be null or whitespace."))
                {
                    //This is a valid case: only control characters or lots of whitespaces
                    // for example: "\n\r \esc \t    "
                }
            }

            [FsCheck.Xunit.Property]
            public void CalculatePartitionKeyForAlphaNumericRowKeyPropertyTest(NonEmptyString input)
            {
                try
                {
                    //Act
                    var partition = input.Item.CreateAlphaNumericPartitionKey();
                    var samePartition = input.Item
                                             .CreateAlphaNumericKey(KeyGenerators.AlphaNumericKeyGenOptions.None)
                                             .CalculatePartitionKeyForAlphaNumericRowKey();

                    //Assert
                    partition.Should().Match<char>(c => (char.IsLetterOrDigit(c) && char.IsUpper(c)) || char.IsDigit(c) || c == '-');
                    samePartition[0].Should().Be(partition);
                }
                catch (ArgumentException e)
                    when( e.Message.StartsWith("Stripped value has a length of 0. Provide a input string with at least 1 ASCII character"))
                {
                    //This is a valid case: for example: "%^%^#$#$    "
                }
                catch (ArgumentException e)
                    when( e.Message.StartsWith("Value cannot be null or whitespace."))
                {
                    //This is a valid case: only control characters or lots of whitespaces
                    // for example: "\n\r \esc \t    "
                }

            }

            [Fact]
            public void ShouldTrimInput()
            {
                var input = " a";
                var result = input.CreateAlphaNumericPartitionKey();

                result.Should().Be('A');
            }

            [Fact]
            public void ShouldTrimAfterStripInput()
            {
                var input = "_ a      ";
                var result = input.CreateAlphaNumericPartitionKey();

                result.Should().Be('A');
            }

            [InlineData("Acme Inc.  ", "ACME-INC")]
            [InlineData("  8.125 Bri&wst Ncnrp", "8125-BRIWST-NCNRP")]
            [InlineData("Pablo's Ijscobar", "PABLOS-IJSCOBAR")]
            [InlineData("Chiensûr", "CHIENSR")]
            [InlineData("So-de-Jus", "SO-DE-JUS")]
            [Theory]
            public void WithPreserveSpacesAndTrimInput(string companyName, string expected)
            {
                companyName.CreateAlphaNumericKey(KeyGenerators.AlphaNumericKeyGenOptions.PreserveSpacesAsDashes |
                                                  KeyGenerators.AlphaNumericKeyGenOptions.TrimInput).Value.Should().Be(expected);
            }

            [InlineData("Acme Inc.  ",           "ACME-INC")]
            [InlineData("  8.125 Bri&wst Ncnrp", "8125-BRIWST-NCNRP")]
            [InlineData("Pablo's Ijscobar",      "PABLOS-IJSCOBAR")]
            [InlineData("Chiensûr",              "CHIENSR")]
            [InlineData("So-de-Jus",             "SO-DE-JUS")]
            [Theory]
            public void GenerateKeyFromKeyShouldHaveSameResult(string companyName, string expected)
            {
                //Arrange
                var opts = KeyGenerators.AlphaNumericKeyGenOptions
                                                             .PreserveSpacesAsDashes |
                                                KeyGenerators.AlphaNumericKeyGenOptions.TrimInput;
                var key = companyName.CreateAlphaNumericKey(opts);

                //Act
                var derivedKey = key.Value.CreateAlphaNumericKey(opts);

                //Assert
                key.Value.Should().Be(expected, "does not match expected value");
                key.Value.Should().Be(derivedKey.Value, "key of key is not equal, algorithm bug" );
            }

            [FsCheck.Xunit.Property]
            public void WithPreserveSpacesAndTrimInputPropertyTest(NonEmptyString input)
            {
                try
                {
                    var result = input.Item.CreateAlphaNumericPartitionKey(KeyGenerators.AlphaNumericKeyGenOptions.PreserveSpacesAsDashes |
                                                                        KeyGenerators.AlphaNumericKeyGenOptions.TrimInput);
                    (char.IsLetterOrDigit(result) || result == '-').Should().BeTrue();
                }
                catch (ArgumentException e)
                    when( e.Message.StartsWith("Stripped value has a length of 0. Provide a input string with at least 1 ASCII character"))
                {
                    //This is a valid case: for example: "%^%^#$#$    "
                }
                catch (ArgumentException e)
                    when( e.Message.StartsWith("Value cannot be null or whitespace."))
                {
                    //This is a valid case: only control characters or lots of whitespaces
                    // for example: "\n\r \esc \t    "
                }
            }
        }
    }
}