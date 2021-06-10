using System;
using System.Collections.Generic;
using Ruzzie.Common;

namespace Ruzzie.Azure.Storage
{
    public static class KeyGenerators
    {
        public static readonly IReadOnlyList<string> AllAlphaNumericPartitions = new[]
        {
            "-","0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
            "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
        };

        [Flags]
        public enum AlphaNumericKeyGenOptions
        {
            None = 0 ,
            TrimInput = 1,
            PreserveSpacesAsDashes = 2
        }

        public static char CreateAlphaNumericPartitionKey(this string value, AlphaNumericKeyGenOptions options = AlphaNumericKeyGenOptions.None)
        {
            var strippedString = CreateAlphaNumericKey(value, options);

            return PartitionForAlphaNumericKey(strippedString);

        }

        public static string CalculatePartitionKeyForAlphaNumericRowKey(this AlphaNumericKey rowKey)
        {
            return PartitionForAlphaNumericKey(rowKey).ToString();
        }

        private static char PartitionForAlphaNumericKey(in AlphaNumericKey key)
        {
            return key.Value[0];
        }

        public static AlphaNumericKey CreateAlphaNumericKey(this string value, AlphaNumericKeyGenOptions options)
        {
            return new AlphaNumericKey(value, options);
        }


        internal static string CreateAlphaNumericKeyString(string value, AlphaNumericKeyGenOptions options)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(value));
            }

            if ((options & AlphaNumericKeyGenOptions.TrimInput) != 0)
            {
                value = value.Trim();
            }

            if ((options & AlphaNumericKeyGenOptions.PreserveSpacesAsDashes) != 0)
            {
                value = value.Replace(' ', '-');
            }

            var strippedString = StringExtensions.StripAlternative(value).ToUpperInvariant().Trim();
            if (strippedString.Length == 0)
            {
                throw new ArgumentException(
                                            "Stripped value has a length of 0. Provide a input string with at least 1 ASCII character",
                                            nameof(value));
            }

            return strippedString;
        }

        /// <summary>
        /// Represents an AlphaNumeric Key string value.
        /// </summary>
        public readonly struct AlphaNumericKey
        {
            private readonly string _value;

            /// <summary>
            /// Generates a new <see cref="AlphaNumericKey"/> value for a given input string.. Contains only uppercase 0-9A-Z and - characters, the rest is stripped according to given options.
            /// </summary>
            /// <param name="value">The string value to generate the key from</param>
            /// <param name="options">additional options </param>
            /// <returns>an uppercase string that only contains the allowed characters</returns>
            /// <exception cref="ArgumentException">When a string is null or empty</exception>
            /// <exception cref="ArgumentException">When a string is empty after removing the disallowed characters</exception>
            public AlphaNumericKey(string value, AlphaNumericKeyGenOptions options)
            {
                _value = CreateAlphaNumericKeyString(value, options);
            }

            /// <summary>
            /// The uppercase value that only contains allowed alpha numeric characters (a-zA-Z0-9 and -)
            /// </summary>
            public string Value => _value;

            /// <summary>
            /// The string value of the <see cref="AlphaNumericKey"/>
            /// </summary>
            /// <param name="key">the <see cref="AlphaNumericKey"/></param>
            /// <returns>the string value</returns>
            public static implicit operator string(AlphaNumericKey key) => key._value;

            public override string ToString()
            {
                return _value;
            }
        }
    }
}