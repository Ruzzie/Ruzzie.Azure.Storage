using System;
using System.Globalization;

namespace Ruzzie.Azure.Storage
{
    public static class KeyHelpers
    {
        public static string EncodeInvalidKeyChars(this string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                return input;
            }

            return input.Replace("/", "_");
        }

        public static string DecodeInvalidKeyChars(this string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                return input;
            }

            return input.Replace("_", "/");
        }

        public static string CreateKeyForDateTimeWithPaddingAndReverseOrder(this DateTime dateTime)
        {
            //newest is on top when ordered asc by the string value
            return string.Format(CultureInfo.InvariantCulture, "{0:D19}", (long.MaxValue - dateTime.Ticks));
        }
    }
}
