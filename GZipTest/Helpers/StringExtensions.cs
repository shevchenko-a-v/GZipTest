using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GZipTest.Helpers
{
    public static class StringExtensions
    {
        public static bool EqualsNoCase(this string current, string another)
        {
            return current.ToLower() == another.ToLower();
        }
    }
}
