using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GZipTest
{
    internal class DataBlockQueue
    {
        public DataBlockQueue(int maxLength)
        {
            _maxLength = maxLength;
        }

        private int _maxLength;
    }
}
