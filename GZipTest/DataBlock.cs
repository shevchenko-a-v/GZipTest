using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GZipTest
{
    internal class DataBlock
    {
        public DataBlock(ulong index)
        {
            Index = index;
        }

        public int Size { get { return Data?.Length ?? 0; } }
        public ulong Index { get; private set; }
        public byte[] Data { get; set; }
    }
}
