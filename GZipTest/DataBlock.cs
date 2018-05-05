using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GZipTest
{
    internal class DataBlock
    {
        public DataBlock(ulong index, int size)
        {
            Size = size;
            Index = index;
        }

        public int Size { get; set; }
        public ulong Index { get; private set; }
        public byte[] Data { get; set; }
    }
}
