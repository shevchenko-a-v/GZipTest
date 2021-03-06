﻿namespace GZipTest
{
    internal class DataBlock
    {
        public DataBlock(long index)
        {
            Index = index;
        }

        public int Size { get { return Data?.Length ?? 0; } }
        public long Index { get; private set; }
        public byte[] Data { get; set; }
    }
}
