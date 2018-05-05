using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;

namespace GZipTest
{
    public class GZipTest
    {
        public GZipTest()
        {
            _dataBlocksQueue = new DataBlockQueue(MaxQueueLength);
            _processingThreads = new List<Thread>();
        }

        public void Compress(string source, string destination)
        {
            // clean up resources from previous usage
            CleanUp();
            
            // for each thread:
            //   --- enter crit section ---
            //   1 get position to read
            //   2 set position for next thread 
            //   --- crit section leave ---
            //   3 read X bytes from current position
            //   4 compress calculate new size
            //   5 push to queue
            CreateProcessingThreads(() => CompressBlock(source));

            // in main thread until queue is in work:
            //   1 pop from queue
            //   2 write to file 8 bytes for block index and 4 bytes for block length
            //   3 write compressed data
            WriteCompressedBlocks(destination);

        }

        public void Decompress(string source, string destination)
        {
            // 0. clean up resources from previous usage
            CleanUp();

            // 1. check file existence and availability

            // 2. check possibility of destination file creation

            // 3. for each thread:
            //   --- crit section enter ---
            //   3.1 read 8 bytes for block index and 8 bytes for block length
            //   3.2 set position for next thread to read as (curr + length)
            //   --- crit section leave ---
            //   3.3 read compressed data
            //   3.4 decompress data
            //   3.5 push to queue

            // 4. in main thread until queue is in work:
            //   4.1 pop from queue
            //   4.2 write to file decompressed data to the correct position
        }

        #region Private methods

        private void CompressBlock(string sourceFile)
        {
            using (var fs = new FileStream(sourceFile, FileMode.Open, FileAccess.Read))
            {
                int filePositionToRead;
                ulong index;
                int realLength;
                while (true)
                {
                    lock (_filePositionLock)
                    {
                        filePositionToRead = _filePosition;
                        _filePosition += BlockSizeOrigin;
                        index = _currentBlockIndex;
                        _currentBlockIndex++;
                    }
                    byte[] data = new byte[BlockSizeOrigin];
                    fs.Seek(filePositionToRead, SeekOrigin.Begin);
                    realLength = fs.Read(data, 0, BlockSizeOrigin);

                    var block = new DataBlock(index, realLength);
                    using (var compressed = new MemoryStream())
                    {
                        using (var compressor = new GZipStream(compressed, CompressionMode.Compress))
                        {
                            compressor.Write(data, 0, realLength);
                        }
                        block.Data = compressed.ToArray();
                        block.Size = block.Data.Length;
                    }

                    if (filePositionToRead + BlockSizeOrigin >= fs.Length)
                    {
                        lock (_aliveThreadCountLock)
                        {
                            if (--_aliveThreadCount <= 0) // last thread informs queue that we will not add new elements
                            {
                                _dataBlocksQueue.Enqueue(block, true);
                                return;
                            }
                        }
                        break; // exit when we reached end of file
                    }
                    _dataBlocksQueue.Enqueue(block, false);
                }
            }
        }

        private void WriteCompressedBlocks(string destinationFile)
        {
            try
            {
                using (FileStream fs = new FileStream(destinationFile, FileMode.CreateNew, FileAccess.Write))
                {
                    while (true)
                    {
                        var block = _dataBlocksQueue.Dequeue();
                        if (block == null) // returns null when adding completed and queue is empty
                            break;
                        var bytesToWrite = CompressedBlockToByteArray(block);

                        fs.Write(bytesToWrite, 0, bytesToWrite.Length);
                    }
                }

            }
            catch(IOException ex)
            {
                throw new IOException("Destination file already exists. Please remove it and try once more.", ex);
            }
        }

        private byte[] CompressedBlockToByteArray(DataBlock block)
        {
            byte[] ret = new byte[IndexPrefixLength + SizePrefixLength + block.Data.Length];

            var indexArray = BitConverter.GetBytes(block.Index);
            var sizeArray = BitConverter.GetBytes(block.Size);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(sizeArray);
                Array.Reverse(indexArray);
            }
            int destPosition = 0;
            Array.Copy(indexArray, 0, ret, destPosition, indexArray.Length);
            destPosition += IndexPrefixLength;
            Array.Copy(sizeArray, 0, ret, destPosition, sizeArray.Length);
            destPosition += SizePrefixLength;
            Array.Copy(block.Data, 0, ret, destPosition, block.Data.Length);
            return ret;
        }
    
        private void CleanUp()
        {
            _filePosition = 0;
            _currentBlockIndex = 0;
            foreach (var t in _processingThreads)
            {
                t.Abort();
            }
            _processingThreads.Clear();
        }

        private void CreateProcessingThreads(Action actionToRun)
        {
            _aliveThreadCount = ThreadNumber;
            _processingThreads.Clear();
            for (int i = 0; i < ThreadNumber; ++i)
            {
                var t = new Thread(() => actionToRun());
                _processingThreads.Add(t);
                t.Start();
            }
        }

        #endregion

        #region Private fields and properties

        private DataBlockQueue _dataBlocksQueue;

        private List<Thread> _processingThreads;

        private int ThreadNumber { get; } = Math.Max(Environment.ProcessorCount - 1, MinThreadNumber); // try use as many threads as logical processors exsit minus one for main thread

        private ulong _currentBlockIndex;
        private int _filePosition;
        private static object _filePositionLock = new object();

        private int _aliveThreadCount;
        private static object _aliveThreadCountLock = new object();


        #endregion

        #region Private constants

        /// <summary>
        /// Block size used for compression in bytes.
        /// </summary>
        private static readonly int BlockSizeOrigin = 1024 * 1024;

        /// <summary>
        /// Min number of threads which will be used for file processing
        /// </summary>
        private static readonly int MinThreadNumber = 2;

        private static readonly int IndexPrefixLength = 8;
        private static readonly int SizePrefixLength = 4;
        
        private static readonly int MaxQueueLength = (int)(new Microsoft.VisualBasic.Devices.ComputerInfo().TotalPhysicalMemory / (ulong)BlockSizeOrigin / 2);

        #endregion
    }
}
