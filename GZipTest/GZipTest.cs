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
            _datablocks = new DataBlockQueue(MaxQueueLength);
        }

        public void Compress(string source, string destination)
        {
            // 0. clean up resources from previous usage
            CleanUp();


            // 1. check file existence and availability

            // 2. check possibility of destination file creation

            // 3. for each thread:
            CreateProcessingThreads(() => CompressBlock(source));
            //   --- enter crit section ---
            //   3.1 get position to read
            //   3.2 set position for next thread 
            //   --- crit section leave ---
            //   3.3 read X bytes from current position
            //   3.4 compress calculate new size
            //   3.5 push to queue

            // 4. in main thread until queue is in work:
            //   4.1 pop from queue
            //   4.2 write to file 8 bytes for block index and 8 bytes for block length
            //   4.3 write compressed data

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

        private void CompressBlock(string sourceFile)
        {
            using (var fs = new FileStream(sourceFile, FileMode.Open, FileAccess.Read))
            {
                int filePositionToRead;
                int index;
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
                    if (filePositionToRead >= fs.Length)
                        break; // exit when we reached end of file
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
                    }
                    // TODO: push to queue
                }
            }
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
            _processingThreads.Clear();
            for (int i = 0; i < ThreadNumber; ++i)
            {
                var t = new Thread(() => actionToRun());
                _processingThreads.Add(t);
                t.Start();
            }
        }

        #region Private fields and properties

        private DataBlockQueue _datablocks;

        private List<Thread> _processingThreads;

        private int ThreadNumber { get; } = Math.Max(Environment.ProcessorCount - 1, MinThreadNumber); // try use as many threads as logical processors exsit minus one for main thread

        private int _currentBlockIndex;
        private int _filePosition;
        private static object _filePositionLock = new object();



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
        
        private static readonly int MaxQueueLength = (int)(new Microsoft.VisualBasic.Devices.ComputerInfo().TotalPhysicalMemory / (ulong)BlockSizeOrigin / 2);

        #endregion
    }
}
