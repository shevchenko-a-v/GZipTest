using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;

namespace GZipTest
{
    public delegate void ReportProgressDelegate(int current, int total);

    public class GZipTest
    {
        public GZipTest(ReportProgressDelegate report = null)
        {
            if (report != null)
                _report = report;
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
            CreateProcessingThreads(() => ReadAndCompressBlock(source));
           // ReadAndCompressBlock(source);

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
            CreateProcessingThreads(() => ReadAndDecompressBlock(source));
            //ReadAndDecompressBlock(source);
            // 4. in main thread until queue is in work:
            //   4.1 pop from queue
            //   4.2 write to file decompressed data to the correct position
            WriteDecompressedBlocks(destination);
        }

        #region Private methods

        private void ReadAndCompressBlock(string sourceFile)
        {
            using (var fs = new FileStream(sourceFile, FileMode.Open, FileAccess.Read))
            {
                while (true)
                {
                    int filePositionToRead;
                    ulong index;
                    lock (_filePositionLock)
                    {
                        if (_totalTasks == 0)
                            _totalTasks = (int)Math.Ceiling((double)fs.Length / BlockSizeOrigin);
                        filePositionToRead = _filePosition;
                        if (filePositionToRead >= fs.Length)
                        {
                            lock (_aliveThreadCountLock)
                            {
                                if (--_aliveThreadCount <= 0) // last thread informs queue that we will not add new elements
                                {
                                    _dataBlocksQueue.CompleteAdding();
                                }
                            }
                            return; // exit when we reached end of file
                        }
                        _filePosition += BlockSizeOrigin;
                        index = _currentBlockIndex;
                        _currentBlockIndex++;
                    }
                    byte[] data = new byte[BlockSizeOrigin];
                    fs.Seek(filePositionToRead, SeekOrigin.Begin);
                    var realLength = fs.Read(data, 0, BlockSizeOrigin);

                    var block = new DataBlock(index);
                    using (var compressed = new MemoryStream())
                    {
                        using (var compressor = new GZipStream(compressed, CompressionMode.Compress, true))
                        {
                            compressor.Write(data, 0, realLength);
                        }
                        block.Data = compressed.ToArray();
                    }

                    _dataBlocksQueue.Enqueue(block);
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
                        ReportProgress();
                    }
                }

            }
            catch(IOException ex)
            {
                throw new IOException("Destination file already exists. Please remove it and try once more.", ex);
            }
        }

        private void WriteDecompressedBlocks(string destinationFile)
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

                        fs.Seek((long)(block.Index * (ulong)BlockSizeOrigin), SeekOrigin.Begin);
                        fs.Write(block.Data, 0, block.Size);
                        ReportProgress();
                    }
                }

            }
            catch (IOException ex)
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

        private void ReadAndDecompressBlock(string sourceFile)
        {
            using (var fs = new FileStream(sourceFile, FileMode.Open, FileAccess.Read))
            {
                while (true)
                {
                    int filePositionToRead;
                    ulong index;
                    int blockSize;
                    lock (_filePositionLock)
                    {
                        if (_totalTasks == 0)
                            _totalTasks = GetBlocksCount(fs);
                        // read index of block, its size and set _filePosition to next block
                        filePositionToRead = _filePosition;
                        if (filePositionToRead >= fs.Length)
                        {
                            lock (_aliveThreadCountLock)
                            {
                                if (--_aliveThreadCount <= 0) // last thread informs queue that we will not add new elements
                                {
                                    _dataBlocksQueue.CompleteAdding();
                                }
                            }
                            return; // exit when we reached end of file}
                        }

                        byte[] indexArray = new byte[IndexPrefixLength];
                        byte[] sizeArray = new byte[SizePrefixLength];
                        fs.Seek(filePositionToRead, SeekOrigin.Begin);
                        var realIndexLength = fs.Read(indexArray, 0, IndexPrefixLength);
                        var realSizeLength = fs.Read(sizeArray, 0, SizePrefixLength);
                        if (realIndexLength != IndexPrefixLength || realSizeLength != SizePrefixLength)
                            throw new Exception("Unsupported format of decompressing file");
                        if (BitConverter.IsLittleEndian)
                        {
                            Array.Reverse(indexArray);
                            Array.Reverse(sizeArray);
                        }
                        index = BitConverter.ToUInt64(indexArray, 0);
                        blockSize = BitConverter.ToInt32(sizeArray, 0);

                        _filePosition += IndexPrefixLength + SizePrefixLength + blockSize;
                    }
                    byte[] data = new byte[blockSize];
                    if (fs.Read(data, 0, blockSize) != blockSize)
                        throw new Exception("Unsupported format of decompressing file");
                    var block = new DataBlock(index);
                    byte[] decompressedData = new byte[blockSize];

                    using (var src = new MemoryStream(data))
                    using (var decompressor = new GZipStream(src, CompressionMode.Decompress))
                    using (MemoryStream decompressed = new MemoryStream())
                    {
                        CopyStream(decompressor, decompressed);
                        block.Data = decompressed.ToArray();
                    }
                    if (block.Size != BlockSizeOrigin)
                        _wrongSizeBlocks++;
                    if (_wrongSizeBlocks > 1)
                        throw new Exception("Unsupported format of decompressing file");
                    _dataBlocksQueue.Enqueue(block);
                }
            }
        }

        private byte[] CompressData(byte[] data, int length, CompressionMode mode)
        {
            using (var compressed = new MemoryStream())
            {
                using (var compressor = new GZipStream(compressed, mode))
                {
                    compressor.Write(data, 0, length);
                }
                return compressed.ToArray();
            }
        }

        private static void CopyStream(Stream input, Stream output)
        {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = input.Read(buffer, 0, buffer.Length)) > 0)
            {
                output.Write(buffer, 0, bytesRead);
            }
        }

        private int GetBlocksCount(Stream stream)
        {
            if (stream == null || !stream.CanRead)
            {
                return 0;
            }
            int cnt = 0;
            stream.Position = IndexPrefixLength;
            byte[] size = new byte[SizePrefixLength];
            var readBytes = stream.Read(size, 0, SizePrefixLength);
            while (readBytes != 0)
            {
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(size);
                var curBlockSize = BitConverter.ToInt32(size, 0);
                cnt++;
                if (stream.Position + curBlockSize + IndexPrefixLength >= stream.Length)
                    return cnt;
                stream.Position += curBlockSize + IndexPrefixLength;
                readBytes = stream.Read(size, 0, SizePrefixLength);
            }
            return cnt;
        }

        private void CleanUp()
        {
            _totalTasks = 0;
            _currentTask = 0;
            _wrongSizeBlocks = 0;
            _filePosition = 0;
            _currentBlockIndex = 0;
            foreach (var t in _processingThreads)
            {
                if (t.IsAlive)
                    t.Abort();
            }
            _processingThreads.Clear();
            _aliveThreadCount = 0;
        }

        private void ReportProgress()
        {
            ++_currentTask;
            if (_report != null)
                _report(_currentTask, _totalTasks);
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

        #region Progress report
        ReportProgressDelegate _report;
        private int _totalTasks;
        private int _currentTask;
        #endregion Progress report

        private int ThreadNumber { get; } = Math.Max(Environment.ProcessorCount - 1, MinThreadNumber); // try use as many threads as logical processors exsit minus one for main thread

        private ulong _currentBlockIndex;
        private int _filePosition;
        private static object _filePositionLock = new object();

        private int _aliveThreadCount;
        private static object _aliveThreadCountLock = new object();

        private int _wrongSizeBlocks; // only one block during decompressing can be not equal to BlockSizeOrigin - the last one in origin file

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
