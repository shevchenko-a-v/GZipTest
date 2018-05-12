using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace GZipTest
{
    public delegate void ReportProgressDelegate(int current, int total);

    public class GZipTest
    {
        public GZipTest(Action<Exception> threadExceptionHandler, ReportProgressDelegate report = null)
        {
            _report = report;
            _threadExceptionHandler = threadExceptionHandler;
            _processedBlocksQueue = new ThreadSafeQueue<DataBlock>(MaxQueueLength);
            _prepearedBlocksQueue = new ThreadSafeQueue<DataBlock>(MaxQueueLength);
            _processingThreads = new List<Thread>();
        }

        /// <summary>
        /// Compresses <paramref name="source"/> file and writes result to <paramref name="destination"/>
        /// </summary>
        /// <param name="source">Path to the source file</param>
        /// <param name="destination">Path to the destination file</param>
        public void Compress(string source, string destination)
        {
            try
            {
                CreateProcessingThreads(() => ReadBlocksToCompress(source), CompressBlocks);
                WriteBlocks(destination, CompressionMode.Compress);
            }
            finally
            {
                CleanUp();
            }

        }

        /// <summary>
        /// Decompresses <paramref name="source"/> file and writes result to <paramref name="destination"/>
        /// </summary>
        /// <param name="source">Path to the source file</param>
        /// <param name="destination">Path to the destination file</param>
        public void Decompress(string source, string destination)
        {
            try
            {
                CreateProcessingThreads(() => ReadBlocksToDecompress(source), DecompressBlocks);
                WriteBlocks(destination, CompressionMode.Decompress);
            }
            finally
            {
                CleanUp();
            }
        }

        #region Private methods for Compression

        /// <summary>
        /// Read data by blocks from <paramref name="sourceFile"/> and push to _prepearedBlocksQueue
        /// </summary>
        /// <param name="sourceFile">Path to source file</param>
        private void ReadBlocksToCompress(string sourceFile)
        {
            using (var fs = new FileStream(sourceFile, FileMode.Open, FileAccess.Read))
            {
                // calculate total tasks to report
                if (_totalTasks == 0)
                    _totalTasks = (int)Math.Ceiling((double)fs.Length / BlockSizeOrigin);

                int index = 0;
                byte[] data = new byte[BlockSizeOrigin];
                int readBytes = fs.Read(data, 0, BlockSizeOrigin);
                while (readBytes > 0)
                {
                    var block = new DataBlock(index++);
                    block.Data = new byte[readBytes];
                    Array.Copy(data, block.Data, readBytes);
                    _prepearedBlocksQueue.Enqueue(block);
                    readBytes = fs.Read(data, 0, BlockSizeOrigin);
                }
                _prepearedBlocksQueue.CompleteAdding();
            }
        }

        /// <summary>
        /// Take block from _prepearedBlocksQueue, compress it and push to _processedBlocksQueue
        /// </summary>
        private void CompressBlocks()
        {
            try
            {
                while (_prepearedBlocksQueue.IsAlive)
                {
                    var block = _prepearedBlocksQueue.Dequeue();
                    // compress and push to queue
                    using (var compressed = new MemoryStream())
                    {
                        using (var compressor = new GZipStream(compressed, CompressionMode.Compress, true))
                        {
                            compressor.Write(block.Data, 0, block.Size);
                        }
                        block.Data = compressed.ToArray();
                    }
                    _processedBlocksQueue.Enqueue(block);
                }
            }
            finally
            {
                lock (_aliveThreadCountLock)
                {
                    if (--_aliveProcessingThreadCount <= 0) // last thread informs queue that we will not add new elements
                    {
                        _processedBlocksQueue.CompleteAdding();
                    }
                }
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

        #endregion

        #region Private methods for Decompression

        /// <summary>
        /// Read data by blocks from <paramref name="sourceFile"/>
        /// </summary>
        /// <param name="sourceFile">Path to source file</param>
        private void ReadBlocksToDecompress(string sourceFile)
        {
            using (var fs = new FileStream(sourceFile, FileMode.Open, FileAccess.Read))
            {
                // calculate total tasks to report
                if (_totalTasks == 0)
                {
                    _totalTasks = GetBlocksCount(fs);
                    fs.Seek(0, SeekOrigin.Begin); // GetBlockCount changed position -> reset it
                }

                // read index of block, its size and set _filePosition to next block
                byte[] indexArray = new byte[IndexPrefixLength];
                byte[] sizeArray = new byte[SizePrefixLength];

                var realIndexLength = fs.Read(indexArray, 0, IndexPrefixLength);
                var realSizeLength = fs.Read(sizeArray, 0, SizePrefixLength);
                while (realIndexLength > 0 && realSizeLength > 0)
                {
                    // if we cannot read required bytes something went wrong
                    if (realIndexLength != IndexPrefixLength || realSizeLength != SizePrefixLength)
                        throw new Exception("Unsupported format of decompressing file");
                    if (BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(indexArray);
                        Array.Reverse(sizeArray);
                    }
                    var index = BitConverter.ToInt64(indexArray, 0);
                    var blockSize = BitConverter.ToInt32(sizeArray, 0);

                    var block = new DataBlock(index);
                    block.Data = new byte[blockSize];
                    // read data
                    var readBytes = fs.Read(block.Data, 0, blockSize);
                    if (readBytes != blockSize) // if we cannot read required bytes something went wrong
                        throw new Exception("Unsupported format of decompressing file");
                    // push to queue
                    _prepearedBlocksQueue.Enqueue(block);

                    realIndexLength = fs.Read(indexArray, 0, IndexPrefixLength);
                    realSizeLength = fs.Read(sizeArray, 0, SizePrefixLength);
                }
                _prepearedBlocksQueue.CompleteAdding();                
            }
        }

        /// <summary>
        /// Take block from _prepearedBlocksQueue, decompress it and push to _processedBlocksQueue
        /// </summary>
        private void DecompressBlocks()
        {
            try
            {
                while (_prepearedBlocksQueue.IsAlive)
                {
                    var block = _prepearedBlocksQueue.Dequeue();
                    // decompress
                    using (var src = new MemoryStream(block.Data))
                    using (var decompressor = new GZipStream(src, CompressionMode.Decompress))
                    using (MemoryStream decompressed = new MemoryStream())
                    {
                        CopyStream(decompressor, decompressed);
                        block.Data = decompressed.ToArray();
                    }
                    // ensure that size of decompressed block is equal to BlockSizeOrigin (to ensure that file was compressed with this application)
                    if (block.Size != BlockSizeOrigin)
                    {
                        lock (_wrongSizeBlocksLock)
                        {
                            _wrongSizeBlocks++; // only the last block of original file can be shorter than BlockSizeOrigin
                            if (_wrongSizeBlocks > 1)
                                throw new Exception("Unsupported format of decompressing file");
                        }
                    }
                    _processedBlocksQueue.Enqueue(block);
                }
            }
            finally
            {
                lock (_aliveThreadCountLock)
                {
                    if (--_aliveProcessingThreadCount <= 0) // last thread informs queue that we will not add new elements
                    {
                        _processedBlocksQueue.CompleteAdding();
                    }
                }
            }
        }
        
        #endregion

        #region Private methods

        /// <summary>
        /// Writes data blocks to <paramref name="destinationFile"/>.
        /// <para>Pop block from the queue</para>
        /// <para>If mode <paramref name="mode"/> is Compress then it writes first block index, size and then block itself.</para>
        /// <para>If mode <paramref name="mode"/> is Decompress then it writes block to the correct position, calculated from block index.</para>
        /// </summary>
        /// <param name="destinationFile">File to write data</param>
        /// <param name="mode">Compress or Decompress</param>
        private void WriteBlocks(string destinationFile, CompressionMode mode)
        {
            using (FileStream fs = new FileStream(destinationFile, FileMode.CreateNew, FileAccess.Write))
            {
                while (true)
                {
                    if (!_processedBlocksQueue.IsAlive)
                        return;

                    var block = _processedBlocksQueue.Dequeue();
                    var bytesToWrite = mode == CompressionMode.Compress ? CompressedBlockToByteArray(block) : block.Data;

                    if (mode == CompressionMode.Decompress)
                        fs.Seek((block.Index * BlockSizeOrigin), SeekOrigin.Begin);
                    fs.Write(bytesToWrite, 0, bytesToWrite.Length);
                    ReportProgress();
                }
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

            if (_readThread.IsAlive)
                _readThread.Abort();
            foreach (var t in _processingThreads)
            {
                if (t.IsAlive)
                    t.Abort();
            }
            _processingThreads.Clear();

            _aliveProcessingThreadCount = 0;
            _processedBlocksQueue = new ThreadSafeQueue<DataBlock>(MaxQueueLength);
            _prepearedBlocksQueue = new ThreadSafeQueue<DataBlock>(MaxQueueLength);
            _aliveThreadCountLock = new object();
            _wrongSizeBlocksLock = new object();
        }

        private void ReportProgress()
        {
            ++_currentTask;
            if (_report != null)
                _report(_currentTask, _totalTasks);
        }

        private void CreateProcessingThreads(Action actionRead, Action actionProcess)
        {
            // run thread for reading
            _readThread = new Thread(() => ThreadWrapper(actionRead));
            _readThread.Start();
            // run required number of threads to process
            _aliveProcessingThreadCount = CompressingThreadNumber;
            _processingThreads.Clear();
            for (int i = 0; i < CompressingThreadNumber; ++i)
            {
                var t = new Thread(() => ThreadWrapper(actionProcess));
                _processingThreads.Add(t);
                t.Start();
            }
        }

        private void ThreadWrapper(Action action)
        {
            try
            {
                action();
            }
            catch (Exception ex)
            {
                if (_threadExceptionHandler != null)
                    _threadExceptionHandler(ex);
                else
                    throw;
            }
        }
        
        #endregion

        #region Private fields and properties

        private ThreadSafeQueue<DataBlock> _processedBlocksQueue;
        private ThreadSafeQueue<DataBlock> _prepearedBlocksQueue;
        private List<Thread> _processingThreads;
        private Thread _readThread;

        #region Progress report
        ReportProgressDelegate _report;
        private int _totalTasks;
        private int _currentTask;
        Action<Exception> _threadExceptionHandler;
        #endregion Progress report

        private int CompressingThreadNumber { get; } = Math.Max(Environment.ProcessorCount, MinCompressingThreadNumber); // try use as many threads as logical processors exsit
        
        private int _aliveProcessingThreadCount;
        private object _aliveThreadCountLock = new object();

        private int _wrongSizeBlocks; // only one block during decompressing can be not equal to BlockSizeOrigin - the last one in origin file
        private object _wrongSizeBlocksLock = new object();

        #endregion

        #region Private constants

        /// <summary>
        /// Block size used for compression in bytes.
        /// </summary>
        private static readonly int BlockSizeOrigin = 1024 * 1024;

        /// <summary>
        /// Min number of threads which will be used for file processing
        /// </summary>
        private static readonly int MinCompressingThreadNumber = 2;

        private static readonly int IndexPrefixLength = 8;
        private static readonly int SizePrefixLength = 4;
        
        private static readonly int MaxQueueLength = (int)(new Microsoft.VisualBasic.Devices.ComputerInfo().TotalPhysicalMemory / (ulong)BlockSizeOrigin / 4); // how many blocks can be located in quater of RAM, since we have two queues

        #endregion
    }
}
