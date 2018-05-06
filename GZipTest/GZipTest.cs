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
            _dataBlocksQueue = new ThreadSafeQueue<DataBlock>(MaxQueueLength);
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
                CreateProcessingThreads(() => ReadAndCompressBlock(source));
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
                CreateProcessingThreads(() => ReadAndDecompressBlock(source));
                WriteBlocks(destination, CompressionMode.Decompress);
            }
            finally
            {
                CleanUp();
            }
        }

        #region Private methods

        /// <summary>
        /// Read data block from <paramref name="sourceFile"/>, compress it and push to threadsafe queue
        /// </summary>
        /// <param name="sourceFile">Path to source file</param>
        private void ReadAndCompressBlock(string sourceFile)
        {
            using (var fs = new FileStream(sourceFile, FileMode.Open, FileAccess.Read))
            {
                while (true)
                {
                    int filePositionToRead;
                    long index;
                    // --- enter critical section ---
                    lock (_filePositionLock)
                    {
                        // calculate total tasks to report
                        if (_totalTasks == 0)
                            _totalTasks = (int)Math.Ceiling((double)fs.Length / BlockSizeOrigin);
                        // get position to read
                        filePositionToRead = _filePosition;
                        if (filePositionToRead >= fs.Length) // EOF -> exit
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
                        // set position for next thread 
                        _filePosition += BlockSizeOrigin;
                        index = _currentBlockIndex++;
                    // --- leave critical section ---
                    }
                    // read block from current position
                    byte[] data = new byte[BlockSizeOrigin];
                    fs.Seek(filePositionToRead, SeekOrigin.Begin);
                    var realLength = fs.Read(data, 0, BlockSizeOrigin);

                    var block = new DataBlock(index);
                    // compress and push to queue
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
            try
            {
                using (FileStream fs = new FileStream(destinationFile, FileMode.CreateNew, FileAccess.Write))
                {
                    while (true)
                    {
                        DataBlock block = null;
                        try
                        {
                            block = _dataBlocksQueue.Dequeue();
                        }
                        catch(InvalidOperationException)
                        {
                            // queue is empty and no items will be added -> exit
                            return;
                        }
                        var bytesToWrite = mode == CompressionMode.Compress ? CompressedBlockToByteArray(block) : block.Data;

                        if (mode == CompressionMode.Decompress)
                            fs.Seek((block.Index * BlockSizeOrigin), SeekOrigin.Begin);
                        fs.Write(bytesToWrite, 0, bytesToWrite.Length);
                        ReportProgress();
                    }
                }

            }
            catch(IOException ex)
            {
                throw new IOException("Destination file already exists.", ex);
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
        
        /// <summary>
        /// Read data block from <paramref name="sourceFile"/>, decompress it and push to threadsafe queue
        /// </summary>
        /// <param name="sourceFile">Path to source file</param>
        private void ReadAndDecompressBlock(string sourceFile)
        {
            using (var fs = new FileStream(sourceFile, FileMode.Open, FileAccess.Read))
            {
                while (true)
                {
                    int filePositionToRead;
                    long index;
                    int blockSize;
                    //   --- enter crit section ---
                    lock (_filePositionLock)
                    {
                        // calculate total tasks to report
                        if (_totalTasks == 0)
                            _totalTasks = GetBlocksCount(fs);

                        filePositionToRead = _filePosition;
                        if (filePositionToRead >= fs.Length) // EOF -> exit
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

                        // read index of block, its size and set _filePosition to next block
                        byte[] indexArray = new byte[IndexPrefixLength];
                        byte[] sizeArray = new byte[SizePrefixLength];
                        fs.Seek(filePositionToRead, SeekOrigin.Begin);
                        var realIndexLength = fs.Read(indexArray, 0, IndexPrefixLength);
                        var realSizeLength = fs.Read(sizeArray, 0, SizePrefixLength);
                        // if we cannot read required bytes something went wrong
                        if (realIndexLength != IndexPrefixLength || realSizeLength != SizePrefixLength) 
                            throw new Exception("Unsupported format of decompressing file");
                        if (BitConverter.IsLittleEndian)
                        {
                            Array.Reverse(indexArray);
                            Array.Reverse(sizeArray);
                        }
                        index = BitConverter.ToInt64(indexArray, 0);
                        blockSize = BitConverter.ToInt32(sizeArray, 0);
                        
                        // set file position for next thread
                        _filePosition += IndexPrefixLength + SizePrefixLength + blockSize;
                    //   --- leave crit section ---
                    }

                    byte[] data = new byte[blockSize];       
                    // read data
                    if (fs.Read(data, 0, blockSize) != blockSize) // if we cannot read required bytes something went wrong
                        throw new Exception("Unsupported format of decompressing file");
                    var block = new DataBlock(index);
                    byte[] decompressedData = new byte[blockSize];

                    // decompress
                    using (var src = new MemoryStream(data))
                    using (var decompressor = new GZipStream(src, CompressionMode.Decompress))
                    using (MemoryStream decompressed = new MemoryStream())
                    {
                        CopyStream(decompressor, decompressed);
                        block.Data = decompressed.ToArray();
                    }
                    // ensure that size of decompressed block is equal to BlockSizeOrigin (to ensure that file was compressed with this application)
                    if (block.Size != BlockSizeOrigin)
                        _wrongSizeBlocks++; // only the last block of original file can be shorter than BlockSizeOrigin
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
            _dataBlocksQueue = new ThreadSafeQueue<DataBlock>(MaxQueueLength);
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

        private ThreadSafeQueue<DataBlock> _dataBlocksQueue;
        private List<Thread> _processingThreads;

        #region Progress report
        ReportProgressDelegate _report;
        private int _totalTasks;
        private int _currentTask;
        #endregion Progress report

        private int ThreadNumber { get; } = Math.Max(Environment.ProcessorCount - 1, MinThreadNumber); // try use as many threads as logical processors exsit minus one for main thread

        private long _currentBlockIndex;
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
