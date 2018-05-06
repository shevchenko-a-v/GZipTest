using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace GZipTest
{
    internal class DataBlockQueue
    {
        public DataBlockQueue(int maxLength)
        {
            _maxLength = maxLength;
            _maxLengthSemaphore = new Semaphore(_maxLength, _maxLength);
            IsAddingCompleted = false;
        }

        public void CompleteAdding()
        {
            IsAddingCompleted = true;
        }

        public void Enqueue(DataBlock block)
        {
            if (IsAddingCompleted)
                throw new InvalidOperationException("Trying add item when adding is already completed.");
            _maxLengthSemaphore.WaitOne();
            lock(_queue)
            {
                _queue.Enqueue(block);
                Monitor.Pulse(_queue);
            }
        }

        public DataBlock Dequeue()
        {
            DataBlock retVal;
            lock(_queue)
            {
                while (_queue.Count == 0)
                {
                    if (IsAddingCompleted)
                        return null;
                    Monitor.Wait(_queue);
                }
                retVal = _queue.Dequeue();
                _maxLengthSemaphore.Release();
            }
            return retVal;
        }

        private bool IsAddingCompleted { get; set; }

        private Queue<DataBlock> _queue = new Queue<DataBlock>();

        private int _maxLength;
        private Semaphore _maxLengthSemaphore;
        
    }
}
