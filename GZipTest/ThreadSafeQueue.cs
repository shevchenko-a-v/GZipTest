using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace GZipTest
{
    internal class ThreadSafeQueue<T>
    {
        public ThreadSafeQueue(int maxLength)
        {
            _maxLength = maxLength;
            _maxLengthSemaphore = new Semaphore(_maxLength, _maxLength);
            IsAddingCompleted = false;
        }

        public void CompleteAdding()
        {
            IsAddingCompleted = true;
        }

        public void Enqueue(T block)
        {
            if (IsAddingCompleted)
                throw new InvalidOperationException("Attempt to add item when adding is already completed.");
            _maxLengthSemaphore.WaitOne();
            lock(_queue)
            {
                _queue.Enqueue(block);
                Monitor.Pulse(_queue);
            }
        }

        public T Dequeue()
        {
            lock(_queue)
            {
                while (_queue.Count == 0)
                {
                    if (IsAddingCompleted)
                        throw new InvalidOperationException("Attempt to extract item when there are no items anymore and new items are not expected.");
                    Monitor.Wait(_queue);
                }
                var retVal = _queue.Dequeue();
                _maxLengthSemaphore.Release();
                return retVal;
            }
        }

        private bool IsAddingCompleted { get; set; }

        private Queue<T> _queue = new Queue<T>();

        private int _maxLength;
        private Semaphore _maxLengthSemaphore;
        
    }
}
