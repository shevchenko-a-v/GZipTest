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
            maxLength = Math.Max(MinLength, maxLength);
            _maxLengthSemaphore = new Semaphore(maxLength, maxLength);
            IsAddingCompleted = false;
        }

        /// <summary>
        /// Signal that no new items are going to be enqueued
        /// </summary>
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
                    Monitor.Wait(_queue, 250); // AddingCompleted can change value during this wait, so set the timeout interval to check it sometimes
                }
                var retVal = _queue.Dequeue();
                _maxLengthSemaphore.Release();
                return retVal;
            }
        }

        private bool IsAddingCompleted { get; set; }

        private Queue<T> _queue = new Queue<T>();

        private Semaphore _maxLengthSemaphore;

        private static readonly int MinLength = 2;

    }
}
