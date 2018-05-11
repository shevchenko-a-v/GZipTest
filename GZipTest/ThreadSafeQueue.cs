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
            _emptySemaphore = new Semaphore(0, maxLength);
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
                _emptySemaphore.Release();
            }
        }

        public T Dequeue()
        {
            if (!IsAlive)
                throw new InvalidOperationException("Attempt to extract item when there are no items anymore and new items are not expected.");
            _emptySemaphore.WaitOne();
            lock (_queue)
            {
                var retVal = _queue.Dequeue();
                _maxLengthSemaphore.Release();
                return retVal;
            }
        }

        public bool IsAlive
        {
            get
            {
                lock (_queue)
                {
                    return !IsAddingCompleted || _queue.Count > 0;
                }
            }
        }

        private bool IsAddingCompleted { get; set; }

        private Queue<T> _queue = new Queue<T>();

        private Semaphore _maxLengthSemaphore;
        private Semaphore _emptySemaphore;

        private static readonly int MinLength = 2;

    }
}
