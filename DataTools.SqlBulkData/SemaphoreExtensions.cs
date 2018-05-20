using System;
using System.Threading;
using System.Threading.Tasks;

namespace DataTools.SqlBulkData
{
    public static class SemaphoreExtensions
    {
        /// <summary>
        /// Acquire a disposable object representing ownership of a semaphore. This object will release
        /// the semaphore only once, no matter how many times it is disposed.
        /// </summary>
        public static async Task<IDisposable> WaitForLeaseAsync(this SemaphoreSlim semaphore, CancellationToken token)
        {
            await semaphore.WaitAsync(token);
            return new SafeEarlyReleasableSemaphoreLease(semaphore);
        }

        class SafeEarlyReleasableSemaphoreLease : IDisposable
        {
            private SemaphoreSlim semaphore;

            public SafeEarlyReleasableSemaphoreLease(SemaphoreSlim semaphore)
            {
                this.semaphore = semaphore;
            }

            public void Dispose()
            {
                var instance = Interlocked.Exchange(ref semaphore, null);
                instance?.Release();
            }
        }
    }
}
