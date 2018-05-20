using System;
using System.Threading;
using log4net;

namespace DataTools.SqlBulkData
{
    public class CancelKeyMonitor
    {
        private readonly ManualResetEvent terminationEvent = new ManualResetEvent(false);
        private readonly CancellationTokenSource tokenSource = new CancellationTokenSource();
        public CancellationToken GetToken() => tokenSource.Token;

        public CancelKeyMonitor()
        {
            Console.CancelKeyPress += RequestCancel;
        }

        public void CheckForCancellation()
        {
            if (tokenSource.IsCancellationRequested)
            {
                throw new OperationCanceledException("Operation was aborted via Ctrl-C.");
            }
        }

        public void LogRequestsTo(ILog log)
        {
            CancelRequested += (s, e) => log.Warn("Shutting down.");
            KillRequested += (s, e) => log.Warn("CTRL-C pressed twice. Terminating.");
        }

        public EventHandler<ConsoleCancelEventArgs> CancelRequested;
        public EventHandler<ConsoleCancelEventArgs> KillRequested;

        public void RequestCancel(object sender, ConsoleCancelEventArgs args)
        {
            if (tokenSource.IsCancellationRequested)
            {
                KillRequested?.Invoke(sender, args);
                return;
            }
            tokenSource.Cancel();
            terminationEvent.Set();
            args.Cancel = true;
            CancelRequested?.Invoke(sender, args);
        }
    }
}
