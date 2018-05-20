using log4net.Core;
using System;

namespace DataTools.SqlBulkData
{
    public class LoggingVerbosity
    {
        private int verbosity;
        private readonly Level[] verbosities = { Level.Off, Level.Error, Level.Warn, Level.Info, Level.Debug };

        public LoggingVerbosity()
        {
            verbosity = Array.IndexOf(verbosities, Level.Warn);
        }

        public void Increase() => verbosity = Math.Min(verbosities.Length - 1, verbosity + 1);
        public void Decrease() => verbosity = Math.Max(0, verbosity - 1);

        public Level Current => verbosities[verbosity];
    }
}
