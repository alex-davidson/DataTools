using System;

namespace DataTools.SqlBulkData
{
    public class UnrecognisedFileFormatException : ApplicationException
    {
        public UnrecognisedFileFormatException(string message) : base(message)
        {
        }

        public UnrecognisedFileFormatException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
