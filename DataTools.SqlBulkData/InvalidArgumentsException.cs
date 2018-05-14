using System;

namespace DataTools.SqlBulkData
{
    public class InvalidArgumentsException : ApplicationException
    {
        public InvalidArgumentsException(string message) : base(message)
        {
        }
    }
}
