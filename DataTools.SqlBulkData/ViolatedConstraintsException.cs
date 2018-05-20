using System;

namespace DataTools.SqlBulkData
{
    public class ViolatedConstraintsException : ApplicationException
    {
        public ViolatedConstraintsException(string message) : base(message)
        {
        }
    }
}
