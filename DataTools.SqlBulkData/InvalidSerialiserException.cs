using System;
using DataTools.SqlBulkData.Columns;

namespace DataTools.SqlBulkData
{
    public class InvalidSerialiserException : ApplicationException
    {
        public IColumnSerialiser Serialiser { get; }

        public InvalidSerialiserException(IColumnSerialiser serialiser, string message) : base(message)
        {
            Serialiser = serialiser;
        }
    }
}
