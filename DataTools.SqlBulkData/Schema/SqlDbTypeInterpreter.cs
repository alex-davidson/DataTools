using System;
using System.Data;

namespace DataTools.SqlBulkData.Schema
{
    public class SqlDbTypeInterpreter
    {
        public SqlDbType Interpret(string dataType)
        {
            switch (dataType.ToLower())
            {
                case "numeric": return SqlDbType.Decimal;
            }
            if (Enum.TryParse<SqlDbType>(dataType, true, out var sqlDbType)) return sqlDbType;
            throw new ArgumentException($"Unrecognised data type: {dataType}");
        }
    }
}
