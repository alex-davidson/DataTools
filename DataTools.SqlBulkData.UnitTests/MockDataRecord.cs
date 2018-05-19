using System;

namespace DataTools.SqlBulkData.UnitTests
{
    class MockDataRecord : DataReaderBase
    {
        private readonly string[] fieldNames;
        private readonly object[] row;

        public MockDataRecord(string[] fieldNames, object[] row)
        {
            this.fieldNames = fieldNames;
            this.row = row;
        }

        public override string GetName(int ordinal) => fieldNames[ordinal];
        public override int GetOrdinal(string name) => Array.IndexOf(fieldNames, name);
        public override string GetDataTypeName(int ordinal) => throw new NotImplementedException();
        public override Type GetFieldType(int ordinal) => throw new NotImplementedException();

        public override object GetValue(int ordinal) => row[ordinal];

        public override bool Read() => throw new NotImplementedException();

        public override int FieldCount => fieldNames.Length;
        public override bool HasRows => true;
        public override bool IsClosed => false;
    }
}
