using System;
using System.Collections;
using System.Data.Common;

namespace DataTools.SqlBulkData
{
    /// <summary>
    /// Simple base class for IDataReader implementations.
    /// </summary>
    public abstract class DataReaderBase : DbDataReader
    {
        public override int GetValues(object[] values)
        {
            for (var i = 0; i < FieldCount; i++) values[i] = this[i];
            return FieldCount;
        }

        public override bool IsDBNull(int ordinal) => GetValue(ordinal) == DBNull.Value;

        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(GetOrdinal(name));

        public override int RecordsAffected => -1;

        public override bool NextResult() => false;

        public override int Depth => 0;

        public override bool GetBoolean(int ordinal) => Convert.ToBoolean(IsDBNull(ordinal) ? 0 : GetValue(ordinal));
        public override byte GetByte(int ordinal) => Convert.ToByte(IsDBNull(ordinal) ? 0 : GetValue(ordinal));
        public override char GetChar(int ordinal) => Convert.ToChar(IsDBNull(ordinal) ? 0 : GetValue(ordinal));
        public override Guid GetGuid(int ordinal) => IsDBNull(ordinal) ? Guid.Empty : (Guid)GetValue(ordinal);
        public override short GetInt16(int ordinal) => Convert.ToInt16(IsDBNull(ordinal) ? 0 : GetValue(ordinal));
        public override int GetInt32(int ordinal) => Convert.ToInt32(IsDBNull(ordinal) ? 0 : GetValue(ordinal));
        public override long GetInt64(int ordinal) => Convert.ToInt64(IsDBNull(ordinal) ? 0 : GetValue(ordinal));
        public override DateTime GetDateTime(int ordinal) => Convert.ToDateTime(IsDBNull(ordinal) ? 0 : GetValue(ordinal));
        public override string GetString(int ordinal) => Convert.ToString(IsDBNull(ordinal) ? "" : GetValue(ordinal));
        public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(IsDBNull(ordinal) ? 0 : GetValue(ordinal));
        public override double GetDouble(int ordinal) => Convert.ToDouble(IsDBNull(ordinal) ? 0 : GetValue(ordinal));
        public override float GetFloat(int ordinal) => Convert.ToSingle(IsDBNull(ordinal) ? 0 : GetValue(ordinal));

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            if (IsDBNull(ordinal))
            {
                Array.Clear(buffer, bufferOffset, length);
                return length;
            }
            var value = GetValue(ordinal);
            if (value is byte[] byteArray)
            {
                var count = Math.Min(length, byteArray.Length - dataOffset);
                if (count <= 0) return 0;
                Array.Copy(byteArray, (int)dataOffset, buffer, bufferOffset, count);
                return count;
            }
            throw new NotSupportedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            var value = GetValue(ordinal);
            if (value is string stringValue)
            {
                var count = Math.Min(length, stringValue.Length - dataOffset);
                if (count <= 0) return 0;
                stringValue.CopyTo((int)dataOffset, buffer, bufferOffset, (int)count);
                return count;
            }
            if (value is char[] charArray)
            {
                var count = Math.Min(length, charArray.Length - dataOffset);
                if (count <= 0) return 0;
                Array.Copy(charArray, (int)dataOffset, buffer, bufferOffset, count);
                return count;
            }
            throw new NotSupportedException();
        }

        public override IEnumerator GetEnumerator() => new DbEnumerator(this);
    }
}
