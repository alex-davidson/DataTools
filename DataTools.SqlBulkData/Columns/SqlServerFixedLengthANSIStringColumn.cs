using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Text;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData.Columns
{
    public class SqlServerFixedLengthANSIStringColumn : IColumnDefinition
    {
        public SqlServerFixedLengthANSIStringColumn(int length)
        {
            if (length <= 0) throw new ArgumentOutOfRangeException(nameof(length));
            Length = length;
        }

        public string Name { get; set; }
        public int Length { get; }
        public ColumnFlags Flags { get; set; }

        public IColumnSerialiser GetSerialiser() => new Impl(Length, Flags);

        class Impl : IColumnSerialiser
        {
            public Type DotNetType => typeof(string);
            public ColumnDataType DataType => ColumnDataType.FixedLengthString;
            public ColumnFlags Flags { get; }
            public int Length { get; }
            private readonly byte[] buffer;

            public Impl(int length, ColumnFlags flags)
            {
                Length = length;
                Flags = flags & ColumnFlags.AbsentWhenNull;
                buffer = new byte[length];
            }

            void IColumnSerialiser.Write(Stream stream, IDataRecord record, int i)
            {
                Serialiser.AlignWrite(stream, 4);

                if (record.IsDBNull(i))
                {
                    Array.Clear(buffer, 0, buffer.Length);
                    Serialiser.WriteFixedLengthBytes(stream, buffer);
                    return;
                }

                var value = record.GetString(i);
                var count = Encoding.ASCII.GetBytes(value, 0, value.Length, buffer, 0);
                if (count != buffer.Length) throw new InvalidDataException($"Fixed length string did not fill the buffer. Expected {buffer.Length} bytes, got {count}.");
                Serialiser.WriteFixedLengthBytes(stream, buffer);
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                Serialiser.AlignRead(stream, 4);
                Serialiser.ReadFixedLengthBytes(stream, buffer);

                if (nullMap[i]) return null;
                return Encoding.ASCII.GetString(buffer);
            }
        }
    }
}
