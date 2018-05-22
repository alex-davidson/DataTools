using System;
using System.Data;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData.Columns
{
    public class SqlServerFixedLengthBytesColumn : IColumnDefinition
    {
        public SqlServerFixedLengthBytesColumn(int length)
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
            public Type DotNetType => typeof(byte[]);
            public ColumnDataType DataType => ColumnDataType.FixedLengthBytes;
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
                if (record.IsDBNull(i))
                {
                    Serialiser.AlignWrite(stream, 4);
                    Array.Clear(buffer, 0, buffer.Length);
                    Serialiser.WriteFixedLengthBytes(stream, buffer);
                    return;
                }

                Serialiser.AlignWrite(stream, 4);
                var count = record.GetBytes(i, 0, buffer, 0, Length);
                if (count != buffer.Length) throw new InvalidDataException($"Fixed length string did not fill the buffer. Expected {buffer.Length} bytes, got {count}.");
                Serialiser.WriteFixedLengthBytes(stream, buffer);
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                if (nullMap[i])
                {
                    Serialiser.AlignRead(stream, 4);
                    Serialiser.ReadFixedLengthBytes(stream, buffer);
                    return null;
                }

                Serialiser.AlignRead(stream, 4);
                Serialiser.ReadFixedLengthBytes(stream, buffer);
                // WARNING: Exposes internal buffer.
                return buffer;
            }
        }
    }
}
