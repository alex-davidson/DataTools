using System;
using System.Data;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData.Columns
{
    public class SqlServerVariableLengthBytesColumn : IColumnDefinition
    {
        public string Name { get; set; }
        public ColumnFlags Flags { get; set; }

        public IColumnSerialiser GetSerialiser() => new Impl(Flags);

        class Impl : IColumnSerialiser
        {
            public Type DotNetType => typeof(byte[]);
            public ColumnDataType DataType => ColumnDataType.VariableLengthBytes;
            public ColumnFlags Flags { get; }
            public int Length => -1;
            private static readonly byte[] emptyBuffer = new byte[0];

            public Impl(ColumnFlags flags)
            {
                Flags = flags & ColumnFlags.AbsentWhenNull;
            }

            void IColumnSerialiser.Write(Stream stream, IDataRecord record, int i)
            {
                if (record.IsDBNull(i))
                {
                    Serialiser.AlignWrite(stream, 4);
                    Serialiser.WriteBytes(stream, emptyBuffer);
                    return;
                }

                var bytes = record[i] as byte[];
                if (bytes == null) throw new  InvalidDataException($"Unable to write {record[i].GetType()} as byte field.");
                Serialiser.AlignWrite(stream, 4);
                Serialiser.WriteBytes(stream, bytes);
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                Serialiser.AlignRead(stream, 4);
                return Serialiser.ReadBytes(stream);
            }
        }
    }
}
