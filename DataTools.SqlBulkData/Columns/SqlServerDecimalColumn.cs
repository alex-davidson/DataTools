using System;
using System.Data;
using System.Data.SqlTypes;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData.Columns
{
    public class SqlServerDecimalColumn : IColumnDefinition
    {
        private readonly DecimalPacker packer;

        public SqlServerDecimalColumn(DecimalPacker packer)
        {
            if (packer == null) throw new ArgumentNullException(nameof(packer));
            if (packer.ByteCount % 4 != 0) throw new ArgumentException($"Length is not a multiple of 4: {packer.ByteCount}", nameof(packer));
            this.packer = packer;
        }

        public string Name { get; set; }
        public ColumnFlags Flags { get; set; }

        public IColumnSerialiser GetSerialiser() => new Impl(packer, Flags);

        class Impl : IColumnSerialiser
        {
            private static readonly decimal NullPlaceholder = 0;

            public Type DotNetType => typeof(decimal);
            public ColumnDataType DataType => ColumnDataType.DecimalFloatingPoint;
            public ColumnFlags Flags { get; }
            public int Length => buffer.Length;
            private readonly DecimalPacker packer;
            private readonly byte[] buffer;

            public Impl(DecimalPacker packer, ColumnFlags flags)
            {
                this.packer = packer;
                buffer = packer.CreateBuffer();
                Flags = flags & ColumnFlags.AbsentWhenNull;
            }

            void IColumnSerialiser.Write(Stream stream, IDataRecord record, int i)
            {
                Serialiser.AlignWrite(stream, 4);
                packer.Pack(GetDecimalValue(record, i), buffer);
                Serialiser.WriteFixedLengthBytes(stream, buffer);
            }

            private static decimal GetDecimalValue(IDataRecord record, int i)
            {
                if (record.IsDBNull(i)) return NullPlaceholder;
                return record.GetDecimal(i);
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                Serialiser.AlignRead(stream, 4);
                Serialiser.ReadFixedLengthBytes(stream, buffer);
                return packer.Unpack(buffer).Value;
            }
        }
    }
}
