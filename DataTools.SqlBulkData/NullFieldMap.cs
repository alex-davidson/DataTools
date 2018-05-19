using System.Collections.Generic;
using System.IO;
using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData
{
    public class NullFieldMap
    {
        private readonly BitSetPacker packer;
        private readonly byte[] buffer;

        public NullFieldMap(IColumnSerialiser[] columns)
        {
            var nullableColumns = new List<int>();
            for (var i = 0; i < columns.Length; i++)
            {
                if (columns[i].Flags.IsNullable()) nullableColumns.Add(i);
            }
            packer = new BitSetPacker(nullableColumns.ToArray());
            NullFields = new bool[columns.Length];
            buffer = new byte[packer.PackedByteCount];
        }

        public bool[] NullFields { get; }

        public void Read(Stream stream)
        {
            Serialiser.ReadFixedLengthBytes(stream, buffer);
            packer.Unpack(buffer, NullFields);
        }

        public void Write(Stream stream)
        {
            packer.Pack(NullFields, buffer);
            Serialiser.WriteFixedLengthBytes(stream, buffer);
        }
    }
}
