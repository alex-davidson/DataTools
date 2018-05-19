using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData
{
    public class BulkRowReader : IBulkRowReader
    {
        private readonly Stream stream;
        private readonly NullFieldMap nullFieldMap;
        public IList<IColumnSerialiser> Columns { get; }

        public object[] Current { get; }

        public BulkRowReader(Stream stream, IColumnSerialiser[] columns)
        {
            this.stream = stream;
            Columns = new ReadOnlyCollection<IColumnSerialiser>(columns);
            Current = new object[columns.Length];
            nullFieldMap = new NullFieldMap(columns);
        }

        public bool MoveNext()
        {
            Array.Clear(Current, 0, Current.Length);
            if (!Serialiser.TryAlignRead(stream, 4)) return false;
            if (Serialiser.IsEndOfStream(stream)) return false;

            if (Serialiser.ReadByte(stream) != TypeIds.RowHeader) throw new InvalidDataException("Expected a row header but did not find one.");

            nullFieldMap.Read(stream);

            Serialiser.AlignRead(stream, 4);
            for (var i = 0; i < Columns.Count; i++)
            {
                Current[i] = ReadColumn(i) ?? DBNull.Value;
            }
            return true;
        }

        private object ReadColumn(int i)
        {
            if (nullFieldMap.NullFields[i] && Columns[i].Flags.OmitNulls()) return null;
            var value = Columns[i].Read(stream, i, nullFieldMap.NullFields);
            return nullFieldMap.NullFields[i] ? null : value;
        }
    }
}
