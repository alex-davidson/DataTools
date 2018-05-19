using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.IO;
using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData
{
    public class BulkRowWriter
    {
        private readonly Stream stream;
        private readonly NullFieldMap nullFieldMap;
        public IList<IColumnSerialiser> Columns { get; }

        public BulkRowWriter(Stream stream, IColumnSerialiser[] columns)
        {
            this.stream = stream;
            this.Columns = new ReadOnlyCollection<IColumnSerialiser>(columns);
            nullFieldMap = new NullFieldMap(columns);
        }

        public void Write(IDataRecord record)
        {
            Serialiser.AlignWrite(stream, 4);
            Serialiser.WriteByte(stream, TypeIds.RowHeader);

            for (var i = 0; i < Columns.Count; i++)
            {
                nullFieldMap.NullFields[i] = record.IsDBNull(i);
            }
            nullFieldMap.Write(stream);

            Serialiser.AlignWrite(stream, 4);
            for (var i = 0; i < Columns.Count; i++)
            {
                WriteColumn(record, i);
            }
        }

        private void WriteColumn(IDataRecord record, int i)
        {
            if (nullFieldMap.NullFields[i] && Columns[i].Flags.OmitNulls()) return;
            Columns[i].Write(stream, record, i);
        }
    }
}
