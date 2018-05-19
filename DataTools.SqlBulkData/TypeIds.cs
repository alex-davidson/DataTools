using System;
using System.Text;

namespace DataTools.SqlBulkData
{
    public static class TypeIds
    {
        public static readonly uint FileHeader = BitConverter.ToUInt32(Encoding.ASCII.GetBytes("BTBL"), 0);
        public static readonly uint TableNameChunk = BitConverter.ToUInt32(Encoding.ASCII.GetBytes("TABL"), 0);
        public static readonly uint ColumnsChunk = BitConverter.ToUInt32(Encoding.ASCII.GetBytes("COLS"), 0);
        public static readonly uint RowDataChunk = BitConverter.ToUInt32(Encoding.ASCII.GetBytes("ROWD"), 0);
        public static readonly byte RowHeader = 0x52;
    }
}
