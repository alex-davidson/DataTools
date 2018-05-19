using System;
using System.Diagnostics;
using System.Linq;

namespace DataTools.SqlBulkData
{
    /// <summary>
    /// Packs elements of a boolean array into a bitset stored as a byte array.
    /// Elements to pack are selected by an array of indices into the boolean array.
    /// Elements not mentioned in the array of indices will not be modified.
    /// </summary>
    /// <remarks>
    /// This class does not do validation, except for assertions in debug builds.
    /// </remarks>
    public class BitSetPacker
    {
        private readonly int[] indexes;

        public BitSetPacker(int[] indexes)
        {
            this.indexes = indexes;
            UnpackedBitCount = indexes.Any() ? indexes.Max() + 1 : 0;
        }

        public int UnpackedBitCount { get; }
        public int PackedByteCount => (indexes.Length + 8 - 1) / 8;

        public void Pack(bool[] fromBits, byte[] toBytes)
        {
            Debug.Assert(fromBits.Length >= UnpackedBitCount);
            Debug.Assert(toBytes.Length >= PackedByteCount);
            var bitOffset = 0;
            for (var i = 0; i < toBytes.Length; i++)
            {
                byte current = 0;
                for (var j = 0; j < 8; j++)
                {
                    if (bitOffset >= indexes.Length) break;
                    var index = indexes[bitOffset];
                    if (fromBits[index]) current |= Bit(j);
                    bitOffset++;
                }
                toBytes[i] = current;
            }
        }

        public void Unpack(byte[] fromBytes, bool[] toBits)
        {
            Debug.Assert(fromBytes.Length >= PackedByteCount);
            Debug.Assert(toBits.Length >= UnpackedBitCount);
            var bitOffset = 0;
            foreach (var current in fromBytes)
            {
                for (var j = 0; j < 8; j++)
                {
                    if (bitOffset >= indexes.Length) break;
                    var index = indexes[bitOffset];
                    toBits[index] = (current & Bit(j)) != 0;
                    bitOffset++;
                }
            }
        }

        private static byte Bit(int num) => (byte)(0x01 << num);
    }
}
