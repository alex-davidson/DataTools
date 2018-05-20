using System;
using System.Security.Cryptography;
using System.Text;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    /// <summary>
    /// Generates a Guid based on the SHA256 of the table's name and schema.
    /// This is useful for generating identical export files for identical source tables.
    /// </summary>
    public class NameBasedTableGuidPolicy : ITableGuidPolicy
    {
        public Guid GenerateGuid(Table table)
        {
            using (var hashFunction = SHA256.Create())
            {
                var nameBytes = Encoding.UTF8.GetBytes(table.Identify().ToString());
                var hashBytes = hashFunction.ComputeHash(nameBytes);
                var guidBytes = new byte[16];
                Array.Copy(hashBytes, guidBytes, guidBytes.Length);
                return new Guid(guidBytes);
            }
        }
    }
}
