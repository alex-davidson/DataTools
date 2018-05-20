using System;
using System.IO;
using System.Linq;
using DataTools.SqlBulkData.PersistedModel;

namespace DataTools.SqlBulkData
{
    public class TableFileNamingRule
    {
        public string Extension { get; set; } = "bulktable";

        public string GetFileNameForTable(TableDescriptor table)
        {
            if (String.IsNullOrEmpty(table.Name)) throw new ArgumentException("Table name cannot be empty.", nameof(table));
            return String.Concat(table.Schema, ".", table.Name, NormaliseExtension(Extension)).Trim('.');
        }

        public string[] ListTableFiles(string path)
        {
            if (!Directory.Exists(path)) throw new DirectoryNotFoundException($"Directory does not exist: {path}");
            return Directory.GetFiles(path).Where(IsTableFilePath).ToArray();
        }

        private bool IsTableFilePath(string filePath) => StringComparer.OrdinalIgnoreCase.Equals(Path.GetExtension(filePath), NormaliseExtension(Extension));

        private static string NormaliseExtension(string extension)
        {
            var trimmed = extension.TrimStart('.');
            if (String.IsNullOrEmpty(trimmed)) return "";
            return String.Concat(".", trimmed);
        }
    }
}
