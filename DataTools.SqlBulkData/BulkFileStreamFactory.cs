using System.IO;

namespace DataTools.SqlBulkData
{
    public class BulkFileStreamFactory
    {
        public int BufferSizeKilobytes { get; set; } = 4 * 1024;

        public Stream OpenForExport(string filePath)
        {
            var fileStream = File.Open(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.None);
            return new BufferedStream(fileStream, BufferSizeKilobytes);
        }

        public Stream OpenForImport(string filePath)
        {
            var fileStream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None);
            return new BufferedStream(fileStream, BufferSizeKilobytes);
        }
    }
}
