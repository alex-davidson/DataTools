using System;
using System.Collections.Generic;
using System.IO;

namespace DataTools.SqlBulkData
{
    public class ArgumentParser
    {
        public void Parse(IEnumerable<string> arguments, Program program)
        {
            using (var iterator = arguments.GetEnumerator())
            while (iterator.MoveNext())
            {
                if (program.Mode == ProgramMode.None)
                {
                    if (!Enum.TryParse<ProgramMode>(iterator.Current, true, out var mode) || mode == ProgramMode.None)
                    {
                        throw new InvalidArgumentsException($"Not a valid mode: {iterator.Current}");
                    }
                    program.Mode = mode;
                    continue;
                }
                switch (iterator.Current)
                {
                    case "-s":
                    case "--server":
                        if (!iterator.MoveNext()) throw new InvalidArgumentsException("Expected another parameter after --server");
                        program.SubjectDatabase.Server = iterator.Current;
                        break;
                    case "-d":
                    case "--database":
                        if (!iterator.MoveNext()) throw new InvalidArgumentsException("Expected another parameter after --database");
                        program.SubjectDatabase.Database = iterator.Current;
                        break;
                    case "-f":
                    case "--files":
                        if (!iterator.MoveNext()) throw new InvalidArgumentsException("Expected another parameter after --files");
                        program.BulkFilesPath = Path.GetFullPath(iterator.Current).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
                        break;
                    default:
                        throw new InvalidArgumentsException($"Unrecognised argument: {iterator.Current}");
                }
            }
            if (program.Mode == ProgramMode.None) throw new InvalidArgumentsException("No mode specified");
        }

        public void WriteUsage(TextWriter error)
        {
            
        }
    }
}
