using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace FileDuplicates
{
    class Program
    {
        private const int MaxParallelTask = 4;

        static void Main()
        {
            Console.WriteLine("Search file duplicates\n");
            Console.Write("Type a path: ");

            DirectoryInfo sourceFolder;
            while (true)
            {
                var sourcePath = Console.ReadLine();
                if (Directory.Exists(sourcePath))
                {
                    sourceFolder = new DirectoryInfo(sourcePath);
                    break;
                }

                Console.Write("Path does not exist, try again: ");
            }

            SearchDuplicates(sourceFolder);

            Console.WriteLine("Done");
            Console.ReadLine();
        }

        private static void SearchDuplicates(DirectoryInfo sourceFolder)
        {
            var files = new Dictionary<long, IList<string>>();
            SearchFiles(sourceFolder, files);

            var sameSizeFiles = new ConcurrentQueue<IEnumerable<string>>(files.Where(x => x.Value.Count > 1)
                .Select(x => x.Value));

            var tasksCount = sameSizeFiles.Count < MaxParallelTask ? sameSizeFiles.Count : MaxParallelTask;
            var tasks = new Task[tasksCount];

            for (int i = 0; i < tasksCount; i++)
            {
                tasks[i] = Task.Factory.StartNew(() => CompareFiles(sameSizeFiles));
            }

            Task.WaitAll(tasks);
        }

        private static void SearchFiles(DirectoryInfo sourceFolder, Dictionary<long, IList<string>> files)
        {
            try
            {
                foreach (var file in sourceFolder.GetFiles())
                {
                    // Combine name and path to avoid exception for very long FullName (more than 260 symbols)
                    var fileName = Path.Combine(sourceFolder.FullName, file.Name);

                    if (files.ContainsKey(file.Length))
                    {
                        files[file.Length].Add(fileName);
                    }
                    else
                    {
                        var list = new List<string> { fileName };
                        files.Add(file.Length, list);
                    }
                }
            }
            catch (UnauthorizedAccessException)
            {
                Console.WriteLine(string.Format("Access to the path '{0}' is denied", sourceFolder));
                return;
            }

            foreach (var folder in sourceFolder.GetDirectories())
            {
                SearchFiles(folder, files);
            }
        }

        private static void CompareFiles(ConcurrentQueue<IEnumerable<string>> fileQueue)
        {
            while (fileQueue.Count > 0)
            {
                IEnumerable<string> candidateFiles;
                if (fileQueue.TryDequeue(out candidateFiles))
                {
                    var fileHashes = new Dictionary<string, IList<string>>();
                    foreach (var file in candidateFiles)
                    {
                        var hash = GetFileHash(file);
                        if (fileHashes.ContainsKey(hash))
                        {
                            fileHashes[hash].Add(file);
                        }
                        else
                        {
                            var list = new List<string> { file };
                            fileHashes.Add(hash, list);
                        }
                    }

                    var duplicateFiles = fileHashes.Where(x => x.Value.Count > 1 && !x.Key.Equals(string.Empty))
                        .Select(x => x.Value);

                    foreach (var duplicate in duplicateFiles)
                    {
                        var message = new StringBuilder("\nDuplicates:\n");
                        foreach (var file in duplicate)
                        {
                            message.AppendFormat(" -- {0}\n", file);
                        }

                        Console.WriteLine(message);
                    }
                }
            }
        }

        private static string GetFileHash(string file)
        {
            try
            {
                // If open file with FileShare.ReadWrite than less "File in use" like exceptions appear
                var bytes = MD5.Create().ComputeHash(new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                return Encoding.UTF8.GetString(bytes);
            }
            catch (UnauthorizedAccessException)
            {
                Console.WriteLine(string.Format("Access to the file '{0}' is denied", file));
            }
            catch (IOException ex)
            {
                Console.WriteLine(ex.Message);
            }

            return string.Empty;
        }
    }
}
