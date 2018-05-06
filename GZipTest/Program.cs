using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace GZipTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string destinationFile = string.Empty;
            try
            {
                if (args.Length != 3)
                {
                    ShowErrorMessage($@"Invalid number of arguments. It should be: [{CompressMode}|{DecompressMode}] [source_file] [destination_file]");
                    return;
                }
                var mode = args[0];
                var sourceFile = args[1];
                destinationFile = args[2];

                if (File.Exists(destinationFile))
                {
                    ShowErrorMessage("Destination file already exists.");
                    return;
                }

                GZipTest gZip = new GZipTest(ShowErrorMessage, ReportProgress);

                if (mode.Equals(CompressMode, StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Compression in progress...");
                    gZip.Compress(sourceFile, destinationFile);
                }
                else if (mode.Equals(DecompressMode, StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Decompression in progress...");
                    gZip.Decompress(sourceFile, destinationFile);
                }
                else
                    ShowErrorMessage($@"Invalid mode: [{mode}]. Only [{CompressMode}] and [{DecompressMode}] values are acceptable.");
            }
            catch (Exception ex)
            {
                ShowErrorMessage(ex);
            }
            finally
            {
                Console.WriteLine();
                if (errorOccurred)
                {
                    if (File.Exists(destinationFile))
                        File.Delete(destinationFile);
                    Console.WriteLine("An error occurred during file processing.");
                }
                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
            }
        }

        #region Report methods

        private static void ReportProgress(int current, int total)
        {
            int percentage = total == 0 ? 0 : (int)((double)current / total * 100);
            Console.Write($"\r{percentage}%");
        }
        
        private static void ShowErrorMessage(Exception ex)
        {
            errorOccurred = true;
            ShowErrorMessage(ex.Message);
        }

        private static void ShowErrorMessage(string message)
        {
            Console.WriteLine(message);
        }

        #endregion

        private static bool errorOccurred = false;

        #region Private constants

        private static readonly string CompressMode = "compress";
        private static readonly string DecompressMode = "decompress";

        #endregion
    }
}
