using GZipTest.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GZipTest
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                ShowErrorMessage($@"Invalid number of arguments. It should be: [{CompressMode}|{DecompressMode}] [source_file] [destination_file]");
                return;
            }
            var mode = args[0];
            var sourceFile = args[1];
            var destinationFile = args[2];

            try
            {
                GZipTest gZip = new GZipTest(ReportProgress);

                if (mode.EqualsNoCase(CompressMode))
                {
                    Console.WriteLine("Compression in progress...");
                    gZip.Compress(sourceFile, destinationFile);
                }
                else if (mode.EqualsNoCase(DecompressMode))
                {
                    Console.WriteLine("Decompression in progress...");
                    gZip.Decompress(sourceFile, destinationFile);
                }
                else
                    ShowErrorMessage($@"Invalid mode: [{mode}]. Only [{CompressMode}] and [{DecompressMode}] values are acceptable.");
            }
            catch (Exception ex)
            {
                ShowErrorMessage(ex.Message);
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
            }
        }

        private static void ReportProgress(int current, int total)
        {
            int percentage = total == 0 ? 0 : (int)((double)current / total * 100);
            Console.Write($"\r{percentage}%");
        }
        
        private static void ShowErrorMessage(string message)
        {
            Console.WriteLine(message);
        }

        #region Private constants

        private static readonly string CompressMode = "compress";
        private static readonly string DecompressMode = "decompress";

        #endregion
    }
}
