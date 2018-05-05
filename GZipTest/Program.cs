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
                GZipTest gZip = new GZipTest();

                if (mode.EqualsNoCase(CompressMode))
                    gZip.Compress(sourceFile, destinationFile);
                else if (mode.EqualsNoCase(DecompressMode))
                    gZip.Decompress(sourceFile, destinationFile);
                else
                    ShowErrorMessage($@"Invalid mode: [{mode}]. Only [{CompressMode}] and [{DecompressMode}] values are acceptable.");
            }
            catch(Exception ex)
            {
                ShowErrorMessage(ex.Message);
            }
        }
        
        private static void ShowErrorMessage(string message)
        {
            Console.WriteLine(message);
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
            

        }

        #region Private constants

        private static readonly string CompressMode = "compress";
        private static readonly string DecompressMode = "decompress";

        #endregion
    }
}
