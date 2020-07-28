using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace maskx.DurableTask.SQLServer.Extensions
{
    public static class StringExtensions
    {
        public static byte[] CompressString(this string str)
        {
            if (string.IsNullOrEmpty(str))
                return null;
            var compressBeforeByte = Encoding.UTF8.GetBytes(str);
            return Compress(compressBeforeByte);
        }
        public static string DecompressString(this byte[] compressBeforeByte)
        {
            var compressAfterByte = Decompress(compressBeforeByte);
            return Encoding.UTF8.GetString(compressAfterByte);
        }
        

        /// <summary>
        /// Compress
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private static byte[] Compress(byte[] data)
        {
            try
            {
                var ms = new MemoryStream();
                var zip = new GZipStream(ms, CompressionMode.Compress, true);
                zip.Write(data, 0, data.Length);
                zip.Close();
                var buffer = new byte[ms.Length];
                ms.Position = 0;
                ms.Read(buffer, 0, buffer.Length);
                ms.Close();
                return buffer;

            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }

        /// <summary>
        /// Decompress
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private static byte[] Decompress(byte[] data)
        {
            try
            {
                var ms = new MemoryStream(data);
                var zip = new GZipStream(ms, CompressionMode.Decompress, true);
                var msreader = new MemoryStream();
                var buffer = new byte[0x1000];
                while (true)
                {
                    var reader = zip.Read(buffer, 0, buffer.Length);
                    if (reader <= 0)
                    {
                        break;
                    }
                    msreader.Write(buffer, 0, reader);
                }
                zip.Close();
                ms.Close();
                msreader.Position = 0;
                buffer = msreader.ToArray();
                msreader.Close();
                return buffer;
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }
    }
}
