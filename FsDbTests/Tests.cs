using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using FsDb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NUnit.Framework;

namespace FsDbTests
{
    [TestFixture]
    [TestClass]
    public class WritePerformanceTest
    {
        
        byte[] data = new byte[3580];
        byte[] stub = new byte[1024*1024];
        private int capacity = 6200;
        private Crc32 crc32 = new Crc32();

        int N = 100000;
        private FileStream CreateFile()
        {
            var filename = @"D:\temp\testfile.db";
            var dir = Path.GetDirectoryName(filename);
            if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
            return new FileStream(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        }

        private FileStream OpenFile()
        {
            var filename = @"D:\temp\testfile.db";
            return new FileStream(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        }

        [TestMethod]
        [Test]
        public void NSegmentRWTest()
        {
            Action<Action> Run = test =>
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                test();
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
            };

            Run(Test1SegmentWriting);
            Run(Test1SegmentReading);
            Run(Test3SegmentWriting);
            Run(Test3SegmentReading);
        }
        
        public void Test1SegmentWriting()
        {
            Console.WriteLine("1 segment writing");
            using (var file = CreateFile())
            {
                for (int i = 0; i < N; ++i)
                {
                    byte[] segment = new byte[capacity + 8];
                    var len = BitConverter.GetBytes(data.Length);
                    Array.Copy(len, 0, segment, 0, 4);
                    Array.Copy(data, 0, segment, 4, data.Length);
                    var checksum = crc32.ComputeChecksumBytes(data);
                    Array.Copy(checksum, 0, segment, 4 + data.Length, 4);
                    Array.Copy(stub, 0, segment, 8 + data.Length, capacity - data.Length);
                    file.Write(segment, 0, segment.Length);
                }
            }
        }


        public void Test1SegmentReading()
        {
            Console.WriteLine("1 segment reading");
            using (var file = OpenFile())
            {
                for (int i = 0; i < N; ++i)
                {
                    byte[] segment = new byte[capacity + 8];
                    file.Read(segment, 0, capacity + 8);
                    int len = BitConverter.ToInt32(segment, 0);
                    var data = new byte[len];
                    Array.Copy(segment, 4, data, 0, data.Length);
                    int chsum = BitConverter.ToInt32(segment, data.Length + 4);
                    var control = crc32.ComputeChecksum(data);
                    var istrue = chsum == control;
                }
            }
        }


        public void Test3SegmentWriting()
        {
            Console.WriteLine("3 segment writing");
            using (var file = CreateFile())
            {
                for (int i = 0; i < N; ++i)
                {
                    var len = BitConverter.GetBytes(data.Length);
                    file.Write(len, 0, 4);
                    file.Write(data, 0, data.Length);
                    var checksum = crc32.ComputeChecksumBytes(data);
                    file.Write(checksum, 0, 4);
                    file.Write(stub, 0, capacity-data.Length);
                }
            }
        }


        public void Test3SegmentReading()
        {
            Console.WriteLine("3 segment reading");
            using (var file = OpenFile())
            {
                for (int i = 0; i < N; ++i)
                {
                    byte[] buf = new byte[4];
                    file.Read(buf, 0, 4);
                    int len = BitConverter.ToInt32(buf, 0);
                    byte[] data = new byte[len];
                    file.Read(data, 0, data.Length);
                    file.Read(buf, 0, 4);
                    var checksum = BitConverter.ToUInt32(buf, 0);
                    var control = crc32.ComputeChecksum(data);
                    bool istrue = checksum == control;
                }
            }
        }
    }


    [TestClass]
    [TestFixture]
    public class HashTest
    {
        Crc32 crc32 = new Crc32();
        MD5CryptoServiceProvider crypto = new MD5CryptoServiceProvider();
        
        [TestMethod]
        [Test]
        public void Test()
        {
            byte[] bytes = new byte[500];
            int N = 1000000;

            bool b = true;
            var v = crc32.ComputeChecksumBytes(bytes);
            var vint = crc32.ComputeChecksum(bytes);

            Console.WriteLine(v.Length);
           
            var t1 = new Stopwatch();
            t1.Start();
            for (int i = 0; i < N; ++i)
            {
                crc32.ComputeChecksumBytes(bytes);
                b &= crc32.ComputeChecksum(bytes) == vint;
            }

            t1.Stop();

            v = crypto.ComputeHash(bytes);
            Console.WriteLine(v.Length);

            var t2 = new Stopwatch();
            t2.Start();
            for (int i = 0; i < N; ++i)
            {
                crypto.ComputeHash(bytes);
                b &= crypto.ComputeHash(bytes).SequenceEqual(v);
            }
            t2.Stop();

            

            Console.WriteLine(t1.Elapsed);
            Console.WriteLine(t2.Elapsed);
        }
    }

    [TestFixture]
    [TestClass]
    public class IOPerformanceTest
    {
        [Test]
        [TestMethod]
        public void Test()
        {
            Random rnd = new Random();
            byte[] stub = new byte[1024 * 1024];
            rnd.NextBytes(stub);
            int N = 10000;
            int minlen = 100;
            string file = @"D:\ProjectData\FsDb\testIO.txt";

            // initialization not empty file
            using (var f = new FileStream(file, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                for (int i = 0; i < N; ++i)
                {
                    int len = rnd.Next(minlen, stub.Length);
                    f.Write(stub, 0, len);
                }
                f.Flush();
            }

            using (var f = new FileStream(file, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                Stopwatch t = new Stopwatch();
                t.Start();
                long totalLen = 0;
                for (int i = 0; i < N; ++i)
                {
                    int len = rnd.Next(minlen, stub.Length);
                    f.Write(stub, 0, len);
                    totalLen += len;
                }
                f.Flush();
                t.Stop();
                Console.WriteLine("Sequential Writing: " + t.Elapsed + " (" + (totalLen / 1024.0 / 1024.0 / t.Elapsed.TotalSeconds).ToString("### ###.00") + " MB/sec)");
            }

           
            using (var f = new FileStream(file, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite, 4 * 1024))
            {
                Stopwatch t = new Stopwatch();
                t.Start();
                long totalLen = 0;
                for (int i = 0; i < N; ++i)
                {
                    int len = rnd.Next(minlen, stub.Length);
                    f.Write(stub, 0, len);
                    totalLen += len;
                }
                f.Flush();
                t.Stop();
                Console.WriteLine("Sequential Writing with buffer: " + t.Elapsed + " (" + (totalLen / 1024.0 / 1024.0 / t.Elapsed.TotalSeconds).ToString("### ###.00") + " MB/sec)");
            }


            
            using (var f = new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                Stopwatch t = new Stopwatch();
                t.Start();
                long totalLen = 0;
                var fileLen = f.Length;
                for (int i = 0; i < N; ++i)
                {
                    int len = rnd.Next(minlen, stub.Length);
                    f.Position = rnd.Next(0, (int)(fileLen - len));
                    f.Write(stub, 0, len);
                    totalLen += len;
                }
                f.Flush();
                t.Stop();
                Console.WriteLine("Random Access Writing: " + t.Elapsed + " (" + (totalLen / 1024.0 / 1024.0 / t.Elapsed.TotalSeconds).ToString("### ###.00") + " MB/sec)");
            }


            
            using (var f = new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 4 * 1024))
            {
                Stopwatch t = new Stopwatch();
                t.Start();
                long totalLen = 0;
                var fileLen = f.Length;
                for (int i = 0; i < N; ++i)
                {
                    int len = rnd.Next(minlen, stub.Length);
                    f.Position = rnd.Next(0, (int)(fileLen - len));
                    f.Write(stub, 0, len);
                    totalLen += len;
                }
                f.Flush();
                t.Stop();
                Console.WriteLine("Random Access Writing with buffer: " + t.Elapsed + " (" + (totalLen / 1024.0 / 1024.0 / t.Elapsed.TotalSeconds).ToString("### ###.00") + " MB/sec)");
            }

            using (var f = new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                Stopwatch t = new Stopwatch();
                t.Start();
                long totalLen = 0;
                long fileLen = f.Length;
                for (int i = 0; i < N; ++i)
                {
                    int len = rnd.Next(minlen, stub.Length);
                    if (f.Position + len >= fileLen)
                        f.Position = 0;
                    totalLen += f.Read(stub, 0, len);
                }
                f.Flush();
                t.Stop();
                Console.WriteLine("Sequential Reading: " + t.Elapsed + " (" + (totalLen / 1024.0 / 1024.0 / t.Elapsed.TotalSeconds).ToString("### ###.00") + " MB/sec)");
            }


            using (var f = new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 4 * 1024))
            {
                Stopwatch t = new Stopwatch();
                t.Start();
                long totalLen = 0;
                long fileLen = f.Length;
                for (int i = 0; i < N; ++i)
                {
                    int len = rnd.Next(minlen, stub.Length);
                    if (f.Position + len >= fileLen)
                        f.Position = 0;
                    totalLen += f.Read(stub, 0, len);
                }
                f.Flush();
                t.Stop();
                Console.WriteLine("Sequential Reading with buffer: " + t.Elapsed + " (" + (totalLen / 1024.0 / 1024.0 / t.Elapsed.TotalSeconds).ToString("### ###.00") + " MB/sec)");
            }


            using (var f = new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                Stopwatch t = new Stopwatch();
                t.Start();
                long totalLen = 0;
                var fileLen = f.Length;
                for (int i = 0; i < N; ++i)
                {
                    int len = rnd.Next(minlen, stub.Length);
                    f.Position = rnd.Next(0, (int)(fileLen - len));
                    totalLen += f.Read(stub, 0, len);
                }
                f.Flush();
                t.Stop();
                Console.WriteLine("Random Access Reading: " + t.Elapsed + " (" + (totalLen / 1024.0 / 1024.0 / t.Elapsed.TotalSeconds).ToString("### ###.00") + " MB/sec)");
            }


            using (var f = new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 4 * 1024))
            {
                Stopwatch t = new Stopwatch();
                t.Start();
                long totalLen = 0;
                var fileLen = f.Length;
                for (int i = 0; i < N; ++i)
                {
                    int len = rnd.Next(minlen, stub.Length);
                    f.Position = rnd.Next(0, (int)(fileLen - len));
                    totalLen += f.Read(stub, 0, len);
                }
                f.Flush();
                t.Stop();
                Console.WriteLine("Random Access Reading with buffer: " + t.Elapsed + " (" + (totalLen / 1024.0 / 1024.0 / t.Elapsed.TotalSeconds).ToString("### ###.00") + " MB/sec)");
            }
        }
    }

    static class Extentions
    {
        public static int[] NextN(this Random rnd, int maxValue, int count)
        {
            int[] randoms = new int[count];
            for (int i = 0; i < count; ++i)
                randoms[i] = rnd.Next(maxValue);
            return randoms;
        }

        public static T[] Select<T>(this T[] array, int[] positions)
        {
            return positions.Select(i => array[i]).ToArray();
        }
    }

}
