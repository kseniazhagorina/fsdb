using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using FsDb;

namespace FsDbTests
{
    public class StorageTest
    {
        protected string dir = @"D:\ProjectData\FsDb\testdb";
        protected string prefix = "tdb";
        protected Random rnd = new Random(123415);

        protected Dictionary<int, byte[]> control_storage;
        protected Dictionary<int, IPtr> control_index;

        protected void Clear()
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
            control_index = new Dictionary<int, IPtr>();
            control_storage = new Dictionary<int, byte[]>();
        }

        protected byte[] CreateData(int val, int len)
        {
            byte[] v = BitConverter.GetBytes(val);
            byte[] data = new byte[len];
            rnd.NextBytes(data);
            for (int j = 0; j < data.Length && j< 4; ++j)
                data[j] = v[j];
            return data;
        }

        protected TimeSpan MeasureTimeOfThredas(int NThreads, Action<int> action)
        {
            Thread[] threads = new Thread[NThreads];
            for (int i = 0; i < threads.Length; ++i)
            {
                var tid = i;
                threads[i] = new Thread(new ThreadStart(() => action(tid)));
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();
            foreach (var thread in threads)
                thread.Start();
            foreach (var thread in threads)
                thread.Join();
            sw.Stop();
            return sw.Elapsed;
        }
    }
}