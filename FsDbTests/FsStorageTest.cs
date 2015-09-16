using System;
using System.Collections.Generic;
using System.Linq;
using FsDb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NUnit.Framework;

namespace FsDbTests
{
    [TestClass]
    public class FsStorageTest : StorageTest
    {
       
        private FsStorage storage;
        private int N = 10000;

        
        private FsStorage CreateNewStorage(bool clear)
        {
            if (clear)
                Clear();
            return new FsStorage(dir, prefix, maxDbFileLength: 500*1024*1024 /*500Mb*/);
        }


        public void Store(int key, int salt)
        {
            byte[] v = BitConverter.GetBytes(key ^ salt);
            byte[] data = new byte[key * 4];
            for (int j = 0; j < data.Length; ++j)
                data[j] = v[j % 4];
            IPtr oldptr;
            control_index.TryGetValue(key, out oldptr);
            var ptr = storage.Save(data, oldptr);
            control_index[key] = ptr;
            control_storage[key] = data;
        }

        public void Read(int key)
        {
            IPtr ptr;
            if (control_index.TryGetValue(key, out ptr))
            {
                byte[] data = storage.Load(ptr);
                var control = control_storage[key];
                if (control == null) 
                    Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsNull(data);
                else
                {
                    Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsNotNull(data);
                    Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsTrue(data.SequenceEqual(control));
                }
            }
        }

        [Test]
        [TestMethod]
        public void SimpleSaveLoadTest()
        {
            using (storage = CreateNewStorage(true))
            {
                for (int i = 0; i < N; ++i)
                {
                    int key = rnd.Next(100);
                    if (i < 200 || i%2 == 0) Store(key, rnd.Next());
                    else Read(key);

                    if (i%1000 == 0) storage.Flush();
                }

                Read(200);
                Store(200, 1240281);
                Read(200);
                Store(200, 291483);
                Read(200);

            }

            using (storage = CreateNewStorage(false))
            {
                for (int i = 0; i < N; ++i)
                {
                    int key = rnd.Next(100);
                    if (i < 200 || i%2 == 0) Store(key, rnd.Next());
                    else Read(key);

                    if (i%1000 == 0) storage.Flush();
                }

                // test wrong ptr
                /*
            IPtr wrong = new FsStorage.Ptr() {Capacity = 3600, FileNum = 0, Position = 140};
            control_index[1001] = wrong;
            control_storage[1001] = null;
            Read(1001);
             */

                int big = 1024*1024*2/4;
                Read(big);
                Store(big, rnd.Next());
                Read(big);

            }
        }

        [Test]
        [TestMethod]
        public void PerformanceTest()
        {
            int N, NThreads, NKeys;
            bool SAVETEST = true, READTEST = true;

            NKeys = 5000;
            byte[][] data = new byte[NKeys][];
            IPtr[] ptrs = new IPtr[NKeys];

            N = 100000;
            NThreads = 10;
            int NReadThreads = 8, NWriteThreads = 2;

            using (storage = CreateNewStorage(true))
            {
                for (int i = 0; i < data.Length; ++i)
                {
                    data[i] = CreateData(i, 100 + rnd.Next(5000));
                    ptrs[i] = storage.Save(data[i]);
                }


                if (SAVETEST)
                {
                    
                    double[] KbytesSaved = Enumerable.Repeat(0.0, NThreads).ToArray();
                    Action<int> save = (tid) =>
                    {
                        for (int i = 0; i < N; ++i)
                        {
                            int key = rnd.Next(NKeys);
                            var value = data[rnd.Next(NKeys)];
                            storage.Save(value, ptrs[key]);
                            KbytesSaved[tid] += value.Length / 1024.0;

                        }
                    };
                    var tsave = MeasureTimeOfThredas(NThreads, save);
                    Console.WriteLine("" + (N * NThreads) + " save operations in " + NThreads + " threads elapsed: " +
                                      tsave);
                    Console.WriteLine("Save Operations in 1 sec: " + ((double)N * NThreads / tsave.TotalSeconds).ToString("### ###"));
                    Console.WriteLine("KBytes saved in 1 sec: " +
                                      (KbytesSaved.Sum() / tsave.TotalSeconds).ToString("### ### ### Kb"));
                    Console.WriteLine("Total KBytes saved: " + KbytesSaved.Sum());
                }


                if (READTEST)
                {
                    double[] KbytesReaded = Enumerable.Repeat(0.0, NThreads).ToArray();
                    Action<int> read = (tid) =>
                    {
                        for (int i = 0; i < N; ++i)
                        {
                            int key = rnd.Next(NKeys);
                            var value = storage.Load(ptrs[key]);
                            KbytesReaded[tid] += value != null ? value.Length / 1024.0 : 0;
                        }
                    };
                    var tread = MeasureTimeOfThredas(NThreads, read);
                    Console.WriteLine("" + (N * NThreads) + " read operations in " + NThreads + " threads elapsed: " +
                                      tread);
                    Console.WriteLine("Read Operations in 1 sec: " + ((double)N * NThreads / tread.TotalSeconds).ToString("### ###"));
                    Console.WriteLine("KBytes readed in 1 sec: " +
                                      (KbytesReaded.Sum() / tread.TotalSeconds).ToString("### ### ### Kb"));
                    Console.WriteLine("Total KBytes readed: " + KbytesReaded.Sum());
                }

                if (SAVETEST && READTEST)
                {
                    NThreads = NReadThreads + NWriteThreads;
                    double[] KbytesProcessed = Enumerable.Repeat(0.0, NThreads).ToArray();
                    Action<int> action = (tid) =>
                    {
                        for (int i = 0; i < N; ++i)
                        {
                            int key = rnd.Next(NKeys);
                            byte[] value;
                            if (tid < NWriteThreads) value = storage.Load(ptrs[key]);
                            else
                            {
                                
                                value = data[rnd.Next(NKeys)];
                                storage.Save(value, ptrs[key]);
                            }
                            KbytesProcessed[tid] += value != null ? value.Length / 1024.0 : 0;
                        }
                    };
                    var time = MeasureTimeOfThredas(NThreads, action);
                    Console.WriteLine("" + (N * NReadThreads) + " read operations in " + NReadThreads + " threads and \n" +
                                      "" + (N * NWriteThreads) + " write operations in " + NWriteThreads + " threads elapsed: " +
                                      time);
                    Console.WriteLine("Read|Write Operations in 1 sec: " + ((double)N * NThreads / time.TotalSeconds).ToString("### ###"));
                    Console.WriteLine("KBytes readed|writed in 1 sec: " +
                                      (KbytesProcessed.Sum() / time.TotalSeconds).ToString("### ### ### Kb"));
                    Console.WriteLine("Total KBytes processed: " + KbytesProcessed.Sum());
                }
            }
        }


        [Test]
        [TestMethod]
        public void PerformanceBatchTest()
        {
            int N, NThreads, NBatch, NKeys;
            bool SAVETEST = true, READTEST = true;

            NKeys = 5000;
            byte[][] data = new byte[NKeys][];
            IPtr[] ptrs = new IPtr[NKeys];

            N = 100000;
            NThreads = 10;
            NBatch = 5000;
            int NReadThreads = 8, NWriteThreads = 2;

            using (storage = CreateNewStorage(true))
            {
                for (int i = 0; i < data.Length; ++i)
                {
                    data[i] = CreateData(i, 100 + rnd.Next(5000));
                    ptrs[i] = storage.Save(data[i]);
                }


                if (SAVETEST)
                {
                    double[] KbytesSaved = Enumerable.Repeat(0.0, NThreads).ToArray();
                    Action<int> save = (tid) =>
                    {
                        for (int i = 0; i < N; i+= NBatch)
                        {
                            Tuple<IPtr, byte[]>[] values = Enumerable.Repeat(0, NBatch).Select(_ => Tuple.Create(ptrs[rnd.Next(NKeys)], data[rnd.Next(NKeys)])).ToArray();
                            storage.BatchSave(values);
                            KbytesSaved[tid] += values.Sum(v => (long)v.Item2.Length) / 1024.0;
                        }
                    };
                    var tsave = MeasureTimeOfThredas(NThreads, save);
                    Console.WriteLine("" + (N * NThreads) + " save operations in " + NThreads + " threads elapsed: " +
                                      tsave);
                    Console.WriteLine("Save Operations in 1 sec: " + ((double)N * NThreads / tsave.TotalSeconds).ToString("### ###"));
                    Console.WriteLine("KBytes saved in 1 sec: " +
                                      (KbytesSaved.Sum() / tsave.TotalSeconds).ToString("### ### ### Kb"));
                    Console.WriteLine("Total KBytes saved: "+KbytesSaved.Sum());
                }


                if (READTEST)
                {
                    double[] KbytesReaded = Enumerable.Repeat(0.0, NThreads).ToArray();
                    Action<int> read = (tid) =>
                    {
                        for (int i = 0; i < N; i+= NBatch)
                        {
                            var keys = rnd.NextN(NKeys, NBatch).Select(key => ptrs[key]).ToArray();
                            var values = storage.BatchLoad(keys);
                            KbytesReaded[tid] += values.Sum(v => v != null ? (long)v.Item2.Length : (long)0) / 1024.0;
                        }
                    };
                    var tread = MeasureTimeOfThredas(NThreads, read);
                    Console.WriteLine("" + (N * NThreads) + " read operations in " + NThreads + " threads elapsed: " +
                                      tread);
                    Console.WriteLine("Read Operations in 1 sec: " + ((double)N * NThreads / tread.TotalSeconds).ToString("### ###"));
                    Console.WriteLine("KBytes readed in 1 sec: " +
                                      (KbytesReaded.Sum() / tread.TotalSeconds).ToString("### ### ##0 Kb"));
                    Console.WriteLine("Total KBytes readed: " + KbytesReaded.Sum());
                }

                if (SAVETEST && READTEST)
                {
                    
                    NThreads = NReadThreads + NWriteThreads;
                    double[] KbytesProcessed = Enumerable.Repeat(0.0, NThreads).ToArray();
                    Action<int> action = (tid) =>
                    {
                        for (int i = 0; i < N; i+=NBatch)
                        {
 
                            Tuple<IPtr,byte[]>[] values;
                            if (tid < NWriteThreads)
                            {
                                var keys = rnd.NextN(NKeys, NBatch).Select(key => ptrs[key]).ToArray();
                                values = storage.BatchLoad(keys).ToArray();
                            }
                            else
                            {
                                values = Enumerable.Repeat(0, NBatch).Select(_ => Tuple.Create(ptrs[rnd.Next(NKeys)], data[rnd.Next(NKeys)])).ToArray();
                                storage.BatchSave(values);
                            }
                            KbytesProcessed[tid] += values.Sum(v => v!=null ? (long)v.Item2.Length : (long)0) / 1024.0;
                        }
                    };
                    var time = MeasureTimeOfThredas(NThreads, action);
                    Console.WriteLine("" + (N * NReadThreads) + " read operations in " + NReadThreads + " threads and \n" +
                                      "" + (N * NWriteThreads) + " write operations in " + NWriteThreads + " threads elapsed: " +
                                      time);
                    Console.WriteLine("Read|Write Operations in 1 sec: " + ((double)N * NThreads / time.TotalSeconds).ToString("### ###"));
                    Console.WriteLine("KBytes readed|writed in 1 sec: " +
                                      (KbytesProcessed.Sum() / time.TotalSeconds).ToString("### ### ### Kb"));
                    Console.WriteLine("Total KBytes processed: " + KbytesProcessed.Sum());
                }
            }
        }

        [Test]
        [TestMethod]
        public void BatchSaveTest()
        {
            Dictionary<int, byte[]> data = new Dictionary<int, byte[]>();
            Dictionary<int, IPtr> ptrs = new Dictionary<int, IPtr>();
            using (storage = CreateNewStorage(true))
            {
                data[1] = CreateData(1, 100);
                data[2] = CreateData(2, 200);
                data[3] = CreateData(3, 300);
                ptrs[1] = storage.Save(data[1]);
                ptrs[2] = storage.Save(data[2]);
                var p = storage.BatchSave(new Tuple<IPtr, byte[]>[]
                {
                    new Tuple<IPtr, byte[]>(ptrs[1], data[1]),
                    new Tuple<IPtr, byte[]>(ptrs[2], data[2]),
                    new Tuple<IPtr, byte[]>(null, data[3]),
                });
                for (int i = 0; i < p.Length; ++i)
                    ptrs[i + 1] = p[i];
                foreach (var key in data.Keys)
                    Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsTrue(
                        data[key].SequenceEqual(storage.Load(ptrs[key])));


            }
        }
    }
}