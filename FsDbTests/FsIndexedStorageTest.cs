using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using FsDb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NUnit.Framework;

namespace FsDbTests
{
    [TestClass]
    public class FsIndexedStorageTest : StorageTest
    {
        FsIndexedStorage<int> storage;

        private FsIndexedStorage<int> CreateNewStorage(bool clear, long maxDbFileLength = 100 * 1024 /*100Mb*/)
        {
            if (clear)
                Clear();
            return new FsIndexedStorage<int>(
                new FsStorage(dir, prefix, maxDbFileLength: maxDbFileLength),
                new FsIndex<int>(dir, prefix, FsStorage.CreatePtr, BitConverter.GetBytes, bytes => BitConverter.ToInt32(bytes, 0)));
        }

        private List<string> operations;

        private FsIndexedStorage<int> CreateNewStorageWithLogging(bool clear)
        {
            
            if (clear)
            {
                operations = new List<string>();
                Clear();
            }
            var db = new FsStorage(dir, prefix, maxDbFileLength: 100 * 1024);
            var index = new FsIndex<int>(dir, prefix, FsStorage.CreatePtr, BitConverter.GetBytes, bytes => BitConverter.ToInt32(bytes, 0));
#if DEBUG
            db.onReading += (ptr, bytes) => { Thread.Sleep(rnd.Next(200)); lock (operations) operations.Add(String.Format("[{0}] read from ptr {1} value {2}", Thread.CurrentThread.ManagedThreadId, ptr.ToString(), BitConverter.ToInt32(bytes, 0))); };
            db.onWriting += (ptr, bytes) => { Thread.Sleep(rnd.Next(200)); lock (operations) operations.Add(String.Format("[{0}] write to ptr {1} value {2}", Thread.CurrentThread.ManagedThreadId, ptr.ToString(), BitConverter.ToInt32(bytes, 0))); };

            index.onGet += (key, ptr) => { Thread.Sleep(rnd.Next(200)); lock (operations) operations.Add(String.Format("[{0}] get by key {1} ptr {2}", Thread.CurrentThread.ManagedThreadId, key, ptr != null ? ptr.ToString() : "")); };
            index.onSet += (key, ptr) => { Thread.Sleep(rnd.Next(200)); lock (operations) operations.Add(String.Format("[{0}] set by key {1} ptr {2}", Thread.CurrentThread.ManagedThreadId, key, ptr.ToString())); };
            index.onRem += (key, ptr) => { Thread.Sleep(rnd.Next(200)); lock (operations) operations.Add(String.Format("[{0}] rem key {1} with ptr {2}", Thread.CurrentThread.ManagedThreadId, key, ptr.ToString())); };
#endif
            return new FsIndexedStorage<int>(db, index);
        }



        private void Get(int key)
        {
            byte[] control;
            control_storage.TryGetValue(key, out control);
            byte[] data = storage.Get(key);
            if (control == null) Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsNull(data);
            else Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsTrue(control.SequenceEqual(data));
        }

        private void Save(int key, int len)
        {
            var data = CreateData(key, len);
            storage.Save(key, data);
            control_storage[key] = data;
        }

        private void Remove(int key)
        {
            storage.Remove(key);
            control_storage[key] = null;
            Get(key);
        }

        protected void MakeOperationsWithStorage(int N)
        {
            for (int i = 0; i < N; ++i)
            {
                int key = rnd.Next(1000);
                if (i < 2000 || i % 5 == 0) Save(key, rnd.Next(5000));
                else if (i % 5 == 1) Remove(key);
                else Get(key);

                if (i % 1000 == 0) storage.Flush();
            }
        }

        [Test]
        [TestMethod]
        public void SimpleGetSaveRemoveTest()
        {
            int N = 100000;
            // Начинаем с чистой базы
            using (storage = CreateNewStorage(true))
            {
                MakeOperationsWithStorage(N);
                storage.Dispose();
            }

            // Загружаем данные с диска
            using (storage = CreateNewStorage(false))
            {
                MakeOperationsWithStorage(N);
                storage.Dispose();
            }
        }

        [Test]
        [TestMethod]
        public void MultithreadTest()
        {
            int N = 100;
            using (storage = CreateNewStorageWithLogging(true))
            {
                Action<int> action = (tid) =>
                {

                    for (int i = 0; i < N; ++i)
                    {
                        int key = i%5;

                        var v = (i + tid)%3;
                        if (i < 20 || v == 0)
                            storage.Save(key,
                                CreateData(key*100 + Thread.CurrentThread.ManagedThreadId%100, rnd.Next(100)*4));
                        else if (v == 1) storage.Remove(key);
                        else storage.Get(key);

                        if (i%50 == 0) storage.Flush();
                    }
                };

                Thread[] threads = new Thread[5];
                for (int i = 0; i < threads.Length; ++i)
                {
                    var tid = i;
                    threads[i] = new Thread(new ThreadStart(() => action(tid)));
                }
                foreach (var thread in threads)
                    thread.Start();
                foreach (var thread in threads)
                    thread.Join();

                File.WriteAllLines("output.fsstorage.multithreadtest.txt", operations);
            }

        }

        [Test]
        [TestMethod]
        public void PerformanceTest()
        {
            int NKeys = 500000;
            int N, NThreads;
            bool SAVETEST = true, READTEST = true;

            byte[][] values = new byte[NKeys][];
            for (int i = 0; i < values.Length; ++i)
                values[i] = CreateData(i, 100 + rnd.Next(5000));



            using (storage = CreateNewStorage(true, 1L * 1024 * 1024 * 1024))
            {
                for (int i = 0; i < NKeys; ++i)
                    storage.Save(i, values[rnd.Next(NKeys)]);


                if (SAVETEST)
                {
                    N = 100000;
                    NThreads = 10;
                    double[] KbytesSaved = Enumerable.Repeat(0.0, NThreads).ToArray();
                    Action<int> save = (tid) =>
                    {
                        for (int i = 0; i < N; ++i)
                        {
                            int key = rnd.Next(NKeys);
                            var value = values[key];
                            storage.Save(key, value);
                            KbytesSaved[tid] += value.Length / 1024.0;

                        }
                    };
                    var tsave = MeasureTimeOfThredas(NThreads, save);
                    Console.WriteLine("" + (N * NThreads) + " save operations in " + NThreads + " threads elapsed: " +
                                      tsave);
                    Console.WriteLine("Save Operations in 1 sec: " + ((double)N * NThreads / tsave.TotalSeconds).ToString("### ###"));
                    Console.WriteLine("KBytes saved in 1 sec: " +
                                      (KbytesSaved.Sum() / tsave.TotalSeconds).ToString("### ### ### Kb"));
                }


                if (READTEST)
                {
                    N = 100000;
                    NThreads = 10;
                    double[] KbytesReaded = Enumerable.Repeat(0.0, NThreads).ToArray();
                    Action<int> read = (tid) =>
                    {
                        for (int i = 0; i < N; ++i)
                        {
                            int key = rnd.Next(NKeys);
                            var value = storage.Get(key);
                            KbytesReaded[tid] += value != null ? value.Length / 1024.0 : 0;
                        }
                    };
                    var tread = MeasureTimeOfThredas(NThreads, read);
                    Console.WriteLine("" + (N * NThreads) + " read operations in " + NThreads + " threads elapsed: " +
                                      tread);
                    Console.WriteLine("Read Operations in 1 sec: " + ((double)N * NThreads / tread.TotalSeconds).ToString("### ###"));
                    Console.WriteLine("KBytes readed in 1 sec: " +
                                      (KbytesReaded.Sum() / tread.TotalSeconds).ToString("### ### ### Kb"));
                }

                if (SAVETEST && READTEST)
                {
                    N = 100000;
                    int NReadThreads = 8, NWriteThreads = 2;
                    NThreads = NReadThreads + NWriteThreads;
                    double[] KbytesProcessed = Enumerable.Repeat(0.0, NThreads).ToArray();
                    Action<int> action = (tid) =>
                    {
                        for (int i = 0; i < N; ++i)
                        {
                            int key = rnd.Next(NKeys);
                            byte[] value;
                            if (tid < NWriteThreads) value = storage.Get(key);
                            else
                            {
                                value = values[rnd.Next(NKeys)];
                                storage.Save(key, value);
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
                }
            }
        }

        [Test]
        [TestMethod]
        public void BatchGetSaveTest()
        {
            int NKeys = 500000;
            byte[][] data = new byte[NKeys][];
            bool[] isSaved = new bool[NKeys];

            using (storage = CreateNewStorage(true)){}
            for (int k=0; k<3; ++k)
                using (storage = CreateNewStorage(false))
                {
                    List<Tuple<int, byte[]>> dataToSave = new List<Tuple<int, byte[]>>();
                    for (int i=0; i<NKeys; ++i)
                        if (rnd.NextDouble() < 0.33)
                        {
                            data[i] = CreateData(-rnd.Next(NKeys), 50 + rnd.Next(1000));
                            dataToSave.Add(new Tuple<int, byte[]>(i, data[i]));
                            isSaved[i] = true;
                        }
                    storage.SaveBatch(dataToSave.ToArray());
                    for(int i=0; i<NKeys; ++i)
                        if (isSaved[i]) Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsTrue(data[i].SequenceEqual(storage.Get(i)));
                        else Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsFalse(storage.Contains(i));

                    List<int> dataToGet = new List<int>();
                    for (int i = 0; i < NKeys; ++i)
                        if (rnd.NextDouble() < 0.33)
                            dataToGet.Add(i);
                    var got = storage.GetBatch(dataToGet);
                    foreach (var item in got)
                        if (data[item.Item1] != null) Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsTrue(data[item.Item1].SequenceEqual(item.Item2));

                    storage.Flush();
                }
        }

    }
}