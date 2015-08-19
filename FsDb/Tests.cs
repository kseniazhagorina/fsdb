using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using NUnit.Framework;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FsDb
{

    [TestFixture]
    [TestClass]
    class WritePerformanceTest
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


    class StorageTest
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

        protected TimeSpan MeasureTimeOfThreas(int NThreads, Action<int> action)
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

    [TestClass]
    class FsStorageTest : StorageTest
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
                    var tsave = MeasureTimeOfThreas(NThreads, save);
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
                    var tread = MeasureTimeOfThreas(NThreads, read);
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
                    var time = MeasureTimeOfThreas(NThreads, action);
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
                    var tsave = MeasureTimeOfThreas(NThreads, save);
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
                    var tread = MeasureTimeOfThreas(NThreads, read);
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
                    var time = MeasureTimeOfThreas(NThreads, action);
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
    }

    [TestClass]
    class FsIndexTest : StorageTest
    {
        private FsIndex<int> index;
        private int N = 10000;

        private class PtrStub : IPtr
        {
            public int ptr;

            public byte[] Serialize()
            {
                return BitConverter.GetBytes(ptr);
            }

            public void Deserialize(byte[] value)
            {
                ptr = BitConverter.ToInt32(value, 0);
            }
        }

        private FsIndex<int> CreateNewIndex(bool clear)
        {
            if (clear)
                Clear();
            return new FsIndex<int>(dir, prefix, () => new PtrStub(), key => BitConverter.GetBytes(key), bytes => BitConverter.ToInt32(bytes,0));
        }

        private void Set(int key, bool useprev)
        {
            
            IPtr newptr = new PtrStub() {ptr = rnd.Next()};
            IPtr ptr;
            if (!control_index.TryGetValue(key, out ptr) || !useprev)
                ptr = newptr;
            control_index[key] = ptr;
            index.Set(key, ptr);
        }

        private void Set(params int[] keys)
        {
            foreach (var key in keys)
                Set(key, true);
        }

        //private Func<FsStorage.Ptr, string> tostr = ptr => ptr == null ? "null" : String.Format("ptr({0},{1},{2})",ptr.Capacity, ptr.FileNum, ptr.Position); 
        private void Get(int key)
        {
           
            var ptr = index.Get(key);
            IPtr control;
            control_index.TryGetValue(key, out control);
            if (control == null) Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsNull(ptr);
            else Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsTrue(control.Equals(ptr));
            
        }

        private void Get(params int[] keys)
        {
            foreach (var key in keys)
                Get(key);
        }

        private void Remove(int key)
        {
            index.Remove(key);
            control_index.Remove(key);
            var ptr = index.Get(key);
            Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsNull(ptr);
        }

        [Test]
        [TestMethod]
        public void SimpleGetSaveRemoveIndexTest()
        {
            using (index = CreateNewIndex(true))
            {

                for (int i = 0; i < N; ++i)
                {
                    int key = rnd.Next(10);
                    if (i < 20 || i%5 == 0) Set(key, rnd.Next()%5 == 0);
                    else if (i%5 == 1) Remove(key);
                    else Get(key);

                    if (i%1000 == 0) index.Flush();
                }

                Get(200);
                Set(200, false);
                Get(200);
                Set(200, true);
                Get(200);
                Remove(200);
                Get(200);
                Set(200, true);
                Get(200);
                Set(200, false);
                Get(200);
                index.Flush();

            }

            using (index = CreateNewIndex(false))
            {
                for (int i = 0; i < N; ++i)
                {
                    int key = rnd.Next(1000);
                    if (i < 2000 || i%5 == 0) Set(key, rnd.Next()%5 == 0);
                    else if (i%5 == 1) Remove(key);
                    else Get(key);

                    if (i%1000 == 0) index.Flush();
                }
                index.Flush();

            }
        }

        [Test]
        [TestMethod]
        public void KeysCountIndexTest()
        {

            Action Control = () =>
            {
                Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsTrue(index.Count == control_index.Count);
                var keys = index.Keys.Distinct().ToArray();
                Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsTrue(keys.Length == control_index.Count);
                foreach (var key in keys)
                    Microsoft.VisualStudio.TestTools.UnitTesting.Assert.IsTrue(control_index.ContainsKey(key));
            };
            
            using (index = CreateNewIndex(true))
            {
                Set(0, 1, 3, 4);
                Control();
                Remove(4);
                Set(5, true);
                Set(0, false);
                Get(0, 1,2,3,4,5,6);
                Control();
                index.Flush();
            }

            using (index = CreateNewIndex(false))
            {
                Control();
                Remove(2);
                Remove(0);
                Control();
                Set(0, 1);
                Control();
                index.Flush();
            }
        }

        [Test]
        [TestMethod]
        public void PerformanceTest()
        {
            int N, NThreads, NKeys;
            bool SAVETEST = true, READTEST = true;

            NKeys = 5000;

            PtrStub[] ptrs = new PtrStub[NKeys];
            for (int i = 0; i < ptrs.Length; ++i)
                ptrs[i] = new PtrStub() {ptr = i};


            using (var index = CreateNewIndex(true))
            {
                for (int i=0; i<NKeys; ++i)
                    index.Set(i, ptrs[i]);


                if (SAVETEST)
                {
                    N = 100000;
                    NThreads = 10;
                    Action<int> save = (tid) =>
                    {
                        for (int i = 0; i < N; ++i)
                        {
                            int key = rnd.Next(NKeys);
                            index.Set(key, ptrs[key]);
                        }
                    };
                    var tsave = MeasureTimeOfThreas(NThreads, save);
                    Console.WriteLine("" + (N*NThreads) + " save operations in " + NThreads + " threads elapsed: " + tsave);
                    Console.WriteLine("Save Operations in 1 sec: " + ((double) N*NThreads/tsave.TotalSeconds).ToString("### ### ###"));
                }
                

                if (READTEST)
                {
                    N = 100000;
                    NThreads = 10;
                    Action<int> read = (tid) =>
                    {
                        for (int i = 0; i < N; ++i)
                        {
                            int key = rnd.Next(NKeys);
                            var ptr = index.Get(key);
                        }
                    };
                    var tread = MeasureTimeOfThreas(NThreads, read);
                    Console.WriteLine("" + (N*NThreads) + " read operations in " + NThreads + " threads elapsed: " +
                                      tread);
                    Console.WriteLine("Read Operations in 1 sec: " + ((double)N * NThreads / tread.TotalSeconds).ToString("### ### ###"));
                }

                if (SAVETEST && READTEST)
                {
                    N = 100000;
                    int NReadThreads = 8, NWriteThreads = 2;
                    NThreads = NReadThreads + NWriteThreads;
                    Action<int> action = (tid) =>
                    {
                        for (int i = 0; i < N; ++i)
                        {
                            int key = rnd.Next(NKeys);
                            if (tid < NWriteThreads) index.Get(key);
                            else index.Set(key, ptrs[key]);
                        }
                    };
                    var time = MeasureTimeOfThreas(NThreads, action);
                    Console.WriteLine("" + (N * NReadThreads) + " read operations in " + NReadThreads + " threads and \n"+
                                      "" + (N*NWriteThreads) + " write operations in " +NWriteThreads +" threads elapsed: " +
                                      time);
                    Console.WriteLine("Read|Write Operations in 1 sec: " + ((double)N * NThreads / time.TotalSeconds).ToString("### ### ###"));
                }
            }
        }
    }

    [TestClass]
    class FsIndexedStorageTest : StorageTest
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
                    var tsave = MeasureTimeOfThreas(NThreads, save);
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
                    var tread = MeasureTimeOfThreas(NThreads, read);
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
                    var time = MeasureTimeOfThreas(NThreads, action);
                    Console.WriteLine("" + (N * NReadThreads) + " read operations in " + NReadThreads + " threads and \n" +
                                      "" + (N * NWriteThreads) + " write operations in " + NWriteThreads + " threads elapsed: " +
                                      time);
                    Console.WriteLine("Read|Write Operations in 1 sec: " + ((double)N * NThreads / time.TotalSeconds).ToString("### ###"));
                    Console.WriteLine("KBytes readed|writed in 1 sec: " +
                                      (KbytesProcessed.Sum() / time.TotalSeconds).ToString("### ### ### Kb"));
                }
            }
        }

    }

    


    [TestFixture]
    [TestClass]
    class HashTest
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
    class IOPerformanceTest
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
                f.Close();
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
