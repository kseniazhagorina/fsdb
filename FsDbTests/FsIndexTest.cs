using System;
using System.Linq;
using FsDb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NUnit.Framework;

namespace FsDbTests
{
    [TestClass]
    public class FsIndexTest : StorageTest
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
                    var tsave = MeasureTimeOfThredas(NThreads, save);
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
                    var tread = MeasureTimeOfThredas(NThreads, read);
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
                    var time = MeasureTimeOfThredas(NThreads, action);
                    Console.WriteLine("" + (N * NReadThreads) + " read operations in " + NReadThreads + " threads and \n"+
                                      "" + (N*NWriteThreads) + " write operations in " +NWriteThreads +" threads elapsed: " +
                                      time);
                    Console.WriteLine("Read|Write Operations in 1 sec: " + ((double)N * NThreads / time.TotalSeconds).ToString("### ### ###"));
                }
            }
        }
    }
}