using System;
using System.Collections.Generic;
using System.Linq;

namespace FsDb
{
    public class FsIndexedStorage<TKey> : IDisposable
    {
        private FsStorage db;
        private FsIndex<TKey> index;
 
        public FsIndexedStorage(FsStorage db, FsIndex<TKey> index)
        {
            this.db = db;
            this.index = index;
        }

        public bool Contains(TKey key)
        {
            return index.Get(key) != null;
        }

        public byte[] Get(TKey key)
        {
            var ptr = index.Get(key);
            if (ptr == null) return null;
            return db.Load(ptr);

        }

        public IEnumerable<Tuple<TKey, byte[]>> GetBatch(IEnumerable<TKey> keys, int batchSizeInMb = 1024)
        {
            Dictionary<IPtr, List<TKey>> reversed = new Dictionary<IPtr, List<TKey>>();
            List<TKey> notPresented = new List<TKey>();
            foreach (var key in keys)
            {
                var ptr = index.Get(key);
                if (ptr == null) notPresented.Add(key);
                else
                {
                    if (!reversed.ContainsKey(ptr))
                        reversed[ptr] = new List<TKey>();
                    reversed[ptr].Add(key);
                }
            }
            foreach (var item in db.BatchLoad(reversed.Keys.ToArray(), batchSizeInMb))
                foreach (var key in reversed[item.Item1])
                    yield return Tuple.Create(key, item.Item2);
            foreach (var key in notPresented)
                yield return Tuple.Create(key, (byte[])null);

        }

        public void Save(TKey key, byte[] data)
        {
            var ptr = index.Get(key);
            var newptr = db.Save(data, ptr);
            index.Set(key, newptr);
        }

        public void SaveBatch(IEnumerable<Tuple<TKey, byte[]>> data, int batchSizeInMb = 1024)
        {
            List<Tuple<IPtr, byte[]>> batch = new List<Tuple<IPtr, byte[]>>();
            List<TKey> keysInBatch = new List<TKey>();
            long bytesInBatch = 0;
            Action store = () =>
            {

                var newptrs = db.BatchSave(batch.ToArray());
                for (int i = 0; i < newptrs.Length; ++i)
                    index.Set(keysInBatch[i], newptrs[i]);
                bytesInBatch = 0;
                keysInBatch.Clear();
                batch.Clear();
            };
            foreach (var item in data)
            {
                keysInBatch.Add(item.Item1);
                var ptr = index.Get(item.Item1);
                batch.Add(Tuple.Create(ptr, item.Item2));
                bytesInBatch += item.Item2.Length;
                if (bytesInBatch / (1024 * 1024) >= batchSizeInMb)
                    store();
            }
            if (batch.Count > 0)
                store();
        }

        public void Remove(TKey key)
        {
            index.Remove(key);
        }

        public void Flush()
        {
            db.Flush();
            index.Flush();
        }

        public void Dispose()
        {
            index.Dispose();
            db.Dispose();
        }
    }
}