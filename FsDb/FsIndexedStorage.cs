using System;

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

        public byte[] Get(TKey key)
        {
            var ptr = index.Get(key);
            if (ptr == null) return null;
            return db.Load(ptr);

        }

        public void Save(TKey key, byte[] data)
        {
            var ptr = index.Get(key);
            var newptr = db.Save(data, ptr);
            index.Set(key, newptr);
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