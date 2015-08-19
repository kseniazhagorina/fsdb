using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;

namespace FsDb
{
    public class FsIndex<TKey> : IDisposable
    {

        #if DEBUG
        public event Action<TKey, IPtr> onGet = delegate { };
        public event Action<TKey, IPtr> onSet = delegate { };
        public event Action<TKey, IPtr> onRem = delegate { };
        #endif

        private FileStream indexFile;
        private ConcurrentDictionary<TKey, Idx> indexes = new ConcurrentDictionary<TKey, Idx>(); 
        private Func<TKey, byte[]> serializeKey;
        private Func<byte[], TKey> deserializeKey;
        private Func<IPtr> createPtr; 

        public FsIndex(string directory, string fileprefix, Func<IPtr> createPtr, Func<TKey, byte[]> serializeKey, Func<byte[], TKey> deserializeKey)
        {
            var indexFilePath = Path.Combine(directory, fileprefix + ".pidx");
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);
            indexFile = new FileStream(indexFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4*1024);
            this.serializeKey = serializeKey;
            this.deserializeKey = deserializeKey;
            this.createPtr = createPtr;

            indexFile.Position = 0;
            Idx idx;
            while (true)
            {
                var record = ReadNextIndexRecord();
                if (record == null) break;
                if (record.Item3) indexes[record.Item1] = record.Item2;
                else indexes.TryRemove(record.Item1, out idx);
            }
        }

        // Устройство индексного файла: 
        // Запись: 0/1 (действительна или нет), длинна ключа, ключ, длинна указателя, указатель.
        private class Idx
        {
            public IPtr Ptr;
            public long IndexFilePosition;
        }

        // Читает запись в файле индекса. Возвращает null в случае конца файла
        // Ключ - индекс - валиден или нет
        private Tuple<TKey, Idx, bool> ReadNextIndexRecord()
        {
            long fileposition = indexFile.Position;
            byte[] buf = new byte[3];
            if (indexFile.Read(buf, 0, 3) < 3) return null;
            bool isvalid = buf[0] > 0;
            int lenkey = BitConverter.ToInt16(buf, 1);
            byte[] keybuf = new byte[lenkey];
            if (indexFile.Read(keybuf, 0, lenkey) < lenkey) return null;
            if (indexFile.Read(buf, 0, 2) < 2) return null;
            int lenptr = BitConverter.ToInt16(buf, 0);
            byte[] ptrbuf = new byte[lenptr];
            if (indexFile.Read(ptrbuf, 0, lenptr) < lenptr) return null;
            
            var key = deserializeKey(keybuf);
            var ptr = createPtr();
            ptr.Deserialize(ptrbuf);
            return Tuple.Create(key, new Idx() {IndexFilePosition = fileposition, Ptr = ptr}, isvalid);
        }

        
        public int Count {get { return indexes.Count; }}
        public IEnumerable<TKey> Keys {get { return indexes.Keys; }}

        public IPtr Get(TKey key)
        {
            Idx idx;
            if (indexes.TryGetValue(key, out idx))
            {
                #if DEBUG
                onGet(key, idx.Ptr);
                #endif 
                return idx.Ptr;
            }

            #if DEBUG
            onGet(key, null);
            #endif
            return null;
        }

        public void Set(TKey key, IPtr ptr)
        {
            var keydata = serializeKey(key);
            var ptrdata = ptr.Serialize();
            var lenkey = BitConverter.GetBytes((short)keydata.Length);
            var lenptr = BitConverter.GetBytes((short)ptrdata.Length);
            

            long position;
            Idx newidx;
            lock (indexFile)
            {
                position = indexFile.Seek(0, SeekOrigin.End);
                indexFile.WriteByte(1);
                indexFile.Write(lenkey, 0, 2);
                indexFile.Write(keydata, 0, keydata.Length);
                indexFile.Write(lenptr, 0, 2);
                indexFile.Write(ptrdata, 0, ptrdata.Length);
                
                newidx = new Idx() { IndexFilePosition = position, Ptr = ptr };
                indexes.AddOrUpdate(key,  newidx, (key1, idx) => newidx);
            }
            
            // Нам вообще не важно, что было перед нами. 
            //Все новые записи добавляются в конец файла, т.е. при чтении файла - самая правильная та, что записана последней
            #if DEBUG
            onSet(key, newidx.Ptr);
            #endif
        }

        public void Remove(TKey key)
        {
            if (!indexes.ContainsKey(key)) return;
            Idx idx;
            lock(indexFile)
                if (indexes.TryRemove(key, out idx))
                {
                    indexFile.Position = idx.IndexFilePosition;
                    indexFile.WriteByte(0);
                    
                    #if DEBUG
                    onRem(key, idx.Ptr);
                    #endif
                }
        }

        public void Flush()
        {
            lock(indexFile)
                indexFile.Flush();
        }

        public void Dispose()
        {
            lock (indexFile)
            {
                indexFile.Close();
                indexFile.Dispose();
            }
        }

    }
}