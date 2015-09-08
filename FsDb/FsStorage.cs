using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace FsDb
{

    public interface IPtr
    {
        byte[] Serialize();
        void Deserialize(byte[] value);
    }

    /// <summary>
    /// Storage for binary dataitems in file system.
    /// Support: multithreading, checksum (MD5CryptoServiceProvider),
    /// dataitems with not restricted length, updating dataitems by pointer (in case where there are enough place)
    /// </summary>
    public class FsStorage : IDisposable
    {
        #if DEBUG
        public event Action<IPtr, byte[]> onWriting = delegate {};
        public event Action<IPtr, byte[]> onReading = delegate {};
        #endif

        /// <summary>
        /// Use this function to create specific for this storage pointers
        /// </summary>
        public static IPtr CreatePtr() { return new Ptr(); }

        private class Ptr : IPtr
        {
            internal int Capacity; //lenght of free place by pointer in bytes
            internal short FileNum; 
            internal long Position;

            public Ptr(){}
            internal Ptr(int capacity, short filenum, long position)
            {
                this.Capacity = capacity;
                this.FileNum = filenum;
                this.Position = position;
            }

            public override string ToString()
            {
                return String.Format("({0},{1},{2})", FileNum, Position, Capacity);
            }

            public byte[] Serialize()
            {
                var c = BitConverter.GetBytes(Capacity);
                var f = BitConverter.GetBytes(FileNum);
                var p = BitConverter.GetBytes(Position);
                byte[] result = new byte[4+2+8];
                Array.Copy(c, 0, result, 0, 4);
                Array.Copy(f, 0, result, 4, 2);
                Array.Copy(p, 0, result, 6, 8);
                return result;
            }

            public void Deserialize(byte[] value)
            {
                Capacity = BitConverter.ToInt32(value, 0);
                FileNum = BitConverter.ToInt16(value, 4);
                Position = BitConverter.ToInt64(value, 6);
            }
        }

        private class DbFileResource : IDisposable
        {
            public FileStream stream;
            public MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider();
            public long length;

            public void Dispose()
            {
                if (stream != null) stream.Dispose();
                stream = null;
                if (md5 != null) md5.Dispose();
                md5 = null;
            }
        }
        
        private string dbDir;
        private string dbFilePrefix;
        private List<DbFileResource> dbFiles = new List<DbFileResource>();

        private readonly int minRecordLen = 100;
        private readonly long maxDbFileLength = 4 * 1024 * 1024 * 1024L; //4GB

        public FsStorage(string directory, string filesprefix, int minRecordLen = 100, long maxDbFileLength = 4*1024*1024*1024L /*4Gb*/)
        {
            this.dbDir = directory;
            this.dbFilePrefix = filesprefix;
            this.minRecordLen = minRecordLen;
            this.maxDbFileLength = maxDbFileLength;

            if (!Directory.Exists(dbDir))
                Directory.CreateDirectory(dbDir);
            for (int i = 0; ; ++i)
            {
                var file = OpenDbFile(i, createIfNotExist: i == 0);
                if (file != null) dbFiles.Add(file);
                else break;
            }
        }

        private DbFileResource OpenDbFile(int fileNum, bool createIfNotExist = false)
        {
            var filename = Path.Combine(dbDir, dbFilePrefix + "_" + fileNum.ToString("0000") + ".db");
            FileStream file;
            if (File.Exists(filename)) file = new FileStream(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 4*1024);
            else if (createIfNotExist) file = new FileStream(filename, FileMode.Create, FileAccess.ReadWrite, FileShare.None, 4*1024);
            else return null;
            return new DbFileResource() {stream = file, length = new FileInfo(file.Name).Length};
        }

        private byte[] ReadFromDbFile(DbFileResource dbFile, int capacity, long position)
        {
            byte[] data, hash;
            lock (dbFile)
                ReadFromDbFile(dbFile, capacity, position, out data, out hash);
            var control = new MD5CryptoServiceProvider().ComputeHash(data);
            if (!control.SequenceEqual(hash)) return null;
            return data; 
        }

        /// <summary>
        /// Need locking dbFile before calling!
        /// </summary>
        private void ReadFromDbFile(DbFileResource dbFile, int capacity, long position, out byte[] data, out byte[] hash)
        {
            data = null;
            var buf = hash = new byte[16];
            dbFile.stream.Position = position;
            dbFile.stream.Read(buf, 0, 4);
            int len = BitConverter.ToInt32(buf, 0);
            if (len > capacity || len < 0) return;
            data = new byte[len];
            dbFile.stream.Read(data, 0, len);
            dbFile.stream.Read(buf, 0, 16);
        }

        private byte[] stub = new byte[1024*1024];
        /// <summary> If position == -1 write to end of file, else dataitems will be writed to position </summary>
        /// <returns> Position in file where dataitems were saved. It equals to parameter position, if it was valid,
        ///  else if parameter position was equals -1 or other not valid position, method returns new position of dataitems. </returns>
        private long WriteToDbFile(DbFileResource dbFile, int capacity, byte[] data, long position)
        {
            var len = BitConverter.GetBytes(data.Length);
            var hash = new MD5CryptoServiceProvider().ComputeHash(data);
            lock (dbFile)
                return WriteToDbFile(dbFile, data, len, hash, capacity - data.Length, position);
        }
        
        /// <summary>
        /// Need locking dbFile before calling!
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long WriteToDbFile(DbFileResource dbFile, byte[] data, byte[] datalen, byte[] hash, int stublen, long position)
        {
            bool isAppend = position < 0 || position >= dbFile.length;
            if (isAppend)
                position = dbFile.stream.Seek(0, SeekOrigin.End);
            else
                dbFile.stream.Position = position;
            dbFile.stream.Write(datalen, 0, 4);
            dbFile.stream.Write(data, 0, data.Length);
            dbFile.stream.Write(hash, 0, 16);
            int s;
            for (s = stublen; s > stub.Length; s-=stub.Length)
                dbFile.stream.Write(stub, 0, stub.Length);
            dbFile.stream.Write(stub, 0, s);
            if (isAppend) dbFile.length += data.Length + 20 + stublen;
            return position;
        }

        /// <summary>
        /// Load dataitems by pointer _ptr. 
        /// Return null if dataitems is corrupted and it's checksum is not valid.
        /// </summary>
        public byte[] Load(IPtr _ptr)
        {
            var ptr = _ptr as Ptr;
            if (ptr == null) return null;
            var dbFile = dbFiles[ptr.FileNum];
            byte[] data = ReadFromDbFile(dbFile, ptr.Capacity, ptr.Position);
                
            #if DEBUG
            onReading(ptr, data);
            #endif

            return data;  
        }

        /// <summary>
        /// Batch-loading dataitems from storage. Storage determs the most optimal order for loading pointers, so items will be returned in other order than pointers.
        /// </summary>
        public IEnumerable<Tuple<IPtr, byte[]>> BatchLoad(IPtr[] ptrs, int sizeOfbatchInMb = 1024)
        {
            var md5 = new MD5CryptoServiceProvider();
            long sizeOfbatch = ((long) sizeOfbatchInMb)*1024*1024;
            var groupsByFile = ptrs.GroupBy(p => ((Ptr) p).FileNum);
            foreach (var group in groupsByFile)
            {
                var ordered = group.OrderBy(p => ((Ptr) p).Position).ToArray();
                int processed = 0;
                while (processed < ordered.Length)
                {
                    var dbFile = dbFiles[group.Key];
                    List<Tuple<Ptr, byte[], byte[]>> loaded = new List<Tuple<Ptr, byte[], byte[]>>();
                    byte[] data, hash;
                    long readedBytes = 0;
                    lock (dbFile)
                    {
                        while (readedBytes < sizeOfbatch && processed < ordered.Length)
                        {
                            var ptr = (Ptr) ordered[processed];
                            ReadFromDbFile(dbFile, ptr.Capacity, ptr.Position, out data, out hash);
                            loaded.Add(Tuple.Create(ptr, data, hash));
                            if (data != null) readedBytes += data.Length + hash.Length;
                            ++processed;
                        }
                    }
                    foreach (var item in loaded)
                    {
                        var ptr = item.Item1;
                        data = item.Item2;
                        hash = item.Item3;
                        var control = md5.ComputeHash(data);
                        if (!control.SequenceEqual(hash)) yield return new Tuple<IPtr, byte[]>(ptr, null);
                        else yield return new Tuple<IPtr, byte[]>(ptr, data);
                    }
                }        
            }

        }

        /// <summary>
        /// Save dataitems to end of storage, or update dataitems by pointer. 
        /// If there are not enough place to update dataitems, it will be saved to end of storage.
        /// </summary>
        /// <returns>New pointer to dataitems (it may be equals previous pointer if dataitems were rewrited on the same place)</returns>
        public IPtr Save(byte[] data, IPtr _idx = null)
        {
            //dataitems = Compress.CompressGZip(dataitems);
            var idx = _idx as Ptr;
            if (idx != null && data.Length <= idx.Capacity) return Update(data, idx);
            return Store(data);
        }

        /// <summary>
        /// Save new dataitems or override existing dataitems (if ptr is not null). 
        /// Storage not garantee that items will be saved in the same order as they were passed, because storage determs the most optimal order of saving.
        /// </summary>
        /// <returns>New pointers to saved (overwrited) dataitems in the same order as input dataitems items</returns>
        public IPtr[] BatchSave(Tuple<IPtr, byte[]>[] dataitems)
        {
            var md5 = new MD5CryptoServiceProvider();
            var hash = dataitems.Select(item => md5.ComputeHash(item.Item2)).ToArray();
            var len = dataitems.Select(item => BitConverter.GetBytes(item.Item2.Length)).ToArray();
            var ptrs = new IPtr[dataitems.Length];

            Func<int, int> getFileNum = i =>
            {
                var data = dataitems[i].Item2;
                var ptr = (Ptr) dataitems[i].Item1;
                return ptr == null || ptr.Capacity < data.Length ? -1 : ptr.FileNum;
            };
            var groupsByFile = dataitems.Select((item, i) => i).GroupBy(i => getFileNum(i)).ToArray();
            foreach (var group in groupsByFile)
                if (group.Key != -1)
                {
                    var items = group.OrderBy(i => ((Ptr) dataitems[i].Item1).Position).ToArray();
                    var dbFile = dbFiles[group.Key];
                    lock (dbFile)
                        foreach (var i in items)
                        {
                            var data = dataitems[i].Item2;
                            var ptr = (Ptr) dataitems[i].Item1;
                            var position = WriteToDbFile(dbFile,  data, len[i], hash[i], ptr.Capacity - data.Length, ptr.Position);
                            if (ptr.Position != position)
                                ptr = new Ptr(ptr.Capacity, ptr.FileNum, position);
                            ptrs[i] = ptr;
                        }
                }
                else
                {
                    var items = group.ToArray();
                    int processed = 0;
                    while (processed < items.Length)
                    {
                        short filenum;
                        lock (dbFiles)
                            filenum = (short)(dbFiles.Count - 1);
                        var dbFile = dbFiles[filenum];
                        lock(dbFile)
                            while (processed < items.Length && dbFile.length < maxDbFileLength)
                            {
                                int i = items[processed];
                                var data = dataitems[i].Item2;
                                int k = (int)Math.Ceiling(Math.Log(data.Length / minRecordLen + 1, 2));
                                int capacity = minRecordLen * (1 << k);
                                var position = WriteToDbFile(dbFile, data, len[i], hash[i], capacity - data.Length, -1);
                                ptrs[i] = new Ptr(capacity, filenum, position);
                                ++processed;
                            }
                        if (dbFile.length >= maxDbFileLength)
                            OpenNewDBFileIfOverfull();
                    }
                }
            return ptrs;
        }

        /// <summary>
        /// Flush all files of storage to disk.
        /// </summary>
        public void Flush()
        {
            lock(dbFiles)
                foreach (var dbFile in dbFiles)
                    lock(dbFile)
                        dbFile.stream.Flush();
        }

        /// <summary>
        /// Uppend dataitems to end of storage
        /// </summary>
        private Ptr Store(byte[] data)
        {
            short filenum;

            // Сохраняем в  базу
            lock (dbFiles)
                filenum = (short)(dbFiles.Count - 1);

            var dbFile = dbFiles[filenum];
            int k = (int)Math.Ceiling(Math.Log(data.Length / minRecordLen + 1, 2));
            int capacity = minRecordLen * (1 << k);
            long position = WriteToDbFile(dbFile, capacity, data, -1);
            Ptr ptr = new Ptr(capacity, filenum, position);
                
            #if DEBUG
            onWriting(ptr, data);
            #endif
            
            if (dbFile.length > maxDbFileLength)
                OpenNewDBFileIfOverfull();
            return ptr;
        }

        /// <summary>
        /// Rewrite already exists record (write new dataitems on the same place)
        /// </summary>
        private Ptr Update(byte[] data, Ptr idx)
        {
            var dbFile = dbFiles[idx.FileNum];
            var position = WriteToDbFile(dbFile, idx.Capacity, data, idx.Position);
            if (position != idx.Position) // вообще это не нормально
                idx = new Ptr(idx.Capacity, idx.FileNum, position);

            #if DEBUG
            onWriting(idx, data);
            #endif
            return idx;
        }

        private void OpenNewDBFileIfOverfull()
        {
            lock (dbFiles)
            {
                var lastDbFile = dbFiles[dbFiles.Count - 1];
                if (lastDbFile.length > maxDbFileLength)
                {
                    var nextfile = OpenDbFile(dbFiles.Count, createIfNotExist: true);
                    if (nextfile == null) throw new IOException("Can't open next file (" + dbFiles.Count + ") in storage.");
                    dbFiles.Add(nextfile);
                }
            }
        }

        public void Dispose()
        {
            lock(dbFiles)
                foreach (var dbFile in dbFiles)
                    lock (dbFile)
                        dbFile.Dispose();
        }
    }
}