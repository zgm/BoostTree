using System;
using System.Text;
using System.IO;
using Microsoft.TMSN.IO;
using System.Collections;
using System.Reflection;
using System.Threading;

namespace Microsoft.TMSN.Data
{
    /// <summary>
    /// The TStoreThreadSafeReader can read a database written by a TMSNStoreWriter.  This threadsafe
    /// version of the reader requires enough RAM to read the cache file and both index files.  Eventually
    /// thread safety will be integrated into the TMSNStoreReader.
    /// </summary>
    public class TStoreThreadSafeReader
    {
        private class DiskAccessResponse {
            internal DiskAccessResponse(long position, int length) {
                Position = position;
                Length = length;
                WaitHandle = new AutoResetEvent(false);
            }

            //internal DiskAccessResponse Next;
            public long Position;
            public int Length;
            public byte[] Data = null;
            public EventWaitHandle WaitHandle;
        }

        // instantiated as a singleton only so static fields is ok
        private class QueuedDiskAccessor {
            private static Thread _diskThread = null;
            private static Stream _fileStream = null;
            private static bool _notDone = true;
            private static Queue _queue = new Queue();
            private static EventWaitHandle _diskThreadWait = null;//ew AutoResetEvent(false);

            public QueuedDiskAccessor(string filepath) {
                _fileStream = ZStreamIn.Open(filepath);
                _diskThreadWait = new ManualResetEvent(false);
                ThreadStart threadJob = new ThreadStart(WorkOnQueue);
                _diskThread = new Thread(threadJob);
                _diskThread.Start();
            }

            public void Close() {
                _notDone = false;
                _diskThreadWait.Set();
                if (_diskThread != null) _diskThread.Join();
                _fileStream.Close();
            }

            public DiskAccessResponse QueueRequest(long pos, int length) {
                DiskAccessResponse r = new DiskAccessResponse(pos, length);
                
                lock (_fileStream) {
                    _queue.Enqueue(r);
                    _diskThreadWait.Set();
                    if (_diskThread == null) { // if it died
                        ThreadStart threadJob = new ThreadStart(WorkOnQueue);
                        _diskThread = new Thread(threadJob);
                        _diskThread.Start();
                    }
                }

               r.WaitHandle.WaitOne();

                return r;
            }

            public long Length {
                get {
                    return _fileStream.Length;
                }
            }

            private static void WorkOnQueue() {
                try {
                    DiskAccessResponse response = null;

                    while (_notDone) {
                        response = null;

                        _diskThreadWait.WaitOne();

                        lock (_fileStream) {
                            if (_queue.Count != 0) {
                                response = (DiskAccessResponse)_queue.Dequeue();
                            }

                            else {
                                _diskThreadWait.Reset();
                            }
                        }

                        if (response != null) {
                            // seek and read
                            byte[] data = new byte[response.Length];
                            _fileStream.Seek(response.Position, SeekOrigin.Begin);
                            _fileStream.Read(data, 0, response.Length);
                            response.Data = data;

                            response.WaitHandle.Set();
                        }
                    }
                }

                // if for any reason there is an exception set _diskThread to null
                // so it can be restarted
                catch {
                    _diskThread = null;
                }
            }
        }

        private class ThreadSafeSource : InternalRecordSource
        {
            private TStoreThreadSafeReader _reader = null;
            private string _dbdir;

            public ThreadSafeSource(TStoreThreadSafeReader reader, string dbdir) {
                _reader = reader;
                _dbdir = dbdir;

                string recordInfoFile = Path.Combine(dbdir, "record-info");

                if (File.Exists(recordInfoFile)) {
                    using (FileStream fs = new FileStream(recordInfoFile, FileMode.Open, FileAccess.Read)) {
                        ReadProperties(fs);
                    }

                    TotalRecordsEstimate = reader.NumRecords;
                    TotalRecordBytesEstimate = reader.NumRecordBytes;
                }
            }

            public override void Close() {
            }

            public override bool MoveNext() {
                return false;
            }
        }

        //private Stream _keysFs;
        //private VariableLengthBinaryReader _keysReader;
        private KeyCacheReader2 _keyCache = null;
        private KeyIndex2 _keyIndex = null;
        private int _ascendingFactor = 1;
        private QueuedDiskAccessor _keyData = null;
        //private string _keysDataFile = null;
        //private long _keysDataLength = 0;

        private ThreadSafeSource _recordSource = null;

        /// <summary>
        /// The constructor for the TMSNStoreReader.
        /// </summary>
        /// <param name="dbdir">path to the directory holding the database.</param>
        public TStoreThreadSafeReader(string dbdir) {

            string name = Path.Combine(dbdir, "keys-data");
            //UnbufferedStream.BufferSize = 32*1024;
            //_keysFs = new FileStream(name, FileMode.Open, FileAccess.Read, FileShare.Read);
            //_keysFs = ZStreamIn.Open(name);
            //_keysReader = new VariableLengthBinaryReader(_keysFs);
            _keyData = new QueuedDiskAccessor(name);
            //_keysDataFile = name;
            
            //using (Stream s = ZStreamIn.Open(_keysDataFile)) {
            //    _keysDataLength = s.Length;
            //}

            // open the index as either a mmap or regular file
            name = Path.Combine(dbdir, "keys-data-index");
            _keyIndex = new KeyIndex2(name, _keyData.Length);

            // hard code the group size = 32.
            _keyCache = new KeyCacheReader2(dbdir, 32);

            _recordSource = new ThreadSafeSource(this, dbdir);
            _ascendingFactor = (_recordSource.Sorting.IsSortedAscending ? 1 : -1);
        }

        internal long NumRecordBytes {
            get {
                return _keyData.Length;
            }
        }

        /// <summary>
        /// Returns the number of records per group stored in the TStore.
        /// </summary>
        public uint KeyGroupSize {
            get {
                return _keyIndex.KeyGroupSize;
            }
        }

        /// <summary>
        /// Returns an instance of the record type stored in the TStore.
        /// </summary>
        public DataRecord RecordInstance {
            get {
                return _recordSource.ConstructInstance();
            }
        }

        /// <summary>
        /// The number of records in the database.
        /// </summary>
        public long NumRecords {
            get {
                return _keyIndex.NumRecords;
            }
        }

        /// <summary>
        /// Releases resources
        /// </summary>
        public void Close() {
            if (_keyData != null) _keyData.Close();
            if (_keyCache != null) _keyCache.Close();
            if (_keyIndex != null) _keyIndex.Close();
        }

        /// <summary>
        /// Provides random record access by record key.
        /// </summary>
        /// <param name="key">The key of the record to be retrieved.</param>
        /// <returns>The DataRecord.</returns>
        public DataRecord GetRecord(string key) {
            DataRecord record = _recordSource.ConstructInstance();
            GetRecord(key, record);
            return record;
        }

        /// <summary>
        /// Gets a record from the database accessing by key.
        /// </summary>
        /// <param name="key">The key of record to access.</param>
        /// <param name="record">The record retrieved.</param>
        public void GetRecord(string key, DataRecord record) {
            record.Data = null;
            record.Key = key;
            record._recordNo = -1;

            byte[] keyBytes = UTF8Encoding.UTF8.GetBytes(key);
            //byte[] keyBytes = _encoding.GetBytes(key); NOT THREAD SAFE

            long high, low, mid;  // need to express neg nos for the while constraint
            int diff;
            long lastGroupNo = 0;

            low = 0;
            high = _keyIndex.NumKeyGroups - 1;

            while (high >= low) {
                mid = low + (high - low) / 2;

                diff = _KeyCompare(keyBytes, (uint)mid, false);

                // the key is equal to the first member of the group -- done
                if (diff == 0) {
                    record._recordNo = mid * _keyIndex.KeyGroupSize;
                    record.Data = _GetDataOfFirstMember((uint)mid);
                    return;
                }

                    // the key is greater than -- raise the floor
                else if (diff > 0) {
                    low = mid + 1;
                    lastGroupNo = mid;
                }

                    // the key is less than -- lower the ceiling
                else {
                    high = mid - 1;
                }
            }

            // the key still might be in the low group because we don't check all members
            int groupMemberNum;
            byte[] data = _FindKeyInGroup(keyBytes, (uint)lastGroupNo, out groupMemberNum);
            if (groupMemberNum != -1) {
                record._recordNo = lastGroupNo * _keyIndex.KeyGroupSize + groupMemberNum;
                record.Data = data;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keyBytes"></param>
        /// <param name="keyGroupNo"></param>
        /// <param name="prefixOnly">if the keyBytes represent a prefix that doesn't have to match the key entirely</param>
        /// <returns></returns>
        private int _KeyCompare(byte[] keyBytes, uint keyGroupNo, bool prefixOnly) {
            int diff = _keyCache.CompareKey(keyGroupNo, keyBytes, prefixOnly);

            // we only return when we _don't_ match.
            // if we match then we fall through so that
            // we're in the correct position to read the data
            if (diff != 0) return diff * _ascendingFactor;

            return 0; // we matched!
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keyBytes"></param>
        /// <param name="keyGroupNo"></param>
        /// <param name="groupMemberNum"></param>
        /// <returns>-1 if not found otherwise returns the groupMemberNum of the match</returns>
        private byte[] _FindKeyInGroup(byte[] keyBytes, uint keyGroupNo, out int groupMemberNum) {

            // seek to beginning of group
            long groupPosition, length;
            _keyIndex.GetGroupPositionAndLength(keyGroupNo, out groupPosition, out length);
            
            DiskAccessResponse dar = _keyData.QueueRequest(groupPosition, (int)length);
            //byte[] groupBytes = new byte[(int)length];
            //using (Stream keysData = ZStreamIn.Open(_keysDataFile)) {
            //    keysData.Seek(groupPosition, SeekOrigin.Begin);
            //    keysData.Read(groupBytes, 0, (int)length);
            //}

            byte[] groupBytes = dar.Data;

            //lock (_keysReader.BaseStream) {
            //    // critical
            //    _keysReader.BaseStream.Seek(groupPosition, SeekOrigin.Begin);
            //    groupBytes = _keysReader.ReadBytes((int)length);
            //    // critical
            //}

            uint neu, overlap, lexLen;
            int diff = 0;

            int cursor = 0;
            int varLen;

            // go through each key of this group to see if we have a match
            for (groupMemberNum = 0; groupMemberNum != _keyIndex.KeyGroupSize; groupMemberNum++) {
                // currently pointing to overlap
                overlap = VariableLengthBitConverter.ToUint32(groupBytes, cursor, out varLen);
                cursor += varLen;

                // currently pointing to new
                neu = VariableLengthBitConverter.ToUint32(groupBytes, cursor, out varLen);
                lexLen = overlap + neu;
                cursor += varLen;

                // currently pointing to keyBytes
                // first time through this moves the keyBytes from Member 0 to the left edge
                // of groupBytes[] (since overlap is zero).  Subsequent times through copies
                // overlap of keyBytes from next members into this position. 
                Buffer.BlockCopy(groupBytes, cursor, groupBytes, (int)overlap, (int)neu);
                cursor += (int)neu;
#if DEBUG

                string seeking = UTF8Encoding.UTF8.GetString(keyBytes);
                string seekCandiate = UTF8Encoding.UTF8.GetString(groupBytes, 0, (int)lexLen);
#endif
                diff = TMSNStoreUtils.Utf8BytesCompare(groupBytes, (int)lexLen, keyBytes, keyBytes.Length) * _ascendingFactor;

                // if diff == 0 we've matched so return.
                // if diff < 0 then we know that candidate is less than us so
                // no need to check the other members of this group (which
                // lexically come after us)
                // if diff > 0 keep looping

                // currently pointing to datalen
                ulong keysPayload = VariableLengthBitConverter.ToUint64(groupBytes, cursor, out varLen);
                cursor += varLen;

                // cursor currently pointing to data

                if (diff < 0) break; // can't be in this group, we're past it.

                else if (diff == 0) {
                    if (keysPayload == 0) return null;
                    byte[] data = new byte[keysPayload];
                    Buffer.BlockCopy(groupBytes, cursor, data, 0, (int)keysPayload);
                    return data;
                }

                cursor += (int)keysPayload;
                // currently pointing to beginning of next member.
            }

            groupMemberNum = -1;
            return null;
        }

        private byte[] _GetDataOfFirstMember(uint keyGroupNo) {
            // seek to beginning of group
            long groupPosition, length;
            _keyIndex.GetGroupPositionAndLength(keyGroupNo, out groupPosition, out length);

            //byte[] groupBytes;
            //lock (_keysReader.BaseStream) {
            //    // critical
            //    _keysReader.BaseStream.Seek(groupPosition, SeekOrigin.Begin);
            //    groupBytes = _keysReader.ReadBytes((int)length);
            //    // critical
            //}

            DiskAccessResponse dar = _keyData.QueueRequest(groupPosition, (int)length);
            byte[] groupBytes = dar.Data;

            //byte[] groupBytes = new byte[(int)length];
            //using (Stream keysData = ZStreamIn.Open(_keysDataFile)) {
            //    keysData.Seek(groupPosition, SeekOrigin.Begin);
            //    keysData.Read(groupBytes, 0, (int)length);
            //}

            int cursor = 0;
            int varLen;
            
            uint overlap = VariableLengthBitConverter.ToUint32(groupBytes, cursor, out varLen); // will be zero
            cursor += varLen;

            int keyLen = (int)VariableLengthBitConverter.ToUint32(groupBytes, cursor, out varLen);
            cursor += varLen + keyLen;

            // now in data position

            ulong keysPayload = VariableLengthBitConverter.ToUint64(groupBytes, cursor, out varLen);
            cursor += varLen;

            // data is embedded in the keys file
            // so keysPayload is the length of the data
            // if keysPayLoad == 0, then we have null data.

            if (keysPayload == 0) return null;

            byte[] data = new byte[keysPayload];
            Buffer.BlockCopy(groupBytes, cursor, data, 0, (int)keysPayload);

            return data;
        }
    }

    internal class KeyCacheReader2
    {
        private uint _keyGroupSize = 0;
        private int _bytesPerIndex = 0;
        //private UTF8Encoding _encoding = new UTF8Encoding(false, false);

        private byte[] _cacheIndexBuffer = null;
        private byte[] _cacheBuffer = null;

        private uint _numInternalKeyGroups;

        ~KeyCacheReader2() {
            Close();
        }

        public void Close() {
        }

        public KeyCacheReader2(string dbdir, uint keyGroupSize) {
            _keyGroupSize = keyGroupSize;

            string file = Path.Combine(dbdir, "cache-index");
            Stream fs = ZStreamIn.Open(file);

            _cacheIndexBuffer = new byte[fs.Length];
            fs.Read(_cacheIndexBuffer, 0, (int)fs.Length);
            // this should have an interface. an index file has a long and int at the end.
            _numInternalKeyGroups = (uint)((fs.Length - 12)/ TMSNStoreUtils.BytesPerKeyIndexOffset);
           
            file = Path.Combine(dbdir, "cache");
            fs = ZStreamIn.Open(file);
            _cacheBuffer = new byte[fs.Length];
            fs.Read(_cacheBuffer, 0, (int)fs.Length);


            _bytesPerIndex = TMSNStoreUtils.BytesPerKeyIndexOffset;
        }

        public int CompareKey(uint keyGroupNo, byte[] toCompareBytes, bool prefixOnly) {
            uint internalKeyGroupNo = keyGroupNo / _keyGroupSize;
            int internalMemberNo = (int)(keyGroupNo % _keyGroupSize);

            long groupOffset;

            int indexOffset = (int)(internalKeyGroupNo * _bytesPerIndex);
            groupOffset = TMSNStoreUtils.LongAsFiveBytes(_cacheIndexBuffer, indexOffset);

            long nextGroupOffset;
            if (internalKeyGroupNo == _numInternalKeyGroups - 1) {
                nextGroupOffset = _cacheBuffer.Length;
            }
            else {
                indexOffset += _bytesPerIndex;
                nextGroupOffset = TMSNStoreUtils.LongAsFiveBytes(_cacheIndexBuffer, indexOffset);
            }

            int groupBytesLength = (int)(nextGroupOffset - groupOffset);

            // figure out the overlap (which will be zero) and neu of the first member of the group.
            // this way we can copy the keyBytes leftmost in the buffer.
            int varLen;
            uint overlap = VariableLengthBitConverter.ToUint32(_cacheBuffer, (int)groupOffset, out varLen);
            groupOffset += varLen;
            groupBytesLength -= varLen;

            uint neu = VariableLengthBitConverter.ToUint32(_cacheBuffer, (int)groupOffset, out varLen);
            groupOffset += varLen;
            groupBytesLength -= varLen;

            byte[] groupBytes = new byte[groupBytesLength];
            Buffer.BlockCopy(_cacheBuffer, (int)groupOffset, groupBytes, 0, groupBytesLength);

            uint lexLen = neu; // the first time
            int cursor = (int)neu; // point at next member's key

#if DEBUG
            string candidate3 = UTF8Encoding.UTF8.GetString(groupBytes, 0, (int)lexLen);			// if toCompareBytes represents a prefix we don't need to match entire key
#endif

            while (internalMemberNo != 0) {
                overlap = VariableLengthBitConverter.ToUint32(groupBytes, cursor, out varLen);
                cursor += varLen;

                neu = VariableLengthBitConverter.ToUint32(groupBytes, cursor, out varLen);
                cursor += varLen;

                Buffer.BlockCopy(groupBytes, cursor, groupBytes, (int)overlap, (int)neu);
#if DEBUG
                string candidate2 = UTF8Encoding.UTF8.GetString(groupBytes, 0, (int)(overlap + neu));			// if toCompareBytes represents a prefix we don't need to match entire key
#endif
                cursor += (int)neu;
                internalMemberNo--;
            }

            lexLen = overlap + neu; // we didn't calculate it in the loop cuz we're stingy.
            int diff = 0;
#if DEBUG
            string sought = UTF8Encoding.UTF8.GetString(toCompareBytes);
            string candidate = UTF8Encoding.UTF8.GetString(groupBytes, 0, (int)lexLen);			// if toCompareBytes represents a prefix we don't need to match entire key
#endif
            if (prefixOnly) {
                if (toCompareBytes.Length > lexLen) {
                    groupBytes[lexLen] = 0; // make sure the first position that toCompare is longer is zeroed for comparison
                }
                diff = TMSNStoreUtils.Utf8BytesCompare(groupBytes, toCompareBytes.Length, toCompareBytes, toCompareBytes.Length);
            }

                // else must match everything
            else {
                diff = TMSNStoreUtils.Utf8BytesCompare(groupBytes, (int)lexLen, toCompareBytes, toCompareBytes.Length);
            }

            return diff;
        }
    }


    internal class KeyIndex2
    {
        // buffer for converting 5 byte longs into longs using BitConverter
        //private byte[] _rawIndexOffsetPosition = new byte[8];
        private long _numRecords;
        private uint _keyGroupSize;
        private uint _numKeyGroups;
        //private BinaryFileReader _keyIndexReader = null;
        private long _keysFileLength = 0;
        private byte[] _keyIndexBuffer = null;

        public KeyIndex2(string filename, long keysFileLength) {

            Stream fs = ZStreamIn.Open(filename);
            _keyIndexBuffer = new byte[fs.Length];
            fs.Read(_keyIndexBuffer, 0, (int)fs.Length);

            // 12 = length(long + int)
            _numRecords = BitConverter.ToInt64(_keyIndexBuffer, (int)fs.Length - 12);

            _keyGroupSize = BitConverter.ToUInt32(_keyIndexBuffer, (int)fs.Length - 4);
           
            _numKeyGroups = _CalcNumKeyGroups(_numRecords, _keyGroupSize);

            _keysFileLength = keysFileLength;
        }

        ~KeyIndex2() {
            Close();
        }

        public void Close() {
        }

        private uint _CalcNumKeyGroups(long numRecords, uint keyGroupSize) {
            uint numKeyGroups = (uint)(numRecords / (long)_keyGroupSize);
            if (numRecords % keyGroupSize != 0) numKeyGroups++;

            return numKeyGroups;
        }

        public long GetGroupPosition(uint keyGroupNo) {
            if (keyGroupNo >= _numKeyGroups) {
                return -1;
            }

            long indexSeekPosition = keyGroupNo * TMSNStoreUtils.BytesPerKeyIndexOffset;
            long position = TMSNStoreUtils.LongAsFiveBytes(_keyIndexBuffer, (int)indexSeekPosition);

            return position;
        }

        public void GetGroupPositionAndLength(uint keyGroupNo, out long position, out long length) {
            if (keyGroupNo >= _numKeyGroups) {
                position = -1;
                length = -1;
            }

            long indexSeekPosition = keyGroupNo * TMSNStoreUtils.BytesPerKeyIndexOffset;

            // if last key group there isn't a next group so ...
            if (keyGroupNo == _numKeyGroups - 1) {
                position = TMSNStoreUtils.LongAsFiveBytes(_keyIndexBuffer, (int)indexSeekPosition);
                length = _keysFileLength - position;
                return;
            }

            position = TMSNStoreUtils.LongAsFiveBytes(_keyIndexBuffer, (int)indexSeekPosition);
            long nextPosition = TMSNStoreUtils.LongAsFiveBytes(_keyIndexBuffer, (int)indexSeekPosition + TMSNStoreUtils.BytesPerKeyIndexOffset);

            length = nextPosition - position;
        }

        public uint KeyGroupSize {
            get {
                return _keyGroupSize;
            }
        }

        public uint NumKeyGroups {
            get {
                return _numKeyGroups;
            }
        }

        public long NumRecords {
            get {
                return _numRecords;
            }
        }
    }
}
