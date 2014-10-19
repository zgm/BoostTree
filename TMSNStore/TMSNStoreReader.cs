using System;
using System.Text;
using System.IO;
using Microsoft.TMSN.IO;
using System.Collections;
using System.Reflection;

namespace Microsoft.TMSN.Data
{
	internal class TMSNStoreRecordSource : InternalRecordSource {
		private TMSNStoreReader _reader = null;
        private long _currentRecordNo = -1;
		private long _lastHintNo = -10; // enables hinting
		private string _moveNextHint = null;

        private string _dbdir;

		public TMSNStoreRecordSource(TMSNStoreReader reader, string dbdir) {
			_reader = reader;
            _dbdir = dbdir;

#if NAMEDSTREAMMODE
			string recordInfoFile = dbdir + ":record-info";

			Stream fs = ZStreamIn.Open(recordInfoFile);
			ReadProperties(fs);
			fs.Close();

			TotalRecordsEstimate = reader.NumRecords;
			TotalRecordBytesEstimate = reader.NumRecordBytes;
#else

			string recordInfoFile = Path.Combine(dbdir, "record-info");

			if (File.Exists(recordInfoFile)) {
				using (FileStream fs = new FileStream(recordInfoFile, FileMode.Open, FileAccess.Read)) {
					ReadProperties(fs);
				}

				TotalRecordsEstimate = reader.NumRecords;
				TotalRecordBytesEstimate = reader.NumRecordBytes;
			}

#endif
		}

		public override bool MoveNext() {
            DataRecord outputRecord = null;

			// we moving to _currentRecord + 1
			if (_moveNextHint != null && _currentRecordNo >= _lastHintNo) {
				long hintRecordNo = _reader.GetFirstRecordNoOfGroup(_moveNextHint);

				// don't allow any more hits until group is exhausted and
				// also the first record of the next group (in case the 
				// sought after record should be after the last one of
				// the group, we need to make the compare fail)
				_lastHintNo = hintRecordNo + _reader.KeyGroupSize;

				// we need to null out hint in case no more hints are made
				_moveNextHint = null;

                // only set the current recordno if it's different from the current.
                // set to hint - 1 since it is always incremented below.
                if (hintRecordNo != _currentRecordNo) _currentRecordNo = hintRecordNo - 1;
			}

			if (_currentRecordNo < _reader.NumRecords - 1) {
                outputRecord = _reader.GetRecord(++_currentRecordNo);
			}

            CurrentRecord = outputRecord;
			return (outputRecord != null);
		}

		public override string MoveNextHint {
			set {
				_moveNextHint = value;
			}
		}

		public override void Close() {
			_reader.Close();
		}

        public override string CurrentSourceName {
            get {
                return _dbdir;
            }
        }

        public override TStorageType StorageType {
            get {
                return TStorageType.TStore;
            }
        }

		public TMSNStoreReader GetReader() {
			return _reader;
		}
	}

	/// <summary>
	/// The TMSNStoreReader can read a database written by a TMSNStoreWriter.  Big dbs are not
	/// compatible with SimpleDbs the both use the DataRecord as a payload.
	/// </summary>
	public class TMSNStoreReader : IEnumerable
	{
		private enum FindEdgeAction {
			FIND_ANY,
			FIND_LOW_EDGE,
			FIND_HIGH_EDGE,
		}

		private class TMSNStoreReaderEnumerator : IEnumerator {
			#region IEnumerator Members

			private long _currentRecordNo;
			private long _beginRecordNo;
			private long _endRecordNo;
			private TMSNStoreReader _dbReader;

			public TMSNStoreReaderEnumerator(TMSNStoreReader dbReader, long beginRecordNo, long endRecordNo) {
				_dbReader = dbReader;
				_beginRecordNo = beginRecordNo;
				_currentRecordNo = _beginRecordNo - 1;
				_endRecordNo = endRecordNo;
			}

			public void Reset() {
				_currentRecordNo = _beginRecordNo - 1;
			}

			public object Current {
				get {
                    DataRecord record = _dbReader._recordSource.ConstructInstance();
					_dbReader.GetRecord(_currentRecordNo, record);
					return record;
				}
			}

			public bool MoveNext() {
				_currentRecordNo++;
				if (_currentRecordNo > _endRecordNo) return false;
				else return true;
			}

			#endregion
		}

		private Stream _keysFs;
		private VariableLengthBinaryReader _keysReader;
		private UTF8Encoding _encoding = new UTF8Encoding(false, false);
		private byte[] _lastKeyBytes = new byte[4];
		private long _lastRecordNo = -10;
		private KeyCacheReader _keyCache = null;
		//private DataRecord _recordInstance = null;
		private KeyIndex _keyIndex = null;
        private int _ascendingFactor = 1;

		private TMSNStoreRecordSource _recordSource = null;

		/// <summary>
		/// The constructor for the TMSNStoreReader.
		/// </summary>
		/// <param name="dbdir">path to the directory holding the database.</param>
		/// <param name="mmapIndexFiles">instruct TMSNStore to attempt memory mapping index files otherwise read into memory.</param>
		public TMSNStoreReader(string dbdir, bool mmapIndexFiles) {
			
			string name = Path.Combine(dbdir, "keys-data");
			//UnbufferedStream.BufferSize = 32*1024;
			//_keysFs = new FileStream(name, FileMode.Open, FileAccess.Read, FileShare.Read);
			_keysFs = ZStreamIn.Open(name);
			_keysReader = new VariableLengthBinaryReader(_keysFs);

            // open the index as either a mmap or regular file
            name = Path.Combine(dbdir, "keys-data-index");
            _keyIndex = new KeyIndex(name, _keysFs.Length, mmapIndexFiles);


			// hard code the group size.
			_keyCache = new KeyCacheReader(dbdir, 32, mmapIndexFiles);

			_recordSource = new TMSNStoreRecordSource(this, dbdir);
            _ascendingFactor = (_recordSource.Sorting.IsSortedAscending ? 1 : -1);
		}

		internal long NumRecordBytes {
			get {
				return _keysFs.Length;
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

		internal TMSNStoreRecordSource RecordSource {
			get {
				return _recordSource;
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
		/// TMSNStoreReader destructor
		/// </summary>
		~TMSNStoreReader() {
			Close();
		}

		/// <summary>
		/// Releases resources
		/// </summary>
		public void Close() {
			if (_keysReader != null) _keysReader.Close();
			if (_keyCache != null) _keyCache.Close();
			if (_keyIndex != null)_keyIndex.Close();
		}

		/// <summary>
		/// Provides random record access by record number.
		/// </summary>
		/// <param name="recordNo">The record number of the record to be retrieved.</param>
		/// <returns>The DataRecord.</returns>
		public DataRecord GetRecord(long recordNo) {
			DataRecord record = _recordSource.ConstructInstance();
			GetRecord(recordNo, record);
			return record;
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
        /// Provides random bipartite record access by record key
        /// </summary>
        /// <param name="key">The key of the bipartite record to be retrived</param>
        /// <returns>the bipartite record</returns>
        public BipartiteRecord GetBipartiteRecord(string key)
        {            
            DataRecord record = GetRecord(key);            
            BipartiteRecord biRecord = new BipartiteRecord();
            biRecord._recordNo = record._recordNo;
            if(record.RecordNo!=-1)
                biRecord.Data = record.Data;
            return biRecord;
        }        

		/// <summary>
		/// Gets a record from the database accessing by record number.
		/// </summary>
		/// <param name="recordNo">The record number of the record to access.</param>
		/// <param name="record">The record retrieved.</param>
		public void GetRecord(long recordNo, DataRecord record) {
			record._recordNo = -1;
			record.Data = null; // clear data;
			record.Key = null;

			uint neu, overlap, lexLen;

			if (recordNo >= _keyIndex.NumRecords || recordNo < 0) {	
				return;// false;
			}

			uint keyGroupNo = (uint)(recordNo / _keyIndex.KeyGroupSize);
			uint groupMemberNo = (uint)(recordNo % _keyIndex.KeyGroupSize);
			
			// if case we're iterating
			if (recordNo == _lastRecordNo + 1) {
				groupMemberNo = 0; // this causes the next member to be choosen.
			}

			else  {
				// seek in index file
				long groupPosition = _keyIndex.GetGroupPosition(keyGroupNo);

				// seek in keys file
				_keysReader.BaseStream.Seek(groupPosition, SeekOrigin.Begin);
			}
			
			_lastRecordNo = recordNo;

			// go to the right member
			while (true) {
				overlap = _keysReader.ReadVariableLength();
				neu = _keysReader.ReadVariableLength();
				lexLen = overlap + neu;

				if (_lastKeyBytes.Length < lexLen) {
					byte[] lastLast = _lastKeyBytes;
					_lastKeyBytes = new byte[lexLen + 2];
					Buffer.BlockCopy(lastLast, 0, _lastKeyBytes, 0, (int)overlap);
				}

				_keysReader.Read(_lastKeyBytes, (int)overlap, (int)neu);

				if (groupMemberNo == 0) {
					byte[] keyBytes = new byte[lexLen];
					Buffer.BlockCopy(_lastKeyBytes, 0, keyBytes, 0, (int)lexLen);
					
					record.KeyBytes = keyBytes;
					record._recordNo = recordNo;
					record.Data = _GetData();
					return; // true;
				}

				else {
					// scoot along past the data
					ulong keysPayload = _keysReader.ReadVariableLengthULong();

					// wow! replacing this seek with the read sped things up by 6x
					//_keysReader.BaseStream.Seek((long)keysPayload, SeekOrigin.Current);
					_keysReader.ReadBytes((int)keysPayload);

					// otherwise we do nothing
					groupMemberNo--;
				}
			}
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

			_lastRecordNo = -10; // so that sequential access knows this has intervened.

			byte[] keyBytes = _encoding.GetBytes(key);

			long high, low, mid;  // need to express neg nos for the while constraint
			int diff;
			long lastGroupNo = 0;

			low = 0;
			high = _keyIndex.NumKeyGroups - 1;

			//int cacheEntryNo = 0;
			while (high >= low) {
				mid = low + (high - low) / 2;
	
				diff = _KeyCompare(keyBytes, (uint)mid, false);

				// the key is equal to the first member of the group -- done
				if (diff == 0) {
					record._recordNo = mid * _keyIndex.KeyGroupSize;
					record.Data = _GetData();
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
			int groupMemberNum = _FindKeyInGroup(keyBytes, (uint)lastGroupNo);
			if (groupMemberNum != -1) {
				record._recordNo = lastGroupNo * _keyIndex.KeyGroupSize + groupMemberNum;
				record.Data = _GetData();
			}
		}


		/// <summary>
		/// Returns the recordNo of a record with a matching key.
		/// </summary>
		/// <param name="key">The key of record to access.</param>
		/// <returns>the record no of a matching record or -1.</returns>
		public long GetRecordNo(string key) {
			byte[] keyBytes = _encoding.GetBytes(key);

			long high, low, mid;  // need to express neg nos for the while constraint
			int diff;
			long lastGroupNo = 0;
			_lastRecordNo = -10; // so that sequential access knows this has intervened.

			low = 0;
			high = _keyIndex.NumKeyGroups - 1;

			while (high >= low) {
				mid = low + (high - low) / 2;

				diff = _KeyCompare(keyBytes, (uint)mid, false);

				// the key is equal to the first member of the group -- done
				if (diff == 0) {
					return mid * _keyIndex.KeyGroupSize;
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
			int groupMemberNum = _FindKeyInGroup(keyBytes, (uint)lastGroupNo);
			if (groupMemberNum == -1) return -1;

			return lastGroupNo * (long)_keyIndex.KeyGroupSize + groupMemberNum;
		}


		/// <summary>
		/// Returns the recordNo of the first record of the group which would contain
		/// the record matching the key if it is in the store.  This search is done
		/// completely using index structures only, which is fast.
		/// </summary>
		/// <param name="key">The key of record to access.</param>
		/// <returns>the record no of a matching record or -1.</returns>
		public long GetFirstRecordNoOfGroup(string key) {
			byte[] keyBytes = _encoding.GetBytes(key);

			long high, low, mid;  // need to express neg nos for the while constraint
			int diff;
			long lastGroupNo = 0;
			_lastRecordNo = -10; // so that sequential access knows this has intervened.

			low = 0;
			high = _keyIndex.NumKeyGroups - 1;

			while (high >= low) {
				mid = low + (high - low) / 2;

				diff = _KeyCompare(keyBytes, (uint)mid, false);

				// the key is equal to the first member of the group -- done
				if (diff == 0) {
					return mid * _keyIndex.KeyGroupSize;
				}

					// the key is greater than -- raise the floor
				else if (diff > 0) {
					low = mid + 1;
					lastGroupNo = (uint)mid;
				}

					// the key is less than -- lower the ceiling
				else {
					high = mid - 1;
				}
			}

			// the key still might be in the low group because we don't check members

			return lastGroupNo * (long)_keyIndex.KeyGroupSize;
		}

		private long _FindEdgeGroup(byte[] prefixBytes, long inLow, long inHigh,
							        FindEdgeAction action, out long low, out long high) {

			long mid;  // need to express neg nos for the while constraint
			int diff;
			long lastMid = 0;
			long lastLow = 0;
			long lastHigh = 0;

			low = inLow;
			high = inHigh;

			while (high >= low) {
				mid = low + (high - low) / 2;
				diff = _KeyCompare(prefixBytes, (uint)mid, true);

				// the prefix matches first member of the group
				if (diff == 0) {
					switch (action) {
							// if any match will do return this one
						case FindEdgeAction.FIND_ANY:
							return mid;

						case FindEdgeAction.FIND_LOW_EDGE:
							// if mid == 0 this is low edge
							if (mid == 0) return mid;
							// otherwise look at our neighbor
							diff = _KeyCompare(prefixBytes, (uint)mid-1, true);
							// if we fail here we know that the edge still might be in the
							// mid-1 group, so set lastGroupNo and handle it there
							if (diff != 0) {
								lastMid = mid-1; // set it to one less on purpose
								low = high + 1;  // end the loop
							}
							else high = mid - 1; // didn't find the edge, it's lower
							break;

						case FindEdgeAction.FIND_HIGH_EDGE:
							if (mid == _keyIndex.NumKeyGroups - 1) return mid;
							diff = _KeyCompare(prefixBytes, (uint)mid+1, true);
							// if we fail here we know that the edge is in the
							// mid group, so set lastGroupNo and handle it there
							if (diff != 0) {
								lastMid = mid;
								low = high + 1; // end the loop
							}
							else low = mid + 1; // didn't find the edge, it's higher
							break;

						default:
							break;
					}
				}

					// the key is greater than -- raise the floor
				else if (diff > 0) {
					// we need to save all of these since we must return valid mid, low and high
					lastMid = (uint)mid;
					lastLow = low;
					lastHigh = high;

					low = mid + 1;
				}

					// the key is less than -- lower the ceiling
				else {
					high = mid - 1;
				}
			}

			// if we fail all the way to here we set the last versions of mid,low and high
			// in case we missed the prefix in the last mid (since we don't check all the members)
			high = lastHigh;
			low = lastLow;
			return (uint)lastMid;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyBytes"></param>
		/// <param name="keyGroupNo"></param>
		/// <param name="prefixOnly">if the keyBytes represent a prefix that doesn't have to match the key entirely</param>
		/// <returns></returns>
		private int _KeyCompare(byte[] keyBytes, uint keyGroupNo, bool prefixOnly) {
			int diff = 0;

			diff = _keyCache.CompareKey(keyGroupNo, keyBytes, prefixOnly);
			
			// we only return when we _don't_ match.
			// if we match then we fall through so that
			// we're in the correct position to read the data
            if (diff != 0) return diff * _ascendingFactor;

			// seek to beginning of group
			long groupPosition = _keyIndex.GetGroupPosition(keyGroupNo);

			_keysReader.BaseStream.Seek(groupPosition, SeekOrigin.Begin);

			uint overlap = _keysReader.ReadVariableLength(); // overlap will be zero as first group member
			int keyLen = (int)_keysReader.ReadVariableLength(); // neu is the key length
			byte[] buffer = _keysReader.ReadBytes(keyLen);

			//string foo = _encoding.GetString(buffer);

			// now in data position
			return 0; // we matched!
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyBytes"></param>
		/// <param name="keyGroupNo"></param>
		/// <returns>-1 if not found otherwise returns the groupMemberNum of the match</returns>
		private int _FindKeyInGroup(byte[] keyBytes, uint keyGroupNo) {

			// seek to beginning of group
			long groupPosition = _keyIndex.GetGroupPosition(keyGroupNo);

			_keysReader.BaseStream.Seek(groupPosition, SeekOrigin.Begin);

			uint neu, overlap, lexLen;
			int diff = 0;

			// go through each key of this group to see if we have a match
			for (int groupMemberNum = 0; groupMemberNum != _keyIndex.KeyGroupSize; groupMemberNum++) {
				overlap = _keysReader.ReadVariableLength();
				neu = _keysReader.ReadVariableLength();
				lexLen = overlap + neu;
				
				if (_lastKeyBytes.Length < lexLen) {
					byte[] lastLast = _lastKeyBytes;
					_lastKeyBytes = new byte[lexLen + 2];
					Buffer.BlockCopy(lastLast, 0, _lastKeyBytes, 0, (int)overlap);
				}

				_keysReader.Read(_lastKeyBytes, (int)overlap, (int)neu);

#if DEBUG
				string seeking = _encoding.GetString(keyBytes);
				string seekCandiate = _encoding.GetString(_lastKeyBytes, 0, (int)lexLen);
#endif

				diff = TMSNStoreUtils.Utf8BytesCompare(_lastKeyBytes, (int)lexLen, keyBytes, keyBytes.Length) * _ascendingFactor;

				// if diff == 0 we've matched so return.
				// if diff < 0 then we know that candidate is less than us so
				// no need to check the other members of this group (which
				// lexically come after us)
				// if diff > 0 keep looping
				
				if (diff < 0) return -1;
				else if (diff == 0) return groupMemberNum;

				ulong keysPayload = _keysReader.ReadVariableLengthULong();

				// wow! replacing this seek with the read sped things up by 6x
				//_keysReader.BaseStream.Seek((long)keysPayload, SeekOrigin.Current);
				_keysReader.ReadBytes((int)keysPayload);
			}
			
			// we failed
			return -1;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="prefixBytes"></param>
		/// <param name="keyGroupNo"></param>
		/// <param name="firstMatch">if true finds first match else finds last match.</param>
		/// <returns>-1 if not found otherwise returns the groupMemberNum of the match</returns>
		private int _FindPrefixInGroup(byte[] prefixBytes, uint keyGroupNo, bool firstMatch) {

			// seek to beginning of group
			long groupPosition = _keyIndex.GetGroupPosition(keyGroupNo);

			_keysReader.BaseStream.Seek(groupPosition, SeekOrigin.Begin);

			uint neu, overlap, lexLen;
			int diff = 0;

			int lastMatch = -1;  // last match returned on failure.  Set to -1 which signifies 'not found'

			// this method is complicated because of the following phenomenon:
			// say we're searching for prefix 'aefg*'.  It is in this group.
			//
			// <groupBeginning>
			// abcdefgh
			// abcdefgi
			// abcdefgj
			// abd
			// abdabc
			// abdabd
			// aefg

			// when searching for a prefix make sure lastKeyBytes is at least as big as the sought after bytes
			if (_lastKeyBytes.Length < prefixBytes.Length) {
				_lastKeyBytes = new byte[prefixBytes.Length + 2];
			}


			// go through each key of this group to see if we have a match, take the last match
			for (int groupMemberNum = 0; groupMemberNum != _keyIndex.KeyGroupSize; groupMemberNum++) {
				overlap = _keysReader.ReadVariableLength();
				neu = _keysReader.ReadVariableLength();
				lexLen = overlap + neu;

				// must read the bytes off disk to be able to iterate thru

				if (_lastKeyBytes.Length < lexLen) {
					byte[] lastLast = _lastKeyBytes;
					_lastKeyBytes = new byte[lexLen + 2];
					Buffer.BlockCopy(lastLast, 0, _lastKeyBytes, 0, (int)overlap);
				}

				_keysReader.Read(_lastKeyBytes, (int)overlap, (int)neu);

				// string sought = _encoding.GetString(prefixBytes);
				// string candidate = _encoding.GetString(_lastKeyBytes, 0, (int)lexLen);

				if (prefixBytes.Length > lexLen) {
					_lastKeyBytes[lexLen] = 0; // make sure the first position that toCompare is longer is zeroed for comparison
				}

				diff = _ascendingFactor * TMSNStoreUtils.Utf8BytesCompare(_lastKeyBytes, prefixBytes.Length, prefixBytes, prefixBytes.Length);
				
				// if we fail then either we were 1) looking for first match and didn't find it
				// so return lastMatch (which was initialized to -1) or 2) looking for last match
				// in which case we may have found it previously and set lastMatch accordingly.
				if (diff < 0) return lastMatch;

				if (diff == 0) {// && compareLength == prefixBytes.Length) {  ????
					if (firstMatch) return groupMemberNum; // if looking for first match we're done.
					lastMatch = groupMemberNum;            // else looking for last match
				}

				ulong keysPayload = _keysReader.ReadVariableLengthULong();

				// wow! replacing this seek with the read sped things up by 6x
				//_keysReader.BaseStream.Seek((long)keysPayload, SeekOrigin.Current);
				_keysReader.ReadBytes((int)keysPayload);
			}
			
			return lastMatch;
		}

		private byte[] _GetData() {
			ulong keysPayload = _keysReader.ReadVariableLengthULong();

			// data is embedded in the keys file
			// so keysPayload is the length of the data
			// if keysPayLoad == 0, then we have null data.

			if (keysPayload == 0) return null;
			
			byte[] data = _keysReader.ReadBytes((int)keysPayload);

			return data;
		}

		#region IEnumerable Members

		/// <summary>
		/// Allows enumeration over the records.
		/// </summary>
		/// <returns>The enumerator.</returns>
		public IEnumerator GetEnumerator() {
			return new TMSNStoreReaderEnumerator(this, 0, _keyIndex.NumRecords - 1);
		}

		private class TMSNStoreReaderEnumerable : IEnumerable {
			TMSNStoreReader _reader;
			long _startRecord;
			long _stopRecord;

			public TMSNStoreReaderEnumerable(TMSNStoreReader reader, long startRecord, long stopRecord) {
				_reader = reader;
				_startRecord = startRecord;
				_stopRecord = stopRecord;
			}

			public IEnumerator GetEnumerator() {
				return new TMSNStoreReaderEnumerator(_reader, _startRecord, _stopRecord);
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="startRecordNo"></param>
		/// <param name="stopRecordNo"></param>
		/// <returns></returns>
		private IEnumerable _GetRecords(long startRecordNo, long stopRecordNo) {
			return new TMSNStoreReaderEnumerable(this, startRecordNo, stopRecordNo);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <returns></returns>
		public IEnumerable GetMatch(string key) {
			long recordNo = GetRecordNo(key);
			if (recordNo == -1) {
				// make the end less than the beginning
				return new TMSNStoreReaderEnumerable(this, 10, 0);
			}

			return new TMSNStoreReaderEnumerable(this, recordNo, recordNo);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="prefix"></param>
		/// <returns></returns>
		public IEnumerable GetMatchingPrefix(string prefix) {
			long high, low, midGroup, outHigh, outLow, dummy1, dummy2;
			int groupMemberNo;

			_lastRecordNo = -10; // so that sequential access knows this has intervened.
			low = 0;
			high = _keyIndex.NumKeyGroups - 1;

			byte[] prefixBytes = _encoding.GetBytes(prefix);

			midGroup = _FindEdgeGroup(prefixBytes, low, high,	FindEdgeAction.FIND_ANY, out outLow, out outHigh);

			groupMemberNo = _FindPrefixInGroup(prefixBytes, (uint)midGroup, true);

			// if memberNo == -1 we didn't find a match
			if (groupMemberNo == -1) {
				return new TMSNStoreReaderEnumerable(this, 0, -1);  // the empty enumerator
			}
		
			// at this point we know there is a match for the prefix, now find the edges
			
			long lowRecordNo;

			// if groupMemberNo is not the first member in the mid group (== 0) then we know that the
			// record it corresponds to is the lowRecord.  Otherwise we need to search.

			if (groupMemberNo != 0) lowRecordNo = midGroup * _keyIndex.KeyGroupSize + groupMemberNo;
			else {
				// for the low edge, the groupNo returned might not contain a match
				// if the first record in groupNo+1 was the actual low edge.  In that
				// case we have to look in groupNo+1 too.

				long lowEdge = _FindEdgeGroup(prefixBytes, outLow, midGroup,
					FindEdgeAction.FIND_LOW_EDGE, out dummy1, out dummy2);
				groupMemberNo = _FindPrefixInGroup(prefixBytes, (uint)lowEdge, true);
				if (groupMemberNo == -1) {
					groupMemberNo = _FindPrefixInGroup(prefixBytes, (uint)++lowEdge, true);
					if (groupMemberNo == -1) {
						throw new Exception("bad binary search"); // can't happen
					}
				}
				
				lowRecordNo = lowEdge * _keyIndex.KeyGroupSize + groupMemberNo;
			}


			// the high isn't as complicated as the low edge
			long highEdge = _FindEdgeGroup(prefixBytes, midGroup, outHigh,
				FindEdgeAction.FIND_HIGH_EDGE, out dummy1, out dummy2);
			groupMemberNo = _FindPrefixInGroup(prefixBytes, (uint)highEdge, false);
			if (groupMemberNo == -1) {
				throw new Exception("bad binary search"); // can't happen
			}
			long highRecordNo = highEdge * _keyIndex.KeyGroupSize + groupMemberNo;

			return new TMSNStoreReaderEnumerable(this, lowRecordNo, highRecordNo);
		}

		#endregion
	}

	internal class KeyCacheReader {
		private uint _keyGroupSize = 0;
		// buffer for converting 5 byte longs into longs using BitConverter
		private byte[] _rawIndexOffsetPosition = new byte[8];
		private int _bytesPerIndex = 0;
		private byte[] _lastKeyBytes = new byte[20];
		private UTF8Encoding _encoding = new UTF8Encoding(false, false);

		private BinaryFileReader _cacheIndexReader = null;
		private BinaryFileReader _cacheReader = null;

		~KeyCacheReader() {
			Close();
		}

		public void Close() {
			_cacheIndexReader.Close();
			_cacheReader.Close();
		}

		public KeyCacheReader(string dbdir, uint keyGroupSize, bool mmapFiles) {
			_keyGroupSize = keyGroupSize;

#if NAMEDPIPEMODE
			string file = dbdir + ":cache-index";
			_cacheIndexReader = new BinaryFileReader(file, mmapFiles);

			file = dbdir + ":cache";
			_cacheReader = new BinaryFileReader(file, mmapFiles);

			_bytesPerIndex = TMSNStoreUtils.BytesPerKeyIndexOffset;

#else
			string file = Path.Combine(dbdir, "cache-index");
			_cacheIndexReader = new BinaryFileReader(file, mmapFiles);

			file = Path.Combine(dbdir, "cache");
			_cacheReader = new BinaryFileReader(file, mmapFiles);
			_bytesPerIndex = TMSNStoreUtils.BytesPerKeyIndexOffset;
#endif
		}

		public int CompareKey(uint keyGroupNo, byte[] toCompareBytes, bool prefixOnly) {
			uint internalKeyGroupNo = keyGroupNo / _keyGroupSize;
			int internalMemberNo = (int)(keyGroupNo % _keyGroupSize);

			long groupOffset;

			_cacheIndexReader.Seek(internalKeyGroupNo * _bytesPerIndex, SeekOrigin.Begin);
			for (int i = 0; i < _bytesPerIndex; i++) {
				_rawIndexOffsetPosition[i] = _cacheIndexReader.ReadByte();
			}

			groupOffset = BitConverter.ToInt64(_rawIndexOffsetPosition, 0);
			_cacheReader.Seek(groupOffset, SeekOrigin.Begin);
			

			// when searching for a prefix make sure lastKeyBytes is at least as big as the sought after bytes
			if (_lastKeyBytes.Length < toCompareBytes.Length) {
				_lastKeyBytes = new byte[toCompareBytes.Length + 2];
			}

			uint neu, overlap, lexLen;

			do {
				overlap = _cacheReader.ReadVariableLength();
				neu = _cacheReader.ReadVariableLength();
				lexLen = overlap + neu;

				if (_lastKeyBytes.Length < lexLen) {
					byte[] lastLast = _lastKeyBytes;
					_lastKeyBytes = new byte[lexLen + 2];
					Buffer.BlockCopy(lastLast, 0, _lastKeyBytes, 0, (int) overlap);
				}

				_cacheReader.Read(_lastKeyBytes, (int)overlap, (int)neu);

#if DEBUG
                string candidate2 = _encoding.GetString(_lastKeyBytes, 0, (int)lexLen);			// if toCompareBytes represents a prefix we don't need to match entire key
#endif

				internalMemberNo--;
			} while (internalMemberNo != -1);
			
			int diff = 0;
#if DEBUG
			string sought = _encoding.GetString(toCompareBytes);
			string candidate = _encoding.GetString(_lastKeyBytes, 0, (int)lexLen);			// if toCompareBytes represents a prefix we don't need to match entire key
#endif
			if (prefixOnly) {
				if (toCompareBytes.Length > lexLen) {
					_lastKeyBytes[lexLen] = 0; // make sure the first position that toCompare is longer is zeroed for comparison
				}
				diff = TMSNStoreUtils.Utf8BytesCompare(_lastKeyBytes, toCompareBytes.Length, toCompareBytes, toCompareBytes.Length);
			}
					
				// else must match everything
			else {
				diff = TMSNStoreUtils.Utf8BytesCompare(_lastKeyBytes, (int)lexLen, toCompareBytes, toCompareBytes.Length);
			}

			return diff;
		}

		public byte[] GetKey(uint keyGroupNo) {
			throw new Exception("fixme");
		}
	}
}
