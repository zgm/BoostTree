using System;
using System.IO;
using System.Collections;
using Microsoft.TMSN.IO;
using System.Text;
using System.Reflection;

namespace Microsoft.TMSN.Data {

#if false
	internal class PositionLineReader {
		private Stream _fileStream;
		private byte[] _buffer;
		private int _currentBufferPosition = 0;
		private long _numRead = 0;
		private long _fileLength = 0;
		private long _currentFilePosition = 0;
		private bool _refilledBuffer = false;
		private UTF8Encoding _encoding = new UTF8Encoding(false, false);

		public PositionLineReader(string filePath, int localBufferSize) {
			_fileStream = ZStreamIn.OpenUnbuffered(filePath);
			_buffer = new byte[localBufferSize];
			_numRead = _fileStream.Read(_buffer, 0, _buffer.Length);
			_fileLength = _fileStream.Length;
		}

		public long Position {
			get {
				return _currentFilePosition;
			}
		}

		public string ReadLine() {
			if (_currentBufferPosition == 10241) {
				Console.WriteLine("");
			}

			if (_currentFilePosition == _fileLength) return null;

			int i = _currentBufferPosition;
			while (i < _buffer.Length && _buffer[i] != '\n') i++;

			if (i >= _buffer.Length && _numRead < _fileLength) {
				if (_refilledBuffer) {
					throw new Exception("PositionLineReader buffer to small to hold complete line");
				}

				// move rest to the beginning of the buffer
				int restLen = i - _currentBufferPosition;
				Buffer.BlockCopy(_buffer, _currentBufferPosition, _buffer, 0, restLen);
				_numRead += _fileStream.Read(_buffer, restLen, _buffer.Length - restLen);
				_refilledBuffer = true;
				_currentBufferPosition = 0;
				return ReadLine();
			}

			int lineLen = i - _currentBufferPosition;
			string line = _encoding.GetString(_buffer, _currentBufferPosition, lineLen);

			_currentFilePosition += lineLen + 1; 
			_currentBufferPosition = i + 1; // go past the cr
			_refilledBuffer = false;

			return line;
		}

		

	}
#endif

	/// <summary>
	/// Provides a binaryReader interface to a file.  If mmapFile is false, it tries
	/// to read the entire contents into memory, if this fails it tries to memory map
	/// the file, if this fail it opens a regular file stream on the file.  If mmapFile
	/// is true, it tries to mmap the file, if this fails it opens a regular file stream
	/// on the file.
	/// </summary>
	public class BinaryFileReader : VariableLengthBinaryReader {
		Stream _stream = null;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="filename"></param>
		/// <param name="mmapFile"></param>
		public BinaryFileReader(string filename, bool mmapFile)
			: base(_ConstructorInit(filename, mmapFile)) {
			_stream = base.BaseStream;
		}
		
		private static Stream _ConstructorInit(string filename, bool mmapFile) {
			Stream stream = null;
            bool mmapAble = true;

            // the MemoryMappedFile object doesn't handle files > 4GB
            FileInfo fi = new FileInfo(filename);
            if (fi.Length >= uint.MaxValue) mmapAble = false;

			if (!mmapFile) {
				Stream fs = ZStreamIn.Open(filename);
				//Stream fs = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read);
				try {
					byte[] buffer = new byte[fs.Length];
					fs.Read(buffer, 0, (int)fs.Length);
					stream = new MemoryStream(buffer);
				}

				catch {
					stream = null;
				}
			}

			// either malloc failed or we wanted to mmap from the start
			MemoryMappedFile mmap = null;
			if (stream == null && mmapAble) {
				try {
					mmap = new MemoryMappedFile(filename);
					if (mmap != null) stream = mmap.MapView();
				} catch {
					if (mmap != null) mmap.Dispose();
				}
			}

			// we failed at mmap so open a filestream
			if (stream == null) {
#if NAMEDPIPEMODE
                stream = ZStreamIn.Open(filename);
#else
				stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read); 
#endif
			}

			return stream;
		}

		/// <summary>
		/// Seek the underlying stream.
		/// </summary>
		/// <param name="offset"></param>
		/// <param name="origin"></param>
		/// <returns></returns>
		public long Seek(long offset, SeekOrigin origin) {
			return _stream.Seek(offset, origin);
		}
	}

	internal class SortedRecordMerger : InternalRecordSource {
		private InternalRecordSource[] _heapArray = null;
		private int _numSources = 0;
		private bool _mergeAscending = true;
		//private DataRecord _recordInstance = null;

		// segmentedInputMode, when true means that the key space of the input files
		// is segmented, i.e. file1=a-h file2=i-m file3=n-z.
		private bool _segmentedInputMode = false; // when the key space is segmented

		//private RawRecordWrapper _rawRecordWrapper = new RawRecordWrapper();

		public SortedRecordMerger() {
			// we don't set any properties, just let them flow from input to output.
		}

		private bool _IsLeaf(int pos) {
			return pos >= _numSources/2;
		}

		private int _Parent(int position) {
			return (position -1) / 2;
		}

		private int _LeftChild(int position) {
			return (position * 2 + 1);
		}
			
		private int _RightChild(int position) {
			return (position * 2 + 2);
		}

		private void _AddInput(InternalRecordSource input) {
			if (!input.Sorting.IsSorted) {
				throw new Exception("can't merge non-sortable record source");
			}

			bool notEmpty = input.MoveNext();
			if (!notEmpty) return;

			int current = _numSources++;

			_heapArray[current] = input;
	
			int parent = _Parent(current);

			// while the current is less than the parent
			while ((current != 0) &&
				_MergeCompare(_heapArray[current].CurrentRecord.KeyBytes,
				_heapArray[parent].CurrentRecord.KeyBytes)) {
				_Swap(current, parent);
				current = _Parent(current);
				parent = _Parent(current);
			}
		}

		private void _SiftDown(int pos) {
			while (!_IsLeaf(pos)) {
				int compPosition = _LeftChild(pos);
				int rightChild = _RightChild(pos);

				if ((rightChild < _numSources)
					&& !_MergeCompare((byte[])_heapArray[compPosition].CurrentRecord.KeyBytes,
					(byte[])_heapArray[rightChild].CurrentRecord.KeyBytes)) {
					compPosition = rightChild;
				}

				if (_MergeCompare((byte[])_heapArray[pos].CurrentRecord.KeyBytes,
					(byte[])_heapArray[compPosition].CurrentRecord.KeyBytes))
					return; // we're done

				// else
				_Swap(pos, compPosition);
				pos = compPosition;
			}
		}

		private bool _MergeCompare(byte[] left, byte[] right) {
			int diff = TMSNStoreUtils.Utf8BytesCompare(left, right);
			if (_mergeAscending) {
				if (diff > 0) return true;
				else return false;
			}

			// not ascending
			if (diff < 0) return true;
			else return false;
		}

		/// <summary>
		/// if true will trust that input is bucketted and only siftdown
		/// when top enumerator is exhausted.
		/// </summary>
		public bool SegmentedInputMode {
			get {
				return _segmentedInputMode;
			}
			set {
				_segmentedInputMode = value;
			}
		}

		public override bool MoveNext() {
			// initialize
			if (_heapArray == null) {

				// if there is only one input no need to use the heap.
				if (Inputs.Length == 1) {
					bool notDone = Inputs[0].MoveNext();
					if (!notDone) return false;

					CurrentRecord = Inputs[0].CurrentRecord;
					return true;
				}

				_heapArray = new InternalRecordSource[Inputs.Length];
				int i = 0;
				foreach (InternalRecordSource input in Inputs) {
					_AddInput(input);
					_mergeAscending = input.Sorting.IsSortedAscending;
					// check to make sure sorted direction is correct for subsequent inputs
					if (i != 0 && _mergeAscending != input.Sorting.IsSortedAscending) {
						throw new Exception("improperly sorted merger input");
					}
				}
			}

            DataRecord outputRecord = _RemoveMin();
            CurrentRecord = outputRecord;

			if (outputRecord == null) return false;
			else return true;
		}

		public DataRecord _RemoveMin() {
			if (_numSources == 0) return null;

			DataRecord record = _heapArray[0].CurrentRecord;

			bool notEmpty = _heapArray[0].MoveNext();

			if (notEmpty) {
				// only sift down if not segmented
				if (!_segmentedInputMode)_SiftDown(0);
			}

				// the zero position enumerator was empty so
			else {
				_Swap(0, --_numSources);
				if (_numSources != 0) _SiftDown(0);
			}

			return record;
		}

		private void _Swap(int posa, int posb) {
			InternalRecordSource temp = _heapArray[posa];
			_heapArray[posa] = _heapArray[posb];
			_heapArray[posb] = temp;
		}
	}

	internal class InternalRecordFileReader : InternalRecordSource {
		private Stream _fileStream;
		private VariableLengthBinaryReader _fileReader;
		private string _filename;
		//private DataRecord _recordInstance = null;
		private long _currentRecordNo = 0;
		private long _fileLength = 0;
		private long _currentFilePosition = 0;

		public InternalRecordFileReader(string filepath) {
			_filename = filepath;
			_fileStream = ZStreamIn.Open(_filename);
			this.ReadProperties(_fileStream);
			_fileReader = new VariableLengthBinaryReader(_fileStream);
			_currentFilePosition = _fileStream.Position;
			_fileLength = _fileStream.Length;
		}

        public override string CurrentSourceName {
            get {
                return _filename;
            }
        }

        public override TStorageType StorageType {
            get {
                return TStorageType.RecordFile;
            }
        }

		public override bool MoveNext() {

			// don't poll Stream.Position -- way slow.
			if (_currentFilePosition >= _fileLength) {
				return false;
			}

			uint keyLen = _fileReader.ReadVariableLength(ref _currentFilePosition);

            DataRecord outputRecord = ConstructInstance();

			// read the key
			byte[] keyBytes = _fileReader.ReadBytes((int)keyLen);
			_currentFilePosition += keyLen;
			
			// read data len
			uint dataLen = _fileReader.ReadVariableLength(ref _currentFilePosition);
			byte[] data = _fileReader.ReadBytes((int)dataLen);
			_currentFilePosition += dataLen;

			outputRecord.KeyBytes = keyBytes;
			outputRecord.Data = data;
			outputRecord._recordNo = _currentRecordNo++;

            CurrentRecord = outputRecord;

			return true;
		}

		public override void Close() {
			try {
				if (_fileStream != null) _fileStream.Close();
					
			}
			catch {
				// ignore
			}

            base.Close();
		}
	}

	/// <summary>
	/// 
	/// </summary>
	public class RecordFileReader {
		private InternalRecordFileReader _internalRecordFileReader = null;
		/// <summary>
		/// 
		/// </summary>
		/// <param name="filepath"></param>
		public RecordFileReader(string filepath) {
			_internalRecordFileReader = new InternalRecordFileReader(filepath);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public bool MoveNext() {
			return _internalRecordFileReader.MoveNext();
		}

		/// <summary>
		/// 
		/// </summary>
		public DataRecord CurrentRecord {
			get {
				return _internalRecordFileReader.CurrentRecord;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		public void Close() { }
	}

	/// <summary>
	/// 
	/// </summary>
	public class RecordFileWriter {
		private Stream _outputStream = null;
		private VariableLengthBinaryWriter _outputWriter = null;
		private string _filepath;
		private long _totalRecordBytesEstimate = 0;
		private long _totalRecordsEstimate = 0;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="filepath"></param>
		public RecordFileWriter(string filepath) {
			_filepath = filepath;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		public void WriteRecord(DataRecord record) {
			if (_outputStream == null) {
                OpenOutputStream();
				_outputWriter = new VariableLengthBinaryWriter(_outputStream);
				EmptyInternalSource e = new EmptyInternalSource();

				e.CurrentRecord = record; // updates the record info
                e.WriteProperties(_outputStream);
			}

			_outputWriter.WriteVariableLength((uint)record.KeyBytes.Length);
			_outputWriter.Write(record.KeyBytes);

			// output data
			if (record.Data != null) {
				_outputWriter.WriteVariableLength((uint)record.Data.Length);
				_outputWriter.Write(record.Data);
			}
			else {
				_outputWriter.WriteVariableLength((uint)0);
			}

			// slow do elsewhere _totalRecordBytesEstimate += rawRecord.Length;
			_totalRecordsEstimate++;
		}

		/// <summary>
		/// 
		/// </summary>
		public void Close() {
			if (_outputStream == null) return; // already done

			InternalRecordSource.WriteEstimates(_outputStream, _totalRecordBytesEstimate, _totalRecordsEstimate);
			_outputStream.Close();
			_outputStream = null;
		}

		internal bool WriteRecordFileMaxSize(InternalRecordSource input, long maxFileSize) {
			// initialize
			if (_outputStream == null) {
                OpenOutputStream();
				_outputWriter = new VariableLengthBinaryWriter(_outputStream);
				input.WriteProperties(_outputStream);
			}

			bool done = false;
			while (!done && input.MoveNext()) {
				DataRecord record = input.CurrentRecord;
				// output key
				_outputWriter.WriteVariableLength((uint)record.KeyBytes.Length);
				_outputWriter.Write(record.KeyBytes);

				// output data
				if (record.Data != null) {
					_outputWriter.WriteVariableLength((uint)record.Data.Length);
					_outputWriter.Write(record.Data);
				}
				else {
					_outputWriter.WriteVariableLength((uint)0);
				}

				if (maxFileSize != -1 && _outputStream.Position >= maxFileSize) {
					done = true;
				}
			}									

			_outputStream.Close();
			_outputStream = null;

			// we return notDone to our caller, i.e. we return true if there is more
			// to be written, false if we wrote it all.
			return done;
		}

        internal void OpenOutputStream()
        {
	        _outputStream = new FileStream(_filepath, FileMode.Create, FileAccess.Write, FileShare.None, 1024 * 1024 * 16);
        }


		internal void Write(InternalRecordSource input) {

			while (input.MoveNext()) {
				DataRecord record = input.CurrentRecord;
			
				// we need to do a moveNext on our input in order for RecordInstance
				// to be valid.
				if (_outputStream == null) {
                    OpenOutputStream();
					input.WriteProperties(_outputStream);
					_outputWriter = new VariableLengthBinaryWriter(_outputStream);
				}

				// output key
				_outputWriter.WriteVariableLength((uint)record.KeyBytes.Length);
				_outputWriter.Write(record.KeyBytes);

				// output data
				if (record.Data != null) {
					_outputWriter.WriteVariableLength((uint)record.Data.Length);
					_outputWriter.Write(record.Data);
				}
				else {
					_outputWriter.WriteVariableLength((uint)0);
				}

				// slow. do elsewhere _totalRecordBytesEstimate = _outputStream.Position;
				_totalRecordsEstimate++;
			}


			_totalRecordBytesEstimate = _outputStream.Position;
			InternalRecordSource.WriteEstimates(_outputStream, _totalRecordBytesEstimate, _totalRecordsEstimate);
			_outputStream.Close();
			_outputStream = null;
		}
	}

	/// <summary>
	/// for iterating through an unfull array
	/// </summary>
	internal class BoundedEnumerator : IEnumerator {
		private IEnumerator _enumerator = null;
		private int _boundedSize;
		private int _currentElement = -1;

		#region IEnumerator Members

		public BoundedEnumerator(IEnumerator enumerator, int boundedSize) {
			_enumerator = enumerator;
			_boundedSize = boundedSize;
		}

		public void Reset() {
			_enumerator.Reset();
		}

		public object Current {
			get {
				return _enumerator.Current;
			}
		}

		public bool MoveNext() {
			_currentElement++;
			if (_currentElement >= _boundedSize) return false;

			return _enumerator.MoveNext();
		}

		#endregion
	}

	internal class CascadedEnumerator : IEnumerator {
		IEnumerator[] _enumerators = null;
		int _currentEnumerator = 0;

		public CascadedEnumerator(IEnumerator[] enumerators) {
			_enumerators = enumerators;
		}

		#region IEnumerator Members

		public void Reset() {

		}

		public object Current {
			get {
				return _enumerators[_currentEnumerator].Current;
			}
		}

		public bool MoveNext() {

			// don't even loop if we don't have any enumerators
			while (_enumerators != null) {
				bool moveNext = _enumerators[_currentEnumerator].MoveNext();
				if (moveNext) return true;

				if (_currentEnumerator < _enumerators.Length - 1)
					_currentEnumerator++;

				else return false;
			}

			return false;
		}

		#endregion


		public void SortEnumeratorsByFirstObject(IComparer comparer) {
			foreach (IEnumerator e in _enumerators) {
				bool notDone = e.MoveNext();

			}
		}
	}

	/// <summary>
	/// Summary description for SimpleDbUtils.
	/// </summary>
	internal class TMSNStoreUtils {
        public static long LongAsFiveBytes(byte[] buffer, int offset) {

            // must add to ulong so that neg doesn't spread in big numbers.
            ulong answer = buffer[offset++]; // 1
            answer |= (ulong)buffer[offset++] << 8; // 2
            answer |= (ulong)buffer[offset++] << 16; // 3
            answer |= (ulong)buffer[offset++] << 24; // 4
            answer |= (ulong)buffer[offset++] << 32; // 5

            // typecast back to long
            return (long)answer;
        }

        public static uint KeyGroupSize = 32;
		public static int BytesPerKeyIndexOffset = 5;

		public static uint KeyLength(byte[] rawRecord) {
			// figure out the key length to return it
			uint i = 0;
			while (i < rawRecord.Length) {
				if (rawRecord[i] == 0xFF) {
					return i;
				}
				i++;
			}

			return 0;
		}

		public static uint CalculateOverlap(byte[] previous, byte[] current) {
			if (previous == null) return 0;

			uint len = (uint)Math.Min(previous.Length, current.Length);

			for (uint i = 0; i < len; i++) {
				if (previous[i] != current[i] 
					|| previous[i] == 0xFF
					|| current[i] == 0xFF) return i;
			}
			return len;
		}

		// assume non-zero length 
		public static int Utf8BytesCompare(byte[] key1, byte[] key2, int key2Len) {
			int diff;
			bool key2Shorter = false;

			int len = key1.Length;

			if (key2Len < len) {
				len = key2Len;
				key2Shorter = true;
			}

			for (uint i = 0; i < len; i++) {
				byte b1 = key1[i];
				byte b2 = key2[i];

				diff = b2 - b1;

				// when either string reaches it's zero
				// (and the other one isn't at zero which is caught above)
				// the difference must be non-zero so we break

				if (diff != 0) return diff;
			}

			// at this point strings are equal for the length that overlaps
			
			if (key2Shorter) return -1;
			else if (key1.Length == key2Len) return 0;
			else return 1;
		}

		// assume non-zero length 
		public static int Utf8BytesCompare(byte[] key1, int key1Len, byte[] key2, int key2Len) {
			int diff;
			bool key2Shorter = false;

			int len = key1Len;

			if (key2Len < len) {
				len = key2Len;
				key2Shorter = true;
			}

			for (uint i = 0; i < len; i++) {
				byte b1 = key1[i];
				byte b2 = key2[i];

				diff = b2 - b1;

				// when either string reaches it's zero
				// (and the other one isn't at zero which is caught above)
				// the difference must be non-zero so we break

				if (diff != 0) return diff;
			}

			if (key2Shorter) return -1;
			else if (key1Len == key2Len) return 0;
			else return 1;
		}

		public static int Utf8BytesCompare(byte[] key1, byte[] key2) {
			int diff;
			
			bool key2Shorter = false;

			int len = key1.Length;
			if (key2.Length < len) {
				len = key2.Length;
				key2Shorter = true;
			}

			for (int i = 0; i < len; i++) {
				byte b1 = key1[i];
				byte b2 = key2[i];

				diff = b2 - b1;

				// when either string reaches it's zero
				// (and the other one isn't at zero which is caught above)
				// the difference must be non-zero so we break

				if (diff != 0) return diff;
			}

			if (key2Shorter) return -1;
			else if (key1.Length == key2.Length) return 0;
			else return 1;
		}

		public class UTF8BytesComparer : IComparer {
			#region IComparer Members

			public int Compare(object x, object y) {
				byte[] bx = (byte[]) x;
				byte[] by = (byte[]) y;

				return TMSNStoreUtils.Utf8BytesCompare(by, bx);
			}

			#endregion
		}

		public static DataRecord CreateRecordInstance(DataRecord record) {
			ConstructorInfo constructor = TMSNStoreUtils.GetConstructor(record);
			DataRecord newRecord = (DataRecord)constructor.Invoke(null);
			return newRecord;
		}

		public static ConstructorInfo GetConstructor(DataRecord record) {
			Type recordType = record.GetType();
			ConstructorInfo constructor = recordType.GetConstructor(new Type[0]);
			return constructor;
		}
	}
}
