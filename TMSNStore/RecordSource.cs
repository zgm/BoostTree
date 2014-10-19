using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Microsoft.TMSN.IO;
using System.Text.RegularExpressions;


namespace Microsoft.TMSN.Data {
	/// <summary>
	/// Summary description for InternalRecordSource.
	/// </summary>

    internal abstract class InternalRecordSource {
		protected ArrayList _inputList = new ArrayList();
        private bool _propertiesSet = false;
        protected SortInfo _sorting = new SortInfo();
        protected BucketInfo _bucketting = new BucketInfo();
        protected SegmentInfo _segmenting = new SegmentInfo();
        private long _totalRecordsEstimate = 0;
        private long _totalRecordBytesEstimate = 0;
        protected RecordInformation _recordInfo = new RecordInformation('\t');
        private DataRecord _currentRecord = null;
        private bool _recordInfoUpdated = false;
		internal string ProcessTreeComment = null;

        private RecordConstructor _recordConstructor = null;
        
        // single hashtable for assembly location for all InternalRecordSources
        protected static Hashtable _assemblyLocations = new Hashtable();

        internal string GetAssemblyLocation(string recordTypeString) {
            object o = _assemblyLocations[recordTypeString];
            if (o == null) return null;

            return (string)o;
        }

        internal void AddAssemblyLocation(string recordTypeString, string assemblyLocation) {
            _assemblyLocations[recordTypeString] = assemblyLocation;
        }

        internal DataRecord ConstructInstance() {
            if (_recordConstructor == null) {
                _recordConstructor = _recordInfo.GetRecordConstructor(this);
            }

            return _recordConstructor.Construct();
		}

		public List<string> Statistics {
			get {
				List<string> stats = new List<string>();

				stats.Add("CurrentSourceName:\t" + CurrentSourceName);
				stats.Add("RecordType:\t" + _recordInfo.RecordType.ToString());
				if (_recordInfo.RecordType == typeof(TableRecord)) {
					foreach (string colname in _recordInfo.TableColumnNames) {
						stats.Add("TableColumn\t" + colname);
					}
				}

				stats.Add("NumRecords:\t" + TotalRecordsEstimate);
				stats.Add("NumRecordBytes:\t" + TotalRecordBytesEstimate);
				stats.Add("IsSorted:\t" + Sorting.IsSorted);
				stats.Add("IsSortedAscending:\t" + Sorting.IsSortedAscending);
				stats.Add("IsReduced:\t" + IsReduced);
				stats.Add("IsBucketted:\t" + Bucketting.IsBucketted);
				stats.Add("NumBuckets:\t" + Bucketting.NumBuckets);
				stats.Add("BucketNo:\t" + Bucketting.BucketNo);
				stats.Add("IsSegmented:\t" + Segmenting.IsSegmented);
				stats.Add("NumSegments:\t" + Segmenting.NumSegments);
				stats.Add("SegmentBeginKey:\t" + Segmenting.SegmentBeginKey);

				return stats;
			}
		}

		protected bool _passThruInputSorting = true; // must be set to true or false in component constructor
		public class SortInfo {
			public bool IsSorted = false;
			public bool IsSortedAscending = false;
		}

		protected bool _passThruInputBucketting = true; // must be set to true or false in component constructor
		public class BucketInfo {
			public bool IsBucketted = false;
			public int BucketNo = 0;
			public int NumBuckets = 0;
		}

		protected bool _passThruInputSegmenting = true; // must be set to true or false in component constructor
		public class SegmentInfo {
			public bool IsSegmented = false;
			public string SegmentBeginKey = null;
			public int NumSegments = 0;
		}

		protected bool _passThruInputReduced = true; // must be set to true or false in component constructor
		protected bool _isReduced = false;
		public virtual bool IsReduced {
			get {
				if (!_propertiesSet) SetProperties();
				return _isReduced;
			}
			set {
				_isReduced = value;
			}
				
		}

        public void PrintProcessTree(int level) {
			// if ProcessTreeComment == Logging we ignore the source
			if (ProcessTreeComment != null && ProcessTreeComment.StartsWith("Logging")) {
				InternalRecordSource source = Inputs[0];
				source.ProcessTreeComment += ProcessTreeComment.Substring(7);
				source.PrintProcessTree(level);

				return;
			}

            if (level != 0) {
                // space over
                for (int i = 1; i < level; i++) Console.Error.Write("   ");
                Console.Error.Write("^--");
            }

            string type = GetType().ToString(); // get the type of this string
            if (type.StartsWith("Microsoft.TMSN.Data.")) {
                type = type.Remove(0, 20);
            }

            Console.Error.Write(type);
            if (ProcessTreeComment != null) Console.Error.Write(ProcessTreeComment);

            Console.Error.WriteLine();

			foreach (InternalRecordSource input in Inputs) {
				input.PrintProcessTree(level + 1);
			}
        }

        public virtual TStorageType StorageType {
            get {
                return Inputs[0].StorageType;
            }
        }

		public virtual string CurrentSourceName {
			get {
                return Inputs[0].CurrentSourceName;
			}
		}

		public virtual long CurrentSourcePosition {
			get {
				return -1;
			}
		}

		public virtual SortInfo Sorting {
			get {
				if (!_propertiesSet) SetProperties();
				return _sorting;
			}
			set {
				_sorting = value;
			}
		}

		public virtual BucketInfo Bucketting {
			get {
				if (!_propertiesSet) SetProperties();
				return _bucketting;
			}
			set {
				_bucketting = value;
			}
		}
		
		public virtual SegmentInfo Segmenting {
			get {
				if (!_propertiesSet) SetProperties();
				return _segmenting;
			}
			set {
				_segmenting = value;
			}
		}

		public virtual long TotalRecordsEstimate {
			get {
				if (!_propertiesSet) SetProperties();
				return _totalRecordsEstimate;
			}
			set {
				_totalRecordsEstimate = value;
			}
		}

		public virtual long TotalRecordBytesEstimate {
			get {
				if (!_propertiesSet) SetProperties();
				return _totalRecordBytesEstimate;
			}
			set {
				_totalRecordBytesEstimate = value;
			}
		}

#if false
		public RecordInformation RecordInfo {
			get {
				return _recordInfo;
			}
		}
#endif

		// guarenteed to be set AFTER the first MoveNext();
		public abstract bool MoveNext();

		/// <summary>
		/// the current record.
		/// </summary>
		public DataRecord CurrentRecord {
            get {
                return _currentRecord;
            }

            set {
                // if first time then set the record info
                if (!_recordInfoUpdated && value != null) {
                    _recordInfo.Update(value);
                    _recordInfoUpdated = true;
                }

                _currentRecord = value;
            }
		}

		/// <summary>
		/// If non-null before a MoveNext this tells the source to advance forward
		/// until reaching a record with Key lessthan or equal to the hint.  
		/// Sources upstream of hint will either 1) use the hint, 2) ignore the
		/// hint and pass it on further upstream, or 3) prevent the hint from 
        /// being passed further upstream.
		/// Examples of 1) a TStore will search. 2) logging source will pass it on.
		/// 3) a filter will prevent the hint from being passed on.
		/// In case 2) it is up to the source to pass on the hint (by calling MoveNextHint on
        /// its source).  Ignoring the hint comes for free.
		/// </summary>
		public virtual string MoveNextHint {
			set {
			}
		}

		/// <summary>
		/// Adds an input into the InternalRecordSource.  At this point we set our properties to be a function
		/// of our input's properties.  If the component doesn't want properties to propagate then it should
		/// set the appropriate _passThruInput... to false.
		/// </summary>
		/// <param name="input"></param>
		public void AddInput(InternalRecordSource input) {
			_inputList.Add(input);
			if (!_propertiesSet) SetProperties();
			_totalRecordsEstimate += input.TotalRecordsEstimate;
			_totalRecordBytesEstimate += input.TotalRecordBytesEstimate;

            // default is our output has the same record type as the input
            _recordInfo.Update(input._recordInfo);
		}

        /// <summary>
        /// Clears the inputs so that inputs may be re-added.
        /// </summary>
        public void ClearInputs() {
            _propertiesSet = false;
            _totalRecordBytesEstimate = 0;
            _totalRecordsEstimate = 0;
            _inputList.Clear();
        }

		public InternalRecordSource[] Inputs {
			get {
				return (InternalRecordSource[])_inputList.ToArray(typeof(InternalRecordSource));
			}
		}

		#region SERIALIZING
		private static void _Write(Stream outputStream, bool value) {
			byte[] buffer = null;
			buffer = BitConverter.GetBytes(value);
			outputStream.Write(buffer, 0, buffer.Length);
		}

		private static void _Write(Stream outputStream, long value) {
			byte[] buffer = BitConverter.GetBytes(value);
			outputStream.Write(buffer, 0, buffer.Length);
		}

		private static void _Write(Stream outputStream, ulong value) {
			byte[] buffer = BitConverter.GetBytes(value);
			outputStream.Write(buffer, 0, buffer.Length);
		}

		private static void _Write(Stream outputStream, int value) {
			byte[] buffer = BitConverter.GetBytes(value);
			outputStream.Write(buffer, 0, buffer.Length);
		}

		private static void _Write(Stream outputStream, string value) {
			UTF8Encoding encoding = new UTF8Encoding(false, false);
			byte[] buffer = encoding.GetBytes(value);
			_Write(outputStream, buffer.Length);
			outputStream.Write(buffer, 0, buffer.Length);
		}
		
		private static void _Read(Stream inputStream, out bool value) {
			int anInt = inputStream.ReadByte();
			if (anInt != 0) value = true;
			else value = false;
		}

		private static void _Read(Stream inputStream, out long value) {
			byte[] buffer = new byte[8];
			inputStream.Read(buffer, 0, 8);
			value = BitConverter.ToInt64(buffer, 0);
		}

		private static void _Read(Stream inputStream, out ulong value) {
			byte[] buffer = new byte[8];
			inputStream.Read(buffer, 0, 8);
			value = BitConverter.ToUInt64(buffer, 0);
		}

		private static void _Read(Stream inputStream, out int value) {
			byte[] buffer = new byte[4];
			inputStream.Read(buffer, 0, 4);
			value = BitConverter.ToInt32(buffer, 0);
		}

		private static void _Read(Stream inputStream, out string value) {
			UTF8Encoding encoding = new UTF8Encoding(false, false);
			int len;
			_Read(inputStream, out len);
			byte[] buffer = new byte[len];
			inputStream.Read(buffer, 0, len);
			value = encoding.GetString(buffer, 0, len);
		}

		internal static void WriteEstimates(Stream outputStream, long totalRecordBytesEstimate, long totalRecordsEstimate) {
			outputStream.Position = 16; // PAST THE BOMB!
			_Write(outputStream, totalRecordBytesEstimate);
			_Write(outputStream, totalRecordsEstimate);
		}

		private static ulong _bomb = 0xFFFEDADADADAFFFE;
		
		internal void WriteProperties(Stream outputStream) {
            if (outputStream.Position != 0)
			    outputStream.Position = 0;

			// write a bomb.  RecordFiles start with RecordSource properties.
			// by recognizing the presence or absence of the bomb we know
			// whether or not we have a RecordFile.
			_Write(outputStream, _bomb);

			// some space for future info
			ulong temp = 0;
			_Write(outputStream, temp);

			// estimates
			_Write(outputStream, TotalRecordBytesEstimate);
			_Write(outputStream, TotalRecordsEstimate);

			// sorting
			_Write(outputStream, Sorting.IsSorted);
			_Write(outputStream, Sorting.IsSortedAscending);

			// reduction
			_Write(outputStream, IsReduced);

			// bucketting
			_Write(outputStream, Bucketting.IsBucketted);
			_Write(outputStream, Bucketting.BucketNo);
			_Write(outputStream, Bucketting.NumBuckets);

			// segmenting
			_Write(outputStream, Segmenting.IsSegmented);
			_Write(outputStream, Segmenting.NumSegments);
			if (Segmenting.SegmentBeginKey == null) Segmenting.SegmentBeginKey = "";
			_Write(outputStream, Segmenting.SegmentBeginKey);

			// a string which is the name of the type.  In the special case of a TableRecord
            // we also store the column names here.
            string typeString = _recordInfo.RecordType.ToString();
            if (_recordInfo.TableColumnNames != null) {
				// type | col \t col ... | columnSeparator | keyColumnNo [\t KeyColumnNo]
                typeString += "|" + string.Join("\t", _recordInfo.TableColumnNames);
				typeString += "|" + _recordInfo.TableColumnSeparator + "|";
				for (int j = 0; j < _recordInfo.KeyColumnNos.Length; j++) {
					if (j != 0) typeString += "\t";
					typeString += _recordInfo.KeyColumnNos[j];
				}
            }
			_Write(outputStream, typeString);

			string assemblyName = "";

			byte[] asmBytes = new byte[0]; // zero size in case don't need it

			// copy the assembly containing the record type to the database
			Assembly recAsm = Assembly.GetAssembly(_recordInfo.RecordType);
			Assembly thisAsm = Assembly.GetCallingAssembly();
			if (recAsm != thisAsm) {
                string recAsmLocation = recAsm.Location;
                if (recAsmLocation == null || recAsmLocation.Length == 0) {
                    recAsmLocation = GetAssemblyLocation(typeString);
                    //recAsmLocation = RecordInfo.AssemblyLocation;
                }

                if (recAsmLocation == null || recAsmLocation.Length == 0) {
                    throw new Exception("cannot determine record assembly location");
                }

				Stream stream = ZStreamIn.Open(recAsmLocation);
				asmBytes = new byte[stream.Length];
				stream.Read(asmBytes, 0, (int)stream.Length);
				stream.Close();

				int comma = recAsm.FullName.IndexOf(",");
				if (comma < 0) throw new Exception("bad assembly name");
				assemblyName = recAsm.FullName.Substring(0, comma);
			}

			// output length of assembly
			_Write(outputStream, asmBytes.Length);

			// output assembly
			outputStream.Write(asmBytes, 0, asmBytes.Length);

			// output assembly name
			_Write(outputStream, assemblyName);
		}
	
		internal void ReadProperties(Stream inputStream) {
            if (inputStream.Position != 0)
			    inputStream.Position = 0;

			// read bomb
			ulong temp;
			_Read(inputStream, out temp);

			if (temp != _bomb) {
				throw new Exception("bad bomb: " + _bomb.ToString() + "!= " + temp.ToString());
			}

			// read info
			_Read(inputStream, out temp);

			// estimates
			_Read(inputStream, out _totalRecordBytesEstimate);
			_Read(inputStream, out _totalRecordsEstimate);

			// sorting
			_Read(inputStream, out _sorting.IsSorted);
			_Read(inputStream, out _sorting.IsSortedAscending);

			// reduction
			_Read(inputStream, out _isReduced);

			// bucketting
			_Read(inputStream, out _bucketting.IsBucketted);
			_Read(inputStream, out _bucketting.BucketNo);
			_Read(inputStream, out _bucketting.NumBuckets);

			// segmenting
			_Read(inputStream, out _segmenting.IsSegmented);
			_Read(inputStream, out _segmenting.NumSegments);
			_Read(inputStream, out _segmenting.SegmentBeginKey);

			// record type string.  In the special case of a TableRecord, also contains columnNames
			// this needs to be cleaned up.  Too many vars in this string.
			string recordTypeString;
            string[] tableColumnNames = null;
			char tableColumnSeparator = '\t';
			int[] keyColumnNos = null;
			_Read(inputStream, out recordTypeString);
            int pipe = recordTypeString.IndexOf('|'); // divides the typeString from the columnNames
            if (pipe >= 0) {
				string[] cols = recordTypeString.Split('|');
				recordTypeString = cols[0];
                tableColumnNames = cols[1].Split('\t');
				tableColumnSeparator = cols[2][0]; // only one char
				cols = cols[3].Split('\t');
				keyColumnNos = new int[cols.Length];
				for (int j = 0; j < cols.Length; j++) {
					keyColumnNos[j] = int.Parse(cols[j]);
				}
            }

			// read in size of asm bytes
			int asmSize;
			_Read(inputStream, out asmSize);

			Assembly asm = null;
			byte[] recordAssemblyBytes = null;

			// must read these bytes to advance the stream (but might not need them)
			if (asmSize != 0) {
				recordAssemblyBytes = new byte[asmSize];
				inputStream.Read(recordAssemblyBytes, 0, asmSize);
			}

			string assemblyName;
			_Read(inputStream, out assemblyName);
			if (assemblyName.Length == 0) assemblyName = null; // so we can only test for null

			// get entry assembly (in case record defined in executable)
			asm = Assembly.GetEntryAssembly();
            Type recordType = null;
            string executableDir = null;

            // if we are a web app there is no entry assembly
            if (asm != null) {

                // ask for the type
                recordType = asm.GetType(recordTypeString);

                // we are going to copy the record assembly bytes to the directory of the executable
                // it needs to live here for loading for various unexplicable reasons.
                executableDir = Path.GetDirectoryName(asm.Location);
            }

			if (recordType == null) {
				// look in our assembly
				asm = Assembly.GetCallingAssembly();
				recordType = asm.GetType(recordTypeString);
			}

			string assemblyLocation = null;
			// use the assembly saved in the record info if it's there
			if (recordType == null &&
                recordAssemblyBytes != null &&
                recordAssemblyBytes.Length != 0) {
				assemblyLocation = Path.Combine(executableDir, assemblyName + ".dll");

                // could be there from another instance (version problems?)
                if (!File.Exists(assemblyLocation)) {
                    using (FileStream fs = new FileStream(assemblyLocation, FileMode.Create)) {
                        fs.Write(recordAssemblyBytes, 0, recordAssemblyBytes.Length);
                    }
                }

				asm = Assembly.LoadFile(assemblyLocation);
				recordType = asm.GetType(recordTypeString);
			}

			if (recordType == null) recordType = typeof(DataRecord);

			// set up record info
			_recordInfo.Update(recordType, tableColumnNames, keyColumnNos, tableColumnSeparator);
			if (assemblyLocation != null) {
				AddAssemblyLocation(recordTypeString, assemblyLocation);
			}
		}

		#endregion SERIALIZING

        public virtual void Close() {
            foreach (InternalRecordSource input in Inputs) {
                input.Close();
            }
        }

		internal void SetProperties() {
			if (_propertiesSet) return;

			// first make sure your set to your input's properties
			foreach (InternalRecordSource source in _inputList) {
				source.SetProperties();
				if (_passThruInputSorting) _sorting = source.Sorting;
				if (_passThruInputBucketting) _bucketting = source.Bucketting;
				if (_passThruInputSegmenting) _segmenting = source.Segmenting;
				if (_passThruInputReduced) _isReduced = source.IsReduced;
			}

			_propertiesSet = true;
		}
	}

	/// <summary>
	/// Allows user to define programmatic source.  Delegate returns next DataRecord from source.
	/// Must return null when source is depleted.
	/// </summary>
	/// <returns></returns>
	public delegate DataRecord SourceDelegate();

	internal class InternalUserSource : InternalRecordSource {
		private SourceDelegate _sourceDelegate = null;
		private IUserSource _userSource = null;

		public InternalUserSource(SourceDelegate sourceDelegate) {
			Sorting.IsSorted = false;
			Sorting.IsSortedAscending = false;
			_sourceDelegate = sourceDelegate;
		}

		public InternalUserSource(IUserSource userSource) {
            Sorting.IsSorted = userSource.IsSorted;
            Sorting.IsSortedAscending = userSource.IsSortedAscending;
			_userSource = userSource;
		}

		public override bool MoveNext() {
			if (_userSource != null) {
				CurrentRecord = _userSource.NextRecord();
			}

			else {
				CurrentRecord = _sourceDelegate();
			}

			if (CurrentRecord != null) return true;

			return false;
		}

		public override void Close() {
			if (_userSource != null) {
				_userSource.Close();
			}

            base.Close();
		}
	}

	/// <summary>
	/// Interface allows user to define programmatic source.
	/// </summary>
	public interface IUserSource {
		/// <summary>
		/// Returns the next DataRecord from the source.  Must return null at end of source.
		/// </summary>
		/// <returns></returns>
		DataRecord NextRecord();

		/// <summary>
		/// Is called when source is depleted
		/// </summary>
		void Close();

        /// <summary>
        /// Indicates whether the record source is sorted.
        /// </summary>
        bool IsSorted {
            get;
        }

        /// <summary>
        /// Indicates whether the record source is sorted ascending.
        /// </summary>
        bool IsSortedAscending {
            get;
        }
	}

	internal class EmptyInternalSource : InternalRecordSource {
        public EmptyInternalSource() {
            // doesn't matter really but prevents a sorter from being introduced
            Sorting.IsSorted = true;
            Sorting.IsSortedAscending = true;
        }

		public override bool MoveNext() {
            CurrentRecord = null;
			return false;
		}
	}

    internal class LoggingSource : InternalRecordSource {
        private InternalRecordSource _source = null;
        private long _recordCount = 0;
        private bool _doEstimate = false;
        private long _lineBytes = 0;
        private long _nextLogCount = 0;
        private long _logInterval = 0;
        private int _numLogs = 20; // 5%
        private DateTime _beginDateTime;
		private string _displayString = null;

		public LoggingSource() {
			ProcessTreeComment = "Logging";
		}

		// since we only log we just pass it on.  This may screw us up tho.
		public override string MoveNextHint {
			set {
				_source.MoveNextHint = value; // pass it on
			}
		}

        public override bool MoveNext() {
            bool notDone = true;

            if (_source == null) {
                _source = (InternalRecordSource)_inputList[0];
                notDone = _source.MoveNext();
                CurrentRecord = _source.CurrentRecord;

                TotalRecordBytesEstimate = _source.TotalRecordBytesEstimate;
                TotalRecordsEstimate = _source.TotalRecordsEstimate;

				if (_displayString == null) {

					if (_source is InternalUserSource) {
						_displayString = "[UserSource]";
					}

					else {
						_displayString = "[" + _source.StorageType + ":" + _source.CurrentSourceName + "]";
					}
				}

				Console.Error.Write(_displayString + "\tBegin Read\t" + TotalRecordsEstimate + " records");

                if (_source is FlatFileMapper || _source is DirectoryMapper || _source is InternalUserSource) {
                    Console.Error.Write(" (estimate)");
                    _doEstimate = true;
                }

                Console.Error.WriteLine();

                _logInterval = TotalRecordsEstimate / _numLogs;
                if (_logInterval == 0) _logInterval = 1;

                _nextLogCount = _logInterval;
                _beginDateTime = DateTime.UtcNow;
            }

            else notDone = _source.MoveNext();
            CurrentRecord = _source.CurrentRecord;

            if (_recordCount == _nextLogCount) {

				Console.Error.Write(_displayString + "\tBegin Read\t");
                double percent = 100.0 * _recordCount / TotalRecordsEstimate;
                Console.Error.Write("{0:0.00}% done\t", percent);

                TimeSpan span = DateTime.UtcNow - _beginDateTime;
                _beginDateTime = DateTime.UtcNow;
                double recsPerSecond = _logInterval / (span.Seconds + 1); // div by 0
                Console.Error.WriteLine(recsPerSecond + " recs/sec");

                if (_doEstimate) {
                    double aveBytesPerRecord = (double)_lineBytes / (double)(_recordCount + 1); // div by 0
					TotalRecordsEstimate = (long)(((double)TotalRecordBytesEstimate) / aveBytesPerRecord);
                    long recordsLeft = TotalRecordsEstimate - _recordCount;
                    _logInterval = recordsLeft / (_numLogs - 1);

                    // only reestimate one time
                    _doEstimate = false;
                }

                _nextLogCount += _logInterval;
            }

            _recordCount++;

            if (!notDone) {
                Console.Error.WriteLine(_displayString + "\tEnd Read\t" + DateTime.Now);
            }

            if (_doEstimate && notDone) {
                _lineBytes += _source.CurrentRecord.Key.Length;
            }

            return notDone;
        }
    }

    internal class FlatFileMapper : InternalRecordSource {
        private Stream _flatFileStream = null;
        private StreamReader _flatFileReader = null;
		private DataRecord _record = new DataRecord();
        private string _pathInfo = null;

        private byte[] _nullFixed = null;
        private bool _nullInput = false;

        public override TStorageType StorageType {
            get {
                return TStorageType.FlatFile;
            }
        }

        public FlatFileMapper(string pathInfo) {
            _pathInfo = pathInfo;

            // useful for debugging
            if (pathInfo.Equals("null") || pathInfo.Equals("null0")) {
                _nullInput = true;

                if (pathInfo.IndexOf("0") >= 0) {
					_nullFixed = Guid.NewGuid().ToByteArray();
                }

				return;
            }

			else if (pathInfo.Equals("$")) {
				Stream stdin = Console.OpenStandardInput();
				_flatFileReader = new StreamReader(stdin);
				TotalRecordBytesEstimate = 0;
				TotalRecordsEstimate = 0;
				return;
			}


            try {
                _flatFileStream = ZStreamIn.OpenUnbuffered(pathInfo);
                TotalRecordBytesEstimate = _flatFileStream.Length;
                TotalRecordsEstimate = _flatFileStream.Length / 50; // wild guess
                _flatFileReader = new StreamReader(_flatFileStream);
            }
            catch {
                string message = "Cannot open file: " + _pathInfo;
                throw new Exception(message);
            }
        }

        public override bool MoveNext() {
            if (_nullInput) {
                return NullMoveNext();
            }

            string line = _flatFileReader.ReadLine();
            if (line == null) return false;

			_record = new DataRecord();

			_record.KeySpaceNormalized = line;
            CurrentRecord = _record;

            return true;
        }

		private bool NullMoveNext() {
			_record = new DataRecord();

			if (_nullFixed != null) {
				_record.KeyBytes = _nullFixed;
			}

			else {
				_record.KeyBytes = Guid.NewGuid().ToByteArray();
			}

			CurrentRecord = _record;
			return true;
		}

        public override string CurrentSourceName {
            get {
                return _pathInfo;
            }
        }

        public override long CurrentSourcePosition {
            get {
                return -1;
            }
        }

        public override void Close() {
            if (_nullInput) return;
            _flatFileReader.Close();
        }
    }

    internal class DirectoryMapper : InternalRecordSource {
        private StreamReader _flatFileReader = null;
        private DataRecord _record = new DataRecord();
        private string[] _files = null;
        private int _currentFileNo = -1;
        private string _directory = null;

        public override TStorageType StorageType {
            get {
                return TStorageType.Directory;
            }
        }

        public DirectoryMapper(string pathInfo) {
            _directory = pathInfo;

            // if path specifies a dir then no pattern given get all files
            if (Directory.Exists(pathInfo)) {
                _files = Directory.GetFiles(pathInfo);
            }

            else {
                // pathInfo has a pattern after last slash
                string directory = Path.GetDirectoryName(pathInfo);
                string pattern = Path.GetFileName(pathInfo);

                _files = Directory.GetFiles(directory, pattern);
            }

            TotalRecordBytesEstimate = 0;
            foreach (string file in _files) {
                FileInfo fi = new FileInfo(file);
                TotalRecordBytesEstimate += fi.Length;
            }

            TotalRecordsEstimate = TotalRecordBytesEstimate / 50; // wild guess
        }

        public override string CurrentSourceName {
            get {
                if (_currentFileNo < 0) return _directory;
                return _files[_currentFileNo];
            }
        }


        public override bool MoveNext() {
            string line = null;

            if (_flatFileReader != null) {
                line = _flatFileReader.ReadLine();
            }

            while (line == null && _currentFileNo < _files.Length - 1) {

                Stream stream = null;
                try {
                    stream = ZStreamIn.Open(_files[++_currentFileNo]);
                }
                catch {
                    Console.Error.WriteLine("Error opening: " + _files[_currentFileNo] + ". Skipping.");
                    continue;
                }

                if (_flatFileReader != null) {
                    _flatFileReader.Close(); // close prev
                }

                _flatFileReader = new StreamReader(stream);

                line = _flatFileReader.ReadLine();
            }

            if (line == null) return false;

            _record = new DataRecord();
			//_record.Key = line;
			_record.KeySpaceNormalized = line;
            CurrentRecord = _record;

            return true;
        }

        public override void Close() {
            _flatFileReader.Close();
        }
    }

	internal class QuerySource : InternalRecordSource {
		private string _query = null;
		private bool _prefix = false;
		private IEnumerator _tstoreEnumer = null;
		private InternalRecordSource _source = null;

		public QuerySource(string query) {

#if false
			Match m = Regex.Match(query, @"\(?<c>.)");
			if (m.Success) {
				char theChar = m.Groups["c"].ToString()[0];
				switch (theChar) {
					case '\\':
						sb.Append('\\');
						break;
					case 't':
						sb.Append('\t');
						break;
					case 'n':
						sb.Append('\n');
						break;
					case 'r':
						sb.Append('\r');
						break;
				}
			}

			int start = 0;
			while (backSlash >= 0) {
				sb.Append(query, start, backSlash);

				if (backSlash < query.Length - 1) {
					char c = query[backSlash + 1];


					

				}

			}

#endif
			Match m = Regex.Match(query, "{(?<n>[^}]+)}");
			if (m.Success) {
				string ascii = m.Groups["n"].ToString();
				string c = "" + (char)int.Parse(ascii);
				query = query.Replace("{" + ascii + "}", c);
			}

			if (query.EndsWith("*")) {
				query = query.TrimEnd("*".ToCharArray());
				_prefix = true;
			}

			_query = query;
		}

		public override bool MoveNext() {
			if (_tstoreEnumer == null) {
				InternalRecordSource source = (InternalRecordSource)Inputs[0];

				TotalRecordBytesEstimate = 0; // don' know
				TotalRecordsEstimate = 0; // don' know

				// if logging source is in the way look around it.
				if (source is LoggingSource && source.Inputs[0] is TMSNStoreRecordSource) {
					source = source.Inputs[0];
				}

				if (source is TMSNStoreRecordSource) {
					TMSNStoreReader tstoreReader = ((TMSNStoreRecordSource)source).GetReader();

					if (_prefix) {
						_tstoreEnumer = tstoreReader.GetMatchingPrefix(_query).GetEnumerator();
					}

					else {
						_tstoreEnumer = tstoreReader.GetMatch(_query).GetEnumerator();
					}
				}

				else {
					_source = Inputs[0] as InternalRecordSource;
				}
			}

			bool notDone = true;
			if (_tstoreEnumer != null) {
				notDone = _tstoreEnumer.MoveNext();
				CurrentRecord = (DataRecord)_tstoreEnumer.Current;
				return notDone;
			}

			else {
				while (notDone) {
					notDone = _source.MoveNext();
					if (!notDone) return false;

					if (_prefix && _source.CurrentRecord.Key.StartsWith(_query)) {
						CurrentRecord = _source.CurrentRecord;
						return true;
					}

					else if (_source.CurrentRecord.Key.Equals(_query)) {
						CurrentRecord = _source.CurrentRecord;
						return true;
					}
				}

				return false;
			}
		}

		public override void Close() {
		}
	}
}
