using System;
using System.Text;
using System.Collections;
using System.IO;
using System.Reflection;
using Microsoft.TMSN.IO;

namespace Microsoft.TMSN.Data {

#if false
	//
	// Record processing component interfaces/enums
	//

	/// <summary>
	/// 
	/// </summary>
	internal class RecordAggregator : InternalRecordSource {
		private bool _reductionEnabled = true;
		private string _tempDir = null;

		private InternalRecordSource _output = null;
		
		public RecordAggregator(string tempDir) {
			_tempDir = tempDir;
		}

		private void _Initialize() {

			// assume theyre either all sorted or all not.  Also, some of these might
			// be empty assuming there were empty buckets so handle that.
			
			InternalRecordSource mergeDevice = null;

			if (Sorting.IsSorted) {
				mergeDevice = new SortedRecordMerger();
			}

			else {
				// is it right to hard code sortAscending!?!?
				mergeDevice = new RecordSorterReducer(_tempDir, true, _reductionEnabled);
			}

			// add the sources
			foreach (InternalRecordSource source in Inputs) {
				mergeDevice.AddInput(source);
			}

			_output = mergeDevice;
		}

		public override void Close() {
			foreach (InternalRecordSource source in Inputs) {
				source.Close();
			}
			_output.Close();
		}

		public override bool MoveNext() {
			// set up the input the first time.  We have to postpone
			// these things because properties get set AFTER the constructor
			if (_output == null) {
				_Initialize();
			}

			return _output.MoveNext();
		}

		public bool ReductionEnabled {
			get {
				return _reductionEnabled;
			}
			set {
				_reductionEnabled = value;
			}
		}
	}
#endif

	internal class RecordOutputter {
		private TStoreUri _outputUri = null;
		//private RawRecordWrapper _rawRecordWrapper = new RawRecordWrapper();
		//private RecordFileWriter _recordFileWriter = null;
		private StreamWriter _flatFileWriter = null;
		private InternalRecordSource _output = null;
		public uint TStoreGroupSize = 32;
		public bool SuppressTableHeaders = true;

		public RecordOutputter(TStoreUri outputUri) {
			_outputUri = outputUri;
		}

		public void SetInput(InternalRecordSource input) {
			_output = input;
		}

#if COSMOS
		public bool WriteRecordsMaxSize(long maxFileSize, string filePath) {
			// if we have a final filter insert it.  Here we're assuming that on
			// subsequent calls of this method that the _input is waiting at the
			// place we left it.

			RecordFileWriter recordFileWriter = new RecordFileWriter(filePath);
			return recordFileWriter.WriteRecordFileMaxSize(_output, maxFileSize);
		}
#endif

		public void Write() {
			TStorageType outputType = TStorageType.FlatFile;
			
			if (_outputUri == null) {
				throw new Exception("must use constructor with OutputUri parameter when using this method");
			}

			outputType = _outputUri.StorageType;


            Console.Error.Write("[" + outputType + ":" + _outputUri.FilePath + "]\tBegin Write\t");
            Console.Error.WriteLine(DateTime.Now);
            Console.Error.Flush();
			
			switch (outputType) {
				case TStorageType.TStore:
					TMSNStoreWriter twriter = new TMSNStoreWriter(_output, _outputUri.FilePath);
					twriter.KeyGroupSize = TStoreGroupSize;
					twriter.Write();
					break;

				case TStorageType.FlatFile:
					_WriteFlatFile(_outputUri.FilePath);
					break;

				case TStorageType.RecordFile:
					RecordFileWriter writer = new RecordFileWriter(_outputUri.FilePath);
					writer.Write(_output);
					break;
			}

			_output.Close();

            Console.Error.Write("[" + outputType + ":" + _outputUri.FilePath + "]\tEnd Write\t");
			Console.Error.WriteLine(DateTime.Now);
            Console.Error.Flush();
		}

		// we output TableRecords in a special way.  First we output the column names
		// then we output only the data.  The key is one of the columns in the data.
		private void _OutputTable(InternalRecordSource output, StreamWriter writer) {
			TableRecord record = _output.CurrentRecord as TableRecord;
			char separator = record.TableColumnSeparator;

			if (!SuppressTableHeaders) {
				for (int i = 0; i < record.ColumnNames.Length; i++) {
					if (i != 0) _flatFileWriter.Write(separator);
					_flatFileWriter.Write(record.ColumnNames[i]);

					// mark the keys with =Key.<keyColumnNo>
					for (int j = 0; j < record.KeyColumnNos.Length; j++) {
						if (record.KeyColumnNos[j] == i) {
							_flatFileWriter.Write("=Key.");
							_flatFileWriter.Write(j + 1);
						}
					}
				}

				_flatFileWriter.WriteLine();
			}

			do {
				_flatFileWriter.WriteLine(_output.CurrentRecord.DataAsString);
			} while (_output.MoveNext());
		}

		private void _WriteFlatFile(string filePath) {
			Stream stream = null;

            #region NULL_OUTPUT
            if (filePath.Equals(@"null")) {
				long time = DateTime.UtcNow.Ticks;
				long recordCount = 0;
				while(_output.MoveNext()) {
					recordCount++;
                    if (recordCount % 4000000 == 0) {
						double t = (DateTime.UtcNow.Ticks - time) / (double)TimeSpan.TicksPerSecond;
						Console.Error.WriteLine("MoveNext: " + t.ToString("0.00").PadLeft(6) + " sec " +
							"[" + (t * 1000 / (recordCount / 1000.0)).ToString("0.00").PadLeft(6) +
							" ms / Krec, " + (recordCount / t).ToString("0.0").PadLeft(9) + " records/sec]");
						time = DateTime.UtcNow.Ticks;
						recordCount = 0;
					}
				}
                time = DateTime.UtcNow.Ticks - time;
                Console.Error.WriteLine("MoveNext: " + (time / (double)TimeSpan.TicksPerSecond).ToString("0.000") + " sec");
                return;
            }
            #endregion

			if (filePath.Equals("$")) {
				stream = Console.OpenStandardOutput();
			} else {
				//Stream stream = ZStreamOut.Open(filePath); slow
				stream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 1024 * 1024 * 16);
			}

			string data = null;
			bool notDone = _output.MoveNext();
			if (!notDone) return;
			
			_flatFileWriter = new StreamWriter(stream);
			_flatFileWriter.AutoFlush = true;

			// special case for tables.
			if (_output.CurrentRecord is TableRecord) {
				_OutputTable(_output, _flatFileWriter);
			}

			else do {
					_flatFileWriter.Write(_output.CurrentRecord.Key);     // write the string
					data = _output.CurrentRecord.DataAsString;
					if (data != null && data.Length > 0) {
						_flatFileWriter.Write('\t');                     // tab separation
						_flatFileWriter.WriteLine(data);
					}

					else {
						_flatFileWriter.WriteLine();
					}

				} while (_output.MoveNext());

			_flatFileWriter.Close();
		}
	}

	/// <summary>
	/// Interface for mapping utf8 strings to a uint hash.
	/// </summary>
	public interface IUtf8StringHasher {
		/// <summary>
		/// Maps a utf8 string to a uint hash.
		/// </summary>
		/// <param name="utf8String">utf8 string to be hashed.</param>
		/// <param name="strlen">length of utf8 string.</param>
		/// <returns>hash value.</returns>
		uint GetHashCode(byte[] utf8String, int strlen);
	}

	/// <summary>
	/// Maps utf8 strings to a uint hash.
	/// </summary>
	public class TMSNStoreHasher : IUtf8StringHasher {
		private byte[] _primes = {61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
									 61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1};

		/// <summary>
		/// Maps a utf8 string to a uint hash.
		/// </summary>
		/// <param name="utf8String">Utf8 string to be hashed.</param>
		/// <param name="strlen">length of utf8 string.</param>
		/// <returns>hash value.</returns>
		public virtual uint GetHashCode(byte[] utf8String, int strlen) {
			int len = Math.Min(strlen, _primes.Length);
			len = Math.Min(len, utf8String.Length);

			uint hashVal = 0;
			for (int i = 0; i < len; i++) {
				byte b = utf8String[i];
				if (b == 0xFF) return hashVal;
				hashVal += (uint)(b * _primes[i]);
				hashVal *= 5;
			}

			return hashVal;
		}
	}
	
#if false
	public class RecordBucketter {
		private byte[][] _bucketBoundaryBeginBytes = null;
		private IRecordOutputable _input = null;
		//private int _currentBoundaryNo = 0;
		private IUtf8StringHasher _hasher = null;
		private bool _isSorted = false;
		private RecordFileWriter[] _recordFileWriters = null;
		private string[] _recordFilesOut = null;
		private IDataRecord _recordInstance = null;
		private RawRecordCreator _rawRecordCreator = new RawRecordCreator();

		public RecordBucketter(string[] recordFilesOut, IDataRecord recordInstance) {
			_recordFilesOut = recordFilesOut;
			_recordInstance = recordInstance;
		}

		public IRecordOutputable Input {
			set {
				_input = value;
				_isSorted = value.IsSorted;
			}
		}

		public IUtf8StringHasher CustomBucketHasher {
			set {
				_hasher = value;
			}
		}

		public void SetBucketBoundaryBeginStrings(string[] bucketBoundaryBeginStrings) {
			if (_recordFileWriters.Length != bucketBoundaryBeginStrings.Length + 1) {
				throw new Exception("Improper MapOutputter initialization");
			}

			UTF8Encoding encoding = new UTF8Encoding(false, false);

			// even tho we don't specify the beginning of the first bucket, allocate
			// space so that it's easier to increment thru.
			_bucketBoundaryBeginBytes = new byte[bucketBoundaryBeginStrings.Length + 1][];
			_bucketBoundaryBeginBytes[0] = encoding.GetBytes(""); // everything after this
			for (int i = 0; i < bucketBoundaryBeginStrings.Length; i++) {
				_bucketBoundaryBeginBytes[i+1] = encoding.GetBytes(bucketBoundaryBeginStrings[i]);
			}
		}

#if false
		public void AddRawRecord(byte[] rawRecord) {
			if (_recordFileWriters == null) {
				_recordFileWriters = new RecordFileWriter[_recordFilesOut.Length];

				for (int i = 0; i < _recordFilesOut.Length; i++) {
					_recordFileWriters[i] = new RecordFileWriter(_recordFilesOut[i], _recordInstance);
					_recordFileWriters[i].AddedRecordsInSortedOrder = _isSorted;
				}

				// we could have been set by the customHasher property otherwise...
				if (_hasher == null) {
					_hasher = new TMSNStoreHasher();
				}
			}

			// if we're bucketting based on begin strings
			if (_bucketBoundaryBeginBytes != null) {
				while ((_currentBoundaryNo != _bucketBoundaryBeginBytes.Length - 1) &&
					(TMSNStoreUtils.Utf8BytesCompare(_bucketBoundaryBeginBytes[_currentBoundaryNo+1], rawRecord) >= 0)) {
					_currentBoundaryNo++;
				}

				//_recordFileWriters.
				_recordFileWriters[_currentBoundaryNo].AddRawRecord(rawRecord);
			}

			// else if we only have one bucket
			else if (_recordFileWriters.Length == 1) {
				_recordFileWriters[0].AddRawRecord(rawRecord);
			}

			// else we have multiple buckets and we bucket based on a hash of the key
			else {
				// the default hasher respects null termination.
				uint bucketNo = _hasher.GetHashCode(rawRecord, rawRecord.Length) % (uint)_recordFileWriters.Length;
				_recordFileWriters[bucketNo].AddRawRecord(rawRecord);
			}
		}

		public void DoneAdding() {
			for (int i = 0; i < _recordFileWriters.Length; i++) {
				_recordFileWriters[i].DoneAdding();
			}
		}
#endif

		public void Write() {
			if (_input == null) {
				throw new Exception("only for use with set input property");
			}

			// we could have been set by the customHasher property otherwise...
			if (_hasher == null) {
				_hasher = new TMSNStoreHasher();
			}

			// if we have a gettable then iterate through them otherwise our Add method was used
			while (_input.MoveNext()) {
				//foreach (byte[] rawRecord in _recordFilter) {
				byte[] rawRecord = _input.CurrentRawRecord;

				// increment the currentBucketNo until the key is >= the corresponding bucket
				// beginString or until we reach the last bucket

				//AddRawRecord(rawRecord);
			}
		}
	}
#endif
}

