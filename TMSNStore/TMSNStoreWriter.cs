using System;
using System.Text;
using System.IO;
using Microsoft.TMSN.IO;
using System.Collections;
using System.Reflection;

namespace Microsoft.TMSN.Data {
	/// <summary>
	/// The TMSNStoreWriter writes DataRecords to disk which can be randomly accessed
	/// using the TMSNStoreReader.  The major difference between the TMSNStore and SimpleDb is
	/// that the TMSNStoreWriter doesn't provide access to records after they have been added
	/// to the db.
	/// </summary>
	public class TMSNStoreWriter {
		private string _outputDir = null;
		private string _fileSetName = null;
		private InternalRecordSource _input = null;  // either input is used
		private RecordSorterReducer _sorterReducer = null; // or the sorter reducer (uses AddRecord())
		private byte[] _prevKey = null;
		private long _numWritten = 0;

		private Stream _keyFileStream = null;
		private VariableLengthBinaryWriter _keyFileWriter = null;
		private Stream _indexFileStream = null;
		private BinaryWriter _indexFileWriter = null;

		/// <summary>
		/// The directory where temporary sort files are to be written.
		/// </summary>
		public string TempDir = Environment.GetEnvironmentVariable("TEMP");
		/// <summary>
		/// The number of records to include within a record group of a TStore.
		/// </summary>
		public uint KeyGroupSize = TMSNStoreUtils.KeyGroupSize;

		internal long TotalRecordsEstimate = 0;
		internal long TotalRecordBytesEstimate = 0;

		// used internally only by the TMSNStoreWriter itself to write its cache.
		private TMSNStoreWriter(string outputDir, bool ignoreMe) {
			_Construct(null, outputDir, "cache");
		}

		internal TMSNStoreWriter(InternalRecordSource input, string outputDir) {
			_Construct(input, outputDir, "keys-data");
		}

		/// <summary>
		/// Constructor for the push model (e.g. uses AddRecord method).
		/// </summary>
		/// <param name="outputDir"></param>
		public TMSNStoreWriter(string outputDir) {
			_Construct(null, outputDir, "keys-data");
		}

		private void _Construct(InternalRecordSource input, string outputDir, string fileSetName) {

			if (input != null && !input.Sorting.IsSorted) {
				throw new Exception("can't output non-sorted input as TMSNStore");
			}

			_input = input;
			_outputDir = outputDir;
			_fileSetName = fileSetName;

			// if directory doesn't exist make it
			if (!Directory.Exists(outputDir)) {
				Directory.CreateDirectory(outputDir);
			}

			string keyFile = Path.Combine(outputDir, _fileSetName);
			string indexFile = Path.Combine(outputDir, _fileSetName + "-index");

			// create fileSetName file
			_keyFileStream = new FileStream(keyFile, FileMode.Create, FileAccess.Write, FileShare.None, 1024 * 1024 * 16);
			_keyFileWriter = new VariableLengthBinaryWriter(_keyFileStream);

			// create index file
			_indexFileStream = new FileStream(indexFile, FileMode.Create, FileAccess.Write, FileShare.None, 1024 * 1024 * 16);
			_indexFileWriter = new BinaryWriter(_indexFileStream);
		}
		
		internal void Initialize() {
			if (TotalRecordBytesEstimate != 0) {
				_keyFileStream.SetLength(TotalRecordBytesEstimate);
			}

			if (TotalRecordsEstimate != 0) {
				long numKeyGroups = TotalRecordsEstimate / KeyGroupSize;
				if (TotalRecordsEstimate % KeyGroupSize != 0) numKeyGroups++;
				_indexFileStream.SetLength(numKeyGroups * TMSNStoreUtils.BytesPerKeyIndexOffset + 12); // add one long one uint on end
			}

		}

		internal void Finish() {
			_keyFileStream.SetLength(_keyFileStream.Position);  // needs to be resized
			_keyFileStream.Close();

			// write the total records (adjusted for dup keys)
			_indexFileWriter.Write(_numWritten);
			// write the key group size
			_indexFileWriter.Write(KeyGroupSize);
			_indexFileStream.SetLength(_indexFileStream.Position);
			_indexFileStream.Close();
		}

        /// <summary>
        /// Specifies whether the writer should reduce the data given to it or not
        /// Must be set before the first call to AddRecord()
        /// </summary>
        public bool ReductionEnabled
        {
            get { return reductionEnabled; }
            set
            {
                if (_sorterReducer != null)
                    throw new InvalidOperationException("Error: Cannot set ReductionEnabled after the first record is added to the writer");
                reductionEnabled = value;
            }
        }
        private bool reductionEnabled = false;

        /// <summary>
        /// Specifies the maximum amount of memory the TStoreWriter may use for operations
        ///  such as sorting, etc. 
        /// </summary>
        public int MaxMemorySize
        {
            get { return MaxMemorySize; }
            set
            {
                maxMemorySize = value;
                if (_sorterReducer != null)
                    _sorterReducer.MaxMemorySize = maxMemorySize;
            }
        }
        private int maxMemorySize = -1;     // -1 means leave as default





		/// <summary>
		/// Method for adding records to a TStore.  (Push model).
		/// </summary>
		/// <param name="record">Record to be added.</param>
		public void AddRecord(DataRecord record) {
			if (_sorterReducer == null) {
				if (_input != null) {
					throw new Exception("cannot use AddRecord method together with InternalRecordSource");
				}
                _sorterReducer = new RecordSorterReducer(TempDir, true, reductionEnabled);
                if (maxMemorySize != -1)
                    _sorterReducer.MaxMemorySize = maxMemorySize;
			}

			_sorterReducer.AddRecord(record);
		}

		/// <summary>
		/// Finalizes the writing of a TStore to disk.
		/// </summary>
		public void Write() {
			// we use the same disk structure to store the cache
			TMSNStoreWriter keyCacheWriter = new TMSNStoreWriter(_outputDir, true);
			uint cacheKeyGroupSize = 32; // we fix this one
			keyCacheWriter.KeyGroupSize = cacheKeyGroupSize;

			// if we are in the push mode use the sorterReducer that the records were pushed into.
			if (_input == null) {
                _input = _sorterReducer;
            }
            // Note, _input can be null if the user never pushed anything onto this writer.
            if (_input != null)
            {
                while (_input.MoveNext())
                {
                    // initialize file lengths.  We wait till after the first MoveNext so props are set
                    if (_numWritten == 0)
                    {
                        TotalRecordBytesEstimate = _input.TotalRecordBytesEstimate;
                        TotalRecordsEstimate = _input.TotalRecordsEstimate;
                        Initialize();

                        keyCacheWriter.TotalRecordsEstimate = TotalRecordsEstimate / (long)KeyGroupSize;
                        keyCacheWriter.TotalRecordBytesEstimate = TotalRecordBytesEstimate / (long)KeyGroupSize;
                        keyCacheWriter.Initialize();
                    }

                    DataRecord record = _input.CurrentRecord;

                    // write first key of group to cache
                    if (_numWritten % KeyGroupSize == 0)
                    {
                        // write the key of the first group member to the cache.
                        keyCacheWriter.AddRecord(record.KeyBytes, record.Data, false);
                    }

                    // write to main store
                    AddRecord(record.KeyBytes, record.Data, true);
                }

                if (_numWritten == 0) return;

                // if needed, pad the last group written with null entries so that
                // we don't have to special-case reading the last group.
                long unfullGroupNumMembers = _numWritten % KeyGroupSize;
                if (unfullGroupNumMembers > 0)
                {
                    uint numPadEntries = (uint)(KeyGroupSize - unfullGroupNumMembers);
                    for (int i = 0; i < numPadEntries; i++)
                    {
                        _keyFileWriter.Write((byte)0); // overlap
                        _keyFileWriter.Write((byte)0); // neu
                        _keyFileWriter.Write((byte)0); // dataLen
                    }
                }


                // take the input source and write it's properties to disk
                string recordInfoFile = Path.Combine(_outputDir, "record-info");
                using (Stream sw = new FileStream(recordInfoFile, FileMode.Create))
                {
                    _input.TotalRecordsEstimate = _numWritten;
                    _input.TotalRecordBytesEstimate = _keyFileStream.Position;
                    _input.WriteProperties(sw);
                }
            }

			Finish();
			keyCacheWriter.Finish();
            if (_sorterReducer != null) _sorterReducer.Close();
		}
	
		internal void AddRecord(byte[] keyBytes, byte[] dataBytes, bool writeData) {
			// write offsets for each group
			if (_numWritten % KeyGroupSize == 0) {
				byte[] position = BitConverter.GetBytes(_keyFileStream.Position);
				_indexFileWriter.Write(position, 0, TMSNStoreUtils.BytesPerKeyIndexOffset);
				_prevKey = null;
			}
			
			uint overlap = TMSNStoreUtils.CalculateOverlap(_prevKey, keyBytes);
			uint neu = (uint)keyBytes.Length - overlap;

			_keyFileWriter.WriteVariableLength(overlap); // how much of key in common w/prev
			_keyFileWriter.WriteVariableLength(neu);     // how much of key is new
			_keyFileWriter.Write(keyBytes, (int)overlap, (int)neu); // the key

			// not writing data is different than not having data to write.
			// if we don't have data to write we still write a dataLen of zero
			if (writeData) {
				if (dataBytes == null) {
					_keyFileWriter.WriteVariableLength((uint)0);
					// store no data
				}

				// non null data
				else {
					// store the length of the data before the data
					_keyFileWriter.WriteVariableLength((uint)dataBytes.Length);

					// store the data
					_keyFileWriter.Write(dataBytes, 0, dataBytes.Length);
				}
			}

			_prevKey = keyBytes;
			_numWritten++;
		}
	}

	internal class KeyIndex {

		// buffer for converting 5 byte longs into longs using BitConverter
		private byte[] _rawIndexOffsetPosition = new byte[8];
		private long _numRecords;
		private uint _keyGroupSize;
		private uint _numKeyGroups;
		private BinaryFileReader _keyIndexReader = null;
        private long _keysFileLength = 0;

        public KeyIndex(string filename, long keysFileLength, bool mmapFile) {
            _keyIndexReader = new BinaryFileReader(filename, mmapFile);
            long filelength = _keyIndexReader.BaseStream.Length;

            _keyIndexReader.Seek(-12, SeekOrigin.End);
            _numRecords = _keyIndexReader.ReadInt64();

            _keyGroupSize = _keyIndexReader.ReadUInt32();
            _numKeyGroups = _CalcNumKeyGroups(_numRecords, _keyGroupSize);

            _keysFileLength = keysFileLength;
        }

		~KeyIndex() {
			Close();
		}

		public void Close() {
			if (_keyIndexReader != null) _keyIndexReader.Close();
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

            _keyIndexReader.Seek(indexSeekPosition, SeekOrigin.Begin);
            byte[] index = _keyIndexReader.ReadBytes(TMSNStoreUtils.BytesPerKeyIndexOffset);

            long position = TMSNStoreUtils.LongAsFiveBytes(index, 0);

            return position;
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
