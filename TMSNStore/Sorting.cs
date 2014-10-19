using System;
using System.Text;
using System.IO;
using System.Collections.Generic;
using System.Threading;

namespace Microsoft.TMSN.Data {

	/// <summary>
	/// 
	/// </summary>
	internal class SortableRecordMemory : InternalRecordSource {
		[System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential, Pack = 8)]
		private struct RecordEntry {
			public int KeyLen;
			public int DataLen;
			public int ByteOffset;
		}

		private RecordEntry[] _recordEntries = new RecordEntry[1024];
		private int _recordBytesUsed = 0;
		private int _recordBytesSize = 1024 * 1024;
		private int _maxRecordBytes = 0;
		private byte[] _recordBytes = null;

		private bool _internalIsSorted = false;
		private bool _internalSortAscending = false;

		private MemoryStream _recordMemoryStream = null;
		private VariableLengthBinaryWriter _recordMemoryWriter = null;
		private UTF8Encoding _encoding = new UTF8Encoding(false, false);

		private int _numRecords = 0;
		private int _inputIndex = 0;
		private int _outputIndex = -1;
		private long _startTick = 0;

		/// <summary>
		/// SortAscending is a parameter of the constructor because we have to know immediately
		/// what our properties are so we can pass them on to whoever asks of us.
		/// </summary>
		public SortableRecordMemory(bool sortAscending, int maxMemorySize) {
			_recordMemoryStream = new MemoryStream();
			_recordMemoryWriter = new VariableLengthBinaryWriter(_recordMemoryStream);
			_recordBytes = new byte[_recordBytesSize];
			_maxRecordBytes = maxMemorySize;
			_internalSortAscending = sortAscending;

			// means our sorting properties are not propagated from inputs to outputs.
			_passThruInputSorting = false;

			// the sorting properties, like (segmenting, bucketing, reduced, ...) have to reflect
			// immediately what the component is going to do even before it has done it.  So even
			// before the records are sorted we set these properties because the world wants to
			// know what's true of our output.

			_sorting.IsSorted = true; // tells the world we sort.
			_sorting.IsSortedAscending = sortAscending;

			_startTick = DateTime.UtcNow.Ticks;
        }

        public double TotalSeconds {
            get {
                long time = DateTime.UtcNow.Ticks - _startTick;
                double secs = time / (double)TimeSpan.TicksPerSecond;
                return secs;
            }
        }
        

        public int NumRecords {
            get {
                return _numRecords;
            }
        }

        private void _QuickSortUp(int left, int right) {
            while (left < right && left >= 0 && right < _numRecords) {
                int num1 = left;
                int num2 = right;
                RecordEntry e1 = _recordEntries[(num1 + num2) >> 1];

            Label_001B:
                //while (e1 > entries[num1]) {
                while (_CompareKeyBytesUp(e1, _recordEntries[num1]) < 0) {
                    num1++;
                }
                //while (e1 < entries[num2]) {
                while (_CompareKeyBytesUp(e1, _recordEntries[num2]) > 0) {
                    num2--;
                }

                if (num1 <= num2) {
                    if (num1 < num2) {
                        RecordEntry e2 = _recordEntries[num1];
                        _recordEntries[num1] = _recordEntries[num2];
                        _recordEntries[num2] = e2;
                    }
                    num1++;
                    num2--;
                    if (num1 <= num2) {
                        goto Label_001B;
                    }
                }
                if ((num2 - left) <= (right - num1)) {
                    if (left < num2) {
                        _QuickSortUp(left, num2);
                    }
                    left = num1;
                }
                else {
                    if (num1 < right) {
                        _QuickSortUp(num1, right);
                    }
                    right = num2;
                }
            }
        }

        private void _QuickSortDown(int left, int right) {
            while (left < right && left >= 0 && right < _numRecords) {
                int num1 = left;
                int num2 = right;
                RecordEntry e1 = _recordEntries[(num1 + num2) >> 1];

            Label_001B:
                //while (e1 > entries[num1]) {
                while (_CompareKeyBytesDown(e1, _recordEntries[num1]) < 0) {
                    num1++;
                }
                //while (e1 < entries[num2]) {
                while (_CompareKeyBytesDown(e1, _recordEntries[num2]) > 0) {
                    num2--;
                }

                if (num1 <= num2) {
                    if (num1 < num2) {
                        RecordEntry e2 = _recordEntries[num1];
                        _recordEntries[num1] = _recordEntries[num2];
                        _recordEntries[num2] = e2;
                    }
                    num1++;
                    num2--;
                    if (num1 <= num2) {
                        goto Label_001B;
                    }
                }
                if ((num2 - left) <= (right - num1)) {
                    if (left < num2) {
                        _QuickSortDown(left, num2);
                    }
                    left = num1;
                }
                else {
                    if (num1 < right) {
                        _QuickSortDown(num1, right);
                    }
                    right = num2;
                }
            }
        }

        private unsafe int _CompareKeyBytesUp(RecordEntry entry1, RecordEntry entry2) {
            // see if first bytes differ so we can avoid all the set up.
            fixed (byte* keyp = _recordBytes) {
                byte* b1 = keyp + entry1.ByteOffset;
                byte* b2 = keyp + entry2.ByteOffset;

                int key1Shorter = entry1.KeyLen.CompareTo(entry2.KeyLen);
                int len = ((key1Shorter < 0) ? entry1.KeyLen : entry2.KeyLen);

                for (int i = 0; i < len; b1++, b2++, i++) {
                    int diff = *b2 - *b1;
                    // when either string reaches it's zero
                    // (and the other one isn't at zero which is caught above)
                    // the difference must be non-zero so we break
                    if (diff != 0) return diff;
                }

                // at this point strings are equal for the length that overlaps
                return key1Shorter * -1;
            }
        }

        private unsafe int _CompareKeyBytesDown(RecordEntry entry1, RecordEntry entry2) {
            // see if first bytes differ so we can avoid all the set up.
            fixed (byte* keyp = _recordBytes) {
                byte* b1 = keyp + entry1.ByteOffset;
                byte* b2 = keyp + entry2.ByteOffset;

                int key1Shorter = entry1.KeyLen.CompareTo(entry2.KeyLen);
                int len = ((key1Shorter < 0) ? entry1.KeyLen : entry2.KeyLen);

                for (int i = 0; i < len; b1++, b2++, i++) {
                    int diff = *b1 - *b2;
                    // when either string reaches it's zero
                    // (and the other one isn't at zero which is caught above)
                    // the difference must be non-zero so we break
                    if (diff != 0) return diff;
                }

                // at this point strings are equal for the length that overlaps
                return key1Shorter;
            }
        }

        public void Sort() {
            // return if we don't need to sort
            if (_internalIsSorted) return;

            if (_internalSortAscending) {
                _QuickSortUp(0, (int)_numRecords - 1);
            }

            else {
                _QuickSortDown(0, (int)_numRecords - 1);
            }

            _internalIsSorted = true;
        }

        public bool AddRecord(DataRecord record) {

            // special case this InternalRecordSource since we don't add a source to it.
            if (_recordInfo.RecordType == null) {
                _recordInfo.Update(record);
            }

            int dataLen = 0;

            byte[] data = record.Data;

            if (data != null) dataLen = data.Length;

            long recordSize = record.KeyBytes.Length + dataLen;

            // if buffer too small double size
            if (recordSize > (_recordBytesSize - _recordBytesUsed)) {

                // if we've reach max size we can't grow anymore.  Return failure
                if (_recordBytesSize >= _maxRecordBytes) return false;

                // double memory upto max size
                long tempSize = (long)_recordBytesSize + (long)_recordBytesSize;
                tempSize = Math.Min(tempSize, (long)_maxRecordBytes); // maxRecordBytes << 2GB (BlockCopy doesnt work with long)
                _recordBytesSize = (int)tempSize;

                // if still not big enuf - fail.
                if (recordSize > (_recordBytesSize - _recordBytesUsed)) return false;

                // else get the memory.
                byte[] oldRecordBytes = _recordBytes;
                _recordBytes = new byte[_recordBytesSize];
                Buffer.BlockCopy(oldRecordBytes, 0, _recordBytes, 0, oldRecordBytes.Length);
            }

            int recordOffset = _recordBytesUsed;

            // copy in the key
            Buffer.BlockCopy(record.KeyBytes, 0, _recordBytes, _recordBytesUsed, record.KeyBytes.Length);
            _recordBytesUsed += record.KeyBytes.Length;

            // copy in the data
            if (data != null) {
                Buffer.BlockCopy(data, 0, _recordBytes, _recordBytesUsed, dataLen);
                _recordBytesUsed += dataLen;
            }

            // if we're out of entries double it.
            if (_inputIndex >= _recordEntries.Length) {
                RecordEntry[] oldEntries = _recordEntries;
                _recordEntries = new RecordEntry[_recordEntries.Length + _recordEntries.Length];

                for (int i = 0; i < oldEntries.Length; i++) {
                    _recordEntries[i].ByteOffset = oldEntries[i].ByteOffset;
                    _recordEntries[i].KeyLen = oldEntries[i].KeyLen;
                    _recordEntries[i].DataLen = oldEntries[i].DataLen;
                }
            }

            _recordEntries[_inputIndex].ByteOffset = recordOffset;
            _recordEntries[_inputIndex].KeyLen = record.KeyBytes.Length;
            _recordEntries[_inputIndex].DataLen = dataLen;

            _inputIndex++;
            _numRecords++;

            TotalRecordBytesEstimate += recordSize;
            TotalRecordsEstimate++;

            return true;
        }

        public override void Close() {
        }

		public void Clear() {
			_numRecords = 0;
			_recordBytesUsed = 0;
			_inputIndex = 0;
			_outputIndex = -1;
			TotalRecordBytesEstimate = 0;
			TotalRecordsEstimate = 0;
			_internalIsSorted = false;
		}

        // we act like a fifo, first in first out.
        public override bool MoveNext() {
            _outputIndex++; // advance the index to the next record to output

            if (_outputIndex < _inputIndex) {
                DataRecord outputRecord = ConstructInstance();

                int byteOffset = _recordEntries[_outputIndex].ByteOffset;

                byte[] keyBytes = new byte[_recordEntries[_outputIndex].KeyLen];
                Buffer.BlockCopy(_recordBytes, byteOffset, keyBytes, 0, keyBytes.Length);
                outputRecord.KeyBytes = keyBytes;
                byteOffset += keyBytes.Length;

                if (_recordEntries[_outputIndex].DataLen != 0) {
                    byte[] data = new byte[_recordEntries[_outputIndex].DataLen];
                    Buffer.BlockCopy(_recordBytes, byteOffset, data, 0, data.Length);
                    outputRecord.Data = data;
                }

                _numRecords--;
                // if this is the last record we reset the input position
                if (_numRecords == 0) {
                    _inputIndex = 0;
                    _outputIndex = -1;
                    _recordBytesUsed = 0;
                }

                CurrentRecord = outputRecord;
                return true;
            }

            return false;
        }
    }

	/// <summary>
	/// 
	/// </summary>
	internal class RecordSorterReducer : InternalRecordSource {
		private string _tempDir = null;
		private int _numTempFiles = 0;
		private string _guid = Guid.NewGuid().ToString(); // for temp files
		private SortedRecordMerger _merger = null;
		private int _sortableRecordMaxMemorySize = 1024 * 1024 * 512; // 512MB
		private InternalRecordSource _output = null;
		private bool _internalSortAscending = true;
		private bool _internalReductionEnabled = false;
		private bool _reductionDetermined = false;

		private SortableRecordMemory _recordsToSort = null;
		private SortableRecordMemory _recordsToWrite = null;
		private SortableRecordMemory _sortedRecords = null;

		private List<Thread> _threads = new List<Thread>();

		private bool _doThreading = true;

		private bool _deleteTempFiles = true;

		public RecordSorterReducer(string tempDir, bool sortAscending, bool reductionEnabled) {
			_tempDir = tempDir;

			// so that we don't take on our input's properties
			_passThruInputSorting = false;
			_passThruInputReduced = false;
			_sorting.IsSorted = true;
			_sorting.IsSortedAscending = sortAscending;
			_internalSortAscending = sortAscending;
			_isReduced = reductionEnabled;
			_internalReductionEnabled = reductionEnabled;

			ProcessTreeComment = "[ascending:" + _sorting.IsSortedAscending + " reduction:" + _internalReductionEnabled + "]";
		}

		public void AddRecord(DataRecord record) {
			if (_recordsToSort == null) {
				_recordsToSort = new SortableRecordMemory(_internalSortAscending, _sortableRecordMaxMemorySize);
			}

			// attempt to add the new record
			bool success = _recordsToSort.AddRecord(record);

            // see if we need to reduce by setting the _filter or leaving it null
            if (!_reductionDetermined)
            {
                if (!(record is IReducableRecord))
                {
                    _internalReductionEnabled = false;
                }
                _reductionDetermined = true;
            }

            //if (inputInstance == null) inputInstance = input.CurrentRecord;

            // rats! not enough memory.  Sort and write temp file, then add record to new sorter
            if (!success)
            {
                if (_doThreading) _SortAndWriteThreaded();
                else _SortAndWrite();

                // try again
                AddRecord(record);
            }
		}

		private void _SortAndWrite() {
			_recordsToSort.Sort();
			_recordsToWrite = _recordsToSort;
			_WriteSortedRecords();
		}

		private void _SortAndWriteThreaded() {
			// threading model looks like this:
			// MainThread: | Ri | Rj |        | Rk |          | Rl
			// SortThread:      | Si          | Sj            | Sk
			// WriteThread:                   | Wi     |      | Wj

			// wait on all previous threads, sorts and writes
			foreach (Thread t in _threads) {
				t.Join();
			}
			_threads.Clear();

			SortableRecordMemory writtenRecords = _recordsToWrite;
			
			_recordsToWrite = _sortedRecords;

			_sortedRecords = _recordsToSort;

			ThreadStart sortJob = new ThreadStart(_sortedRecords.Sort);
			Thread tt = new Thread(sortJob);
			tt.Start();
			_threads.Add(tt);


			// write sorted records
			if (_recordsToWrite != null) {
				ThreadStart writeJob = new ThreadStart(_WriteSortedRecords);
				tt = new Thread(writeJob);
				tt.Start();
				_threads.Add(tt);
			}

			_recordsToSort = writtenRecords;
			if (_recordsToSort != null) _recordsToSort.Clear();
		}

		public bool ReductionEnabled {
			set {
				_internalReductionEnabled = value;
				ProcessTreeComment = "[ascending:" + _sorting.IsSortedAscending + " reduction:" + _internalReductionEnabled + "]";
			}

			get {
				return _internalReductionEnabled;
			}
		}

		public long MaxMemorySize {
			set {
				_sortableRecordMaxMemorySize = (int)value;
			}
		}

		public bool DeleteTempFiles {
			get {
				return _deleteTempFiles;
			}
			set {
				_deleteTempFiles = value;
			}
		}

		public override void Close() {
			if (_merger != null) _merger.Close();

            foreach (Thread t in _threads) {
                t.Abort();
            }

            // wait on threads to be die
            foreach (Thread t in _threads) {
                t.Join();
            }

            // delete our temp files
            _DeleteTempFiles();

            base.Close();
		}

		private void _DeleteTempFiles() {
			for (int i = 0; _deleteTempFiles && i < _numTempFiles; i++) {
				File.Delete(GetTempFilename(i));
			}
		}

        private string GetTempFilename(int num)
        {
            return Path.Combine(_tempDir, _guid + "." + num.ToString());
        }


		public override bool MoveNext() {
			bool notDone = true;

			// first time thru
			if (_output == null) {

				// this means we are in streaming mode.  Sort all the inputs
				if (Inputs.Length != 0) {
					Console.Error.WriteLine("[Sort Begin]");
					foreach (InternalRecordSource source in Inputs) {
						while (source.MoveNext()) {
							AddRecord(source.CurrentRecord);
						}
					}
				}

				_Finish();

				if (Inputs.Length != 0) {
					Console.Error.WriteLine("[Sort End]\t{0} records, {1} bytes, {2} merge files", _output.TotalRecordsEstimate, _output.TotalRecordBytesEstimate, _numTempFiles);
				}
			}

			notDone = _output.MoveNext();

			// if we throw an exception here then we need to clean up the temp files
			//catch (Exception ex) {
			//	Close();
			//	throw ex;
			//}

			CurrentRecord = _output.CurrentRecord;
			return notDone;
		}

		private void _Finish() {
			if (_doThreading) {
				_SortAndWriteThreaded();
				// wait on all previous threads, sorts and writes
				foreach (Thread t in _threads) {
					t.Join();
				}
				_threads.Clear();
			}

			else {
				// don't write the last one to disk
				_recordsToSort.Sort();
				_sortedRecords = _recordsToSort;
			}

			int numToMerge = _numTempFiles;

			if (_sortedRecords != null && _sortedRecords.NumRecords != 0) {
				numToMerge++;
			}

			if (numToMerge > 1) {
				_merger = new SortedRecordMerger();

				if (_sortedRecords != null && _sortedRecords.NumRecords != 0) {
					_merger.AddInput(_sortedRecords);
				}

				for (int i = 0; i < _numTempFiles; i++) {
                    InternalRecordSource source = new InternalRecordFileReader(GetTempFilename(i));
					_merger.AddInput(source);
				}

				_output = _merger;
			}

			else {
				_output = _sortedRecords;
			}

			// set up reduction filter for reduction across merged sources
			if (_internalReductionEnabled) {
				ReduceFilter reducer = new ReduceFilter();
				RecordFilterDriver filterDriver = new RecordFilterDriver(reducer);
				filterDriver.AddInput(_output);
				_output = filterDriver;
			}

			// this is kind of a hack till i figure out how these should be set
			TotalRecordBytesEstimate = _output.TotalRecordBytesEstimate;
			TotalRecordsEstimate = _output.TotalRecordsEstimate;
		}

		// write this chunk to disk.  This is where reduction happens if it happens.
		private void _WriteSortedRecords() {
			if (!Directory.Exists(_tempDir)) {
				Directory.CreateDirectory(_tempDir);
			}

			InternalRecordSource output = _recordsToWrite;

			// set up reduction filter.  The whole reason we have a sorterReducer
			// and not just a sorter is so that we can reduce before we write our
			// temp files to disk.
			if (_internalReductionEnabled) {
				ReduceFilter rf = new ReduceFilter();
				RecordFilterDriver filterDriver = new RecordFilterDriver(rf);
				filterDriver.AddInput(output);
				output = filterDriver;
			}

			RecordFileWriter recordFileWriter = new RecordFileWriter(GetTempFilename(_numTempFiles));
            _numTempFiles++;
            try {
                recordFileWriter.Write(output);
            }

            finally {
                _recordsToWrite.Close();
            }
		}
	}
}
