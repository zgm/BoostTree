using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Microsoft.TMSN.Data
{
	// largely stolen from FilterLeftByRightKey
	/// <summary>
	/// Class performs inner join based on record keys of input sources.
	/// </summary>
	internal class InnerJoinOld : PairFilter {
		private InternalRecordSource _leftSource = null;
		private InternalRecordSource _rightSource = null;
		//private DataRecord _currentLeftRecord = null;
		//private DataRecord _currentRightRecord = null;
		private char _tableColumnSeparator = '\t';

		private bool _leftNotDone = true;
		private bool _rightNotDone = true;

		private RecordJoiner _joiner = null;

		/// <summary>
		/// Inner Join Constructor
		/// </summary>
		/// <param name="tableColumnSeparator">Character for column delimiting.</param>
		public InnerJoinOld(char tableColumnSeparator) {
			_tableColumnSeparator = tableColumnSeparator;
		}

		/// <summary>
		/// Specifies that the key order of the input streams is preserved.
		/// </summary>
		public override bool KeyOrderIsPreserved {
			get {
				return true;
			}
		}

		/// <summary>
		/// Advances Iteration.
		/// </summary>
		/// <returns>False if at end of iteration, true otherwise.</returns>
		public override bool MoveNext() {
			if (_leftSource == null) {
				_leftSource = (InternalRecordSource)_inputList[0];
				_rightSource = (InternalRecordSource)_inputList[1];

				_rightNotDone = _rightSource.MoveNext();
				if (!_rightNotDone) return false;
				//_currentRightRecord = _rightSource.CurrentRecord;
			}

			while (true) {
                // advance the left side first
				_leftNotDone = _leftSource.MoveNext();
				if (!_leftNotDone) return false;

				// advance the right side
				int diff = -1;
				bool firstTime = true;
				while (diff < 0) {

					// the first time we test we check against the currentRightSource since
					// we allow dups on the left side.
					if (!firstTime && _rightNotDone) {
                        _rightSource.MoveNextHint = _leftSource.CurrentRecord.Key;
						_rightNotDone = _rightSource.MoveNext();
						if (!_rightNotDone) return false;
					}

					diff = TMSNStoreUtils.Utf8BytesCompare(_leftSource.CurrentRecord.KeyBytes, _rightSource.CurrentRecord.KeyBytes);
					firstTime = false;
				}

				// if there's a match, join the right to the left
				if (diff == 0) {
					if (_joiner == null) {
						_joiner = new RecordJoiner(_leftSource, _rightSource, _tableColumnSeparator);
					}

					CurrentRecord = _joiner.Join(_leftSource.CurrentRecord, _rightSource.CurrentRecord, true);
					return true;
				}
			}
		}
	}

    // largely stolen from FilterLeftByRightKey
    /// <summary>
    /// Class performs inner join based on record keys of input sources.
    /// </summary>
    internal class InnerJoin : PairFilter
    {
        private BookmarkableSource _leftSource = null;
        private LookAheadWrapper _rightSource = null;
        private char _tableColumnSeparator = '\t';
        private bool _advanceLeft = true;
        private bool _advanceRight = true;
        private bool _compareRequired = true;

        private RecordJoiner _joiner = null;

        /// <summary>
        /// Inner Join Constructor
        /// </summary>
        /// <param name="tableColumnSeparator">Character for column delimiting.</param>
        public InnerJoin(char tableColumnSeparator) {
            _tableColumnSeparator = tableColumnSeparator;
        }

        /// <summary>
        /// Specifies that the key order of the input streams is preserved.
        /// </summary>
        public override bool KeyOrderIsPreserved {
            get {
                return true;
            }
        }

        /// <summary>
        /// Advances Iteration.
        /// </summary>
        /// <returns>False if at end of iteration, true otherwise.</returns>
        public override bool MoveNext() {
            if (_leftSource == null) {
                _leftSource = new BookmarkableSource();
                _rightSource = new LookAheadWrapper();

                _leftSource.AddInput(Inputs[0] as InternalRecordSource);
                _rightSource.AddInput(Inputs[1] as InternalRecordSource);
            }

            int diff = 0; // if we skip compare then diff = 0 for equality

            // the top of this loop is always ready to compare the current records
            while (true) {
                if (_advanceLeft && !_leftSource.MoveNext()) return false;
                if (_advanceRight && !_rightSource.MoveNext()) return false;

                if (_compareRequired) {
                    diff = TMSNStoreUtils.Utf8BytesCompare(_leftSource.CurrentRecord.KeyBytes, _rightSource.CurrentRecord.KeyBytes);
                }

                #region MATCH_CODE
                // if there's a match, join the right to the left
                if (diff == 0) {
                    if (_joiner == null) {
                        _joiner = new RecordJoiner(_leftSource, _rightSource, _tableColumnSeparator);
                    }

                    CurrentRecord = _joiner.Join(_leftSource.CurrentRecord, _rightSource.CurrentRecord, true);
                    //                          LeftNextEqual   RightNextEqual
                    // A    1       B   2
                    // M    3       M   4   ==> F               F
                    // X    4       Y   1

                    // A    1       B   2
                    // M    3       M   41  ==> F               T    (advance right)
                    // X    4       M   2   ==> F               F
                    // Z    1       Y   1

                    // A    1       B   2
                    // M    3       M   41  ==> T               F    (advance left)
                    // M    4       N   2   ==> F               F
                    // Z    1       Y   1

                    // A    1       B   2
                    // M    3       M   41  ==> T               T     (set left bookmark, advance left)
                    // M    4       M   2   ==> F               T     (goto left bookmark, advance left, advance right)
                    // Z    1       Y   1   ==> T               F     (advance left)
                    //                      ==> F               F     (advance left, advance right)

                    // figure out what to advance
                    // LNE  RNE    =>   AdvanceLeft AdvanceRight    Note
                    // F    F           T           T               if (bookmarkExists) ClearBookmark();
                    // F    T           X           T               if (bookmarkExists) {gotoBookmark, X=true} else X=false
                    // T    F           T           F
                    // T    T           T           F               set left bookmark


                    // LNE == T
                    if (_leftSource.NextRecordEqualsCurrent) {
                        _advanceLeft = true;
                        _advanceRight = false;
                        _compareRequired = false; // no need to compare

                        // LNE == T, RNE == T
                        if (_rightSource.NextRecordEqualsCurrent) {
                            _leftSource.SetBookmark();
                        }
                    }

                    // LNE == F, RNE == T
                    else if (_rightSource.NextRecordEqualsCurrent) {
                        _advanceRight = true;
                        _compareRequired = false; // no need to compare

                        if (_leftSource.BookmarkExists) {
                            _leftSource.GoToBookmark();
                            _advanceLeft = true;
                        }

                        else _advanceLeft = false;
                    }

                    // LNE == F, RNE == F
                    else {
                        _leftSource.ClearBookmark();
                        _advanceLeft = true;
                        _advanceRight = true;
                        _compareRequired = true;
                    }

                    return true;
                }
                #endregion

                else if (diff > 0) {
                    _advanceLeft = true;
                    _advanceRight = false;
                    _leftSource.MoveNextHint = _rightSource.CurrentRecord.Key;
                }

                else {
                    _advanceLeft = false;
                    _advanceRight = true;
                    _rightSource.MoveNextHint = _leftSource.CurrentRecord.Key;
                }
            }
        }
    }

	internal class ToCountFilter : SortHintingFilter {
		private char _separator = '\t';
		private string _countExpression = null;
		private int _countColumnNo = -2;
		private int[] _keyColumnNos = null;
		private string _sourceName = null;

		private ColumnWrapper _wrapper = null;
		private int _caseNum = 0;
		private StringBuilder _stringBuilder = new StringBuilder();
		private bool _interpretAsZero = false;
		private int _numParseErrors = 0;

		//private CountRecord _outRecord = new CountRecord();

		public ToCountFilter(string keyExpression, string countExpression, char columnSeparator, string sourceName)
			:
			base(keyExpression) {
			// undocumented feature: if keyExpression ends in bang it means that
			// the user guarentees (believes really hard) that KeyOrderIsPreserved==true.
			// This this makes it possible to avoid sorts in certain situations.

			_countExpression = countExpression;
			_separator = columnSeparator;
			_sourceName = sourceName;
		}

		public override void ProcessRecord(DataRecord record, RecordAccepter accepter) {
			if (_wrapper == null) {
				bool hasHeaders = false;

				HintMessageToConsole();

				if (record is TableRecord) {
					_wrapper = new ColumnWrapper(record as TableRecord);
					_keyColumnNos = _wrapper.GetColumnNos(KeyExpression);
				}

				else {
					hasHeaders = ColumnWrapper.HasHeaders(KeyExpression);
					_wrapper = new ColumnWrapper(record, _sourceName, _separator, hasHeaders);
					_keyColumnNos = _wrapper.GetColumnNos(KeyExpression);
				}

				// if the countColumnExpression ends in a bang! we allow ulong.Parse errors.
				// otherwise we abort on an error.

				if (_countExpression != null && _countExpression.EndsWith("!")) {
					_interpretAsZero = true;
					_countExpression = _countExpression.TrimEnd('!');
				}

				if (_countExpression != null) {
					int[] countColumnNos = _wrapper.GetColumnNos(_countExpression);
					if (countColumnNos.Length != 1) {
						Console.WriteLine("Illegal Count Column expression");
						Environment.Exit(1);
					}

					_countColumnNo = countColumnNos[0];
					if (_countColumnNo > _wrapper.ColumnNames.Length - 1) {
						Console.WriteLine("Illegal Count Column expression");
						Environment.Exit(1);
					}
				}

				// if countRecord and count column is last column.
				if (record is CountRecord && _countColumnNo == _wrapper.ColumnNames.Length - 1) {
					_caseNum = 0;
				}

				// if no expression given use 1
				else if (_countExpression == null) {
					_caseNum = 1;
				}

				else _caseNum = 2;

				if (hasHeaders) return; // eat up this record containing headers
			}

			// cases:
			// 0 : record = CountRecord && countColumn refers to the count.
			// 1 : countExpression == null.  Just 1-count the keys
			// 2 : everything else

			// not sure if this is the best way to ignore blank lines coming in.
			if (record.Key.Length == 0) return;

			CountRecord outRecord = new CountRecord();
			_wrapper.SetRecord(record);

			// build the key
			_stringBuilder.Length = 0;

			for (int i = 0; i < _keyColumnNos.Length; i++) {
				if (i != 0) _stringBuilder.Append(_separator);
				_wrapper.AppendColumn(_keyColumnNos[i], _stringBuilder);
			}

			outRecord.Key = _stringBuilder.ToString();

			// we special case 0, because then we can avoid converting from ulong to string
			// and back to ulong.
			switch (_caseNum) {
				case 0:
					outRecord.Count = ((CountRecord)record).Count;
					break;

				case 1:
					outRecord.Count = 1;
					break;

				case 2:
					_stringBuilder.Length = 0;
					_wrapper.AppendColumn(_countColumnNo, _stringBuilder);
					try {
						outRecord.Count = ulong.Parse(_stringBuilder.ToString());
					}
					catch {
						if (!_interpretAsZero) {
							Console.WriteLine("Illegal ulong string '{0}'.\nTo interpret as zero: count column expression = ${1}!", _stringBuilder.ToString(), _countColumnNo+1);
							Environment.Exit(-1);
						}

						outRecord.Count = 0;

						_numParseErrors++;
						//return; // abort this record
					}
					break;
			}

			accepter.AddRecord(outRecord);
		}

		public override void Finish(RecordAccepter accepter) {
			if (_interpretAsZero) {
				Console.Error.WriteLine(_numParseErrors + "interpretted zeros");
			}
		}

		public override bool KeyOrderIsPreserved {
			get {
				return false;
			}
		}
	}

	internal class ToDataFilter : SortHintingFilter {
		private char _separator = '\t';
		private int[] _keyColumnNos = null;
		private string _sourceName = null;

		private ColumnWrapper _wrapper = null;
		private StringBuilder _stringBuilder = new StringBuilder();
		//private DataRecord _outRecord = null;

		public ToDataFilter(string keyExpression, char columnSeparator, string sourceName)
			:
			base(keyExpression) {
			// undocumented feature: if keyExpression ends in bang it means that
			// the user guarentees (believes really hard) that KeyOrderIsPreserved==true.
			// This this makes it possible to avoid sorts in certain situations.

			_separator = columnSeparator;
			_sourceName = sourceName;
		}

		public override void ProcessRecord(DataRecord record, RecordAccepter accepter) {
			if (_wrapper == null) {
				bool hasHeaders = false;

				HintMessageToConsole();

				if (record is TableRecord) {
					_wrapper = new ColumnWrapper(record as TableRecord);
					_keyColumnNos = _wrapper.GetColumnNos(KeyExpression);
				}

				else {
					hasHeaders = ColumnWrapper.HasHeaders(KeyExpression);
					_wrapper = new ColumnWrapper(record, _sourceName, _separator, hasHeaders);
					_keyColumnNos = _wrapper.GetColumnNos(KeyExpression);
				}

				if (hasHeaders) return; // eat up this record containing headers
			}

			//if (_outRecord == null) {
			//	_outRecord = new DataRecord();
			//}
			DataRecord outRecord = new DataRecord();

			_wrapper.SetRecord(record);

			// build the key
			_stringBuilder.Length = 0;

			for (int i = 0; i < _keyColumnNos.Length; i++) {
				if (i != 0) _stringBuilder.Append(_separator);
				_wrapper.AppendColumn(_keyColumnNos[i], _stringBuilder);
			}

			outRecord.Key = _stringBuilder.ToString();
			accepter.AddRecord(outRecord);
		}

		public override void Finish(RecordAccepter accepter) {
		}

		public override bool KeyOrderIsPreserved {
			get {
				return false;
			}
		}
	}

	internal class ToTableFilter : SortHintingFilter {
		private char _columnSeparator = '\t';
		private int[] _keyColumnNos = null;
		private string[] _columnNames = null;
		private string _sourceName = null;
		private int _caseNum = 0; // default = no conversion
		//private TableRecord _outRecord = null;

		public string[] ColumnNames {
			set {
				_columnNames = value;
			}
		}

		private ColumnWrapper _wrapper = null;
		private StringBuilder _stringBuilder = new StringBuilder();

		public ToTableFilter(string keyExpression, char columnSeparator, string sourceName) :
			base(keyExpression) {
			// undocumented feature: if keyExpression ends in !+ or !- it means that
			// the user guarentees (believes really hard) that the input is sorted.
			// This this makes it possible to avoid sorts in certain situations.

			_columnSeparator = columnSeparator;
			_sourceName = sourceName;
		}

		public override void ProcessRecord(DataRecord record, RecordAccepter accepter) {

			// caseNum 0: null conversion (key expression == incoming key)
			// caseNum 1: table record
			// caseNum 2: everything else

			// So, the thing is a null conversion is costly, potentially super
			// costly.  Instead of trying to rebuild the whole tree without
			// conversion (which brought with it potential sorting nodes as
			// well) we will error and message the user to remove the null
			// conversion.

			// initialize the wrapper
			if (_wrapper == null) {
				bool hasHeaders = false;
				int caseNum = 0;

				HintMessageToConsole();

				// case 1: table record.
				if (record is TableRecord) {
					TableRecord t = record as TableRecord;
					_wrapper = new ColumnWrapper(t);
					caseNum = 1; // table record
				}

				else {
					caseNum = 2; // everything else
					hasHeaders = ColumnWrapper.HasHeaders(KeyExpression);
					_wrapper = new ColumnWrapper(record, _sourceName, _columnSeparator, hasHeaders);
					
					// if columnNames provided by filter user
					if (_columnNames != null) {
						if (_columnNames.Length != _wrapper.ColumnNames.Length) {
							Console.WriteLine("too few column names provided");
							Environment.Exit(1);
						}
					}

					else {
						_columnNames = _wrapper.ColumnNames; // use default						
					}
				}

				// making table records is costly.  If the key columns have not
				// changed there is no reason to do the conversion.  (since
				// sorting and joining work the same for all records).

				_keyColumnNos = _wrapper.GetColumnNos(KeyExpression);
				int[] currentKeyColumnNos = _wrapper.KeyColumnNos;

				// _keyColumnNos == the requested new key columns
				if (_keyColumnNos.Length == currentKeyColumnNos.Length) {
					for (int i = 0; i < _keyColumnNos.Length; i++) {
						if (_keyColumnNos[i] != currentKeyColumnNos[i])
							_caseNum = caseNum;
					}
				}

				else _caseNum = caseNum;

				// we special case flat files converting to tables allowing null conversions
				// since when they define headers ToTable is evaluating and dropping them
				// from the input.
				if (hasHeaders) {
					_caseNum = 2;
					return;  // eat up this record containing headers
				}
			}

			switch (_caseNum) {
				case 0: // null conversion see comments above
					Console.WriteLine("Null-table conversions are costly (i.e. key expression equal to incoming key).");
					Console.WriteLine("Remove unnecessary 'ToTable(" + KeyExpression + ")' from expression.");
					Environment.Exit(1);
					break;

				case 1: // table record
					TableRecord outRecord = record as TableRecord;
					outRecord.KeyColumnNos = _keyColumnNos;
					accepter.AddRecord(outRecord);
					break;

				case 2: // everything else
					_wrapper.SetRecord(record);

					// ignore null records: no key no data
					if (record.Key.Length == 0 && record.Data == null)
						return;

					outRecord = new TableRecord(_columnNames, _columnSeparator);
					//if (_outRecord == null) {
					//	_outRecord = new TableRecord(_columnNames, _columnSeparator);
					//}

					outRecord.KeyColumnNos = _keyColumnNos;
					_stringBuilder.Length = 0;

					for (int i = 0; i < _wrapper.ColumnNames.Length; i++) {
						if (i != 0) _stringBuilder.Append('\t');
						_wrapper.AppendColumn(i, _stringBuilder);
					}

					outRecord.DelimitedColumns = _stringBuilder.ToString();
					accepter.AddRecord(outRecord);
					break;
			}
		}

		public override bool KeyOrderIsPreserved {
			get {
				return false;
			}
		}
	}

	internal class ColumnWrapper {
		private string[] _columnNames = null;
		private char _separator = '\t';
		private int[] _keyColumnNos = null;

		private string _delimitedColumns = null;
		private int _currentColumnNo = 0;
		private int _currentStart = 0;
		private int _currentEnd = 0;

		private DataRecord _nonTableRecord = null;
		private bool _isTableRecord = false;

        // for wrapping table records for table conversion
        public ColumnWrapper(TableRecord record) {
            _separator = record.TableColumnSeparator;
            _columnNames = ((TableRecord)record).ColumnNames;
            _keyColumnNos = ((TableRecord)record).KeyColumnNos;
            _isTableRecord = true;
            return;
        }

        // for wrapping records to be joined together. Either table or non-table records
        public ColumnWrapper(DataRecord record, string sourceName, char separator) {
            if (record is TableRecord) {
                _isTableRecord = true;
                _columnNames = ((TableRecord)record).ColumnNames;
                _keyColumnNos = ((TableRecord)record).KeyColumnNos;
                return;
            }

            _separator = separator;
            _isTableRecord = false;
            _SetColumnNames(record, sourceName);
		}

        // for wrapping non-table records for table conversion
        public ColumnWrapper(DataRecord record, string sourceName, char separator, bool hasHeaders) {
            _separator = separator;
            _isTableRecord = false;

            if (hasHeaders) {
                // only allow headers for flat files (they have Key=line Data=null)
                if (record.Data != null) {
                    Console.WriteLine("Only legal to specify headers for flat file input");
                    Environment.Exit(1);
                }

                // ignore the wrapper produced columnNames, this record has them
				// as it's key.  No Data columns allowed.  Must come from file.
                _columnNames = record.Key.Split(_separator);
				_keyColumnNos = new int[_columnNames.Length];
				for (int i = 0; i < _keyColumnNos.Length; i++) _keyColumnNos[i] = i;
            }

            // no headers make up column names
            else {
                _SetColumnNames(record, sourceName);
            }
        }

        private void _SetColumnNames(DataRecord record, string sourceName) {
            string[] keyCols = record.Key.Split(_separator);
            string[] dataCols = new string[0];

            if (record.Data != null) {
                dataCols = record.DataAsString.Split(_separator);
            }

            _columnNames = new string[keyCols.Length + dataCols.Length];
			for (int i = 0; i < keyCols.Length; i++)
				// display is one-based
				_columnNames[i] = sourceName + ".Key." + (i + 1).ToString();
			for (int i = 0; i < dataCols.Length; i++)
				// display is one-based
				_columnNames[keyCols.Length + i] = sourceName + ".Data." + (i + 1).ToString();

			// keyColumnNos is zero-based
			_keyColumnNos = new int[keyCols.Length];
			for (int i = 0; i < _keyColumnNos.Length; i++) _keyColumnNos[i] = i;
        }

		public void SetRecord(DataRecord record) {
			if (record == null) return;

			if (_isTableRecord) {
				TableRecord tableRecord = record as TableRecord;
				_delimitedColumns = tableRecord.DelimitedColumns;
				_AdvanceColumn(true); // sets up iteration
			}

			// else non-table so convert into columns using separator over key and data
			else {
				// for non-table records we set delimitedColumns equal to the key.
				// In this way the columns of the key are available and if the data
				// columns aren't needed they aren't computed.  The record has been
				// saved away for this computation.

				_nonTableRecord = record;
				_AdvanceColumn(true); // sets up iteration
			}
		}

		public string[] ColumnNames {
			get {
				return _columnNames;
			}
		}

		public int[] KeyColumnNos {
			get {
				return _keyColumnNos;
			}
		}

		private void _AdvanceColumn(bool reset) {
			if (reset) {
				_currentColumnNo = -1;
				_currentEnd = -1;
				if (_nonTableRecord != null) _delimitedColumns = _nonTableRecord.Key;
			}

			_currentColumnNo++;
			if (_currentColumnNo >= _columnNames.Length) {
				throw new Exception("No such column");
			}

			// if we're at the end
			if (_currentEnd == _delimitedColumns.Length) {
				_delimitedColumns = _nonTableRecord.DataAsString;
				_currentEnd = -1;
			}

			_currentStart = _currentEnd + 1;
			_currentEnd = _delimitedColumns.IndexOf(_separator, _currentStart);
			if (_currentEnd < 0) _currentEnd = _delimitedColumns.Length;
		}

		public void AppendColumn(int columnNo, StringBuilder outputStringBuilder) {
			// if were not going sequencially we need to start over.
			if (columnNo == _currentColumnNo) {
				outputStringBuilder.Append(_delimitedColumns, _currentStart, _currentEnd - _currentStart);
				return;
			}

			// we need to reset because it's left of us
			else if (columnNo < _currentColumnNo) {
				_AdvanceColumn(true);
				if (columnNo == _currentColumnNo) {
					outputStringBuilder.Append(_delimitedColumns, _currentStart, _currentEnd - _currentStart);
					return;
				}
			}

			// else we just proceed
			do {
				_AdvanceColumn(false);

			} while (_currentColumnNo < columnNo);

			outputStringBuilder.Append(_delimitedColumns, _currentStart, _currentEnd - _currentStart);
		}

		public int[] GetColumnNos(string columnExpression) {
			// grammar:
			// Expression -> C
			//            -> C+Expression (i.e. C [+C ...])
			// C -> <columnName>
			// C -> $<columnNumber> (one based like AWK)

			string[] pieces;
			if (columnExpression.IndexOf('+') >= 0) {
				pieces = columnExpression.Split('+');
			}

			else {
				pieces = new string[1];
				pieces[0] = columnExpression;
			}

			int[] columnNos = new int[pieces.Length];
			for (int i = 0; i < pieces.Length; i++) {
				columnNos[i] = -1;

				if (pieces[i].Trim().StartsWith("$")) {
					try {
						// convert from base 1 to base 0
						columnNos[i] = int.Parse(pieces[i].Substring(1)) - 1;
						if (columnNos[i] < 0) {
							Console.WriteLine("Column numbers are 1 based");
							Environment.Exit(1);
						}
					}
					catch {
						Console.WriteLine("Illegal Column expression");
						Environment.Exit(1);
					}
				}

				// else it's the name of the column
				else {
					for (int j = 0; j < _columnNames.Length; j++) {
						if (_columnNames[j].Equals(pieces[i])) {
							columnNos[i] = j;
							break;
						}
					}

					if (columnNos[i] == -1) {
						Console.WriteLine("No such column name: " + pieces[i]);
						Environment.Exit(-1);
					}
				}

			}

			return columnNos;
		}

		public static bool HasHeaders(string keyExpression) {
			return (keyExpression.IndexOf('$') < 0);
		}
	}

	/// <summary>
	/// 
	/// </summary>
	internal class RecordJoiner {
		private string[] _outputColumnNames = null;
		private string _emptyRightColumns = null;
		private StringBuilder _delimitedColumns = new StringBuilder();
		private char _separator = '\t';

		private ColumnWrapper _leftWrapper = null;
		private ColumnWrapper _rightWrapper = null;

		// when left.CurrentRecord is always != null.
		// joiner also does table conversion if necessary.
		public RecordJoiner(InternalRecordSource left, InternalRecordSource right, char columnSeparator) {
			Hashtable nameHash = new Hashtable();
			_separator = columnSeparator;

			_leftWrapper = new ColumnWrapper(left.CurrentRecord, left.CurrentSourceName, _separator);
			_rightWrapper = new ColumnWrapper(right.CurrentRecord, right.CurrentSourceName, _separator);

			_leftWrapper.SetRecord(left.CurrentRecord);
			_rightWrapper.SetRecord(right.CurrentRecord);

			int numNewCols = _leftWrapper.ColumnNames.Length;

			// In calculating new column names we remove the Key Columns
			// from the right side since this is redundant information.

			numNewCols += (_rightWrapper.ColumnNames.Length - _rightWrapper.KeyColumnNos.Length);

			_outputColumnNames = new string[numNewCols];

			int newCol = 0;
			foreach (string name in _leftWrapper.ColumnNames) {
				_outputColumnNames[newCol] = _leftWrapper.ColumnNames[newCol++];
				nameHash[name] = 1;
			}

			int j = 0;
			for (int i = 0; i < _rightWrapper.ColumnNames.Length; i++) {

				// skip keys
				if (i == _rightWrapper.KeyColumnNos[j]) {
					if (_rightWrapper.KeyColumnNos.Length > j + 1) j++;
				}

				else {
					string name = _rightWrapper.ColumnNames[i];

					object o = nameHash[name];
					if (o != null) {
						name = Path.GetFileNameWithoutExtension(right.CurrentSourceName) + ":" + name;
					}

					_outputColumnNames[newCol++] = name;
				}
			}

			// create empy columns for right side
			int numEmptyColumns = _rightWrapper.ColumnNames.Length - _rightWrapper.KeyColumnNos.Length;
			for (int i = 0; i < numEmptyColumns; i++) {
				_emptyRightColumns += _separator;
			}
		}

		public TableRecord Join(DataRecord left, DataRecord right, bool match) {
			_leftWrapper.SetRecord(left);
			_rightWrapper.SetRecord(right);

			TableRecord outRecord = new TableRecord(_outputColumnNames, _separator);
			outRecord.KeyColumnNos = _leftWrapper.KeyColumnNos;

			_delimitedColumns.Length = 0; // reset

            // add the left columns
			for (int i = 0; i < _leftWrapper.ColumnNames.Length; i++) {
				if (i != 0) _delimitedColumns.Append(_separator);
				_leftWrapper.AppendColumn(i, _delimitedColumns);
			}

			if (!match) {
				_delimitedColumns.Append(_emptyRightColumns);
				outRecord.DelimitedColumns = _delimitedColumns.ToString();
				return outRecord;
			}

			// we matched.

            // add the right columns
			int j = 0;
			for (int i = 0; i < _rightWrapper.ColumnNames.Length; i++) {
				if (i == _rightWrapper.KeyColumnNos[j]) { // don't add keys
					if (_rightWrapper.KeyColumnNos.Length > j + 1) j++;
				}

				else {
					_delimitedColumns.Append(_separator);
					_rightWrapper.AppendColumn(i, _delimitedColumns);
				}
			}

			outRecord.DelimitedColumns = _delimitedColumns.ToString();
			return outRecord;
		}
	}

	// largely stolen from FilterLeftByRightKey

	/// <summary>
	/// Class performs Left outer join based on record key of input sources.
	/// </summary>
	internal class LeftOuterJoin2 : PairFilter {
		private InternalRecordSource _leftSource = null;
		private InternalRecordSource _rightSource = null;
		private DataRecord _currentLeftRecord = null;
		private DataRecord _currentRightRecord = null;
		private char _tableColumnSeparator = '\t';
		private bool _leftNotDone = true;
		private bool _rightNotDone = true;

		private RecordJoiner _joiner = null;

		//private bool _useLookup = false;

		/// <summary>
		/// Left outer join constructor
		/// </summary>
		/// <param name="tableColumnSeparator">Character for column delimiting.</param>
		public LeftOuterJoin2(char tableColumnSeparator) {
			_tableColumnSeparator = tableColumnSeparator;
		}

		/// <summary>
		/// Specifies that key order of the input streams is preserved.
		/// </summary>
		public override bool KeyOrderIsPreserved {
			get {
				return true;
			}
		}

		/// <summary>
		/// Advances iteration.
		/// </summary>
		/// <returns>False if at end of iteration, true otherwise.</returns>
		public override bool MoveNext() {
			if (_leftSource == null) {
				_leftSource = (InternalRecordSource)_inputList[0];
				_rightSource = (InternalRecordSource)_inputList[1];

				// advance the right side from the start so that
				// a record is there waiting to be compared to
				_rightNotDone = _rightSource.MoveNext();
				if (_rightNotDone) _currentRightRecord = _rightSource.CurrentRecord;
			}

			_leftNotDone = _leftSource.MoveNext();
			if (!_leftNotDone) return false;
			_currentLeftRecord = _leftSource.CurrentRecord;

			// Don't need a match to create a joiner
			if (_joiner == null) {
				_joiner = new RecordJoiner(_leftSource, _rightSource, _tableColumnSeparator);
			}

			// advance the right side
			int diff = -1;
			bool firstTime = true;
			while (diff < 0) {

				// the first time we test we check against the currentRightSource
				// (i.e. don't go in this block) since we allow dups on the left side.
				if (!firstTime && _rightNotDone) {
					_rightSource.MoveNextHint = _currentLeftRecord.Key;
					_rightNotDone = _rightSource.MoveNext();
					if (!_rightNotDone) _currentRightRecord = null;
					else _currentRightRecord = _rightSource.CurrentRecord;
				}

				if (_currentRightRecord != null) {
					diff = TMSNStoreUtils.Utf8BytesCompare(_currentLeftRecord.KeyBytes, _currentRightRecord.KeyBytes);
				}

				else diff = 1; // break out of loop

				firstTime = false;
			}

			CurrentRecord = _joiner.Join(_currentLeftRecord, _currentRightRecord, (diff == 0));
			return true;
		}

		/// <summary>
		/// Closes joiner.
		/// </summary>
		public override void Close() {
		}
	}

    internal class LeftOuterJoin : PairFilter
    {
        private BookmarkableSource _leftSource = null;
        private LookAheadWrapper _rightSource = null;
        private char _tableColumnSeparator = '\t';
        private bool _advanceLeft = true;
        private bool _advanceRight = true;
        private bool _compareRequired = true;

        private RecordJoiner _joiner = null;

        //private bool _useLookup = false;

        /// <summary>
        /// Left outer join constructor
        /// </summary>
        /// <param name="tableColumnSeparator">Character for column delimiting.</param>
        public LeftOuterJoin(char tableColumnSeparator) {
            _tableColumnSeparator = tableColumnSeparator;
        }

        /// <summary>
        /// Specifies that key order of the input streams is preserved.
        /// </summary>
        public override bool KeyOrderIsPreserved {
            get {
                return true;
            }
        }

        /// <summary>
        /// Advances iteration.
        /// </summary>
        /// <returns>False if at end of iteration, true otherwise.</returns>
        public override bool MoveNext() {
            if (_leftSource == null) {
                _leftSource = new BookmarkableSource();
                _rightSource = new LookAheadWrapper();

                _leftSource.AddInput(Inputs[0] as InternalRecordSource);
                _rightSource.AddInput(Inputs[1] as InternalRecordSource);
            }

            // start by advancing the left
            if (_advanceLeft && !_leftSource.MoveNext()) return false;

            int diff = -1;

            while (diff < 0) {
                bool notDone = true;
                if (_advanceRight) notDone = _rightSource.MoveNext();

                if (!notDone) {
                    // diff will not equal zero at this point so we'll join as non-match
                    _advanceRight = true; // so we continue to find out we're done
                    break; // so we goto join 
                }

                if (_compareRequired) {
                    diff = TMSNStoreUtils.Utf8BytesCompare(_leftSource.CurrentRecord.KeyBytes, _rightSource.CurrentRecord.KeyBytes);
                }

                else diff = 0; // so it looks like we matched

                #region MATCH_CODE
                if (diff == 0) {
                    // see notes in InnerJoin.  LeftOuterJoin is exactly the same in the match case.

                     // LNE == T
                    if (_leftSource.NextRecordEqualsCurrent) {
                        _advanceLeft = true;
                        _advanceRight = false;
                        _compareRequired = false; // no need to compare

                        // LNE == T, RNE == T
                        if (_rightSource.NextRecordEqualsCurrent) {
                            _leftSource.SetBookmark();
                        }
                    }

                    // LNE == F, RNE == T
                    else if (_rightSource.NextRecordEqualsCurrent) {
                        _advanceRight = true;
                        _compareRequired = false; // no need to compare

                        if (_leftSource.BookmarkExists) {
                            _leftSource.GoToBookmark();
                            _advanceLeft = true;
                        }

                        else _advanceLeft = false;
                    }

                    // LNE == F, RNE == F
                    else {
                        _leftSource.ClearBookmark();
                        _advanceLeft = true;
                        _advanceRight = true;
                        _compareRequired = true;
                    }

                    break;  // go to join code
                }
                #endregion

                else if (diff > 0) { // right side is ahead of left side
                    _advanceRight = false;
                    _leftSource.MoveNextHint = _rightSource.CurrentRecord.Key;
                }

                // diff < 0 (right side is behind left side)
                else {
                    // leave advance left as it is (which is true).
                    _advanceRight = true;
                    _rightSource.MoveNextHint = _leftSource.CurrentRecord.Key;
                }
            }

            if (_joiner == null) {
                _joiner = new RecordJoiner(_leftSource, _rightSource, _tableColumnSeparator);
            }

            // joiner knows how to create record if match or if non-match
            CurrentRecord = _joiner.Join(_leftSource.CurrentRecord, _rightSource.CurrentRecord, (diff == 0));
            return true;
        }
    }

    /// <summary>
    /// This class is used for joining.  The left source is wrapped in this class so
    /// that if there is duplication of keys on the right side, we can rewind to the
    /// bookmark.
    /// </summary>
    internal class BookmarkableSource : InternalRecordSource
    {
        private List<DataRecord> _recordList = new List<DataRecord>();
        private int _currentListIndex = -1;
        private bool _listActive = false;
        private LookAheadWrapper _source;

        public bool NextRecordEqualsCurrent {
            get {
                // if we're in the list and there we are not at the end ...
                if (_listActive) {
                    if (_currentListIndex < _recordList.Count - 1) return true;
                    return false;
                }

                else return _source.NextRecordEqualsCurrent;
            }
        }

        public void ClearBookmark() {
            _recordList.Clear();
            _listActive = false;
        }

        public void SetBookmark() {
            if (_recordList.Count != 0) return;  // if already marked ignore 
            _recordList.Add(_source.CurrentRecord);
        }

        public void GoToBookmark() {
            if (_recordList.Count == 0) {
                throw new Exception("no bookmark");
            }

            _currentListIndex = -1;
            _listActive = true;
        }

        public bool BookmarkExists {
            get {
                return (_recordList.Count != 0);
            }
        }

        public override bool MoveNext() {
            if (_source == null) {
                LookAheadWrapper law = new LookAheadWrapper();
                law.AddInput(Inputs[0] as InternalRecordSource);
                _source = law;
            }

            if (_listActive) {
                _currentListIndex++;
                if (_currentListIndex == _recordList.Count) return false;
                CurrentRecord = _recordList[_currentListIndex];
                return true;
            }

            // else the source is active
            bool notDone = _source.MoveNext();
            if (!notDone) return false;

            CurrentRecord = _source.CurrentRecord;
            if (_recordList.Count != 0) _recordList.Add(CurrentRecord); // he who has gets more
            return true;
        }

        public override string MoveNextHint {
            set {
                if (value != null) {
                    _source.MoveNextHint = value;
                    _recordList.Clear(); // blows away bookmark
                }
            }
        }
    }

    internal class LookAheadWrapper : InternalRecordSource {
        private InternalRecordSource _source = null;
        private DataRecord _nextRecord = null;

        public override bool MoveNext() {
            if (_source == null) {
                _source = Inputs[0];
            }

            // we already retrieved the next record and stuck it here.
            if (_nextRecord != null) {
                CurrentRecord = _nextRecord;
                _nextRecord = null; // reset it.
                return true;
            }

            bool notDone = _source.MoveNext();
            CurrentRecord = _source.CurrentRecord;

            return notDone;
        }

        public override string MoveNextHint {
            set {
                if (value != null) {
                    _source.MoveNextHint = value;
                    _nextRecord = null;
                }
            }
        }

        public bool NextRecordEqualsCurrent {
            get {
                if (_nextRecord == null) {
                    bool notDone = _source.MoveNext();
                    if (!notDone) return false;  // if no next record can't be =
                    _nextRecord = _source.CurrentRecord;
                }

                int diff = TMSNStoreUtils.Utf8BytesCompare(CurrentRecord.KeyBytes, _nextRecord.KeyBytes);
                if (diff == 0) return true;

                return false;
            }
        }

    }
}
