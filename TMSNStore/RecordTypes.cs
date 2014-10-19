using System;
using System.Text;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.CSharp;
using System.CodeDom.Compiler;
using System.Reflection.Emit;


namespace Microsoft.TMSN.Data {

	/// <summary>
	/// Records implementing this interface will be reduced when appropriate.
	/// </summary>
	public interface IReducableRecord {
		/// <summary>
		/// The method which is used to combine the data of records with the same key.
		/// </summary>
		/// <param name="record">The record whose data will be combined.</param>
		void ReduceData(IReducableRecord record);
	}

	/// <summary>
	/// Record used to read/write data to/from TStore
	/// </summary>
	public class DataRecord {
		private string _key = null;
		private byte[] _keyBytes = null;
		private byte[] _data = null;
		internal long _recordNo = -1;

		/// <summary>
		/// A basic record for Data and BigDb
		/// </summary>
		public DataRecord() {
		}

		/// <summary>
		/// Gets or sets the key value for the record.
		/// </summary>
		public string Key {
			get {
				if (_key == null) {
					if (_keyBytes == null) return null;
					_key = UTF8Encoding.UTF8.GetString(_keyBytes);
				}

				return _key;
			}

			set {
				_keyBytes = null;
				_key = value;
			}
		}

		/// <summary>
		/// Alternate property for defining the key of a record.  Leading and trailing
        /// spaces are elimitated.  Sequences of spaces become a single space.  Spaces
        /// include horizontal and vertical space, carriage return and all UTF8 values
        /// below 0x09.
		/// </summary>
		public string KeySpaceNormalized {
			get {
				return Key;
			}
			set {
				_keyBytes = UTF8Encoding.UTF8.GetBytes(value);
				_key = null;

				int j = 0; // out index

				// at beginning of string pretend last was space so no beginning spaces
				bool lastSpace = true;

				for (int i = 0; i < _keyBytes.Length; i++) {
					switch (_keyBytes[i]) {
						// we consider these to be space (no horizontal tab)
						case 0x00: case 0x01: case 0x02: case 0x03: case 0x04:
						case 0x05: case 0x06: case 0x07: case 0x08: case 0x0A:
						case 0x0B: case 0x0C: case 0x0D: case 0x20:

							if (!lastSpace) _keyBytes[j++] = 0x20; // space
							lastSpace = true;
							break;

						default:
						_keyBytes[j++] = _keyBytes[i];
						lastSpace = false;
							break;
					}
				}

				// remove the last space but only if we have a non-zero length string.
				if (lastSpace && j != 0) j--;

				// we got smaller
				if (j < _keyBytes.Length) {
					Array.Resize<byte>(ref _keyBytes, j);
				}
			}
		}

		/// <summary>
		/// This property sets and gets the utf8 bytes representing the Key.
		/// </summary>
		public byte[] KeyBytes {
			get {
				if (_keyBytes == null) {
					if (_key == null) return null;
					_keyBytes = UTF8Encoding.UTF8.GetBytes(_key);
				}

				return _keyBytes;
			}

			set {
				_key = null;
				_keyBytes = value;
			}
		}

		/// <summary>
		/// Gets or sets the data to be stored/retrieved from the database.
		/// </summary>
		public virtual byte[] Data {
			get {
				return _data;
			}
			set {
				_data = value;
			}
		}

		/// <summary>
		/// Null implementation.
		/// </summary>
		public virtual string DataAsString {
			get {
				return null;
			}
		}

		/// <summary>
		/// After retrieving from the database reflects the record number of the record.
		/// </summary>
		public long RecordNo {
			get {
				return _recordNo;
			}
		}
	}

	/// <summary>
	/// Hash lists exist in SLogs.  They're of the form: xxxhhhhhhhhhhhhhhhhhhhh;...
	/// Where xxx is a prefix encoding what type of urlHash this is.
	/// And hh..hh is a 10 byte encoded hash code of the url.
	/// </summary>
	public class HashListRecord : DataRecord {

		private byte[] _data = null;
		/// <summary>
		/// This property sets the semi-colon separated list of hashes of an SLog entry.
		/// </summary>
		public string HashList {
			set {
				string[] entries = value.Split(';');
				byte[] storage = new byte[entries.Length * 13 + 1];
				byte numEntries = 0;
				int offset = 1;
				for (int i = 0; i <= 255; i++) {
					if (i >= entries.Length) break;
					if (entries[i].Length != 23) continue;
					string prefix = entries[i].Substring(0, 3);
					string hash = entries[i].Substring(3);

					byte[] prefixBytes = Encoding.Default.GetBytes(prefix);
					storage[offset++] = prefixBytes[0];
					storage[offset++] = prefixBytes[1];
					storage[offset++] = prefixBytes[2];

					for (int j = 0; j < 10; j++) {
						storage[offset++] = byte.Parse(hash.Substring(j * 2, 2), System.Globalization.NumberStyles.HexNumber);
					}

					numEntries++;
				}

				storage[0] = numEntries;
				_data = storage;
			}
		}

		/// <summary>
		/// This property returns a byte array representing the record data.
		/// </summary>
		public override byte[] Data {
			get {
				return _data;
			}
			set {
				_data = value;
			}
		}

		/// <summary>
		/// This property returns a string representing the record data.
		/// </summary>
		public override string DataAsString {
			get {
				byte[] storage = _data;
				if (storage == null) return null;

				byte numEntries = storage[0];
				int offset = 1;
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < numEntries; i++) {
					if (i != 0) sb.Append(';');
					string prefix = Encoding.Default.GetString(storage, offset, 3);
					offset += 3;
					sb.Append(prefix);
					for (int j = 0; j < 10; j++) {
						sb.Append(storage[offset++].ToString("x").PadLeft(2, '0'));
					}
				}

				return sb.ToString();
			}
		}
	}
	/// <summary>
	/// DataRecord for reading/writing strings to/from database.
	/// </summary>
	public class StringRecord : DataRecord, IReducableRecord {
		private UTF8Encoding _encoding = new UTF8Encoding(false, false);

		/// <summary>
		/// This method performs reduction for a string record.  The string data
		/// of the two records are appended.
		/// </summary>
		/// <param name="record">Record to be reduced with.</param>
		public void ReduceData(IReducableRecord record) {
			StringRecord inRecord = (StringRecord)record;
				Value += inRecord.Value;
		}

		/// <summary>
		/// Gets or sets the string data of record.
		/// </summary>
		public override string DataAsString {
			get {
				if (base.Data != null)
					return _encoding.GetString(base.Data);
				else
					return null;
			}
		}

		/// <summary>
		/// This property sets and gets the string value of the record.
		/// </summary>
		public string Value {
			get {
				if (base.Data != null)
					return _encoding.GetString(base.Data);
				else
					return null;
			}
			set {
				base.Data = _encoding.GetBytes(value);
			}
		}
	}

    internal class TableRecord : DataRecord {
        private static UTF8Encoding _encoding = new UTF8Encoding(false, false);
        private string[] _columnNames = null;
		private string _delimitedColumns = null;
        private byte[] _data = null;
		private int[] _keyColumnNos = null;
		public char TableColumnSeparator = '\t';

        public TableRecord(string[] columnNames, char tableColumnSeparator) {
            _columnNames = columnNames;
			TableColumnSeparator = tableColumnSeparator;
        }

        public string[] ColumnNames {
            get {
                return _columnNames;
            }
        }

		public int[] KeyColumnNos {
			set {
				// see if these are new key columns
				if (_keyColumnNos == null || !_ArraysAreEqual(_keyColumnNos, value)) {

					// reconstruct the delimitedColumns from the data and key before we switch the key
					if (_delimitedColumns == null && _data != null) {
						_delimitedColumns = _GetDelimitedColumnsFromData();
					}

					_keyColumnNos = value;

					if (_delimitedColumns != null) {
						_SetKey();
						_data = null;
					}

					return;
				}
			}

			get {
				return _keyColumnNos;
			}
		}

		internal static bool _ArraysAreEqual(int[] a, int[] b) {
			if (a.Length != b.Length) return false;

			for (int i = 0; i < a.Length; i++) {
				if (a[i] != b[i]) return false;
			}

			return true;
		}

		public string DelimitedColumns {
			set {
				_delimitedColumns = value;
				if (_keyColumnNos != null) {
					_SetKey();
				}

				_data = null;
			}

			get {
				if (_delimitedColumns == null) {
					_delimitedColumns = _GetDelimitedColumnsFromData();
				}

				return _delimitedColumns;
			}
		}

		public override byte[] Data {
			get {
				if (_data == null) {
					_data = _GetDataFromDelimitedColumns();
				}

				return _data;
			}

			set {
				_data = value;
				_delimitedColumns = _GetDelimitedColumnsFromData();
			}
		}

		// removes key columns from delimitedColumns
		private byte[] _GetDataFromDelimitedColumns() {
			StringBuilder sb = new StringBuilder();

			// create a string with the delimited columns with the keys removed.
			// this is all so complicated to avoid spewing string arrays all over the place

			// get them in ascending order
			ArrayList sortedKeyColumnNos = new ArrayList(_keyColumnNos);
			sortedKeyColumnNos.Sort();

			int colNo = 0;
			int j = 0;
			do {
				if (colNo != 0) sb.Append(TableColumnSeparator);

				// skip the columns which are contained in the key
				if (colNo == (int)sortedKeyColumnNos[j]) {
					//sb.Append(TableColumnSeparator);
					if (j < sortedKeyColumnNos.Count - 1) j++;
				}
				else {
					_AppendCol(colNo, sb);
				}

				colNo++;
			} while (colNo < _columnNames.Length);

			return _encoding.GetBytes(sb.ToString());
		}

		// adds key columns into delimited data
		private string _GetDelimitedColumnsFromData() {
			if (_data == null) return null;

			StringBuilder sb = new StringBuilder();
			
			_delimitedColumns = _encoding.GetString(_data);

			int colNo = 0;
			int j = 0;
			do {
				if (colNo != 0) sb.Append(TableColumnSeparator);

				bool found = false;
				for (j = 0; j < _keyColumnNos.Length; j++) {
					if (_keyColumnNos[j] == colNo) { // insert the jth column in key into delimitedColumns
						_AppendKeyCol(j, sb);
						found = true;
						break;
					}
				}

				if (!found) _AppendCol(colNo, sb);

				colNo++;
			} while (colNo < _columnNames.Length);

			return sb.ToString();
		}

		public override string DataAsString {
            get {
				return DelimitedColumns;
            }
        }

		private void _AppendKeyCol(int keyColNo, StringBuilder output) {
            int start = -1;
            for (int i = 0; i < keyColNo; i++) {
                start = Key.IndexOf(TableColumnSeparator, start + 1);
                if (start < 0) {
                    Console.WriteLine("No key column [#{0}] in key string [{1}]", keyColNo);
                    Environment.Exit(-1);
                }
            }

            start++;
            int end = Key.IndexOf(TableColumnSeparator, start);
            if (end < 0) end = Key.Length;
            output.Append(Key, start, end - start);
        }

		private void _AppendCol(int colNo, StringBuilder output) {
			int start = -1;
			for (int i = 0; i < colNo; i++) {
				start = _delimitedColumns.IndexOf(TableColumnSeparator, start + 1);
				if (start < 0) {
					// make 1 based for outside world
					Console.WriteLine("No column [${0}] in delimited string [{1}]", colNo+1, _delimitedColumns);
					Environment.Exit(-1);
				}
			}

			start++;
			int end = _delimitedColumns.IndexOf(TableColumnSeparator, start);
			if (end < 0) end = _delimitedColumns.Length;
			output.Append(_delimitedColumns, start, end-start);
		}

		private void _SetKey() {
			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < _keyColumnNos.Length; i++) {
				if (i != 0) sb.Append(TableColumnSeparator);
				_AppendCol(_keyColumnNos[i], sb);
			}

			Key = sb.ToString();
		}
    }

    /// <summary>
    /// Record for save nodes in bipartite graph
    /// </summary>
    public class BipartiteRecord : DataRecord, IReducableRecord
    {

        private Dictionary<string,int> edgeValue;
        /// <summary>
        /// It stores the mappings between a node on one side and its 
        /// connected nodes on the other side of the bipartite graph.
        /// </summary>        
        public Dictionary<string, int> Value
        {
            get { return edgeValue; }
        }
        
        /// <summary>
        /// the binary representation of Edge::Value
        /// </summary>
        public override byte[] Data
        {
            get
            {                
                return Serializer.Serialize(edgeValue);
            }
            set
            {
                if (value == null)
                    edgeValue = null;
                else
                {
                    MemoryStream stream = new MemoryStream(value);
                    edgeValue = Serializer.Deserialize(stream);
                }
            }
        }

        ///// <summary>
        ///// Constructor of BipartiteReoord
        ///// </summary>
        ///// <param name="record">record of DataRecord type</param>
        //public BipartiteRecord(DataRecord record)
        //{            
        //    this._recordNo = record._recordNo;
        //    this.Data = record.Data;
        //}

        ///// <summary>
        ///// Default constructor of BipartiteRecord
        ///// </summary>
        //public BipartiteRecord()
        //{
        //    edgeValue = new Dictionary<string, int>();
            
        //}
        /// <summary>
        /// Set value to edges between nodes in bipartite graph
        /// </summary>
        /// <param name="other"></param>
        public void SetEdge(Dictionary<string, int> other)
        {
            edgeValue = other;
        }

        /// <summary>
        /// Records with the same Key are consolidated into a single record using
        /// this method.
        /// </summary>
        /// <param name="record"></param>
        public void ReduceData(IReducableRecord record)
        {
            DataRecord localRecord = (DataRecord)record;
            MemoryStream stream = new MemoryStream(localRecord.Data);
            Dictionary<string, int> edgeRecord = Serializer.Deserialize(stream);            

            Dictionary<string, int>.Enumerator iter = edgeRecord.GetEnumerator();
            while (iter.MoveNext())
            {
                if (Value.ContainsKey(iter.Current.Key))
                {
                    Value[iter.Current.Key] += iter.Current.Value;
                }
                else
                {
                    Value.Add(iter.Current.Key, iter.Current.Value);
                }
            }
        }
    }

	/// <summary>
	/// Record for counting instances.  Implements IReducableRecord.  Ulong is stored compressed.
	/// </summary>
    public class CountRecord : DataRecord, IReducableRecord {
        private ulong _value = 0;
        private byte[] _data = null;
		private static byte[][] _smallCounts = null;

		/// <summary>
		/// Constructor for CountRecord.
		/// </summary>
        public CountRecord() {
			if (_smallCounts == null) {
				_smallCounts = new byte[0x80][];
				for (int i = 0; i < 0x80; i++) {
					_smallCounts[i] = new byte[1];
					_smallCounts[i][0] = (byte)i;
				}
			}
        }

		/// <summary>
		/// Method used to reduce CountRecords.
		/// </summary>
		/// <param name="record">Record to be reduced.</param>
		public void ReduceData(IReducableRecord record) {
            Count += ((CountRecord)record).Count;
        }

		/// <summary>
		/// Count of key instances.
		/// </summary>
        public ulong Count {
            get {
                return _value;
            }
            set {
                _value = value;
                _data = null;
            }
        }

		/// <summary>
		/// A byte array serializing the count.
		/// </summary>
        public override byte[] Data {
            get {
				if (_data == null) {
					if (_value < 0x80) return _smallCounts[_value]; // avoids allocations
					VariableLengthBitConverter converter = new VariableLengthBitConverter();
					_data = converter.GetBytes(_value);
				}

                return _data;
            }
            set {
                if (value == null) {
                    _data = null;
                    _value = 0;
                    return;
                }

                _value = VariableLengthBitConverter.ToUint64(value, 0);
            }
        }

		/// <summary>
		/// This method returns the count as a string.  The string returned is left-padded with zeros
		/// to facilitate sorting.
		/// </summary>
        public override string DataAsString {
            get {
				//return string.Format("{0,12}", Count);
				return string.Format("{0:000000000000}", Count);
            }
        }
    }

	/// <summary>
	/// Class for efficient construction of DataRecords.
	/// </summary>
    public class RecordConstructor {
		/// <summary>
		/// Virtual constructor method.
		/// </summary>
		/// <returns>DataRecord.</returns>
        public virtual DataRecord Construct() {
            return new DataRecord();
        }
    }

    internal class BipartiteRecordConstructor : RecordConstructor
    {
        public override DataRecord Construct()
        {
            return new BipartiteRecord();
        }
    }    

	internal class CountRecordConstructor : RecordConstructor {
		public override DataRecord Construct() {
			return new CountRecord();
		}
	}

    internal class StringRecordConstructor : RecordConstructor {
        public override DataRecord Construct()
        {
            return new StringRecord();
        }
    }

    internal class TableRecordConstructor : RecordConstructor {
        private string[] _tableColumnNames = null;
		private int[] _keyColumnNos = null;
		private char _tableColumnSeparator = '\t';

        public TableRecordConstructor(string[] tableColumnNames, int[] keyColumnNos, char tableColumnSeparator) {
            _tableColumnNames = tableColumnNames;
			_keyColumnNos = keyColumnNos;
			_tableColumnSeparator = tableColumnSeparator;
        }

        public override DataRecord Construct() {
			TableRecord record = new TableRecord(_tableColumnNames, _tableColumnSeparator);
			record.KeyColumnNos = _keyColumnNos;
			return record;
        }
    }

    internal class RecordInformation {
        private Type _type = null;
        private string[] _tableColumnNames = null; // in the special case the record is TableRecord
		public int[] KeyColumnNos = null;
		public char TableColumnSeparator = '\t';

        // default constructor
		public RecordInformation(char tableColumnSeparator) {
			TableColumnSeparator = tableColumnSeparator;
		}

        public void Update(RecordInformation recordInfo) {
            _type = recordInfo._type;
            _tableColumnNames = recordInfo._tableColumnNames;
			KeyColumnNos = recordInfo.KeyColumnNos;
        }

        public void Update(Type recordType) {
            _type = recordType;
        }

        public void Update(Type recordType, string[] tableColumnNames, int[] keyColumnNos, char tableColumnSeparator) {
            _type = recordType;
            _tableColumnNames = tableColumnNames;
			KeyColumnNos = keyColumnNos;
			TableColumnSeparator = tableColumnSeparator;
        }

        public void Update(DataRecord recordInstance) {
            _type = recordInstance.GetType();

            if (recordInstance is TableRecord) {
                _tableColumnNames = ((TableRecord)recordInstance).ColumnNames;
				KeyColumnNos = ((TableRecord)recordInstance).KeyColumnNos;
            }
        }

        public string[] TableColumnNames {
            get {
                return _tableColumnNames;
            }
        }

        public Type RecordType {
            get {
                return _type;
            }
        }

        internal RecordConstructor GetRecordConstructor(InternalRecordSource source) {
            if (_type == typeof(DataRecord)) {
                return new RecordConstructor();
            }
            else if (_type == typeof(StringRecord))
            {
                return new StringRecordConstructor();
            }
            else if (_type == typeof(CountRecord))
            {
                return new CountRecordConstructor();
            }

            else if (_type == typeof(TableRecord))
            {
                return new TableRecordConstructor(_tableColumnNames, KeyColumnNos, TableColumnSeparator);
            }
            else if (_type == typeof(BipartiteRecord))
            {
                return new BipartiteRecordConstructor();
            }

            Assembly asm = Assembly.GetEntryAssembly();

            CSharpCodeProvider codeProvider = new CSharpCodeProvider();
            CompilerParameters parms = new CompilerParameters();

            // if we're a webservice no entry assembly
            if (asm != null) {
                string binDir = Path.GetDirectoryName(asm.Location);
                parms.ReferencedAssemblies.Add(Path.Combine(binDir, "TMSNStore.dll"));
            }

            // if we can get type from a loaded assembly add that assembly
            asm = Assembly.GetAssembly(_type);
            parms.ReferencedAssemblies.Add(asm.Location);

            // the assemblyLocation could be external and the source knows where
            string assemblyLocation = source.GetAssemblyLocation(_type.ToString());

            if (assemblyLocation != null) {
                parms.ReferencedAssemblies.Add(assemblyLocation);
            }

            parms.GenerateExecutable = false;
            parms.GenerateInMemory = true;
            parms.IncludeDebugInformation = true;

#if DOTNET2
            CompilerResults results = codeProvider.CompileAssemblyFromSource(parms, SourceCode);
#else
			CompilerResults results = codeProvider.CreateCompiler().CompileAssemblyFromSource(parms, SourceCode);
#endif

            if (results.Errors.HasErrors) {
                Console.WriteLine(SourceCode);
                foreach (CompilerError e in results.Errors) {
                    Console.WriteLine(e.ErrorText);
                }
                Environment.Exit(-1);
            }

            asm = results.CompiledAssembly;
            Type type = asm.GetType("Microsoft.TMSN.Data.UserRecordConstructor");
            ConstructorInfo constInfo = type.GetConstructor(new Type[0]);
            RecordConstructor constructor = (RecordConstructor)constInfo.Invoke(null);

            return constructor;
        }

        private string SourceCode {
            get {
                StringBuilder sb = new StringBuilder();
                sb.Append("using System;\n");
                sb.Append("namespace Microsoft.TMSN.Data\n");
                sb.Append("{\n");
                // CLASS
                sb.Append("   public class UserRecordConstructor : RecordConstructor {\n\n");
                sb.Append("      public override DataRecord Construct() {\n");
                sb.Append("          return new ");
                string typename = RecordType.ToString().Replace('+', '.');
                sb.Append(typename);
                sb.Append("();\n");
                sb.Append("      }\n");
                sb.Append("   }\n");
                sb.Append("}\n");

                return sb.ToString();
            }
        }
    }
}

