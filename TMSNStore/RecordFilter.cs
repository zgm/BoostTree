using System;
using System.Collections;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Reflection;
using Microsoft.CSharp;
using System.CodeDom.Compiler;
using System.Reflection.Emit;

namespace Microsoft.TMSN.Data {

	/// <summary>
	/// User filter interface for a record source.
	/// </summary>
	public interface IRecordFilter {
		/// <summary>
		/// This method is called for each record of the record source to which
		/// the filter is connected.  The record accepter is used to pass on the
		/// output records of the filter.  The accepter has a IsDone property to
		/// signal early termination of the input source.
		/// </summary>
		/// <param name="record">The incoming record.</param>
		/// <param name="accepter">The accepter used to pass records to be outputted.</param>
		void ProcessRecord(DataRecord record, RecordAccepter accepter);

		/// <summary>
		/// This method is called once after the input source is exhaused.  This allows
		/// the filter writer to process any state it may contain and output any final
		/// records through the accepter.
		/// </summary>
		/// <param name="accepter"></param>
		void Finish(RecordAccepter accepter);

		/// <summary>
		/// if true indicates that the filter does not change the sorted state
		/// of the record keys.  For example, if the input to this filter is sorted
		/// and this filter scrambles the characters of each key then this property
		/// should be set to false.
		/// </summary>
		bool KeyOrderIsPreserved {
			get;
		}
	}

	internal abstract class SortHintingFilter : IRecordFilter,ISetRecordSource {
		private string _keyExpression = null;
		private bool _haveHint = false;
		private bool _isSorted = false;
		private bool _isSortedAscending = false;
		private RecordSource _source = null;

		public SortHintingFilter(string keyExpression) {
			char lastChar = keyExpression[keyExpression.Length - 1];

			if (keyExpression.EndsWith("!-")) {
				_haveHint = true;
				_isSorted = true;
				_isSortedAscending = false;
				_keyExpression = keyExpression.Substring(0, keyExpression.Length-2);
			}

			else if (keyExpression.EndsWith("!+")) {
				_haveHint = true;
				_isSorted = true;
				_isSortedAscending = true;
				_keyExpression = keyExpression.Substring(0, keyExpression.Length - 2);
			}

			else {
				_keyExpression = keyExpression;
			}
		}

		public string KeyExpression {
			get { return _keyExpression; }
		}

		public bool HaveHint {
			get { return _haveHint; }
		}

		public bool IsSorted {
			get { return _isSorted; }
		}

		public bool IsSortedAscending {
			get { return _isSortedAscending; }
		}

		public virtual void ProcessRecord(DataRecord record, RecordAccepter accepter) {
		}

		public virtual void Finish(RecordAccepter accepter) {
		}

		public void HintMessageToConsole() {
			if (_haveHint) {
				Console.Error.Write("[Using Sort Hint ");
				if (_isSortedAscending) Console.Error.Write("(Ascending)");
				else Console.Error.Write("(Descending)");
				if (_source != null) Console.Error.Write(": " + _source.CurrentSourceName);
				Console.Error.WriteLine("]");
			}
		}

		public virtual bool KeyOrderIsPreserved {
			get { return false; }
		}

		public RecordSource Source {
			set {
				_source = value;
			}
		}
	}

	/// <summary>
	/// Interface for passing a RecordSource to an IRecordFilter.
	/// </summary>
	public interface ISetRecordSource {
		/// <summary>
		/// The record source to be passed to an IRecordFilter.
		/// </summary>
		RecordSource Source {
			set;
		}
	}

	internal class LimitFilter : IRecordFilter {
		private long _numRecords = 0;
		private long _recordLimit;

		internal LimitFilter(long recordLimit) {
			_recordLimit = recordLimit;
		}

		public void ProcessRecord(DataRecord record, RecordAccepter accepter) {
			if (_numRecords >= _recordLimit) {
				accepter.IsDone = true;
			}

			else {
				accepter.AddRecord(record);
				_numRecords++;
			}
		}

		public void Finish(RecordAccepter accepter) {
			// do nothing
		}

		public bool KeyOrderIsPreserved {
			get {
				return true;
			}
		}
	}

	internal class RandomFilter : IRecordFilter {
		private DataRecord[] _recordArray = null;
		private long _rank = 0;
		private Random _random = null;

		// BEGIN
		public RandomFilter(int numToKeep) {
			_recordArray = new DataRecord[numToKeep];
			_random = new Random(31415927);
		}

		public RandomFilter(int numToKeep, int seed) {
			_recordArray = new DataRecord[numToKeep];
			_random = new Random(seed);
		}

		// EACH RECORD (LINE)
		public void ProcessRecord(DataRecord record, RecordAccepter accepter) {

			if (_rank < _recordArray.Length) {
				_recordArray[_rank] = record;
			}

			else {
				// pick random from 0 to rank
				long l = (long)(_random.NextDouble() * (double)_rank);
				if (l < _recordArray.Length) {
					_recordArray[l] = record;
				}
			}

			// output no records till done
			// accepter.AddRecord(record);

			_rank++;
		}

		// END
		public void Finish(RecordAccepter accepter) {
			// we may have not have had enough input to fill array.
			int numKept = _recordArray.Length;
			if (_rank < _recordArray.Length) numKept = (int)_rank;

			for (int i = 0; i < numKept; i++) {
				accepter.AddRecord(_recordArray[i]);
			}
		}

		public bool KeyOrderIsPreserved {
			get {
				return false;
			}
		}
	}

	internal class StatisticsPseudoFilter : IRecordFilter {
		private InternalRecordSource _source = null;

		public StatisticsPseudoFilter(InternalRecordSource source) {
			_source = source;
		}

		public void ProcessRecord(DataRecord record, RecordAccepter accepter) {
			// here we load the accepter with the stats we get from the source
			// and we call it a day.
			foreach (string stat in _source.Statistics) {
				DataRecord outRecord = new DataRecord();
				outRecord.Key = stat;
				accepter.AddRecord(outRecord);
			}

			accepter.IsDone = true;
		}

		public void Finish(RecordAccepter accepter) {

		}

		public bool KeyOrderIsPreserved {
			get { return false; }
		}
	}

	internal class RecordFilterDriver : InternalRecordSource {
		private InternalRecordSource _input = null;
		private bool _notDone = true;
		private IRecordFilter _filter = null;
		private RecordAccepter _recordAccepter = null;
		private string _filterType = null;
		private ulong _caughtErrors = 0;

		public RecordFilterDriver(IRecordFilter filter) {
			_filter = filter;
			_recordAccepter = new RecordAccepter();

			// some filters (ToTable, ToCount, ...) can bless the
			// input to be sorted (the responsibility falls on the
			// user to be correct).  If the filter implements
			// ISortHint we ask the filter if it has a hint.
			// if True: set _passThruInputSorting = false;
			//          set _sorting.IsSorted = ISortHint.IsSorted
			//          set _sorting.IsSortedAscending = ISortHint.IsSortedAscending
			//
			// if False: set _passThruInputSorting = filter.KeyOrderIsPreserved;

			if (filter is SortHintingFilter && ((SortHintingFilter)filter).HaveHint) {
				_passThruInputSorting = false;
				_sorting.IsSorted = ((SortHintingFilter)filter).IsSorted;
				_sorting.IsSortedAscending = ((SortHintingFilter)filter).IsSortedAscending;
			}

			else {
				// if keyOrder preserved then just pass thru
				_passThruInputSorting = filter.KeyOrderIsPreserved;
			}

			_filterType = _filter.GetType().ToString();
			if (_filterType.StartsWith("Microsoft.TMSN.Data.")) {
				_filterType = _filterType.Remove(0, 20);
			}

			ProcessTreeComment = "[filter:" + _filterType + "]";
		}

		public override void Close() {
			if (_caughtErrors > 0) {
				Console.Error.WriteLine(_caughtErrors + " caught " + _filterType + " error(s) a");
			}

            base.Close();
		}

		public override bool MoveNext() {
			// initialize
			if (_input == null) {
				_input = (InternalRecordSource)_inputList[0];
			}

			// fill up the accepter with at least one record
			while (_recordAccepter.Empty && _notDone) {

				_notDone = _input.MoveNext();

				// run the current record through the filter
				if (_notDone) {
					// put a try catch around user code
					try {
						_filter.ProcessRecord(_input.CurrentRecord, _recordAccepter);
					}

					catch (Exception ex) {
						_caughtErrors++;
						//if (_caughtErrors == 1 || (_caughtErrors % 1000 == 0)) {
							Console.Error.WriteLine(_caughtErrors + " caught " + _filterType + " error(s)");
							Console.Error.WriteLine(ex.Message + " " + _notDone + " " + _recordAccepter.IsDone);
							Console.Error.WriteLine(ex.StackTrace);
						//}
					}

					_notDone = !_recordAccepter.IsDone; // filter may elect to finish early
				}

					// there is a finish call because the filter might have state
				// this gives it a chance to output one last record.
				else {
					try {
						_filter.Finish(_recordAccepter);
					}
					catch {
						_caughtErrors++;
					}
				}
			}

			// grab the record out of the accepter
			bool notDone = _recordAccepter.MoveNext();
			if (!notDone) return false;

			CurrentRecord = _recordAccepter.CurrentRecord;

			return true;
		}
	}

	internal class DynamicRecordFilter : InternalRecordSource {
		private InternalRecordSource _input = null;
		private bool _notDone = true;
		private IRecordFilter _filter = null;
		private string _filterType = null;
		private RecordAccepter _recordAccepter = new RecordAccepter();
		private string _filterDllLocation = null;
		private string _whereClause = null;
		private string _cSharpSnippetFile = null;
		private TStoreProcessor _processor = null;
		private ulong _caughtErrors = 0;

		public override void Close() {
			if (_caughtErrors > 0) {
				Console.Error.WriteLine(_caughtErrors + " caught " + _filterType + " error(s) d");
			}

            base.Close();
		}

		public DynamicRecordFilter(TStoreProcessor processor, string whereClause) {
			_whereClause = whereClause;
			_passThruInputSorting = true;  // since a where clause can't change key order
			_processor = processor;

			// for a whereClause we have to wait to on compiling the dynamic filter
			// until we have the first record (so we know how to cast the incoming
			// record type).  So _filter remains null

			ProcessTreeComment = "[OrderPreserved: True whereClause:" + _whereClause + "]";
		}

		public DynamicRecordFilter(string cSharpSnippetFile, TStoreProcessor processor) {
			_cSharpSnippetFile = cSharpSnippetFile;
			// since the filter could change key order
			_processor = processor;

			// we can pass this early because the snippet's already written and it was
			// upto the writer to make sure the input/output types are corrrect.

			// if the file exists then we can compile it now because it's "code complete"
			// and we need to get the keyOrderIsPreserved;  Bummer, but we need to compile
			// it later when our internalSource is valid so that the filter has access to
			// it.  So we toss it after we get KeyOrderIsPreserved, and compile it
			// again at MoveNext time.

			if (File.Exists(_cSharpSnippetFile)) {
				IRecordFilter filter = _GetFilter(null, _assemblyLocations);
				_passThruInputSorting = filter.KeyOrderIsPreserved;
				_filterType = filter.GetType().ToString();
				if (_filterType.StartsWith("Microsoft.TMSN.Data.")) {
					_filterType = _filterType.Remove(0, 20);
				}

				ProcessTreeComment = "[OrderPreserved:" + _passThruInputSorting + " filter: " + _filterType + "]";
			}

			else {
				_passThruInputSorting = true;
			}
		}

		private IRecordFilter _GetFilter(string inputTypeString, Hashtable assemblyLocations) {
			CSharpRecordFilterBuilder builder = new CSharpRecordFilterBuilder(inputTypeString, assemblyLocations);

			if (_whereClause != null) {
				builder.WhereClause = _whereClause;
			}

			else if (_cSharpSnippetFile != null) {
				builder.SetCSharpSnippetFile(_cSharpSnippetFile);
			}

			else throw new Exception("must provide whereClause or snippet file");

			IRecordFilter filter = builder.GetFilter(out _filterDllLocation);

			if (filter is ISetRecordSource) {
				// if this filter implements IRecordSourceAccess then
				// we use it to set the RecordSource so the filter
				// itself will have access to it.
				RecordSource wrapper = new RecordSource(_processor);
				wrapper.InternalSource = _input;
				((ISetRecordSource)filter).Source = wrapper;
			}

			return filter;
		}

		public override bool MoveNext() {
			// initialize
			if (_input == null) {
				_input = (InternalRecordSource)_inputList[0];
			}

			// get at least one record into the accepter
			while (_recordAccepter.Empty && _notDone) {

				_notDone = _input.MoveNext();

				// run the current record through the filter
				if (_notDone) {

					// build the filter and put in place
					if (_filter == null) {
						_filter = _GetFilter(_input.CurrentRecord.GetType().ToString(), _assemblyLocations);

						// we already determined if the filter's KeyOrderIsPreserved
						// make sure we were right
						if (_passThruInputSorting != _filter.KeyOrderIsPreserved) {
							throw new Exception("user filter error: improper KeyOrderIsPreserved detection");
						}
					}

					// put a try catch around user code
					try {
						_filter.ProcessRecord(_input.CurrentRecord, _recordAccepter);
					}

					catch {
						_caughtErrors++;
						if (_caughtErrors == 1 || (_caughtErrors % 1000 == 0)) {
							Console.WriteLine(_caughtErrors + " caught " + _filterType + " error(s) d");
						}
					}

					_notDone = !_recordAccepter.IsDone; // filter may elect to finish early
				}

					// there is a finish call because the filter might have state
				// this gives it a chance to output one last record.
				else {
					try {
						_filter.Finish(_recordAccepter);
					}
					catch {
						_caughtErrors++;
					}
				}
			}

			// grab a record from the accepter
			bool notDone = _recordAccepter.MoveNext();
			if (!notDone) return false;

			CurrentRecord = _recordAccepter.CurrentRecord;

			return true;
		}
	}

	internal class CSharpRecordFilterBuilder {
		public string WhereClause = null;
		private string _cSharpSnippetFile = null;

		private string _typeString = null;
		private Hashtable _assemblyLocations = null;

		private string _userProvidedSourceCode = null;
		private string _filterName = "UserFilter";

		public CSharpRecordFilterBuilder(string typeString, Hashtable assemblyLocations) {
			if (typeString != null) _typeString = typeString.Replace("+", ".");
			_assemblyLocations = assemblyLocations;
		}

		public void SetCSharpSnippetFile(string filename) {
			_cSharpSnippetFile = filename;

			if (!File.Exists(filename)) {
				Console.WriteLine("Filter file not found");
				Console.WriteLine("Created IRecordFilter template: {0}", filename);
				string name = Path.GetFileNameWithoutExtension(filename);
				_filterName = name.Substring(0, 1).ToUpper() + name.Substring(1);

				using (StreamWriter sw = new StreamWriter(filename)) {
					sw.Write(SourceCode);
				}

				Environment.Exit(0);
			}

			string code = null;
			using (StreamReader sr = new StreamReader(filename)) {
				while (true) {
					string line = sr.ReadLine();
					if (line == null) break;
					code += line + "\n";
				}
			}

			_userProvidedSourceCode = code;
		}

		public string SourceCode {
			get {
				if (_userProvidedSourceCode != null) return _userProvidedSourceCode;
				return _SourceCode; // generated.
			}
		}

		public IRecordFilter GetFilter(out string outputAssembly) {
			if (WhereClause != null && _cSharpSnippetFile != null) {
				throw new Exception("do not set both WhereClause and CSharpSnippetFile");
			}

			Assembly asm = Assembly.GetEntryAssembly();
			string binDir = Path.GetDirectoryName(asm.Location);

			CSharpCodeProvider codeProvider = new CSharpCodeProvider();

			CompilerParameters parms = new CompilerParameters();
			string outputAssemblyLocation = Path.Combine(binDir, "filterAssembly.dll");

			// could be an assembly made by this very process.  Make a unique name
			if (_assemblyLocations.ContainsKey(outputAssemblyLocation)) {
				outputAssemblyLocation = Path.Combine(binDir, "filterAssembly0.dll");
			}

			parms.OutputAssembly = outputAssemblyLocation;

			// let the caller know too
			outputAssembly = outputAssemblyLocation;

			parms.ReferencedAssemblies.Add(Path.Combine(binDir, "TMSNStore.dll"));
			parms.ReferencedAssemblies.Add(Path.Combine(binDir, "TMSNStreams.dll"));
			parms.ReferencedAssemblies.Add("System.Web.dll");

			foreach (string location in _assemblyLocations.Values) {
				parms.ReferencedAssemblies.Add(location);
			}

			parms.GenerateExecutable = false;
			parms.GenerateInMemory = true;
			parms.IncludeDebugInformation = false;

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
			Type[] types = asm.GetExportedTypes();
			if (types.Length != 1) {
				throw new Exception("user must provide a single IRecordFilter definition in cSharpSnippetFile");
			}

			ConstructorInfo constInfo = types[0].GetConstructor(new Type[0]);
			IRecordFilter filter = (IRecordFilter)constInfo.Invoke(null);

			return filter;
		}

		private string _SourceCode {
			get {
				StringBuilder sb = new StringBuilder();
				sb.Append("using System;\n");
				sb.Append("namespace Microsoft.TMSN.Data\n");
				sb.Append("{\n");
				// CLASS
				sb.Append("   public class ");
                sb.Append(_filterName);
                sb.Append(" : IRecordFilter {\n\n");

				// CONSTRUCTOR
				sb.Append("      // BEGIN\n");
				sb.Append("      public ");
                sb.Append(_filterName);
				sb.Append("() {\n");
				sb.Append("      }\n\n");

				// PROCESSRECORD
				sb.Append("      // EACH RECORD (LINE)\n");
				if (_typeString.Equals((typeof(DataRecord)).ToString())) {
					sb.Append("      public void ProcessRecord(DataRecord record, RecordAccepter accepter) {\n");
				}

				else {
					sb.Append("      public void ProcessRecord(DataRecord inRecord, RecordAccepter accepter) {\n");
					sb.Append("         " + _typeString);
					sb.Append(" record = (");
					sb.Append(_typeString);
					sb.Append(") inRecord;\n");
				}

				// if a where clause insert it
				if (WhereClause != null) {
					sb.Append("				if (");
					sb.Append(WhereClause);
					sb.Append(") {\n");
					sb.Append("					accepter.AddRecord(record);\n");
					sb.Append("				}\n");
				}

				// otherwise default is just accept
				else {
					sb.Append("         accepter.AddRecord(record);\n");
				}

				sb.Append("      }\n\n");

				// FINISH
				sb.Append("      // END\n");
				sb.Append("      public void Finish(RecordAccepter accepter) {\n");
				sb.Append("      }\n\n");

				// KEYORDERISPRESERVED
				sb.Append("\n");
				sb.Append("      public bool KeyOrderIsPreserved {\n");
				sb.Append("        get {\n");
				sb.Append("              return true;\n");
				sb.Append("        }\n");
				sb.Append("      }\n");
				sb.Append("   }\n");
				sb.Append("}\n");

				return sb.ToString();
			}
		}
	}
}