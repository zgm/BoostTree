using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.TMSN.IO;

namespace Microsoft.TMSN.Data
{
	/// <summary>
	/// Enum specifies the record source input types.
	/// </summary>
	public enum TStoreInputType {
		/// <summary>
		/// Source is a flat file
		/// </summary>
		FlatFile,
		/// <summary>
		/// Source is a TStore
		/// </summary>
		TStore,

		/// <summary>
		/// Source is a RecordFile
		/// </summary>
		RecordFile,

		/// <summary>
		/// Source is a directory of flat files.
		/// </summary>
		Directory,
	}

	/// <summary>
	/// Enum specifies the record source output type.
	/// </summary>
	public enum TStoreOutputType {
		/// <summary>
		/// Output is a flat file.
		/// </summary>
		FlatFile,
		/// <summary>
		/// Output is a TStore.
		/// </summary>
		TStore,
		/// <summary>
		/// Output is a RecordFile.
		/// </summary>
		RecordFile,
	}

	/// <summary>
	/// Enum specifies the Storage Type
	/// </summary>
	public enum TStorageType {
		/// <summary>
		/// Storage is a flat file.
		/// </summary>
		FlatFile,
		/// <summary>
		/// Storage is a TStore.
		/// </summary>
		TStore,
		/// <summary>
		/// Storage is a RecordFile.
		/// </summary>
		RecordFile,
		/// <summary>
		/// Storage is a directory of flat files.
		/// </summary>
		Directory,
	}

	/// <summary>
	/// The uri of the input data.  This can be:
	/// a local flat file = /data/myFile
	/// a remote cosmos flat file = cosmos://tmsncosmos/vol1/myFile
	/// a local records file = records:/data/myRecords
	/// a remote cosmos records file = records:cosmos://tmsncosmos/vol1/myRecords
	/// a local TMSNStore = store:/data/myTMSNStore
	/// </summary>
	public class TStoreUri {
		private string _rawUri;
		private string _filePath;
		private TStorageType _storageType;

		/// <summary>
		/// The file path of the uri (e.g. \data\mystore).
		/// </summary>
		public string FilePath {
			get {
				return _filePath;
			}
		}

		/// <summary>
		/// The storage type of the uri (e.g. TStore).
		/// </summary>
		public TStorageType StorageType {
			get {
				return _storageType;
			}
		}

		/// <summary>
		/// Constructor for the TStoreUri.
		/// </summary>
		/// <param name="type">The TStorageType of the uri.</param>
		/// <param name="filePath">The file path of the uri.</param>
		public TStoreUri(TStorageType type, string filePath) {
			_storageType = type;
			_filePath = filePath;
			_rawUri = null;
		}

		/// <summary>
		/// Constructor for Uri class.
		/// </summary>
		/// <param name="uri">The protocol and filepath of the uri.</param>
		public TStoreUri(string uri){
			_rawUri = uri;

			if (_rawUri.StartsWith("records:") || _rawUri.StartsWith("recs:")) {
				int colon = _rawUri.IndexOf(':');
				_filePath = _rawUri.Substring(colon+1);
				_storageType = TStorageType.RecordFile;
			}

			else if (_rawUri.StartsWith("store:") || _rawUri.StartsWith("tstore:")) {
				int colon = _rawUri.IndexOf(':');
				_filePath = _rawUri.Substring(colon+1);
				_storageType = TStorageType.TStore;
			}
			
			else if (_rawUri.StartsWith("dir:") || _rawUri.StartsWith("directory:")) {
				int colon = _rawUri.IndexOf(':');
				_filePath = _rawUri.Substring(colon+1);
				_storageType = TStorageType.Directory;
			}

			else {
				_filePath = _rawUri;
				// it may be that the user just forgot to put the proper leading protocol
				// try to figure out if this is the case so they don't have to.

				// if this is a directory then the input is either a Directory or TStore.
				if (Directory.Exists(_rawUri)) {
					if (File.Exists(Path.Combine(_rawUri, "keys-data"))) { // this is a cheat
						_storageType = TStorageType.TStore;
					}

					else _storageType = TStorageType.Directory;
				}


					// directory doesn't exist
				else if (File.Exists(_rawUri)) {
					EmptyInternalSource source = new EmptyInternalSource();
					//FileStream fs = new FileStream(RawUri, FileMode.Open);
					//FileStream fs = new FileStream(RawUri, FileMode.Open, FileAccess.Read, FileShare.Read);
					Stream fs = ZStreamIn.Open(_rawUri); // so we can read gz files

					bool flatFile = false;
					try {
						// try reading the properties of a recordsFile.
						source.ReadProperties(fs);
					}
					catch {
						flatFile = true;
					}

					fs.Close();

					if (flatFile) _storageType = TStorageType.FlatFile;
					else _storageType = TStorageType.RecordFile;
				}
			}
		}
	}


#if false
	internal class ClusterTask {
		internal int TaskNo = -1;
		private ArrayList _inputs = new ArrayList();
		internal InternalRecordSource Processor = null;

		internal void AddInput(ClusterTask input) {
			_inputs.Add(input);
		}
	}
#endif

    /// <summary>
    /// Provides a stream of records.
    /// </summary>
	public class RecordSource {
		internal int SourceNo = -1;
		internal InternalRecordSource InternalSource = null;
		internal TStoreProcessor Processor = null;

        /// <summary>
        /// construction of Sources only available through TStoreProcessor factory methods
        /// </summary>
        /// <param name="processor"></param>
		internal RecordSource(TStoreProcessor processor) {
			Processor = processor;
		}

		/// <summary>
		/// Returns a string denoting the name of the current record source.
		/// </summary>
        public string CurrentSourceName {
            get {
                return InternalSource.CurrentSourceName;
            }
        }

		private void _SortReduce(bool sortAscending, bool reductionEnabled) {
			// lets change sort needs to a number for easy comparison.
			// no sort = 0, sort ascending = 1, sort descending = 2

			int askSortNum = 1; // we are asking for sorting on our output
			if (!sortAscending) askSortNum = 2;

			bool inputIsSorted = InternalSource.Sorting.IsSorted;			
			bool inputIsSortedAscending = InternalSource.Sorting.IsSortedAscending;

			int inputSortNum = 0;
			if (inputIsSorted)
				inputSortNum = 1;
			if (inputIsSorted && !inputIsSortedAscending)
				inputSortNum = 2;

			// (aside: one might ask, why not just have a separate sorter
			// and reducer.  Reduction is combined in the sorter so that 
			// reduction can happen in memory before temp files are written
			// to disk.

			if (askSortNum == inputSortNum && !reductionEnabled) return; // don't insert a sorter or reducer

			if (askSortNum != inputSortNum) {
				RecordSorterReducer sr = new RecordSorterReducer(Processor.TempDir, sortAscending, reductionEnabled);
				sr.MaxMemorySize = Processor.MaxMemorySize;
				sr.DeleteTempFiles = Processor.DeleteTempFiles;

				InternalRecordSource temp = InternalSource; // grab our input
				sr.AddInput(temp); // pipe it into the sorterReducer
				this.InternalSource = sr; // make the sorterReducer the output of this source
			}

			else {
				IRecordFilter reducer = new ReduceFilter();
				RecordFilterDriver driver = new RecordFilterDriver(reducer);
				driver.AddInput(InternalSource);
				InternalSource = driver;
			}
		}

		/// <summary>
		/// Returns a string representing various statistics about the record source.
		/// </summary>
		public List<String> Statistics {
			get {
				return InternalSource.Statistics;
			}
		}

		/// <summary>
		/// Causes reduction to occur on the record source (if record is IReducable).
		/// </summary>
		public void Reduce() {
			if (InternalSource is RecordSorterReducer) {
				RecordSorterReducer sr = InternalSource as RecordSorterReducer;
				sr.ReductionEnabled = true;
			}

			else {
				_SortReduce(true, true);
			}
		}

        /// <summary>
        /// Sorts and optionally reduces the records flowing from the record source
        /// </summary>
        /// <param name="sortAscending">causes the sort to be ascending otherwise descending</param>
        /// <param name="reductionEnabled">causes the records to be reduced.</param>
		public void SortReduce(bool sortAscending, bool reductionEnabled) {
			_SortReduce(sortAscending, reductionEnabled);
		}

		/// <summary>
		/// Truncates the number of records coming from the record source.
		/// </summary>
		/// <param name="recordLimit">Number of records to limit source to.</param>
		public void Limit(long recordLimit) {
			LimitFilter filter = new LimitFilter(recordLimit);
			RecordFilterDriver filterDriver = new RecordFilterDriver(filter);

			double fractionKept = (double)recordLimit / (double)InternalSource.TotalRecordsEstimate;
			long bytesEstimate = (long)(fractionKept * InternalSource.TotalRecordBytesEstimate);

			if (InternalSource is LoggingSource) {
				filterDriver.AddInput(InternalSource.Inputs[0]);
				filterDriver.TotalRecordsEstimate = recordLimit; // unfortunately must be tweaked from outside after AddInput 
				filterDriver.TotalRecordBytesEstimate = bytesEstimate;
				InternalSource.ClearInputs();
				InternalSource.AddInput(filterDriver);
			}

			else {
				filterDriver.AddInput(InternalSource);
				filterDriver.TotalRecordsEstimate = recordLimit; // unfortunately must be tweaked from outside after AddInput 
				filterDriver.TotalRecordBytesEstimate = bytesEstimate;
				InternalSource = filterDriver;
			}
		}

		/// <summary>
		/// Chooses random records from source.
		/// </summary>
		/// <param name="numToKeep">number of random records to pass through from source.</param>
		/// <param name="seed">a seed to the random number generator.</param>
		public void Random(int numToKeep, int seed) {
			RandomFilter filter = new RandomFilter(numToKeep, seed);
			RecordFilterDriver filterDriver = new RecordFilterDriver(filter);
			
			double fractionKept = (double)numToKeep / (double)InternalSource.TotalRecordsEstimate;
			long bytesEstimate = (long)(fractionKept * InternalSource.TotalRecordBytesEstimate);

			if (InternalSource is LoggingSource) {
				filterDriver.AddInput(InternalSource.Inputs[0]);
				filterDriver.TotalRecordsEstimate = (long)numToKeep; // unfortunately must be tweaked from outside after AddInput 
				filterDriver.TotalRecordBytesEstimate = bytesEstimate;
				InternalSource.ClearInputs();
				InternalSource.AddInput(filterDriver);
			}

			else {
				filterDriver.AddInput(InternalSource);
				filterDriver.TotalRecordsEstimate = (long)numToKeep; // unfortunately must be tweaked from outside after AddInput 
				filterDriver.TotalRecordBytesEstimate = bytesEstimate;
				InternalSource = filterDriver;
			}
		}

		/// <summary>
		/// Writes the record source to a uri.  This can one of 3 things:
		/// 1) A searchable TStore using the protocol "store:" e.g. store:foo
		/// 2) A RecordsFile using the protocol "recs:" e.g. recs:foobar
		/// 3) A FlatFile using no protocol e.g. flatfile
		/// </summary>
		/// <param name="outputUri"></param>
		public void Write(string outputUri) {
			TStoreUri uri = new TStoreUri(outputUri);
			_Write(uri);
		}

		/// <summary>
		/// Writes the record source.
		/// </summary>
		/// <param name="outputType">The type of output either TStore, RecordsFile or FlatFile</param>
		/// <param name="pathInfo">The disk path of the output</param>
		public void Write(TStoreOutputType outputType, string pathInfo) {
			TStoreUri uri = new TStoreUri((TStorageType)outputType, pathInfo);
			_Write(uri);
		}

        private void _Write(TStoreUri uri) {
            // special case writing a tstore.  If we're not sorted sort our bad selves
            if (uri.StorageType == TStorageType.TStore &&
                InternalSource.Sorting.IsSorted == false) {
                SortReduce(true, false);
            }

            if (Processor._logFile != null) {
                Console.Error.WriteLine("[Process Tree]");
				InternalSource.ProcessTreeComment += " --> [" + uri.StorageType + ":" + uri.FilePath + "]";
                InternalSource.PrintProcessTree(0);
            }

            RecordOutputter outputter = new RecordOutputter(uri);
            outputter.SetInput(InternalSource);
            outputter.TStoreGroupSize = Processor.TStoreGroupSize;
			outputter.SuppressTableHeaders = Processor.SuppressTableHeaders;
            outputter.Write();
            InternalSource.Close();
        }
	}

       	/// <summary>
		/// The Factory object for generating and filtering RecordSources.
		/// </summary>
	public class TStoreProcessor {
		//private ArrayList _recordSources = new ArrayList();
		private string[] _appInputFiles = null;
		private string[] _appOutputFiles = null;
		private int _appModeSubTaskNo = 0;
		//private bool _inUserMode = false;
		private string[] _userCommandLineArgs = null;

		/// <summary>
		/// Specifies whether table headers are suppressed when written to a flat file or Console.
		/// </summary>
		public bool SuppressTableHeaders = false;

		/// <summary>
		/// The directory where temporary sort files will be written.
		/// </summary>
		public string TempDir = null;

		/// <summary>
		/// Specifies whether temp files should be deleted.  (default true).
		/// </summary>
		public bool DeleteTempFiles = true;

		/// <summary>
		/// Specifies the size of the groups records are stored in in a tstore (default 32).
		/// </summary>
		public uint TStoreGroupSize = 32;
        internal string _logFile = null; // default no logging

		/// <summary>
		/// Specifies the table column delimiter.
		/// </summary>
		public char TableColumnSeparator = '\t';

		private int _maxMemorySize = 1024 * 1024 * 100;

		/// <summary>
		/// The constructor for the TStore processor.
		/// </summary>
		public TStoreProcessor() {

			// if we're on the cluster we replace this
			TempDir = Environment.GetEnvironmentVariable("TEMP");
		}

		/// <summary>
		/// This property returns a parser capable of interpretting a txp expression.
		/// </summary>
		public ExpressionParser Parser {
			get {
				return new ExpressionParser(this);
			}
		}

		/// <summary>
		/// Specifies the maximum memory size to be used by a single sort thread.  (2 threads run concurrently).
		/// </summary>
        public long MaxMemorySize {
            set {
                if (value > 0x80000000) throw new Exception("MaxMemorySize must be less than 2GB");
                _maxMemorySize = (int)value;
                
            }
            get {
                return _maxMemorySize;
            }
        }

        /// <summary>
        /// Sets a file for logging output.  Use "stderr" for Console.Error.
        /// </summary>
        /// <param name="filename"></param>
        public void SetLogging(string filename) {
            _logFile = filename;
        }

        /// <summary>
        /// Does command line argument parsing for programs using the TStoreProcessor.
        /// </summary>
        /// <param name="args"></param>
        /// <param name="cmd"></param>
        /// <returns></returns>
		public bool ParseArgumentsWithUsage(string[] args, object cmd) {
			// args format:
			// if (args[0]) is a number then application mode:
			//  application mode format: <subtaskNo> <numUserArgs> <userArgs...> <nebulaArgs...>
			// else user mode:
			//  user mode format: <userArgs...> 
			
			int i;
			if (args.Length == 0) {
                //if (CommandLine.Parser.ParseArgumentsWithUsage(args, cmd) == false) {
                //    return false;
                //}

				string[] myargs = {" "};
				//CommandLine.Parser.ParseArgumentsWithUsage(myargs, cmd);
				return false;
			}

			// special mode for parsing the ProcessInfo.txt file for the args
			// this is a hack, we shouldn't know about nebula here.
			if (args[0].Equals("Nebula")) {
				string processInfoFile = @"\\" + args[1] + @"\data\pn\Processes\" + args[2] + @"\ProcessInfo.txt";
				if (File.Exists(processInfoFile)) {
					StreamReader reader = new StreamReader(processInfoFile);
					string argsString = null;
					while (true) {
						string line = reader.ReadLine();
						if (line == null) break;

						Match m = Regex.Match(line, @"CommandLine=\[[^ ]+ (?<d>[^\]]*)");
						if (m.Success) {
							argsString = m.Groups["d"].ToString();
							break;
						}
					}

					argsString = Regex.Replace(argsString, @"\s+", " ");
					args = argsString.Split(' ');
				}
			}

			// returns -1 if not a positive number
			_appModeSubTaskNo = _SafePositiveIntParse(args[0]);

			// if still -1 then we are in user mode -- safe to parse the input
			// grab the user command line args to pass on the nebular command line
			if (_appModeSubTaskNo == -1) {
				//_inUserMode = true;
				string[] temp = Environment.GetCommandLineArgs();
				// copy to _userCommandLineArgs.  Skip the first one which strangely
				// is the binary.
				_userCommandLineArgs = new string[temp.Length-1];
				for (i = 1; i < temp.Length; i++) {
					_userCommandLineArgs[i-1] = temp[i];
				}

                //if (CommandLine.Parser.ParseArgumentsWithUsage(args, cmd) == false) {
                //    return false;
                //}

				return true; // we in user mode and we've got our command line args
			}

			// we are in application mode
			//_inUserMode = false;

			int numUserCommandLineArgs = _SafePositiveIntParse(args[1]);
			if (numUserCommandLineArgs < 0) {
				throw new Exception("bad nebula command line");
			}

			string[] userCommandLineArgs = new string[numUserCommandLineArgs];
			// copy the user command line args from the nebula command line
			// into its own array.  This way we can call command line parsing on
			// it and not worry about the args we or nebula adds. 
			
			for (i = 0; i < numUserCommandLineArgs; i++) {
				userCommandLineArgs[i] = args[i+2];
			}
			
			// parse them
            //if (CommandLine.Parser.ParseArgumentsWithUsage(userCommandLineArgs, cmd) == false) {
            //    return false;
            //}

			// colllect these up from nebula.  They start after the user ones
			ArrayList inputFilesArray = new ArrayList();
			ArrayList outputFilesArray = new ArrayList();

			// 2 for ourselves (<subTaskNo> <numUserArgs>
			// numUserCommandlineArgs for the user;
			i = 2 + numUserCommandLineArgs;
			while (i < args.Length) {
				if (args[i].Equals("-i")) {
					inputFilesArray.Add(args[++i]);
				}

				else if (args[i].Equals("-o")) {
					outputFilesArray.Add(args[++i]);
				}

				i++;
			}

			_appInputFiles = (string[])inputFilesArray.ToArray(typeof(string));
			_appOutputFiles = (string[])outputFilesArray.ToArray(typeof(string));

			return true;
		}

		internal static int _SafePositiveIntParse(string inString) {
			int integer = -1; // return -1 if in userMode

			// see if we're in app mode.  If zeroth arg is a number -> yes
			if (char.IsDigit(inString, 0)) {
				integer = int.Parse(inString);
			}

			return integer;
		}

		/// <summary>
		/// Allows user to define programmatic source using IUserSource interface.
		/// </summary>
		/// <param name="userSource">Object implementing IUserSource interface.</param>
		/// <returns>RecordSource</returns>
		public RecordSource Input(IUserSource userSource) {
			RecordSource source2Bmapped = new RecordSource(this);

			InternalRecordSource source = new InternalUserSource(userSource);
			source2Bmapped.InternalSource = _AddLogging(source);
			return source2Bmapped;
		}

		/// <summary>
		/// Allows user to define programmatic source using SourceDelegate.
		/// </summary>
		/// <param name="sourceDelegate">SourceDelegate returning DataRecords and null and end of source.</param>
		/// <returns>RecordSource</returns>
		public RecordSource Input(SourceDelegate sourceDelegate) {
			RecordSource source2Bmapped = new RecordSource(this);

			InternalRecordSource source = new InternalUserSource(sourceDelegate);
			source2Bmapped.InternalSource = _AddLogging(source);
			return source2Bmapped;
		}

        /// <summary>
        /// Creates a record source out of 3 possible input types:
        /// 1) A searchable TStore
        /// 2) A streamable RecordsFile
        /// 3) A streamable FlatFile
        /// </summary>
        /// <param name="inputType">The type of input</param>
        /// <param name="pathInfo">The path to the input</param>
        /// <returns>A RecordSource for further processing</returns>
		public RecordSource Input(TStoreInputType inputType, string pathInfo) {
			InternalRecordSource source = null;

			switch (inputType) {
				case TStoreInputType.RecordFile:
					source = new InternalRecordFileReader(pathInfo);
					break;

				case TStoreInputType.TStore:
					TMSNStoreReader reader = new TMSNStoreReader(pathInfo, true);
					source = reader.RecordSource;
					break;

				case TStoreInputType.FlatFile:
					source = new FlatFileMapper(pathInfo);
					break;

				case TStoreInputType.Directory:
					source = new DirectoryMapper(pathInfo);
					break;
			}

			source.ProcessTreeComment = "[" + pathInfo + "]";

			RecordSource source2Bmapped = new RecordSource(this);
			source2Bmapped.InternalSource = _AddLogging(source);

			return source2Bmapped;
		}

		private InternalRecordSource _AddLogging(InternalRecordSource source) {
			// _logFile == null -> no logging
			// _logFile == "stderr" -> log to Console.Error
			// _logFile == "whatever" -> log to whatever file

			if (_logFile == null) {
				StreamWriter err = new StreamWriter(Stream.Null);
				Console.SetError(err);
				return source; // return source with no logging
			}

			// if != stderr
			if (!_logFile.Equals("stderr")) {
				StreamWriter err = new StreamWriter(_logFile);
				Console.SetError(err);
			}

			LoggingSource logger = new LoggingSource();
			logger.AddInput(source); // insert the logger

			return logger;
		}

        /// <summary>
        /// Creates a record source out of 3 possible input types:
        /// 1) A searchable TStore using protocol "store:" e.g. store:foo
        /// 2) A streamable RecordsFile using protocol "recs:" e.g. recs: foobar
        /// 3) A streamable FlatFile using no protocol e.g. flatfile
        /// </summary>
        /// <param name="inputUri">the uri of the input</param>
        /// <returns>A RecordSource for further processing</returns>
		public RecordSource Input(string inputUri) {
			TStoreUri uri = new TStoreUri(inputUri);

			TStoreInputType inputType = (TStoreInputType)uri.StorageType;
			return Input(inputType, uri.FilePath);
		}

		/// <summary>
		/// Pass through only those records with key matching the query expression.
		/// The expression can be either a literal string or end with a * to denote
		/// prefix matching.
		/// </summary>
		/// <param name="input">The input source to be queried.</param>
		/// <param name="query">The query to be matched.</param>
		/// <returns></returns>
		public RecordSource QueryKey(RecordSource input, string query) {
			QuerySource querier = new QuerySource(query);
			querier.AddInput(input.InternalSource);

			RecordSource source2Bqueried = new RecordSource(this);
			source2Bqueried.InternalSource = querier;

			return source2Bqueried;
		}

        /// <summary>
        /// Filters a RecordSource using a user provided filter
        /// </summary>
        /// <param name="input">The input record source</param>
        /// <param name="filter">The user provided recordFilter</param>
        /// <returns>A RecordSource for further processing</returns>
		public RecordSource Filter(RecordSource input, IRecordFilter filter) {

			if (filter is ISetRecordSource) {
				// if this filter implements IRecordSourceAccess then
				// we use it to set the RecordSource so the filter
				// itself will have access to it.
				((ISetRecordSource)filter).Source = input;
			}

			RecordFilterDriver filterDriver = new RecordFilterDriver(filter);
			filterDriver.AddInput(input.InternalSource);
			RecordSource source2Bfiltered = new RecordSource(this);
			source2Bfiltered.InternalSource = filterDriver;
			return source2Bfiltered;
		}

		/// <summary>
		/// This operation on a record source converts it to a source containing
		/// DataRecords with information about the source itself.  The records of
		/// the input source are ignored completely and information like source name,
		/// estimated size, sorted-ness, etc. flow from the source.
		/// </summary>
		/// <param name="input">Input record source.</param>
		/// <returns>Output Records source.</returns>
		public RecordSource GetStatistics(RecordSource input) {
			// this is implemented like a filter.
			StatisticsPseudoFilter filter = new StatisticsPseudoFilter(input.InternalSource);
			RecordFilterDriver filterDriver = new RecordFilterDriver(filter);
			filterDriver.AddInput(input.InternalSource);
			RecordSource source2Bfiltered = new RecordSource(this);
			source2Bfiltered.InternalSource = filterDriver;
			return source2Bfiltered;
		}

        /// <summary>
        /// Selects records from a RecordSource using a user provided whereClause.  The user
        /// is assumed to know the record type.  For example with a text file input the record
        /// type is the generic DataRecord so a filter might be "record.Key[0] == 'A'" to filter
        /// all records except those that begin with 'A'.
        /// </summary>
        /// <param name="input">The input RecordSource.</param>
        /// <param name="whereClause">The select clause</param>
        /// <returns>A RecordSource for further processing</returns>
		public RecordSource SelectFilter(RecordSource input, string whereClause) {
			whereClause = whereClause.Replace("^", "\"");
			DynamicRecordFilter filter = new DynamicRecordFilter(this, whereClause);
			filter.AddInput(input.InternalSource);
			RecordSource source2Bfiltered = new RecordSource(this);
			source2Bfiltered.InternalSource = filter;
			return source2Bfiltered;
		}

        /// <summary>
        /// Filters the RecordSource by using a user proviced CSharpFilterFile.  The IRecordsFilter
        /// within the file is compiled on the fly and inserted after the input RecordSource.  If 
        /// a non-existant file is provided the method will print template file which can be modified
        /// by the user.
        /// </summary>
        /// <param name="input">The input RecordSource.</param>
        /// <param name="cSharpFilterFile">The csharp file containing a IRecordFilter definition.</param>
        /// <returns>A RecordSource for further processing</returns>
		public RecordSource FilterByCSharpSnippet(RecordSource input, string cSharpFilterFile) {
			DynamicRecordFilter filter = new DynamicRecordFilter(cSharpFilterFile, this);
			filter.AddInput(input.InternalSource);
			RecordSource source2Bfiltered = new RecordSource(this);
			source2Bfiltered.InternalSource = filter;
			return source2Bfiltered;
		}

        /// <summary>
        /// A RecordSource with zero records.
        /// </summary>
        /// <returns>A RecordSource for further processing</returns>
		public RecordSource EmptySource(bool isSorted, bool isSortedAscending) {
			RecordSource emptySource = new RecordSource(this);
			emptySource.InternalSource = new EmptyInternalSource();
			emptySource.InternalSource.Sorting.IsSorted = isSorted;
			emptySource.InternalSource.Sorting.IsSortedAscending = isSortedAscending;
 
			return emptySource;
		}

        /// <summary>
        /// Pair performs operations two input RecordSources, left and right.  These are, for
        /// example, CatLeftThenRight, SortedMerge, FilterLeftByRightKey.
        /// </summary>
        /// <param name="left">One of two RecordSources</param>
        /// <param name="right">One of two RecordSources</param>
        /// <param name="pairOperation">The operation to perform on the input sources</param>
        /// <returns>A RecordSource for further processing</returns>
        public RecordSource Pair(RecordSource left, RecordSource right, PairOperation pairOperation) {
            PairFilter filter = null;
            bool requiresSorting = false; // not all pair operations require inputs sorted
            bool ascending = true;

            switch (pairOperation) {
				case PairOperation.FilterLeftInRight:
					filter = new FilterLeftByRightKey(true);
					requiresSorting = true;
					break;

				case PairOperation.FilterLeftNotInRight:
					filter = new FilterLeftByRightKey(false);
					requiresSorting = true;
					break;

                case PairOperation.MergeAscend:
                    filter = new SortedMerge(true);
                    requiresSorting = true;
                    ascending = true;
                    break;

                case PairOperation.MergeDescend:
                    filter = new SortedMerge(false);
                    requiresSorting = true;
                    ascending = false;
                    break;

                case PairOperation.CatLeftThenRight:
                    filter = new CatLeftThenRight();
                    requiresSorting = false;
                    break;
            }

            // T = TypeSort asked for, either Ascending or Descending

            //			     Right
            //		    !T		T	
            // l	!T	sbT 	slT
            // e
            // f	T	srT		X
            // t

            if (requiresSorting) {
                if (!left.InternalSource.Sorting.IsSorted || left.InternalSource.Sorting.IsSortedAscending != ascending) {
                    left.SortReduce(ascending, false);
                }

                if (!right.InternalSource.Sorting.IsSorted || right.InternalSource.Sorting.IsSortedAscending != ascending) {
                    right.SortReduce(ascending, false);
                }
            }

            RecordSource source2BoperatedOn = new RecordSource(this);
            source2BoperatedOn.InternalSource = filter;
            filter.AddInput(left.InternalSource);
            filter.AddInput(right.InternalSource);
            return source2BoperatedOn;
        }


        /// <summary>
        /// JoinInner
        /// </summary>
        /// <param name="left">One of two RecordSources</param>
        /// <param name="right">One of two RecordSources</param>
        /// <returns>A RecordSource for further processing</returns>
        public RecordSource InnerJoin(RecordSource left, RecordSource right) {
            return _Join(left, right, true);
        }

        /// <summary>
        /// JoinInner
        /// </summary>
        /// <param name="left">One of two RecordSources</param>
        /// <param name="right">One of two RecordSources</param>
        /// <returns>A RecordSource for further processing</returns>
        public RecordSource LeftOuterJoin(RecordSource left, RecordSource right) {
            return _Join(left, right, false);
        }

        // if inner==true, does inner Join else does leftOuter Join
        private RecordSource _Join(RecordSource left, RecordSource right, bool inner) {
            bool requiresSorting = true;
            bool ascending = true;

            PairFilter filter = null;

            if (inner) filter = new InnerJoin(TableColumnSeparator);
            else filter = new LeftOuterJoin(TableColumnSeparator);

            // T = TypeSort asked for, either Ascending or Descending

            //			     Right
            //		    !T		T	
            // l	!T	sbT 	slT
            // e
            // f	T	srT		X
            // t

            if (requiresSorting) {
                if (!left.InternalSource.Sorting.IsSorted || left.InternalSource.Sorting.IsSortedAscending != ascending) {
                    left.SortReduce(ascending, false);
                }

                if (!right.InternalSource.Sorting.IsSorted || right.InternalSource.Sorting.IsSortedAscending != ascending) {
                    right.SortReduce(ascending, false);
                }
            }

            RecordSource source2BoperatedOn = new RecordSource(this);
            source2BoperatedOn.InternalSource = filter;
            filter.AddInput(left.InternalSource);
            filter.AddInput(right.InternalSource);
            return source2BoperatedOn;
        }
    }

    /// <summary>
    /// A helper class used by IRecordFilters.  This object is a FIFO that allows a IRecordFilter
    /// to create multiple output records from a single input record.
    /// </summary>
	public class RecordAccepter {

		private int _fifoMemorySize = 1024;
		private DataRecord[] _recordFifo = null;
		//private byte[][] _keyFifo = null;
		//private byte[][] _dataFifo = null;

		private int _inputIndex = 0;
		private int _outputIndex = -1;
		private int _numRecords = 0;
		private DataRecord _currentRecord = null;
        private bool _isDone = false; // way for a filter to say it's done early

        /// <summary>
        /// A way for an IRecordFilter to signal that is done with the input.
        /// </summary>
        public bool IsDone {
            get {
                return _isDone;
            }
            set {
                _isDone = value;
            }
        }

		internal RecordAccepter() {
			_recordFifo = new DataRecord[_fifoMemorySize];
			//_keyFifo = new byte[_fifoMemorySize][];
			//_dataFifo = new byte[_fifoMemorySize][];
		}

        /// <summary>
        /// Adds a record to the accepter (FIFO).
        /// </summary>
        /// <param name="record">The record to be added.</param>
		public void AddRecord(DataRecord record) {
			if (_inputIndex >= _fifoMemorySize) {
				//byte[][] oldKey = _keyFifo;
				//byte[][] oldData = _dataFifo;
				DataRecord[] oldFifo = _recordFifo;

				// double the size (could be more efficient if we paid attention
				// to the input/output pointers but since the generally records won't
				// have a very high amplification factor this shouldn't be a big deal).
				_fifoMemorySize += _fifoMemorySize;
				//_keyFifo = new byte[_fifoMemorySize][];
				//_dataFifo = new byte[_fifoMemorySize][];
				_recordFifo = new DataRecord[_fifoMemorySize];

				for (int i = 0; i < oldFifo.Length; i++) {
					//_keyFifo[i] = oldKey[i];
					//_dataFifo[i] = oldData[i];
					_recordFifo[i] = oldFifo[i];
				}
			}

			//_keyFifo[_inputIndex] = record.KeyBytes;
			//_dataFifo[_inputIndex] = record.Data;
			_recordFifo[_inputIndex] = record;
			_inputIndex++;

			_numRecords++;

			// we grab the first inputted record.  This will be used as the output
			// record instance we change the key and data on.
			//if (_currentRecord == null) {
			//	_currentRecord = record;
			//}
		}

		internal bool Empty {
			get {
				return (_numRecords == 0);
			}
		}

		internal bool MoveNext() {
			_outputIndex++; // advance the index to the next record to output.

			if (_outputIndex < _inputIndex) {
				//_currentRecord.KeyBytes = _keyFifo[_outputIndex];
				//_currentRecord.Data = _dataFifo[_outputIndex];
				_currentRecord = _recordFifo[_outputIndex];
				_numRecords--;

				// reset indices
				if (_numRecords == 0) {
					_inputIndex = 0;
					_outputIndex = -1;
				}

				return true;
			}

			return false;
		}

		internal DataRecord CurrentRecord {
			get {
				return _currentRecord;
			}
		}

		internal void Close() {
		}
	}
}
