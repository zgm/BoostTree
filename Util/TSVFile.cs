using System;
using System.IO;
using System.Collections;
using Microsoft.TMSN.IO;

namespace Microsoft.TMSN
{

	/// <summary>
	/// Encapsulates TSV Files.
	/// TSV files have two special columns: The "grouping" column and the "label" column.
	/// The grouping column can contain any string, and defines the "chunks" by which the 
	/// TSV file is defined. Each set of rows containing the same value in the grouping column
	/// belong together in some way. Note, all of the lines for a given group are required to 
	/// be together. The other special column is the label column, which defines
	/// the rating given to each line in the file.
	/// </summary>
	/// 
	public class RankingTSVFile : IEnumerable
	{
		/// <summary>
		/// A class which abstracts reading lines from a tsv file.
		/// </summary>
		protected abstract class LineReader : ICloneable {
			/// <summary>
			/// resets the line reader to its beginning
			/// </summary>
			public void Reset() {
				Close();
			}
			/// <summary>
			/// returns the headers of this tsv file
			/// </summary>
			public abstract string Headers {
				get;
			}			
			/// <summary>
			/// closes the line reader
			/// </summary>
			public abstract void Close();
			/// <summary>
			/// opens the line reader
			/// </summary>
			public abstract void Open();
			/// <summary>
			/// reads a single line
			/// </summary>
			/// <returns></returns>
			public abstract string ReadLine();
			/// <summary>
			/// return true if line reader is open
			/// </summary>
			public abstract bool IsOpened {
				get;
			}

			/// <summary>
			/// returns a copy of the line reader
			/// </summary>
			/// <returns></returns>
			public abstract object Clone();
		}

		/// <summary>
		/// implements a line reader on a single file
		/// </summary>
		protected class SingleLineReader : LineReader {
			private string filename;
			private string headers;
			private StreamReader reader = null;

			/// <summary>
			/// constructor
			/// </summary>
			/// <param name="filename">the tsv file to be read</param>
			public SingleLineReader(string filename) {
				this.filename = filename;
			}
			/// <summary>
			/// closes the line reader
			/// </summary>
			public override void Close() {
				if (IsOpened) {
					reader.Close();
				}
				reader = null;
				headers = null;
			}
			/// <summary>
			/// returns true if the line reader is open
			/// </summary>
			public override bool IsOpened {
				get {
					return (reader != null);
				}
			}
			/// <summary>
			/// opens the line reader
			/// </summary>
			public override void Open() {
				reader = ZStreamReader.Open(filename);
				headers = reader.ReadLine();
			}
			/// <summary>
			/// reads a line from the tsv file
			/// </summary>
			/// <returns></returns>
			public override string ReadLine() {
				return reader.ReadLine();
			}

			/// <summary>
			/// returns the headers of the tsv file
			/// </summary>
			public override string Headers {
				get {
					if (reader == null) Open();
					return headers;
				}
			}

			/// <summary>
			/// returns a copy of the line reader
			/// </summary>
			/// <returns></returns>
			public override object Clone() {
				SingleLineReader slr = new SingleLineReader(this.filename);
				return slr;
			}
		}

		/// <summary>
		/// The column name that will represent groups in instances, unless set otherwise.
		/// Currently "m:QueryId".
		/// </summary>
		public static readonly string GroupColumnNameDefault = "m:QueryId";
		// should this really be "m:Rating", and not "m:Label"? ***
		/// <summary>
		/// The column name that will represent labels in instances, unless set otherwise.
		/// Currently "m:Rating".
		/// </summary>
		public static readonly string LabelColumnNameDefault = "m:Rating";

		/// <summary>
		/// The column name that will be used for filtering in instances, unless set otherwise.
		/// Currently "m:ResultType".
		/// </summary>
		public static readonly string FilterColumnNameDefault = "m:ResultType";

		/// <summary>
		/// The column name containing the date for those instances that have labels.
		/// </summary>
		public static readonly string DateStrColumnNameDefault = "m:Date";

		/// <summary>
		/// The label that will be assumed for unlabeled (negative) rows.
		/// Currently 0.
		/// </summary>
		public static readonly int LabelForUnlabeledDefault = 0;

		//private readonly string filename;
		private readonly LineReader lineReader = null;
		private readonly string[] columnNames;
		private readonly string[] featureColumnNames; // Column names of just the features

		private string groupColumnName = GroupColumnNameDefault;  //"m:Group";
		private string labelColumnName = LabelColumnNameDefault;  //"m:Label";       
		private string filterColumnName = FilterColumnNameDefault; //"m:ResultType"
		private string dateStrColumnName = DateStrColumnNameDefault; //"m:Date"
		private int groupCol;
		private int labelCol;
		private string filterValue;

		private string queryIdColumnName = "m:QueryId";
		private string queryColumnName = "m:Query";
		private string docIdColumnName = "m:DocId";
		private string urlStrColumnName = "m:Url";
		private int filterCol;
		private int queryIdCol;
		private int queryCol;
		private int docIdCol;
		private int urlStrCol;
		private int dateStrCol;
        private bool lookedForSpecialColumns = false;

		private bool keepMeta = false; //keep the metadata (QueryStr, QueryIDStr, DocID, URLStr,...)		
		/// <summary>
		/// Creates a RankingTSVFile from a file on disk.
		/// Does not read the file into memory. The file is accessed through enumerators
		/// </summary>
		/// <param name="filename"></param>				
		public RankingTSVFile(string filename) :
			this(new SingleLineReader(filename))
		{
			//this.filename = filename;			
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="lineReader"></param>
		/// <param name="keepMeta"></param>
		protected RankingTSVFile(LineReader lineReader, bool keepMeta) :
			this(lineReader) {
			this.keepMeta = keepMeta;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="lineReader"></param>
		protected RankingTSVFile(LineReader lineReader) {
			this.lineReader = lineReader;

			columnNames = null;
			groupCol = -1;
			labelCol = -1;

			filterCol = -1;
			dateStrCol = -1;
			queryIdCol = -1;
			queryCol = -1;
			docIdCol = -1;
			urlStrCol = -1;

			// Search has informed us that the .tsv files are encoded in UTF8
			string line = this.lineReader.Headers;
			this.lineReader.Close();

			columnNames = line.Split('\t');

			ArrayList ftrColNames = new ArrayList();
			for(int i=0; i<columnNames.Length; ++i) {
				if(IsFeatureColumn(i))
					ftrColNames.Add(columnNames[i]);
			}
			featureColumnNames = (string[])ftrColNames.ToArray(typeof(string));
		}

		/// <summary>
		/// Creates a RankingTSVFile from a file on disk.
		/// Does not read the file into memory. The file is accessed through enumerators
		/// </summary>
		/// <param name="filename"></param>
		/// <param name="filtername"></param>
		/// <param name="filtervalue"></param>
		/// <param name="keepMeta"></param>
		public RankingTSVFile(string filename, string filtername, string filtervalue, bool keepMeta)
			: this(filename)
		{
			this.keepMeta = keepMeta;
			this.filterValue = filtervalue;
			if (filtername != null)
			{
				this.filterColumnName = filtername;
			}
		}

		/// <summary>
		/// Return the first line of the TSV file unchanged (i.e. tab separated headers).
		/// </summary>
		public string Headers
		{
			get { return lineReader.Headers; }
		}

		/// <summary>
		/// Get an array of strings giving the names for each column, as defined in the header of the TSV file
		/// </summary>
		public string[] ColumnNames
		{
			get { return columnNames; }
		}
		/// <summary>
		/// Get an array of strings giving the names for each feature, as defined in the header of the TSV file
		/// </summary>
		public string[] FeatureColumnNames
		{
			get { return featureColumnNames; }
		}
		/// <summary>
		/// Get the number of columns in the TSV file
		/// </summary>
		public int NumCols
		{
			get { return columnNames.Length;}
		}
		/// <summary>
		/// Get the index of the grouping column.
		///	The grouping column defines the "chunks" by which the 
		/// TSV file is defined. Each set of rows containing the same value in the grouping column
		/// belong together in some way. Note, all of the lines for a given group are required to 
		/// be together.  
		/// </summary>
		public int GroupColumn
		{
			get 
			{ 
				if (!lookedForSpecialColumns)
					FindSpecialColumns();
				return groupCol; 
			}
		}
		/// <summary>
		/// Get the index of the label column. The label is the "rating" of each line in the TSV file
		/// </summary>
		public int LabelColumn
		{
			get 
			{ 
				if (!lookedForSpecialColumns)
					FindSpecialColumns();
				return labelCol; 
			}
		}

		/// <summary>
		/// Get the index of the query column. The label is the "Query" of each line in the TSV file
		/// </summary>
		public int QueryColumn
		{
			get
			{
				if (!lookedForSpecialColumns)
					FindSpecialColumns();
				return queryCol;
			}
		}

		/// <summary>
		/// Get the index of the column whose value is used to sub-divede the data. 
		/// The label is supplied from the input. For example, if the label is "ResultType", we can partition the
		/// data into Top2500 and NRandom
		/// </summary>
		public int FilterColumn
		{
			get
			{
				if (!lookedForSpecialColumns)
					FindSpecialColumns();
				return filterCol;
			}
		}

		/// <summary>
		/// Compute index of date column.
		/// </summary>
		public int DateStrColumn
		{
			get
			{
				if (dateStrCol == -1)
					FindSpecialColumns();
				return dateStrCol;
			}
		}

		
		/// <summary>
		/// Get the index of the query ID column. The label is the "QueryId" of each line in the TSV file
		/// </summary>
		public int QueryIdColumn
		{
			get
			{
				if (!lookedForSpecialColumns)
					FindSpecialColumns();
				return queryIdCol;
			}
		}

		/// <summary>
		/// Get the index of the DocId column. The label is the "DocId" of each line in the TSV file
		/// </summary>
		public int DocIdColumn
		{
			get
			{
				if (!lookedForSpecialColumns)
					FindSpecialColumns();
				return docIdCol;
			}
		}

		/// <summary>
		/// Get the index of the url string column. The label is the "Url" of each line in the TSV file
		/// </summary>
		public int URLStrColumn
		{
			get
			{
				if (!lookedForSpecialColumns)
					FindSpecialColumns();
				return urlStrCol;
			}
		}

		/// <summary>
		/// Should we keep track of the meta-data: DocId, Url, QueryId, which are not necessary in normal training and testing
		/// </summary>
		public bool IsKeepMeta
		{
			get 
			{ return keepMeta; 
			}
		}

		/// <summary>
		/// Get or set the header name for the grouping column
		/// </summary>
		public string GroupColumnName
		{
			get { return groupColumnName; }
			set { groupColumnName = value; }
		}
		/// <summary>
		/// Get or set the header name for the label column
		/// </summary>
		public string LabelColumnName
		{
			get { return labelColumnName; }
			set { labelColumnName = value; }
		}

		/// <summary>
		/// Get columnName to pivot the dataset on
		/// </summary>
		public string FilterColumnName
		{
			get { return filterColumnName; }
			set { filterColumnName = value; }
		}

		/// <summary>
		/// 
		/// </summary>
		public string DateStrColumnName
		{
			get { return dateStrColumnName; }
			set { dateStrColumnName = value; }
		}

		/// <summary>
		/// Get the value of the data set name
		/// </summary>
		public string FilterValue
		{
			get { return filterValue; }
			set { filterValue = value; }
		}

		/// <summary>
		/// Get the name of query string column
		/// </summary>
		public string QueryColumnName
		{
			get { return queryColumnName; }
			set { queryColumnName = value; }
		}

		/// <summary>
		/// Get the name of the QueryId Column
		/// </summary>
		public string QueryIdColumnName
		{
			get { return queryIdColumnName; }
			set { queryIdColumnName = value; }
		}

		/// <summary>
		/// Get the name of the DocId Column
		/// </summary>
		public string DocIdColumnName
		{
			get { return docIdColumnName; }
			set { docIdColumnName = value; }
		}

		/// <summary>
		/// Get the name of the URL string column
		/// </summary>
		public string URLStrColumnName
		{
			get { return urlStrColumnName; }
			set { urlStrColumnName = value; }
		}
		
		/// <summary>
		/// Returns the (zero-based) index of the column with the given name.
		/// ColumnNames[GetColumnIndex(X)] = X
		///   Performs case-insensitive comparison
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public int GetColumnIndex(string name)
		{
			for (int i=0; i<columnNames.Length; i++)
				if (columnNames[i].ToLower() == name.ToLower())
					return i;
			return -1;
		}

		// This is fairly MSN-specific, though we could define all TSV files to
		//  have these characteristics.
		// Possibly, provide a struct containing information like an enumerated
		//  column type, the number of bins, etc. instead of having these boolean calls

		/// <summary>
		/// Returns whether column i is a feature column. TSV files have two types of
		/// columns: meta and feature. The meta columns are denoted by "m:" preceding the
		/// column name.
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		public bool IsFeatureColumn(int i)
		{
			return !ColumnNames[i].StartsWith("m:");
		}		

		/// <summary>
		/// Returns whether column i is a binned column. Feature columns may either be
		/// normal or binned. A normal column just contains the floating point value of
		/// its feature. A binned column contains an integer specifying which bin has value
		/// 1, with the remaining bins having value 0. The number of bins is specified in the
		/// column name, which is "bin:numBins:name". Example: "bin:10:TopLevelDomain".
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		public bool IsBinnedColumn(int i)
		{
			return ColumnNames[i].StartsWith("bin:");
		}
		/// <summary>
		/// Gives the number of bins for a binned column
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		public int NumBinsForColumn(int i)
		{
			string[] pieces = ColumnNames[i].Split(':');
			return int.Parse(pieces[1]);
		}

		/// <summary>
		/// Returns whether the given column is a sparse column
		///  a sparse column has the format "sparse:numFeatures:name" such as "sparse:50000:HasWord"
		///  the value of the column is a space separated list of features, from 0 to numFeatures-1.
		///  The feature vector is grown by numFeatures, and each one takes value 0 if not
		///  specified, and 1 otherwise.
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		public bool IsSparseColumn(int i)
		{
			return ColumnNames[i].StartsWith("sparse:");
		}
		/// <summary>
		/// If it is a sparse Column (see IsSparseColumn()), this returns the number
		///  of features the column refers to.
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		public int NumFeaturesForSparseColumn(int i)
		{
			string[] pieces = ColumnNames[i].Split(':');
			return int.Parse(pieces[1]);
		}


		/// <summary>
		/// Compute the indices of the rating and GroupID in the input data file.
		/// </summary>
		private void FindSpecialColumns()
		{
			lookedForSpecialColumns = true;
			labelCol = -1;
			groupCol = -1;

			filterCol = -1;
			queryIdCol = -1;
			queryCol = -1;
			docIdCol = -1;
			urlStrCol = -1;
			dateStrCol = -1;

			for (int i = 0; i < columnNames.Length; i++)
			{
				string featureName = columnNames[i];
				if (string.Compare(featureName, LabelColumnName, true) == 0)
				{
					labelCol = i;
				}
				else if (string.Compare(featureName, GroupColumnName, true) == 0)
				{
					groupCol = i;
				}
				else if (string.Compare(featureName, FilterColumnName, true) == 0 &&
					filterValue != null)
				{
					filterCol = i;
				}
				else if (string.Compare(featureName, DateStrColumnName, true) == 0)
					dateStrCol = i;
//				else if (keepMeta)
//				{
					//don't bother to compute QueryStr, QueryID, URLStr
					if (string.Compare(featureName, QueryColumnName, true) == 0)
					{
						queryCol = i;
					}
					else if (string.Compare(featureName, DocIdColumnName, true) == 0)
					{
						docIdCol = i;
					}
					else if (string.Compare(featureName, URLStrColumnName, true) == 0)
					{
						urlStrCol = i;
					}
//				}
			}
			if (keepMeta)
			{
				queryIdCol = groupCol;
			}
			if (labelCol < 0)
			{
				throw new Exception("Error, couldn't find label column");
			}
			if (groupCol < 0)
			{
				throw new Exception("Error, couldn't find grouping column");
			}
			if (filterValue != null && filterCol < 0)
			{
				throw new Exception("Error, couldn't find filter column");
			}
		}



		#region Enumeration

		/// <summary>
		/// Returns an enumerator that, line by line, returns the entire line.
		/// example: foreach (string line in this)
		/// </summary>
		/// <returns></returns>
		public IEnumerator GetEnumerator()
		{
			return new FieldsEnumeratorClass((LineReader)lineReader.Clone());
		}
		///// <summary>
		///// Returns an enumerator that, line by line, returns the entire line.
		///// example: foreach (string line in this)
		///// </summary>
		///// <returns></returns>
		//public FieldsEnumeratorClass GetEnumerator()
		//{
		//    return new FieldsEnumeratorClass(filename);
		//}

		/// <summary>
		/// Use this enumeration to retrieve, line by line, the set of fields for each line
		/// example: foreach (string[] fields in FieldsEnumeration)
		/// </summary>
		public IEnumerable FieldsEnumeration 
		{
			get { return new FieldsEnumerableClass((LineReader)lineReader.Clone()); }
		}
		/// <summary>
		/// Use this enumeration to retrieve, line by line, the set of float values given
		/// by all the feature columns in the TSV file.
		/// example: foreach (float[] features in FeatureEnumeration)
		/// </summary>
		public IEnumerable FeatureEnumeration
		{
			get { return new FeatureEnumerableClass((LineReader)lineReader.Clone(), ColumnNames); }
		}
		/// <summary>
		/// Use this enumeration to retrieve, group by group, the set of fields for 
		/// all of the lines of the group.
		/// example: foreach (string[][] groupFields in GroupFieldsEnumeration)
		/// </summary>
		public IEnumerable GroupFieldsEnumeration
		{
			get { return new GroupFieldsEnumerableClass((LineReader)lineReader.Clone(), GroupColumn, LabelColumn, QueryColumn, FilterColumn, FilterValue, DateStrColumn); }
		}

		// Enumerate through lines, breaking each into an array of fields
		private class FieldsEnumerableClass : IEnumerable
		{
			private readonly LineReader lineReader;

			public FieldsEnumerableClass(LineReader lineReader)
			{
				this.lineReader = lineReader;
			}
			IEnumerator IEnumerable.GetEnumerator()
			{
				return GetEnumerator();
			}
			public FieldsEnumeratorClass GetEnumerator()
			{
				return new FieldsEnumeratorClass(lineReader);
			}
		}

		private class FieldsEnumeratorClass : IEnumerator, IDisposable
		{
			private readonly LineReader lineReader;
			private string[] currentFields = null;

			public FieldsEnumeratorClass(LineReader lineReader)
			{
				this.lineReader = lineReader;
			}

			#region IDisposable Members

			//~FieldsEnumeratorClass()
			//{
			//    Dispose();
			//}
			public void Dispose()
			{
				if (lineReader.IsOpened)
				{
					try
					{
						lineReader.Close();
					}
					catch
					{
					}
				}
				//GC.SuppressFinalize(this);
			}

			#endregion

			public void Reset()
			{
				lineReader.Reset();
				currentFields = null;
			}

			public virtual object Current
			{
				get
				{
					if (!lineReader.IsOpened || currentFields == null)
					{
						throw new InvalidOperationException();
					}
					return currentFields;
				}
			}

			public bool MoveNext()
			{
				if (!lineReader.IsOpened)
				{
					lineReader.Open();
					//GC.ReRegisterForFinalize(this);
				}
				string line = lineReader.ReadLine();
				if (line == null)
				{
					lineReader.Close();
					currentFields = null;
					//GC.SuppressFinalize(this);
					return false;
				}
				currentFields = line.Split('\t');
				return true;
			}
		}


		// Enumerate through lines, breaking each into an array of float[] for the non-meta features
		class FeatureEnumerableClass : IEnumerable
		{
			private readonly LineReader lineReader;
			private readonly string[] headers;
			public FeatureEnumerableClass(LineReader lineReader, string[] headers)
			{
				this.lineReader = lineReader;
				this.headers = headers;
			}
			IEnumerator IEnumerable.GetEnumerator()
			{
				return new FeatureEnumeratorClass(lineReader, headers);
			}
			public FeatureEnumeratorClass GetEnumerator()
			{
				return new FeatureEnumeratorClass(lineReader, headers);
			}
		}
		class FeatureEnumeratorClass : FieldsEnumeratorClass
		{
			private readonly int[] featureFields;

			public FeatureEnumeratorClass(LineReader lineReader, string[] headers)
				: base(lineReader)
			{
				// Find the headers that don't have "m:" in front of them
				ArrayList featureFieldsList = new ArrayList(headers.Length);
				for (int i = 0; i < headers.Length; i++)
				{
					if (!headers[i].StartsWith("m:"))
					{
						featureFieldsList.Add(i);
					}
				}
				featureFields = (int[])featureFieldsList.ToArray(typeof(int));
			}
			
			public override object Current
			{
				get
				{
					string[] fields = (string[])base.Current;
					float[] ret = new float[featureFields.Length];
					for (int i = 0; i < featureFields.Length; i++)
					{
						ret[i] = float.Parse(fields[featureFields[i]]);
					}
					return ret;
				}
			}
		}


		// Enumerate through queries, breaking each into an array of string[].
		//  that is, it returns a string[][], where each return is all the rows for a give groupid
		private class GroupFieldsEnumerableClass : IEnumerable
		{
			LineReader lineReader;
			int groupIdIndex;
			int labelIndex;
		    int dateIndex;
			int filterIndex;
			string filterValue;
			int queryIndex;
			public GroupFieldsEnumerableClass(LineReader lineReader, int groupIdIndex, int labelIndex, int queryIndex, int filterIndex, string filterValue, int dateIndex)
			{
				this.lineReader = lineReader;
				this.groupIdIndex = groupIdIndex;
				//Todo(qiangwu): define a new interface IFilter which implement one method - IsAccept()
				//to encapsulate the details of the filtering function and decouple it from the enumerator

				this.labelIndex = labelIndex;
				this.queryIndex = queryIndex;
				this.filterIndex = filterIndex;
				this.filterValue = filterValue;
				this.dateIndex = dateIndex;
			}
			public IEnumerator GetEnumerator()
			{
				return new GroupFieldsEnumeratorClass(lineReader, groupIdIndex, labelIndex, queryIndex, filterIndex, filterValue, dateIndex);
			}
		}
		private class GroupFieldsEnumeratorClass : IEnumerator
		{
			private readonly FieldsEnumeratorClass fieldsEnumerator;
			private readonly int groupIdIndex;

			private readonly int labelIndex;
			private readonly int dateIndex;
			private readonly int filterIndex;
			private readonly string filterValue;
			private readonly int queryIndex;

			bool firstMoveNext = true;
			bool lastMoveNext = false;
			string[][] currentRows = null;			

			public GroupFieldsEnumeratorClass(LineReader lineReader, int groupIdIndex, int labelIndex, int queryIndex, int filterIndex, string filterValue, int dateIndex)
			{
				fieldsEnumerator = new FieldsEnumeratorClass(lineReader);
				this.groupIdIndex = groupIdIndex;

				//Todo(qiangwu): define a new interface IFilter which implement one method - IsAccept()
				//to encapsulate the details of the filtering function and decouple it from the enumerator
				this.labelIndex = labelIndex;
				this.filterIndex = filterIndex;
				this.filterValue = filterValue;
				this.dateIndex = dateIndex;
				this.queryIndex = queryIndex;
			}

			#region IEnumerator Members

			public void Reset()
			{
				fieldsEnumerator.Reset();
				firstMoveNext = true;
				lastMoveNext = false;
				currentRows = null;				
			}

			object IEnumerator.Current
			{
				get { return Current; }
			}
			public string[][] Current
			{
				get
				{
					return currentRows;
				}
			}

			private bool ContainQueryStr(string[] fields)
			{
				return (queryIndex >= 0 && fields[queryIndex].Length > 0);
			}

			public bool MoveNext()
			{
				if (lastMoveNext)
				{
					currentRows = null;
					return false;
				}

				if (firstMoveNext)
				{
					bool success = fieldsEnumerator.MoveNext();
					if (!success)
						return false;
					firstMoveNext = false;
				}

				string[] fields = (string[])fieldsEnumerator.Current;
				string currentGroup = fields[groupIdIndex];

				string query = null;
				bool HasQueryStr = false;
				ArrayList currentRowsList = new ArrayList();
				string newGroup;
				do
				{
					if (ContainQueryStr(fields))
					{
						query = fields[queryIndex];
					}
					if (
						//always including original judged urls that are not later filled-holes in any dataset
						(labelIndex >= 0 && fields[labelIndex] != null && fields[labelIndex].Length > 0 && !fields[labelIndex].StartsWith("Filled_")) ||
						//split the unrated urls according to filter value
						filterValue == null || fields[filterIndex] == filterValue 
						)
					{													
						currentRowsList.Add(fieldsEnumerator.Current);						
						if (ContainQueryStr(fields))
						{
							HasQueryStr = true;
						}
					}
					if (!fieldsEnumerator.MoveNext())
					{
						// That was the last row, so this set is the last we'll be returning
						lastMoveNext = true;
						break;
					}
					fields = (string[])fieldsEnumerator.Current;
					newGroup = fields[groupIdIndex];
				} while (string.CompareOrdinal(newGroup, currentGroup) == 0);

				currentRows = (string[][])currentRowsList.ToArray(typeof(string[]));
				if (queryIndex >= 0 && currentRows.Length > 0 && !HasQueryStr)
				{
					//assert query cannot be null	
					if (query == null)
					{
						//Console.WriteLine("Warning: no query string for current group [QueryID =" + currentGroup + "].\n");
						query = "";
					}
					currentRows[0][queryIndex] = query;									
				}
				return true;
			}

			#endregion
		}
		#endregion

	}

}

