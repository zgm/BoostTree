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
	public class RankingTSVFile<D> : IEnumerable
        where D : IData
	{		
		//private readonly string filename;
		private readonly LineReader lineReader = null;
		private readonly string[] columnNames;		
               
        //objects used to segment, parse, store, and process RVT data content
        private IGroupBoundary m_groupBoundary;
        private D m_data;   
 
		/// <summary>
		/// Creates a RankingTSVFile from a file on disk.
		/// Does not read the file into memory. The file is accessed through enumerators
		/// </summary>
		/// <param name="filename"></param>				
		public RankingTSVFile(string filename, D data) :
			this(new SingleLineReader(filename), data)
		{            			
		}
		
		/// <summary>
		/// 
		/// </summary>
		/// <param name="lineReader"></param>
		protected RankingTSVFile(LineReader lineReader, D data) {
			this.lineReader = lineReader;
            
			columnNames = null;						

			// Search has informed us that the .tsv files are encoded in UTF8
			string line = this.lineReader.Headers;
			this.lineReader.Close();

			columnNames = line.Split('\t');

            //set up and initialize the default objects 
            m_groupBoundary = DefaultGroupBoundary();
            m_groupBoundary.ReadColumnNames(ColumnNames);

            m_data = data;	
            m_data.ReadColumnNames(ColumnNames); 
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
		/// Get the number of columns in the TSV file
		/// </summary>
		public int NumCols
		{
			get { return columnNames.Length;}
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
             
        protected IGroupBoundary DefaultGroupBoundary()
        {
            return new QueryBoundary();
        }
        
        // setting new groupbounday implementation to plug in any custom behavior
        public IGroupBoundary GroupBoundary
        {
            set
            {
                if (null != value && value != m_groupBoundary)
                {
                    m_groupBoundary = value;
                    m_groupBoundary.ReadColumnNames(ColumnNames);
                }
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

		/// <summary>
		/// Use this enumeration to retrieve, line by line, the set of fields for each line
		/// example: foreach (string[] fields in FieldsEnumeration)
		/// </summary>
		public IEnumerable FieldsEnumeration 
		{
			get { return new FieldsEnumerableClass((LineReader)lineReader.Clone()); }
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

        /// <summary>
        /// Use this enumeration to retrieve, group by group, the set of fields for 
        /// all of the lines of the group after being parsed and extracted by data and
        /// futher process by dataProc
        /// example: foreach (object groupFieldsData in GroupFieldsEnumeration)
        /// </summary>
        public IEnumerator GetGroupEnumerator() 
        {
            return new GroupFieldsEnumeratorClass((LineReader)lineReader.Clone(), m_data, m_groupBoundary);
        }

        public IEnumerable GroupFieldsEnumeration
        {
            get { return new GroupFieldsEnumerableClass((LineReader)lineReader.Clone(), m_data, m_groupBoundary); }
        }	

		// Enumerate through queries, breaking each into an array of string[].
		// that is, it returns a string[][], where each return is all the rows for a give groupid
		private class GroupFieldsEnumerableClass : IEnumerable
		{
			LineReader lineReader;
            private readonly IGroupBoundary m_groupBoundary;
            private IData m_data;           

            public GroupFieldsEnumerableClass(LineReader lineReader, IData data, IGroupBoundary groupBoundary)
			{
				this.lineReader = lineReader;
                this.m_data = data;               
                this.m_groupBoundary = groupBoundary;
			}
			public IEnumerator GetEnumerator()
			{
                return new GroupFieldsEnumeratorClass(lineReader, m_data, m_groupBoundary);
			}
		}

		private class GroupFieldsEnumeratorClass : IEnumerator
		{
			private readonly FieldsEnumeratorClass fieldsEnumerator;
            private readonly IGroupBoundary m_groupBoundary;
			
			bool firstMoveNext = true;
			bool lastMoveNext = false;			

            private IData m_data;           
            
            public GroupFieldsEnumeratorClass(LineReader lineReader, IData data, IGroupBoundary groupBoundary)
			{
				fieldsEnumerator = new FieldsEnumeratorClass(lineReader);                
                this.m_data = data;                
                this.m_groupBoundary = groupBoundary;
			}

			#region IEnumerator Members

			public void Reset()
			{
				fieldsEnumerator.Reset();
				firstMoveNext = true;
				lastMoveNext = false;
                m_data.Reset();                
			}

			object IEnumerator.Current
			{
                get 
                {                     
                    return m_data;
                }
			}			
			
			public bool MoveNext()
			{                                
				if (lastMoveNext)
				{
                    m_data.Reset();
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
                m_groupBoundary.FirstItem(fields);                							
                
                do
                {                   
                    m_data.ProcessLine(fields);  
                    if (!fieldsEnumerator.MoveNext())
                    {
                        // That was the last row, so this set is the last we'll be returning
                        lastMoveNext = true;                                                
                        break;
                    }
                    fields = (string[])fieldsEnumerator.Current;
                } while (!m_groupBoundary.NewGroup(fields));

                m_data.Complete();
				
				return true;
			}

			#endregion
		}


		#endregion

        #region LineReader
        /// <summary>
        /// A class which abstracts reading lines from a tsv file.
        /// </summary>
        protected abstract class LineReader : ICloneable
        {
            /// <summary>
            /// resets the line reader to its beginning
            /// </summary>
            public void Reset()
            {
                Close();
            }
            /// <summary>
            /// returns the headers of this tsv file
            /// </summary>
            public abstract string Headers
            {
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
            public abstract bool IsOpened
            {
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
        protected class SingleLineReader : LineReader
        {
            private string filename;
            private string headers;
            private StreamReader reader = null;

            /// <summary>
            /// constructor
            /// </summary>
            /// <param name="filename">the tsv file to be read</param>
            public SingleLineReader(string filename)
            {
                this.filename = filename;
            }
            /// <summary>
            /// closes the line reader
            /// </summary>
            public override void Close()
            {
                if (IsOpened)
                {
                    reader.Close();
                }
                reader = null;
                headers = null;
            }
            /// <summary>
            /// returns true if the line reader is open
            /// </summary>
            public override bool IsOpened
            {
                get
                {
                    return (reader != null);
                }
            }
            /// <summary>
            /// opens the line reader
            /// </summary>
            public override void Open()
            {
                reader = ZStreamReader.Open(filename); //new StreamReader(filename);
                headers = reader.ReadLine();
            }
            /// <summary>
            /// reads a line from the tsv file
            /// </summary>
            /// <returns></returns>
            public override string ReadLine()
            {
                return reader.ReadLine();
            }

            /// <summary>
            /// returns the headers of the tsv file
            /// </summary>
            public override string Headers
            {
                get
                {
                    if (reader == null) Open();
                    return headers;
                }
            }

            /// <summary>
            /// returns a copy of the line reader
            /// </summary>
            /// <returns></returns>
            public override object Clone()
            {
                SingleLineReader slr = new SingleLineReader(this.filename);
                return slr;
            }
        }
        #endregion
    }
        
}

