// owner: rragno

using System;
using System.IO;
using System.Collections;
using System.Collections.Specialized;
using System.Text;
#if !ENABLE_BARTOK
using System.Xml;
#endif
#if ALLOW_DB
using System.Data;
using System.Data.OleDb;
#endif


namespace Microsoft.TMSN.IO
{
	//// TODO: consider unifying all formats, syncing with other data access models
	//// TODO: simple XML support
	//// TODO: consider generalizing header support; changing from "row,item" to "item,field";
	////       making a base TableReader abstract class.

	/// <summary>
	/// Process tabular data.
	/// </summary>
	public interface   ITableProcessor 
	{
		/// <summary>
		/// Gets or sets whether to trim whitespace from each field.
		/// </summary>
		bool         TrimWhitespace  { get; set; }
		
		/// <summary>
		/// Gets or sets whether to ignore case when matching header names.
		/// </summary>
		bool         IgnoreHeaderCase  { get; set; }

		/// <summary>
		/// Gets or sets the header names.
		/// </summary>
		string[]     Headers { get; set; }

		/// <summary>
		/// Check for end of file.
		/// </summary>
		/// <returns>true if at end of file, false otherwise</returns>
		bool         Eof();

		/// <summary>
		/// Advance to the next row.
		/// </summary>
		void         NextRow();

		/// <summary>
		/// Close the table.
		/// </summary>
		void         Close();
	}

	/// <summary>
	/// 
	/// </summary>
	public interface ITableRow
	{
		/// <summary>
		/// Get the field at the column index.
		/// </summary>
		string   this[int index]  { get; }

		/// <summary>
		/// Get the field at the column with the given header.
		/// </summary>
		string   this[string header]  { get; }
	}

	/// <summary>
	/// Read tabular data.
	/// </summary>
	public interface   ITableReader : ITableProcessor, IEnumerable, ITableRow
	{

		/// <summary>
		/// Gets or sets whether to return "", not null, when the end of a row is reached,
		/// until the row is advanced:
		/// </summary>
		bool         FillBlankColumns  { get; set; }

		/// <summary>
		/// Check for end of row.
		/// </summary>
		/// <returns>true if at end of row, false otherwise</returns>
		bool         RowEnd();

		/// <summary>
		/// Get the next field and advance the reader.
		/// </summary>
		/// <returns>the field at the next column</returns>
		string       ReadItem();
		
		/// <summary>
		/// Get the next field and advance the reader, filling with empty fields at the end of the row.
		/// </summary>
		/// <returns>the field at the next column</returns>
		string       ReadItemLinear();

		/// <summary>
		/// Get the number of fields in the current row.
		/// </summary>
		/// <returns>the number of fields in the current row</returns>
		int          RowLength();

		/// <summary>
		/// Read an entire row and advance the reader.
		/// </summary>
		/// <returns>The current row as an array of fields</returns>
		string[]     ReadRow();

		/// <summary>
		/// Read an entire row and advance the reader.
		/// </summary>
		/// <param name="len">The length of the row to read, truncating or filling with empty fields as needed</param>
		/// <returns>The current row as an array of fields</returns>
		string[]     ReadRow(int len);

		/*
		/// <summary>
		/// Get the field at the column index.
		/// </summary>
		string   this[int index]  { get; }

		/// <summary>
		/// Get the field at the column with the given header.
		/// </summary>
		string   this[string header]  { get; }
		*/

		/// <summary>
		/// Reset the reader to the beginning.
		/// </summary>
		void         Reset();
	}


	/// <summary>
	/// Write tabular data.
	/// </summary>
	public interface   ITableWriter : ITableProcessor
	{
		/// <summary>
		/// Write the next field and advance the writer.
		/// </summary>
		/// <param name="item">the field to write</param>
		void         WriteItem(string item);
		
		/// <summary>
		/// Write an entire row and advance the writer.
		/// </summary>
		/// <param name="items">The row to write as an array of fields</param>
		void         WriteRow(string[] items);

		/// <summary>
		/// Write an entire row and advance the writer.
		/// </summary>
		/// <param name="len">The length of the row to write, truncating or filling with empty fields as needed</param>
		/// <param name="items">The row to write as an array of fields</param>
		void         WriteRow(string[] items, int len);

		/// <summary>
		/// Set the field at the column index.
		/// </summary>
		string   this[int index]  { set; }

		/// <summary>
		/// Set the field at the column with the given header.
		/// </summary>
		string   this[string header]  { set; }
	}


	/// <summary>
	/// Enumerator to read through the rows in a table.
	/// </summary>
	public class TableEnumerator : IEnumerator
	{
		ITableReader reader;

		#region IEnumerator Members
		/// <summary>
		/// Create a new enumerator to read through the table rows
		/// </summary>
		/// <param name="reader">the table to read lines from</param>
		public TableEnumerator(ITableReader reader)
		{
			this.reader = reader;
		}
		/// <summary>
		/// Return the enumerator to the initial state.
		/// </summary>
		public void Reset()
		{
			reader.Reset();
		}
		/// <summary>
		/// Get the current row of the table.
		/// </summary>
		public ITableRow Current
		{
			get
			{
				return (ITableRow)reader;
			}
		}

		/// <summary>
		/// Get the current row of the table.
		/// </summary>
		object IEnumerator.Current
		{
			get
			{
				return ((TableEnumerator)this).Current;
			}
		}

		/// <summary>
		/// Move the enumerator to the next row.
		/// </summary>
		/// <returns>true if the next row exists, or false if at the end of the table</returns>
		public bool MoveNext()
		{
			reader.NextRow();
			return !reader.Eof();
		}

		#endregion
	}




	//////////////////////////////////////////////
	////// CSV support
	//////////////////////////////////////////////


	/// <summary>
	/// Read TSV formatted data.
	/// </summary>
	public class   TsvReader : CsvReader
	{
		private void Configure()
		{
			this.Delimiter = "\t";
			this.DelimiterSet = false;
			this.ParseQuotes = false;
			this.ReadHeaders = true;
			this.SkipBlankColumnsLines = true;
			this.SkipBlankLines = true;
			// should this one be set? ***
			this.IgnoreHeaderCase = true;
		}

		/// <summary>
		/// Create a TsvReader based on the TextReader,
		/// </summary>
		/// <param name="tr">the TextReader to read the table from</param>
		public   TsvReader(TextReader tr)
			: base(tr)
		{
			Configure();
		}

		/// <summary>
		/// Create a TsvReader based on the specified file,
		/// </summary>
		/// <param name="fname">the name of the file to read the table from</param>
		public   TsvReader(string fname)
			: base(fname)
		{
			Configure();
		}

		/// <summary>
		/// Create a TsvReader based on the Stream,
		/// </summary>
		/// <param name="fstream">the Stream to read the table from</param>
		public   TsvReader(Stream fstream)
			: base(fstream)
		{
			Configure();
		}

		/// <summary>
		/// Create a TsvReader based on the specified file,
		/// </summary>
		/// <param name="fname">the name of the file to read the table from</param>
		/// <param name="encoding">the encoding to use to interpet the file</param>
		public   TsvReader(string fname, Encoding encoding)
			: base(fname, encoding)
		{
			Configure();
		}

		/// <summary>
		/// Create a TsvReader based on the Stream,
		/// </summary>
		/// <param name="fstream">the Stream to read the table from</param>
		/// <param name="encoding">the encoding to use to interpet the Stream</param>
		public   TsvReader(Stream fstream, Encoding encoding)
			: base(fstream, encoding)
		{
			Configure();
		}
	}


	/// <summary>
	/// Tab-Separated Value writer.
	/// </summary>
	public class   TsvWriter : CsvWriter
	{
		private void Configure()
		{
			this.Delimiter = "\t";
			this.ParseQuotes = false;
			this.SkipBlankLines = true;
			this.EndInNewline = true;
			// should this one be set? ***
			this.IgnoreHeaderCase = true;
		}

		/// <summary>
		/// Create a TsvWriter based on the TextWriter,
		/// </summary>
		/// <param name="tr">the TextWriter to write the table to</param>
		public TsvWriter(TextWriter tr)
			: base(tr)
		{
			Configure();
		}

		/// <summary>
		/// Create a TsvWriter based on the specified file,
		/// </summary>
		/// <param name="fname">the name of the file to write the table to</param>
		public TsvWriter(string fname)
			: base(fname)
		{
			Configure();
		}

		/// <summary>
		/// Create a TsvWriter based on the Stream,
		/// </summary>
		/// <param name="fstream">the Stream to write the table to</param>
		public TsvWriter(Stream fstream)
			: base(fstream)
		{
			Configure();
		}

		/// <summary>
		/// Create a TsvWriter based on the specified file,
		/// </summary>
		/// <param name="fname">the name of the file to write the table to</param>
		/// <param name="encoding">the encoding to use</param>
		public TsvWriter(string fname, Encoding encoding)
			: base(fname, encoding)
		{
			Configure();
		}

		/// <summary>
		/// Create a TsvWriter based on the Stream,
		/// </summary>
		/// <param name="fstream">the Stream to write the table to</param>
		/// <param name="encoding">the encoding to use</param>
		public TsvWriter(Stream fstream, Encoding encoding)
			: base(fstream, encoding)
		{
			Configure();
		}
	}

	

	/// <summary>
	/// Read CSV formatted data.
	/// </summary>
	public class   CsvReader : ITableReader, IDisposable
	{
		// trim whitespace from each entry:
		private bool trimWhitespace = true;
		// skip lines that are only whitespace:
		private bool skipBlankLines = true;
		// skip lines that have delimeters but only whitespace otherwise:
		private bool skipBlankColumnsLines = true;
		// return "", not null, when the end of a row is reached, until the row is advanced:
		private bool fillBlankColumns = true;
		// treat repeated delimiters as a single delimiter:
		private bool collapseDelimiters = false;
		// treat delimiter string as a set of delimiter characters:
		private bool delimiterSet = false;
		// read the first line as header names for the columns
		private bool readHeaders = false;
		// the names of the headers
		private string[] headers = null;
		private string[] headersNormalized = null;
		private bool ignoreHeaderCase = true;
		private bool initialized = false;
		// use the quoteChar to determine quoted sections:
		private bool parseQuotes = true;
		private string quoteChar = "\"";
		private string delimiter = ",";

		private TextReader file;
		private string curLine;
		private long rowNumber = 0;  // gives 1-based rows, like excel...

		// support 1-row read-ahead:
		private StringCollection curRow;
		private string[] curRowArray;
		private int      curCol;


		/// <summary>
		/// Create a CsvReader based on the TextReader,
		/// </summary>
		/// <param name="tr">the TextReader to read the table from</param>
		public   CsvReader(TextReader tr)
		{
			file = tr;
			//if (file == null)
			//	Console.Out.WriteLine("  Error: CsvReader could not open null file!");
		}

		/// <summary>
		/// Create a CsvReader based on the specified file,
		/// </summary>
		/// <param name="fname">the name of the file to read the table from</param>
		public   CsvReader(string fname)
			: this(ZStreamReader.Open(fname))
//			: this(new StreamReader(fname, Encoding.UTF8, true))
		{
		}

		/// <summary>
		/// Create a CsvReader based on the Stream,
		/// </summary>
		/// <param name="fstream">the Stream to read the table from</param>
		public   CsvReader(Stream fstream)
			: this(new StreamReader(fstream))
		{
		}

		/// <summary>
		/// Create a CsvReader based on the specified file,
		/// </summary>
		/// <param name="fname">the name of the file to read the table from</param>
		/// <param name="encoding">the encoding to use to interpet the file</param>
		public   CsvReader(string fname, Encoding encoding)
//			: this(ZStreamReader.Open(fname, encoding))
			: this(new StreamReader(fname, encoding, true))
		{
		}

		/// <summary>
		/// Create a CsvReader based on the Stream,
		/// </summary>
		/// <param name="fstream">the Stream to read the table from</param>
		/// <param name="encoding">the encoding to use to interpet the Stream</param>
		public   CsvReader(Stream fstream, Encoding encoding)
			: this(new StreamReader(fstream, encoding, true))
		{
		}

		
		/// <summary>
		/// Gets or sets whether to trim whitespace from each field.
		/// </summary>
		public bool TrimWhitespace
		{
			get { return trimWhitespace; }
			set { trimWhitespace = value; }
		}
		/// <summary>
		/// Get or set whether to skip blank lines.
		/// </summary>
		public bool SkipBlankLines
		{
			get { return skipBlankLines; }
			set { skipBlankLines = value; }
		}
		/// <summary>
		/// Get or set whether to skip lines with all fields empty.
		/// </summary>
		public bool SkipBlankColumnsLines
		{
			get { return skipBlankColumnsLines; }
			set { skipBlankColumnsLines = value; }
		}
		/// <summary>
		/// Gets or sets whether to return "", not null, when the end of a row is reached,
		/// until the row is advanced:
		/// </summary>
		public bool FillBlankColumns
		{
			get { return fillBlankColumns; }
			set { fillBlankColumns = value; }
		}
		/// <summary>
		/// Get or set whether to respect quotes when parsing
		/// </summary>
		public bool ParseQuotes
		{
			get { return quoteChar != null && quoteChar.Length > 0 && parseQuotes; }
			set { parseQuotes = value; }
		}
		/// <summary>
		/// Get or set the string to use for a quote symbol
		/// </summary>
		public string QuoteChar
		{
			get { return quoteChar; }
			set { quoteChar = value; }
		}
		/// <summary>
		/// Get or set the column delimiter string.
		/// </summary>
		public string Delimiter
		{
			get { return delimiter; }
			set { delimiter = value; }
		}
		/// <summary>
		/// Get or set whether to collapse consecutive delimiters.
		/// </summary>
		public bool CollapseDelimiters
		{
			get { return collapseDelimiters; }
			set { collapseDelimiters = value; }
		}
		/// <summary>
		/// Get or set whether to treate the delimiter string as a set of characters.
		/// </summary>
		public bool DelimiterSet
		{
			get { return delimiterSet; }
			set { delimiterSet = value; }
		}
		/// <summary>
		/// Get or set whether to read the headers from the first line of the input.
		/// </summary>
		public bool ReadHeaders
		{
			get { return readHeaders; }
			set { readHeaders = value; }
		}
		/// <summary>
		/// Gets or sets whether to ignore case when matching header names.
		/// </summary>
		public bool IgnoreHeaderCase
		{
			get { return ignoreHeaderCase; }
			set
			{
				if (ignoreHeaderCase != value)
				{
					ignoreHeaderCase = value;
					FixupNormalizedHeaders();
				}
			}
		}
		/// <summary>
		/// Gets or sets the header names.
		/// </summary>
		public string[] Headers
		{
			get
			{
				Initialize();
				return headers;
			}
			set
			{
				headers = value;
				FixupNormalizedHeaders();
			}
		}
		/// <summary>
		/// Get the number of the current row.
		/// </summary>
		public long RowNumber
		{
			get { return rowNumber; }
		}

		/// <summary>
		/// Check for end of file.
		/// </summary>
		/// <returns>true if at end of file, false otherwise</returns>
		public bool Eof()
		{
			// a little tricky when finishing the last row with remaining whitespace...
			// won't show as EOF until after nextRow is called...
			if (file == null)  return true;
			if (file.Peek() != -1)  return false;
			if (!initialized)  Initialize();
			return RowEnd();
		}

		/// <summary>
		/// Check for end of row.
		/// </summary>
		/// <returns>true if at end of row, false otherwise</returns>
		public bool RowEnd()
		{
			return ( (curRow == null || curCol >= curRow.Count) &&
				(curRowArray == null || curCol >= curRowArray.Length) );
		}

		private void Initialize()
		{
			if (initialized)  return;
			initialized = true;
			NextRow();
			// read and store the column header names:
			if (ReadHeaders)
			{
				// always skip blanks when reading headers
				bool givenSkipBlankLines = SkipBlankLines;
				bool givenSkipBlankColumnsLines = SkipBlankColumnsLines;
				SkipBlankLines = true;
				SkipBlankColumnsLines = true;
				Headers = ReadRow();
				SkipBlankLines = givenSkipBlankLines;
				SkipBlankColumnsLines = givenSkipBlankColumnsLines;
			}
		}

		private void FixupNormalizedHeaders()
		{
			if (headers == null)
			{
				headersNormalized = null;
				return;
			}
			headersNormalized = new string[headers.Length];
			for (int i = 0; i < headers.Length; i++)
			{
				string header = headers[i];
				if (header == null)  continue;
				header = header.Trim();
				if (IgnoreHeaderCase)  header = header.ToLower();
				headersNormalized[i] = header;
			}
		}

		/// <summary>
		/// Get the field at the column index.
		/// </summary>
		public string this[int index]
		{
			get
			{
				if (!initialized)  Initialize();
				if (curRow == null)
				{
					// look in the string[], instead...
					if (curRowArray == null)  return null;
					if (index < 0 || index >= curRowArray.Length)
					{
						if (fillBlankColumns)  return "";
						return null;
					}
					return curRowArray[index];
				}
				if (index < 0 || index >= curRow.Count)
				{
					if (fillBlankColumns)  return "";
					return null;
				}
				return curRow[index];
			}
		}

		/// <summary>
		/// Get the field at the column with the given header.
		/// </summary>
		public string this[string header]
		{
			get
			{
				if (!initialized)  Initialize();
				if (header == null || headers == null) return null;
				header = header.Trim();
				if (IgnoreHeaderCase)  header = header.ToLower();
				for (int i = 0; i < headers.Length; i++)
				{
					if (header == headersNormalized[i])
					{
						return this[i];
					}
				}
				return null;
			}
		}

		/// <summary>
		/// Get the index of the column which has the given header.
		/// </summary>
		public int GetColumnIndex(string header) 
		{
			if (!initialized)  Initialize();
			if (header == null || headers == null) return -1;
			header = header.Trim();
			if (IgnoreHeaderCase)  header = header.ToLower();
			for (int i = 0; i < headers.Length; i++)
			{
				if (header == headersNormalized[i])
				{
					return i;
				}
			}
			return -1;
		}

		/// <summary>
		/// Get the next field and advance the reader.
		/// </summary>
		/// <returns>the field at the next column</returns>
		public string ReadItem()
		{
			// check if we need to setup the reading:
			if (!initialized)  Initialize();

			//// Should actually return a blank entry for the *last* blank entry and null beyond ***
			if (RowEnd())
			{
				if (fillBlankColumns)
					return "";
				else
					return null;
			}

			string res = curRow != null ? curRow[curCol] : curRowArray[curCol];
			curCol++;
			return res;
		}

		// should optimize for 1-character quotes and delimiters...
		private string ReadItemFromLine(ref int curPos)
		{
			if (curPos >= curLine.Length)  return null;

			string resStr;
			curPos++;
			// check for empty case:
			bool empty = (curPos >= curLine.Length);
			if (!empty)
			{
				// optimized check for empty in middle of line
				if (delimiterSet)  // look for *any* of the characters
				{
					if (curLine.Length - curPos >= 1 &&
						delimiter.IndexOf(curLine[curPos]) >= 0)
					{
						// leave cursor on the delimiter
						empty = true;
					}
				}
				else
				{
					if (curLine.Length - curPos >= delimiter.Length &&
						string.CompareOrdinal(curLine, curPos, delimiter, 0, delimiter.Length) == 0)
					{
						// leave cursor on end of the delimiter:
						curPos += delimiter.Length - 1;
						empty = true;
					}
				}
			}
			if (empty)
			{
				resStr = "";
			}
			else
			{
				// check for unquoted case:
				int nextDelimiterIndex;
				if (delimiterSet)
				{
					// should optimize this:
					nextDelimiterIndex = curLine.IndexOfAny(delimiter.ToCharArray(), curPos);
				}
				else
				{
					nextDelimiterIndex = curLine.IndexOf(delimiter, curPos);
				}
				if (nextDelimiterIndex < 0)
				{
					nextDelimiterIndex = curLine.Length;
				}
				if (!parseQuotes || curLine.IndexOf(quoteChar, curPos, nextDelimiterIndex - curPos) < 0)
				{
					// simple case! no quote characters...
					resStr = curLine.Substring(curPos, nextDelimiterIndex - curPos);
					curPos = nextDelimiterIndex;
				}
				else
				{
					StringBuilder res = new StringBuilder();
					for (bool inQuoted = false; true; curPos++)
					{
						// check for end of line:
						if (curPos >= curLine.Length)
						{
							if (!inQuoted)
							{
								// done with item!
								break;
							}
							else
							{
								// uh oh... a quoted newline...
								if (file == null)  break;
								if (file.Peek() == -1)  break;
								// read a new row...
								// rowNumber++; -- don't count it as a new row.
								curLine = file.ReadLine();
								curPos = -1;
								res.Append('\n');
								continue;
							}
						}

						// check for delimiters:
						if (!inQuoted)
						{
							if (delimiterSet)  // look for *any* of the characters
							{
								if (curLine.Length - curPos >= 1 &&
									delimiter.IndexOf(curLine[curPos]) >= 0)
								{
									// leave cursor on the delimiter
									break;
								}
							}
							else
							{
								if (curLine.Length - curPos >= delimiter.Length &&
									string.CompareOrdinal(curLine, curPos, delimiter, 0, delimiter.Length) == 0)
								{
									// leave cursor on end of the delimiter:
									curPos += delimiter.Length - 1;
									break;
								}
							}
						}
						// hard to say how to best handle quotes... Want to be lenient.
						if (parseQuotes &&
							(curLine.Length - curPos >= quoteChar.Length &&
							string.CompareOrdinal(curLine, curPos, quoteChar, 0, quoteChar.Length) == 0))
						{
							// double quote characters mean a literal quote character
							// - but only when in quoted mode!
							if (inQuoted &&
								curLine.Length - curPos >= 2*quoteChar.Length && 
								string.CompareOrdinal(curLine, curPos + quoteChar.Length, quoteChar, 0, quoteChar.Length) == 0)
							{
								res.Append(quoteChar);
								curPos += 2*quoteChar.Length - 1;
							}
							else
							{
								inQuoted = !inQuoted;
								curPos += quoteChar.Length - 1;
							}
							continue;
						}
						res.Append(curLine[curPos]);
					}
					resStr = res.ToString();
				}
				if (trimWhitespace)
				{
					// not perfect! ***
					if (resStr.Length > 0 && (resStr[0] == ' ' || resStr[resStr.Length-1] == ' '))
					{
						resStr = resStr.Trim();
					}
				}
			}
			// if we are collapsing repeated delimiters, we can do this by skipping blank elements...
			if (collapseDelimiters && resStr.Length == 0)
			{
				return ReadItemFromLine(ref curPos);
			}
			//Console.Out.WriteLine("   Item:  " + resStr);
			return resStr;
		}

		/// <summary>
		/// Get the next field and advance the reader, filling with empty fields at the end of the row.
		/// </summary>
		/// <returns>the field at the next column</returns>
		public string ReadItemLinear()
		{
			if (!initialized)  Initialize();

			if (RowEnd())
				NextRow();
			return ReadItem();
		}

		/// <summary>
		/// Get the number of fields in the current row.
		/// </summary>
		/// <returns>the number of fields in the current row</returns>
		public int RowLength()
		{
			if (!initialized)  Initialize();

			if (curRow == null)
			{
				if (curRowArray == null)  return 0;
				return curRowArray.Length;
			}
			return curRow.Count;
		}

		// test if all entries in current row are blank
		private bool RowBlank()
		{
			if (!initialized)  Initialize();

			if (curRow == null)
			{
				if (curRowArray == null)  return true;
				for (int i = 0; i < curRowArray.Length; i++)
				{
					string field = curRowArray[i];
					if (field.Length != 0)
					{
						if (trimWhitespace &&
							(field[0] == ' ' || field[field.Length-1] == ' '))
						{
							if (field.Trim().Length != 0)  return false;
						}
						else
						{
							return false;
						}
					}
				}
				return true;
			}
			for (int i = 0; i < curRow.Count; i++)
			{
				string field = curRow[i];
				if (field.Length != 0)
				{
					if (trimWhitespace &&
						(field[0] == ' ' || field[field.Length-1] == ' '))
					{
						if (field.Trim().Length != 0)  return false;
					}
					else
					{
						return false;
					}
				}
			}
			return true;
		}

		/// <summary>
		/// Read an entire row and advance the reader.
		/// </summary>
		/// <returns>The current row as an array of fields</returns>
		public string[] ReadRow()
		{
			return ReadRow(-1);
		}

		/// <summary>
		/// Read an entire row and advance the reader.
		/// </summary>
		/// <param name="len">The length of the row to read, truncating or filling with empty fields as needed</param>
		/// <returns>The current row as an array of fields</returns>
		public string[] ReadRow(int len)
		{
			// check if we need to setup the reading:
			if (!initialized)
			{
				Initialize();
			}
			if (Eof())  return null;
			if (curRow == null && curRowArray == null)
			{
				NextRow();
			}
			if (curRow == null && curRowArray == null)  return null;

			if (!parseQuotes && (delimiterSet || delimiter.Length == 1) && curRow == null)
			{
				readRowResult = curRowArray;
				NextRow();
				return readRowResult;
			}

			if (curRow == null)
			{
				if (len < 0 || len == curRowArray.Length)
				{
					readRowResult = curRowArray;
				}
				else
				{
					if (readRowResult.Length != len)  readRowResult = new string[len];
					if (len < curRowArray.Length)
					{
#if ENABLE_BARTOK
						for (int i = 0; i < readRowResult.Length; i++)
						{
							readRowResult[i] = curRowArray[i];
						}
#else
						Array.Copy(curRowArray, readRowResult, len);
#endif
					}
					else
					{
						curRowArray.CopyTo(readRowResult, 0);
						for (int i = curRowArray.Length; i < readRowResult.Length; i++)
						{
							readRowResult[i] = "";
						}
					}
				}
				NextRow();
				return readRowResult;
			}
			// always make a new array, for now...
			readRowResult = new string[len < 0 ? curRow.Count : len];
			if (len < 0 || len == curRow.Count)
			{
				if (readRowResult.Length != curRow.Count)  readRowResult = new string[curRow.Count]; 
				curRow.CopyTo(readRowResult, 0);
			}
			else
			{
				if (readRowResult.Length != len)  readRowResult = new string[len]; 
				if (len < curRow.Count)
				{
					for (int i = 0; i < readRowResult.Length; i++)
					{
						readRowResult[i] = curRow[i];
					}
				}
				else
				{
					curRow.CopyTo(readRowResult, 0);
					for (int i = curRow.Count; i < readRowResult.Length; i++)
					{
						readRowResult[i] = "";
					}
				}
			}
			NextRow();
			return readRowResult;
		}
		string[] readRowResult = new string[0];

		// only reads the *remaining* items...
		// also advances the row!
		private void  ReadRowFromLine()
		{
			curRow = null;
			curRowArray = null;
			if (curLine == null)  return;
			if (file == null)  return;

			if (parseQuotes || !(delimiterSet || delimiter.Length == 1))
			{
				curRow = new StringCollection();
				int linePos = -1;
				while (linePos < curLine.Length-1)
				{
					string field = ReadItemFromLine(ref linePos);
					if (field != null)
					{
						curRow.Add(field);
					}
				}
			}
			else
			{
				// we just split, now...
				if (delimiter.Length == 1)
				{
					curRowArray = curLine.Split(delimiter[0]);
				}
				else
				{
					curRowArray = curLine.Split(delimiter.ToCharArray());
				}
			}
			curLine = null;
			curCol = 0;
		}

		/// <summary>
		/// Advance to the next row.
		/// </summary>
		public void NextRow()
		{
			if (file == null)
				return;
			if (!initialized)
			{
				Initialize();
				return;
			}
			curRow = null;
			curRowArray = null;
			curCol = 0;
			while (true)
			{
				rowNumber++;
				curLine = file.ReadLine();
				if (curLine == null)
				{
					return;
				}
				if (skipBlankLines &&
					(curLine.Length == 0 || 
					(curLine[0] == ' ' && curLine.Trim().Length == 0)))
				{
					//Console.Out.WriteLine("(Skipping row as blank line...)");
					continue;
				}
				ReadRowFromLine();
				if (skipBlankColumnsLines && RowBlank())
				{
					//Console.Out.WriteLine("(Skipping row with blank columns...)");
					curRow = null;
					curRowArray = null;
					continue;
				}
				break;
			}

			//ReadRowFromLine();
		}

		



		/// <summary>
		/// Close the table.
		/// </summary>
		public void Close()
		{
			if (file == null)
				return;
			file.Close();
			curCol = -1;
			curLine = null;
			curRow = null;
			curRowArray = null;
			file = null;
		}

		/// <summary>
		/// Reset the reader to the beginning.
		/// </summary>
		/// <exception cref="InvalidOperationException">The reader is not based on a Stream.</exception>
		public void Reset()
		{
			if (file == null)  return;
			if (!(file is StreamReader))
			{
				throw new InvalidOperationException("Cannot reset CsvReader not based on a Stream.");
			}
			((StreamReader)file).BaseStream.Seek(0, SeekOrigin.Begin);
			((StreamReader)file).DiscardBufferedData();
			initialized = false;
			rowNumber = 0;
			curCol = -1;
			curLine = null;
			curRow = null;
			curRowArray = null;
		}
	
		/// <summary>
		/// Allows for random access into the file.  The user is responsible
		/// to make sure that position is at the beginning of a row.
		/// </summary>
		/// <param name="position">new position</param>
		/// <param name="origin">relative to what</param>
		/// <exception cref="InvalidOperationException">The reader is not based on a Stream.</exception>
		public void Seek(long position, SeekOrigin origin) 
		{
			if (file == null) return;
			if (!(file is StreamReader))
			{
				throw new InvalidOperationException("Cannot seek reader not based on a Stream.");
			}

			((StreamReader)file).BaseStream.Seek(position, origin);
			if (position == 0) {
				initialized = false;
			}
			curCol = -1;
			curLine = null;
			curRow = null;
			curRowArray = null;
		}
	
		/// <summary>
		/// Returns position of the cursor in the file
		/// </summary>
		/// <returns>byte offset of the current position</returns>
		/// <exception cref="InvalidOperationException">The reader is not based on a Stream.</exception>
		public long Position()
		{
			if (file == null) return 0;
			if (!(file is StreamReader))
			{
				throw new InvalidOperationException("Cannot tell position for reader not based on a Stream.");
			}
			return ((StreamReader)file).BaseStream.Position;
		}

		#region IDisposable Members

		/// <summary>
		/// Dispose.
		/// </summary>
		public void Dispose()
		{
			Close();
		}

		#endregion
		/// <summary>
		/// Return the enumerator
		/// </summary>
		/// <returns>an enumerator for the rows in this table</returns>
		public IEnumerator GetEnumerator()
		{
			return new TableEnumerator(this);
		}

	}

	
	
	//// TODO: support reorder list
	//// TODO: support filtering (ignoring) columns
	/// <summary>
	/// Write CSV formatted data.
	/// </summary>
	public class   CsvWriter : ITableWriter, IDisposable
	{
		// trim whitespace from each entry:
		private bool trimWhitespace = true;
		// skip lines that are only whitespace:
		private bool skipBlankLines = true;
		// use the quoteChar to determine quoted sections:
		private bool parseQuotes = true;
		// always end in a newline:
		private bool endInNewline = true;
		// transform tab, carriage return, and newline into space:
		private bool normalizeWhitespace = false;
		// characters to use if normalizing whitespace:
		private char[] whitespaceChars = new char[] {'\r', '\n', '\t'};

		// support for addressing by column headers:
		private string[] headers = null;
		private string[] headersNormalized = null;
		private bool     ignoreHeaderCase = true;
		private bool     initialized = false;
		private StringCollection curRow = null;
		private bool writeHeaders = true;

		private string quoteChar = "\"";
		private string delimiter = ",";
		private char[] delimiterOrQuoteOrNewline = new char[] {',', '"', '\r', '\n'};

		private bool lineStart = true;
		private long rowNumber;

		private TextWriter file;


		/// <summary>
		/// Create a CsvWriter based on the TextWriter,
		/// </summary>
		/// <param name="tr">the TextWriter to write the table to</param>
		public CsvWriter(TextWriter tr)
		{
			file = tr;
			//if (file == null)
			//	Console.Out.WriteLine("  Error: CsvReader could not open null file!");
			rowNumber = 1;
		}

		/// <summary>
		/// Create a CsvWriter based on the specified file,
		/// </summary>
		/// <param name="fname">the name of the file to write the table to</param>
		public CsvWriter(string fname)
			: this(ZStreamWriter.Open(fname))
		{
		}

		/// <summary>
		/// Create a CsvWriter based on the Stream,
		/// </summary>
		/// <param name="fstream">the Stream to write the table to</param>
		public CsvWriter(Stream fstream)
			: this(new StreamWriter(fstream))
		{
		}

		/// <summary>
		/// Create a CsvWriter based on the specified file,
		/// </summary>
		/// <param name="fname">the name of the file to write the table to</param>
		/// <param name="encoding">the encoding to use</param>
		public CsvWriter(string fname, Encoding encoding)
			: this(new StreamWriter(fname, false, encoding))
		{
		}

		/// <summary>
		/// Create a CsvWriter based on the Stream,
		/// </summary>
		/// <param name="fstream">the Stream to write the table to</param>
		/// <param name="encoding">the encoding to use</param>
		public CsvWriter(Stream fstream, Encoding encoding)
			: this(new StreamWriter(fstream, encoding))
		{
		}


		/// <summary>
		/// Get or set whether to convert all whitespace into space characters.
		/// </summary>
		public bool NormalizeWhitespace
		{
			get { return normalizeWhitespace; }
			set { normalizeWhitespace = value; }
		}
		/// <summary>
		/// Get or set the characters to consider as whitespace.
		/// </summary>
		public char[] WhitespaceChars
		{
			get { return whitespaceChars; }
			set
			{
				if (value != null)
				{
					whitespaceChars = value;
				}
				else
				{
					whitespaceChars = new char[] {'\r', '\n', '\t', ' '};
				}
			}
		}

		/// <summary>
		/// Get or set whether to write the headers as the first row.
		/// </summary>
		public bool WriteHeaders
		{
			get { return writeHeaders; }
			set { writeHeaders = value; }
		}

		/// <summary>
		/// Gets or sets whether to trim whitespace from each field.
		/// </summary>
		public bool TrimWhitespace
		{
			get { return trimWhitespace; }
			set { trimWhitespace = value; }
		}

		/// <summary>
		/// Get or set whether to skip blank lines.
		/// </summary>
		public bool SkipBlankLines
		{
			get { return skipBlankLines; }
			set { skipBlankLines = value; }
		}

		/// <summary>
		/// Get or set whether to interpet quote characters when parsing.
		/// </summary>
		public bool ParseQuotes
		{
			get { return parseQuotes; }
			set { parseQuotes = value; }
		}

		/// <summary>
		/// Get or set the string to use as a quote symbol.
		/// </summary>
		public string QuoteChar
		{
			get { return quoteChar; }
			set
			{
				quoteChar = value;
				FixupDelimiterOrQuoteOrNewline();
			}
		}

		/// <summary>
		/// Get or set the string to use to delimit columns.
		/// </summary>
		public string Delimiter
		{
			get { return delimiter; }
			set
			{
				delimiter = value;
				FixupDelimiterOrQuoteOrNewline();
			}
		}
		private void FixupDelimiterOrQuoteOrNewline()
		{
			string dqn = (delimiter == null ? "" : delimiter) + (quoteChar == null ? "" : quoteChar) + "\r\n";
			delimiterOrQuoteOrNewline = dqn.ToCharArray();
		}

		/// <summary>
		/// Get or set whether to end the file in a newline.
		/// </summary>
		public bool EndInNewline
		{
			get { return endInNewline; }
			set { endInNewline = value; }
		}

		/// <summary>
		/// Gets or sets whether to ignore case when matching header names.
		/// </summary>
		public bool IgnoreHeaderCase
		{
			get { return ignoreHeaderCase; }
			set
			{
				if (ignoreHeaderCase != value)
				{
					ignoreHeaderCase = value;
					FixupNormalizedHeaders();
				}
			}
		}
		/// <summary>
		/// Gets or sets the header names.
		/// </summary>
		public string[] Headers
		{
			get
			{
				return headers;
			}
			set
			{
				headers = value;
				FixupNormalizedHeaders();
			}
		}
		private void FixupNormalizedHeaders()
		{
			if (headers == null)
			{
				headersNormalized = null;
				return;
			}
			headersNormalized = new string[headers.Length];
			for (int i = 0; i < headers.Length; i++)
			{
				string header = headers[i];
				if (header == null)  continue;
				header = header.Trim();
				if (IgnoreHeaderCase)  header = header.ToLower();
				headersNormalized[i] = header;
			}
		}


		/// <summary>
		/// Add a new header to the header list.
		/// </summary>
		/// <param name="header">the header to add</param>
		public void AddHeader(string header)
		{
			if (headers == null)  headers = new string[0];
			string[] newHeaders = new string[headers.Length + 1];
			headers.CopyTo(newHeaders, 0);
			newHeaders[newHeaders.Length-1] = header;
			Headers = newHeaders;
		}


		/// <summary>
		/// Check for end of file.
		/// </summary>
		/// <returns>true if at end of file, false otherwise</returns>
		public bool Eof()
		{
			return (file == null);
		}


		private void Initialize()
		{
			if (!initialized)
			{
				initialized = true;
				// check if headers were needed and not written:
				if (writeHeaders && headers != null)
				{
					WriteRow(headers);
				}
			}
		}


		private string Quotify(string item)
		{
			if (trimWhitespace)
			{
				if (item.Length != 0 &&
					(item[0] == ' ' || item[item.Length-1] == ' '))
				{
					item = item.Trim();
				}
			}
			if (!parseQuotes)
			{
				return item;
			}

			if (item.IndexOfAny(delimiterOrQuoteOrNewline) < 0)
			{
				// no delimiters, newlines, or quotes in string
				return item;
			}
			StringBuilder sb = new StringBuilder(item);
			sb.Replace(quoteChar, quoteChar + quoteChar);
			sb.Insert(0, quoteChar);
			sb.Append(quoteChar);
			return sb.ToString();
		}


		/// <summary>
		/// Write the next field and advance the writer.
		/// </summary>
		/// <param name="item">the field to write</param>
		public void WriteItem(string item)
		{
			Initialize();
			WriteStoredRow();
			if (Eof())  return;

			if (lineStart)
			{
				lineStart = false;
			}
			else
			{
				file.Write(delimiter);
			}

			if (normalizeWhitespace)
			{
				item = NormalizeWS(item);
			}
			item = Quotify(item);
			file.Write(item);
		}

		private string NormalizeWS(string orig)
		{
			if (orig.IndexOfAny(whitespaceChars) < 0)  return orig;
			orig = orig.Replace("\r\n", " ");
			orig = orig.Replace('\r', ' ');
			orig = orig.Replace('\n', ' ');
			orig = orig.Replace('\t', ' ');
			return orig;
		}

		/// <summary>
		/// Write an entire row and advance the writer.
		/// </summary>
		/// <param name="items">The row to write as an array of fields</param>
		public void WriteRow(string[] items)
		{
			WriteRow(items, -1);
		}

		/// <summary>
		/// Write an entire row and advance the writer.
		/// </summary>
		/// <param name="len">The length of the row to write, truncating or filling with empty fields as needed</param>
		/// <param name="items">The row to write as an array of fields</param>
		public void WriteRow(string[] items, int len)
		{
			Initialize();
			WriteStoredRow();
			if (Eof())  return;

			if (!parseQuotes)
			{
				if (normalizeWhitespace)
				{
					// this modifies the input parameter!
					for (int i = 0; i < items.Length; i++)
					{
						items[i] = NormalizeWS(items[i]);
					}
				}

				if (skipBlankLines && lineStart && items.Length == 0)  return;
//				if (len < 0 || len == items.Length)
//				{
//					file.WriteLine(string.Join(delimiter, items));
//				}
//				else
//				{
//					if (items.Length < len)
//					{
//						file.Write(string.Join(delimiter, items));
//						// pad with empty strings:
//						for (int i = items.Length; i < len; i++)
//						{
//							file.Write(delimiter);
//						}
//						file.WriteLine();
//					}
//					else
//					{
//						file.WriteLine(string.Join(delimiter, items, 0, len));
//					}
//				}
				if (items.Length == 0)
				{
					if (len > 0)
					{
						for (int i = 1; i < len; i++)
						{
							file.Write(delimiter);
						}
					}
				}
				else
				{
					file.Write(items[0]);
					if (len < 0 || len >= items.Length)
					{
						for (int i = 1; i < items.Length; i++)
						{
							file.Write(delimiter);
							if (items[i].Length != 0)  file.Write(items[i]);
						}
						for (int i = items.Length; i < len; i++)
						{
							file.Write(delimiter);
						}
					}
					else
					{
						for (int i = 1; i < len; i++)
						{
							file.Write(delimiter);
							if (items[i].Length != 0)  file.Write(items[i]);
						}
					}
				}
				file.WriteLine();
				
				lineStart = true;
				rowNumber++;
				return;
			}

			for (int i = 0; i < items.Length && (len < 0 || i < len); i++)
			{
				WriteItem(items[i]);
			}
			// pad with empty strings:
			if (len > 0)
			{
				for (int i = items.Length; i < len; i++)
				{
					WriteItem("");
				}
			}
			NextRow();
		}

		/// <summary>
		/// Write an entire row and advance the writer.
		/// </summary>
		/// <param name="items">The row to write as a collection of fields</param>
		public void WriteRow(StringCollection items)
		{
			WriteRow(items, -1);
		}

		/// <summary>
		/// Write an entire row and advance the writer.
		/// </summary>
		/// <param name="len">The length of the row to write, truncating or filling with empty fields as needed</param>
		/// <param name="items">The row to write as a collection of fields</param>
		public void WriteRow(StringCollection items, int len)
		{
			Initialize();
			WriteStoredRow();
			if (Eof())  return;

			if (!parseQuotes)
			{
//				string[] itemsA = new string[items.Count];
//				items.CopyTo(itemsA, 0);

				if (normalizeWhitespace)
				{
//					for (int i = 0; i < itemsA.Length; i++)
//					{
//						itemsA[i] = NormalizeWS(itemsA[i]);
//					}
					for (int i = 0; i < items.Count; i++)
					{
						items[i] = NormalizeWS(items[i]);
					}
				}

//				if (skipBlankLines && lineStart && itemsA.Length == 0)  return;
				if (skipBlankLines && lineStart && items.Count == 0)  return;
//				if (len < 0 || len == itemsA.Length)
//				{
//					file.WriteLine(string.Join(delimiter, itemsA));
//				}
//				else
//				{
//					if (itemsA.Length < len)
//					{
//						file.Write(string.Join(delimiter, itemsA));
//						// pad with empty strings:
//						for (int i = itemsA.Length; i < len; i++)
//						{
//							file.Write(delimiter);
//						}
//						file.WriteLine();
//					}
//					else
//					{
//						file.WriteLine(string.Join(delimiter, itemsA, 0, len));
//					}
//				}
				if (items.Count == 0)
				{
					if (len > 0)
					{
						for (int i = 1; i < len; i++)
						{
							file.Write(delimiter);
						}
					}
				}
				else
				{
					file.Write(items[0]);
					if (len < 0 || len >= items.Count)
					{
						for (int i = 1; i < items.Count; i++)
						{
							file.Write(delimiter);
							if (items[i].Length != 0)  file.Write(items[i]);
						}
						for (int i = items.Count; i < len; i++)
						{
							file.Write(delimiter);
						}
					}
					else
					{
						for (int i = 1; i < len; i++)
						{
							file.Write(delimiter);
							if (items[i].Length != 0)  file.Write(items[i]);
						}
					}
				}
				file.WriteLine();
				
				lineStart = true;
				rowNumber++;
				return;
			}

			for (int i = 0; i < items.Count && (len < 0 || i < len); i++)
			{
				WriteItem(items[i]);
			}
			// pad with empty strings:
			if (len > 0)
			{
				for (int i = items.Count; i < len; i++)
				{
					WriteItem("");
				}
			}
			NextRow();
		}


		/// <summary>
		/// Set the field at the column index.
		/// </summary>
		public string this[int index]
		{
			set
			{
				if (index < 0)  return;
				if (curRow == null)  curRow = new StringCollection();
				if (curRow.Count <= index)  // extend it...
				{
					for (int i = curRow.Count; i <= index; i++)
					{
						curRow.Add("");
					}
				}
				curRow[index] = value;
			}
		}


		/// <summary>
		/// Set the field at the column with the given header.
		/// </summary>
		public string this[string header]
		{
			set
			{
				if (header == null || headers == null) return;
				header = header.Trim();
				if (IgnoreHeaderCase)  header = header.ToLower();
				for (int i = 0; i < headersNormalized.Length; i++)
				{
					if (header == headersNormalized[i])
					{
						this[i] = value;
						return;
					}
				}
				// do nothing!
				return;
			}
		}


		private void  WriteStoredRow()
		{
			if (curRow == null)  return;
			WriteRow(curRow);
			curRow = null;
		}


		/// <summary>
		/// Advance to the next row.
		/// </summary>
		public void NextRow()
		{
			if (Eof())  return;
			if (curRow != null)
			{
				WriteStoredRow();
				return;
			}
			if (lineStart && skipBlankLines)
				return;
			file.WriteLine();
			lineStart = true;
			rowNumber++;
		}

		/// <summary>
		/// Get the number of the current row.
		/// </summary>
		public long RowNumber
		{
			get { return rowNumber; }
		}

		/// <summary>
		/// Close the table.
		/// </summary>
		public void Close()
		{
			if (Eof())  return;
			if (curRow != null)
			{
				WriteStoredRow();
				return;
			}
			if (!lineStart && endInNewline)  NextRow();
			file.Flush();
			file.Close();
			lineStart = true;
			file = null;
		}
		#region IDisposable Members

		/// <summary>
		/// Dispose.
		/// </summary>
		public void Dispose()
		{
			Close();
		}

		#endregion
	}



#if !ENABLE_BARTOK

#if !DISABLE_XML
	//////////////////////////////////////////////
	////// XML support
	//////////////////////////////////////////////

	//// TODO: Decide how to handle attributes vs. child elements
	//// TODO: Allow for categories (pushing and popping levels)
	//// TODO: Allow override of given element names by header order?

	/// <summary>
	/// Read XML formatted data.
	/// </summary>
	public class XmlTableReader : ITableReader
	{
		private bool trimWhitespace = true;
		private bool fillBlankColumns = true;
		private bool ignoreHeaderCase = true;
		private bool addUnknownHeaders = true;
		private bool headerOrdered = true;

		private XmlTextReader file;
		private Hashtable     currentRow;
		private ArrayList     currentRowSequence;
		private string        currentName;
		private int           currentCol;
		//private bool          initialized = false;
		private string        tableName;
		private string[]      headers;
		private bool          initialized = false;

		private int level = 0;


		/// <summary>
		/// Create a new XmlTableReader
		/// </summary>
		/// <param name="tr">the source to base the XmlTableReader on</param>
		public XmlTableReader(XmlTextReader tr)
		{
			headers = new string[0];
			file = tr;
			if (file != null)
			{
				file.WhitespaceHandling = WhitespaceHandling.None;
				file.MoveToContent();
				// read in the "Table" element
				tableName = "";
				if (file.IsStartElement())
				{
					tableName = file.Name;
					file.Read();
				}

				// NextRow();  // - don't do this! Might initialize too soon.
			}
		}

		/// <summary>
		/// Create a new XmlTableReader
		/// </summary>
		/// <param name="fname">the name of the file to base the XmlTableReader on</param>
		public XmlTableReader(string fname)
			: this(new XmlTextReader(ZStreamReader.Open(fname)))
		{
		}

		/// <summary>
		/// Create a new XmlTableReader
		/// </summary>
		/// <param name="tr">the source to base the XmlTableReader on</param>
		public XmlTableReader(Stream tr)
			: this(new XmlTextReader(tr))
		{
		}

		/// <summary>
		/// Create a new XmlTableReader
		/// </summary>
		/// <param name="tr">the source to base the XmlTableReader on</param>
		public XmlTableReader(TextReader tr)
			: this(new XmlTextReader(tr))
		{
		}


		/// <summary>
		/// Get the current in the hierarchy.
		/// </summary>
		public int Level
		{
			get { return level; }
		}

		/// <summary>
		/// Gets or sets the header names.
		/// </summary>
		public string[] Headers
		{
			get { return headers; }
			set { headers = value; }
		}

		/// <summary>
		/// Get or set whether to add new headers to the header list as they are encountered.
		/// </summary>
		public bool AddUnknownHeaders
		{
			get { return addUnknownHeaders; }
			set { addUnknownHeaders = value; }
		}

		/// <summary>
		/// Get or set whether to sort the headers.
		/// </summary>
		public bool HeaderOrdered
		{
			get { return headerOrdered; }
			set { headerOrdered = value; }
		}

		/// <summary>
		/// Get the name of the table.
		/// </summary>
		public string TableName
		{
			get { return tableName; }
		}

		/// <summary>
		/// Check for end of file.
		/// </summary>
		/// <returns>true if at end of file, false otherwise</returns>
		public bool Eof()
		{
			if (!initialized)  Initialize();
			return currentRow == null && (file.ReadState == ReadState.Closed || file.EOF);
		}

		/// <summary>
		/// Check for end of row.
		/// </summary>
		/// <returns>true if at end of row, false otherwise</returns>
		public bool RowEnd()
		{
			if (!initialized)  Initialize();
			return Eof() || currentRowSequence == null ||
				currentCol >= currentRowSequence.Count;
		}

		/// <summary>
		/// Get the next field and advance the reader.
		/// </summary>
		/// <returns>the field at the next column</returns>
		public string ReadItem()
		{
			if (!initialized)  Initialize();
			if (Eof())  return null;
			if (RowEnd())
			{
				// should we fill even this??
				if (FillBlankColumns)  return "";
				return null;
			}
			string res = (string)currentRowSequence[currentCol];
			currentCol++;
			return res;
		}

		/// <summary>
		/// Get the next field and advance the reader, filling with empty fields at the end of the row.
		/// </summary>
		/// <returns>the field at the next column</returns>
		public string ReadItemLinear()
		{
			if (!initialized)  Initialize();
			if (Eof())  return null;
			if (RowEnd())  NextRow();
			return ReadItem();
		}

		/// <summary>
		/// Get the number of fields in the current row.
		/// </summary>
		/// <returns>the number of fields in the current row</returns>
		public int RowLength()
		{
			if (!initialized)  Initialize();
			if (currentRowSequence == null)  return 0;
			return currentRowSequence.Count;
		}

		/// <summary>
		/// Read an entire row and advance the reader.
		/// </summary>
		/// <returns>The current row as an array of fields</returns>
		public string[] ReadRow()
		{
			if (!initialized)  Initialize();
			if (currentRowSequence == null)  return null;
			string[] res = (string[])currentRowSequence.ToArray(typeof(string));
			NextRow();
			return res;
		}

		/// <summary>
		/// Read an entire row and advance the reader.
		/// </summary>
		/// <param name="len">The length of the row to read, truncating or filling with empty fields as needed</param>
		/// <returns>The current row as an array of fields</returns>
		public string[] ReadRow(int len)
		{
			if (!initialized)  Initialize();
			if (len < 0)  return ReadRow();
			if (currentRowSequence == null)  return null;
			string[] res = new string[len];
			for (int i = 0; i < res.Length; i++)
			{
				if (i < currentRowSequence.Count)
				{
					res[i] = (string)currentRowSequence[i];
				}
				else
				{
					res[i] = "";
				}
			}
			NextRow();
			return res;
		}

		private void Initialize()
		{
			if (initialized)  return;
			initialized = true;
			NextRow();
			// can't read and store the column header names:
		}

		/// <summary>
		/// Advance to the next row.
		/// </summary>
		public void NextRow()
		{
			if (!initialized)
			{
				Initialize();
				return;
			}
			currentRow = null;
			currentRowSequence = null;
			currentName = null;
			currentCol = -1;
			if (Eof())
			{
				return;
			}

			if (!file.IsStartElement())
			{
				// what to do here? It's wrong.
				file.Read();
				NextRow();
				return;
			}

			currentName = file.Name;
			currentRow = new Hashtable();
			// assumes headers not null
			currentRowSequence = new ArrayList(headers.Length);
			currentCol = 0;
			if (!file.IsEmptyElement)
			{
				string givenHeader, name, val;

				if (file.HasAttributes)
				{
					for (int a = 0; a < file.AttributeCount; a++)
					{
						file.MoveToAttribute(a);
						givenHeader = file.Name;
						name = givenHeader.Trim();
						if (IgnoreHeaderCase)  name = name.ToLower();
						val = file.Value;
						if (TrimWhitespace)  val = val.Trim();

						int index = -1;
						for (int i = 0; i < headers.Length; i++)
						{
							string match = headers[i].Trim();
							if (IgnoreHeaderCase)  match = match.ToLower();
							if (name == match)
							{
								index = i;
								break;
							}
						}
						if (index < 0)
						{
							if (addUnknownHeaders)
							{
								string[] oldHeaders = headers;
								headers = new string[oldHeaders.Length + 1];
								oldHeaders.CopyTo(headers, 0);
								index = headers.Length-1;
								headers[index] = givenHeader;
							}
						}
						//currentRow.Add(name, val);
						currentRow[name] = val;
						if (!headerOrdered)
						{
							// skip adding to the sequence for attributes!
							//currentRowSequence.Add(val);
						}
						else
						{
							if (val.Length != 0 && index >= 0 && index < headers.Length)
							{
								for (int b = currentRowSequence.Count; b <= index; b++)
								{
									currentRowSequence.Add("");
								}
								currentRowSequence[index] = val;
							}
						}
					}
					file.MoveToElement();  // Moves the reader back to the element node.
				}

				file.Read();  // pass start element
				while (!(file.NodeType == XmlNodeType.EndElement))
				{
					if (file.IsStartElement())
					{
						givenHeader = file.Name;
						name = givenHeader.Trim();
						if (IgnoreHeaderCase)  name = name.ToLower();
						val = "";
						if (!file.IsEmptyElement)
						{
							file.Read();
							val = file.ReadString();
							if (TrimWhitespace)  val = val.Trim();
						}

						int index = -1;
						for (int i = 0; i < headers.Length; i++)
						{
							string match = headers[i].Trim();
							if (IgnoreHeaderCase)  match = match.ToLower();
							if (name == match)
							{
								index = i;
								break;
							}
						}
						if (index < 0)
						{
							if (addUnknownHeaders)
							{
								string[] oldHeaders = headers;
								headers = new string[oldHeaders.Length + 1];
								oldHeaders.CopyTo(headers, 0);
								index = headers.Length-1;
								headers[index] = givenHeader;
							}
						}
						//currentRow.Add(name, val);
						currentRow[name] = val;
						if (!headerOrdered)
						{
							// skip adding to the sequence for attributes!
							//currentRowSequence.Add(val);
						}
						else
						{
							if (val.Length != 0 && index >= 0 && index < headers.Length)
							{
								for (int b = currentRowSequence.Count; b <= index; b++)
								{
									currentRowSequence.Add("");
								}
								currentRowSequence[index] = val;
							}
						}
						file.Read();  // skip empty element OR end element...
					}
					else
					{
						// text not within a child element??
						val = file.ReadString();
						if (TrimWhitespace)  val = val.Trim();
						//currentRow.Add("", val);
						currentRow[""] = val;
					}
				}
			}
			file.Read();
			// try to skip the document closing tag? (XmlReader is disgusting, really)
			while (file.NodeType == XmlNodeType.EndElement)
			{
				file.Read();
				level--;
			}
			if (level < 0)  level = 0;
		}

		/// <summary>
		/// Get the field at the column index.
		/// </summary>
		public string this[int index]
		{
			get
			{
				if (!initialized)  Initialize();
				if (currentRowSequence == null || index < 0 ||
					index >= currentRowSequence.Count)
				{
					if (FillBlankColumns)  return "";
					return null;
				}
				return (string)currentRowSequence[index];
			}
		}

		/// <summary>
		/// Get the field at the column with the given header.
		/// </summary>
		public string this[string header]
		{
			get
			{
				if (!initialized)  Initialize();
				header = header.Trim();
				if (IgnoreHeaderCase)  header = header.ToLower();
				string res = (string)currentRow[header];
				if (res == null && FillBlankColumns)  res = "";
				return res;
			}
		}

		/// <summary>
		/// Close the table.
		/// </summary>
		public void Close()
		{
			file.Close();
		}

		/// <summary>
		/// Reset the reader to the beginning.
		/// </summary>
		/// <exception cref="InvalidOperationException">Always thrown, currently.</exception>
		public void Reset()
		{
			throw new InvalidOperationException("Cannot reset XmlTableReader.");
		}

		/// <summary>
		/// Gets or sets whether to trim whitespace from each field.
		/// </summary>
		public bool TrimWhitespace
		{
			get
			{
				return trimWhitespace;
			}
			set
			{
				trimWhitespace = value;
			}
		}

		/// <summary>
		/// Gets or sets whether to return "", not null, when the end of a row is reached,
		/// until the row is advanced:
		/// </summary>
		public bool FillBlankColumns
		{
			get
			{
				return fillBlankColumns;
			}
			set
			{
				fillBlankColumns = value;
			}
		}

		/// <summary>
		/// Gets or sets whether to ignore case when matching header names.
		/// </summary>
		public bool IgnoreHeaderCase
		{
			get
			{
				return ignoreHeaderCase;
			}
			set
			{
				ignoreHeaderCase = value;
			}
		}

		/// <summary>
		/// Get a row enumerator.
		/// </summary>
		/// <returns>and enumerator for the row in this table</returns>
		public IEnumerator GetEnumerator()
		{
			return new TableEnumerator(this);
		}

	}


	/// <summary>
	/// Write XML formatted data.
	/// </summary>
	public class XmlTableWriter : ITableWriter
	{
		private bool trimWhitespace = true;
		private bool ignoreHeaderCase = true;
		private bool addUnknownHeaders = true;
		private bool skipEmptyElements = true;

		private XmlTextWriter file;
		//private Hashtable     currentRow;
		private ArrayList     currentRowSequence;
		private string        tableName = "Table";
		private string        currentName = "Item";
		private int           currentCol;
		private bool          initialized = false;
		private string[]      headers;
		private bool          fieldsAsAttributes = false;
		private bool          elementEndPending = false;
		private int           openElementCount = 0;


		/// <summary>
		/// Create a new XmlTableWriter.
		/// </summary>
		/// <param name="tr">the destination to write the table to</param>
		public XmlTableWriter(XmlTextWriter tr)
		{
			file = tr;
			currentCol = 0;
			if (file != null)
			{
				file.Formatting = Formatting.Indented;
				file.Indentation = 4;
				//Write the XML delcaration 
				file.WriteStartDocument();

				////Write the ProcessingInstruction node
				//string PItext="type=\"text/xsl\" href=\"table.xsl\"";
				//file.WriteProcessingInstruction("xml-stylesheet", PItext);
				//
				////Write the DocumentType node
				//file.WriteDocType("table", null , null, "<!ENTITY h \"tablexml\">");
			}
		}

		/// <summary>
		/// Create a new XmlTableWriter.
		/// </summary>
		/// <param name="fname">the filename of the destination to write the table to</param>
		public XmlTableWriter(string fname)
			: this(new XmlTextWriter(ZStreamWriter.Open(fname)))
		{
		}

		/// <summary>
		/// Create a new XmlTableWriter.
		/// </summary>
		/// <param name="tr">the destination to write the table to</param>
		public XmlTableWriter(Stream tr)
			: this(new XmlTextWriter(tr, null))
		{
		}

		/// <summary>
		/// Create a new XmlTableWriter.
		/// </summary>
		/// <param name="tr">the destination to write the table to</param>
		public XmlTableWriter(TextWriter tr)
			: this(new XmlTextWriter(tr))
		{
		}


		/// <summary>
		/// Gets or sets the header names.
		/// </summary>
		public string[] Headers
		{
			get
			{
				return headers;
			}
			set
			{
				headers = value;
				if (headers != null)
				{
					for (int i = 0; i < headers.Length; i++)
					{
						if (headers[i] == null || headers[i].Length == 0)
						{
							headers[i] = "C" + i;
						}
						else
						{
							headers[i] = headers[i].Replace(' ', '_');
							headers[i] = XmlConvert.EncodeName(headers[i]);
						}
					}
				}
			}
		}

		/// <summary>
		/// Get or set the name to use for each item element,
		/// </summary>
		public string ItemName
		{
			get
			{
				return currentName;
			}
			set
			{
				currentName = value;
			}
		}

		/// <summary>
		/// Get or set the name to use for the table element.
		/// </summary>
		public string TableName
		{
			get
			{
				return tableName;
			}
			set
			{
				tableName = value;
			}
		}

		/// <summary>
		/// Get or set whether to represent the fields as attributes, instead of children.
		/// </summary>
		public bool FieldsAsAttributes
		{
			get { return fieldsAsAttributes; }
			set { fieldsAsAttributes = value; }
		}

		/// <summary>
		/// Get or set whether to skip all empty elements.
		/// </summary>
		public bool SkipEmptyElements
		{
			get { return skipEmptyElements; }
			set { skipEmptyElements = value; }
		}


		/// <summary>
		/// Check for end of file.
		/// </summary>
		/// <returns>true if at end of file, false otherwise</returns>
		public bool Eof()
		{
			return file.WriteState == WriteState.Closed;
		}

		/// <summary>
		/// Write the next field and advance the writer.
		/// </summary>
		/// <param name="item">the field to write</param>
		public void WriteItem(string item)
		{
			if (Eof())  return;
			this[currentCol] = item;
			currentCol++;
		}

		/// <summary>
		/// Write an entire row and advance the writer.
		/// </summary>
		/// <param name="items">The row to write as an array of fields</param>
		public void WriteRow(string[] items)
		{
			WriteRow(items, -1);
		}

		/// <summary>
		/// Write an entire row and advance the writer.
		/// </summary>
		/// <param name="len">The length of the row to write, truncating or filling with empty fields as needed</param>
		/// <param name="items">The row to write as an array of fields</param>
		public void WriteRow(string[] items, int len)
		{
			// only handles sequence - not named items!!! ***
			if (Eof())  return;
			if (items == null)  return;
			if (!initialized)
			{
				initialized = true;
				file.WriteStartElement(TableName);
			}

			if (elementEndPending)
			{
				if (openElementCount > 0)
				{
					if (TrimWhitespace)
					{
						file.WriteEndElement();
					}
					else
					{
						file.WriteFullEndElement();
					}
					openElementCount--;
				}
				elementEndPending = false;
			}

			if (len < 0)  len = items.Length;
			file.WriteStartElement(ItemName);
			for (int i = 0; i < len; i++)
			{
				string name;
				if (headers != null && i < headers.Length)
				{
					name = headers[i];
				}
				else
				{
					name = "C" + i;
				}
				if (FieldsAsAttributes)
				{
					string val = (i < items.Length) ? (string)items[i] : "";
					if (val == null)  val = "";
					if (TrimWhitespace)  val = val.Trim();
					if (SkipEmptyElements && val.Length == 0)
					{
						// just skip it...
					}
					else
					{
						file.WriteAttributeString(name, val);
					}
				}
				else
				{
					string val = (i < items.Length) ? (string)items[i] : "";
					if (val == null)  val = "";
					if (TrimWhitespace)  val = val.Trim();
					////file.WriteElementString(name, val);
					if (SkipEmptyElements && val.Length == 0)
					{
						// just skip it...
					}
					else
					{
						file.WriteStartElement(name);
						if (val.Length == 0)
						{
							file.WriteEndElement();
						}
						else
						{
							file.WriteString(val);
							file.WriteFullEndElement();
						}
					}
				}
			}

			// delay writing the end element:
			//file.WriteEndElement();
			elementEndPending = true;
			openElementCount++;
		}

		/// <summary>
		/// Advance to the next row.
		/// </summary>
		public void NextRow()
		{
			if (currentRowSequence != null)
			{
				// only handles sequence - not named items!!! ***
				WriteRow((string[])currentRowSequence.ToArray(typeof(string)));
				currentRowSequence = null;
			}

			//currentRow = null;
			currentRowSequence = null;
			currentCol = 0;
		}

		/// <summary>
		/// Set the field at the column index.
		/// </summary>
		public string this[int index]
		{
			// checking headers can be done here or at write time. Not much difference.
			set
			{
				if (index < 0)  return;
				if (currentRowSequence == null)  currentRowSequence = new ArrayList();
				if (currentRowSequence.Count <= index)  // extend it...
				{
					for (int i = currentRowSequence.Count; i <= index; i++)
					{
						currentRowSequence.Add("");
					}
				}
				currentRowSequence[index] = value;
			}
		}

		/// <summary>
		/// Set the field at the column with the given header.
		/// </summary>
		public string this[string header]
		{
			// checking headers can be done here or at write time. Not much difference.
			set
			{
				if (header == null)  return;
				//if (currentRow == null)  currentRow = new Hashtable();
				string givenHeader = header;
				header = header.Trim();
				if (IgnoreHeaderCase)  header = header.ToLower();
				// don't use hash table:
				//currentRow[header] = value;
				// find in headers:
				for (int i = 0; i < headers.Length; i++)
				{
					string match = headers[i].Trim();
					if (IgnoreHeaderCase)  match = match.ToLower();
					if (header == match)
					{
						this[i] = value;
						return;
					}
				}
				// add new header if required:
				if (!AddUnknownHeaders)  return;
				string[] oldHeaders = headers;
				headers = new string[oldHeaders.Length + 1];
				oldHeaders.CopyTo(headers, 0);
				headers[headers.Length-1] = givenHeader;
				this[headers.Length-1] = value;
			}
		}

		/// <summary>
		/// Close the table.
		/// </summary>
		public void Close()
		{
			if (file == null || Eof())  return;
			// we don't really want this NextRow unless needed... ***
			NextRow();

			for (; openElementCount > 0; openElementCount--)
			{
				if (TrimWhitespace)
				{
					file.WriteEndElement();
				}
				else
				{
					file.WriteFullEndElement();
				}
			}
			elementEndPending = false;

			file.WriteFullEndElement();
			file.WriteEndDocument();
			file.Flush();
			file.Close();
		}

		/// <summary>
		/// Increase the hierarchy depth.
		/// </summary>
		public void LevelIn()
		{
			elementEndPending = false;
		}

		/// <summary>
		/// Decrease the hierarchy depth.
		/// </summary>
		public void LevelOut()
		{
			if (openElementCount > 1 || (!elementEndPending && openElementCount > 0))
			{
				if (TrimWhitespace)
				{
					file.WriteEndElement();
				}
				else
				{
					file.WriteFullEndElement();
				}

				openElementCount--;
			}
		}

		/// <summary>
		/// Gets or sets whether to trim whitespace from each field.
		/// </summary>
		public bool TrimWhitespace
		{
			get { return trimWhitespace; }
			set { trimWhitespace = value; }
		}

		/// <summary>
		/// Get or set whether to add new headers to the header list as they are encountered.
		/// </summary>
		public bool AddUnknownHeaders
		{
			get { return addUnknownHeaders; }
			set { addUnknownHeaders = value; }
		}

		/// <summary>
		/// Gets or sets whether to ignore case when matching header names.
		/// </summary>
		public bool IgnoreHeaderCase
		{
			get { return ignoreHeaderCase; }
			set { ignoreHeaderCase = value; }
		}
	}

#endif

#endif




#if ALLOW_DB
	//////////////////////////////////////////////
	////// Excel support
	//////////////////////////////////////////////


	//// TODO: ODBC driver support?
	////       Bare CsvReader support / merging?
	////       Decent exception handling
	////       Add in enumerator

	/// <summary>
	/// Read spreadsheet data in various formats.
	/// </summary>
	public class SpreadsheetReader
	{
		private string filename;
		private bool xlsFormat;
		private OleDbConnection oleConn;
		private OleDbDataReader oleReader;
		private bool isEof;
		private string[] cols;

		// trim whitespace from each entry:
		private bool trimWhitespace = true;
		// skip lines that are empty:
		private bool skipBlankLines = true;
		// skip lines that have fields but only whitespace in them:
		private bool skipBlankColumnsLines = true;
		private long rowNumber = 2;  // gives 1-base row numbers...

		/// <summary>
		/// Gets or sets whether to trim whitespace from each field.
		/// </summary>
		public bool TrimWhitespace
		{
			get { return trimWhitespace; }
			set { trimWhitespace = value; }
		}
		/// <summary>
		/// Get or set whether to skip blank lines.
		/// </summary>
		public bool SkipBlankLines
		{
			get { return skipBlankLines; }
			set { skipBlankLines = value; }
		}
		/// <summary>
		/// Get or set whether to skip lines with all columns blank.
		/// </summary>
		public bool SkipBlankColumnsLines
		{
			get { return skipBlankColumnsLines; }
			set { skipBlankColumnsLines = value; }
		}
		/// <summary>
		/// Get the number of the current row.
		/// </summary>
		public long RowNumber
		{
			get { return rowNumber; }
		}

		/// <summary>
		/// Create a new SpreadsheetReader.
		/// </summary>
		/// <param name="fname">the source of the spreadsheet</param>
		public SpreadsheetReader(string fname)
		{
			filename = fname;
			// determine format:
			xlsFormat = false;
			//Console.WriteLine("Extension: " + Path.GetExtension(filename).ToLower());
			if (Path.GetExtension(filename).ToLower() == ".xls")
			{
				xlsFormat = true;
			}
			// construct connection string:
			string connString = "Provider=Microsoft.Jet.OLEDB.4.0;" +
				"Data Source=";
			if (xlsFormat) // Excel Format
			{
				connString += "\"" + Path.GetFullPath(filename) + "\"" + ";" +
					//	"Extended Properties=\"Excel 8.0;HDR=No\"";
					"Extended Properties=\"Excel 8.0;HDR=Yes\"";
			}
			else  // assume CSV
			{
				connString += Path.GetDirectoryName(Path.GetFullPath(filename)) + ";" +
					//	"Extended Properties=\"text;HDR=No;FMT=Delimited\"";
					"Extended Properties=\"text;HDR=Yes;FMT=Delimited\"";
			}
			//Console.WriteLine("Opening with: " + connString);
			string selectString = "SELECT * FROM ";
			if (xlsFormat) // Excel Format
			{
				selectString += "[Sheet1$]";  // hard-coded first sheet? ***
				//selectString += "foo";  // hard-coded first sheet? ***
			}
			else  // assume CSV
			{
				selectString += Path.GetFileName(filename);
			}
			//Console.WriteLine("Selecting with: " + selectString);
			
			oleConn = new OleDbConnection(connString);
			oleConn.Open();
			//OleDbCommand openCmd = new OleDbCommand(selectString, oleConn);
			OleDbCommand openCmd = oleConn.CreateCommand();
			openCmd.CommandText = selectString;
			//openCmd.Connection.Open();
			//Console.WriteLine("Opened connection.");
			//Console.WriteLine("Database:  " + oleConn.Database);
			//Console.WriteLine("Datasource:  " + oleConn.DataSource);

			// start up reader:
			//oleReader = openCmd.ExecuteReader(CommandBehavior.CloseConnection);

			//MessageBox.Show(connString, "Excel Connection String");
			//MessageBox.Show(selectString, "Excel Select String");
			oleReader = openCmd.ExecuteReader();
			//Console.WriteLine("Started Reader.");

			//TestConnection();

			// Initialize the row reading:
			isEof = !Next();
			// Initialize the column names:
			SetupColumnNames();
		}

		/// <summary>
		/// Test the connection to the spreadsheet.
		/// </summary>
		public void TestConnection()
		{
			if (oleReader == null)
			{
				Console.WriteLine("OleDbDataReader is not initialized!");
				return;
			}
			// display column names
			for (int c = 0; c < oleReader.FieldCount; c++)
			{
				Console.Write(oleReader.GetName(c) + " \t");
			}
			Console.WriteLine("");
			Console.WriteLine("--------------------------------------------------");
			// display data
			while(oleReader.Read()) 
			{
				object[] fields = new Object[oleReader.FieldCount];
				oleReader.GetValues(fields);
				for (int i = 0; i < fields.Length; i++)
				{
					Console.Write("" + fields[i] + " \t");
				}
				Console.WriteLine("");
			}
		}

		/// <summary>
		/// Read an entire row and advance the reader.
		/// </summary>
		/// <returns>The current row as an array of fields</returns>
		public object[] ReadRowObjects()
		{
			if (Eof())  return new Object[0];  // should it be a null?
			object[] fields = new Object[oleReader.FieldCount];
			oleReader.GetValues(fields);
			for (int i = 0; i < fields.Length; i++)
			{
				if (fields[i] == DBNull.Value)
				{
					fields[i] = null;
				}
			}
			Next();
			return fields;
		}

		/// <summary>
		/// Read an entire row and advance the reader.
		/// </summary>
		/// <returns>The current row as an array of fields</returns>
		public string[] ReadRow()
		{
			if (Eof())  return new string[0];  // should it be a null?
			object[] fields = new Object[oleReader.FieldCount];
			string[] fieldsStr = new string[oleReader.FieldCount];
			oleReader.GetValues(fields);
			for (int i = 0; i < fields.Length; i++)
			{
				if (fields[i] == DBNull.Value)
				{
					fieldsStr[i] = "";
				}
				else
				{
					fieldsStr[i] = fields[i].ToString();
				}
			}
			Next();
			return fieldsStr;
		}

		//// Not certain if the FieldCount is stable. ***
		private void SetupColumnNames()
		{
			if (oleReader.FieldCount <= 0)
			{
				cols = new string[0];
				return;
			}
			cols = new string[oleReader.FieldCount];
			for (int c = 0; c < oleReader.FieldCount; c++)
			{
				cols[c] = oleReader.GetName(c);
				if (cols[c] == null)
				{
					cols[c] = "";
				}
			}
		}

		/// <summary>
		/// Get the names of the columns.
		/// </summary>
		public string[] ColumnNames
		{
			get
			{
				return cols;
			}
		}


		/// <summary>
		/// Get the field at the column index.
		/// </summary>
		public object this[int i]
		{
			get
			{
				if (Eof())  return null;
				object item = oleReader[i];
				if (item == DBNull.Value)  return null;
				if (TrimWhitespace)
				{
					if (item.GetType() == typeof(String))
					{
						item = ((string)item).Trim();
					}
				}
				return item;
			}
		}

		/// <summary>
		/// Set the field at the column with the given header.
		/// </summary>
		public object this[string col]
		{
			get
			{
				if (Eof())  return null;
				object item = oleReader[col];
				if (item == DBNull.Value)  return null;
				if (TrimWhitespace)
				{
					if (item.GetType() == typeof(String))
					{
						item = ((string)item).Trim();
					}
				}
				return item;
			}
		}

		/// <summary>
		/// Get the field at the column index.
		/// </summary>
		public string this[int i, bool b]
		{
			get
			{
				object item = this[i];
				if (item == null)  return "";
				string res = item.ToString();
				if (TrimWhitespace)
				{
					res = res.Trim();
				}
				return res;
			}
		}

		/// <summary>
		/// Set the field at the column with the given header.
		/// </summary>
		public string this[string col, bool b]
		{
			get
			{
				object item = this[col];
				if (item == null)  return "";
				string res = item.ToString();
				if (TrimWhitespace)
				{
					res = res.Trim();
				}
				return res;
			}
		}

		/// <summary>
		/// Advance to the next row.
		/// </summary>
		/// <returns>true if there are more rows, false if at end of table</returns>
		public bool Next()
		{
			if (Eof() || oleReader == null)  return false;
			while (true)
			{
				rowNumber++;
				isEof = !oleReader.Read();
				if (isEof)  break;
				if (!SkipBlankLines && !SkipBlankColumnsLines)  break;
				if (oleReader.FieldCount == 0)  continue;  // does this happen?
				int col;
				for (col = 0; col < RowLength(); col++)
				{
					if (SkipBlankColumnsLines)
					{
						if (this[col, true] != "")  break;
					}
					else
					{
						if (this[col] != null)  break;
					}
				}
				if (col < RowLength())  break;
			}
			return !isEof;
		}

		/// <summary>
		/// Get the number of fields in the current row.
		/// </summary>
		/// <returns>the number of fields in the current row</returns>
		public int RowLength()
		{
			if (Eof())  return 0;
			return oleReader.FieldCount;
		}

		/// <summary>
		/// Check for end of file.
		/// </summary>
		/// <returns>true if at end of file, false otherwise</returns>
		public bool Eof()
		{
			return isEof;
		}

		/// <summary>
		/// Close the table.
		/// </summary>
		public void Close()
		{
			if (oleReader != null)
				oleReader.Close();
			oleReader = null;
			if (oleConn != null)
				oleConn.Close();
			oleConn = null;
			isEof = true;
		}
	}



	//// TODO: ODBC driver support?
	////       Bare CsvReader support / merging?
	////       Decent exception handling
	/// <summary>
	/// Write spreadsheet data in various formats.
	/// </summary>
	public class SpreadsheetWriter
	{
		private string filename;
		private bool xlsFormat;
		private OleDbConnection oleConn;
		//private OleDbDataAdapter oleAdapter;
		//private DataTable dataTable;
		//private string[] cols;

		// trim whitespace from each entry:
		private bool trimWhitespace = true;


		/// <summary>
		/// Gets or sets whether to trim whitespace from each field.
		/// </summary>
		public bool TrimWhitespace
		{
			get { return trimWhitespace; }
			set { trimWhitespace = value; }
		}


		/// <summary>
		/// Create a new SpreadsheetWriter.
		/// </summary>
		/// <param name="fname">the source of the spreadsheet</param>
		public SpreadsheetWriter(string fname)
		{
			filename = fname;

			// determine format:
			xlsFormat = false;
			//Console.WriteLine("Extension: " + Path.GetExtension(filename).ToLower());
			if (Path.GetExtension(filename).ToLower() == ".xls")
			{
				xlsFormat = true;
			}

			// construct connection string:
			string connString = "Provider=Microsoft.Jet.OLEDB.4.0;" +
				"Data Source=";
			if (xlsFormat) // Excel Format
			{
				connString += filename + ";" +
					//	"Extended Properties=Excel 8.0;";
					"Extended Properties=\"Excel 8.0;HDR=No\"";
			}
			else  // assume CSV
			{
				connString += Path.GetDirectoryName(Path.GetFullPath(filename)) + ";" +
					"Extended Properties=\"text;HDR=Yes;FMT=Delimited\"";
			}
			//Console.WriteLine("Opening with: " + connString);

			string selectString = "SELECT * FROM ";
			if (xlsFormat) // Excel Format
			{
				selectString += "[Sheet1$]";  // hard-coded first sheet? ***
			}
			else  // assume CSV
			{
				selectString += Path.GetFileName(filename);
			}
			
			//// open the connection:
			oleConn = new OleDbConnection(connString);
			oleConn.Open();

			//oleAdapter = new OleDbDataAdapter(selectString, connString);
			//oleAdapter = new OleDbDataAdapter(selectString, oleConn);
			//OleDbCommandBuilder oleCommandBuilder = new OleDbCommandBuilder(oleAdapter);
			//oleConn = oleAdapter.SelectCommand.Connection;
			//oleConn.Open();
			//dataTable = new DataTable("sheet");

			//oleAdapter.InsertCommand = new OleDbCommand();
			//oleAdapter.InsertCommand.CommandText = "INSERT INTO [Sheet1$]"; 
			//// VALUES ('Other','Figimingle','c:\\images\\gardenhose.bmp')";
			//oleAdapter.InsertCommand.Connection = oleConn;


			//OleDbCommand openCmd = new OleDbCommand(selectString, oleConn);
			////OleDbCommand openCmd = oleConn.CreateCommand();
			////openCmd.CommandText = selectString;
			//openCmd.Connection.Open();
			//Console.WriteLine("Opened connection.");
			//Console.WriteLine("Database:  " + oleConn.Database);
			//Console.WriteLine("Datasource:  " + oleConn.DataSource);

			// start up reader:
			//oleReader = openCmd.ExecuteReader(CommandBehavior.CloseConnection);
			////oleReader = openCmd.ExecuteReader();
			//Console.WriteLine("Started Reader.");

			//TestConnection();

			// Initialize the column names:
			//SetupColumnNames();
		}


		private string sqlString(string item)
		{
			return "'" + item.Replace("'", "''") + "'";
		}

		private string columnNameTrue(int col)
		{
			int aInt = (int)'A';
			int zInt = (int)'Z';
			int range = (zInt - aInt + 1);

			string res = "";
			int high = col / range;
			if (high > 0)
			{
				col = col % range;
				res += (char)(aInt + high - 1);
			}
			res += (char)(aInt + col);
			return res;
		}

		private string columnName(int col)
		{
			string res = "F";
			res += (col + 1);
			return res;
		}

		/// <summary>
		/// Write an entire row and advance the writer.
		/// </summary>
		/// <param name="fields">The row to write as an array of fields</param>
		public void WriteRow(object[] fields)
		{
			// widen if needed:
			//while (fields.Length > dataTable.Columns.Count)
			//{
			//	DataColumn col = new DataColumn();
			//	dataTable.Columns.Add(col);
			//}
			OleDbCommand oleCmd = new OleDbCommand();
			oleCmd.Connection = oleConn;
			string cmd = "INSERT INTO " + "[Sheet1$] "; // + "(FirstName, LastName) ";

			// give column names:
			cmd += "(";
			for (int i = 0;  i < fields.Length; i++)
			{
				if (i > 0)
				{
					cmd += ", ";
				}
				cmd += columnName(i);
			}
			cmd += ") ";

			cmd += "VALUES (";
			for (int i = 0; i < fields.Length; i++)
			{
				if (i > 0)
				{
					cmd += ", ";
				}
				cmd += sqlString(fields[i].ToString());
			}
			//'Bill', 'Brown'
			cmd += ")";
			oleCmd.CommandText = cmd;
			try
			{
				oleCmd.ExecuteNonQuery();
			}
			catch (Exception)
			{
				throw new Exception("Excel insert error.");
			}

			//if (oleAdapter == null)  return;
			// widen if needed:
			//while (fields.Length > dataTable.Columns.Count)
			//{
			//	DataColumn col = new DataColumn();
			//	dataTable.Columns.Add(col);
			//}
			//dataTable.LoadDataRow(fields, false);
			//for (int i = 0; i < fields.Length; i++)
			//{
			//	if (fields[i] == DBNull.Value)
			//	{
			//		fields[i] = null;
			//	}
			//}
		}

		/// <summary>
		/// Get the number of fields in the current row.
		/// </summary>
		/// <returns>the number of fields in the current row</returns>
		public int RowLength()
		{
			return 0;
			//return dataTable.Columns.Count;
		}


		/// <summary>
		/// Close the table.
		/// </summary>
		public void Close()
		{
			//if (oleAdapter != null)
			//{
			//	oleAdapter.Update(dataTable);
			//	oleAdapter = null;
			//}
			if (oleConn != null)
			{
				oleConn.Close();
				oleConn = null;
			}
		}
	}
#endif

}

