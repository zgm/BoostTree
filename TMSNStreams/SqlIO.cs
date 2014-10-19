using System;
using System.IO;
using System.Data;
using System.Data.OleDb;
using System.Collections;
using System.Collections.Specialized;
using System.Xml;
using System.Text;
using System.Data.Common;
using System.Data.SqlClient;

namespace Microsoft.TMSN.IO
{
	// WHERE, ORDER BY, column list
	// table creation, (deletion?)
	// path combining
	// directory support
	// wildcard pattern improvement
	// rows as files
	// line count / size
	// header line
	// check Exists for permission problems
	// transaction support
	// non-integrated authentication

	/// <summary>
	/// Reader for SQL Server tables.
	/// </summary>
	/// <remarks>
	/// <para>
	/// This reader makes SQL Server tables appear as standard text files. By default,
	/// they are tab-delimited for fields and newline-delimited for rows.
	/// </para>
	/// <para>
	/// When specified as a single string, tables are expected to be in the form
	/// "sql:server/db/table". A missing "server" element defaults to localhost,
	/// and a missing "db" element defaults to the default database. A query may
	/// optionally be specified with the syntax "sql:server/db/{query}".
	/// </para>
	/// </remarks>
	public class SqlTextReader : StreamReader
	{
		SqlDataReader sqlReader = null;
		SqlCommand openedCommand = null;
		StringBuilder currentLine = new StringBuilder();
		int currentPosition = 0;
		int rowSeperatorPosition = -1;
		string rowSeperator = "\n";
		string fieldSeperator = "\t";
		string headers;
		bool eof = false;
		bool allowMultipleResultSets = false;
		private static int timeout = 600;

		/// <summary>
		/// Get or Set a global timeout for SqlTextReader and SqlTextWriter operations, in seconds.
		/// The default is 600 seconds.
		/// </summary>
		public static int Timeout
		{
			get { return timeout; }
			set { timeout = value; }
		}


		#region Exists Checks

		/// <summary>
		/// Determine if a path is a SqlTextReader-formatted specification of a SQL table.
		/// </summary>
		/// <param name="path">the path to check</param>
		/// <returns>true if path is a SqlTextReader path; false otherwise</returns>
		/// <remarks>
		/// This checks the path textually, and does not verify that the given
		/// location actually exists.
		/// </remarks>
		public static bool IsSqlTextReader(string path)
		{
			return path != null && path.Length > "sql:".Length && path.StartsWith("sql:", StringComparison.OrdinalIgnoreCase);
		}

		internal static string PathRoot(string path)
		{
			int s = 4;
			//if (path.StartsWith("sql://", StringComparison.OrdinalIgnoreCase)) s = 6;
			s = path.IndexOf('/', s);
			if (s < 0) return null;
			s = path.IndexOf('/', s + 1);
			if (s < 0) return path + "/";
			return path.Substring(0, s + 1);
		}
		internal static string GetCanonicalPath(string path)
		{
			int s = 4;
			//if (path.StartsWith("sql://", StringComparison.OrdinalIgnoreCase)) s = 6;
			s = path.IndexOf('/', s);
			if (s < 0) return path;
			for (int i = 0; i < s; i++)
			{
				if (path[i] >='A' && path[i] <= 'Z')
				{
					path = path.Substring(0, s).ToLowerInvariant() + path.Substring(s);
					break;
				}
			}

			if (!path.EndsWith("/"))
			{
				int s2 = path.IndexOf('/', s + 1);
				if (s2 < 0)
				{
					path = path + "/";
				}
			}
			return path;
		}
		internal static string GetFileName(string path)
		{
			int s = 4;
			//if (path.StartsWith("sql://", StringComparison.OrdinalIgnoreCase)) s = 6;
			s = path.IndexOf('/', s);
			if (s < 0) return "";
			s = path.IndexOf('/', s + 1);
			if (s < 0) return "";
			return path.Substring(s + 1);
		}
		internal static string GetName(string path)
		{
			int s = 4;
			//if (path.StartsWith("sql://", StringComparison.OrdinalIgnoreCase)) s = 6;
			s = path.IndexOf('/', s);
			if (s < 0) return "";
			int s1 = s + 1;
			s = path.IndexOf('/', s1);
			if (s < 0) return path.Substring(s1);
			if (s == path.Length - 1) return path.Substring(s1, s - s1);
			return path.Substring(s + 1);
		}

		private static string SqlString(string item)
		{
			return "'" + item.Replace("'", "''") + "'";
		}

		/// <summary>
		/// Determine if a SQL table exists.
		/// </summary>
		/// <param name="path">the SQL table to look for, in SqlTextReader form</param>
		/// <returns>true if the SQL table exists, false otherwise</returns>
		/// <remarks>
		/// This is not extremely efficient to check, since it must contact the server.
		/// </remarks>
		public static bool Exists(string path)
		{
			//try
			//{
			string server;
			string db;
			string table;
			bool isQuery;
			if (!ParsePath(path, out server, out db, out table, out isQuery) || table == null)
			{
				return false;
			}
			string cnString = GetConnectionString(server, db);
				SqlConnection cn = null;
				SqlCommand cmd = null;
				SqlDataReader sqlReader = null;
				try
				{
					cn = new SqlConnection(cnString);
					cmd = new SqlCommand(
						"SELECT COUNT(name) FROM sys.sysobjects WHERE (xtype = 'U' OR xtype = 'V') AND name=" + SqlString(table),
						cn);
					if (cmd.Connection.State == System.Data.ConnectionState.Closed)
					{
						cmd.Connection.Open();
					}
					sqlReader = cmd.ExecuteReader(CommandBehavior.CloseConnection |
						CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);
					if (sqlReader.Read())
					{
						return sqlReader.GetInt32(0) != 0;
					}
					return false;
				}
				catch
				{
					// ignore
					return false;
				}
				finally
				{
					if (cmd != null)
					{
						try
						{
							cmd.Cancel();
							cmd.Dispose();
						}
						catch
						{
						}
					}
					if (sqlReader != null)
					{
						try
						{
							sqlReader.Close();
						}
						catch
						{
						}
					}
					if (cn != null)
					{
						if (cn.State != ConnectionState.Closed)
						{
							try
							{
								cn.Close();
							}
							catch
							{
							}
						}
					}
				}
			//}
			//catch
			//{
			//    if (path == null || path.Length == 0) return false;
			//    try
			//    {
			//        //Console.WriteLine("Testing...");
			//        // this is particularly bad for queries with an ORDER BY...
			//        using (SqlTextReader s = new SqlTextReader(path))
			//        {
			//        }
			//    }
			//    catch
			//    {
			//        //Console.WriteLine("Exists: false");
			//        return false;
			//    }
			//    //Console.WriteLine("Exists: true");
			//    return true;
			//}
		}

		/// <summary>
		/// Get the paths to tables within a sql: database path.
		/// </summary>
		/// <param name="path">the database to look in, in the form "sql:server/db" or "sql:server/db/"</param>
		/// <returns>the set of paths for the tables in that database in the form "sql:server/db/table"</returns>
		/// <remarks>
		/// <p>
		/// This will silently return the empty list if there are any problems.
		/// </p>
		/// </remarks>
		public static string[] DatabaseTablePaths(string path)
		{
			string server;
			string db;
			string table;
			bool isQuery;
			if (!ParsePath(path, out server, out db, out table, out isQuery) || table != null)
			{
				return new string[0];
			}
			string[] tables = DatabaseTables(server, db);
			string[] res = new string[tables.Length];
			if (!path.EndsWith("/")) path = path + "/";
			for (int i = 0; i < res.Length; i++)
			{
				res[i] = path + tables[i];
			}
			return res;
		}

		////SELECT name
		////FROM sys.sysobjects
		////WHERE xtype = 'U' OR xtype = 'V'
		////ORDER BY name

		/// <summary>
		/// Get the paths to tables within a dtabase on a SQL Server.
		/// </summary>
		/// <param name="server">the server to look in</param>
		/// <param name="database">the database to look in</param>
		/// <returns>the list of names for the tables in that database</returns>
		/// <remarks>
		/// <p>
		/// This will silently return the empty list if there are any problems.
		/// </p>
		/// </remarks>
		public static string[] DatabaseTables(string server, string database)
		{
			string cnString = GetConnectionString(server, database);
			SqlConnection cn = null;
			SqlCommand cmd = null;
			SqlDataReader sqlReader = null;
			try
			{
				cn = new SqlConnection(cnString);
				cmd = new SqlCommand(
					"SELECT name FROM sys.sysobjects WHERE xtype = 'U' OR xtype = 'V' ORDER BY name",
					cn);
				if (cmd.Connection.State == System.Data.ConnectionState.Closed)
				{
					cmd.Connection.Open();
				}
				sqlReader = cmd.ExecuteReader(CommandBehavior.CloseConnection |
					CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
				ArrayList res = new ArrayList();
				while (sqlReader.Read())
				{
					string name = sqlReader.GetString(0);
					//Console.WriteLine(":: " + name);
					res.Add(name);
				}
				return (string[])res.ToArray(typeof(string));
			}
			catch
			{
				// ignore
				return new string[0];
			}
			finally
			{
				if (cmd != null)
				{
					try
					{
						cmd.Cancel();
						cmd.Dispose();
					}
					catch
					{
					}
				}
				if (sqlReader != null)
				{
					try
					{
						sqlReader.Close();
					}
					catch
					{
					}
				}
				if (cn != null)
				{
					if (cn.State != ConnectionState.Closed)
					{
						try
						{
							cn.Close();
						}
						catch
						{
						}
					}
				}
			}
		}

		internal static bool ExistsDatabase(string path)
		{
			string server;
			string db;
			string table;
			bool isQuery;
			if (!ParsePath(path, out server, out db, out table, out isQuery)) return false;
			if (table != null) return false;
			return ExistsDatabase(server, db);
		}
		internal static bool ExistsDatabase(string server, string database)
		{
			string cnString = GetConnectionString(server, database);
			SqlConnection cn = null;
			SqlCommand cmd = null;
			SqlDataReader sqlReader = null;
			try
			{
				cn = new SqlConnection(cnString);
				//cmd = new SqlCommand(
				//    "SELECT name FROM sys.sysobjects WHERE xtype = 'U' OR xtype = 'V' ORDER BY name",
				//    cn);
				//if (cmd.Connection.State == System.Data.ConnectionState.Closed)
				//{
				//    cmd.Connection.Open();
				//}
				//sqlReader = cmd.ExecuteReader();
				cmd = new SqlCommand(
					"SELECT TOP 1 name FROM sys.sysobjects",
					cn);
				if (cmd.Connection.State == System.Data.ConnectionState.Closed)
				{
					cmd.Connection.Open();
				}
				cmd.ExecuteNonQuery();
				return true;
			}
			catch
			{
				return false;
			}
			finally
			{
				if (cmd != null)
				{
					try
					{
						cmd.Cancel();
						cmd.Dispose();
					}
					catch
					{
					}
				}
				if (sqlReader != null)
				{
					try
					{
						sqlReader.Close();
					}
					catch
					{
					}
				}
				if (cn != null)
				{
					if (cn.State != ConnectionState.Closed)
					{
						try
						{
							cn.Close();
						}
						catch
						{
						}
					}
				}
			}
		}

		#endregion


		internal static bool ParsePath(string path, out string server, out string database, out string table, out bool isQuery)
		{
			server = null;
			database = null;
			table = null;
			isQuery = false;
			if (path == null) return false;
			if (path.Length == 0) return false;
			if (path.StartsWith("sql:", StringComparison.OrdinalIgnoreCase))
			{
				path = path.Substring("sql:".Length);
			}
			//if (path.StartsWith("//"))
			//{
			//    // allow for "sql://", if needed:
			//    if (path.Length < 3) return false;
			//    int s1 = path.IndexOf('/', 3);
			//    if (s1 > 0)
			//    {
			//        int c1 = path.IndexOf(':');
			//        if (c1 < 0 || s1 < c1)
			//        {
			//            path = path.Substring(2);
			//        }
			//    }
			//}
			//if (path.Length == 0) return false;
			int s = path.IndexOf('/');
			if (s < 0) return false;
			server = path.Substring(0, s);
			path = path.Substring(s + 1);
			if (path.Length == 0) return false;
			//int dbEnd = path.IndexOf('/');
			//int c = path.IndexOf('{');
			//if (dbEnd < 0 || c >= 0 && c < dbEnd)
			//{
			//    // allow query in the form "sql:server/db{query}"
			//    if (c < 0) return false;
			//    isQuery = true;
			//    database = path.Substring(0, c);
			//    int qEnd = path.Length;
			//    if (path.EndsWith("}")) qEnd = path.Length - 1;
			//    table = path.Substring(c + 1, qEnd - c - 1);
			//}
			//else
			//{
			//    database = path.Substring(0, dbEnd);
			//    table = path.Substring(dbEnd + 1);
			//    if (table.StartsWith("{"))
			//    {
			//        // allow query in the form "sql:server/db/{query}"
			//        isQuery = true;
			//        int qEnd = table.Length;
			//        if (table.EndsWith("}")) qEnd = table.Length - 1;
			//        table = table.Substring(1, qEnd - 1);
			//    }
			//}
			int dbEnd = path.IndexOf('/');
			if (dbEnd < 0)
			{
				database = path;
				table = null;
				return true;
			}

			database = path.Substring(0, dbEnd);
			table = path.Substring(dbEnd + 1).Trim();
			if (table.Length == 0)
			{
				table = null;
			}
			else
			{
				if (table.StartsWith("{"))
				{
					// allow query in the form "sql:server/db/{query}"
					isQuery = true;
					int qEnd = table.Length;
					if (table.EndsWith("}")) qEnd = table.Length - 1;
					table = table.Substring(1, qEnd - 1);
				}
			}
			return true;
		}


		/// <summary>
		/// Create a new reader for the given table or query.
		/// </summary>
		/// <param name="path">The table or query, as a SqlTextReader path.</param>
		/// <remarks>
		/// <para>
		/// When specified as a single string, tables are expected to be in the form
		/// "sql:server/db/table". A missing "server" element defaults to localhost,
		/// and a missing "db" element defaults to the default database. A query may
		/// optionally be specified with the syntax "sql:server/db/{query}".
		/// </para>
		/// </remarks>
		public SqlTextReader(string path)
			: this()
		{
			string server;
			string db;
			string table;
			bool isQuery;
			if (!ParsePath(path, out server, out db, out table, out isQuery) || table == null)
			{
				throw new ArgumentException("path", "Malformed SQL streamname '" + path + "'");
			}
			if (isQuery)
			{
				LoadQuery(server, db, table);
			}
			else
			{
				LoadTable(server, db, table);
			}
		}

		/// <summary>
		/// Create a new reader for the given server, database, and table.
		/// </summary>
		/// <param name="server">The server to connect to. If null, use localhost.</param>
		/// <param name="database">The database to connect to. If null, use the default.</param>
		/// <param name="table">The table to read from.</param>
		public SqlTextReader(string server, string database, string table)
			: this()
		{
			LoadTable(server, database, table);
		}

		/// <summary>
		/// Create a new reader for the given server, database, and query.
		/// </summary>
		/// <param name="server">The server to connect to. If null, use localhost.</param>
		/// <param name="database">The database to connect to. If null, use the default.</param>
		/// <param name="table">Ignored.</param>
		/// <param name="query">The query to execute.</param>
		public SqlTextReader(string server, string database, string table, string query)
			: this()
		{
			LoadQuery(server, database, query);
		}

		internal static string GetConnectionString(string server, string database)
		{
			if (server == null || server.Length == 0) server = "localhost";
			string cnString = "Server=" + server + ";";
			if (database != null && database.Length != 0)
			{
				cnString += "Database=" + database + ";";
			}
			cnString += "Persist Security Info=True;Enlist=false;";
			cnString += "Timeout=" + Timeout + ";Connect Timeout=" + Timeout + ";";
			cnString += "Integrated Security=true;";
			//cnString += "Integrated Security=false;User ID=" + username + ";Password=" + password + ";";
			//Console.WriteLine(cnString);
			return cnString;
		}

		private void LoadTable(string server, string database, string table)
		{
			string cnString = GetConnectionString(server, database);
			SqlConnection cn = null;
			try
			{
				cn = new SqlConnection(cnString);
				SqlCommand cmd = new SqlCommand("SELECT * FROM [" + table + "]", cn);
				if (cmd.Connection.State == System.Data.ConnectionState.Closed)
				{
					cmd.Connection.Open();
					openedCommand = cmd;
				}
				//this.sqlReader = cmd.ExecuteReader();
				this.sqlReader = cmd.ExecuteReader(CommandBehavior.CloseConnection |
					CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
				FillHeaders();
			}
			catch
			{
				if (cn != null)
				{
					try
					{
						cn.Close();
					}
					catch
					{
					}
				}
				throw;
			}
		}

		private void LoadQuery(string server, string database, string query)
		{
			allowMultipleResultSets = true;
			string cnString = GetConnectionString(server, database);
			SqlConnection cn = null;
			try
			{
				cn = new SqlConnection(cnString);
				SqlCommand cmd = new SqlCommand(query, cn);
				if (cmd.Connection.State == System.Data.ConnectionState.Closed)
				{
					cmd.Connection.Open();
					openedCommand = cmd;
				}
				//this.sqlReader = cmd.ExecuteReader();
				sqlReader = cmd.ExecuteReader(CommandBehavior.CloseConnection |
					CommandBehavior.SequentialAccess);
				FillHeaders();
			}
			catch
			{
				if (cn != null)
				{
					try
					{
						cn.Close();
					}
					catch
					{
					}
				}
				throw;
			}
		}

		private void FillHeaders()
		{
			int c = sqlReader.FieldCount;
			string[] header = new string[c];
			for (int i = 0; i < c; i++)
			{
				header[i] = sqlReader.GetName(i);
			}
			headers = string.Join("\t", header);
		}

		/// <summary>
		/// Create a new reader for the given SqlDataReader.
		/// </summary>
		/// <param name="sqlReader">The SqlDataReader to wrap</param>
		public SqlTextReader(SqlDataReader sqlReader)
			: this()
		{
			allowMultipleResultSets = true;
			this.sqlReader = sqlReader;
			FillHeaders();
		}
		/// <summary>
		/// Create a new reader for the given SqlCommand.
		/// </summary>
		/// <param name="cmd">The SqlCommand to execute and wrap</param>
		public SqlTextReader(SqlCommand cmd)
			: this()
		{
			allowMultipleResultSets = true;
			if (cmd.Connection.State == System.Data.ConnectionState.Closed)
			{
				cmd.Connection.Open();
				openedCommand = cmd;
			}
			//this.sqlReader = cmd.ExecuteReader();
			this.sqlReader = cmd.ExecuteReader(CommandBehavior.CloseConnection |
				CommandBehavior.SequentialAccess);
			FillHeaders();
		}

		private SqlTextReader()
			: base(Stream.Null)
		{
		}

		/// <summary>
		/// Get the names of the columns.
		/// </summary>
		public string[] GetHeaders()
		{
			return HeaderLine.Split('\t');
		}

		/// <summary>
		/// Get the names of the columns.
		/// </summary>
		public string HeaderLine
		{
			get
			{
				// clearly, not ideal to return this not read-only
				return headers;
			}
		}

		/// <summary>
		/// Gets or Sets the string appended to every row. This defaults to a newline and can
		/// be the empty string if desired. If it is null, it will default to the field
		/// separator.
		/// </summary>
		public string RowSeperator
		{
			get { return (rowSeperator == null) ? FieldSeperator : rowSeperator; }
			set { rowSeperator = value; }
		}

		/// <summary>
		/// Gets or Sets the string appended to every field. This defaults to a tab and can
		/// be the empty string if desired.
		/// </summary>
		public string FieldSeperator
		{
			get { return fieldSeperator; }
			set { fieldSeperator = value; }
		}

		/// <summary>Reads a maximum of count characters from the current stream and writes the data to buffer, beginning at index.</summary>
		/// <returns>The number of characters that have been read. The number will be less than or equal to count, depending on whether all input characters have been read.</returns>
		/// <param name="count">The maximum number of characters to read. </param>
		/// <param name="buffer">When this method returns, this parameter contains the specified character array with the values between index and (index + count -1) replaced by the characters read from the current source. </param>
		/// <param name="index">The place in buffer at which to begin writing. </param>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		/// <exception cref="System.ArgumentOutOfRangeException">index or count is negative. </exception>
		/// <exception cref="System.ArgumentException">The buffer length minus index is less than count. </exception>
		/// <exception cref="System.ArgumentNullException">buffer is null. </exception>
		/// <exception cref="System.ObjectDisposedException">The reader is closed. </exception>
		public override int Read(char[] buffer, int index, int count)
		{
			// leave as inefficient for now? ***
			//return ReadBlock(buffer, index, count);
			return base.Read(buffer, index, count);
		}
		/// <summary>Reads a maximum of count characters from the current stream and writes the data to buffer, beginning at index.</summary>
		/// <returns>The number of characters that have been read. The number will be less than or equal to count, depending on whether all input characters have been read.</returns>
		/// <param name="count">The maximum number of characters to read. </param>
		/// <param name="buffer">When this method returns, this parameter contains the specified character array with the values between index and (index + count -1) replaced by the characters read from the current source. </param>
		/// <param name="index">The place in buffer at which to begin writing. </param>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		/// <exception cref="System.ArgumentOutOfRangeException">index or count is negative. </exception>
		/// <exception cref="System.ArgumentException">The buffer length minus index is less than count. </exception>
		/// <exception cref="System.ArgumentNullException">buffer is null. </exception>
		/// <exception cref="System.ObjectDisposedException">The reader is closed. </exception>
		public override int ReadBlock(char[] buffer, int index, int count)
		{
			// leave as inefficient for now? ***
			return base.ReadBlock(buffer, index, count);
		}

		/// <summary>Reads a line of characters from the current stream and returns the data as a string.</summary>
		/// <returns>The next line from the input stream, or null if all characters have been read.</returns>
		/// <exception cref="System.ArgumentOutOfRangeException">The number of characters in the next line is larger than <see cref="F:System.Int32.MaxValue"></see></exception>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		/// <exception cref="System.OutOfMemoryException">There is insufficient memory to allocate a buffer for the returned string. </exception>
		/// <exception cref="System.ObjectDisposedException">The reader is closed. </exception>
		public override string ReadLine()
		{
			// ignore being in middle of RowSeperator? ***
			if (eof) return null;
			if (currentPosition >= currentLine.Length)
			{
				NextLine();
			}
			if (eof) return null;
			if (currentPosition != 0)
			{
				currentLine.Remove(0, currentPosition);
			}
			string res = currentLine.ToString();
			currentPosition = currentLine.Length;
			return res;
		}

		/// <summary>Reads all characters from the current position to the end of the TextReader and returns them as one string.</summary>
		/// <returns>A string containing all characters from the current position to the end of the TextReader.</returns>
		/// <exception cref="System.ArgumentOutOfRangeException">The number of characters in the next line is larger than <see cref="F:System.Int32.MaxValue"></see></exception>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		/// <exception cref="System.OutOfMemoryException">There is insufficient memory to allocate a buffer for the returned string. </exception>
		/// <exception cref="System.ObjectDisposedException">The reader is closed. </exception>
		public override string ReadToEnd()
		{
			if (eof) return "";
			StringBuilder res = new StringBuilder();
			if (currentPosition < currentLine.Length)
			{
				res.Append(ReadLine());
				if (RowSeperator != null && RowSeperator.Length != 0)
				{
					res.Append(RowSeperator);
				}
			}
			currentLine = new StringBuilder();
			do
			{
				while (sqlReader.Read())
				{
					for (int fieldIndex = 0; fieldIndex < sqlReader.FieldCount; fieldIndex++)
					{
						object curObj = sqlReader.IsDBNull(fieldIndex) ? null :
							sqlReader.GetValue(fieldIndex);
						if (curObj == null)
						{
							//currentLine.Append("");
						}
						else
						{
							res.Append(curObj.ToString());
						}
						if (fieldIndex < sqlReader.FieldCount - 1)
						{
							if (FieldSeperator != null && FieldSeperator.Length != 0)
							{
								res.Append(FieldSeperator);
							}
						}
					}
					if (RowSeperator != null && RowSeperator.Length != 0)
					{
						res.Append(RowSeperator);
					}
				}
			}
			while (allowMultipleResultSets && sqlReader.NextResult());
			eof = true;
			return res.ToString();
		}

		private void NextLine()
		{
			if (eof) return;
			if (!sqlReader.Read())
			{
				if (allowMultipleResultSets && sqlReader.NextResult())
				{
					NextLine();
					return;
				}
				eof = true;
				currentLine = null;
				return;
			}

			currentLine.Length = 0;
			for (int fieldIndex = 0; fieldIndex < sqlReader.FieldCount; fieldIndex++)
			{
				object curObj = sqlReader.IsDBNull(fieldIndex) ? null :
					sqlReader.GetValue(fieldIndex);
				if (curObj == null)
				{
					//currentLine.Append("");
				}
				else
				{
					currentLine.Append(curObj.ToString());
				}
				if (fieldIndex < sqlReader.FieldCount - 1)
				{
					if (FieldSeperator != null && FieldSeperator.Length != 0)
					{
						currentLine.Append(FieldSeperator);
					}
				}
			}
			currentPosition = 0;
			rowSeperatorPosition = 0;
		}

		/// <summary>
		/// Return the next character without advancing the reader, or -1 if
		/// at end of stream.
		/// </summary>
		/// <returns>the next character, or -1 if at end of stream</returns>
		public override int Peek()
		{
			if (eof) return -1;
			if (currentPosition < currentLine.Length)
			{
				int res = currentLine[currentPosition];
				//currentPosition++;
				return res;
			}
			if (rowSeperatorPosition >= 0 && RowSeperator != null && rowSeperatorPosition < RowSeperator.Length)
			{
				int res = RowSeperator[rowSeperatorPosition];
				//rowSeperatorPosition++;
				return res;
			}
			NextLine();
			return Peek();
		}

		/// <summary>
		/// Read the next character and advance the stream.
		/// </summary>
		/// <returns>the next character, or -1 if at end of stream</returns>
		public override int Read()
		{
			if (eof) return -1;
			if (currentPosition < currentLine.Length)
			{
				int res = currentLine[currentPosition];
				currentPosition++;
				return res;
			}
			if (rowSeperatorPosition >= 0 && RowSeperator != null && rowSeperatorPosition < RowSeperator.Length)
			{
				int res = RowSeperator[rowSeperatorPosition];
				rowSeperatorPosition++;
				return res;
			}
			NextLine();
			return Read();
		}

		/// <summary>
		/// Get the base stream - always throws an exception.
		/// </summary>
		public override Stream BaseStream
		{
			get
			{
				throw new NotSupportedException("SqlTextReader has no BaseStream.");
			}
		}

		/// <summary>
		/// Close the connection if needed.
		/// </summary>
		~SqlTextReader()
		{
			// will this break things? ***
			if (openedCommand != null)
			{
				try
				{
					openedCommand.Connection.Close();
				}
				catch
				{
				}
			}
		}
		/// <summary>
		/// Release any resources.
		/// </summary>
		/// <param name="disposing"></param>
		protected override void Dispose(bool disposing)
		{
			//Console.WriteLine("Disposing...");
			if (sqlReader != null)
			{
				//try
				//{
					try
					{
						openedCommand.Cancel();
					}
					catch
					{
					}
					try
					{
						sqlReader.Close();
						sqlReader.Dispose();
					}
					catch
					{
					}
					try
					{
						openedCommand.Connection.Close();
					}
					catch
					{
					}
					try
					{
						openedCommand.Dispose();
					}
					catch
					{
					}
				//}
				//catch (Exception ex)
				//{
				//    // ignore? ***
				//    Console.Error.WriteLine(ex.Message);
				//}
				sqlReader = null;
				openedCommand = null;
			}
			//Console.WriteLine("Reader closed...");
			if (openedCommand != null)
			{
				openedCommand.Cancel();
				if (openedCommand.Connection.State != ConnectionState.Closed)
				{
					try
					{
						openedCommand.Connection.Close();
					}
					catch
					{
						// ignore
					}
				}
				openedCommand = null;
			}
			currentLine = null;
			eof = true;

			base.Dispose(disposing);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// Close the reader.
		/// </summary>
		public override void Close()
		{
			Dispose(false);
		}

	}



	/// <summary>
	/// Writer for SQL Server tables.
	/// </summary>
	/// <remarks>
	/// <para>
	/// This writer makes SQL Server tables appear as standard text files. By default,
	/// input is expected to be tab-delimited for fields and newline-delimited for rows.
	/// </para>
	/// <para>
	/// When specified as a single string, tables are expected to be in the form
	/// "sql:server/db/table". A missing "server" element defaults to localhost,
	/// and a missing "db" element defaults to the default database.
	/// </para>
	/// </remarks>
	public class SqlTextWriter : StreamWriter
	{
		SqlConnection cn = null;
		SqlBulkCopy sqlBulkCopy = null;
		StringBuilder currentLine = new StringBuilder();
		RowCollection rows;
		private Type[] fieldTypes;
		string[] typeNames;
		string[] fieldNames;
		char fieldSeperator = '\t';
		int batchSize = 8;

		/// <summary>
		/// Get or Set a global timeout for SqlTextReader and SqlTextWriter operations, in seconds.
		/// The default is 600 seconds.
		/// </summary>
		public static int Timeout
		{
			get { return SqlTextReader.Timeout; }
			set { SqlTextReader.Timeout = value; }
		}

		private class RowCollection : IDataReader
		{
			Var[][] rows;
			Type[] types;
			string[] typeNames;
			string[] fieldNames;
			int current;
			int end;

			public void Clear()
			{
				current = -1;
				end = 0;
			}
			public void Add(string[] vals)
			{
				Var[] cur = rows[end];
				if (cur.Length != vals.Length) throw new InvalidDataException("Wrong column count in data");
				end++;
				for (int i = 0; i < vals.Length; i++)
				{
					cur[i] = new Var(vals[i]);
				}
			}
			public bool IsFull
			{
				get { return end >= rows.Length; }
			}
			public bool IsEmpty
			{
				get { return end == 0; }
			}

			public RowCollection(int count, string[] fieldNames, Type[] types, string[] typeNames)
			{
				this.rows = new Var[count][];
				for (int i = 0; i < rows.Length; i++)
				{
					rows[i] = new Var[types.Length];
				}
				end = 0;
				current = -1;
				this.types = types;
				this.typeNames = typeNames;
				this.fieldNames = fieldNames;
			}

			#region IDataReader Members

			public void Close()
			{
			}

			public int Depth
			{
				get { return 1; }
			}

			public DataTable GetSchemaTable()
			{
				throw new Exception("The method or operation is not implemented.");
			}

			public bool IsClosed
			{
				get { return false; }
			}

			public bool NextResult()
			{
				current++;
				return (current < end);
			}

			public bool Read()
			{
				current++;
				return (current < end);
			}

			public int RecordsAffected
			{
				get { return 0; }
			}

			#endregion

			#region IDisposable Members

			public void Dispose()
			{
			}

			#endregion

			#region IDataRecord Members

			public int FieldCount
			{
				get { return types.Length; }
			}

			public bool GetBoolean(int i)
			{
				return rows[current][i];
			}

			public byte GetByte(int i)
			{
				return rows[current][i];
			}

			public char GetChar(int i)
			{
				return rows[current][i];
			}

			public DateTime GetDateTime(int i)
			{
				return DateTime.Parse(rows[current][i]);
			}

			public decimal GetDecimal(int i)
			{
				return rows[current][i];
			}

			public double GetDouble(int i)
			{
				return rows[current][i];
			}

			public float GetFloat(int i)
			{
				return rows[current][i];
			}

			public Guid GetGuid(int i)
			{
				return rows[current][i];
			}

			public short GetInt16(int i)
			{
				return rows[current][i];
			}

			public int GetInt32(int i)
			{
				return rows[current][i];
			}

			public long GetInt64(int i)
			{
				return rows[current][i];
			}

			public string GetString(int i)
			{
				return rows[current][i];
			}

			public bool IsDBNull(int i)
			{
				return rows[current][i] == null || (string)rows[current][i] == null;
			}

			public object this[string name]
			{
				get { return GetValue(GetOrdinal(name)); }
			}

			public object this[int i]
			{
				get { return GetValue(i); }
			}

			public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
			{
				throw new Exception("The method or operation is not implemented.");
			}

			public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
			{
				throw new Exception("The method or operation is not implemented.");
			}

			public IDataReader GetData(int i)
			{
				return null;
			}

			public string GetDataTypeName(int i)
			{
				return typeNames[i];
			}

			public Type GetFieldType(int i)
			{
				return types[i];
			}

			public object GetValue(int i)
			{
				//return Convert.ChangeType(rows[current][i], types[i]);
				return rows[current][i];
			}

			public int GetValues(object[] values)
			{
				//object[] res = new object[rows[current].Length];
				int max = Math.Min(values.Length, rows[current].Length);
				for (int i = 0; i < max; i++)
				{
					values[i] = GetValue(i);
				}
				return max;
			}

			public string GetName(int i)
			{
				return fieldNames[i];
			}

			public int GetOrdinal(string name)
			{
				return Array.IndexOf<string>(fieldNames, name);
			}

			#endregion
		}

		/// <summary>
		/// Create a new writer for the given table.
		/// </summary>
		/// <param name="path">The table or query, as a SqlTextReader path.</param>
		/// <remarks>
		/// <para>
		/// When specified as a single string, tables are expected to be in the form
		/// "sql:server/db/table". A missing "server" element defaults to localhost,
		/// and a missing "db" element defaults to the default database. A query may
		/// optionally be specified with the syntax "sql:server/db/{query}".
		/// </para>
		/// </remarks>
		public SqlTextWriter(string path)
			: this()
		{
			string server;
			string db;
			string table;
			bool isQuery;
			if (!SqlTextReader.ParsePath(path, out server, out db, out table, out isQuery) || table == null)
			{
				throw new ArgumentException("path", "Malformed SQL streamname '" + path + "'");
			}
			if (isQuery)
			{
				throw new ArgumentException("path", "Cannot open a query for writing: '" + path + "'");
			}
			else
			{
				OpenTable(server, db, table);
			}
		}

		/// <summary>
		/// Create a new reader for the given server, database, and table.
		/// </summary>
		/// <param name="server">The server to connect to. If null, use localhost.</param>
		/// <param name="database">The database to connect to. If null, use the default.</param>
		/// <param name="table">The table to read from.</param>
		public SqlTextWriter(string server, string database, string table)
			: this()
		{
			OpenTable(server, database, table);
		}


		private void OpenTable(string server, string database, string table)
		{
			string cnString = SqlTextReader.GetConnectionString(server, database);

			// get the types
			SqlCommand cmd = null;
			SqlDataReader sqlReader = null;
			try
			{
				cn = new SqlConnection(cnString);
				if (cn.State == System.Data.ConnectionState.Closed)
				{
					cn.Open();
				}
				cmd = new SqlCommand("SELECT TOP 1 * FROM [" + table + "]", cn);
				//sqlReader = cmd.ExecuteReader();
				sqlReader = cmd.ExecuteReader(CommandBehavior.CloseConnection |
					CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow |
					CommandBehavior.SchemaOnly);
				int c = sqlReader.FieldCount;
				fieldTypes = new Type[c];
				typeNames = new string[c];
				fieldNames = new string[c];
				for (int i = 0; i < c; i++)
				{
					fieldTypes[i] = sqlReader.GetFieldType(i);
					typeNames[i] = sqlReader.GetDataTypeName(i);
					fieldNames[i] = sqlReader.GetName(i);
				}
			}
			finally
			{
				if (cmd != null)
				{
					try
					{
						cmd.Cancel();
					}
					catch
					{
					}
				}
				if (sqlReader != null)
				{
					try
					{
						sqlReader.Close();
						sqlReader.Dispose();
					}
					catch
					{
					}
				}
				if (cn != null)
				{
					try
					{
						cn.Close();
					}
					catch
					{
					}
				}
				if (cmd != null)
				{
					try
					{
						cmd.Dispose();
					}
					catch
					{
					}
				}
			}


			rows = new RowCollection(batchSize, fieldNames, fieldTypes, typeNames);


			// open a bulk copy
			try
			{
				cn = new SqlConnection(cnString);
				if (cn.State == System.Data.ConnectionState.Closed)
				{
					cn.Open();
				}
				sqlBulkCopy = new SqlBulkCopy(cn);
				sqlBulkCopy.BatchSize = batchSize;
				sqlBulkCopy.DestinationTableName = table;
				sqlBulkCopy.BulkCopyTimeout = Timeout;
				//sqlBulkCopy.ColumnMappings
			}
			catch
			{
				try
				{
					cn.Close();
				}
				catch
				{
				}
				throw;
			}
		}

		private SqlTextWriter()
			: base(Stream.Null)
		{
		}

		///// <summary>
		///// Gets or Sets the string appended to every row. This defaults to a newline and can
		///// be the empty string if desired. If it is null, it will default to the field
		///// separator.
		///// </summary>
		//public string RowSeperator
		//{
		//    get { return (rowSeperator == null) ? FieldSeperator : rowSeperator; }
		//    set { rowSeperator = value; }
		//}

		/// <summary>
		/// Gets or Sets the char separating the fields. This defaults to a tab.
		/// </summary>
		public char FieldSeperator
		{
			get { return fieldSeperator; }
			set { fieldSeperator = value; }
		}


		/// <summary>Clears all buffers for the current writer and causes any buffered data to be written to the underlying stream.</summary>
		/// <exception cref="System.IO.IOException">An I/O error has occurred. </exception>
		/// <exception cref="System.ObjectDisposedException">The current writer is closed. </exception>
		public override void Flush()
		{
			Flush(false);
		}
		private void Flush(bool final)
		{
			if (final)
			{
				WriteLine();
			}

			if (!rows.IsEmpty)
			{
				sqlBulkCopy.WriteToServer(rows);
				rows.Clear();
			}
		}
		/// <summary>Writes a character array to the stream.</summary>
		/// <param name="buffer">A character array containing the data to write. If buffer is null, nothing is written. </param>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		public override void Write(char[] buffer)
		{
			currentLine.Append(buffer);
			if (Array.IndexOf<char>(buffer, '\n') >= 0)
			{
				SubmitLines();
			}
		}
		/// <summary>Writes a character to the stream.</summary>
		/// <param name="value">The character to write to the text stream. </param>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		public override void Write(char value)
		{
			if (value == '\n')
			{
				SubmitCurrent();
				//SubmitLines();
			}
			else
			{
				currentLine.Append(value);
			}
		}
		/// <summary>Writes a string to the stream.</summary>
		/// <param name="value">The string to write to the stream. If value is null, nothing is written. </param>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		public override void Write(string value)
		{
			currentLine.Append(value);
			if (value != null && value.IndexOf('\n') >= 0)
			{
				SubmitLines();
			}
		}
		/// <summary>Writes a subarray of characters to the stream.</summary>
		/// <param name="count">The number of characters to read from buffer. </param>
		/// <param name="buffer">A character array containing the data to write. </param>
		/// <param name="index">The index into buffer at which to begin writing. </param>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		/// <exception cref="System.ArgumentOutOfRangeException">index or count is negative. </exception>
		/// <exception cref="System.ArgumentException">The buffer length minus index is less than count. </exception>
		/// <exception cref="System.ArgumentNullException">buffer is null. </exception>
		public override void Write(char[] buffer, int index, int count)
		{
			currentLine.Append(buffer, index, count);
			if (Array.IndexOf<char>(buffer, '\n', index, count) >= 0)
			{
				SubmitLines();
			}
		}
		/// <summary>Writes a newline to the stream.</summary>
		/// <exception cref="System.ObjectDisposedException"><see cref="System.IO.StreamWriter.AutoFlush"></see> is true or the <see cref="T:System.IO.StreamWriter"></see> buffer is full, and current writer is closed. </exception>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		public override void WriteLine()
		{
			//Write('\n');
			SubmitCurrent();
		}
		/// <summary>Writes a string to the stream, followed by a newline.</summary>
		/// <param name="value">The string to write to the stream. If value is null, only the newline is written. </param>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		public override void WriteLine(string value)
		{
			Write(value);
			WriteLine();
		}

		private void SubmitCurrent()
		{
			if (currentLine.Length == 0) return;
			if (currentLine[currentLine.Length - 1] == '\r') currentLine.Length = currentLine.Length - 1;
			if (currentLine.Length == 0) return;
			SubmitLine(currentLine.ToString());
			currentLine.Length = 0;
		}
		private void SubmitLines()
		{
			if (currentLine.Length == 0) return;
			string line = currentLine.ToString();
			int start = 0;
			int end = 0;
			while (end < currentLine.Length)
			{
				if (currentLine[end] == '\n')
				{
					int next = end + 1;
					if (end > 0 && currentLine[end - 1] == '\r') end--;
					if (end > start)
					{
						SubmitLine(line.Substring(start, end - start));
					}
					start = next;
					end = next - 1;
				}
				end++;
			}
			if (end == currentLine.Length)
			{
				currentLine.Length = 0;
			}
			else
			{
				line = line.Substring(end, line.Length - end);
				currentLine.Length = 0;
				currentLine.Append(line);
			}
		}
		private void SubmitLine(string line)
		{
			if (line == null || line.Length == 0) return;
			string[] cols = line.Split(FieldSeperator);
			//Console.WriteLine(line);
			rows.Add(cols);
			if (rows.IsFull)
			{
				Flush(false);
			}
		}


		/// <summary>
		/// Get the base stream - always throws an exception.
		/// </summary>
		public override Stream BaseStream
		{
			get
			{
				throw new NotSupportedException("SqlTextWriter has no BaseStream.");
			}
		}

		/// <summary>
		/// Release any resources.
		/// </summary>
		~SqlTextWriter()
		{
			if (sqlBulkCopy != null)
			{
				Flush(true);
			}
			// will this break things? ***
			if (cn != null)
			{
				try
				{
					cn.Close();
				}
				catch
				{
				}
			}
			//Dispose(true);
		}
		/// <summary>
		/// Release any resources.
		/// </summary>
		/// <param name="disposing"></param>
		protected override void Dispose(bool disposing)
		{
			if (sqlBulkCopy != null)
			{
				Flush(true);

				try
				{
					sqlBulkCopy.Close();
				}
				catch
				{
					// ignore?
				}
				try
				{
					((IDisposable)sqlBulkCopy).Dispose();
				}
				catch
				{
					// ignore? ***
				}

				sqlBulkCopy = null;
			}
			if (cn != null)
			{
				try
				{
					cn.Close();
				}
				catch
				{
				}
				cn = null;
			}
			currentLine = null;

			base.Dispose(disposing);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// Close the reader.
		/// </summary>
		public override void Close()
		{
			Dispose(false);
		}

	}




	// This could be done with a StreamWriter backed by a MemoryStream...
	// Hard to say whether that would be better or worse than a line-by-line
	// byte buffer method.

	/// <summary>
	/// A writable Stream that is backed by a TextWriter.
	/// </summary>
	internal class TextWriterStream : Stream
#if !DOTNET2
		, IDisposable
#endif
	{
		private TextWriter baseWriter;
		private Encoding encoding;
		private Decoder decoder;
		//private long position;
		//private StreamWriter memWriter;
		//private MemoryStream memStream;
		//private char[] currentLineChars = null;
		//private byte[] currentLine = null;
		//private int currentPosition = 0;
		private char[] charBuffer = new char[5 * 32768];
		private byte[] byteBuf = new byte[1];
		private bool eof = false;

		/// <summary>
		/// Create a Stream wrapping the given StreamWriter, using a UTF-8 encoding.
		/// </summary>
		/// <param name="baseWriter">the reader to wrap</param>
		public TextWriterStream(StreamWriter baseWriter)
			: this(baseWriter, Encoding.UTF8)
		{
		}
		/// <summary>
		/// Create a Stream wrapping the given StreamWriter, using the given encoding.
		/// </summary>
		/// <param name="baseWriter">the writer to wrap</param>
		/// <param name="encoding">the Encoding to use</param>
		public TextWriterStream(StreamWriter baseWriter, Encoding encoding)
		{
			this.baseWriter = baseWriter;
			this.encoding = encoding;
			//this.currentLineChars = new char[4096];
			decoder = encoding.GetDecoder();
		}

		public override bool CanRead
		{
			get { return false; }
		}

		public override bool CanSeek
		{
			get { return false; }
		}

		public override bool CanWrite
		{
			get { return true; }
		}

		public override void Close()
		{
			if (baseWriter != null)
			{
				Flush();
				baseWriter.Close();
				baseWriter = null;
				eof = true;
				GC.SuppressFinalize(this);
			}
		}

		~TextWriterStream()
		{
			if (baseWriter != null)
			{
				Flush();
			}
			//Close();
		}

		protected override void Dispose(bool disposing)
		{
			Close();
		}

		public override void Flush()
		{
			// ignore decoder state?
			baseWriter.Flush();
		}

		public override void WriteByte(byte value)
		{
			// not very efficient:
			if (eof) throw new IOException("Stream is closed.");
			byteBuf[0] = value;
			int c = decoder.GetChars(byteBuf, 0, 1, charBuffer, 0, false);
			if (c > 0) baseWriter.Write(charBuffer, 0, c);
		}
		public override void Write(byte[] buffer, int offset, int count)
		{
			if (eof) throw new IOException("Stream is closed.");
			if (count <= 0) return;
			//decoder.GetCharCount(buffer, offset, count);
			if (charBuffer.Length < 5 * count)
			{
				while (count > 5 * charBuffer.Length)
				{
					int c1 = decoder.GetChars(buffer, offset, charBuffer.Length / 5, charBuffer, 0, false);
					baseWriter.Write(charBuffer, 0, c1);
					offset += charBuffer.Length / 5;
					count -= charBuffer.Length / 5;
				}
			}

			int c = decoder.GetChars(buffer, offset, count, charBuffer, 0, false);
			if (c > 0) baseWriter.Write(charBuffer, 0, c);
		}

		public override long Length
		{
			get
			{
				throw new NotSupportedException("Stream cannot determine length.");
			}
		}

		public override long Position
		{
			get
			{
				// this is a problem, since we normally are appending!
				//return position;
				throw new NotSupportedException("Stream cannot determine position.");
			}
			set
			{
				if (value != Position)
				{
					throw new NotSupportedException("Stream cannot seek.");
				}
			}
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException("Stream is not readable.");
		}

		public override int ReadByte()
		{
			throw new NotSupportedException("Stream is not readable.");
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException("Stream cannot seek.");
		}

		public override void SetLength(long value)
		{
			//// simply ignore?
			//throw new NotSupportedException("Stream cannot set length.");
		}

	}



	// This could be done with a TextReader backed by a MemoryStream...
	// Hard to say whether that would be better or worse than a line-by-line
	// byte buffer method.

	/// <summary>
	/// A readable Stream that is backed by a TextReader.
	/// </summary>
	internal class TextReaderStream : Stream
#if !DOTNET2
		, IDisposable
#endif
	{
		private TextReader baseReader;
		private Encoding encoding;
		//private Encoder encoder;
		private long position;
		//private StreamWriter memWriter;
		//private MemoryStream memStream;
		//private char[] currentLineChars = null;
		private byte[] currentLine = null;
		private int currentPosition = 0;
		private bool eof = false;

		/// <summary>
		/// Create a Stream wrapping the given TextReader, using a UTF-8 encoding.
		/// </summary>
		/// <param name="baseReader">the reader to wrap</param>
		public TextReaderStream(TextReader baseReader)
			: this(baseReader, Encoding.UTF8)
		{
		}
		/// <summary>
		/// Create a Stream wrapping the given TextReader, using the given encoding.
		/// </summary>
		/// <param name="baseReader">the reader to wrap</param>
		/// <param name="encoding">the Encoding to use</param>
		public TextReaderStream(TextReader baseReader, Encoding encoding)
		{
			this.baseReader = baseReader;
			this.encoding = encoding;
			//this.currentLineChars = new char[4096];
			//encoder = encoding.GetEncoder();
		}

		public override bool CanRead
		{
			get { return true; }
		}

		public override bool CanSeek
		{
			get { return false; }
		}

		public override bool CanWrite
		{
			get { return false; }
		}

		public override void Close()
		{
			baseReader.Close();
			eof = true;
		}

#if DOTNET2
		protected override void Dispose(bool disposing)
		{
			baseReader.Dispose();
		}
#else
		void IDisposable.Dispose()
		{
			baseReader.Close();
		}
#endif

		public override void Flush()
		{
			return;
		}

		public override long Length
		{
			get
			{
				throw new NotSupportedException("Stream cannot determine length.");
			}
		}

		public override long Position
		{
			get
			{
				return position;
			}
			set
			{
				if (value != Position)
				{
					throw new NotSupportedException("Stream cannot seek.");
				}
			}
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			//int origCount = count;
			int readCount = 0;
			while (!eof && readCount < count)
			{
				if (currentLine == null || currentLine.Length <= currentPosition)
				{
					// refresh buffer:
					//int charsRead = baseReader.ReadBlock(currentLineChars, 0, currentLineChars.Length);
					//if (charsRead <= 0)
					string line = baseReader.ReadLine();
					if (line == null)
					{
						currentLine = null;
						eof = true;
						break;
					}
					//encoder.GetBytes(charBuffer, offset, count, byteBuffer, 0, false);
					//currentLine = encoding.GetBytes(currentLineChars, 0, charsRead);
					// use Environment.NewLine?
					currentLine = encoding.GetBytes(line + "\n");
					currentPosition = 0;
				}

				if (currentLine.Length - currentPosition >= count)
				{
					Buffer.BlockCopy(currentLine, currentPosition, buffer, offset, count);
					currentPosition += count;
					readCount += count;
					break;
				}
				int c = currentLine.Length - currentPosition;
				Buffer.BlockCopy(currentLine, currentPosition, buffer, offset, c);
				currentLine = null;
				readCount += c;
				offset += c;
				count -= c;
			}
			//if (readCount == 0) return -1;
			position += readCount;
			//Console.WriteLine("Requested " + origCount + ", 
			return readCount;
		}

		public override int ReadByte()
		{
			// really, really inefficient:
			return base.ReadByte();
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			if ((offset == 0 && origin == SeekOrigin.Current) ||
				(offset == Position && origin == SeekOrigin.Begin))
			{
				return Position;
			}
			throw new NotSupportedException("Stream cannot seek.");
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException("Stream is not writable.");
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException("Stream is not writable.");
		}

	}

}

