// owner: rragno

#define LZMA_PLAIN
#define UNBUFFERED

using System;
using System.IO;
using System.Collections;
using System.Collections.Specialized;
using System.Text;
using System.Threading;
using System.Runtime.InteropServices;
//using System.Xml;
//using System.Data;
//using System.Data.OleDb;


namespace Microsoft.TMSN.IO
{

	#region 7z

	/// <summary>
	/// Wrapper Stream to perform 7-zip decompression.
	/// </summary>
	/// <remarks>
	/// This class will decompress .7z, .gz, .zip, .rar, .cab, .arj, and other formats.
	/// </remarks>
	public class Z7zDecodeStream : CmdStream
	{
		private static readonly string z7zCmd = "7za.exe";
		private static readonly string z7zFullCmd = "7z.exe";
		private static readonly string[] z7zCmdSet = new string[] { z7zCmd, z7zFullCmd };
		private static readonly string[] z7zFullCmdSet = new string[] { z7zFullCmd, z7zCmd };
		//// archiveName [fileName]
		private static readonly string z7zCmdDecompressFromFileArgs = "e -y -bd -so \"{0}\" \"{1}\"";
		//// archiveName [fileName]
		private static readonly string z7zCmdListFileArgs = "l -y -bd \"{0}\" \"{1}\"";

		private readonly string archiveName;
		private readonly string fileName;
		private readonly bool full7z;

		/// <summary>
		/// Create a stream to decompress with 7-zip.
		/// The "7z" or "7za" program must be in the path for this to work.
		/// </summary>
		/// <param name="fileName">name of the file to be wrapped for reading</param>
		/// <exception cref="FileNotFoundException">fileName cannot be found</exception>
		/// <exception cref="IOException">fileName cannot be opened</exception>
		/// <exception cref="InvalidOperationException">7z.exe or 7za.exe cannot be found.</exception>
		/// <exception cref="ArgumentNullException">The fileName is null.</exception>
		public Z7zDecodeStream(string fileName)
			: this(fileName, true)
		{
		}

		/// <summary>
		/// Create a stream to decompress a particular file with 7-zip.
		/// The "7z" or "7za" program must be in the path for this to work.
		/// </summary>
		/// <param name="archiveName">name of the compressed archive to be wrapped for reading</param>
		/// <param name="fileName">name of the file to read from the archive</param>
		/// <exception cref="FileNotFoundException">fileName cannot be found</exception>
		/// <exception cref="IOException">fileName cannot be opened</exception>
		/// <exception cref="InvalidOperationException">7z.exe or 7za.exe cannot be found.</exception>
		/// <exception cref="ArgumentNullException">The fileName is null.</exception>
		public Z7zDecodeStream(string archiveName, string fileName)
			: this(archiveName, fileName, true)
		{
		}

		/// <summary>
		/// Create a stream to decompress with 7-zip.
		/// The "7za" or "7z" program must be in the path for this to work,
		/// depending on the full7z parameter.
		/// </summary>
		/// <param name="fileName">name of the file to be wrapped for reading</param>
		/// <param name="full7z">if true, use the full 7z.exe application;
		/// if false, use the self-contained 7za.exe</param>
		/// <exception cref="FileNotFoundException">fileName cannot be found</exception>
		/// <exception cref="IOException">fileName cannot be opened</exception>
		/// <exception cref="InvalidOperationException">7z.exe or 7za.exe cannot be found.</exception>
		/// <exception cref="ArgumentNullException">The fileName is null.</exception>
		public Z7zDecodeStream(string fileName, bool full7z)
			: this(fileName, "", full7z)
		{
		}

		/// <summary>
		/// Create a stream to decompress with 7-zip.
		/// The "7za" or "7z" program must be in the path for this to work,
		/// depending on the full7z parameter.
		/// </summary>
		/// <param name="fileName">name of the file to be wrapped for reading</param>
		/// <param name="archiveName">name of the compressed archive to be wrapped for reading</param>
		/// <param name="full7z">if true, use the full 7z.exe application;
		/// if false, use the self-contained 7za.exe</param>
		/// <exception cref="FileNotFoundException">fileName cannot be found</exception>
		/// <exception cref="IOException">fileName cannot be opened</exception>
		/// <exception cref="InvalidOperationException">7z.exe or 7za.exe cannot be found.</exception>
		/// <exception cref="ArgumentNullException">The fileName is null.</exception>
		public Z7zDecodeStream(string archiveName, string fileName, bool full7z)
			: this(archiveName, fileName, full7z, GetLengthInner(archiveName, fileName, full7z))
		{
		}

		private Z7zDecodeStream(string archiveName, string fileName, bool full7z, long length)
			: base(
			full7z ? z7zFullCmdSet : z7zCmdSet,
			string.Format(z7zCmdDecompressFromFileArgs, archiveName, fileName == null ? "" : fileName),
			false,
			true,
			archiveName)
		{
			this.archiveName = archiveName;
			this.fileName = fileName;
			//if (fileName.Length != 0)  this.fileName = this.fileName + "/" + fileName;
			this.full7z = full7z;
			this.length = length;
		}


		private long length = -1;

		/// <summary>
		/// Get the uncompressed length of the file, in bytes.
		/// </summary>
		/// <exception cref="NotSupportedException">The length cannot be retrieved, sometimes.</exception>
		public override long Length
		{
			get
			{
				if (length < 0)
				{
					length = GetLengthInner(archiveName, fileName, full7z);
					if (length < 0)
					{
						throw new NotSupportedException("Cannot always get length of compressed file.");
					}
				}
				return length;
			}
		}


		internal static bool Exists7z
		{
			get { return full7zExists; }
		}
		internal static bool Exists7za
		{
			get { return a7zExists; }
		}

		private static bool full7zExists = true;
		private static bool a7zExists = true;

		internal static System.Diagnostics.Process Exec(string args)
		{
			if (!full7zExists)  return ExecA(args);
			try
			{
				System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
					z7zFullCmd,
					args);
				//psi.WorkingDirectory = workingDir;
				psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
				psi.CreateNoWindow = true;
				psi.RedirectStandardInput = false;
				psi.RedirectStandardOutput = true;
				psi.UseShellExecute = false;
				return System.Diagnostics.Process.Start(psi);
			}
			catch
			{
				full7zExists = false;
				return ExecA(args);
			}
		}
		internal static System.Diagnostics.Process ExecA(string args)
		{
			try
			{
				System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
					z7zCmd,
					args);
				//psi.WorkingDirectory = workingDir;
				psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
				psi.CreateNoWindow = true;
				psi.RedirectStandardInput = false;
				psi.RedirectStandardOutput = true;
				psi.UseShellExecute = false;
				return System.Diagnostics.Process.Start(psi);
			}
			catch
			{
				a7zExists = false;
				throw;
			}
		}


		/// <summary>
		/// Get the uncompressed length of a compressed file, in bytes.
		/// </summary>
		/// <param name="archiveName">name of the compressed file</param>
		/// <returns>the uncompressed length in bytes, or -1 if it cannot be determined</returns>
		public static long GetLength(string archiveName)
		{
			return GetLength(archiveName, null);
		}

		/// <summary>
		/// Get the uncompressed length of a file in a compressed archive, in bytes.
		/// </summary>
		/// <param name="archiveName">name of the compressed archive</param>
		/// <param name="fileName">path of the file in the archive</param>
		/// <returns>the uncompressed length in bytes, or -1 if it cannot be determined</returns>
		public static long GetLength(string archiveName, string fileName)
		{
			try
			{
				return GetLengthInner(archiveName, fileName, true);
			}
			catch
			{
				return -1;
			}
		}

		private static long GetLengthInner(string archiveName, string fileName, bool full7z)
		{
			if (archiveName == null)  throw new ArgumentNullException("archiveName", "archiveName cannot be null");
			if (!File.Exists(archiveName))  throw new FileNotFoundException("File cannot be found: " + archiveName);
#if !GZIP_WEAK_LENGTH
			if (archiveName.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
			{
				return GzipEncodeStream.GetLengthTag(archiveName);
			}
#endif
			if (fileName == null)  fileName = "";
			long length = -1;
			System.Diagnostics.Process proc = null;
			try
			{
				string args = string.Format(z7zCmdListFileArgs, archiveName, fileName);
				if (full7z)
				{
					proc = Exec(args);
				}
				else
				{
					proc = ExecA(args);
				}

				using (StreamReader sr = proc.StandardOutput)
				{
					////// gzip will simply fail for large files!!
					//   Date      Time    Attr         Size   Compressed  Name
					//------------------- ----- ------------ ------------  ------------
					//2005-12-17 19:43:29         2824003416   4936836925  sr20051210.txt
					//------------------- ----- ------------ ------------  ------------
					//                            2824003416   4936836925  1 files
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						if (line.StartsWith("-----"))
						{
							length = 0;
							try
							{
								while ((line = sr.ReadLine()) != null && !(line = line.Trim()).StartsWith("-----"))
								{
									//string[] cols = System.Text.RegularExpressions.Regex.Split(line, @"\s+");
									line = line.Replace('\t', ' ');
									int oldLen = -1;
									while (line.Length != oldLen)
									{
										oldLen = line.Length;
										line = line.Replace("  ", " ");
									}
									line = line.Trim();
									string[] cols = line.Split(' ');
									try
									{
										string size = cols[cols.Length - 3];
										length += long.Parse(size);
									}
									catch
									{
										// maybe missing compressed size?
										string size = cols[cols.Length - 2];
										length += long.Parse(size);
									}

									if (fileName.ToLower().EndsWith(".gz"))
									{
										long compressedLength = long.Parse(cols[cols.Length - 2]);
										// what to do?? It is only correct mod 2^32! ***
										while (compressedLength > length + 1000)
										{
											length += (1L << 32);
										}
									}
								}
							}
							catch
							{
							}

							break;
						}
					}
				}
			}
			catch (System.ComponentModel.Win32Exception)
			{
				throw new InvalidOperationException("Z7zDecodeStream requires 7z.exe or 7za.exe to be in the " +
					"path for decompression. " +
					"See http://7-zip.org");
			}
			catch (Exception ex)
			{
				System.Diagnostics.Debug.WriteLine("Exception when getting uncompressed length: " +
					ex.ToString());
			}
			finally
			{
				try
				{
					if (proc != null)  proc.Close();
				}
				catch
				{
					// ignore
				}
			}
			if (length < 0)
			{
				throw new IOException("File cannot be opened: " + archiveName);
			}
			return length;
		}


		/// <summary>
		/// Determine if a given file exists in an archive.
		/// </summary>
		/// <param name="archiveName">the archive to look in</param>
		/// <param name="fileName">the file path to look for</param>
		/// <returns>true if the file exists in the archive; false, otherwise</returns>
		public static bool Exists(string archiveName, string fileName)
		{
			if (fileName == null || fileName.Length == 0)  return false;
			if (!File.Exists(archiveName))  return false;
			System.Diagnostics.Process proc = null;
			try
			{
				string args = string.Format(z7zCmdListFileArgs, archiveName, fileName);
				proc = Exec(args);

				using (StreamReader sr = proc.StandardOutput)
				{
					////// gzip will simply fail for large files!!
					//   Date      Time    Attr         Size   Compressed  Name
					//------------------- ----- ------------ ------------  ------------
					//2005-12-17 19:43:29         2824003416   4936836925  sr20051210.txt
					//------------------- ----- ------------ ------------  ------------
					//                            2824003416   4936836925  1 files
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						//Console.WriteLine(":: " + line);
						if (line.StartsWith("-----"))
						{
							line = sr.ReadLine();
							if (line != null)
							{
								line = line.Trim();
								if (!line.StartsWith("-----"))
								{
									return true;
								}
							}
							return false;
						}
					}
				}
			}
			catch (System.ComponentModel.Win32Exception)
			{
//				throw new InvalidOperationException("Z7zDecodeStream requires 7z.exe or 7za.exe to be in the " +
//					"path for decompression. " +
//					"See http://7-zip.org");
				return false;
			}
			catch //(Exception ex)
			{
//				System.Diagnostics.Debug.WriteLine("Exception when getting uncompressed length: " +
//					ex.ToString());
				return false;
			}
			finally
			{
				try
				{
					if (proc != null)  proc.Close();
				}
				catch
				{
					// ignore
				}
			}
			return false;
		}


		/// <summary>
		/// Get the paths to files within an archive.
		/// </summary>
		/// <param name="archiveName">the archive to look in</param>
		/// <returns>the set of file paths for the files in that archive</returns>
		/// <remarks>
		/// This will silently return the empty list if there are any problems.
		/// </remarks>
		public static string[] DirectoryFiles(string archiveName)
		{
			return DirectoryFiles(archiveName, null);
		}
		/// <summary>
		/// Get the paths to files within an archive.
		/// </summary>
		/// <param name="archiveName">the archive to look in</param>
		/// <param name="path">the path in the archive to look in</param>
		/// <returns>the set of file paths for the files in that archive</returns>
		/// <remarks>
		/// This will silently return the empty list if there are any problems.
		/// </remarks>
		public static string[] DirectoryFiles(string archiveName, string path)
		{
			return DirectoryEntries(archiveName, path, true, false);
		}
		/// <summary>
		/// Get the paths to files and directories within an archive.
		/// </summary>
		/// <param name="archiveName">the archive to look in</param>
		/// <returns>the set of file paths for the files and directories in that archive</returns>
		/// <remarks>
		/// <p>
		/// Directories will be distinguished by ending with "/".
		/// </p>
		/// <p>
		/// This will silently return the empty list if there are any problems.
		/// </p>
		/// </remarks>
		public static string[] DirectoryEntries(string archiveName)
		{
			return DirectoryEntries(archiveName, null);
		}
		/// <summary>
		/// Get the paths to files and directories within an archive.
		/// </summary>
		/// <param name="archiveName">the archive to look in</param>
		/// <param name="path">the path in the archive to look in</param>
		/// <returns>the set of file paths for the files and directories in that archive</returns>
		/// <remarks>
		/// <p>
		/// Directories will be distinguished by ending with "/".
		/// </p>
		/// <p>
		/// This will silently return the empty list if there are any problems.
		/// </p>
		/// </remarks>
		public static string[] DirectoryEntries(string archiveName, string path)
		{
			return DirectoryEntries(archiveName, path, true, true);
		}

		private static readonly char[] pathSeperators = new char[] { '/', '\\' };

		internal static string[] DirectoryEntries(string archiveName, string fileName,
			bool allowFile, bool allowDirectory)
		{
			if (archiveName == null || archiveName.Length == 0)  return new string[0];
			// archive support is limited to file systems!
			if (!File.Exists(archiveName))  return new string[0];
			if (fileName == null)  fileName = "";
			System.Diagnostics.Process proc = null;
			try
			{
				string args = string.Format(z7zCmdListFileArgs, archiveName, fileName);
				proc = Exec(args);

				string path = fileName;
				if (path.Length != 0)
				{
					if (path[path.Length - 1] == '*' || path[path.Length - 1] == '?')
					{
						path = path.TrimEnd('*', '?');
						if (path.Length != 0)
						{
							if (path[path.Length - 1] == '/' || path[path.Length - 1] == '\\')
							{
							}
							else
							{
								int end = path.LastIndexOfAny(pathSeperators);
								if (end < 0)
								{
									path = "";
								}
								else
								{
									path = path.Substring(0, end + 1);
								}
							}
						}
					}
					else
					{
						if (path[path.Length - 1] == '/' || path[path.Length - 1] == '\\')
						{
						}
						else
						{
							// this is ambiguous - is it the directory, or its contents?
							path = path + "/";
						}
					}
				}

				ArrayList res = new ArrayList();
				ArrayList resDirs = new ArrayList();
				using (StreamReader sr = proc.StandardOutput)
				{
					////// gzip will simply fail for large files!!
					//   Date      Time    Attr         Size   Compressed  Name
					//------------------- ----- ------------ ------------  ------------
					//2005-12-17 19:43:29         2824003416   4936836925  sr20051210.txt
					//------------------- ----- ------------ ------------  ------------
					//                            2824003416   4936836925  1 files
					bool started = false;
					int nameStart = 0;
					int col2 = 0;
					int col3 = 0;
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						if (!started)
						{
							if (line.TrimStart().StartsWith("-----"))
							{
								started = true;
								nameStart = line.LastIndexOf(' ');
								if (nameStart < 0 || nameStart >= line.Length - 1) break;
								nameStart++;
								// leading whitespace would be a problem here:
								col2 = line.IndexOf(' ');
								if (col2 < 0 || col2 == nameStart - 1)
								{
									col2 = col3 = 0;
								}
								else
								{
									col2++;
									col3 = line.IndexOf(' ', col2);
									if (col3 < 0 || col3 == nameStart - 1)
									{
										col3 = 0;
									}
									else
									{
										col3++;
									}
								}
							}
							continue;
						}
						if (line.TrimStart().StartsWith("-----"))
						{
							break;
						}

						if (line.Length <= nameStart) continue;
						string name = line.Substring(nameStart).Trim();

						// check for directory attribute - is this flexible enough?
						bool isDir = (line[col2] == 'D' || line[col3] == 'D');
						if (name.Length > path.Length)
						{
							// should be that name[path.Length-1] == '\'
							int nextSep = name.IndexOfAny(pathSeperators, path.Length);
							if (nextSep > 0)
							{
								name = name.Substring(0, nextSep);
								isDir = true;
							}
						}

						if (isDir)
						{
							if (!allowDirectory) continue;
							resDirs.Add(archiveName + "/" + name + "/");
						}
						else
						{
							if (!allowFile) continue;
							//res.Add(archiveName + "\\" + name);
							res.Add(archiveName + "/" + name);
						}
					}
				}
				res.Sort();
				for (int i = res.Count - 1; i > 0; i--)
				{
					if (string.CompareOrdinal((string)res[i], (string)res[i - 1]) == 0)
					{
						res.RemoveAt(i);
					}
				}
				if (resDirs.Count != 0)
				{
					resDirs.Sort();
					for (int i = resDirs.Count - 1; i > 0; i--)
					{
						if (string.CompareOrdinal((string)resDirs[i], (string)resDirs[i - 1]) == 0)
						{
							resDirs.RemoveAt(i);
						}
					}
					resDirs.AddRange(res);
					res = resDirs;
				}
				return (string[])res.ToArray(typeof(string));
			}
			catch (System.ComponentModel.Win32Exception)
			{
				//				throw new InvalidOperationException("Z7zDecodeStream requires 7z.exe or 7za.exe to be in the " +
				//					"path for decompression. " +
				//					"See http://7-zip.org");
				return new string[0];
			}
			catch //(Exception ex)
			{
				//				System.Diagnostics.Debug.WriteLine("Exception when getting uncompressed length: " +
				//					ex.ToString());
				return new string[0];
			}
			finally
			{
				try
				{
					if (proc != null)  proc.Close();
				}
				catch
				{
					// ignore
				}
			}
		}


		internal static StreamInfo[] DirectoryEntriesInfo(string archiveName, string fileName,
			bool allowFile, bool allowDirectory)
		{
			if (archiveName == null || archiveName.Length == 0) return new StreamInfo[0];
			// archive support is limited to file systems!
			if (!File.Exists(archiveName)) return new StreamInfo[0];
			if (fileName == null)  fileName = "";
			System.Diagnostics.Process proc = null;
			try
			{
				string args = string.Format(z7zCmdListFileArgs, archiveName, fileName);
				proc = Exec(args);

				string path = fileName;
				if (path.Length != 0)
				{
					if (path[path.Length - 1] == '*' || path[path.Length - 1] == '?')
					{
						path = path.TrimEnd('*', '?');
						if (path.Length != 0)
						{
							if (path[path.Length - 1] == '/' || path[path.Length - 1] == '\\')
							{
							}
							else
							{
								int end = path.LastIndexOfAny(pathSeperators);
								if (end < 0)
								{
									path = "";
								}
								else
								{
									path = path.Substring(0, end + 1);
								}
							}
						}
					}
					else
					{
						if (path[path.Length - 1] == '/' || path[path.Length - 1] == '\\')
						{
						}
						else
						{
							// this is ambiguous - is it the directory, or its contents?
							path = path + "/";
						}
					}
				}

				ArrayList res = new ArrayList();
				ArrayList resDirs = new ArrayList();
				using (StreamReader sr = proc.StandardOutput)
				{
					////// gzip will simply fail for large files!!
					//   Date      Time    Attr         Size   Compressed  Name
					//------------------- ----- ------------ ------------  ------------
					//2005-12-17 19:43:29         2824003416   4936836925  sr20051210.txt
					//------------------- ----- ------------ ------------  ------------
					//                            2824003416   4936836925  1 files
					bool started = false;
					int nameStart = 0;
					int col2 = 0;
					int col3 = 0;
					int col4 = 0;
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						if (!started)
						{
							if (line.TrimStart().StartsWith("-----"))
							{
								started = true;
								nameStart = line.LastIndexOf(' ');
								if (nameStart < 0 || nameStart >= line.Length - 1) break;
								nameStart++;
								// leading whitespace would be a problem here:
								col2 = line.IndexOf(' ');
								if (col2 < 0 || col2 == nameStart - 1)
								{
									col2 = col3 = col4 = 0;
								}
								else
								{
									col2++;
									col3 = line.IndexOf(' ', col2);
									if (col3 < 0 || col3 == nameStart - 1)
									{
										col3 = col4 = 0;
									}
									else
									{
										col3++;
										col4 = line.IndexOf(' ', col3);
										if (col4 < 0 || col4 == nameStart - 1)
										{
											col4 = 0;
										}
										else
										{
											col4++;
										}
									}
								}
							}
							continue;
						}
						if (line.TrimStart().StartsWith("-----"))
						{
							break;
						}

						if (line.Length <= nameStart) continue;
						string name = line.Substring(nameStart).Trim();

						// check for directory attribute - is this flexible enough?
						bool isDir = (line[col2] == 'D' || line[col3] == 'D');
						if (name.Length > path.Length)
						{
							// should be that name[path.Length-1] == '\'
							int nextSep = name.IndexOfAny(pathSeperators, path.Length);
							if (nextSep > 0)
							{
								name = name.Substring(0, nextSep);
								isDir = true;
							}
						}

						ArrayList list;
						if (isDir)
						{
							if (!allowDirectory) continue;
							name = archiveName + "/" + name + "/";
							list = resDirs;
						}
						else
						{
							if (!allowFile) continue;
							name = archiveName + "/" + name;
							list = res;
						}

						long len = 0;
						DateTime lastMod = DateTime.MinValue;
						try
						{
							// what about TB files? ***
							len = long.Parse(line.Substring(col3, col4 - col3).Trim());
							lastMod = DateTime.Parse(line.Substring(0, col2).Trim());
						}
						catch
						{
							// ignore??
						}
						list.Add(new StreamInfo(name, len, lastMod));
					}
				}
				res.Sort();
				for (int i = res.Count - 1; i > 0; i--)
				{
					if (((StreamInfo)res[i]).Equals(res[i - 1]))
					{
						res.RemoveAt(i);
					}
				}
				if (resDirs.Count != 0)
				{
					resDirs.Sort();
					for (int i = resDirs.Count - 1; i > 0; i--)
					{
						if (((StreamInfo)resDirs[i]).Equals(resDirs[i - 1]))
						{
							resDirs.RemoveAt(i);
						}
					}
					resDirs.AddRange(res);
					res = resDirs;
				}
				return (StreamInfo[])res.ToArray(typeof(StreamInfo));
			}
			catch (System.ComponentModel.Win32Exception)
			{
				//				throw new InvalidOperationException("Z7zDecodeStream requires 7z.exe or 7za.exe to be in the " +
				//					"path for decompression. " +
				//					"See http://7-zip.org");
				return new StreamInfo[0];
			}
			catch //(Exception ex)
			{
				//				System.Diagnostics.Debug.WriteLine("Exception when getting uncompressed length: " +
				//					ex.ToString());
				return new StreamInfo[0];
			}
			finally
			{
				try
				{
					if (proc != null)  proc.Close();
				}
				catch
				{
					// ignore
				}
			}
		}



		/// <summary>
		/// Get whether the stream can seek. Currently returns true, although Seek()
		/// will actually work with low performance.
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				return true;
			}
		}


		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for compressed files, and seeking backwards reopens the file!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset)
		{
			return base.Seek(offset);
		}
		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for compressed files, and seeking backwards reopens the file!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			return base.Seek(offset, origin);
		}
		/// <summary>
		/// Get or Set the position in the file, in bytes. Note that setting the position can
		/// be very slow for compressed files!
		/// </summary>
		public override long Position
		{
			get
			{
				return base.Position;
			}
			set
			{
				base.Position = value;
			}
		}

	}



	/// <summary>
	/// Wrapper Stream to perform 7-zip compression.
	/// </summary>
	/// <remarks>
	/// This class can compress 7z and gz.
	/// </remarks>
	public class Z7zEncodeStream : CmdStream
	{
		private static readonly string z7zCmd = "7za.exe";
		private static readonly string z7zFullCmd = "7z.exe";
		private static readonly string[] z7zCmdSet = new string[] { z7zCmd, z7zFullCmd };
		//// archiveName [fileName [archiveType]]
		private static readonly string z7zCmdCompressToFileArgs = "a -y -bd -si\"{1}\" {2} \"{0}\"";
		//// archiveName [fileName [archiveType]]
		private static readonly string z7zCmdDeleteFileArgs = "d -y -bd \"{0}\" \"{1}\"";
		////// archiveName [fileName]
		//private static readonly string z7zCmdListFileArgs = "l -y -bd \"{0}\" \"{1}\"";

		/// <summary>
		/// The set of formats supported for 7-zip compression.
		/// </summary>
		public enum CompressionFormat
		{
			/// <summary>
			/// The .7z format (supports multi-file archive usage).
			/// </summary>
			Z7z,
			/// <summary>
			/// The .gz format.
			/// </summary>
			Gzip,
			/// <summary>
			/// The .zip format.
			/// </summary>
			Zip,
			/// <summary>
			/// The .bzip2 format.
			/// </summary>
			Bzip2,
			/// <summary>
			/// The .tar format.
			/// </summary>
			Tar,
			/// <summary>
			/// Unspecified, defaulting to extension-specified in recent versions.
			/// </summary>
			Unspecified,
		}

		// *** Potentially, files in archives can be copied to others without recompressing

		/// <summary>
		/// Create a compressed stream.
		/// The "7za" or "7z" program must be in the path for this to work.
		/// </summary>
		/// <remarks>
		/// The compression format will be based on the file extension. The 7z
		/// format will be used for unknown extentions.
		/// </remarks>
		/// <param name="fileName">name of the file to be wrapped for writing</param>
		public Z7zEncodeStream(string fileName)
			: this(fileName, "")
		{
			// shouldn't we test for problems and throw exceptions? At least for exe existance? ***
		}

		/// <summary>
		/// Create a stream for a particular file in a compressed archive.
		/// The "7za" or "7z" program must be in the path for this to work.
		/// </summary>
		/// <remarks>
		/// The compression format will be based on the file extension. The 7z
		/// format will be used for unknown extentions.
		/// </remarks>
		/// <param name="archiveName">name of the archive to be wrapped for writing</param>
		/// <param name="fileName">name of the file to write in the archive</param>
		public Z7zEncodeStream(string archiveName, string fileName)
			: this(archiveName, fileName, CompressionFormat.Unspecified)
		{
			// shouldn't we test for problems and throw exceptions? At least for exe existance? ***
		}

		/// <summary>
		/// Create a compressed stream.
		/// The "7za" or "7z" program must be in the path for this to work.
		/// </summary>
		/// <param name="fileName">name of the file to be wrapped for writing</param>
		/// <param name="type">type of compression to use, such as "7z" or "zip"</param>
		public Z7zEncodeStream(string fileName, CompressionFormat type)
			: base(
			z7zCmdSet,
			string.Format(z7zCmdCompressToFileArgs, fileName, "", MapType(type)),
			true,
			false)
		{
			// shouldn't we test for problems and throw exceptions? At least for exe existance? ***
		}

		/// <summary>
		/// Create a stream for a particular file in a compressed archive.
		/// The "7za" or "7z" program must be in the path for this to work.
		/// </summary>
		/// <remarks>
		/// The compression format will be based on the file extension. The 7z
		/// format will be used for unknown extentions.
		/// </remarks>
		/// <param name="archiveName">name of the archive to be wrapped for writing</param>
		/// <param name="fileName">name of the file to write in the archive</param>
		/// <param name="type">type of compression to use, such as "7z" or "zip"</param>
		public Z7zEncodeStream(string archiveName, string fileName, CompressionFormat type)
			: base(
			z7zCmdSet,
			string.Format(z7zCmdCompressToFileArgs, archiveName, fileName == null ? "" : fileName, MapType(type)),
			true,
			false)
		{
			// shouldn't we test for problems and throw exceptions? At least for exe existance? ***
		}


		/// <summary>
		/// Create a compressed stream.
		/// The "7za" or "7z" program must be in the path for this to work.
		/// </summary>
		/// <remarks>
		/// The compression format will be based on the file extension. The 7z
		/// format will be used for unknown extentions.
		/// </remarks>
		/// <param name="fileName">name of the file to be wrapped for writing</param>
		/// <param name="compressionLevel">level of compression to use, from 1 (low) to 9 (high)</param>
		public Z7zEncodeStream(string fileName, int compressionLevel)
			: this(fileName, "", compressionLevel)
		{
			// shouldn't we test for problems and throw exceptions? At least for exe existance? ***
		}

		/// <summary>
		/// Create a stream for a particular file in a compressed archive.
		/// The "7za" or "7z" program must be in the path for this to work.
		/// </summary>
		/// <remarks>
		/// The compression format will be based on the file extension. The 7z
		/// format will be used for unknown extentions.
		/// </remarks>
		/// <param name="archiveName">name of the archive to be wrapped for writing</param>
		/// <param name="fileName">name of the file to write in the archive</param>
		/// <param name="compressionLevel">level of compression to use, from 1 (low) to 9 (high)</param>
		public Z7zEncodeStream(string archiveName, string fileName, int compressionLevel)
			: this(archiveName, fileName, CompressionFormat.Unspecified, compressionLevel)
		{
			// shouldn't we test for problems and throw exceptions? At least for exe existance? ***
		}

		/// <summary>
		/// Create a compressed stream.
		/// The "7za" or "7z" program must be in the path for this to work.
		/// </summary>
		/// <param name="fileName">name of the file to be wrapped for writing</param>
		/// <param name="type">type of compression to use, such as "7z" or "zip"</param>
		/// <param name="compressionLevel">level of compression to use, from 1 (low) to 9 (high)</param>
		public Z7zEncodeStream(string fileName, CompressionFormat type, int compressionLevel)
			: base(
			z7zCmdSet,
			string.Format(z7zCmdCompressToFileArgs, fileName, "", MapType(type, compressionLevel)),
			true,
			false)
		{
			// shouldn't we test for problems and throw exceptions? At least for exe existance? ***
		}

		/// <summary>
		/// Create a stream for a particular file in a compressed archive.
		/// The "7za" or "7z" program must be in the path for this to work.
		/// </summary>
		/// <remarks>
		/// The compression format will be based on the file extension. The 7z
		/// format will be used for unknown extentions.
		/// </remarks>
		/// <param name="archiveName">name of the archive to be wrapped for writing</param>
		/// <param name="fileName">name of the file to write in the archive</param>
		/// <param name="type">type of compression to use, such as "7z" or "zip"</param>
		/// <param name="compressionLevel">level of compression to use, from 1 (low) to 9 (high)</param>
		public Z7zEncodeStream(string archiveName, string fileName, CompressionFormat type, int compressionLevel)
			: base(
			z7zCmdSet,
			string.Format(z7zCmdCompressToFileArgs, archiveName, fileName == null ? "" : fileName, MapType(type, compressionLevel)),
			true,
			false)
		{
			// shouldn't we test for problems and throw exceptions? At least for exe existance? ***
		}


		/// <summary>
		/// Delete a given file in an archive.
		/// </summary>
		/// <param name="archiveName">the archive to look in</param>
		/// <param name="fileName">the file path to delete</param>
		/// <remarks>
		/// <p>
		/// Note that this will fail for solid archives, generally!
		/// </p>
		/// <p>
		/// This will actually work on wildcard patterns, also.
		/// </p>
		/// </remarks>
		/// <exception cref="IOException">The file does not exist or cannot be deleted.</exception>
		public static void Delete(string archiveName, string fileName)
		{
			if (fileName == null || fileName.Length == 0)  return;
			if (!File.Exists(archiveName))  throw new IOException("Archive '" + archiveName + "' not found");
			System.Diagnostics.Process proc = null;
			try
			{
				string args = string.Format(z7zCmdDeleteFileArgs, archiveName, fileName);
				Console.WriteLine(args);
				proc = Z7zDecodeStream.Exec(args);

				using (StreamReader sr = proc.StandardOutput)
				{
					sr.ReadToEnd();
				}
				//proc.WaitForExit(60000);
				proc.WaitForExit();
				if (!proc.HasExited || proc.ExitCode != 0)
				{
					throw new IOException("Could not delete '" + fileName + "' from '" + archiveName + "'");
				}
				return;
			}
			catch (System.ComponentModel.Win32Exception)
			{
				//				throw new InvalidOperationException("Z7zDecodeStream requires 7z.exe or 7za.exe to be in the " +
				//					"path for decompression. " +
				//					"See http://7-zip.org");
				throw new IOException("Could not delete '" + fileName + "' from '" + archiveName + "'");
			}
			catch //(Exception ex)
			{
				//				System.Diagnostics.Debug.WriteLine("Exception when getting uncompressed length: " +
				//					ex.ToString());
				throw new IOException("Could not delete '" + fileName + "' from '" + archiveName + "'");
			}
			finally
			{
				try
				{
					if (proc != null)  proc.Close();
				}
				catch
				{
					// ignore
				}
			}
		}


		private static string MapExtension(string fileName)
		{
			return MapExtension(fileName, 5);
		}
		private static string MapExtension(string fileName, int compressionLevel)
		{
			if (fileName == null || fileName.Length == 0)  return "";
			string t = Path.GetExtension(fileName);
			t = t.ToLower().Trim(' ', '.');
			switch (t)
			{
				case "zip":
					return MapType(CompressionFormat.Zip, compressionLevel);

				case "gz":
				case "gzip":
					return MapType(CompressionFormat.Gzip, compressionLevel);

				case "bzip":
				case "bzip2":
				case "bz2":
					return MapType(CompressionFormat.Bzip2, compressionLevel);

				case "tar":
					return MapType(CompressionFormat.Tar, compressionLevel);

				case "7z":
				case "7zip":
				case "7-zip":
				default:
					return MapType(CompressionFormat.Z7z, compressionLevel);
			}
		}
		private static string MapType(CompressionFormat t)
		{
			return MapType(t, 5);
		}
		private static string MapType(CompressionFormat t, int compressionLevel)
		{
			//if (!Enum.IsDefined(typeof(CompressionFormat), t))  return "";
			switch (t)
			{
				case CompressionFormat.Z7z:
					return "-t7z -ms=off -mx=" + Math.Min(9, Math.Max(0, compressionLevel));

				case CompressionFormat.Zip:
					return "-tzip -mx=" + Math.Min(9, Math.Max(0, compressionLevel));

				case CompressionFormat.Gzip:
					if (compressionLevel <= 5)
					{
						// This makes it much bigger, but it is also twice as
						// fast as the default:
						return "-tgzip -mfb=3 -mx=" + Math.Min(9, Math.Max(1, compressionLevel));
					}
					else
					{
						return "-tgzip -mx=" + Math.Min(9, Math.Max(1, compressionLevel));
					}

				case CompressionFormat.Bzip2:
					return "-tbzip2 -mx=" + Math.Min(9, Math.Max(1, compressionLevel));

				case CompressionFormat.Tar:
					return "-ttar";

				case CompressionFormat.Unspecified:
					return "-mx=" + Math.Min(9, Math.Max(1, compressionLevel));

				default:
					return "-ms=off -mx=" + Math.Min(9, Math.Max(0, compressionLevel));
			}
		}
	}



	/// <summary>
	/// Wrapper Stream to perform gzip compression with 7-zip.
	/// </summary>
	/// <remarks>
	/// This is equivalent to constructing a Z7zEncodeStream with a type
	/// of <see cref="Z7zEncodeStream.CompressionFormat.Gzip"/>.
	/// </remarks>
	public class Z7zGzEncodeStream : Z7zEncodeStream
	{
		//private static readonly string z7zCmd = "7za";
		//private static readonly string z7zCmdCompressToFileArgs = "a -y -bd -si -tgzip \"{0}\"";

		// level could be added with "-mx=9 -mfb=128", but it slows the process down by up to half,
		// and the size is already typically over 20% smaller than gzip or SharpZipLib. ***

		/// <summary>
		/// Create a stream to decompress with 7-zip.
		/// The "7za" or "7z" program must be in the path for this to work.
		/// </summary>
		/// <param name="fileName">name of the file to be wrapped for writing</param>
		public Z7zGzEncodeStream(string fileName)
			: base(fileName, Z7zEncodeStream.CompressionFormat.Gzip)
		{
			// shouldn't we test for problems and throw exceptions? At least for exe existance? ***
		}
	}

	#endregion


	#region RAR

	//	public class Rar
	//	{
	//		public static byte[] Decompress(byte[] input)
	//		{
	//			return Process(input, false);
	//		}
	//		
	//		private static byte[] Process(byte[] input, bool compress)
	//		{
	//			string rarCmd = "unrar";
	//			string rarCmdArgs = "p -inul";
	//
	//			System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(rarCmd, rarCmdArgs);
	//			//psi.WorkingDirectory = workingDir;
	//			psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
	//			psi.CreateNoWindow = true;
	//			psi.RedirectStandardInput = true;
	//			psi.RedirectStandardOutput = true;
	//			psi.UseShellExecute = false;
	//			System.Diagnostics.Process proc = System.Diagnostics.Process.Start(psi);
	//			StreamWriter inWriter  = proc.StandardInput;
	//			StreamReader outReader = proc.StandardOutput;
	//			Stream inStream = inWriter.BaseStream;
	//			Stream outStream = outReader.BaseStream;
	//
	//			inStream.Write(input, 0, input.Length);
	//			inStream.Flush();
	//			inStream.Close();
	//
	//			ArrayList res = new ArrayList();
	//			for (int b = outStream.ReadByte(); b >= 0; b = outStream.ReadByte())
	//			{
	//				res.Add((byte)b);
	//			}
	//			return (byte[])res.ToArray(typeof(byte));
	//		}
	//	}

	//	class LzmaStream : CmdStream
	//	{
	//		private static readonly string lzmaCmd = "lzma";
	//		private static readonly string lzmaCmdCompressArgs   = "e -si -so";
	//		private static readonly string lzmaCmdDecompressArgs = "d -si -so";
	//
	//		public LzmaStream(Stream orig, bool compress, bool forWriting)
	//			: base(orig, lzmaCmd, (compress ? lzmaCmdCompressArgs : lzmaCmdDecompressArgs), forWriting)
	//		{
	//		}
	//		public LzmaStream(byte[] orig, bool compress)
	//			: base(orig, lzmaCmd, (compress ? lzmaCmdCompressArgs : lzmaCmdDecompressArgs))
	//		{
	//		}
	//	}

	/// <summary>
	/// Wrapper Stream to perform RAR decompression.
	/// </summary>
	public class RarDecodeStream : CmdStream
	{
		private static readonly string rarCmd = "unrar";
		//private static readonly string rarCmdDecompressArgs = "p -inul";
		//private static readonly string rarCmdDecompressToFileArgs = "e -inul \"{0}\"";
		private static readonly string rarCmdDecompressFromFileArgs = "p -dh -c- -idp -inul \"{0}\" \"{1}\"";
		private static readonly string rarCmdListArgs = "l -dh -c- \"{0}\" \"{1}\"";

		//		/// <summary>
		//		/// Create a stream to decompress with RAR.
		//		/// If forWriting is false, the stream will be read from orig and decompressed on the fly.
		//		/// If forWriting is true, the stream will write a decompressed version of all written data to orig.
		//		/// Even if Close() is not called, Finish() should be used to terminate the decompression when writing.
		//		/// The "unrar" program must be in the path for this to work.
		//		/// </summary>
		//		/// <param name="orig">the stream to be wrapped for reading or writing</param>
		//		/// <param name="forWriting">If false, makes a read-only stream that reads from orig.
		//		/// If true, makes a write-only stream that writes to orig.</param>
		//		public RarDecodeStream(Stream orig, bool compress, bool forWriting)
		//			: base(orig, rarCmd, rarCmdDecompressArgs, forWriting)
		//		{
		//		}

		//		/// <summary>
		//		/// Create a stream to decompress with RAR.
		//		/// The stream will be read from orig and decompressed on the fly.
		//		/// The "unrar" program must be in the path for this to work.
		//		/// </summary>
		//		/// <param name="orig">the bytes to be wrapped for reading</param>
		//		public RarDecodeStream(byte[] orig, bool compress)
		//			: base(orig, rarCmd, rarCmdDecompressArgs)
		//		{
		//		}


		/// <summary>
		/// Create a stream to decompress with RAR.
		/// The "unrar" program must be in the path for this to work.
		/// </summary>
		/// <param name="fileName">name of the file to be wrapped for reading</param>
		/// <exception cref="FileNotFoundException">fileName cannot be found</exception>
		/// <exception cref="IOException">fileName cannot be opened</exception>
		/// <exception cref="InvalidOperationException">unrar.exe cannot be found.</exception>
		/// <exception cref="ArgumentNullException">The fileName is null.</exception>
		public RarDecodeStream(string fileName)
			: this(fileName, null, GetLengthInner(fileName, null))
		{
		}
		/// <summary>
		/// Create a stream to decompress a particular file with RAR.
		/// The "unrar" program must be in the path for this to work.
		/// </summary>
		/// <param name="archiveName">name of the archive to be wrapped for reading</param>
		/// <param name="fileName">name of the file in the archive to read</param>
		/// <exception cref="FileNotFoundException">archiveName cannot be found</exception>
		/// <exception cref="IOException">archiveName cannot be opened</exception>
		/// <exception cref="InvalidOperationException">unrar.exe cannot be found.</exception>
		/// <exception cref="ArgumentNullException">The archiveName is null.</exception>
		public RarDecodeStream(string archiveName, string fileName)
			: this(archiveName, fileName, GetLengthInner(archiveName, fileName))
		{
		}
		private RarDecodeStream(string archiveName, string fileName, long length)
			: base(
			rarCmd,
			string.Format(rarCmdDecompressFromFileArgs, archiveName, fileName),
			false,
			true,
			archiveName)
		{
			this.archiveName = archiveName;
			this.fileName = fileName;
			this.length = length;
		}

		private string archiveName;
		private string fileName;
		private long length = -1;

		/// <summary>
		/// Get the uncompressed length of the file, in bytes.
		/// </summary>
		/// <exception cref="NotSupportedException">The length cannot be retrieved, sometimes.</exception>
		public override long Length
		{
			get
			{
				if (length < 0)
				{
					length = GetLength(archiveName, fileName);
					if (length < 0)
					{
						throw new NotSupportedException("Cannot always get length of compressed file.");
					}
				}
				return length;
			}
		}

		/// <summary>
		/// Get the uncompressed length of a compressed file, in bytes.
		/// </summary>
		/// <param name="archiveName">name of the compressed file</param>
		/// <returns>the uncompressed length in bytes, or -1 if it cannot be determined</returns>
		public static long GetLength(string archiveName)
		{
			return GetLength(archiveName, null);
		}

		/// <summary>
		/// Get the uncompressed length of a file in a compressed archive, in bytes.
		/// </summary>
		/// <param name="archiveName">name of the compressed archive</param>
		/// <param name="fileName">path of the file in the archive</param>
		/// <returns>the uncompressed length in bytes, or -1 if it cannot be determined</returns>
		public static long GetLength(string archiveName, string fileName)
		{
			try
			{
				return GetLengthInner(archiveName, fileName);
			}
			catch
			{
				return -1;
			}
		}


		private static long GetLengthInner(string archiveName, string fileName)
		{
			if (archiveName == null) throw new ArgumentNullException("archiveName", "archiveName cannot be null");
			if (!File.Exists(archiveName)) throw new FileNotFoundException("File cannot be found: " + archiveName);
			if (fileName == null) fileName = "";
			long length = -1;
			System.Diagnostics.Process proc = null;
			try
			{
				System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
					rarCmd,
					string.Format(rarCmdListArgs, archiveName, fileName));
				//psi.WorkingDirectory = workingDir;
				psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
				psi.CreateNoWindow = true;
				psi.RedirectStandardInput = false;
				psi.RedirectStandardOutput = true;
				psi.UseShellExecute = false;
				proc = System.Diagnostics.Process.Start(psi);
				using (StreamReader sr = proc.StandardOutput)
				{
					//UNRAR 3.41 freeware      Copyright (c) 1993-2004 Alexander Roshal
					//
					//Archive \\tmsns\g$\Loghouse\SLogs\sr20050607_11001200_TK2CTWBS03_s.msn.com_w3svc1.log.rar
					//
					// Name             Size   Packed Ratio  Date   Time     Attr      CRC   Meth Ver
					//-------------------------------------------------------------------------------
					// sr20050607_11001200_TK2CTWBS03_s.msn.com_w3svc1.log 1338736271 359255839  26% 07-06-05 12:00  .....A.   51ED9AAF m3e 2.0
					//-------------------------------------------------------------------------------
					//    1       1338736271 359255839  26%
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						if (line.Trim().StartsWith("-----"))
						{
							length = 0;
							while ((line = sr.ReadLine()) != null && !line.StartsWith("-----"))
							{
								line = line.Trim();
								if (line.Length == 0) continue;
								//string[] cols = System.Text.RegularExpressions.Regex.Split(line, @"\s+");
								line = line.Replace('\t', ' ');
								int oldLen = -1;
								while (line.Length != oldLen)
								{
									oldLen = line.Length;
									line = line.Replace("  ", " ");
								}
								line = line.Trim();
								string[] cols = line.Split(' ');

								string size = cols[1];
								length += long.Parse(size);
							}
							break;
						}
					}
				}
			}
			catch (System.ComponentModel.Win32Exception)
			{
				throw new InvalidOperationException("RarDecodeStream requires unrar.exe to be in the " +
					"path for decompression. " +
					"See http://rarsoft.com");
			}
			catch (Exception ex)
			{
				System.Diagnostics.Debug.WriteLine("Exception when getting uncompressed length: " +
					ex.ToString());
			}
			finally
			{
				try
				{
					if (proc != null) proc.Close();
				}
				catch
				{
					// ignore
				}
			}
			if (length < 0)
			{
				throw new IOException("File cannot be opened: " + archiveName);
			}
			return length;
		}


		/// <summary>
		/// Determine if a given file exists in an archive.
		/// </summary>
		/// <param name="archiveName">the archive to look in</param>
		/// <param name="fileName">the file path to look for</param>
		/// <returns>true if the file exists in the archive; false, otherwise</returns>
		public static bool Exists(string archiveName, string fileName)
		{
			if (fileName == null || fileName.Length == 0) return false;
			if (!File.Exists(archiveName)) return false;
			System.Diagnostics.Process proc = null;
			try
			{
				System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
					rarCmd,
					string.Format(rarCmdListArgs, archiveName, fileName));
				//psi.WorkingDirectory = workingDir;
				psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
				psi.CreateNoWindow = true;
				psi.RedirectStandardInput = false;
				psi.RedirectStandardOutput = true;
				psi.UseShellExecute = false;
				proc = System.Diagnostics.Process.Start(psi);
				using (StreamReader sr = proc.StandardOutput)
				{
					//UNRAR 3.41 freeware      Copyright (c) 1993-2004 Alexander Roshal
					//
					//Archive \\tmsns\g$\Loghouse\SLogs\sr20050607_11001200_TK2CTWBS03_s.msn.com_w3svc1.log.rar
					//
					// Name             Size   Packed Ratio  Date   Time     Attr      CRC   Meth Ver
					//-------------------------------------------------------------------------------
					// sr20050607_11001200_TK2CTWBS03_s.msn.com_w3svc1.log 1338736271 359255839  26% 07-06-05 12:00  .....A.   51ED9AAF m3e 2.0
					//-------------------------------------------------------------------------------
					//    1       1338736271 359255839  26%
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						//Console.WriteLine(":: " + line);
						if (line.StartsWith("-----"))
						{
							line = sr.ReadLine();
							if (line != null)
							{
								line = line.Trim();
								if (!line.StartsWith("-----"))
								{
									return true;
								}
							}
							return false;
						}
					}
				}
			}
			catch (System.ComponentModel.Win32Exception)
			{
				//				throw new InvalidOperationException("Z7zDecodeStream requires 7z.exe or 7za.exe to be in the " +
				//					"path for decompression. " +
				//					"See http://7-zip.org");
				return false;
			}
			catch //(Exception ex)
			{
				//				System.Diagnostics.Debug.WriteLine("Exception when getting uncompressed length: " +
				//					ex.ToString());
				return false;
			}
			finally
			{
				try
				{
					if (proc != null) proc.Close();
				}
				catch
				{
					// ignore
				}
			}
			return false;
		}


		/// <summary>
		/// Get the paths to files within an archive.
		/// </summary>
		/// <param name="archiveName">the archive to look in</param>
		/// <returns>the set of file paths for the files in that archive</returns>
		/// <remarks>
		/// This will silently return the empty list if there are any problems.
		/// </remarks>
		public static string[] DirectoryFiles(string archiveName)
		{
			return DirectoryFiles(archiveName, null);
		}
		/// <summary>
		/// Get the paths to files within an archive.
		/// </summary>
		/// <param name="archiveName">the archive to look in</param>
		/// <param name="path">the path in the archive to look in</param>
		/// <returns>the set of file paths for the files in that archive</returns>
		/// <remarks>
		/// This will silently return the empty list if there are any problems.
		/// </remarks>
		public static string[] DirectoryFiles(string archiveName, string path)
		{
			return DirectoryEntries(archiveName, path, true, false);
		}
		/// <summary>
		/// Get the paths to files and directories within an archive.
		/// </summary>
		/// <param name="archiveName">the archive to look in</param>
		/// <returns>the set of file paths for the files and directories in that archive</returns>
		/// <remarks>
		/// <p>
		/// Directories will be distinguished by ending with "/".
		/// </p>
		/// <p>
		/// This will silently return the empty list if there are any problems.
		/// </p>
		/// </remarks>
		public static string[] DirectoryEntries(string archiveName)
		{
			return DirectoryEntries(archiveName, null);
		}
		/// <summary>
		/// Get the paths to files and directories within an archive.
		/// </summary>
		/// <param name="archiveName">the archive to look in</param>
		/// <param name="path">the path in the archive to look in</param>
		/// <returns>the set of file paths for the files and directories in that archive</returns>
		/// <remarks>
		/// <p>
		/// Directories will be distinguished by ending with "/".
		/// </p>
		/// <p>
		/// This will silently return the empty list if there are any problems.
		/// </p>
		/// </remarks>
		public static string[] DirectoryEntries(string archiveName, string path)
		{
			return DirectoryEntries(archiveName, path, true, true);
		}

		//private static readonly System.Text.RegularExpressions.Regex regexWhitespace =
		//    new System.Text.RegularExpressions.Regex(
		//        @"\s+",
		//    System.Text.RegularExpressions.RegexOptions.CultureInvariant | System.Text.RegularExpressions.RegexOptions.Compiled);
		private static readonly char[] pathSeperators = new char[] { '/', '\\' };

		internal static string[] DirectoryEntries(string archiveName, string fileName,
			bool allowFile, bool allowDirectory)
		{
			if (archiveName == null || archiveName.Length == 0) return new string[0];
			if (!File.Exists(archiveName)) return new string[0];
			if (fileName == null) fileName = "";
			System.Diagnostics.Process proc = null;
			try
			{
				System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
					rarCmd,
					string.Format(rarCmdListArgs, archiveName, fileName));
				//psi.WorkingDirectory = workingDir;
				psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
				psi.CreateNoWindow = true;
				psi.RedirectStandardInput = false;
				psi.RedirectStandardOutput = true;
				psi.UseShellExecute = false;
				proc = System.Diagnostics.Process.Start(psi);

				string path = fileName;
				if (path.Length != 0)
				{
					if (path[path.Length - 1] == '*' || path[path.Length - 1] == '?')
					{
						path = path.TrimEnd('*', '?');
						if (path.Length != 0)
						{
							if (path[path.Length - 1] == '/' || path[path.Length - 1] == '\\')
							{
							}
							else
							{
								int end = path.LastIndexOfAny(pathSeperators);
								if (end < 0)
								{
									path = "";
								}
								else
								{
									path = path.Substring(0, end + 1);
								}
							}
						}
					}
					else
					{
						if (path[path.Length - 1] == '/' || path[path.Length - 1] == '\\')
						{
						}
						else
						{
							// this is ambiguous - is it the directory, or its contents?
							path = path + "/";
						}
					}
				}

				ArrayList res = new ArrayList();
				ArrayList resDirs = new ArrayList();
				using (StreamReader sr = proc.StandardOutput)
				{
					//UNRAR 3.41 freeware      Copyright (c) 1993-2004 Alexander Roshal
					//
					//Archive \\tmsns\g$\Loghouse\SLogs\sr20050607_11001200_TK2CTWBS03_s.msn.com_w3svc1.log.rar
					//
					// Name             Size   Packed Ratio  Date   Time     Attr      CRC   Meth Ver
					//-------------------------------------------------------------------------------
					// sr20050607_11001200_TK2CTWBS03_s.msn.com_w3svc1.log 1338736271 359255839  26% 07-06-05 12:00  .....A.   51ED9AAF m3e 2.0
					//-------------------------------------------------------------------------------
					//    1       1338736271 359255839  26%
					//int sizeCol = 1;
					//int dateCol = 4;
					//int timeCol = 5;
					int attrCol = 6;

					bool started = false;
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						line = line.Trim();
						if (!started)
						{
							if (line.StartsWith("-----"))
							{
								started = true;
							}
							continue;
						}
						if (line.StartsWith("-----"))
						{
							break;
						}
						// filenames with spaces do not work...
						int f = line.IndexOf(' ');
						if (f < 0 || f >= line.Length - 1) continue;
						string name = line.Substring(0, f);

						// check for directory attribute - is this flexible enough?
						//string[] cols = regexWhitespace.Split(line);
						line = line.Replace('\t', ' ');
						int oldLen = -1;
						while (line.Length != oldLen)
						{
							oldLen = line.Length;
							line = line.Replace("  ", " ");
						}
						line = line.Trim();
						string[] cols = line.Split(' ');

						if (cols.Length <= attrCol) continue;
						bool isDir = (cols[attrCol].ToLower().IndexOf('D') >= 0);

						if (name.Length > path.Length)
						{
							// should be that name[path.Length-1] == '\'
							int nextSep = name.IndexOfAny(pathSeperators, path.Length);
							if (nextSep > 0)
							{
								name = name.Substring(0, nextSep);
								isDir = true;
							}
						}

						if (isDir)
						{
							if (!allowDirectory) continue;
							resDirs.Add(archiveName + "/" + name + "/");
						}
						else
						{
							if (!allowFile) continue;
							//res.Add(archiveName + "\\" + name);
							res.Add(archiveName + "/" + name);
						}
					}
				}
				res.Sort();
				for (int i = res.Count - 1; i > 0; i--)
				{
					if (string.CompareOrdinal((string)res[i], (string)res[i - 1]) == 0)
					{
						res.RemoveAt(i);
					}
				}
				if (resDirs.Count != 0)
				{
					resDirs.Sort();
					for (int i = resDirs.Count - 1; i > 0; i--)
					{
						if (string.CompareOrdinal((string)resDirs[i], (string)resDirs[i - 1]) == 0)
						{
							resDirs.RemoveAt(i);
						}
					}
					resDirs.AddRange(res);
					res = resDirs;
				}
				return (string[])res.ToArray(typeof(string));
			}
			catch (System.ComponentModel.Win32Exception)
			{
				//				throw new InvalidOperationException("Z7zDecodeStream requires 7z.exe or 7za.exe to be in the " +
				//					"path for decompression. " +
				//					"See http://7-zip.org");
				return new string[0];
			}
			catch //(Exception ex)
			{
				//				System.Diagnostics.Debug.WriteLine("Exception when getting uncompressed length: " +
				//					ex.ToString());
				return new string[0];
			}
			finally
			{
				try
				{
					if (proc != null) proc.Close();
				}
				catch
				{
					// ignore
				}
			}
		}


		internal static StreamInfo[] DirectoryEntriesInfo(string archiveName, string fileName,
			bool allowFile, bool allowDirectory)
		{
			if (archiveName == null || archiveName.Length == 0) return new StreamInfo[0];
			if (!File.Exists(archiveName)) return new StreamInfo[0];
			if (fileName == null) fileName = "";
			System.Diagnostics.Process proc = null;
			try
			{
				System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
					rarCmd,
					string.Format(rarCmdListArgs, archiveName, fileName));
				//psi.WorkingDirectory = workingDir;
				psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
				psi.CreateNoWindow = true;
				psi.RedirectStandardInput = false;
				psi.RedirectStandardOutput = true;
				psi.UseShellExecute = false;
				proc = System.Diagnostics.Process.Start(psi);

				string path = fileName;
				if (path.Length != 0)
				{
					if (path[path.Length - 1] == '*' || path[path.Length - 1] == '?')
					{
						path = path.TrimEnd('*', '?');
						if (path.Length != 0)
						{
							if (path[path.Length - 1] == '/' || path[path.Length - 1] == '\\')
							{
							}
							else
							{
								int end = path.LastIndexOfAny(pathSeperators);
								if (end < 0)
								{
									path = "";
								}
								else
								{
									path = path.Substring(0, end + 1);
								}
							}
						}
					}
					else
					{
						if (path[path.Length - 1] == '/' || path[path.Length - 1] == '\\')
						{
						}
						else
						{
							// this is ambiguous - is it the directory, or its contents?
							path = path + "/";
						}
					}
				}

				ArrayList res = new ArrayList();
				ArrayList resDirs = new ArrayList();
				using (StreamReader sr = proc.StandardOutput)
				{
					//UNRAR 3.41 freeware      Copyright (c) 1993-2004 Alexander Roshal
					//
					//Archive \\tmsns\g$\Loghouse\SLogs\sr20050607_11001200_TK2CTWBS03_s.msn.com_w3svc1.log.rar
					//
					// Name             Size   Packed Ratio  Date   Time     Attr      CRC   Meth Ver
					//-------------------------------------------------------------------------------
					// sr20050607_11001200_TK2CTWBS03_s.msn.com_w3svc1.log 1338736271 359255839  26% 07-06-05 12:00  .....A.   51ED9AAF m3e 2.0
					//-------------------------------------------------------------------------------
					//    1       1338736271 359255839  26%
					int sizeCol = 1;
					int dateCol = 4;
					int timeCol = 5;
					int attrCol = 6;

					bool started = false;
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						line = line.Trim();
						if (!started)
						{
							if (line.StartsWith("-----"))
							{
								started = true;
							}
							continue;
						}
						if (line.StartsWith("-----"))
						{
							break;
						}
						// filenames with spaces do not work...
						int f = line.IndexOf(' ');
						if (f < 0 || f >= line.Length - 1) continue;
						string name = line.Substring(0, f);

						// check for directory attribute - is this flexible enough?
						//string[] cols = regexWhitespace.Split(line);
						line = line.Replace('\t', ' ');
						int oldLen = -1;
						while (line.Length != oldLen)
						{
							oldLen = line.Length;
							line = line.Replace("  ", " ");
						}
						line = line.Trim();
						string[] cols = line.Split(' ');

						if (cols.Length <= attrCol) continue;
						bool isDir = (cols[attrCol].ToLower().IndexOf('D') >= 0);

						if (name.Length > path.Length)
						{
							// should be that name[path.Length-1] == '\'
							int nextSep = name.IndexOfAny(pathSeperators, path.Length);
							if (nextSep > 0)
							{
								name = name.Substring(0, nextSep);
								isDir = true;
							}
						}

						ArrayList list;
						if (isDir)
						{
							if (!allowDirectory) continue;
							name = archiveName + "/" + name + "/";
							list = resDirs;
						}
						else
						{
							if (!allowFile) continue;
							name = archiveName + "/" + name;
							list = res;
						}

						long len = 0;
						DateTime lastMod = DateTime.MinValue;
						try
						{
							len = long.Parse(cols[sizeCol]);
							lastMod = DateTime.Parse(cols[dateCol] + " " + cols[timeCol]);
						}
						catch
						{
							// ignore??
						}
						list.Add(new StreamInfo(name, len, lastMod));
					}
				}
				res.Sort();
				for (int i = res.Count - 1; i > 0; i--)
				{
					if (((StreamInfo)res[i]).Equals(res[i - 1]))
					{
						res.RemoveAt(i);
					}
				}
				if (resDirs.Count != 0)
				{
					resDirs.Sort();
					for (int i = resDirs.Count - 1; i > 0; i--)
					{
						if (((StreamInfo)resDirs[i]).Equals(resDirs[i-1]))
						{
							resDirs.RemoveAt(i);
						}
					}
					resDirs.AddRange(res);
					res = resDirs;
				}
				return (StreamInfo[])res.ToArray(typeof(StreamInfo));
			}
			catch (System.ComponentModel.Win32Exception)
			{
				//				throw new InvalidOperationException("Z7zDecodeStream requires 7z.exe or 7za.exe to be in the " +
				//					"path for decompression. " +
				//					"See http://7-zip.org");
				return new StreamInfo[0];
			}
			catch //(Exception ex)
			{
				//				System.Diagnostics.Debug.WriteLine("Exception when getting uncompressed length: " +
				//					ex.ToString());
				return new StreamInfo[0];
			}
			finally
			{
				try
				{
					if (proc != null) proc.Close();
				}
				catch
				{
					// ignore
				}
			}
		}



		/// <summary>
		/// Get whether the stream can seek. Currently returns true, although Seek()
		/// will actually work with low performance.
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				return true;
			}
		}


		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for compressed files, and seeking backwards reopens the file!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset)
		{
			return base.Seek(offset);
		}
		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for compressed files, and seeking backwards reopens the file!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			return base.Seek(offset, origin);
		}
		/// <summary>
		/// Get or Set the position in the file, in bytes. Note that setting the position can
		/// be very slow for compressed files!
		/// </summary>
		public override long Position
		{
			get
			{
				return base.Position;
			}
			set
			{
				base.Position = value;
			}
		}
	}

	#endregion


	#region Gzip

	/// <summary>
	/// Wrapper Stream to perform gzip decompression.
	/// </summary>
	public class GzipDecodeStream : CmdStream
	{
		private static readonly string gzipCmd = "gzip.exe";
		private static readonly string gzipCmdDecompressFromFileArgs = "-d -q -c \"{0}\"";
		private static readonly string gzipCmdDecompressFromStreamArgs = "-d -q -c";
		private readonly string fileName;

		/// <summary>
		/// Create a stream to decompress with gzip.
		/// The "gzip" program must be in the path for this to work.
		/// </summary>
		/// <param name="fileName">name of the file to be wrapped for reading</param>
		/// <exception cref="FileNotFoundException">fileName cannot be found</exception>
		/// <exception cref="IOException">fileName cannot be opened</exception>
		/// <exception cref="InvalidOperationException">gzip.exe cannot be found.</exception>
		/// <exception cref="ArgumentNullException">The fileName is null.</exception>
		public GzipDecodeStream(string fileName)
			: this(fileName, GetLengthInner(fileName))
		{
		}

		private GzipDecodeStream(string fileName, long length)
			: base(
			gzipCmd,
			string.Format(gzipCmdDecompressFromFileArgs, fileName),
			false,
			true,
			fileName)
		{
			this.fileName = fileName;
			this.length = length;
		}


		/// <summary>
		/// Create a readable stream to decompress with gzip.
		/// The "gzip" program must be in the path for this to work.
		/// </summary>
		/// <param name="baseStream">Stream to be wrapped for reading</param>
		public GzipDecodeStream(Stream baseStream)
			: base(
			baseStream,
			gzipCmd,
			gzipCmdDecompressFromStreamArgs)
		{
			this.fileName = null;
			this.length = -1;
		}


		private long length = -1;

		/// <summary>
		/// Get the uncompressed length of the file, in bytes.
		/// </summary>
		/// <exception cref="NotSupportedException">The length cannot be retrieved, sometimes.</exception>
		public override long Length
		{
			get
			{
				if (length < 0)
				{
					if (fileName == null)
					{
						throw new NotSupportedException("Cannot get length of compressed Stream.");
					}
					length = GetLength(fileName);
					if (length < 0)
					{
						throw new NotSupportedException("Cannot always get length of compressed file.");
					}
				}
				return length;
			}
		}

		/// <summary>
		/// Get the uncompressed length of a compressed file, in bytes.
		/// </summary>
		/// <param name="fileName">name of the compressed file</param>
		/// <returns>the uncompressed length in bytes, or -1 if it cannot be determined</returns>
		public static long GetLength(string fileName)
		{
			try
			{
				return GetLengthInner(fileName);
			}
			catch
			{
				return -1;
			}
		}
		private static long GetLengthInner(string fileName)
		{
			if (fileName == null)  throw new ArgumentNullException("fileName", "fileName cannot be null");
			if (!File.Exists(fileName))  throw new FileNotFoundException("File cannot be found: " + fileName);
#if GZIP_WEAK_LENGTH
			long length = -1;
			System.Diagnostics.Process proc = null;
			try
			{
				System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
					gzipCmd,
					"-l \"" + fileName + "\"");
				//psi.WorkingDirectory = workingDir;
				psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
				psi.CreateNoWindow = true;
				psi.RedirectStandardInput = false;
				psi.RedirectStandardOutput = true;
				psi.UseShellExecute = false;
				proc = System.Diagnostics.Process.Start(psi);
				using (StreamReader sr = proc.StandardOutput)
				{
					////// gzip will simply fail for large files!!
					//         compressed        uncompressed  ratio uncompressed_name
					//         6928853283           159682881 -4239.1% sr20051221.txt
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						if (line.Trim().StartsWith("compressed"))
						{
							line = sr.ReadLine();
							if (line != null)
							{
								line = line.Trim();
								//string[] cols = System.Text.RegularExpressions.Regex.Split(line, @"\s+");
								line = line.Replace('\t', ' ');
								int oldLen = -1;
								while (line.Length != oldLen)
								{
									oldLen = line.Length;
									line = line.Replace("  ", " ");
								}
								line = line.Trim();
								string[] cols = line.Split(' ');

								string size = cols[1];
								length = long.Parse(size);
								long compressedLength = long.Parse(cols[0]);
								// what to do?? It is only correct mod 2^32 ***
								while (compressedLength > length + 1000)
								{
									length += (1L << 32);
								}
								break;
							}
						}
					}
				}
			}
			catch (System.ComponentModel.Win32Exception)
			{
				throw new InvalidOperationException("GzipDecodeStream requires gzip.exe to be in the " +
					"path for decompression. " +
					"See http://gnuwin32.sourceforge.net/packages/gzip.htm");
			}
			catch (Exception ex)
			{
				System.Diagnostics.Debug.WriteLine("Exception when getting uncompressed length: " +
					ex.ToString());
			}
			finally
			{
				try
				{
					if (proc != null)  proc.Close();
				}
				catch
				{
					// ignore
				}
			}
			if (length < 0)
			{
				throw new IOException("File cannot be opened: " + fileName);
			}
			return length;
#else
			// we lose the helpful message about not having the tools... ***
			try
			{
				return GzipEncodeStream.GetLengthTag(fileName);
			}
			catch
			{
				throw new IOException("File cannot be opened: " + fileName);
			}
#endif
		}
	

		/// <summary>
		/// Get whether the stream can seek. Currently returns true, although Seek()
		/// will actually work with low performance.
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				return true;
			}
		}


		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for compressed files, and seeking backwards reopens the file!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset)
		{
			return base.Seek(offset);
		}
		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for compressed files, and seeking backwards reopens the file!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			return base.Seek(offset, origin);
		}
		/// <summary>
		/// Get or Set the position in the file, in bytes. Note that setting the position can
		/// be very slow for compressed files!
		/// </summary>
		public override long Position
		{
			get
			{
				return base.Position;
			}
			set
			{
				base.Position = value;
			}
		}
	
	}




	/// <summary>
	/// Wrapper Stream to perform gzip compression.
	/// </summary>
	public class GzipEncodeStream : CmdStream
	{
		private static readonly string gzipCmd = "gzip.exe";
		private static readonly string gzipCmdCompressToFileArgs = "-q -c {0}";

		private string fileName;

		//// *** Append could be enabled...
		//// *** Should check if it is always better to open the stream ourselves -
		////     it shouldn't be, but it seems to be so.

		/// <summary>
		/// Create a writable stream to compress with gzip.
		/// The "gzip" program must be in the path for this to work.
		/// </summary>
		/// <param name="fileName">name of the file to be wrapped for writing</param>
		public GzipEncodeStream(string fileName)
			: base(
			gzipCmd,
			string.Format(gzipCmdCompressToFileArgs, ""),
			//new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.Read))
			fileName + "$", true)
		{
			this.fileName = fileName;
		}

		/// <summary>
		/// Create a writable stream to compress with gzip.
		/// The "gzip" program must be in the path for this to work.
		/// </summary>
		/// <param name="fileName">name of the file to be wrapped for writing</param>
		/// <param name="compressionLevel">level of compression to use, from 1 (low) to 9 (high)</param>
		public GzipEncodeStream(string fileName, int compressionLevel)
			: base(
			gzipCmd,
			string.Format(gzipCmdCompressToFileArgs, "-" + Math.Max(1, Math.Min(9, compressionLevel))),
			//new FileStream(fileName, FileMode.Create,FileAccess.Write, FileShare.Read))
			fileName + "$", true)
		{
			this.fileName = fileName;
		}

		/// <summary>
		/// Create a writable stream to compress with gzip.
		/// The "gzip" program must be in the path for this to work.
		/// </summary>
		/// <param name="baseStream">stream to be wrapped for writing</param>
		public GzipEncodeStream(Stream baseStream)
			: base(
			gzipCmd,
			string.Format(gzipCmdCompressToFileArgs, ""),
			baseStream)
		{
			this.fileName = null;
		}

		/// <summary>
		/// Create a writable stream to compress with gzip.
		/// The "gzip" program must be in the path for this to work.
		/// </summary>
		/// <param name="baseStream">stream to be wrapped for writing</param>
		/// <param name="compressionLevel">level of compression to use, from 1 (low) to 9 (high)</param>
		public GzipEncodeStream(Stream baseStream, int compressionLevel)
			: base(
			gzipCmd,
			string.Format(gzipCmdCompressToFileArgs, "-" + Math.Max(1, Math.Min(9, compressionLevel))),
			baseStream)
		{
			this.fileName = null;
		}

		// RFC1952:

		/*

+---+---+---+---+---+---+---+---+---+---+
|ID1|ID2|CM |FLG|     MTIME     |XFL|OS | (more-->)
+---+---+---+---+---+---+---+---+---+---+
		ID1 = 31 (0x1f, \037), ID2 = 139 (0x8b, \213)
		CM = 0-7 are reserved. CM = 8 is Deflate
		FLG :
			bit 0   FTEXT
			bit 1   FHCRC
			bit 2   FEXTRA
			bit 3   FNAME
			bit 4   FCOMMENT
			bit 5   reserved
			bit 6   reserved
			bit 7   reserved
(if FLG.FEXTRA set) 
+---+---+=================================+
| XLEN  |...XLEN bytes of "extra field"...| (more-->)
+---+---+=================================+
(if FLG.FNAME set) 
+=========================================+
|...original file name, zero-terminated...| (more-->)
+=========================================+
(if FLG.FCOMMENT set) 
+===================================+
|...file comment, zero-terminated...| (more-->)
+===================================+
(if FLG.FHCRC set) 
+---+---+
| CRC16 |
+---+---+

+=======================+
|...compressed blocks...| (more-->)
+=======================+

  0   1   2   3   4   5   6   7
+---+---+---+---+---+---+---+---+
|     CRC32     |     ISIZE     |
+---+---+---+---+---+---+---+---+

		 */

		// when empty:
		// 1F | 8B | 08 | 00 | 00 00 00 00 | 00 | 0B | 03 00 | 00 00 00 00 | 00 00 00 00

		private static readonly byte[] emptyGzip = new byte[]
			{
				0x1F, 0x8B, 0x08, 0x00, 0x00,0x00,0x00,0x00,  0x00, 0x0B,
				0x03,0x00,
				0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00
			};

		private static readonly byte[] lenGzip = new byte[]
			{
				0x1F, 0x8B, 0x08, 0x04, 0x00,0x00,0x00,0x00,  0x00, 0x0B,
				0x0C,0x00, (byte)'L', (byte)'N', 0x00,0x08, 0x01,0x01,0x01,0x01,0x01,0x01,0x01,0x01,
				0x03,0x00,
				0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00
			};
		private static readonly byte[] lenPreGzip = new byte[]
			{
				0x1F, 0x8B, 0x08, 0x04, 0x00,0x00,0x00,0x00,  0x00, 0x0B,
				0x0C,0x00, (byte)'L', (byte)'N', 0x00,0x08
			};
		private static readonly byte[] lenPostGzip = new byte[]
			{
				0x03,0x00,
				0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00
			};
		// gzip.exe verifies that length; other applications do not.

		/// <summary>
		/// 
		/// </summary>
		/// <param name="fileName"></param>
		/// <param name="length"></param>
		private static void AddLengthTag(string fileName, long length)
		{
			//using (Stream s = ZStreamOut.Open(fileName, true))
			using (Stream s = new FileStream(fileName, FileMode.Open, FileAccess.Write))
			{
				s.Seek(0, SeekOrigin.End);
				s.Write(lenPreGzip, 0, lenPreGzip.Length);
				s.Write(BitConverter.GetBytes(length), 0, 8);
				s.Write(lenPostGzip, 0, lenPostGzip.Length);
			}
		}

		/// <summary>
		/// <para>Releases the unmanaged resources and optionally
		/// releases the managed resources.</para>
		/// </summary>
		/// <remarks>
		/// Actually, the value of disposing is ignored, just as in <see cref="FileStream"/>.
		/// </remarks>
		/// <param name="disposing">
		/// <see langword="true" /> to release both managed and unmanaged resources; <see langword="false" /> to release only unmanaged resources.</param>
		protected override void Dispose(bool disposing)
		{
			long len = Position;
			base.Dispose(disposing);
			if (fileName != null)
			{
				if (len > uint.MaxValue)
				{
					try
					{
						// hack the length
						AddLengthTag(fileName, len);
					}
					catch
					{
						// ignore
					}
				}
			}
		}

		internal static long GetLengthTag(string fileName)
		{
			//// skip for small files?
			//FileInfo fi = new FileInfo(fileName);
			//if (fi.Length < 12000000) return -1;

			using (Stream s = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
			{
				//// This really should support reading the tag from the beginning!
				//// TODO: ***
				////  - Support LN tag at the beginning
				////  - Enable easy adding of tag to end of existing gzip files
				////  - Enable seekable gzip files

				if (s.Length < 34)
				{
					s.Seek(-4, SeekOrigin.End);
					byte[] sbuf = new byte[4];
					if (s.Read(sbuf, 0, 4) != 4) return -1;
					return BitConverter.ToUInt32(sbuf, sbuf.Length - 4);
				}

				s.Seek(-34, SeekOrigin.End);
				byte[] buf = new byte[34];
				int c = s.Read(buf, 0, 34);
				if (c < 34)
				{
					if (c < 4) return -1;
					return BitConverter.ToUInt32(buf, c - 4);
				}

				//0x1F, 0x8B, 0x08, 0x04, 0x00,0x00,0x00,0x00,  0x00, 0x0B,
				//0x0C,0x00, (byte)'L', (byte)'N', 0x00,0x08, 0x01,0x01,0x01,0x01,0x01,0x01,0x01,0x01,
				//0x03,0x00,
				//0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00

				// cross our fingers?? ***
				bool match = true;
				for (int i = 0; i < lenPreGzip.Length; i++)
				{
					if (buf[i] != lenPreGzip[i])
					{
						match = false;
						break;
					}
				}
				if (match)
				{
					int offset = buf.Length - lenPostGzip.Length;
					for (int i = 0; i < lenPostGzip.Length; i++)
					{
						if (buf[offset + i] != lenPostGzip[i])
						{
							match = false;
							break;
						}
					}
				}
				if (!match)
				{
					return BitConverter.ToUInt32(buf, buf.Length - 4);
				}
				return BitConverter.ToInt64(buf, lenPreGzip.Length);
			}
		}
	}

	#endregion


	#region LZMA
#if ENABLE_LZMA
#if LZMA_PLAIN
	/// <summary>
	/// Class to perform LZMA encoding and decoding.
	/// </summary>
	public class Lzma
	{
		/// <summary>
		/// Compress the given buffer.
		/// </summary>
		/// <param name="input">the original data</param>
		/// <returns>the compressed data</returns>
		public static byte[] Compress(byte[] input)
		{
			return Process(input, true);
		}
		
		/// <summary>
		/// Decompress the given buffer.
		/// </summary>
		/// <param name="input">the original compressed data</param>
		/// <returns>the uncompressed data</returns>
		public static byte[] Decompress(byte[] input)
		{
			return Process(input, false);
		}
		
		private static byte[] Process(byte[] input, bool compress)
		{
#if ENABLE_PROCESS
#if LZMA_PLAIN
			string lzmaCmd = "lzma";
			string lzmaCmdArgs = compress ? "e -si -so" : "d -si -so";
#else
			string lzmaCmd = "7za";
			string lzmaCmdArgs = compress ? "a -t7z -bd -si -so" : "e -t7z -bd -si -so";
#endif
			System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(lzmaCmd, lzmaCmdArgs);
			//psi.WorkingDirectory = workingDir;
			psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
			psi.CreateNoWindow = true;
			psi.RedirectStandardInput = true;
			psi.RedirectStandardOutput = true;
			psi.UseShellExecute = false;
			System.Diagnostics.Process lzmaProc = System.Diagnostics.Process.Start(psi);
			StreamWriter inWriter  = lzmaProc.StandardInput;
			StreamReader outReader = lzmaProc.StandardOutput;
			Stream inStream = inWriter.BaseStream;
			Stream outStream = outReader.BaseStream;

			inStream.Write(input, 0, input.Length);
			inStream.Flush();
			inStream.Close();

			ArrayList res = new ArrayList();
			for (int b = outStream.ReadByte(); b >= 0; b = outStream.ReadByte())
			{
				res.Add((byte)b);
			}
			return (byte[])res.ToArray(typeof(byte));
#else
			return null;
#endif
		}
	}



	//	class LzmaStream : CmdStream
	//	{
	//		private static readonly string lzmaCmd = "lzma";
	//		private static readonly string lzmaCmdCompressArgs   = "e -si -so";
	//		private static readonly string lzmaCmdDecompressArgs = "d -si -so";
	//
	//		public LzmaStream(Stream orig, bool compress, bool forWriting)
	//			: base(orig, lzmaCmd, (compress ? lzmaCmdCompressArgs : lzmaCmdDecompressArgs), forWriting)
	//		{
	//		}
	//		public LzmaStream(byte[] orig, bool compress)
	//			: base(orig, lzmaCmd, (compress ? lzmaCmdCompressArgs : lzmaCmdDecompressArgs))
	//		{
	//		}
	//	}


	/// <summary>
	/// Wrapper Stream to perform LZMA compression.
	/// </summary>
	public class LzmaEncodeStream : CmdStream
	{
#if LZMA_PLAIN
		private static readonly string lzmaCmd = "lzma";
		private static readonly string lzmaCmdCompressArgs   = "e -si -so";
		private static readonly string lzmaCmdCompressToFileArgs   = "e -si \"{0}\"";
		private static readonly string lzmaCmdCompressFromFileArgs   = "e -so \"{0}\"";
#else
		private static readonly string lzmaCmd = "7za";
		private static readonly string lzmaCmdCompressArgs   = "a -t7z -bd -si -so";
		private static readonly string lzmaCmdCompressToFileArgs   = "a -t7z -bd -si \"{0}\"";
		private static readonly string lzmaCmdCompressFromFileArgs   = "a -t7z -bd -so \"{0}\"";
#endif

		/// <summary>
		/// Create a stream to compress with LZMA.
		/// If forWriting is false, the stream will be read from orig and compressed on the fly.
		/// If forWriting is true, the stream will write a compressed version of all written data to orig.
		/// Even if Close() is not called, Finish() should be used to terminate the compression when writing.
		/// The "lzma" program must be in the path for this to work.
		/// </summary>
		/// <param name="orig">the stream to be wrapped for reading or writing</param>
		/// <param name="forWriting">If false, makes a read-only stream that reads from orig.
		/// If true, makes a write-only stream that writes to orig.</param>
		public LzmaEncodeStream(Stream orig, bool forWriting)
			: base(orig, lzmaCmd, lzmaCmdCompressArgs, forWriting)
		{
		}

		/// <summary>
		/// Create a stream to compress with LZMA.
		/// The stream will be read from orig and compressed on the fly.
		/// The "lzma" program must be in the path for this to work.
		/// </summary>
		/// <param name="orig">the bytes to be wrapped for reading</param>
		/// <param name="compress">if true, compress; if false, decompress</param>
		public LzmaEncodeStream(byte[] orig, bool compress)
			: base(orig, lzmaCmd, lzmaCmdCompressArgs)
		{
		}

		/// <summary>
		/// Create a stream to compress with LZMA.
		/// If forWriting is false, the stream will be read from orig and compressed on the fly.
		/// If forWriting is true, the stream will write a compressed version of all written data to orig.
		/// Even if Close() is not called, Finish() should be used to terminate the compression when writing.
		/// The "lzma" program must be in the path for this to work.
		/// </summary>
		/// <param name="fileName">the name of the file to be wrapped for reading or writing</param>
		/// <param name="forWriting">If false, makes a read-only stream that reads from orig.
		/// If true, makes a write-only stream that writes to orig.</param>
		public LzmaEncodeStream(string fileName, bool forWriting)
			: base(
			new FileStream(fileName, forWriting ? FileMode.Create : FileMode.Open),
			lzmaCmd,
			string.Format(forWriting ? lzmaCmdCompressToFileArgs : lzmaCmdCompressFromFileArgs, fileName),
			forWriting)
		{
		}

	}


	/// <summary>
	/// Wrapper Stream to perform LZMA decompression.
	/// </summary>
	public class LzmaDecodeStream : CmdStream
	{
#if LZMA_PLAIN
		private static readonly string lzmaCmd = "lzma";
		private static readonly string lzmaCmdDecompressArgs = "d -si -so";
		private static readonly string lzmaCmdDecompressToFileArgs = "d -si \"{0}\"";
		private static readonly string lzmaCmdDecompressFromFileArgs = "d -so \"{0}\"";
#else
		private static readonly string lzmaCmd = "7za";
		private static readonly string lzmaCmdDecompressArgs = "e -t7z -bd -si -so";
		private static readonly string lzmaCmdDecompressToFileArgs = "e -t7z -bd -si \"{0}\"";
		private static readonly string lzmaCmdDecompressFromFileArgs = "e -t7z -bd -so \"{0}\"";
#endif

		/// <summary>
		/// Create a stream to decompress with LZMA.
		/// If forWriting is false, the stream will be read from orig and decompressed on the fly.
		/// If forWriting is true, the stream will write a decompressed version of all written data to orig.
		/// Even if Close() is not called, Finish() should be used to terminate the decompression when writing.
		/// The "lzma" program must be in the path for this to work.
		/// </summary>
		/// <param name="orig">the stream to be wrapped for reading or writing</param>
		/// <param name="compress">if true, compress; if false, decompress</param>
		/// <param name="forWriting">If false, makes a read-only stream that reads from orig.
		/// If true, makes a write-only stream that writes to orig.</param>
		public LzmaDecodeStream(Stream orig, bool compress, bool forWriting)
			: base(orig, lzmaCmd, lzmaCmdDecompressArgs, forWriting)
		{
		}

		/// <summary>
		/// Create a stream to decompress with LZMA.
		/// The stream will be read from orig and decompressed on the fly.
		/// The "lzma" program must be in the path for this to work.
		/// </summary>
		/// <param name="orig">the bytes to be wrapped for reading</param>
		/// <param name="compress">if true, compress; if false, decompress</param>
		public LzmaDecodeStream(byte[] orig, bool compress)
			: base(orig, lzmaCmd, lzmaCmdDecompressArgs)
		{
		}


		/// <summary>
		/// Create a stream to decompress with LZMA.
		/// If forWriting is false, the stream will be read from orig and decompressed on the fly.
		/// If forWriting is true, the stream will write a decompressed version of all written data to orig.
		/// Even if Close() is not called, Finish() should be used to terminate the decompression when writing.
		/// The "lzma" program must be in the path for this to work.
		/// </summary>
		/// <param name="fileName">the name of the file to be wrapped for reading or writing</param>
		/// <param name="forWriting">If false, makes a read-only stream that reads from orig.
		/// If true, makes a write-only stream that writes to orig.</param>
		public LzmaDecodeStream(string fileName, bool forWriting)
			: base(
			new FileStream(fileName, forWriting ? FileMode.Create : FileMode.Open, forWriting ? FileAccess.Write : FileAccess.Read, forWriting ? FileShare.Read : FileShare.Read),
			lzmaCmd,
			string.Format(forWriting ? lzmaCmdDecompressToFileArgs : lzmaCmdDecompressFromFileArgs, fileName),
			forWriting)
		{
		}
	}
#endif
#endif
	#endregion

}

