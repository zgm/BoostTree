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
	/// <summary>
	/// General functionality supporting TMSNStore usage.
	/// </summary>
	/// <remarks>
	/// <p>
	/// This class provides functionality that supports the <see cref="TMSNStoreStream"/>
	/// class.
	/// </p>
	/// <p>
	/// A tstore.exe command is needed in the path in order for this functionality
	/// to operate correctly.
	/// </p>
	/// </remarks>
	///// <p>
	///// A shared set of tools will be used if no version is found in the path, although this
	///// increases the startup time. The environment variable TMSNSTORE_TOOLS can be used to
	///// specify a directory or share for this purpose, instead of the default.
	///// </p>
	internal class TMSNStoreUtility
	{
		//private static bool checkedTMSNStoreToolShare = false;
		//private static string tmsnStoreToolShare = @"\\tmsn\tmsnStore";

		private static string tmsnStoreCmd = "tstore.exe";
		private static bool tmsnStoreCmdFailed = false;
		private static bool tmsnStoreCmdChecked = false;
		internal static string[] tmsnStoreCmds = { tmsnStoreCmd };

		//internal static readonly string tmsnStoreDirCmdArgs = "dir \"{0}\"";
		internal static readonly string tmsnStoreInCmdArgs = "/i \"{0}\"";


		/// <summary>
		/// Helper function to reliably execute a comsos.exe command.
		/// </summary>
		/// <param name="args">the arguments to provide to tmsnStore.exe</param>
		/// <returns>A Process for the executing command</returns>
		internal static System.Diagnostics.Process TMSNStoreExec(string args)
		{
			System.Diagnostics.Process proc = null;
			if (!tmsnStoreCmdFailed)
			{
				try
				{
					if (!tmsnStoreCmdChecked)
					{
						try
						{
							if (File.Exists(Path.Combine(DllPath, tmsnStoreCmd)))
							{
								tmsnStoreCmd = Path.Combine(DllPath, tmsnStoreCmd);
							}
							else if (File.Exists(Path.Combine(Path.Combine(DllPath, "bin"), tmsnStoreCmd)))
							{
								tmsnStoreCmd = Path.Combine(DllPath, tmsnStoreCmd);
							}
							tmsnStoreCmds[0] = tmsnStoreCmd;
						}
						catch
						{
							// ignore...
						}
						tmsnStoreCmdChecked = true;
					}
					System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
						tmsnStoreCmd, args);
					//psi.WorkingDirectory = workingDir;
					psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
					psi.CreateNoWindow = true;
					psi.RedirectStandardInput = false;
					psi.RedirectStandardOutput = true;
					psi.UseShellExecute = false;
					proc = System.Diagnostics.Process.Start(psi);
				}
				catch
				{
					tmsnStoreCmdFailed = true;
					tmsnStoreCmds[0] = null;
				}
			}

			if (proc == null)
			{
				throw new IOException("No TMSNStore client executable available");
			}
			return proc;
		}

		private static string dllPath = null;
		private static string DllPath
		{
			get
			{
				if (dllPath == null)
				{
					lock (typeof(TMSNStoreUtility))
					{
						dllPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
						dllPath = Path.GetDirectoryName(dllPath);
					}
				}
				return dllPath;
			}
		}

		#region Hiding Members
		private TMSNStoreUtility()
		{
		}

		/// <exclude/>
		/// <summary></summary>
		/// <returns></returns>
		protected new object MemberwiseClone()
		{
			return base.MemberwiseClone();
		}

		/// <exclude/>
		/// <summary></summary>
		/// <param name="obj"></param>
		/// <returns></returns>
		public override bool Equals(object obj)
		{
			return base.Equals(obj);
		}

		/// <exclude/>
		/// <summary></summary>
		/// <returns></returns>
		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		/// <exclude/>
		/// <summary></summary>
		/// <returns></returns>
		public override string ToString()
		{
			return base.ToString();
		}
		#endregion


		/// <summary>
		/// Determine if a path is a TMSNStore table.
		/// </summary>
		/// <param name="path">the path to check</param>
		/// <returns>true if path is a TMSNStore; false otherwise</returns>
		/// <remarks>
		/// This checks the path textually, and does not verify that the given
		/// location actually exists.
		/// </remarks>
		public static bool IsTMSNStore(string path)
		{
			if (path == null || path.Length == 0 ||
				(string.Compare(path, 0, "store:", 0, "store:".Length, true) != 0 &&
				string.Compare(path, 0, "tmsnstore:", 0, "tmsnstore:".Length, true) != 0 &&
				string.Compare(path, 0, "records:", 0, "records:".Length, true) != 0))
			{
				return false;
			}
			return true;
		}

		/// <summary>
		/// Get the standard form of the given TMSNStore path.
		/// </summary>
		/// <param name="path">the path to transform</param>
		/// <returns>the canonical version of the path</returns>
		public static string GetCanonicalPath(string path)
		{
			if (path == null || path.Length == 0) return path;
			if (!IsTMSNStore(path))
			{
				return IOUtil.GetCanonicalPath(path);
			}
			int c = path.IndexOf(':');
			return path.Substring(0, c + 1).ToLower() +
				IOUtil.GetCanonicalPath(path.Substring(c + 1)).TrimEnd(IOUtil.pathSeparators);
		}

		/// <summary>
		/// Get the inner path to a TMSNStore, without the protocol information.
		/// </summary>
		/// <param name="path">the original store path, with the "store:" protocol</param>
		/// <returns>the location of the store data</returns>
		public static string StorePath(string path)
		{
			if (!IsTMSNStore(path))
			{
				return path;
			}
			// don't allow "//" decoration
			string innerPath = path.Substring(path.IndexOf(':') + 1);
			// strip off extra filename, if given? questionable... ***
			// hack for fake filename - not ideal... ***
			if (innerPath[innerPath.Length - 1] != '/' && innerPath[innerPath.Length - 1] != '\\')
			{
				string name = IOUtil.GetFileName(innerPath);
				if (name.Length != 0 && string.Compare(innerPath, innerPath.Length - 2 * name.Length - 1,
					name, 0, name.Length, true) == 0)
				{
					innerPath = innerPath.Substring(0, innerPath.Length - name.Length);
				}
			}
			return innerPath;
		}

		/// <summary>
		/// Get the root path for a TMSNStore (typically, the same path).
		/// </summary>
		/// <param name="path">the original store path, with the "store:" protocol</param>
		/// <returns>the path for the 'root'</returns>
		internal static string PathRoot(string path)
		{
			if (!IsTMSNStore(path))
			{
				return path;
			}
			//// don't allow "//" decoration
			//string innerPath = path["store:".Length - 1] == ':' ?
			//    path.Substring("store:".Length) : path.Substring("tmsnstore:".Length);
			// strip off extra filename, if given? questionable... ***
			// hack for fake filename - not ideal... ***
			if (path[path.Length - 1] != '/' && path[path.Length - 1] != '\\')
			{
				string name = IOUtil.GetFileName(path);
				if (name.Length != 0 && string.Compare(path, path.Length - 2 * name.Length - 1,
					name, 0, name.Length, true) == 0)
				{
					path = path.Substring(0, path.Length - name.Length);
				}
			}
			return path;
		}

		/// <summary>
		/// Get the paths to file-like entities within a store.
		/// </summary>
		/// <param name="path">the store to look in</param>
		/// <returns>the set of file paths for the file-like entities for that store</returns>
		/// <remarks>
		/// <p>
		/// Directories will be distinguished by ending with "/".
		/// </p>
		/// <p>
		/// This will silently return the empty list if there are any problems.
		/// </p>
		/// </remarks>
		internal static string[] DirectoryEntries(string path)
		{
			string innerPath = StorePath(path);
			if (innerPath.Length == 0) return new string[0];
			if (!IOUtil.DirectoryExists(innerPath)) return new string[0];
			string name = IOUtil.GetName(path);
			if (name.Length == 0) return new string[0];
			if (path[path.Length - 1] != '/' && path[path.Length - 1] != '\\')
			{
				path = path + (path.IndexOf('/') < 0 ? "\\" : "/");
			}
			return new string[] { path + name };
		}

		/// <summary>
		/// Get the StreamInfos for the file-like entities within a store.
		/// </summary>
		/// <param name="path">the store to look in</param>
		/// <returns>the set of StreamInfos for the file-like entities for that store</returns>
		/// <remarks>
		/// <p>
		/// This will silently return the empty list if there are any problems.
		/// </p>
		/// </remarks>
		internal static StreamInfo[] DirectoryEntriesInfo(string path)
		{
			string[] res = DirectoryEntries(path);
			if (res.Length == 0) return new StreamInfo[0];
			DateTime time = DateTime.MinValue;
			//time = Directory.GetLastWriteTime(StorePath(path));
			StreamInfo[] dir = IOUtil.DirectoryEntriesInfo(StorePath(path));
			for (int i = 0; i < dir.Length; i++)
			{
				if (dir[i].LastWriteTime > time) time = dir[i].LastWriteTime;
			}
			StreamInfo[] resi = new StreamInfo[res.Length];
			for (int i = 0; i < res.Length; i++)
			{
				resi[i] = new StreamInfo(res[i], 0, time);
			}
			return resi;
		}


		/// <summary>
		/// Get the length of a TMSNStore stream, in bytes.
		/// </summary>
		/// <param name="path">name of the stream</param>
		/// <returns>the length of the stream in bytes, or -1 if it cannot be determined</returns>
		public static long GetLength(string path)
		{
			try
			{
				return GetLengthInner(path);
			}
			catch
			{
				return -1;
			}
		}

		internal static long GetLengthInner(string path)
		{
			if (Exists(path))
			{
				return 100000000; //-1;
			}
			throw new FileNotFoundException("Store cannot be found or opened: " + path);
		}


		#region Exists Checks

		//internal static readonly System.Text.RegularExpressions.Regex regexWhitespace =
		//    new System.Text.RegularExpressions.Regex(@"\s+",
		//    System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.CultureInvariant);

		/// <summary>
		/// Determine if a TMSNStore exists.
		/// </summary>
		/// <param name="path">the path of the store</param>
		/// <returns>true if the TMSNStore exists, false otherwise</returns>
		public static bool Exists(string path)
		{
			string innerPath = StorePath(path);
			if (innerPath.Length == 0) return false;
			// is this right?? ***
			if (string.Compare(path, 0, "records:", 0, "records:".Length, true) == 0)
			{
				return IOUtil.FileExists(innerPath);
			}
			else
			{
				return IOUtil.DirectoryExists(innerPath);
			}
		}

		///// <summary>
		///// Remove the protocol, if it exists, leaving a path.
		///// </summary>
		///// <param name="path">the original stream name</param>
		///// <returns>the modified protocol</returns>
		//internal static string RemoveProtocol(string path)
		//{
		//    if (path == null || path.Length == 0 ||
		//        (string.Compare(path, 0, "tmsnstore:", 0, "tmsnstore:".Length) != 0 &&
		//        string.Compare(path, 0, "store:", 0, "store:".Length) != 0))
		//    {
		//        return path;
		//    }
		//    if (path[5] == ':')
		//    {
		//        path = path.Substring("store:".Length);
		//    }
		//    else
		//    {
		//        path = path.Substring("tmsnstore:".Length);
		//    }
		//    if (path.StartsWith("//"))
		//    {
		//        //if (path.Length < 3) return false;
		//        //if (path[2] == '/' || path[2] == '\\')
		//        //{
		//        path = path.Substring(2);
		//        //}
		//    }
		//    return path;
		//}

		#endregion

	}



	/// <summary>
	/// Wrapper Stream to perform TMSNStore reading.
	/// </summary>
	/// <remarks>
	/// <p>
	/// A tmsnStore.cmd or tmsnStore.exe command is needed in the path in order for this functionality
	/// to operate correctly.
	/// </p>
	/// <p>
	/// A shared set of tools will be used if no version is found in the path, although this
	/// increases the startup time. The environment variable COSMOS_TOOLS can be used to
	/// specify a directory or share for this purpose, instead of the default.
	/// </p>
	/// </remarks>
	public class TMSNStoreStream : CmdStream
	{

		private readonly string fileName;

		/// <summary>
		/// Create a stream to read from TMSNStore.
		/// </summary>
		/// <param name="fileName">Name of the TMSNStore stream to be wrapped for reading</param>
		/// <exception cref="ArgumentNullException">The stream name is null.</exception>
		/// <exception cref="ArgumentException">The stream name is invalid.</exception>
		/// <exception cref="InvalidOperationException">tmsnStore.cmd is not in the path, or TMSNStore cannot be contacted.</exception>
		/// <exception cref="FileNotFoundException">The TMSNStore stream cannot be found.</exception>
		public TMSNStoreStream(string fileName)
			: this(fileName, TMSNStoreUtility.GetLengthInner(fileName))
		{
		}
		private TMSNStoreStream(string fileName, long length)
			: base(
			TMSNStoreUtility.tmsnStoreCmds,
			string.Format(TMSNStoreUtility.tmsnStoreInCmdArgs,
			//TMSNStoreUtility.StorePath(fileName)),
			fileName),
			false,
			true,
			null)
		{
			this.fileName = fileName;
			this.length = length;
		}

		private long length = -1;

		/// <summary>
		/// Get the length of the stream, in bytes.
		/// </summary>
		/// <exception cref="NotSupportedException">The length cannot be retrieved, sometimes.</exception>
		/// <exception cref="ArgumentNullException">The stream name is null.</exception>
		/// <exception cref="ArgumentException">The stream name is invalid.</exception>
		/// <exception cref="InvalidOperationException">tmsnStore.cmd is not in the path, or TMSNStore cannot be contacted.</exception>
		public override long Length
		{
			get
			{
				if (length < 0)
				{
					length = TMSNStoreUtility.GetLengthInner(fileName);
					if (length < 0)
					{
						throw new NotSupportedException("Cannot always get length of TMSNStore file.");
					}
				}
				return length;
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
		/// be very slow for TMSNStore streams, and seeking backwards reopens the stream!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset)
		{
			return base.Seek(offset);
		}
		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for TMSNStore streams, and seeking backwards reopens the stream!
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
		/// be very slow for TMSNStore streams!
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

}

