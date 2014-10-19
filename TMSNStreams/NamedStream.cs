// owner: rragno

#define USE_HANDLE
//#define MONITOR
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
	/// Open named stream on NTFS.
	/// </summary>
	/// <remarks>
	/// <p>
	/// Named streams are data regions in NTFS files. The default, unnamed stream
	/// is commonly accessed, but any number of others are possible. This allows
	/// storing additional data in the file.
	/// </p>
	/// <p>
	/// Named streams only exist on NTFS file systems, generally, and might not be
	/// preserved as files are moved around in various forms.
	/// Named streams can be specified with the syntax "filename:streamname".
	/// This is supported by very few applications, for some reason. The syntax
	/// can be used at the command line for redirection, at least, with "&lt;" and
	/// "&gt;". The size given for a file always represents only the size of the
	/// default stream.
	/// </p>
	/// <p>
	/// The function <see cref="NamedStream.GetNamedStreams"/> will show the non-default
	/// stream names, if any are present. This class would largely not be needed if
	/// FileStream did not explicitly avoid opening filenames of this form, but unfortunately
	/// it does. The performance might not be optimal, so a normal Stream should
	/// be used for the default named stream.
	/// </p>
	/// </remarks>
#if USE_HANDLE
	public class NamedStream : FileStream
#else
	public class NamedStream : Stream, IDisposable
#endif
	{
		#region Utility

		/// <summary>
		/// Delete a named stream from a file.
		/// </summary>
		/// <remarks>
		/// This is equivalent to Delete(fileName + ":" + streamName).
		/// </remarks>
		/// <param name="fileName">name of file to delete the stream from</param>
		/// <param name="streamName">name of the stream to delete - if null or "*",
		/// all named streams are deleted</param>
		/// <exception cref="ArgumentNullException">fileName is null.</exception>
		/// <exception cref="IOException"> The stream could not be deleted.</exception>
		public static void Delete(string fileName, string streamName)
		{
			if (fileName == null) throw new ArgumentNullException("fileName", "fileName cannot be null.");
			if (streamName == null || streamName == "*")
			{
				foreach (string name in IOUtil.GetNamedStreams(fileName))
				{
					Delete(fileName, name);
				}
			}
			else if (streamName.Length == 0)
			{
				// delete the file completely if empty stream name? ***
				if (!IOUtil.Win32.DeleteFile(fileName))
				{
					throw new IOException("Could not delete the file: " + fileName);
				}
			}
			else
			{
				Delete(fileName + ":" + streamName);
			}
		}

		/// <summary>
		/// Delete a named stream from a file.
		/// </summary>
		/// <param name="fileName">name of file stream to delete, as "filename:streamname" -
		/// if the stream name is "*", all named streams are removed</param>
		/// <exception cref="ArgumentNullException">fileName is null.</exception>
		/// <exception cref="IOException"> The stream could not be deleted.</exception>
		public static void Delete(string fileName)
		{
			if (fileName == null) throw new ArgumentNullException("fileName", "fileName cannot be null.");
			if (fileName.EndsWith(":*"))
			{
				Delete(fileName.Substring(0, fileName.Length - 2), "*");
				return;
			}
			// delete the file completely if empty stream name? ***
			if (!IOUtil.Win32.DeleteFile(fileName))
			{
				throw new IOException("Could not delete the file: " + fileName);
			}
		}



		/// <summary>
		/// Get the named streams present in the given file.
		/// </summary>
		/// <param name="fileName">the file to inspect</param>
		/// <returns>the list of named streams available for the file, excluding the default stream</returns>
		/// <remarks>
		/// <p>
		/// An empty string array will be returned if there are any errors or if
		/// no named streams exist.
		/// </p>
		/// </remarks>
		public static string[] GetNamedStreams(string fileName)
		{
			IntPtr handle = IntPtr.Zero;
			try
			{
				handle = IOUtil.Win32.CreateFile(
					fileName,
					IOUtil.Win32.FileAccess.GENERIC_READ,
					IOUtil.Win32.FileShare.FILE_SHARE_READ | IOUtil.Win32.FileShare.FILE_SHARE_WRITE,
					IntPtr.Zero,
					IOUtil.Win32.CreationDisposition.OPEN_EXISTING,
					IOUtil.Win32.FileFlagsAndAttributes.FILE_FLAG_BACKUP_SEMANTICS,
					IntPtr.Zero);
				//if (handle == IntPtr.Zero)  return new string[0];
				if (handle.ToInt64() <= 0) return new string[0];

				byte[] streamNames = new byte[32768 << 2];
				IOUtil.Win32.IO_STATUS_BLOCK statusBlock = new IOUtil.Win32.IO_STATUS_BLOCK();

				uint status = IOUtil.Win32.NtQueryInformationFile(handle, ref statusBlock,
					streamNames, (uint)streamNames.Length, IOUtil.Win32.FILE_INFORMATION_CLASS.FileStreamInformation);

				if (!IOUtil.Win32.NT_SUCCESS(status) || statusBlock.Information == 0) return new string[0];

				ArrayList names = new ArrayList();
				for (int pos = 0; pos < streamNames.Length; )
				{
					try
					{
						int start = pos;
						// get next entry:
						int next = (int)BitConverter.ToUInt32(streamNames, pos);
						pos += 4;
						// get length:
						int len = (int)BitConverter.ToUInt32(streamNames, pos);
						pos += 4;
						// get size:
						long size = BitConverter.ToInt64(streamNames, pos);
						pos += 8;
						// get allocation size:
						long allocationSize = BitConverter.ToInt64(streamNames, pos);
						pos += 8;
						// get name:
						string name = Encoding.Unicode.GetString(streamNames, pos, len);

						if (name.Length != 0)
						{
							if (name[0] == ':' && name.EndsWith(":$DATA"))
							{
								name = name.Substring(1, name.Length - "::$DATA".Length);
							}
							if (name.Length != 0)
							{
								names.Add(name);
							}
						}

						if (next == 0) break;
						pos = start + next;
						//		private struct FILE_STREAM_INFORMATION
						//		{
						//			ULONG    	        NextEntry;
						//			ULONG    	        NameLength;
						//			LARGE_INTEGER    	Size;
						//			LARGE_INTEGER    	AllocationSize;
						//			USHORT    	        Name[1];
						//		}
					}
					catch
					{
						// ignore running off the end??
					}
				}

				return (string[])names.ToArray(typeof(string));
			}
			catch
			{
				return new string[0];
			}
			finally
			{
				if (handle != IntPtr.Zero)
				{
					try
					{
						IOUtil.Win32.CloseHandle(handle);
					}
					catch
					{
						// ignore...
					}
				}
			}
		}

		#endregion


		#region Instance Fields

		//private long length;
		private string fileName;
		private string baseName;
		private string streamName;

#if !USE_HANDLE
		private bool forWriting;
		private readonly IntPtr handle;
		private bool closed = false;
#endif

#if !USE_HANDLE
		/// <summary>
		/// Get the full name of the file opened, including the filename and
		/// the stream name.
		/// </summary>
		public string Name
		{
			get { return fileName; }
		}
#endif
		/// <summary>
		/// Get the name of the file opened, without any stream name.
		/// </summary>
		public string FileName
		{
			get { return baseName; }
		}
		/// <summary>
		/// Get the name of the stream opened in the file, or the empty
		/// string if the default stream is opened.
		/// </summary>
		public string StreamName
		{
			get { return streamName; }
		}
		#endregion


		#region Creation and Cleanup

		/// <summary>
		/// Open a named stream in a file for reading or writing.
		/// </summary>
		/// <remarks>
		/// This is equivalent to NamedStream(fileName + ":" + streamName, forWriting).
		/// </remarks>
		/// <param name="fileName">name of file to open</param>
		/// <param name="streamName">The name of the stream to open</param>
		/// <param name="forWriting">if true, open for writing; otherwise, open for reading</param>
		/// <returns>Stream to access the specified named stream</returns>
		/// <exception cref="ArgumentNullException">fileName or streamName is null.</exception>
		/// <exception cref="ArgumentException">fileName is invalid.</exception>
		/// <exception cref="FileNotFoundException">fileName cannot be found.</exception>
		/// <exception cref="IOException"> An I/O error has occurred.</exception>
		public NamedStream(string fileName, string streamName, bool forWriting)
			: this(fileName, streamName, forWriting, false)
		{
		}

		/// <summary>
		/// Open a named stream in a file for reading or writing.
		/// </summary>
		/// <remarks>
		/// This is equivalent to NamedStream(fileName + ":" + streamName, forWriting).
		/// </remarks>
		/// <param name="fileName">name of file to open</param>
		/// <param name="streamName">The name of the stream to open</param>
		/// <param name="forWriting">if true, open for writing; otherwise, open for reading</param>
		/// <param name="append">if writing, append to existing stream if true</param>
		/// <returns>Stream to access the specified named stream</returns>
		/// <exception cref="ArgumentNullException">fileName or streamName is null.</exception>
		/// <exception cref="ArgumentException">fileName is invalid.</exception>
		/// <exception cref="FileNotFoundException">fileName cannot be found.</exception>
		/// <exception cref="IOException"> An I/O error has occurred.</exception>
		public NamedStream(string fileName, string streamName, bool forWriting, bool append)
			: this((streamName == null || streamName.Length == 0) ? fileName :
			fileName + ":" + streamName, forWriting, append)
		{
		}

		/// <summary>
		/// Open a named stream in a file for reading or writing.
		/// </summary>
		/// <param name="fileName">name of file to open, as "filename:streamname"</param>
		/// <param name="forWriting">if true, open for writing; otherwise, open for reading</param>
		/// <returns>Stream to access the specified named stream</returns>
		/// <exception cref="ArgumentNullException">fileName is null.</exception>
		/// <exception cref="ArgumentException">fileName is invalid.</exception>
		/// <exception cref="FileNotFoundException">fileName cannot be found.</exception>
		/// <exception cref="IOException"> An I/O error has occurred.</exception>
		public NamedStream(string fileName, bool forWriting)
			: this(fileName, forWriting, false)
		{
		}


		private static readonly char[] pathSeparators = new char[] { '/', '\\' };

#if USE_HANDLE

				/// <summary>
		/// Open a named stream in a file for reading or writing.
		/// </summary>
		/// <remarks>
		/// This is equivalent to NamedStream(fileName + ":" + streamName, forWriting).
		/// </remarks>
		/// <param name="fileName">name of file to open, as "filename:streamname"</param>
		/// <param name="forWriting">if true, open for writing; otherwise, open for reading</param>
		/// <param name="append">if writing, append to existing stream if true</param>
		/// <returns>Stream to access the specified named stream</returns>
		/// <exception cref="ArgumentNullException">fileName is null.</exception>
		/// <exception cref="ArgumentException">fileName is invalid.</exception>
		/// <exception cref="FileNotFoundException">fileName cannot be found.</exception>
		/// <exception cref="IOException"> An I/O error has occurred.</exception>
		public NamedStream(string fileName, bool forWriting, bool append)
#if DOTNET2
			: base(new Microsoft.Win32.SafeHandles.SafeFileHandle(GetHandle(fileName, forWriting, append), true),
			forWriting ? FileAccess.Write : FileAccess.Read)
#else
			: base(GetHandle(fileName, forWriting, append), forWriting ? FileAccess.Write : FileAccess.Read, true)
#endif
		{
			if (append)
			{
				Position = Length;
			}

			this.fileName = fileName;
			this.baseName = this.fileName;
			this.streamName = "";
			int cIndex = this.fileName.LastIndexOf(':');
			if (cIndex > 0)
			{
				string bName = this.fileName.Substring(0, cIndex);
				if (File.Exists(bName))
				{
					if (cIndex > 1 ||
						fileName.IndexOfAny(pathSeparators, 2) < 0)
					{
						// named:
						this.baseName = bName;
						this.streamName = this.fileName.Substring(cIndex + 1);
					}
				}
				else
				{
					if (cIndex > 1)
					{
						throw new FileNotFoundException("Cannot find the file: " + fileName);
					}
				}
			}
		}

		private static IntPtr GetHandle(string fileName, bool forWriting, bool append)
		{
			//string baseName = fileName;
			//string streamName = "";
			//int cIndex = fileName.LastIndexOf(':');
			//if (cIndex > 0)
			//{
			//    string bName = fileName.Substring(0, cIndex);
			//    if (File.Exists(bName))
			//    {
			//        if (cIndex > 1 ||
			//            fileName.IndexOfAny(pathSeparators, 2) < 0)
			//        {
			//            // named:
			//            baseName = bName;
			//            streamName = fileName.Substring(cIndex + 1);
			//        }
			//    }
			//    else
			//    {
			//        if (cIndex > 1)
			//        {
			//            throw new FileNotFoundException("Cannot find the file: " + fileName);
			//        }
			//    }
			//}

			//if (!File.Exists(baseName))
			//{
			//    throw new FileNotFoundException("Cannot find the file: " + fileName);
			//}

			IntPtr handle = IOUtil.Win32.CreateFile(
				fileName,
				forWriting ? IOUtil.Win32.FileAccess.GENERIC_WRITE : IOUtil.Win32.FileAccess.GENERIC_READ,
				IOUtil.Win32.FileShare.FILE_SHARE_READ,
				IntPtr.Zero,
				forWriting ? (append ? IOUtil.Win32.CreationDisposition.OPEN_ALWAYS : IOUtil.Win32.CreationDisposition.CREATE_ALWAYS) : IOUtil.Win32.CreationDisposition.OPEN_EXISTING,
				0,
				IntPtr.Zero);

			if (handle.ToInt64() <= 0)
			{
				throw new IOException("Cannot open the named stream: " + fileName);
			}

			return handle;
		}

#else
		/// <summary>
		/// Open a named stream in a file for reading or writing.
		/// </summary>
		/// <remarks>
		/// This is equivalent to NamedStream(fileName + ":" + streamName, forWriting).
		/// </remarks>
		/// <param name="fileName">name of file to open, as "filename:streamname"</param>
		/// <param name="forWriting">if true, open for writing; otherwise, open for reading</param>
		/// <param name="append">if writing, append to existing stream if true</param>
		/// <returns>Stream to access the specified named stream</returns>
		/// <exception cref="ArgumentNullException">fileName is null.</exception>
		/// <exception cref="ArgumentException">fileName is invalid.</exception>
		/// <exception cref="FileNotFoundException">fileName cannot be found.</exception>
		/// <exception cref="IOException"> An I/O error has occurred.</exception>
		public NamedStream(string fileName, bool forWriting, bool append)
		{
			this.forWriting = forWriting;
			this.fileName = fileName;
			this.baseName = this.fileName;
			this.streamName = "";
			int cIndex = this.fileName.LastIndexOf(':');
			if (cIndex > 0)
			{
				string bName = this.fileName.Substring(0, cIndex);
				if (File.Exists(bName))
				{
					if (cIndex > 1 ||
						fileName.IndexOfAny(pathSeparators, 2) < 0)
					{
						// named:
						this.baseName = bName;
						this.streamName = this.fileName.Substring(cIndex + 1);
					}
				}
				else
				{
					if (cIndex > 1)
					{
						throw new FileNotFoundException("Cannot find the file: " + fileName);
					}
				}
			}

			if (!File.Exists(this.baseName))
			{
				throw new FileNotFoundException("Cannot find the file: " + fileName);
			}

			handle = IOUtil.Win32.CreateFile(
				this.fileName,
				forWriting ? IOUtil.Win32.FileAccess.GENERIC_WRITE : IOUtil.Win32.FileAccess.GENERIC_READ,
				IOUtil.Win32.FileShare.FILE_SHARE_READ,
				IntPtr.Zero,
				forWriting ? (append ? IOUtil.Win32.CreationDisposition.OPEN_ALWAYS : IOUtil.Win32.CreationDisposition.CREATE_ALWAYS) : IOUtil.Win32.CreationDisposition.OPEN_EXISTING,
				0,
				IntPtr.Zero);

			if (handle.ToInt64() <= 0)
			{
				throw new IOException("Cannot open the named stream: " + fileName);
			}
			//GetFileSizeEx(handle, out length);

			if (forWriting && append)
			{
				Position = Length;
			}
		}

		/// <summary>
		/// Release resources.
		/// </summary>
		~NamedStream()
		{
			((IDisposable)this).Dispose();
		}

		/// <summary>
		/// Release the resources used for the unbuffered file.
		/// </summary>
//		/// <param name="disposing">true if disposing, false otherwise</param>
		void IDisposable.Dispose()
		{
			//try
			//{
			//	base.Dispose(disposing);
			//}
			//catch
			//{
			//	// ignore
			//}
			try
			{
				IOUtil.Win32.CloseHandle(handle);
			}
			catch
			{
				// ignore
			}
#if MONITOR
			Console.WriteLine(":: " + Path.GetFileName(fileName) + " :: " + "Close");
			Console.WriteLine(new string('-', (":: " + Path.GetFileName(fileName) + " :: " + "Close").Length));
#endif
			GC.SuppressFinalize(this);
			closed = true;
		}

		/// <summary>
		/// Close the stream.
		/// </summary>
		public override void Close()
		{
			if (closed) return;
			closed = true;
			base.Close();
			((IDisposable)this).Dispose();
		}

#endif
		#endregion


#if !USE_HANDLE
		#region Properties

		/// <summary>
		/// Get the Length of this file, in bytes.
		/// </summary>
		public override long Length
		{
			get
			{
				long length;
				if (!IOUtil.Win32.GetFileSizeEx(handle, out length))
				{
					throw new IOException("Cannot obtain length for this named stream.");
				}
				return length;

				//// use the simple, recorded length:
				//return length;
			}
		}

		/// <summary>
		/// Get whether the stream supports reading.
		/// </summary>
		public override bool CanRead
		{
			get
			{
				// should we allow writers to also read? ***
				return !forWriting;
			}
		}

		/// <summary>
		/// Get whether the stream supports writing.
		/// </summary>
		public override bool CanWrite
		{
			get
			{
				return forWriting;
			}
		}

		#endregion


		#region Seeking

		/// <summary>
		/// Get or Set the position in the file, in bytes.
		/// </summary>
		public override long Position
		{
			get
			{
				long position;
				IOUtil.Win32.SetFilePointerEx(handle, 0, out position, IOUtil.Win32.SeekOrigin.FILE_CURRENT);
				return position;
			}
			set
			{
				Seek(value);
			}
		}

		/// <summary>
		/// Move forward by reading and discarding bytes.
		/// </summary>
		/// <param name="count">the number of bytes to skip</param>
		private void Skip(long count)
		{
			if (count == 0)  return;
			//if (forWriting)  throw new NotSupportedException();
			byte[] dump = new byte[Math.Min(count, 256 * 1024)];
			if (dump.Length != count)
			{
				int rem = (int)(count % dump.Length);
				if (Read(dump, 0, rem) < rem)  return;
				count -= rem;
			}
			while (count > 0)
			{
				int read = Read(dump, 0, dump.Length);
				if (read != dump.Length)
				{
					//System.Diagnostics.Debug.WriteLine("Skip failed! Read " + read + " / " + dump.Length + " for chunk.");
					return;
				}
				count -= dump.Length;
			}
		}


		/// <summary>
		/// Seek to a new position in the file, in bytes.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public long Seek(long offset)
		{
			return Seek(offset, SeekOrigin.Begin);
		}
		/// <summary>
		/// Seek to a new position in the file, in bytes.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			long cur = Position;
			switch (origin)
			{
				case SeekOrigin.Begin:
					break;
				case SeekOrigin.Current:
					offset += cur;
					break;
				case SeekOrigin.End:
					offset = Length - offset;
					break;
			}
			if (offset < 0) offset = 0;
			if (offset == cur) return cur;

			if (offset > cur && (offset - cur) < 100000)
			{
				Skip(offset - cur);
			}
			else
			{
				// ignore errors?
				long newPosition;
				bool res = IOUtil.Win32.SetFilePointerEx(handle, offset, out newPosition, IOUtil.Win32.SeekOrigin.FILE_BEGIN);
			}
			return Position;
		}

		/// <summary>
		/// Get whether the stream supports seeking.
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				return true;
			}
		}

		#endregion


		#region Reading
		
		#region Public Read Functionality

		/// <summary>
		/// Reads a block of bytes from the stream and writes the data in a given buffer.
		/// </summary>
		/// <param name="buffer">the array in which the values are replaced by the bytes read</param>
		/// <returns>
		/// The total number of bytes read into the buffer. This will be 0 if the end
		/// of the stream has been reached, and is guaranteed to be less than buffer.Length only if
		/// fewer than buffer.Length bytes remain (and it will then equal the remainder of the bytes).
		/// </returns>
		/// <exception cref="ArgumentNullException">The buffer is null</exception>
		/// <exception cref="NotSupportedException">The stream was opened for writing.</exception>
		public int Read(byte[] buffer)
		{
			return Read(buffer, 0, buffer.Length);
		}
		/// <summary>
		/// Reads a block of bytes from the stream and writes the data in a given buffer.
		/// </summary>
		/// <param name="buffer">the array in which the values between offset and (offset + count - 1) are replaced by the bytes read</param>
		/// <param name="offset">The byte offset in array at which to begin reading. </param>
		/// <param name="count">The maximum number of bytes to read. </param>
		/// <returns>
		/// The total number of bytes read into the buffer. This will be 0 if the end
		/// of the stream has been reached, and is guaranteed to be less than count only if
		/// fewer than count bytes remain (and it will then equal the remainder of the bytes).
		/// </returns>
		/// <exception cref="ArgumentOutOfRangeException">The counts are out of range.</exception>
		/// <exception cref="ArgumentNullException">The buffer is null</exception>
		/// <exception cref="NotSupportedException">The stream was opened for writing.</exception>
		public override int Read(byte[] buffer, int offset, int count)
		{

			if (offset + count > buffer.Length) throw new ArgumentOutOfRangeException("offset + count > buffer.Length");
			if (buffer == null) throw new ArgumentNullException("buffer is null", "buffer");
			if (offset < 0 || count < 0) throw new ArgumentOutOfRangeException("Negative offset or count");

			int bytesRead;
			if (offset == 0)
			{
				if (!IOUtil.Win32.ReadFile(handle, buffer, count, out bytesRead, IntPtr.Zero))
				{
					bytesRead = 0;
				}
			}
			else
			{
				byte[] tmp = new byte[count];
				if (!IOUtil.Win32.ReadFile(handle, tmp, count, out bytesRead, IntPtr.Zero))
				{
					bytesRead = 0;
				}
				Buffer.BlockCopy(tmp, 0, buffer, offset, bytesRead);
			}
			return bytesRead;
		}

		private readonly byte[] readByteBuffer = new byte[1];
		/// <summary>
		/// Retrieve the next byte in the stream and advance the position.
		/// </summary>
		/// <returns>the next byte, or -1 if at end of stream</returns>
		/// <remarks>This is not as efficient as block reading, because of overhead issues.</remarks>
		/// <exception cref="NotSupportedException">The stream was opened for writing.</exception>
		public override int ReadByte()
		{
			if (Read(readByteBuffer) == 0)
			{
				return -1;
			}
			return readByteBuffer[0];
		}

		///// <summary>
		///// Check if the end of file has been reached.
		///// </summary>
		///// <returns>true if no more bytes remain; false otherwise</returns>
		//public bool Eof()
		//{
		//    if (currentBufferBottom < currentBufferLimit)  return false;
		//    //if (gotDone)  return true;
		//    // is this good enough? We want to be cheap...
		//    return (Position < Length);
		//}
		
		#endregion

		#endregion


		#region Writing

		/// <summary>
		/// Write all pending data, if writing.
		/// </summary>
		public override void Flush()
		{
			if (forWriting)
			{
				IOUtil.Win32.FlushFileBuffers(handle);
			}
		}

		/// <summary>
		/// Set the length of the file - only supported if writing.
		/// </summary>
		/// <param name="value">the length that the file will be set to</param>
		/// <exception cref="NotSupportedException">The stream was opened for reading.</exception>
		public override void SetLength(long value)
		{
			if (!forWriting) throw new NotSupportedException("NamedStream cannot set the file length when reading.");

			long pos = Position;
			Seek(value);
			IOUtil.Win32.SetEndOfFile(handle);
			Seek(pos);
		}

		/// <summary>
		/// Write a section of buffer to the stream.
		/// </summary>
		/// <param name="buffer">the buffer that will be written</param>
		/// <param name="offset">the offset in buffer at which to start writing</param>
		/// <param name="count">the number of bytes to write</param>
		/// <exception cref="NotSupportedException">The stream was opened for reading.</exception>
		public override void Write(byte[] buffer, int offset, int count)
		{
			// handle error conditions better? ***
			if (!forWriting)  throw new NotSupportedException("NamedStream cannot write when reading.");

			int bytesWritten;
			if (offset == 0)
			{
				IOUtil.Win32.WriteFile(handle, buffer, count, out bytesWritten, IntPtr.Zero);
			}
			else
			{
				byte[] tmp = new byte[count];
				Buffer.BlockCopy(buffer, offset, tmp, 0, count);
				IOUtil.Win32.WriteFile(handle, tmp, count, out bytesWritten, IntPtr.Zero);
			}
		}
		/// <summary>
		/// Write a buffer to the stream.
		/// </summary>
		/// <param name="buffer">the buffer that will be written</param>
		/// <exception cref="NotSupportedException">The stream was opened for reading.</exception>
		public void Write(byte[] buffer)
		{
			Write(buffer, 0, buffer.Length);
		}

		private readonly byte[] writeByteBuffer = new byte[1];
		/// <summary>
		/// Write a single byte to the stream.
		/// </summary>
		/// <param name="value">the byte that will be written</param>
		/// <exception cref="NotSupportedException">The stream was opened for reading.</exception>
		public override void WriteByte(byte value)
		{
			writeByteBuffer[0] = value;
			Write(writeByteBuffer);
		}

		#endregion

#endif

	}


#if COPY_FRAMEWORK
	//// * Check if this is needed!

	///// <summary>
	///// 
	///// </summary>
	//public class NamedStream : Stream
	//{
	//    private long _appendStart;
	//    private byte[] _buffer;
	//    private int _bufferSize;
	//    private bool _canRead;
	//    private bool _canSeek;
	//    private static readonly bool _canUseAsync;
	//    private bool _canWrite;
	//    private string _fileName;
	//    private __HandleProtector _handleProtector;
	//    private bool _isAsync;
	//    private bool _isPipe;
	//    private long _pos;
	//    private int _readLen;
	//    private int _readPos;
	//    private int _writePos;
	//    internal const int DefaultBufferSize = 0x1000;
	//    private const int ERROR_BROKEN_PIPE = 0x6d;
	//    private const int ERROR_HANDLE_EOF = 0x26;
	//    private const int ERROR_INVALID_PARAMETER = 0x57;
	//    private const int ERROR_IO_PENDING = 0x3e5;
	//    private const int ERROR_NO_DATA = 0xe8;
	//    private const int FILE_ATTRIBUTE_NORMAL = 0x80;
	//    private const int FILE_BEGIN = 0;
	//    private const int FILE_CURRENT = 1;
	//    private const int FILE_END = 2;
	//    private const int FILE_FLAG_OVERLAPPED = 0x40000000;
	//    private const int GENERIC_READ = -2147483648;
	//    private const int GENERIC_WRITE = 0x40000000;
	//    private static readonly IOCompletionCallback IOCallback;


	//    internal abstract class __HandleProtector
	//    {
	//        protected internal __HandleProtector(IntPtr handle)
	//        {
	//            if (handle == ((IntPtr) (-1)))
	//            {
	//                throw new ArgumentException("__HandleProtector doesn't expect an invalid handle!");
	//            }
	//            this._inUse = 1;
	//            this._closed = 0;
	//            this._handle = handle.ToInt32();
	//        }

 
	//        internal void Close()
	//        {
	//            int num1 = this._closed;
	//            if ((num1 != 1) && (num1 == Interlocked.CompareExchange(ref this._closed, 1, num1)))
	//            {
	//                this.Release();
	//            }
	//        }

 
	//        internal void ForciblyMarkAsClosed()
	//        {
	//            this._closed = 1;
	//            this._handle = -1;
	//        }

 
	//        protected internal abstract void FreeHandle(IntPtr handle);

	//        internal void Release()
	//        {
	//            if (Interlocked.Decrement(ref this._inUse) == 0)
	//            {
	//                int num1 = this._handle;
	//                if ((num1 != -1) && (num1 == Interlocked.CompareExchange(ref this._handle, -1, num1)))
	//                {
	//                    this.FreeHandle(new IntPtr(num1));
	//                }
	//            }
	//        }

 
	//        internal bool TryAddRef(ref bool incremented)
	//        {
	//            if (this._closed == 0)
	//            {
	//                Interlocked.Increment(ref this._inUse);
	//                incremented = true;
	//                if (this._closed == 0)
	//                {
	//                    return true;
	//                }
	//                this.Release();
	//                incremented = false;
	//            }
	//            return false;
	//        }

 

	//        internal IntPtr Handle
	//        {
	//            get
	//            {
	//                return (IntPtr) this._handle;
	//            }
	//        }
 
	//        internal bool IsClosed
	//        {
	//            get
	//            {
	//                return (this._closed != 0);
	//            }
	//        }
 

	//        private int _closed;
	//        private int _handle;
	//        private int _inUse;
	//        private const int InvalidHandle = -1;
	//    }
 


	//    private sealed class __NamedStreamHandleProtector : __HandleProtector
	//    {
	//        internal __NamedStreamHandleProtector(IntPtr handle, bool ownsHandle) : base(handle)
	//        {
	//            this._ownsHandle = ownsHandle;
	//        }

	//        protected internal override void FreeHandle(IntPtr handle)
	//        {
	//            if (this._ownsHandle)
	//            {
	//                Win32Native.CloseHandle(handle);
	//            }
	//        }


	//        private bool _ownsHandle;
	//    }

	//    internal class AsyncNamedStream_AsyncResult : IAsyncResult
	//    {
	//        public AsyncNamedStream_AsyncResult()
	//        {
	//        }

	//        internal void CallUserCallback()
	//        {
	//            if (this._userCallback != null)
	//            {
	//                this._completedSynchronously = false;
	//                this._userCallback.BeginInvoke(this, null, null);
	//            }
	//        }
 
	//        internal static NamedStream.AsyncNamedStream_AsyncResult CreateBufferedReadResult(int numBufferedBytes, AsyncCallback userCallback, object userStateObject)
	//        {
	//            NamedStream.AsyncNamedStream_AsyncResult result1 = new NamedStream.AsyncNamedStream_AsyncResult();
	//            result1._userCallback = userCallback;
	//            result1._userStateObject = userStateObject;
	//            result1._isComplete = true;
	//            result1._isWrite = false;
	//            result1._numBufferedBytes = numBufferedBytes;
	//            return result1;
	//        }

 
	//        internal void PinBuffer(byte[] buffer)
	//        {
	//            this._bufferHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
	//            this._bufferIsPinned = true;
	//        }

 
	//        internal void UnpinBuffer()
	//        {
	//            if (this._bufferIsPinned)
	//            {
	//                this._bufferHandle.Free();
	//                this._bufferIsPinned = false;
	//            }
	//        }


	//        public virtual object AsyncState
	//        {
	//            get
	//            {
	//                return this._userStateObject;
	//            }
	//        }
	//        public WaitHandle AsyncWaitHandle
	//        {
	//            get
	//            {
	//                return this._waitHandle;
	//            }
	//        }
 
	//        public bool CompletedSynchronously
	//        {
	//            get
	//            {
	//                return this._completedSynchronously;
	//            }
	//        }
 
	//        public bool IsCompleted
	//        {
	//            get
	//            {
	//                return this._isComplete;
	//            }
	//            set
	//            {
	//                this._isComplete = value;
	//            }
	//        }
 

	//        internal GCHandle _bufferHandle;
	//        internal bool _bufferIsPinned;
	//        internal bool _completedSynchronously;
	//        internal int _EndXxxCalled;
	//        internal int _errorCode;
	//        internal bool _isComplete;
	//        internal bool _isWrite;
	//        internal int _numBufferedBytes;
	//        internal int _numBytes;
	//        internal unsafe NativeOverlapped* _overlapped;
	//        internal AsyncCallback _userCallback;
	//        internal object _userStateObject;
	//        internal WaitHandle _waitHandle;
	//    }


	//    static NamedStream()
	//    {
	//        NamedStream._canUseAsync = false;  //NamedStream.RunningOnWinNTNative();
	//        NamedStream.IOCallback = null;  //new IOCompletionCallback(NamedStream.AsyncFSCallback);
	//    }


	//    /// <summary>
	//    /// <para>Gets or sets the current
	//    /// position of this stream.</para>
	//    /// </summary>
	//    /// <exception cref="T:System.NotSupportedException">The stream does not support seeking.</exception>
	//    /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
	//    /// <exception cref="T:System.ArgumentOutOfRangeException">Attempted to set the position to a negative value.</exception>
	//    /// <exception cref="T:System.IO.EndOfStreamException">Attempted seeking past the end of a stream that does not support this.</exception>
	//    public override long Position
	//    {
	//        get
	//        {
	//            if (this._handleProtector.IsClosed)
	//            {
	//                __Error.FileNotOpen();
	//            }
	//            if (!this.CanSeek)
	//            {
	//                __Error.SeekNotSupported();
	//            }
	//            this.VerifyOSHandlePosition();
	//            return (this._pos + ((this._readPos - this._readLen) + this._writePos));
	//        }
	//        set
	//        {
	//            if (value < 0)
	//            {
	//                throw new ArgumentOutOfRangeException("value", "ArgumentOutOfRange_NeedNonNegNum");
	//            }
	//            if (this._writePos > 0)
	//            {
	//                this.FlushWrite();
	//            }
	//            this._readPos = 0;
	//            this._readLen = 0;
	//            this.Seek(value, SeekOrigin.Begin);
	//        }
	//    }
 
	//    internal string NameInternal
	//    {
	//        get
	//        {
	//            if (this._fileName == null)
	//            {
	//                return "<UnknownFileName>";
	//            }
	//            return this._fileName;
	//        }
	//    }
 
	//    /// <summary>
	//    /// <para>Gets the name of the <see langword="NamedStream" /> that was passed to the
	//    /// constructor.</para>
	//    /// </summary>
	//    public string Name
	//    {
	//        get
	//        {
	//            if (this._fileName == null)
	//            {
	//                return "IO_UnknownFileName";
	//            }
	//            new FileIOPermission(FileIOPermissionAccess.PathDiscovery, new string[] { this._fileName }, false, false).Demand();
	//            return this._fileName;
	//        }
	//    }
 
	//    /// <summary>
	//    /// <para>Gets the length in bytes of the stream.</para>
	//    /// </summary>
	//    /// <exception cref="T:System.NotSupportedException">
	//    /// <see cref="P:System.IO.NamedStream.CanSeek" /> for this stream is <see langword="false" />.</exception>
	//    /// <exception cref="T:System.IO.IOException">An I/O error occurs, such as the file being closed.</exception>
	//    public override long Length
	//    {
	//        get
	//        {
	//            if (this._handleProtector.IsClosed)
	//            {
	//                __Error.FileNotOpen();
	//            }
	//            if (!this.CanSeek)
	//            {
	//                __Error.SeekNotSupported();
	//            }
	//            int num1 = 0;
	//            int num2 = 0;
	//            bool flag1 = false;
	//            try
	//            {
	//                if (this._handleProtector.TryAddRef(ref flag1))
	//                {
	//                    num2 = Win32Native.GetFileSize(this._handleProtector.Handle, out num1);
	//                }
	//                else
	//                {
	//                    __Error.FileNotOpen();
	//                }
	//            }
	//            finally
	//            {
	//                if (flag1)
	//                {
	//                    this._handleProtector.Release();
	//                }
	//            }
	//            if (num2 == -1)
	//            {
	//                int num3 = Marshal.GetLastWin32Error();
	//                if (num3 != 0)
	//                {
	//                    __Error.WinIOError(num3, string.Empty);
	//                }
	//            }
	//            long num4 = (num1 << 0x20) | ((long) ((ulong) num2));
	//            if ((this._writePos > 0) && ((this._pos + this._writePos) > num4))
	//            {
	//                num4 = this._writePos + this._pos;
	//            }
	//            return num4;
	//        }
	//    }
 
	//    /// <summary>
	//    /// <para>Gets a value indicating whether the
	//    /// <see langword="NamedStream" /> was opened asynchronously or
	//    /// synchronously.</para>
	//    /// </summary>
	//    public virtual bool IsAsync
	//    {
	//        get
	//        {
	//            return this._isAsync;
	//        }
	//    }
 
	//    /// <summary>
	//    /// <para>Gets the operating system file handle for the file that
	//    /// the current <see langword="NamedStream" />
	//    /// object encapsulates.</para>
	//    /// </summary>
	//    /// <exception cref="T:System.Security.SecurityException">The caller does not have the required permission.</exception>
	//    public virtual IntPtr Handle
	//    {
	//        //[SecurityPermission(SecurityAction.InheritanceDemand), SecurityPermission(SecurityAction.LinkDemand)]
	//        get
	//        {
	//            this.Flush();
	//            this._readPos = 0;
	//            this._readLen = 0;
	//            this._writePos = 0;
	//            return this._handleProtector.Handle;
	//        }
	//    }
 
	//    /// <summary>
	//    /// <para>Gets a value indicating whether the current stream supports writing.</para>
	//    /// </summary>
	//    public override bool CanWrite
	//    {
	//        get
	//        {
	//            return this._canWrite;
	//        }
	//    }
 
	//    /// <summary>
	//    /// <para>Gets a value indicating whether the current stream supports seeking.</para>
	//    /// </summary>
	//    public override bool CanSeek
	//    {
	//        get
	//        {
	//            return this._canSeek;
	//        }
	//    }
 
	//    /// <summary>
	//    /// <para>Gets a value indicating whether the current stream supports reading.</para>
	//    /// </summary>
	//    public override bool CanRead
	//    {
	//        get
	//        {
	//            return this._canRead;
	//        }
	//    }
 
	//    internal unsafe int WriteFileNative(__HandleProtector hp, byte[] bytes, int offset, int count, NativeOverlapped* overlapped, out int hr)
	//    {
	//        if ((bytes.Length - offset) < count)
	//        {
	//            throw new IndexOutOfRangeException("IndexOutOfRange_IORaceCondition");
	//        }
	//        if (bytes.Length == 0)
	//        {
	//            hr = 0;
	//            return 0;
	//        }
	//        int num1 = 0;
	//        int num2 = 0;
	//        bool flag1 = false;
	//        try
	//        {
	//            if (hp.TryAddRef(ref flag1))
	//            {
	//                fixed (byte* numRef1 = bytes)
	//                {
	//                    if (this._isAsync)
	//                    {
	//                        num2 = NamedStream.WriteFile(hp.Handle, numRef1 + offset, count, IntPtr.Zero, overlapped);
	//                        goto Label_008B;
	//                    }
	//                    num2 = NamedStream.WriteFile(hp.Handle, numRef1 + offset, count, out num1, overlapped);
	//                    goto Label_008B;
	//                }
	//            }
	//            hr = 6;
	//        }
	//        finally
	//        {
	//            if (flag1)
	//            {
	//                hp.Release();
	//            }
	//        }
	//        Label_008B:
	//            if (num2 == 0)
	//            {
	//                hr = Marshal.GetLastWin32Error();
	//                if ((hr != 0xe8) && (hr == 6))
	//                {
	//                    this._handleProtector.ForciblyMarkAsClosed();
	//                }
	//                return -1;
	//            }
	//        hr = 0;
	//        return num1;
	//    }

 
	//    private void WriteCore(byte[] buffer, int offset, int count)
	//    {
	//        if (this._isAsync)
	//        {
	//            IAsyncResult result1 = this.BeginWriteCore(buffer, offset, count, null, null);
	//            this.EndWrite(result1);
	//            return;
	//        }
	//        int num1 = 0;
	//        int num2 = this.WriteFileNative(this._handleProtector, buffer, offset, count, null, out num1);
	//        if (num2 == -1)
	//        {
	//            switch (num1)
	//            {
	//                case 0xe8:
	//                    num2 = 0;
	//                    goto Label_0062;

	//                case 0x57:
	//                    throw new IOException("IO.IO_FileTooLongOrHandleNotSync");
	//            }
	//            __Error.WinIOError(num1, string.Empty);
	//        }
	//        Label_0062:
	//            this._pos += num2;
	//    }

 
	//    /// <summary>
	//    /// <para>Writes a byte to the current position in the file stream.</para>
	//    /// </summary>
	//    /// <param name="value">A byte to write to the stream.</param>
	//    /// <exception cref="T:System.ObjectDisposedException">The stream is closed.</exception>
	//    /// <exception cref="T:System.NotSupportedException">The stream does not support writing.</exception>
	//    public override void WriteByte(byte value)
	//    {
	//        if (this._handleProtector.IsClosed)
	//        {
	//            __Error.FileNotOpen();
	//        }
	//        if (this._writePos == 0)
	//        {
	//            if (!this.CanWrite)
	//            {
	//                __Error.WriteNotSupported();
	//            }
	//            if (this._readPos < this._readLen)
	//            {
	//                this.FlushRead();
	//            }
	//            this._readPos = 0;
	//            this._readLen = 0;
	//            if (this._buffer == null)
	//            {
	//                this._buffer = new byte[this._bufferSize];
	//            }
	//        }
	//        if (this._writePos == this._bufferSize)
	//        {
	//            this.FlushWrite();
	//        }
	//        this._buffer[this._writePos++] = value;
	//    }

 
	//    /// <summary>
	//    /// <para>Writes a block of bytes to this stream using data
	//    /// from a buffer.</para>
	//    /// </summary>
	//    /// <param name="array">The array to which bytes are written.</param>
	//    /// <param name="offset">The byte offset in <paramref name="array" /> at which to begin writing.</param>
	//    /// <param name="count">The maximum number of bytes to write.</param>
	//    /// <exception cref="T:System.ArgumentNullException">
	//    /// <paramref name="array" /> is <see langword="null" />.</exception>
	//    /// <exception cref="T:System.ArgumentException">
	//    /// <paramref name="offset" /> and <paramref name="count" /> describe an invalid range in <paramref name="array" />.</exception>
	//    /// <exception cref="T:System.ArgumentOutOfRangeException">
	//    /// <paramref name="offset" /> or <paramref name="count" /> is negative.</exception>
	//    /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
	//    /// <exception cref="T:System.ObjectDisposedException">The stream is closed.</exception>
	//    /// <exception cref="T:System.NotSupportedException">The current stream instance does not support writing.</exception>
	//    public override void Write(byte[] array, int offset, int count)
	//    {
	//        if (array == null)
	//        {
	//            throw new ArgumentNullException("array", "ArgumentNull_Buffer");
	//        }
	//        if (offset < 0)
	//        {
	//            throw new ArgumentOutOfRangeException("offset", "ArgumentOutOfRange_NeedNonNegNum");
	//        }
	//        if (count < 0)
	//        {
	//            throw new ArgumentOutOfRangeException("count", "ArgumentOutOfRange_NeedNonNegNum");
	//        }
	//        if ((array.Length - offset) < count)
	//        {
	//            throw new ArgumentException("Argument_InvalidOffLen");
	//        }
	//        if (this._handleProtector.IsClosed)
	//        {
	//            __Error.FileNotOpen();
	//        }
	//        if (this._writePos == 0)
	//        {
	//            if (!this.CanWrite)
	//            {
	//                __Error.WriteNotSupported();
	//            }
	//            if (this._readPos < this._readLen)
	//            {
	//                this.FlushRead();
	//            }
	//            this._readPos = 0;
	//            this._readLen = 0;
	//        }
	//        if (this._writePos > 0)
	//        {
	//            int num1 = this._bufferSize - this._writePos;
	//            if (num1 > 0)
	//            {
	//                if (num1 > count)
	//                {
	//                    num1 = count;
	//                }
	//                Buffer.InternalBlockCopy(array, offset, this._buffer, this._writePos, num1);
	//                this._writePos += num1;
	//                if (count == num1)
	//                {
	//                    return;
	//                }
	//                offset += num1;
	//                count -= num1;
	//            }
	//            if (this._isAsync)
	//            {
	//                IAsyncResult result1 = this.BeginWriteCore(this._buffer, 0, this._writePos, null, null);
	//                this.EndWrite(result1);
	//            }
	//            else
	//            {
	//                this.WriteCore(this._buffer, 0, this._writePos);
	//            }
	//            this._writePos = 0;
	//        }
	//        if (count >= this._bufferSize)
	//        {
	//            this.WriteCore(array, offset, count);
	//        }
	//        else if (count != 0)
	//        {
	//            if (this._buffer == null)
	//            {
	//                this._buffer = new byte[this._bufferSize];
	//            }
	//            Buffer.InternalBlockCopy(array, offset, this._buffer, this._writePos, count);
	//            this._writePos = count;
	//        }
	//    }

 
	//    private void VerifyOSHandlePosition()
	//    {
	//        if (this.CanSeek)
	//        {
	//            long num1 = this._pos;
	//            long num2 = this.SeekCore((long) 0, SeekOrigin.Current);
	//            if (num2 != num1)
	//            {
	//                this._readPos = 0;
	//                this._readLen = 0;
	//            }
	//        }
	//    }

 
	//    private void VerifyHandleIsSync()
	//    {
	//        byte[] buffer1 = new byte[1];
	//        int num1 = 0;
	//        if (this.CanRead)
	//        {
	//            this.ReadFileNative(this._handleProtector, buffer1, 0, 0, null, out num1);
	//        }
	//        else if (this.CanWrite)
	//        {
	//            this.WriteFileNative(this._handleProtector, buffer1, 0, 0, null, out num1);
	//        }
	//        if (num1 == 0x57)
	//        {
	//            throw new ArgumentException("Arg_HandleNotSync");
	//        }
	//        if (num1 == 6)
	//        {
	//            __Error.WinIOError(num1, "<OS handle>");
	//        }
	//    }

 
	//    /// <summary>
	//    /// <para>Allows access by other processes to all or part of a file that was previously
	//    /// locked.</para>
	//    /// </summary>
	//    /// <param name="position">The beginning of the range to unlock.</param>
	//    /// <param name="length">The range to be unlocked.</param>
	//    /// <exception cref="T:System.ArgumentOutOfRangeException">
	//    /// <paramref name="position" /> or <paramref name="length" /> is negative.</exception>
	//    public virtual void Unlock(long position, long length)
	//    {
	//        if ((position < 0) || (length < 0))
	//        {
	//            throw new ArgumentOutOfRangeException((position < 0) ? "position" : "length", "ArgumentOutOfRange_NeedNonNegNum");
	//        }
	//        bool flag1 = false;
	//        try
	//        {
	//            if (this._handleProtector.TryAddRef(ref flag1))
	//            {
	//                if (!Win32Native.UnlockFile(this._handleProtector.Handle, position, length))
	//                {
	//                    __Error.WinIOError();
	//                }
	//            }
	//            else
	//            {
	//                __Error.FileNotOpen();
	//            }
	//        }
	//        finally
	//        {
	//            if (flag1)
	//            {
	//                this._handleProtector.Release();
	//            }
	//        }
	//    }

 
	//    private void SetLengthCore(long value)
	//    {
	//        long num1 = this._pos;
	//        bool flag1 = false;
	//        try
	//        {
	//            if (this._handleProtector.TryAddRef(ref flag1))
	//            {
	//                if (this._pos != value)
	//                {
	//                    this.SeekCore(value, SeekOrigin.Begin);
	//                }
	//                if (!Win32Native.SetEndOfFile(this._handleProtector.Handle))
	//                {
	//                    int num2 = Marshal.GetLastWin32Error();
	//                    if (num2 == 0x57)
	//                    {
	//                        throw new ArgumentOutOfRangeException("value", "ArgumentOutOfRange_FileLengthTooBig");
	//                    }
	//                    __Error.WinIOError(num2, string.Empty);
	//                }
	//                if (num1 != value)
	//                {
	//                    if (num1 < value)
	//                    {
	//                        this.SeekCore(num1, SeekOrigin.Begin);
	//                    }
	//                    else
	//                    {
	//                        this.SeekCore((long) 0, SeekOrigin.End);
	//                    }
	//                }
	//                this.VerifyOSHandlePosition();
	//            }
	//            else
	//            {
	//                __Error.FileNotOpen();
	//            }
	//        }
	//        finally
	//        {
	//            if (flag1)
	//            {
	//                this._handleProtector.Release();
	//            }
	//        }
	//    }

 
	//    /// <summary>
	//    /// <para>Sets the length
	//    /// of this stream to the given value.</para>
	//    /// </summary>
	//    /// <param name="value">The new length of the stream.</param>
	//    /// <exception cref="T:System.IO.IOException">An I/O error has occurred.</exception>
	//    /// <exception cref="T:System.NotSupportedException">The stream does not support both writing and seeking.</exception>
	//    /// <exception cref="T:System.ArgumentOutOfRangeException">Attempted to set the <paramref name="value" /> parameter to less than 0.</exception>
	//    public override void SetLength(long value)
	//    {
	//        if (value < 0)
	//        {
	//            throw new ArgumentOutOfRangeException("value", "ArgumentOutOfRange_NeedNonNegNum");
	//        }
	//        if (this._handleProtector.IsClosed)
	//        {
	//            __Error.FileNotOpen();
	//        }
	//        if (!this.CanSeek)
	//        {
	//            __Error.SeekNotSupported();
	//        }
	//        if (!this.CanWrite)
	//        {
	//            __Error.WriteNotSupported();
	//        }
	//        if (this._writePos > 0)
	//        {
	//            this.FlushWrite();
	//        }
	//        else if (this._readPos < this._readLen)
	//        {
	//            this.FlushRead();
	//        }
	//        if ((this._appendStart != -1) && (value < this._appendStart))
	//        {
	//            throw new IOException("IO.IO_SetLengthAppendTruncate");
	//        }
	//        this.SetLengthCore(value);
	//    }

 
	//    private long SeekCore(long offset, SeekOrigin origin)
	//    {
	//        int num1 = 0;
	//        long num2 = 0;
	//        bool flag1 = false;
	//        try
	//        {
	//            if (this._handleProtector.TryAddRef(ref flag1))
	//            {
	//                num2 = Win32Native.SetFilePointer(this._handleProtector.Handle, offset, origin, out num1);
	//            }
	//            else
	//            {
	//                __Error.FileNotOpen();
	//            }
	//        }
	//        finally
	//        {
	//            if (flag1)
	//            {
	//                this._handleProtector.Release();
	//            }
	//        }
	//        if (num2 == -1)
	//        {
	//            __Error.WinIOError(num1, string.Empty);
	//        }
	//        this._pos = num2;
	//        return num2;
	//    }

 
	//    /// <summary>
	//    /// <para>Sets the current position of this stream to the given value.</para>
	//    /// </summary>
	//    /// <param name="offset">The point relative to <paramref name="origin" /> from which to begin seeking.</param>
	//    /// <param name="origin">Specifies the beginning, the end, or the current position as a reference point for <paramref name="origin" /> , using a value of type <see cref="T:System.IO.SeekOrigin" /> .</param>
	//    /// <returns>
	//    /// <para>The new position in the stream.</para>
	//    /// </returns>
	//    /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
	//    /// <exception cref="T:System.NotSupportedException">The stream does not support seeking, such as if the <see langword="NamedStream" /> is constructed from a pipe or console output.</exception>
	//    /// <exception cref="T:System.ArgumentException">Attempted seeking before the beginning of the stream.</exception>
	//    /// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed.</exception>
	//    public override long Seek(long offset, SeekOrigin origin)
	//    {
	//        if ((origin < SeekOrigin.Begin) || (origin > SeekOrigin.End))
	//        {
	//            throw new ArgumentException("Argument_InvalidSeekOrigin");
	//        }
	//        if (this._handleProtector.IsClosed)
	//        {
	//            __Error.FileNotOpen();
	//        }
	//        if (!this.CanSeek)
	//        {
	//            __Error.SeekNotSupported();
	//        }
	//        if (this._writePos > 0)
	//        {
	//            this.FlushWrite();
	//        }
	//        else if (origin == SeekOrigin.Current)
	//        {
	//            offset -= this._readLen - this._readPos;
	//        }
	//        this.VerifyOSHandlePosition();
	//        long num1 = this._pos + (this._readPos - this._readLen);
	//        long num2 = this.SeekCore(offset, origin);
	//        if ((this._appendStart != -1) && (num2 < this._appendStart))
	//        {
	//            this.SeekCore(num1, SeekOrigin.Begin);
	//            throw new IOException("IO.IO_SeekAppendOverwrite");
	//        }
	//        if (this._readLen > 0)
	//        {
	//            if (num1 == num2)
	//            {
	//                if (this._readPos > 0)
	//                {
	//                    Buffer.BlockCopy(this._buffer, this._readPos, this._buffer, 0, this._readLen - this._readPos);
	//                    this._readLen -= this._readPos;
	//                    this._readPos = 0;
	//                }
	//                if (this._readLen > 0)
	//                {
	//                    this.SeekCore((long) this._readLen, SeekOrigin.Current);
	//                }
	//                return num2;
	//            }
	//            if (((num1 - this._readPos) < num2) && (num2 < ((num1 + this._readLen) - this._readPos)))
	//            {
	//                int num3 = (int) (num2 - num1);
	//                Buffer.BlockCopy(this._buffer, this._readPos + num3, this._buffer, 0, this._readLen - (this._readPos + num3));
	//                this._readLen -= this._readPos + num3;
	//                this._readPos = 0;
	//                if (this._readLen > 0)
	//                {
	//                    this.SeekCore((long) this._readLen, SeekOrigin.Current);
	//                }
	//                return num2;
	//            }
	//            this._readPos = 0;
	//            this._readLen = 0;
	//        }
	//        return num2;
	//    }

 
	//    internal unsafe int ReadFileNative(__HandleProtector hp, byte[] bytes, int offset, int count, NativeOverlapped* overlapped, out int hr)
	//    {
	//        if ((bytes.Length - offset) < count)
	//        {
	//            throw new IndexOutOfRangeException("IndexOutOfRange_IORaceCondition");
	//        }
	//        if (bytes.Length == 0)
	//        {
	//            hr = 0;
	//            return 0;
	//        }
	//        int num1 = 0;
	//        int num2 = 0;
	//        bool flag1 = false;
	//        try
	//        {
	//            if (hp.TryAddRef(ref flag1))
	//            {
	//                fixed (byte* numRef1 = bytes)
	//                {
	//                    if (this._isAsync)
	//                    {
	//                        num1 = NamedStream.ReadFile(hp.Handle, numRef1 + offset, count, IntPtr.Zero, overlapped);
	//                        goto Label_008B;
	//                    }
	//                    num1 = NamedStream.ReadFile(hp.Handle, numRef1 + offset, count, out num2, overlapped);
	//                    goto Label_008B;
	//                }
	//            }
	//            hr = 6;
	//        }
	//        finally
	//        {
	//            if (flag1)
	//            {
	//                hp.Release();
	//            }
	//        }
	//        Label_008B:
	//            if (num1 == 0)
	//            {
	//                hr = Marshal.GetLastWin32Error();
	//                if ((hr != 0x6d) && (hr == 6))
	//                {
	//                    this._handleProtector.ForciblyMarkAsClosed();
	//                }
	//                return -1;
	//            }
	//        hr = 0;
	//        return num2;
	//    }

 
	//    private int ReadCore(byte[] buffer, int offset, int count)
	//    {
	//        if (this._isAsync)
	//        {
	//            IAsyncResult result1 = this.BeginReadCore(buffer, offset, count, null, null, 0);
	//            return this.EndRead(result1);
	//        }
	//        int num1 = 0;
	//        int num2 = this.ReadFileNative(this._handleProtector, buffer, offset, count, null, out num1);
	//        if (num2 == -1)
	//        {
	//            switch (num1)
	//            {
	//                case 0x6d:
	//                    num2 = 0;
	//                    goto Label_0060;

	//                case 0x57:
	//                    throw new ArgumentException("Arg_HandleNotSync");
	//            }
	//            __Error.WinIOError(num1, string.Empty);
	//        }
	//        Label_0060:
	//            this._pos += num2;
	//        this.VerifyOSHandlePosition();
	//        return num2;
	//    }

 
	//    /// <summary>
	//    /// <para> Reads a byte from the file
	//    /// and advances the read position one byte.</para>
	//    /// </summary>
	//    /// <returns>
	//    /// <para>The byte cast to an <see langword="int" />, or -1 if reading from
	//    /// the end of the stream.</para>
	//    /// </returns>
	//    /// <exception cref="T:System.NotSupportedException">The current stream does not support reading.</exception>
	//    /// <exception cref="T:System.ObjectDisposedException">The current stream is closed.</exception>
	//    public override int ReadByte()
	//    {
	//        if (this._handleProtector.IsClosed)
	//        {
	//            __Error.FileNotOpen();
	//        }
	//        if ((this._readLen == 0) && !this.CanRead)
	//        {
	//            __Error.ReadNotSupported();
	//        }
	//        if (this._readPos == this._readLen)
	//        {
	//            if (this._writePos > 0)
	//            {
	//                this.FlushWrite();
	//            }
	//            if (this._buffer == null)
	//            {
	//                this._buffer = new byte[this._bufferSize];
	//            }
	//            this._readLen = this.ReadCore(this._buffer, 0, this._bufferSize);
	//            this._readPos = 0;
	//        }
	//        if (this._readPos == this._readLen)
	//        {
	//            return -1;
	//        }
	//        return this._buffer[this._readPos++];
	//    }

 
	//    /// <summary>
	//    /// <para>Reads a block of bytes from the stream and writes the data in a
	//    /// given buffer.</para>
	//    /// </summary>
	//    /// <param name="array">When this method returns, contains the specified byte array with the values between <paramref name="offset " />and (<paramref name="offset + count - 1) " />replaced by the bytes read from the current source.</param>
	//    /// <param name="offset">The byte offset in <paramref name="array" /> at which to begin reading.</param>
	//    /// <param name="count">The maximum number of bytes to read.</param>
	//    /// <returns>
	//    /// <para>The total number of bytes read into the buffer.
	//    /// This might be less than the number of bytes requested if that number of bytes
	//    /// are not currently available, or zero if the end of the stream is reached.</para>
	//    /// </returns>
	//    /// <exception cref="T:System.ArgumentNullException">
	//    /// <paramref name="array" /> is <see langword="null" /> .</exception>
	//    /// <exception cref="T:System.ArgumentOutOfRangeException">
	//    /// <paramref name="offset" /> or <paramref name="count " /> is negative.</exception>
	//    /// <exception cref="T:System.NotSupportedException">The stream does not support reading.</exception>
	//    /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
	//    /// <exception cref="T:System.ArgumentException">
	//    /// <paramref name="offset" /> and <paramref name="count" /> describe an invalid range in <paramref name="array" />.</exception>
	//    /// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed.</exception>
	//    public override int Read([In, Out] byte[] array, int offset, int count)
	//    {
	//        if (array == null)
	//        {
	//            throw new ArgumentNullException("array", "ArgumentNull_Buffer");
	//        }
	//        if (offset < 0)
	//        {
	//            throw new ArgumentOutOfRangeException("offset", "ArgumentOutOfRange_NeedNonNegNum");
	//        }
	//        if (count < 0)
	//        {
	//            throw new ArgumentOutOfRangeException("count", "ArgumentOutOfRange_NeedNonNegNum");
	//        }
	//        if ((array.Length - offset) < count)
	//        {
	//            throw new ArgumentException("Argument_InvalidOffLen");
	//        }
	//        if (this._handleProtector.IsClosed)
	//        {
	//            __Error.FileNotOpen();
	//        }
	//        bool flag1 = false;
	//        int num1 = this._readLen - this._readPos;
	//        if (num1 == 0)
	//        {
	//            if (!this.CanRead)
	//            {
	//                __Error.ReadNotSupported();
	//            }
	//            if (this._writePos > 0)
	//            {
	//                this.FlushWrite();
	//            }
	//            if (count >= this._bufferSize)
	//            {
	//                num1 = this.ReadCore(array, offset, count);
	//                this._readPos = 0;
	//                this._readLen = 0;
	//                return num1;
	//            }
	//            if (this._buffer == null)
	//            {
	//                this._buffer = new byte[this._bufferSize];
	//            }
	//            num1 = this.ReadCore(this._buffer, 0, this._bufferSize);
	//            if (num1 == 0)
	//            {
	//                return 0;
	//            }
	//            flag1 = num1 < this._bufferSize;
	//            this._readPos = 0;
	//            this._readLen = num1;
	//        }
	//        if (num1 > count)
	//        {
	//            num1 = count;
	//        }
	//        Buffer.InternalBlockCopy(this._buffer, this._readPos, array, offset, num1);
	//        this._readPos += num1;
	//        if ((num1 < count) && !flag1)
	//        {
	//            int num2 = this.ReadCore(array, offset + num1, count - num1);
	//            num1 += num2;
	//            this._readPos = 0;
	//            this._readLen = 0;
	//        }
	//        return num1;
	//    }

 
	//    /// <summary>
	//    /// <para>Prevents other processes from changing the <see cref="T:System.IO.NamedStream" /> while permitting read
	//    /// access.</para>
	//    /// </summary>
	//    /// <param name="position">The beginning of the range to lock. The value of this parameter must be equal to or greater than zero (0).</param>
	//    /// <param name="length">The range to be locked.</param>
	//    /// <exception cref="T:System.ArgumentOutOfRangeException">
	//    /// <paramref name="position" /> or <paramref name="length" /> is negative.</exception>
	//    /// <exception cref="T:System.ObjectDisposedException">The file is closed.</exception>
	//    public virtual void Lock(long position, long length)
	//    {
	//        if ((position < 0) || (length < 0))
	//        {
	//            throw new ArgumentOutOfRangeException((position < 0) ? "position" : "length", "ArgumentOutOfRange_NeedNonNegNum");
	//        }
	//        bool flag1 = false;
	//        try
	//        {
	//            if (this._handleProtector.TryAddRef(ref flag1))
	//            {
	//                if (!Win32Native.LockFile(this._handleProtector.Handle, position, length))
	//                {
	//                    __Error.WinIOError();
	//                }
	//            }
	//            else
	//            {
	//                __Error.FileNotOpen();
	//            }
	//        }
	//        finally
	//        {
	//            if (flag1)
	//            {
	//                this._handleProtector.Release();
	//            }
	//        }
	//    }

 
	//    private void FlushWrite()
	//    {
	//        if (this._isAsync)
	//        {
	//            IAsyncResult result1 = this.BeginWriteCore(this._buffer, 0, this._writePos, null, null);
	//            this.EndWrite(result1);
	//        }
	//        else
	//        {
	//            this.WriteCore(this._buffer, 0, this._writePos);
	//        }
	//        this._writePos = 0;
	//    }

 
	//    private void FlushRead()
	//    {
	//        if ((this._readPos - this._readLen) != 0)
	//        {
	//            this.SeekCore((long) (this._readPos - this._readLen), SeekOrigin.Current);
	//        }
	//        this._readPos = 0;
	//        this._readLen = 0;
	//    }

 
	//    /// <summary>
	//    /// <para>Clears all buffers for this stream and causes any buffered data to be written
	//    /// to the underlying device.</para>
	//    /// </summary>
	//    /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
	//    /// <exception cref="T:System.ObjectDisposedException">The stream is closed.</exception>
	//    public override void Flush()
	//    {
	//        if (this._handleProtector.IsClosed)
	//        {
	//            __Error.FileNotOpen();
	//        }
	//        if (this._writePos > 0)
	//        {
	//            this.FlushWrite();
	//        }
	//        else if ((this._readPos < this._readLen) && this.CanSeek)
	//        {
	//            this.FlushRead();
	//        }
	//    }

 
	//    /// <summary>
	//    /// <para>Ensures that resources are freed and other cleanup operations are performed when the garbage collector
	//    /// reclaims the <see langword="NamedStream" /> .</para>
	//    /// </summary>
	//    ~NamedStream()
	//    {
	//        if (this._handleProtector != null)
	//        {
	//            this.Dispose(false);
	//        }
	//    }

 
	//    /// <summary>
	//    /// <para> Ends an asynchronous write, blocking until the I/O operation
	//    /// has completed.</para>
	//    /// </summary>
	//    /// <param name="asyncResult">The pending asynchronous I/O request.</param>
	//    /// <exception cref="T:System.ArgumentNullException">
	//    /// <paramref name="asyncResult" /> is <see langword="null" /> .</exception>
	//    /// <exception cref="T:System.ArgumentException">This <see cref="T:System.IAsyncResult" /> object was not created by calling <see cref="M:System.IO.Stream.BeginWrite(System.Byte[],System.Int32,System.Int32,System.AsyncCallback,System.Object)" /> on this class.</exception>
	//    /// <exception cref="T:System.InvalidOperationException">
	//    /// <see cref="M:System.IO.NamedStream.EndWrite(System.IAsyncResult)" /> is called multiple times.</exception>
	//    public override unsafe void EndWrite(IAsyncResult asyncResult)
	//    {
	//        if (asyncResult == null)
	//        {
	//            throw new ArgumentNullException("asyncResult");
	//        }
	//        if (!this._isAsync)
	//        {
	//            base.EndWrite(asyncResult);
	//        }
	//        else
	//        {
	//            NamedStream.AsyncNamedStream_AsyncResult result1 = asyncResult as NamedStream.AsyncNamedStream_AsyncResult;
	//            if ((result1 == null) || !result1._isWrite)
	//            {
	//                __Error.WrongAsyncResult();
	//            }
	//            if (1 == Interlocked.CompareExchange(ref result1._EndXxxCalled, 1, 0))
	//            {
	//                __Error.EndWriteCalledTwice();
	//            }
	//            WaitHandle handle1 = result1.AsyncWaitHandle;
	//            if (handle1 != null)
	//            {
	//                if (!result1.IsCompleted)
	//                {
	//                    handle1.WaitOne();
	//                    result1._isComplete = true;
	//                }
	//                handle1.Close();
	//            }
	//            NativeOverlapped* overlappedPtr1 = result1._overlapped;
	//            if (overlappedPtr1 != null)
	//            {
	//                Overlapped.Free(overlappedPtr1);
	//            }
	//            result1.UnpinBuffer();
	//            if (result1._errorCode != 0)
	//            {
	//                __Error.WinIOError(result1._errorCode, Path.GetFileName(this._fileName));
	//            }
	//        }
	//    }

 
	//    /// <summary>
	//    /// <para> Waits for the
	//    /// pending asynchronous read to complete.</para>
	//    /// </summary>
	//    /// <param name="asyncResult">The reference to the pending asynchronous request to wait for.</param>
	//    /// <returns>
	//    /// <para>The number of bytes read from the stream, between 0 and
	//    /// the number of bytes you requested. Streams only return 0 at the end of the
	//    /// stream, otherwise, they should block until at least 1 byte is available.</para>
	//    /// </returns>
	//    /// <exception cref="T:System.ArgumentNullException">
	//    /// <paramref name="asyncResult" /> is <see langword="null" /> .</exception>
	//    /// <exception cref="T:System.ArgumentException">This <see cref="T:System.IAsyncResult" /> object was not created by calling <see cref="M:System.IO.NamedStream.BeginRead(System.Byte[],System.Int32,System.Int32,System.AsyncCallback,System.Object)" /> on this class.</exception>
	//    /// <exception cref="T:System.InvalidOperationException">
	//    /// <see cref="M:System.IO.NamedStream.EndRead(System.IAsyncResult)" /> is called multiple times.</exception>
	//    public override unsafe int EndRead(IAsyncResult asyncResult)
	//    {
	//        if (asyncResult == null)
	//        {
	//            throw new ArgumentNullException("asyncResult");
	//        }
	//        if (!this._isAsync)
	//        {
	//            return base.EndRead(asyncResult);
	//        }
	//        NamedStream.AsyncNamedStream_AsyncResult result1 = asyncResult as NamedStream.AsyncNamedStream_AsyncResult;
	//        if ((result1 == null) || result1._isWrite)
	//        {
	//            __Error.WrongAsyncResult();
	//        }
	//        if (1 == Interlocked.CompareExchange(ref result1._EndXxxCalled, 1, 0))
	//        {
	//            __Error.EndReadCalledTwice();
	//        }
	//        WaitHandle handle1 = result1.AsyncWaitHandle;
	//        if (handle1 != null)
	//        {
	//            if (!result1.IsCompleted)
	//            {
	//                handle1.WaitOne();
	//                result1._isComplete = true;
	//            }
	//            handle1.Close();
	//        }
	//        NativeOverlapped* overlappedPtr1 = result1._overlapped;
	//        if (overlappedPtr1 != null)
	//        {
	//            Overlapped.Free(overlappedPtr1);
	//        }
	//        result1.UnpinBuffer();
	//        if (result1._errorCode != 0)
	//        {
	//            __Error.WinIOError(result1._errorCode, Path.GetFileName(this._fileName));
	//        }
	//        return (result1._numBytes + result1._numBufferedBytes);
	//    }

 
	//    /// <summary>
	//    /// <para>Releases the unmanaged resources used by the <see cref="T:System.IO.NamedStream" /> and optionally
	//    /// releases the managed resources.</para>
	//    /// </summary>
	//    /// <param name="disposing">
	//    /// <see langword="true" /> to release both managed and unmanaged resources; <see langword="false" /> to release only unmanaged resources.</param>
	//    protected virtual void Dispose(bool disposing)
	//    {
	//        if (this._handleProtector != null)
	//        {
	//            if (!this._handleProtector.IsClosed)
	//            {
	//                this.Flush();
	//            }
	//            this._handleProtector.Close();
	//        }
	//        this._canRead = false;
	//        this._canWrite = false;
	//        this._canSeek = false;
	//        this._buffer = null;
	//    }

 
	//    /// <summary>
	//    /// <para>Closes the file and releases any resources associated with
	//    /// the current file stream.</para>
	//    /// </summary>
	//    /// <exception cref="T:System.IO.IOException">An error occurred while trying to close the stream.</exception>
	//    public override void Close()
	//    {
	//        this.Dispose(true);
	//        GC.nativeSuppressFinalize(this);
	//    }

 
	//    private unsafe NamedStream.AsyncNamedStream_AsyncResult BeginWriteCore(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject)
	//    {
	//        NamedStream.AsyncNamedStream_AsyncResult result1 = new NamedStream.AsyncNamedStream_AsyncResult();
	//        result1._userCallback = userCallback;
	//        result1._userStateObject = stateObject;
	//        result1._isWrite = true;
	//        ManualResetEvent event1 = new ManualResetEvent(false);
	//        result1._waitHandle = event1;
	//        NativeOverlapped* overlappedPtr1 = new Overlapped(0, 0, 0, result1).Pack(NamedStream.IOCallback);
	//        result1._overlapped = overlappedPtr1;
	//        if (this.CanSeek)
	//        {
	//            long num1 = this.Length;
	//            this.VerifyOSHandlePosition();
	//            if ((this._pos + numBytes) > num1)
	//            {
	//                this.SetLengthCore(this._pos + numBytes);
	//            }
	//            overlappedPtr1->OffsetLow = (int) this._pos;
	//            overlappedPtr1->OffsetHigh = (int) (this._pos >> 0x20);
	//            this._pos += numBytes;
	//            this.SeekCore((long) numBytes, SeekOrigin.Current);
	//        }
	//        result1.PinBuffer(array);
	//        int num2 = 0;
	//        int num3 = this.WriteFileNative(this._handleProtector, array, offset, numBytes, overlappedPtr1, out num2);
	//        if ((num3 == -1) && (numBytes != -1))
	//        {
	//            switch (num2)
	//            {
	//                case 0xe8:
	//                    NamedStream.SetEvent(result1._waitHandle.Handle);
	//                    result1._isComplete = true;
	//                    result1.CallUserCallback();
	//                    return result1;

	//                case 0x3e5:
	//                    return result1;
	//            }
	//            if (!this._handleProtector.IsClosed && this.CanSeek)
	//            {
	//                this.SeekCore((long) 0, SeekOrigin.Current);
	//            }
	//            if (num2 == 0x26)
	//            {
	//                __Error.EndOfFile();
	//                return result1;
	//            }
	//            __Error.WinIOError(num2, string.Empty);
	//        }
	//        return result1;
	//    }

 
	//    /// <summary>
	//    /// <para>Begins an asynchronous write.</para>
	//    /// </summary>
	//    /// <param name="array">The buffer to write data to.</param>
	//    /// <param name="offset">The zero based byte offset in <paramref name="array" /> at which to begin writing.</param>
	//    /// <param name="numBytes">The maximum number of bytes to write.</param>
	//    /// <param name="userCallback">The method to be called when the asynchronous write operation is completed.</param>
	//    /// <param name="stateObject">A user-provided object that distinguishes this particular asynchronous write request from other requests.</param>
	//    /// <returns>
	//    /// <para>An <see cref="T:System.IAsyncResult" /> that references the asynchronous write.</para>
	//    /// </returns>
	//    /// <exception cref="T:System.ArgumentException">
	//    /// <paramref name="array" /> length minus <paramref name="offset" /> is less than <paramref name="numBytes" />.</exception>
	//    /// <exception cref="T:System.ArgumentNullException">
	//    /// <paramref name="array" /> is <see langword="null" />.</exception>
	//    /// <exception cref="T:System.ArgumentOutOfRangeException">
	//    /// <paramref name="offset" /> or <paramref name="numBytes" /> is negative.</exception>
	//    /// <exception cref="T:System.NotSupportedException">The stream does not support writing.</exception>
	//    /// <exception cref="T:System.ObjectDisposedException">The stream is closed.</exception>
	//    /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
	//    public override IAsyncResult BeginWrite(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject)
	//    {
	//        if (array == null)
	//        {
	//            throw new ArgumentNullException("array");
	//        }
	//        if (offset < 0)
	//        {
	//            throw new ArgumentOutOfRangeException("offset", "ArgumentOutOfRange_NeedNonNegNum");
	//        }
	//        if (numBytes < 0)
	//        {
	//            throw new ArgumentOutOfRangeException("numBytes", "ArgumentOutOfRange_NeedNonNegNum");
	//        }
	//        if ((array.Length - offset) < numBytes)
	//        {
	//            throw new ArgumentException("Argument_InvalidOffLen");
	//        }
	//        if (this._handleProtector.IsClosed)
	//        {
	//            __Error.FileNotOpen();
	//        }
	//        if (!this._isAsync)
	//        {
	//            return base.BeginWrite(array, offset, numBytes, userCallback, stateObject);
	//        }
	//        if (!this.CanWrite)
	//        {
	//            __Error.WriteNotSupported();
	//        }
	//        if (this._isPipe)
	//        {
	//            if (this._writePos > 0)
	//            {
	//                this.FlushWrite();
	//            }
	//            return this.BeginWriteCore(array, offset, numBytes, userCallback, stateObject);
	//        }
	//        if (this._writePos == 0)
	//        {
	//            if (this._readPos < this._readLen)
	//            {
	//                this.FlushRead();
	//            }
	//            this._readPos = 0;
	//            this._readLen = 0;
	//        }
	//        int num1 = this._bufferSize - this._writePos;
	//        if (numBytes <= num1)
	//        {
	//            if (this._writePos == 0)
	//            {
	//                this._buffer = new byte[this._bufferSize];
	//            }
	//            Buffer.InternalBlockCopy(array, offset, this._buffer, this._writePos, numBytes);
	//            this._writePos += numBytes;
	//            NamedStream.AsyncNamedStream_AsyncResult result1 = new NamedStream.AsyncNamedStream_AsyncResult();
	//            result1._userCallback = userCallback;
	//            result1._userStateObject = stateObject;
	//            result1._waitHandle = null;
	//            result1._isComplete = true;
	//            result1._isWrite = true;
	//            result1._numBufferedBytes = numBytes;
	//            result1.CallUserCallback();
	//            return result1;
	//        }
	//        if (this._writePos > 0)
	//        {
	//            this.FlushWrite();
	//        }
	//        return this.BeginWriteCore(array, offset, numBytes, userCallback, stateObject);
	//    }

 
	//    private unsafe NamedStream.AsyncNamedStream_AsyncResult BeginReadCore(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject, int numBufferedBytesRead)
	//    {
	//        NamedStream.AsyncNamedStream_AsyncResult result1 = new NamedStream.AsyncNamedStream_AsyncResult();
	//        result1._userCallback = userCallback;
	//        result1._userStateObject = stateObject;
	//        result1._isWrite = false;
	//        result1._numBufferedBytes = numBufferedBytesRead;
	//        ManualResetEvent event1 = new ManualResetEvent(false);
	//        result1._waitHandle = event1;
	//        NativeOverlapped* overlappedPtr1 = new Overlapped(0, 0, 0, result1).Pack(NamedStream.IOCallback);
	//        result1._overlapped = overlappedPtr1;
	//        if (this.CanSeek)
	//        {
	//            long num1 = this.Length;
	//            this.VerifyOSHandlePosition();
	//            if ((this._pos + numBytes) > num1)
	//            {
	//                if (this._pos <= num1)
	//                {
	//                    numBytes = (int) (num1 - this._pos);
	//                }
	//                else
	//                {
	//                    numBytes = 0;
	//                }
	//            }
	//            overlappedPtr1->OffsetLow = (int) this._pos;
	//            overlappedPtr1->OffsetHigh = (int) (this._pos >> 0x20);
	//            this._pos += numBytes;
	//            this.SeekCore((long) numBytes, SeekOrigin.Current);
	//        }
	//        result1.PinBuffer(array);
	//        int num2 = 0;
	//        int num3 = this.ReadFileNative(this._handleProtector, array, offset, numBytes, overlappedPtr1, out num2);
	//        if ((num3 == -1) && (numBytes != -1))
	//        {
	//            switch (num2)
	//            {
	//                case 0x6d:
	//                    NamedStream.SetEvent(result1._waitHandle.Handle);
	//                    result1._isComplete = true;
	//                    result1.CallUserCallback();
	//                    return result1;

	//                case 0x3e5:
	//                    return result1;
	//            }
	//            if (!this._handleProtector.IsClosed && this.CanSeek)
	//            {
	//                this.SeekCore((long) 0, SeekOrigin.Current);
	//            }
	//            if (num2 == 0x26)
	//            {
	//                __Error.EndOfFile();
	//                return result1;
	//            }
	//            __Error.WinIOError(num2, string.Empty);
	//        }
	//        return result1;
	//    }

 
	//    /// <summary>
	//    /// <para>Begins an asynchronous read.</para>
	//    /// </summary>
	//    /// <param name="array">The buffer to read data into.</param>
	//    /// <param name="offset">The byte offset in <paramref name="array" /> at which to begin reading.</param>
	//    /// <param name="numBytes">The maximum number of bytes to read.</param>
	//    /// <param name="userCallback">The method to be called when the asynchronous read operation is completed.</param>
	//    /// <param name="stateObject">A user-provided object that distinguishes this particular asynchronous read request from other requests.</param>
	//    /// <returns>
	//    /// <para>An <see cref="T:System.IAsyncResult" /> that references the asynchronous read.</para>
	//    /// </returns>
	//    /// <exception cref="T:System.ArgumentException">The array length minus <paramref name="offset" /> is less than <paramref name="numBytes" />.</exception>
	//    /// <exception cref="T:System.ArgumentNullException">
	//    /// <paramref name="array" /> is <see langword="null" /> . </exception>
	//    /// <exception cref="T:System.ArgumentOutOfRangeException">
	//    /// <paramref name="offset" /> or <paramref name="numBytes" /> is negative. </exception>
	//    /// <exception cref="T:System.IO.IOException">An asynchronous read was attempted past the end of the file.</exception>
	//    public override IAsyncResult BeginRead(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject)
	//    {
	//        NamedStream.AsyncNamedStream_AsyncResult result1;
	//        if (array == null)
	//        {
	//            throw new ArgumentNullException("array");
	//        }
	//        if (offset < 0)
	//        {
	//            throw new ArgumentOutOfRangeException("offset", "ArgumentOutOfRange_NeedNonNegNum");
	//        }
	//        if (numBytes < 0)
	//        {
	//            throw new ArgumentOutOfRangeException("numBytes", "ArgumentOutOfRange_NeedNonNegNum");
	//        }
	//        if ((array.Length - offset) < numBytes)
	//        {
	//            throw new ArgumentException("Argument_InvalidOffLen");
	//        }
	//        if (this._handleProtector.IsClosed)
	//        {
	//            __Error.FileNotOpen();
	//        }
	//        if (!this._isAsync)
	//        {
	//            return base.BeginRead(array, offset, numBytes, userCallback, stateObject);
	//        }
	//        if (!this.CanRead)
	//        {
	//            __Error.ReadNotSupported();
	//        }
	//        if (this._isPipe)
	//        {
	//            return this.BeginReadCore(array, offset, numBytes, userCallback, stateObject, 0);
	//        }
	//        if (this._writePos > 0)
	//        {
	//            this.FlushWrite();
	//        }
	//        if (this._readPos == this._readLen)
	//        {
	//            if (numBytes < this._bufferSize)
	//            {
	//                if (this._buffer == null)
	//                {
	//                    this._buffer = new byte[this._bufferSize];
	//                }
	//                IAsyncResult result2 = this.BeginReadCore(this._buffer, 0, this._bufferSize, null, null, 0);
	//                this._readLen = this.EndRead(result2);
	//                int num1 = this._readLen;
	//                if (num1 > numBytes)
	//                {
	//                    num1 = numBytes;
	//                }
	//                Buffer.InternalBlockCopy(this._buffer, 0, array, offset, num1);
	//                this._readPos = num1;
	//                result1 = NamedStream.AsyncNamedStream_AsyncResult.CreateBufferedReadResult(num1, userCallback, stateObject);
	//                result1.CallUserCallback();
	//                return result1;
	//            }
	//            this._readPos = 0;
	//            this._readLen = 0;
	//            return this.BeginReadCore(array, offset, numBytes, userCallback, stateObject, 0);
	//        }
	//        int num2 = this._readLen - this._readPos;
	//        if (num2 > numBytes)
	//        {
	//            num2 = numBytes;
	//        }
	//        Buffer.InternalBlockCopy(this._buffer, this._readPos, array, offset, num2);
	//        this._readPos += num2;
	//        if (num2 >= numBytes)
	//        {
	//            result1 = NamedStream.AsyncNamedStream_AsyncResult.CreateBufferedReadResult(num2, userCallback, stateObject);
	//            result1.CallUserCallback();
	//            return result1;
	//        }
	//        this._readPos = 0;
	//        this._readLen = 0;
	//        return this.BeginReadCore(array, offset + num2, numBytes - num2, userCallback, stateObject, num2);
	//    }

 
	//    private static unsafe void AsyncFSCallback(uint errorCode, uint numBytes, NativeOverlapped* pOverlapped)
	//    {
	//        Overlapped overlapped1 = Overlapped.Unpack(pOverlapped);
	//        NamedStream.AsyncNamedStream_AsyncResult result1 = (NamedStream.AsyncNamedStream_AsyncResult) overlapped1.AsyncResult;
	//        result1._numBytes = (int) numBytes;
	//        if ((errorCode == 0x6d) || (errorCode == 0xe8))
	//        {
	//            errorCode = 0;
	//        }
	//        result1._errorCode = (int) errorCode;
	//        WaitHandle handle1 = result1._waitHandle;
	//        if ((handle1 != null) && !NamedStream.SetEvent(handle1.Handle))
	//        {
	//            __Error.WinIOError();
	//        }
	//        result1._isComplete = true;
	//        result1._completedSynchronously = false;
	//        AsyncCallback callback1 = result1._userCallback;
	//        if (callback1 != null)
	//        {
	//            callback1(result1);
	//        }
	//    }

 
	//    internal NamedStream()
	//    {
	//        this._fileName = null;
	//        this._handleProtector = null;
	//    }

 



		
	//    /// <summary>
	//    /// 
	//    /// </summary>
	//    /// <param name="path"></param>
	//    public NamedStream(string path)
	//    {
	//        IntPtr ptr1;
	//        this._fileName = msgPath;
	//        if (path == null)
	//        {
	//            throw new ArgumentNullException("path", "ArgumentNull_Path");
	//        }
	//        if (path.Length == 0)
	//        {
	//            throw new ArgumentException("Argument_EmptyPath");
	//        }
	//        if ((((mode < FileMode.CreateNew) || (mode > FileMode.Append)) || ((access < FileAccess.Read) || (access > FileAccess.ReadWrite))) || ((share < FileShare.None) || (share > FileShare.ReadWrite)))
	//        {
	//            string text1 = "mode";
	//            if ((access < FileAccess.Read) || (access > FileAccess.ReadWrite))
	//            {
	//                text1 = "access";
	//            }
	//            if ((share < FileShare.None) || (share > FileShare.ReadWrite))
	//            {
	//                text1 = "share";
	//            }
	//            throw new ArgumentOutOfRangeException(text1, "ArgumentOutOfRange_Enum");
	//        }
	//        if (bufferSize <= 0)
	//        {
	//            throw new ArgumentOutOfRangeException("bufferSize", "ArgumentOutOfRange_NeedPosNum");
	//        }
	//        int num1 = (access == FileAccess.Read) ? -2147483648 : ((access == FileAccess.Write) ? 0x40000000 : -1073741824);
	//        string text2 = Path.GetFullPathInternal(path);
	//        this._fileName = text2;
	//        if (text2.StartsWith(@"\\.\"))
	//        {
	//            throw new ArgumentException("Arg_DevicesNotSupported");
	//        }
	//        FileIOPermissionAccess access1 = FileIOPermissionAccess.NoAccess;
	//        if ((access & FileAccess.Read) != ((FileAccess) 0))
	//        {
	//            if (mode == FileMode.Append)
	//            {
	//                throw new ArgumentException("Argument_InvalidAppendMode");
	//            }
	//            access1 |= FileIOPermissionAccess.Read;
	//        }
	//        if ((access & FileAccess.Write) != ((FileAccess) 0))
	//        {
	//            if (mode == FileMode.Append)
	//            {
	//                access1 |= FileIOPermissionAccess.Append;
	//            }
	//            else
	//            {
	//                access1 |= FileIOPermissionAccess.Write;
	//            }
	//        }
	//        else if (((mode == FileMode.Truncate) || (mode == FileMode.CreateNew)) || ((mode == FileMode.Create) || (mode == FileMode.Append)))
	//        {
	//            throw new ArgumentException("Argument_InvalidFileMode&AccessCombo : " + mode + " : " + access);
	//        }
	//        new FileIOPermission(access1, new string[] { text2 }, false, false).Demand();
	//        bool flag1 = mode == FileMode.Append;
	//        if (mode == FileMode.Append)
	//        {
	//            mode = FileMode.OpenOrCreate;
	//        }
	//        Win32Native.SECURITY_ATTRIBUTES security_attributes1 = null;
	//        if ((share & FileShare.Inheritable) != FileShare.None)
	//        {
	//            security_attributes1 = new Win32Native.SECURITY_ATTRIBUTES();
	//            security_attributes1.nLength = Marshal.SizeOf(security_attributes1);
	//            security_attributes1.bInheritHandle = 1;
	//            share &= ~FileShare.Inheritable;
	//        }
	//        if (NamedStream._canUseAsync && useAsync)
	//        {
	//            ptr1 = Win32Native.UnsafeCreateFile(text2, num1, share, security_attributes1, mode, 0x40000000, Win32Native.NULL);
	//            this._isAsync = true;
	//        }
	//        else
	//        {
	//            ptr1 = Win32Native.UnsafeCreateFile(text2, num1, share, security_attributes1, mode, 0x80, Win32Native.NULL);
	//            this._isAsync = false;
	//        }
	//        if (ptr1 != Win32Native.INVALID_HANDLE_VALUE)
	//        {
	//            this._handleProtector = new NamedStream.__NamedStreamHandleProtector(ptr1, true);
	//        }
	//        else
	//        {
	//            int num2 = Marshal.GetLastWin32Error();
	//            if ((num2 == 3) && text2.Equals(Directory.InternalGetDirectoryRoot(text2)))
	//            {
	//                num2 = 5;
	//            }
	//            bool flag2 = false;
	//            if (!bFromProxy)
	//            {
	//                try
	//                {
	//                    new FileIOPermission(FileIOPermissionAccess.PathDiscovery, new string[] { this._fileName }, false, false).Demand();
	//                    flag2 = true;
	//                }
	//                catch (SecurityException)
	//                {
	//                }
	//            }
	//            if (flag2)
	//            {
	//                __Error.WinIOError(num2, this._fileName);
	//            }
	//            else
	//            {
	//                __Error.WinIOError(num2, msgPath);
	//            }
	//        }
	//        int num3 = Win32Native.GetFileType(ptr1);
	//        if (num3 != 1)
	//        {
	//            this._handleProtector.Close();
	//            throw new NotSupportedException("NotSupported_NamedStreamOnNonFiles");
	//        }
	//        if (this._isAsync)
	//        {
	//            bool flag3 = false;
	//            new SecurityPermission(SecurityPermissionFlag.UnmanagedCode).Assert();
	//            try
	//            {
	//                flag3 = ThreadPool.BindHandle(ptr1);
	//            }
	//            finally
	//            {
	//                CodeAccessPermission.RevertAssert();
	//            }
	//            if (!flag3)
	//            {
	//                throw new IOException("IO.IO_BindHandleFailed");
	//            }
	//        }
	//        this._canRead = (access & FileAccess.Read) != ((FileAccess) 0);
	//        this._canWrite = (access & FileAccess.Write) != ((FileAccess) 0);
	//        this._canSeek = true;
	//        this._isPipe = false;
	//        this._pos = 0;
	//        this._bufferSize = bufferSize;
	//        this._readPos = 0;
	//        this._readLen = 0;
	//        this._writePos = 0;
	//        if (flag1)
	//        {
	//            this._appendStart = this.SeekCore((long) 0, SeekOrigin.End);
	//        }
	//        else
	//        {
	//            this._appendStart = -1;
	//        }
	//    }
	//}
#endif

}

