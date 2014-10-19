using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Microsoft.TMSN.IO
{
	/// <summary>
	/// Wrapper class around the Win32 memory-mapped file APIs.
	/// </summary>
	public class MemoryMappedFile : MarshalByRefObject, IDisposable
	{
		/// <summary>
		/// Handle to MemoryMappedFile object
		/// </summary>
		private IntPtr hMap = IntPtr.Zero;
		private long fileLength;
		private bool canWrite;


		/// <summary>
		/// Dispose of the file memory map.
		/// </summary>
		~MemoryMappedFile()
		{
			Dispose(false);
		}

		/// <summary>
		/// Create a memory map instance that views can be opened from, for reading.
		/// </summary>
		/// <param name="fileName">name of the backing file</param>
		/// <exception cref="IOException">Memory mapping failed at the Win32 layer</exception>
		/// <returns>A Stream for a view on the file</returns>
		public static Stream MemoryMappedStream(string fileName)
		{
			using (MemoryMappedFile mmf = new MemoryMappedFile(fileName))
			{
				return mmf.MapView();
			}
		}
		/// <summary>
		/// Create a memory map instance that views can be opened from.
		/// </summary>
		/// <param name="fileName">name of the backing file (or null for a pagefile-backed map)</param>
		/// <param name="write">if true, write access is allowed; if false, it is read-only</param>
		/// <exception cref="IOException">Memory mapping failed at the Win32 layer</exception>
		/// <returns>A Stream for a view on the file</returns>
		public static Stream MemoryMappedStream(string fileName, bool write)
		{
			using (MemoryMappedFile mmf = new MemoryMappedFile(fileName, write))
			{
				return mmf.MapView();
			}
		}
		/// <summary>
		/// Create a memory map instance that views can be opened from.
		/// </summary>
		/// <param name="fileName">name of the backing file (or null for a pagefile-backed map)</param>
		/// <param name="write">if true, write access is allowed; if false, it is read-only</param>
		/// <param name="writeCopy">if true, copy on write; if false, write to original memory</param>
		/// <exception cref="IOException">Memory mapping failed at the Win32 layer</exception>
		/// <returns>A Stream for a view on the file</returns>
		public static Stream MemoryMappedStream(string fileName, bool write, bool writeCopy)
		{
			using (MemoryMappedFile mmf = new MemoryMappedFile(fileName, write, writeCopy))
			{
				return mmf.MapView();
			}
		}

		/// <summary>
		/// Create a memory map instance that views can be opened from, for reading.
		/// </summary>
		/// <param name="fileName">name of the backing file</param>
		/// <exception cref="IOException">Memory mapping failed at the Win32 layer</exception>
		public MemoryMappedFile(string fileName)
			: this(fileName, false)
		{
		}
		/// <summary>
		/// Create a memory map instance that views can be opened from.
		/// </summary>
		/// <param name="fileName">name of the backing file (or null for a pagefile-backed map)</param>
		/// <param name="write">if true, write access is allowed; if false, it is read-only</param>
		/// <exception cref="IOException">Memory mapping failed at the Win32 layer</exception>
		public MemoryMappedFile(string fileName, bool write)
			: this(fileName, write, false)
		{
		}
		/// <summary>
		/// Create a memory map instance that views can be opened from.
		/// </summary>
		/// <param name="fileName">name of the backing file (or null for a pagefile-backed map)</param>
		/// <param name="write">if true, write access is allowed; if false, it is read-only</param>
		/// <param name="writeCopy">if true, copy on write; if false, write to original memory</param>
		/// <exception cref="IOException">Memory mapping failed at the Win32 layer</exception>
		public MemoryMappedFile(string fileName, bool write, bool writeCopy)
		{
			canWrite = write;

			// open file first
			IntPtr hFile = IOUtil.Win32.INVALID_HANDLE_VALUE;

			if (fileName != null)
			{
				// determine file access needed
				IOUtil.Win32.FileAccess desiredAccess = IOUtil.Win32.FileAccess.GENERIC_READ;
				if (write)
				{
					desiredAccess |= IOUtil.Win32.FileAccess.GENERIC_WRITE;
				}

				// let the application handle the problems...
				IOUtil.Win32.FileShare desiredShare = IOUtil.Win32.FileShare.FILE_SHARE_WRITE | IOUtil.Win32.FileShare.FILE_SHARE_READ;

				// open or create the file: if it doesn't exist, it gets created
				fileLength = IOUtil.GetLength(fileName);
				hFile = IOUtil.Win32.CreateFile(fileName, desiredAccess, desiredShare, IntPtr.Zero, IOUtil.Win32.CreationDisposition.OPEN_ALWAYS, 0, IntPtr.Zero);
				if (hFile == IOUtil.Win32.INVALID_HANDLE_VALUE)
				{
					throw new IOException("Invalid handle value", Marshal.GetHRForLastWin32Error());
				}
			}

			IOUtil.Win32.Protect protection = write ?
				(writeCopy ? IOUtil.Win32.Protect.PAGE_READWRITE : IOUtil.Win32.Protect.PAGE_WRITECOPY)
				: IOUtil.Win32.Protect.PAGE_READONLY;

			hMap = IOUtil.Win32.CreateFileMapping(hFile, IntPtr.Zero, protection, 0, 0, null);

			if (hFile != IOUtil.Win32.INVALID_HANDLE_VALUE) IOUtil.Win32.CloseHandle(hFile);
			if (hMap == IOUtil.Win32.NULL_HANDLE)
			{
				throw new IOException("Null handle", Marshal.GetHRForLastWin32Error());
			}
		}


		//		/// <summary>
		//		/// Open an existing named file mapping object.
		//		/// </summary>
		//		/// <param name="access">desired access to the map</param>
		//		/// <param name="name">name of object</param>
		//		/// <returns>the memory mapped file instance</returns>
		//		/// <exception cref="IOException">Memory mapping failed at the Win32 layer</exception>
		//		public static MemoryMappedFile Open(MapAccess access, string name)
		//		{
		//			MemoryMappedFile map = new MemoryMappedFile();
		//			map.hMap = IOUtil.Win32.OpenFileMapping((int)access, false, name);
		//			if (map.hMap == NULL_HANDLE)
		//			{
		//				throw new IOException("Null handle", Marshal.GetHRForLastWin32Error());
		//			}
		//			return map;
		//		}

		//		/// <summary>
		//		/// Close this file mapping object.
		//		/// </summary>
		//		public void Close()
		//		{
		//			Dispose(true);
		//		}

		/// <summary>
		/// Release the resources used by this instance.
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
		}
		private void Dispose(bool disposing)
		{
			if (hMap != IOUtil.Win32.NULL_HANDLE)
			{
				IOUtil.Win32.CloseHandle(hMap);
				hMap = IOUtil.Win32.NULL_HANDLE;
			}

			if (disposing)
			{
				GC.SuppressFinalize(this);
			}
		}

		/// <summary>
		/// Map a view as a Stream.
		/// </summary>
		public Stream MapView()
		{
			return MapView(canWrite ? IOUtil.Win32.MapAccess.FILE_MAP_ALL_ACCESS : IOUtil.Win32.MapAccess.FILE_MAP_READ, 0, (int)fileLength);
		}

		/// <summary>
		/// Map a view as a Stream.
		/// </summary>
		/// <param name="offset">offset of the file mapping object to start view at</param>
		/// <param name="size">size of the view</param>
		/// <exception cref="ObjectDisposedException">
		/// MemoryMappedFile cannot create views after it is disposed.
		/// </exception>
		/// <exception cref="IOException">Memory mapping failed at the Win32 layer</exception>
		public Stream MapView(long offset, int size)
		{
			return MapView(canWrite ? IOUtil.Win32.MapAccess.FILE_MAP_ALL_ACCESS : IOUtil.Win32.MapAccess.FILE_MAP_READ, offset, size);
		}

		/// <summary>
		/// Map a view as a Stream.
		/// </summary>
		/// <param name="access">desired access to the view</param>
		private Stream MapView(IOUtil.Win32.MapAccess access)
		{
			return MapView(access, 0, (int)fileLength);
		}

		/// <summary>
		/// Map a view as a Stream.
		/// </summary>
		/// <param name="access">desired access to the view</param>
		/// <param name="offset">offset of the file mapping object to start view at</param>
		/// <param name="size">size of the view</param>
		/// <exception cref="ObjectDisposedException">
		/// MemoryMappedFile cannot create views after it is disposed.
		/// </exception>
		/// <exception cref="IOException">Memory mapping failed at the Win32 layer</exception>
		private Stream MapView(IOUtil.Win32.MapAccess access, long offset, int size)
		{
			if (hMap == IOUtil.Win32.NULL_HANDLE)
			{
				throw new ObjectDisposedException("MemoryMappedFile is disposed and cannot create views");
			}

			IntPtr baseAddress = IntPtr.Zero;
			baseAddress = IOUtil.Win32.MapViewOfFile(hMap, (int)access, 0, 0, size);
			if (baseAddress == IntPtr.Zero)
			{
				throw new IOException("Null return for MapViewOfFile", Marshal.GetHRForLastWin32Error());
			}

			// Find out what MapProtection to use based on the MapAccess flags:
			IOUtil.Win32.Protect protection;
			if ((access & IOUtil.Win32.MapAccess.FILE_MAP_READ) != 0)
			{
				protection = IOUtil.Win32.Protect.PAGE_READONLY;
			}
			else
			{
				protection = IOUtil.Win32.Protect.PAGE_READWRITE;
			}

			return new MapViewStream(baseAddress, size, protection);
		}


		/// <summary>
		/// Stream that reads and writes from a view of a mapped file.
		/// </summary>
		internal class MapViewStream : Stream, IDisposable
		{
			private IOUtil.Win32.Protect protection = IOUtil.Win32.Protect.PAGE_NONE;
			/// <summary>
			/// ase address of the buffer
			/// </summary>
			IntPtr bufferBase = IntPtr.Zero;
			/// <summary>
			/// buffer length
			/// </summary>
			long length = 0;
			/// <summary>
			/// position within the buffer
			/// </summary>
			long position = 0;
			/// <summary>
			/// true if view is not closed
			/// </summary>
			bool isOpen = false;

			/// <summary>
			/// Constructor used internally by MemoryMappedFile.
			/// </summary>
			/// <param name="baseAddress">base address where the view starts</param>
			/// <param name="length">length of view in bytes</param>
			/// <param name="protection"></param>
			internal MapViewStream(IntPtr baseAddress, long length, IOUtil.Win32.Protect protection)
			{
				this.bufferBase = baseAddress;
				this.length = length;
				this.protection = protection;
				this.position = 0;
				this.isOpen = (baseAddress != IntPtr.Zero);
			}
			/// <summary>
			/// Release the resources used by this instance.
			/// </summary>
			~MapViewStream()
			{
				Dispose(false);
			}

			/// <summary>
			/// Get whether the Stream can read (true).
			/// </summary>
			public override bool CanRead
			{
				get { return true; }
			}
			/// <summary>
			/// Get whether the Stream can seek (true).
			/// </summary>
			public override bool CanSeek
			{
				get { return true; }
			}
			/// <summary>
			/// Get true if the Stream can be written to, false otherwise.
			/// </summary>
			public override bool CanWrite
			{
				get { return (((int)protection) & 0x000000C) != 0; }
			}
			/// <summary>
			/// Get the length of the Stream, in bytes.
			/// </summary>
			public override long Length
			{
				get { return length; }
			}
			/// <summary>
			/// Get or Set the Stream position, in bytes.
			/// </summary>
			public override long Position
			{
				get { return position; }
				set { Seek(value, SeekOrigin.Begin); }
			}

			/// <summary>
			/// Write all bytes currently buffered.
			/// </summary>
			public override void Flush()
			{
				if (!isOpen)
				{
					return;
				}
				IOUtil.Win32.FlushViewOfFile(bufferBase, (int)length);
			}

			/// <summary>
			/// Read bytes into the buffer.
			/// </summary>
			/// <param name="buffer">the buffer to read into</param>
			/// <param name="offset">the starting position</param>
			/// <param name="count">the number of bytes to read</param>
			/// <returns>the number of bytes read</returns>
			/// <exception cref="ObjectDisposedException">The Stream is already closed</exception>
			/// <exception cref="ArgumentException">The offset or count is out of range</exception>
			public override int Read(byte[] buffer, int offset, int count)
			{
				if (!isOpen)  throw new ObjectDisposedException("Stream is closed");
				if (buffer.Length - offset < count)  throw new ArgumentException("Invalid offset or count", "count");

				int bytesToRead = (int)Math.Min(Length - position, count);
				Marshal.Copy((IntPtr)(bufferBase.ToInt64() + position), buffer, offset, bytesToRead);

				position += bytesToRead;
				return bytesToRead;
			}

			/// <summary>
			/// Write the bytes in the buffer.
			/// </summary>
			/// <param name="buffer">the bytes to write</param>
			/// <param name="offset">the starting position</param>
			/// <param name="count">the number of bytes to write</param>
			/// <exception cref="ObjectDisposedException">The Stream is already closed</exception>
			/// <exception cref="IOException">The Stream cannot be written to</exception>
			/// <exception cref="ArgumentException">The offset or count is out of range</exception>
			public override void Write(byte[] buffer, int offset, int count)
			{
				if (!isOpen)  throw new ObjectDisposedException("Stream is closed");
				if (!CanWrite)  throw new IOException("Stream cannot be written to");
				if (buffer.Length - offset < count)  throw new ArgumentException("Invalid offset or count", "count");

				int bytesToWrite = (int)Math.Min(Length - position, count);
				if (bytesToWrite == 0)  return;

				Marshal.Copy(buffer, offset, (IntPtr)(bufferBase.ToInt64() + position), bytesToWrite);

				position += bytesToWrite;
			}

			/// <summary>
			/// Set the position within the Stream.
			/// </summary>
			/// <param name="offset">the position to set to, in bytes</param>
			/// <param name="origin">the point from which to take the offset</param>
			/// <returns>the new position</returns>
			/// <exception cref="ArgumentException">The seek position was out of range</exception>
			/// <exception cref="ObjectDisposedException">The Stream is already closed</exception>
			public override long Seek(long offset, SeekOrigin origin)
			{
				if (!isOpen)  throw new ObjectDisposedException("Stream is closed");

				long newpos = 0;
				switch (origin)
				{
					case SeekOrigin.Begin:
						newpos = offset;
						break;

					case SeekOrigin.Current:
						newpos = Position + offset;
						break;

					case SeekOrigin.End:
						newpos = Length + offset;
						break;
				}

				if (newpos < 0 || newpos > Length)
				{
					throw new ArgumentException("Invalid Seek offset", "offset");
				}
				position = newpos;
				return position;
			}

			/// <summary>
			/// Set the length - not supported.
			/// </summary>
			/// <param name="value">the value to not set the length to</param>
			/// <exception cref="NotSupportedException">Always thrown, since the length cannot be set.</exception>
			public override void SetLength(long value)
			{
				throw new NotSupportedException("Cannot change the view length.");
			}

			/// <summary>
			/// Close the Stream.
			/// </summary>
			public override void Close()
			{
				Dispose(true);
			}
			/// <summary>
			/// Release the resources used by this instance.
			/// </summary>
#if DOTNET2
			public new void Dispose()
#else
			public void Dispose()
#endif
			{
				Dispose(true);
			}
			/// <summary>
			/// Release the resources used by this instance.
			/// </summary>
			/// <param name="disposing">true if disposing, false otherwise</param>
#if DOTNET2
			protected override void Dispose(bool disposing)
#else
			protected void Dispose(bool disposing)
#endif
			{
				if (isOpen)
				{
					Flush();
					IOUtil.Win32.UnmapViewOfFile(bufferBase);
					isOpen = false;
				}
				if (disposing)
				{
					GC.SuppressFinalize(this);
				}
			}

		}

	}

}

