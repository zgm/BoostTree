// owner: rragno

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
	// * Unbuffered writer
	// * Robust stream
	// * Improve seek?
	// * custom StreamReader
	// * custom BinaryReader / BinaryReaderEx

	/// <summary>
	/// Stream using unbuffered I/O for efficient reading from fast disk arrays.
	/// </summary>
	public sealed class UnbufferedStream : FileStream, IDisposable
	{
		#region Static Configuration

		private static bool parallelRead = true;

		/// <summary>
		/// Get or Set whether to read in the background in a separate thread.
		/// </summary>
		/// <remarks>
		/// <p>
		/// This does not affect existing instances.
		/// </p>
		/// <p>
		/// Normally, it is faster to perform a multithreaded read, even on a
		/// single-processor machine. However, the thread creation cost does make
		/// it somewhat more expensive to create an instance.
		/// </p>
		/// </remarks>
		public static bool ParallelRead
		{
			get { return parallelRead; }
			set { parallelRead = value; }
		}

		//private static int initialBlockSize = 8 * 1024 * 1024;
		private static int initialBlockSize = 4 * 1024 * 1024;

		/// <summary>
		/// Get or Set the memory used for reading, in bytes.
		/// </summary>
		/// <remarks>
		/// <p>
		/// If the memory cannot be allocated, a backoff strategy will be used.
		/// </p>
		/// <p>
		/// The default is 8 MB.
		/// </p>
		/// </remarks>
		public static int BufferSize
		{
			get
			{
				return initialBlockSize * 2;
			}
			set
			{
				int newSize = value / 2;
				if (newSize < 0)  newSize = 1024 * 1024;
				// allow 0 for a minimal (sector-sized) buffer
				if (newSize != initialBlockSize)
				{
#if REUSE_BUFFERS
					// we need to invalidate the buffers, now...
					lock (standingBuffers)
					{
						for (int j = 0; j < standingBufferCount; j++)
						{
							try
							{
								if (standingBuffers[j] != IntPtr.Zero)
								{
									//Console.Write("(freeing... ");
									bool ret = VirtualFree(standingBuffers[j], IntPtr.Zero, /*MEM_DECOMMIT*/ MEM_RELEASE);
									//int count = Interlocked.Decrement(ref allocCount);
									//Console.WriteLine("done: " + ret + " " + count + ")");
									standingBuffers[j] = IntPtr.Zero;
								}
							}
							catch
							{
							}
						}
						standingBufferCount = 0;
					}
#endif
					initialBlockSize = newSize;
				}
			}
		}
		#endregion


		#region Instance Fields

		private long length;
		private string fileName;

		private readonly uint sectorSize;
		private /*readonly*/ IntPtr[] buffer;
		private /*readonly*/ IntPtr[] alignedBuffer;
		private readonly IntPtr handle;
		private /*readonly*/ int blockSize;
		private bool parallel;
		private bool released = true;
		//private int bufferStart = 0;
		//private int bufferEnd = 0;
		//private byte[] mBuffer;

		#endregion


		#region Creation and Cleanup

		/// <summary>
		/// Exception that represents a failure in the internal memory allocation.
		/// </summary>
		public class VirtualAllocException : IOException
		{
			/// <summary>
			/// Create a new exception.
			/// </summary>
			/// <param name="msg">the message associated with this exception</param>
			public VirtualAllocException(string msg)
				: base(msg)
			{
			}
		}


		/// <summary>
		/// Open a file for reading without NTFS caching.
		/// The stream should be accessed sequentially - seeking can be slow - and it will not
		/// support writing.
		/// </summary>
		/// <param name="fileName">name of file to open</param>
		/// <returns>Unbuffered file stream</returns>
		/// <exception cref="ArgumentNullException">fileName is null.</exception>
		/// <exception cref="ArgumentException">fileName is invalid.</exception>
		/// <exception cref="FileNotFoundException">fileName cannot be found.</exception>
		/// <exception cref="IOException"> An I/O error has occurred.</exception>
		//		/// <remarks>This may throw a different exception than FileNotFound if the file does
//		/// not exist.</remarks>
//      is relying on the order of evaluation bad?? ***
		public UnbufferedStream(string fileName)
			: this(fileName,
			new FileAlignmentInfo(fileName),
			IOUtil.Win32.CreateFile(fileName, IOUtil.Win32.FileAccess.GENERIC_READ, IOUtil.Win32.FileShare.FILE_SHARE_READ, IntPtr.Zero, IOUtil.Win32.CreationDisposition.OPEN_EXISTING,
			IOUtil.Win32.FileFlagsAndAttributes.FILE_FLAG_NO_BUFFERING | IOUtil.Win32.FileFlagsAndAttributes.FILE_FLAG_SEQUENTIAL_SCAN, IntPtr.Zero))
		{
		}
		/// <summary>
		/// Open a file for reading without NTFS caching.
		/// The stream should be accessed sequentially - seeking can be slow - and it will not
		/// support writing.
		/// </summary>
		/// <param name="fileName">name of file to open</param>
		/// <param name="alignInfo">the length, sector size, and bytes from the end that are not sector-aligned</param>
		/// <param name="handle">the handle of the specified file</param>
		/// <returns>Unbuffered file stream</returns>
		/// <exception cref="VirtualAllocException">A problem occurred alocating memory at a low level.</exception>
#if !UNBUFFERED_AS_STREAM
		private UnbufferedStream(string fileName, FileAlignmentInfo alignInfo, IntPtr handle)
#if DOTNET2
			: base(new Microsoft.Win32.SafeHandles.SafeFileHandle(handle, true), FileAccess.Read, 1, false)
#else
			: base(handle, FileAccess.Read, true, 1, false)
#endif
#else
		private UnbufferedStream(string fileName, long length, IntPtr handle)
#endif
		{
			//Console.WriteLine("new unbuffered: " + fileName);
#if MONITOR
				Console.WriteLine(new string('-', (":: " + Path.GetFileName(fileName) + " :: " + "Open").Length));
				Console.WriteLine(":: " + Path.GetFileName(fileName) + " :: " + "Open");
#endif
			this.fileName = fileName;
			//this.length = base.Length;
			this.length = alignInfo.Length;
			this.unalignedTail = alignInfo.UnalignedTail;
			this.sectorSize = alignInfo.SectorSize;
			//this.handle = (IntPtr)typeof(FileStream).GetProperty("Handle").GetValue(this, null);
			//this.handle = base.Handle;
			this.handle = handle;

			// open for first time:
			Reopen();
		}

		/// <summary>
		/// Release resources.
		/// </summary>
		~UnbufferedStream()
		{
			Dispose(true);
		}

		/// <summary>
		/// Release the resources used for the unbuffered file.
		/// </summary>
		/// <param name="disposing">true if disposing, false otherwise</param>
		protected override void Dispose(bool disposing)
		{
			try
			{
				base.Dispose(disposing);
			}
			catch
			{
				// ignore
			}
			try
			{
				ReleaseBuffer();
			}
			catch
			{
				// ignore
			}
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
			closed = true;
		}

		//static int allocCount = 0;

		private void ReleaseBuffer()
		{
			//if (checkStream != null)
			//{
			//    checkStream.Close();
			//    checkStream = null;
			//}
			if (!released)
			{
				if (buffer != null)
				{
					try
					{
						for (int j = 0; j < buffer.Length; j++)
						{
							if (buffer[j] != IntPtr.Zero)
							{
								try
								{
#if REUSE_BUFFERS
									if (buffer[j] != IntPtr.Zero)
									{
										if (blockSize == initialBlockSize)
										{
											// save the buffer for later...
											lock (standingBuffers)
											{
												if (standingBufferCount < standingBuffers.Length)
												{
													standingBuffers[standingBufferCount++] = buffer[j];
													buffer[j] = IntPtr.Zero;
												}
											}
										}
									}
#endif

									if (buffer[j] != IntPtr.Zero)
									{
										//Console.Write("(freeing... ");
										bool ret = IOUtil.Win32.VirtualFree(buffer[j], IntPtr.Zero, /*MEM_DECOMMIT*/ IOUtil.Win32.FreeType.MEM_RELEASE);
										//int count = Interlocked.Decrement(ref allocCount);
										//Console.WriteLine("done: " + ret + " " + count + ")");
										buffer[j] = IntPtr.Zero;
									}
								}
								catch
								{
								}
							}
						}
					}
					catch
					{
					}
				}
				buffer = null;
				released = true;
			}
			if (fillThread != null)
			{
				// skip this - we close the handle, so hopefully we are OK, and this can hang...
				//try
				//{
				//    fillThread.Abort();
				//}
				//catch
				//{
				//}
				fillThread = null;
			}
		}

		bool closed = false;

		/// <summary>
		/// Close the stream.
		/// </summary>
		public override void Close()
		{
			if (closed) return;
			closed = true;
			base.Close();
			Dispose(true);
		}


#if REUSE_BUFFERS
		private static IntPtr[] standingBuffers = new IntPtr[32];
		private static int standingBufferCount = 0;
#endif

		private void Reopen()
		{
			Reopen(0, true);
		}
		private void Reopen(long startPosition, bool allocate)
		{
			if (fillThread != null)
			{
				try
				{
					fillThread.Abort();
				}
				catch
				{
				}
				fillThread = null;
			}
			//if (allocate)  ReleaseBuffer();
			long startRemainder = (startPosition % sectorSize);
			try
			{
				startPosition -= startRemainder;
				long res;
				IOUtil.Win32.SetFilePointerEx(handle, startPosition, out res, IOUtil.Win32.SeekOrigin.FILE_BEGIN);
			}
			catch
			{
				// ignore?
			}

			try
			{
				released = false;

				totalRead = startPosition;
				totalGet = startPosition;
				getCount = 0;
				bufferFillIndex = 0;
				bufferGetIndex = 0;
				fillCount = 0;
				currentBuffer = IntPtr.Zero;
				currentBufferLimit = 0;
				currentBufferBottom = 0;
				gotDone = false;

				if (allocate)
				{
					this.parallel = ParallelRead;
					int parallelLevel = parallel ? 2 : 1;
					this.blockSize = initialBlockSize;
					// force non-parallel for small files...
					if (parallel && (length < 4*sectorSize || length < 2*blockSize))
					{
						this.parallel = false;
						parallelLevel = 1;
					}
					this.blockSize = blockSize / parallelLevel;
					if (blockSize % sectorSize != 0)  blockSize -= (int)(blockSize % sectorSize);
					if (blockSize < sectorSize)  blockSize = (int)sectorSize;

					// check on buffer alignment:
					//mBuffer = new byte[blockSize + 16];
					//fixed (byte* mb = mBuffer)
					//{
					//    IntPtr tmp = new IntPtr(mb);
					//    Console.WriteLine(tmp.ToInt64());
					//    Console.WriteLine((tmp.ToInt64() + 7) & ~7);
					//}

					readMutex = new AutoResetEvent[parallelLevel];
					pullMutex = new AutoResetEvent[parallelLevel];
					readSize = new int[parallelLevel];
					done = new bool[parallelLevel];

					buffer = new IntPtr[parallelLevel];
					alignedBuffer = new IntPtr[parallelLevel];
					bool completed = false;
					while (!completed)
					{
						completed = true;
						for (int i = 0; i < buffer.Length; i++)
						{
							//if (buffer[i] != IntPtr.Zero)  throw new VirtualAllocException("Buffer double allocated");
							buffer[i] = IntPtr.Zero;

#if REUSE_BUFFERS
							if (blockSize == initialBlockSize)
							{
								//Console.WriteLine("locking...");
								lock (standingBuffers)
								{
									if (standingBufferCount > 0)
									{
										standingBufferCount--;
										buffer[i] = standingBuffers[standingBufferCount];
									}
								}
								//Console.WriteLine("locked.");
							}
#endif

							if (buffer[i] == IntPtr.Zero)
							{
								//Console.WriteLine("allocated: " +
								//	(System.Diagnostics.Process.GetCurrentProcess().VirtualMemorySize / 1024 / 1024) + " MB");
								//Console.Write("(allocating... ");
								buffer[i] = IOUtil.Win32.VirtualAlloc(IntPtr.Zero, new IntPtr(blockSize + 8), IOUtil.Win32.AllocationType.MEM_RESERVE | IOUtil.Win32.AllocationType.MEM_COMMIT, IOUtil.Win32.Protect.PAGE_READWRITE);
								//if (buffer[i] == IntPtr.Zero || buffer[i].ToInt64() < 0)
								//{
								//	Console.WriteLine("failed @" + (blockSize + 8) + ": " + allocCount + ")");
								//	Console.ReadLine();
								//}
								//else
								//{
								//	int acount = Interlocked.Increment(ref allocCount);
								//	Console.WriteLine("done: " + acount + ")");
								//}
							}

							if (buffer[i] == IntPtr.Zero || buffer[i].ToInt64() < 0)
							{
								for (int j = 0; j < i; j++)
								{
									if (buffer[j] != IntPtr.Zero)
									{
										try
										{
#if REUSE_BUFFERS
											if (buffer[j] != IntPtr.Zero)
											{
												if (blockSize == initialBlockSize)
												{
													// save the buffer for later...
													lock (standingBuffers)
													{
														if (standingBufferCount < standingBuffers.Length)
														{
															standingBuffers[standingBufferCount++] = buffer[j];
															buffer[j] = IntPtr.Zero;
														}
													}
												}
											}
#endif

											if (buffer[j] != IntPtr.Zero)
											{
												//Console.Write("(freeing... ");
												bool ret = IOUtil.Win32.VirtualFree(buffer[j], IntPtr.Zero, /*MEM_DECOMMIT*/ IOUtil.Win32.FreeType.MEM_RELEASE);
												//int count = Interlocked.Decrement(ref allocCount);
												//Console.WriteLine("done: " + ret + " " + count + ")");
												buffer[j] = IntPtr.Zero;
											}
										}
										catch
										{
										}
									}
								}
								if (this.blockSize <= 256 * 1024)
								{
									throw new VirtualAllocException("Could not allocate buffer for UnbufferedStream.");
								}
								// try reducing the buffer size:
								this.blockSize = this.blockSize / 2;
								// collect to compact memory:
								//GC.Collect();
								GC.WaitForPendingFinalizers();
								GC.Collect();
								// try again:
								completed = false;
								break;
							}
							alignedBuffer[i] = new IntPtr((buffer[i].ToInt64() + 7) & ~7);
							readMutex[i] = new AutoResetEvent(false);
							pullMutex[i] = new AutoResetEvent(true);
						}
					}
				}
				else
				{
					// clear these, in any case:
					for (int i = 0; i < readMutex.Length; i++)
					{
						readSize[i] = 0;
						done[i] = false;
						readMutex[i].Reset();
						pullMutex[i].Set();
					}
				}

				alignedLimit = length - (unalignedTail == null ? 0 : unalignedTail.Length);
				//if (alignedLimit < 0)  alignedLimit = 0;
				if (parallel)
				{
					fillThread = new Thread(new ThreadStart(FillBuffer));
					fillThread.IsBackground = true;
					// start now?
					// skip if small?  ***
					fillThread.Start();
				}

				if (startRemainder != 0)
				{
					//Seek(startRemainder, SeekOrigin.Current);
					Skip(startRemainder);
				}
			}
			catch
			{
				for (int j = 0; j < buffer.Length; j++)
				{
					if (buffer[j] != IntPtr.Zero)
					{
						try
						{
							// don't bother trying to save them?
							//Console.Write("(freeing... ");
							bool ret = IOUtil.Win32.VirtualFree(buffer[j], IntPtr.Zero, /*MEM_DECOMMIT*/ IOUtil.Win32.FreeType.MEM_RELEASE);
							//int count = Interlocked.Decrement(ref allocCount);
							//Console.WriteLine("done: " + ret + " " + count + ")");
							buffer[j] = IntPtr.Zero;
						}
						catch
						{
						}
					}
				}
				if (fillThread != null)
				{
					try
					{
						fillThread.Abort();
						fillThread = null;
					}
					catch
					{
					}
				}
				try
				{
					base.Dispose(true);
				}
				catch
				{
				}
				throw;
			}
		}

		#endregion


		#region Properties

		/// <summary>
		/// Get the Length of this file, in bytes.
		/// </summary>
		public override long Length
		{
			get
			{
				//				return base.Length;
				// use the simple, recorded length:
				return length;
			}
		}

		/// <summary>
		/// Get whether the stream supports reading - always true.
		/// </summary>
		public override bool CanRead
		{
			get
			{
				return true;
				//return base.CanRead;
			}
		}

		/// <summary>
		/// Get whether the stream supports writing - always false.
		/// </summary>
		public override bool CanWrite
		{
			get
			{
				return false;
				//return base.CanWrite;
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
				//return base.Position;
				return totalGet - currentBufferLimit + currentBufferBottom;
			}
			set
			{
				//throw new NotSupportedException("UnbufferedStream cannot seek.");
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
			byte[] dump = new byte[Math.Min(count, 128 * 1024)];
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
					System.Diagnostics.Debug.WriteLine("Skip failed! Read " + read + " / " + dump.Length + " for chunk.");
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
#if SLOW_SEEK
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
			if (offset < 0)  offset = 0;
			if (offset == cur)  return cur;

			if (offset > cur)
			{
				Skip(offset - cur);
				return Position;
			}
			else
			{
				Reopen();
				Skip(offset);
				return Position;
			}
#else
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

			if (offset > cur && (offset - cur) < 4000000)
			{
				Skip(offset - cur);
				return Position;
			}
			else
			{
				Reopen(offset, false);
				return Position;
			}
#endif
		}

		/// <summary>
		/// Get whether the stream supports seeking. true, although performance
		/// might not be optimal.
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
		
		#region Private Read Helpers

		private long totalRead = 0;
		private int bufferFillIndex = 0;
		private int bufferGetIndex = 0;
		private int fillCount = 0;
		private Thread fillThread;
		private /*readonly*/ AutoResetEvent[] readMutex;
		private /*readonly*/ AutoResetEvent[] pullMutex;
		//private readonly AutoResetEvent[] readFillMutex;
		//private readonly AutoResetEvent[] readGetMutex;
		private /*readonly*/ int[] readSize;
		private byte[] unalignedTail;


		/// <summary>
		/// Data about the file size and alignment.
		/// </summary>
		private struct FileAlignmentInfo
		{
			public readonly long Length;
			public readonly uint SectorSize;
			public readonly byte[] UnalignedTail;

			/// <summary>
			/// Hack to read the unaligned tail first, instead of at the end. This helps with
			/// keeping the file handles open, which enables seeking later.
			/// </summary>
			/// <param name="fileName">the file to read the tail of</param>
			/// <returns>the bytes of the unaligned tail, or null if the file is aligned</returns>
			/// <exception cref="FileNotFoundException">The file cannot be found.</exception>
			/// <exception cref="IOException">The tail could not be read.</exception>
			public FileAlignmentInfo(string fileName)
			{
				Length = (new FileInfo(fileName)).Length;
				SectorSize = FindSectorSize(fileName);
				int remainder = (int)(Length % SectorSize);
				if (remainder == 0)
				{
					UnalignedTail = null;
				}
				else
				{
					UnalignedTail = new byte[remainder];
					int overCount;
					for (int i = 0; i < 3; i++)
					{
						try
						{
							using (FileStream overRead = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
							{
								overRead.Seek(-UnalignedTail.Length, SeekOrigin.End);
								overCount = overRead.Read(UnalignedTail, 0, UnalignedTail.Length);
							}
							if (overCount != remainder)
							{
								throw new IOException("UnbufferedStream could not read tail of file: " +
									"expected " + remainder + ", read " + overCount);
							}
						}
						catch (FileNotFoundException)
						{
							throw;
						}
						catch
						{
							if (i == 2) throw;
							if (i == 0) Thread.Sleep(10);
							if (i == 1) Thread.Sleep(100);
						}
					}
				}
			}

			/// <summary>
			/// Return the sector size of the drive of the given path.
			/// </summary>
			/// <param name="path">path name for the drive, file, or directory</param>
			/// <returns>drive sector size in bytes </returns>
			private static uint FindSectorSize(string path)
			{
				uint size = 512;
				uint ignore;
				IOUtil.Win32.GetDiskFreeSpace(Path.GetPathRoot(path), out ignore, out size, out ignore, out ignore);
				return size;
			}
		}


		private long alignedLimit;

		private void FillBuffer()
		{
			// This loop is the only one making reads - they are sequential and on only this
			// thread - but the buffers it reads into are read in parallel by other threads.
			// The idea is just to keep passing this thread buckets to let it pump data.
			try
			{
				while (FillBufferPass())
				{
					// keep reading...
				}
			}
			catch (Exception) // ex)
			{
				// ignore all exceptions on the filling thread...
				// may be needed, but will hide potential problems.
				//System.Diagnostics.Debug.WriteLine("");
				//System.Diagnostics.Debug.WriteLine("FillBuffer exception: " + ex.ToString());
			}
		}

		private bool FillBufferPass()
		{
			//Console.WriteLine("[" + totalRead + " / " + alignedLimit + " / " + length + "]");
			// read the aligned amount only, specified by 'alignedLimit':
			if (totalRead < alignedLimit)
			{
				int curBlock = (int)Math.Min(blockSize, alignedLimit - totalRead);

				//Console.WriteLine(" " + totalRead + " -> " + (totalRead + curBlock) + " / " + alignedLimit);
				// wait for pull of last read:
#if MONITOR
				Console.WriteLine(":: " + Path.GetFileName(fileName) + " :: " + "".PadLeft(4) +
					("  fill wait: " + fillCount + " pullMutex[" + bufferFillIndex + "]").PadRight(56) +
					" bytes: " + totalRead + " / " + length + " (" + alignedLimit + ")");
#endif
				pullMutex[bufferFillIndex].WaitOne();
#if MONITOR
				Console.WriteLine(":: " + Path.GetFileName(fileName) + " :: " + "".PadLeft(4) +
					("  fill  got: " + fillCount + " pullMutex[" + bufferFillIndex + "]").PadRight(56) +
					" bytes: " + totalRead + " / " + length + " (" + alignedLimit + ")");
#endif
                try {
				    // read into buffer:
				    int readBytes;
				    //long t = DateTime.UtcNow.Ticks;
				    //long startPos = IOUtil.Win32.Raw.GetFilePos(handle);
				    bool readres = IOUtil.Win32.Raw.ReadFile(handle, alignedBuffer[bufferFillIndex],
					    (int)Math.Min(blockSize, alignedLimit - totalRead), out readBytes);
				    //t = DateTime.UtcNow.Ticks - t;
				    //Console.WriteLine("core time: " + (t / (double)TimeSpan.TicksPerSecond).ToString("0.000"));
				    if (!readres)
				    {
					    // try again?? ***
					    System.Diagnostics.Debug.WriteLine("");
					    System.Diagnostics.Debug.WriteLine("ReadFile failed! at: " + totalRead);
					    Thread.Sleep(10);
					    readres = IOUtil.Win32.Raw.ReadFile(handle, alignedBuffer[bufferFillIndex],
						    (int)Math.Min(blockSize, alignedLimit - totalRead), out readBytes);
					    if (!readres)
					    {
						    System.Diagnostics.Debug.WriteLine("");
						    System.Diagnostics.Debug.WriteLine("ReadFile failed!! at: " + totalRead);

						    Thread.Sleep(50);
						    readres = IOUtil.Win32.Raw.ReadFile(handle, alignedBuffer[bufferFillIndex],
							    (int)Math.Min(blockSize, alignedLimit - totalRead), out readBytes);

						    if (!readres)
						    {
							    System.Diagnostics.Debug.WriteLine("");
							    System.Diagnostics.Debug.WriteLine("ReadFile failed!!! at: " + totalRead);
							    readBytes = 0;
						    }
					    }
				    }

				    // set read amounts:
				    readSize[bufferFillIndex] = readBytes;
				    totalRead += readBytes;

				    // signal completed read:
				    fillCount++;

    #if MONITOR
				    Console.WriteLine(":: " + Path.GetFileName(fileName) + " :: " + "".PadLeft(4) +
					    ("  fill  set: " + fillCount + " readMutex[" + bufferFillIndex + "]").PadRight(56) +
					    " bytes: " + totalRead + " / " + length + " (" + alignedLimit + ")");
    #endif
				    if (readBytes == 0)
				    {
					    if (totalRead < alignedLimit)
					    {
						    throw new IOException("Could not read complete buffer");
					    }
				    }

				    if (totalRead < alignedLimit || (unalignedTail != null && unalignedTail.Length != 0))
				    {
					    //Console.WriteLine("continue...");
					    readMutex[bufferFillIndex].Set();
					    bufferFillIndex = (bufferFillIndex + 1) % readMutex.Length;
					    return true;
				    }
				    else
				    {
					    //Console.WriteLine(" totalRead: " + totalRead + "  alignedLimit: " + alignedLimit);
					    done[bufferFillIndex] = true;
					    readMutex[bufferFillIndex].Set();
					    bufferFillIndex = (bufferFillIndex + 1) % readMutex.Length;
					    return false;
				    }
                }
                catch
                {
                    pullMutex[bufferFillIndex].Set();
                    throw;
                }
			}
			else if (alignedLimit == totalRead && unalignedTail != null && unalignedTail.Length != 0)
			{
				//Console.Error.WriteLine();
				//Console.Error.WriteLine("Unaligned tail: " + alignedLimit + " + " + unalignedTail.Length + " => " + length);

				// wait for pull of last read:
				//Console.WriteLine("  fill get : " + fillCount + " [" + bufferFillIndex + "]");
				pullMutex[bufferFillIndex].WaitOne();

                try {
				    // use the tail we read at the beginning:
				    // unalignedTail must not be null, since we are not aligned
				    totalRead += unalignedTail.Length;
				    CopyBuffer(unalignedTail, alignedBuffer[bufferFillIndex], unalignedTail.Length);
				    readSize[bufferFillIndex] = unalignedTail.Length;
				    //Console.WriteLine(" tail done");
				    done[bufferFillIndex] = true;
				}
				catch(Exception ex)
				{
				    pullMutex[bufferFillIndex].Set();
				    throw ex;
				}

			    //fillCount++;
			    //Console.WriteLine("  fill set : " + fillCount + " [" + bufferFillIndex + "]" + " *");
			    readMutex[bufferFillIndex].Set();
			    bufferFillIndex = (bufferFillIndex + 1) % readMutex.Length;
                
				return false;
			}
			else
			{
				//Console.Error.WriteLine();
				//Console.Error.WriteLine("No unaligned tail.");

				readSize[bufferFillIndex] = 0;
				//Console.WriteLine(" null done?");
				done[bufferFillIndex] = true;
				//Console.WriteLine("  fill set : " + fillCount + " [" + bufferFillIndex + "]" + " **");
				readMutex[bufferFillIndex].Set();
				bufferFillIndex = (bufferFillIndex + 1) % readMutex.Length;
				return false;
			}
		}


		bool[] done;
		bool gotDone = false;
		long totalGet = 0;
		int getCount = 0;

		private void GetBuffer()
		{
			if (gotDone)
			{
				//Console.WriteLine("GetBuffer done");
				currentBufferBottom = 0;
				currentBufferLimit = 0;
				currentBuffer = alignedBuffer[0];
				//unsafe
				//{
				//    current = (byte*)currentBuffer.ToPointer();
				//}
				return;
			}
			//			Console.WriteLine("Copying buffer " + bufferIndex);
			IntPtr res = alignedBuffer[bufferGetIndex];

			if (getCount != 0)
			{
				pullMutex[(bufferGetIndex + readMutex.Length - 1) % readMutex.Length].Set();
#if MONITOR
				Console.WriteLine(":: " + Path.GetFileName(fileName) + " :: " + "".PadLeft(32) +
					(" drain  set: " + getCount + " pullMutex[" + ((bufferGetIndex + readMutex.Length - 1) % readMutex.Length) + "]").PadRight(28) +
					" bytes: " + totalGet + " / " + length);
#endif
			}

#if MONITOR
			Console.WriteLine(":: " + Path.GetFileName(fileName) + " :: " + "".PadLeft(32) +
				(" drain wait: " + getCount + " readMutex[" + bufferGetIndex + "]").PadRight(28) +
				" bytes: " + totalGet + " / " + length);
#endif
			// if needed, do it ourselves...
			if (!parallel)
			{
				FillBufferPass();
			}

			readMutex[bufferGetIndex].WaitOne();
			getCount++;

			currentBufferBottom = 0;
			currentBufferLimit = readSize[bufferGetIndex];
			if (done[bufferGetIndex] || currentBufferLimit == 0)
			{
				//Console.WriteLine("  done : " + done[bufferGetIndex] + " " + currentBufferLimit);
				for (int i = 0; i < readSize.Length; i++)
				{
					readSize[i] = 0;
				}
				gotDone = true;
			}
			totalGet += currentBufferLimit;
			currentBuffer = res;
#if MONITOR
			Console.WriteLine(":: " + Path.GetFileName(fileName) + " :: " + "".PadLeft(32) +
				(" drain  got: " + getCount + " readMutex[" + bufferGetIndex + "]").PadRight(28) +
				" bytes: " + totalGet + " / " + length);
#endif

			bufferGetIndex = (bufferGetIndex + 1) % readMutex.Length;

			//unsafe
			//{
			//    current = (byte*)currentBuffer.ToPointer();
			//}
		}

		private /*unsafe*/ void CopyBuffer(byte[] source, IntPtr dest, int count)
		{
			if (count <= 0) return;

			#region copy variations
			unsafe
			{
				byte* d = (byte*)dest.ToPointer();
				for (int i = 0; i < count; i++)
				{
					*d = source[i];
					d++;
				}
			}
			#endregion

			//// Marshal is incredibly slow!!
			//Marshal.Copy(source, 0, dest, count);
		}
		private /*unsafe*/ void CopyBuffer(IntPtr source, int sourceStart, byte[] dest, int destStart, int count)
		{
			if (count <= 0) return;

			#region copy variations
			unsafe
			{
				// pointer to array copy:
				byte* s = ((byte*)source.ToPointer()) + sourceStart;
				count += destStart;
				for ( ; destStart < count; destStart++)
				{
					dest[destStart] = *(s++);
				}

				// simple pointer copy:
//				byte* s = ((byte*)source.ToPointer()) + sourceStart;
//				fixed (byte* dd = dest)
//				{
//					byte* d = dd + destStart;
//					byte* dEnd = d + count;
//					while (d != dEnd)
//					{
//						*(d++) = *(s++);
//					}
//				}

				// int pointer copy:
//				int* s = (int*)(((byte*)source.ToPointer()) + sourceStart);
//				fixed (byte* dd = dest)
//				{
//					int* d = (int*)(dd + destStart);
//					int* dEnd = d + (count >> 2);
//					while (d != dEnd)
//					{
//						*d = *s;
//						d++;
//						s++;
//					}
//					count = count & 3;
//					if (count != 0)
//					{
//						for (int i = 0; i < count; i++)
//						{
//							*(((byte*)d) + i) = *(((byte*)s) + i);
//						}
//					}
//				}
			}
			#endregion

			//// Marshal is slow ??
//			Marshal.Copy(new IntPtr(source.ToInt64() + sourceStart), dest, destStart, count);
		}


		private IntPtr currentBuffer = IntPtr.Zero;
		private int currentBufferLimit = 0;
		private int currentBufferBottom = 0;

		private byte[] sharedBuffer = null;

		#endregion

		#region Public Read Functionality

		/// <summary>
		/// Reads a block of bytes from the stream and writes the data into a buffer.
		/// The buffer is automatically allocated, but it may be shared across calls to this method.
		/// </summary>
		/// <param name="buffer">the array in which the values are replaced by the bytes read</param>
		/// <returns>
		/// The total number of bytes read into the buffer. This will be 0 if the end
		/// of the stream has been reached, and is guaranteed to be less than buffer.Length only if
		/// fewer than buffer.Length bytes remain (and it will then equal the remainder of the bytes).
		/// </returns>
		public int Read(out byte[] buffer)
		{
			if (sharedBuffer == null)
			{
				// could use a weak reference...
				sharedBuffer = new byte[blockSize];
			}
			buffer = sharedBuffer;
			//Console.WriteLine("(reading out into buffer[" + buffer.Length + "])");
			int res = Read(buffer);
			// should we resize the array here? it might be convenient...
			if (res == 0)
			{
				sharedBuffer = null;
				buffer = new byte[0];
			}
			return res;
		}
		/// <summary>
		/// Reads a block of bytes from the stream and writes the data in a given buffer.
		/// </summary>
		/// <param name="buffer">the array in which the values are replaced by the bytes read
		/// </param>
		/// <returns>
		/// The total number of bytes read into the buffer. This will be 0 if the end
		/// of the stream has been reached, and is guaranteed to be less than buffer.Length only if
		/// fewer than buffer.Length bytes remain (and it will then equal the remainder of the bytes).
		/// </returns>
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
		public override int Read(byte[] buffer, int offset, int count)
		{
			//Console.WriteLine("Read into buffer.Length = " + buffer.Length + ", offset = " + offset + ", count = " + count);
#if !OLD_DIRECT_READ

			if (offset + count > buffer.Length) throw new ArgumentOutOfRangeException("offset + count > buffer.Length");
			if (buffer == null) throw new ArgumentNullException("buffer is null", "buffer");
			if (offset < 0 || count < 0) throw new ArgumentOutOfRangeException("Negative offset or count");

			//Console.WriteLine("(reading " + offset + " , " + count + ")");
			int read = 0;
			//unsafe
			//{
			while (count > 0)
			{
				if (currentBufferLimit == currentBufferBottom)
				{
					GetBuffer();
					if (currentBufferLimit == 0) break;
				}
				if (currentBufferLimit - currentBufferBottom >= count)
				{
					CopyBuffer(currentBuffer, currentBufferBottom, buffer, offset, count);
					currentBufferBottom += count;
					//current += count;
					read += count;
					count = 0;
					break;
				}
				else
				{
					int chunk = currentBufferLimit - currentBufferBottom;
					CopyBuffer(currentBuffer, currentBufferBottom, buffer, offset, chunk);
					read += chunk;
					count -= chunk;
					offset += chunk;
					currentBufferBottom = currentBufferLimit;
				}
			}
			//}

			//if (checkStream == null)
			//{
			//    checkStream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
			//}
			//byte[] cbuf = new byte[count];
			//int checkRead = checkStream.Read(cbuf, 0, cbuf.Length);
			//if (checkRead != read)
			//{
			//    Console.WriteLine("!!! bytes read mismatch at " + fileName + ": "  + checkStream.Position + " / " + Position + ": " +
			//        "read = " + read + "; checkRead = " + checkRead);
			//}
			//else
			//{
			//    Console.WriteLine(">>> bytes read match at " + fileName + ": " + checkStream.Position + " / " + Position + ": " +
			//        "read = " + read + "; checkRead = " + checkRead);
			//}
			return read;

#else
			//if (done) return 0;
			if (overRead != null)  return overRead.Read(buffer, offset, count);
			if (Position + count <= length)
			{
////////					return base.Read(buffer, offset, count);

				//int readBytes;
				//bool readres;
				//fixed (byte* alignedBuffer = mBuffer)
				//{
				//        readres = ReadFile(handle, new IntPtr(alignedBuffer), blockSize, out readBytes, IntPtr.Zero);
				//}
				//return readBytes;

#if SYNCHRONOUS
				int readBytes;
				bool readres;
				readres = ReadFile(handle, alignedBuffer, blockSize, out readBytes, IntPtr.Zero);
				if (!readres) return 0;

				fixed (byte* amBuffer = mBuffer)
				{
					int* abIn = (int*)alignedBuffer.ToPointer();
					int* abOut = (int*)amBuffer;
					int* abOutEnd = (int*)(abOut + ((readBytes + 7) >> 2));
					while (abOut != abOutEnd)
					{
						*abOut = *abIn;
						abOut++;
						abIn++;
					}
				}
				return readBytes;
#else

#if ASYNC_BAD
				int readBytes;

				if (overlappedIndex < 0)
				{
					overlappedIndex = 0;
					overlappeds[overlappedIndex].Offset = (uint)(totalRead & 0xFFFFFFFF);
					overlappeds[overlappedIndex].OffsetHigh = (uint)(totalRead >> 32);
					overlappeds[overlappedIndex].hEvent = IntPtr.Zero;
					ReadFile(handle, alignedBuffer[overlappedIndex], blockSize,
						out readBytes,
						ref overlappeds[overlappedIndex]);
					totalRead += blockSize;
				}

				bool result = GetOverlappedResult(handle,
					alignedBuffer[overlappedIndex],
					ref overlappeds[overlappedIndex], out readBytes, true);

				Console.WriteLine("read: " + readBytes);
				if (readBytes == 0) return 0;

				IntPtr inBuffer = alignedBuffer[overlappedIndex];

				overlappedIndex = (overlappedIndex + 1) % 2;
				overlappeds[overlappedIndex].Offset = (uint)(totalRead & 0xFFFFFFFF);
				overlappeds[overlappedIndex].OffsetHigh = (uint)(totalRead >> 32);
				overlappeds[overlappedIndex].hEvent = IntPtr.Zero;
				ReadFile(handle, alignedBuffer[overlappedIndex], blockSize,
					out readBytes,
					ref overlappeds[overlappedIndex]);
				totalRead += blockSize;

				fixed (byte* amBuffer = mBuffer)
				{
					int* abIn = (int*)inBuffer.ToPointer();
					int* abOut = (int*)amBuffer;
					int* abOutEnd = (int*)(abOut + ((readBytes + 7) >> 2));
					while (abOut != abOutEnd)
					{
						*abOut = *abIn;
						abOut++;
						abIn++;
					}
				}
				return readBytes;
#else
				IntPtr inBuffer = GetBuffer();

				fixed (byte* amBuffer = mBuffer)
				{
					int* abIn = (int*)inBuffer.ToPointer();
					int* abOut = (int*)amBuffer;
					int* abOutEnd = (int*)(abOut + ((blockSize + 7) >> 2));
					while (abOut != abOutEnd)
					{
						*abOut = *abIn;
						abOut++;
						abIn++;
					}
				}
				return blockSize;
#endif
#endif
			}
			if (Position >= length)
			{
				return 0;
			}
			// Unbuffered reads *must* be aligned to the sector size (assumed as 512, here).
			// This will use a padded file to read the correct amount, but otherwise will
			// have to truncate the read.
			if (count == 0)  return 0;
			int remaining = (int)(length - Position);
			int paddedRemaining = (int)(base.Length - Position);
			//Console.WriteLine("Partial read -  requested " + count + ", left: " + remaining +
			//	" [" + paddedRemaining + "]");
			int res = paddedRemaining;
			if (res == 0)  return 0;
			//bool resized = false;
			if (res % sectorSize != 0)
			{
				//// 1. just truncate:
				//res = (int)((res / sectorSize) * sectorSize);

				//// 2. can't resize, we've locked it:
				//ResizeFile(fileName, ((length / sectorSize + 1) * sectorSize));

				//// 3. total hack:
				int first = (int)((res / sectorSize) * sectorSize);
				if (first != 0)
				{
					Read(buffer, offset, first);
					offset += first;
					count -= first;
				}
				long pos = Position;
				//Console.WriteLine("  pos: " + pos);
				Close();
				overRead = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
				overRead.Seek(pos, SeekOrigin.Begin);
				return first + Read(buffer, offset, count);
			}
			//Console.WriteLine("         req: " + res);
			if (res == 0)  return 0;
			res = Math.Min(res, Read(buffer, offset, res));
			//Console.WriteLine("        read: " + res);
			//if (resized)
			//{
			//	ResizeFile(fileName, length);
			//}
			return Math.Min(res, remaining);
#endif
		}

		//private FileStream checkStream = null;

		//private unsafe byte* current = null;

		/// <summary>
		/// Read a block of bytes from the stream and advance the position.
		/// </summary>
		/// <param name="buffer">returns a pointer to a (pinned) buffer of bytes read from the stream</param>
		/// <returns>
		/// the number of bytes read - this will be positive unless the end of
		/// stream has been reached, in which case it will be 0
		/// </returns>
		/// <remarks>The buffer returned here may be shared across calls to this method.</remarks>
		public unsafe int Read(out byte* buffer)
		{
			if (currentBufferLimit == currentBufferBottom)
			{
				GetBuffer();
				//if (gotDone)
				//{
				//	//Console.WriteLine("(gotDone : currentBufferBottom = " + currentBufferBottom +
				//	//	", currentBufferLimit = " + currentBufferLimit + ")");
				//	return 0;
				//}
				//if (currentBufferLimit == 0)  return 0;
			}
			buffer = ((byte*)currentBuffer.ToPointer()) + currentBufferBottom;
			int count = currentBufferLimit - currentBufferBottom;
			currentBufferBottom = currentBufferLimit;
			return count;
		}


		// TODO: make ReadByte() fast! ***

//		private byte[] readByteBuffer = new byte[512];
//		private int readByteBufferPos = 0;
//		private int readByteBufferCount = 0;

		/// <summary>
		/// Retrieve the next byte in the stream and advance the position.
		/// </summary>
		/// <returns>the next byte, or -1 if at end of stream</returns>
		/// <remarks>This is not as efficient as block reading, because of overhead issues.</remarks>
		public override int ReadByte()
		{
			//checkStream.ReadByte();  // check??
			if (currentBufferLimit == currentBufferBottom)
			{
				GetBuffer();
			}
			if (currentBufferLimit == 0)  return -1;
			// note the (post)increment of currentBufferBottom:
			unsafe
			{
				return *(((byte*)currentBuffer.ToPointer()) + currentBufferBottom++);
			}
			//// Marshal is incredibly slow!!
			//return Marshal.ReadByte(currentBuffer, currentBufferBottom++);
		}

		/// <summary>
		/// Retrieve the next byte in the stream, without advancing the position.
		/// </summary>
		/// <returns>the next byte, or -1 if at end of stream</returns>
		/// <remarks>This is not as efficient as block reading, because of overhead issues.</remarks>
		public int Peek()
		{
			if (currentBufferLimit == currentBufferBottom)
			{
				GetBuffer();
			}
			if (currentBufferLimit == 0)  return -1;
			unsafe
			{
				return *(((byte*)currentBuffer.ToPointer()) + currentBufferBottom);
			}
			//// Marshal is incredibly slow!!
			//return Marshal.ReadByte(currentBuffer, currentBufferBottom);
		}


		/// <summary>
		/// Check if the end of file has been reached.
		/// </summary>
		/// <returns>true if no more bytes remain; false otherwise</returns>
		public bool Eof()
		{
			if (currentBufferBottom < currentBufferLimit)  return false;
			//if (gotDone)  return true;
			// is this good enough? We want to be cheap...
			return (Position < Length);
		}
		
		#endregion

		#endregion


		#region Handle Manipulation

//#if DOTNET2
//		/// <summary>
//		/// Get the handle for the underlying file.
//		/// </summary>
//		public override Microsoft.Win32.SafeHandles.SafeFileHandle SafeFileHandle
//		{
//			get
//			{
//				return base.SafeFileHandle;
//			}
//		}
//#else
//		/// <summary>
//		/// Get the handle for the underlying file.
//		/// </summary>
//		public override System.IntPtr Handle
//		{
//			get
//			{
//				return base.Handle;
//			}
//		}
//#endif

		#endregion


		#region Unsupported Members

#if !UNBUFFERED_AS_STREAM
		/// <summary>
		/// Get whether the stream was opened asynchronously. Always false, but this does not
		/// matter for managed code.
		/// </summary>
		public override bool IsAsync
		{
			get
			{
				return false;
				//return base.IsAsync;
			}
		}
#endif

		/// <summary>
		/// Write all pending data. This method does nothing.
		/// </summary>
		public override void Flush()
		{
		}

		/// <summary>
		/// Set the length of the file - not supported.
		/// </summary>
		/// <param name="value">the length that the file will not be set to</param>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override void SetLength(long value)
		{
			throw new NotSupportedException("UnbufferedStream cannot set the file length.");
		}

		/// <summary>
		/// Write a section of buffer to the stream - not supported.
		/// </summary>
		/// <param name="buffer">the buffer that will not be written</param>
		/// <param name="offset">the offset in buffer at which to not start writing</param>
		/// <param name="count">the number of bytes to not write</param>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException("UnbufferedStream cannot write.");
		}

		#endregion
	}


#if UNBUFFEREDREADER
	/// <summary>
	/// StreamReader using unbuffered I/O for efficient reading from fast disk arrays.
	/// </summary>
	public sealed class UnbufferedStreamReader : StreamReader
	{
		private readonly UnbufferedStream stream;
		private Encoding encoding;
		private Decoder decoder;
		private byte* byteBuffer;
		private int byteCount = 0;
		private int bytePos = 0;
		private char[] charBuffer;
		private int charLen = 0;
		private int charPos = 0;

		public UnbufferedStreamReader(string fileName)
			: this(new UnbufferedStream(fileName))
		{
		}

		public UnbufferedStreamReader(UnbufferedStream stream)
			: base(stream)
		{
			this.stream = stream;
			Peek();
			encoding = CurrentEncoding;
			buffer = null;
			bufferCount = 0;
			Init();
		}

		private void Init()
		{
			this.decoder = encoding.GetDecoder();
			byteBuffer = null;
			byteCount = 0;
			charBuffer = null;
			this.byteLen = 0;
			this.bytePos = 0;
			//this._isBlocked = false;
			//this._closable = true;
		}

 
		/// <summary>Reads the next character from the input stream and advances the character position by one character.</summary>
		/// <returns>The next character from the input stream represented as an <see cref="T:System.Int32"></see> object, or -1 if no more characters are available.</returns>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		public override int Read()
		{
			if (stream == null) throw new IOException("Reader is closed.");
			if ((this.charPos == this.charLen) && (this.ReadBuffer() == 0))
			{
				return -1;
			}
			int num1 = this.charBuffer[this.charPos];
			this.charPos++;
			return num1;
		}



		/// <summary>Returns the next available character but does not consume it.</summary>
		/// <returns>The next character to be read, or -1 if no more characters are available or the stream does not support seeking.</returns>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		public override int Peek()
		{
			if (stream == null) throw new IOException("Reader is closed.");
			if (charPos < charLen)
			{
				return charBuffer[charPos];
			}
			if ((this.charPos != this.charLen) || (!this._isBlocked && (this.ReadBuffer() != 0)))
			{
				return this.charBuffer[this.charPos];
			}
			return -1;
		}




		/// <summary>Reads a maximum of count characters from the current stream into buffer, beginning at index.</summary>
		/// <returns>The number of characters that have been read, or 0 if at the end of the stream and no data was read. The number will be less than or equal to the count parameter, depending on whether the data is available within the stream.</returns>
		/// <param name="count">The maximum number of characters to read. </param>
		/// <param name="buffer">When this method returns, contains the specified character array with the values between index and (index + count - 1) replaced by the characters read from the current source. </param>
		/// <param name="index">The index of buffer at which to begin writing. </param>
		/// <exception cref="System.ArgumentNullException">buffer is null. </exception>
		/// <exception cref="System.ArgumentOutOfRangeException">index or count is negative. </exception>
		/// <exception cref="System.ArgumentException">The buffer length minus index is less than count. </exception>
		/// <exception cref="System.IO.IOException">An I/O error occurs, such as the stream is closed. </exception>
		public override int Read([In, Out] char[] buffer, int index, int count)
		{
			if (stream == null) throw new IOException("Reader is closed.");
			if (buffer == null)
			{
				throw new ArgumentNullException("buffer", Environment.GetResourceString("ArgumentNull_Buffer"));
			}
			if ((index < 0) || (count < 0))
			{
				throw new ArgumentOutOfRangeException((index < 0) ? "index" : "count", Environment.GetResourceString("ArgumentOutOfRange_NeedNonNegNum"));
			}
			if ((buffer.Length - index) < count)
			{
				throw new ArgumentException(Environment.GetResourceString("Argument_InvalidOffLen"));
			}
			int num1 = 0;
			bool flag1 = false;
			while (count > 0)
			{
				int num2 = this.charLen - this.charPos;
				if (num2 == 0)
				{
					num2 = this.ReadBuffer(buffer, index + num1, count, out flag1);
				}
				if (num2 == 0)
				{
					return num1;
				}
				if (num2 > count)
				{
					num2 = count;
				}
				if (!flag1)
				{
					Buffer.InternalBlockCopy(this.charBuffer, this.charPos * 2, buffer, (index + num1) * 2, num2 * 2);
					this.charPos += num2;
				}
				num1 += num2;
				count -= num2;
				if (this._isBlocked)
				{
					return num1;
				}
			}
			return num1;
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
		/// <exception cref="System.ObjectDisposedException">The <see cref="T:System.IO.TextReader"></see> is closed. </exception>
		public virtual int ReadBlock([In, Out] char[] buffer, int index, int count)
		{
			int num1;
			int num2 = 0;
			do
			{
				num2 += num1 = this.Read(buffer, index + num2, count - num2);
			}
			while ((num1 > 0) && (num2 < count));
			return num2;
		}




		/// <summary>Reads a line of characters from the current stream and returns the data as a string.</summary>
		/// <returns>The next line from the input stream, or null if the end of the input stream is reached.</returns>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		/// <exception cref="System.OutOfMemoryException">There is insufficient memory to allocate a buffer for the returned string. </exception>
		public override string ReadLine()
		{
			if (stream == null) throw new IOException("Reader is closed.");
			if ((this.charPos == this.charLen) && (this.ReadBuffer() == 0))
			{
				return null;
			}
			StringBuilder builder1 = null;
			while (true)
			{
				int num1 = this.charPos;
				do
				{
					char ch1 = this.charBuffer[num1];
					if ((ch1 == '\r') || (ch1 == '\n'))
					{
						string text1;
						if (builder1 != null)
						{
							builder1.Append(this.charBuffer, this.charPos, num1 - this.charPos);
							text1 = builder1.ToString();
						}
						else
						{
							text1 = new string(this.charBuffer, this.charPos, num1 - this.charPos);
						}
						this.charPos = num1 + 1;
						if (((ch1 == '\r') && ((this.charPos < this.charLen) || (this.ReadBuffer() > 0))) && (this.charBuffer[this.charPos] == '\n'))
						{
							this.charPos++;
						}
						return text1;
					}
					num1++;
				}
				while (num1 < this.charLen);
				num1 = this.charLen - this.charPos;
				if (builder1 == null)
				{
					builder1 = new StringBuilder(num1 + 80);
				}
				builder1.Append(this.charBuffer, this.charPos, num1);
				if (this.ReadBuffer() <= 0)
				{
					return builder1.ToString();
				}
			}
		}



		/// <summary>Reads the stream from the current position to the end of the stream.</summary>
		/// <returns>The rest of the stream as a string, from the current position to the end. If the current position is at the end of the stream, returns the empty string("").</returns>
		/// <exception cref="System.IO.IOException">An I/O error occurs. </exception>
		/// <exception cref="System.OutOfMemoryException">There is insufficient memory to allocate a buffer for the returned string. </exception>
		public override string ReadToEnd()
		{
			if (stream == null) throw new IOException("Reader is closed.");
			StringBuilder builder1 = new StringBuilder(this.charLen - this.charPos);
			do
			{
				builder1.Append(this.charBuffer, this.charPos, this.charLen - this.charPos);
				this.charPos = this.charLen;
				this.ReadBuffer();
			}
			while (this.charLen > 0);
			return builder1.ToString();
		}


		private int ReadBuffer()
		{
			byteCount = stream.Read(out byteBuffer);
			int maxCharsPerBuffer = encoding.GetMaxCharCount(byteCount);
			if (charBuffer == null || charBuffer.Length < maxCharsPerBuffer)
			{
				charBuffer = new char[maxCharsPerBuffer];
			}

			charLen = 0;
			charPos = 0;
		}

 


		/// <summary>Closes the underlying stream, releases the unmanaged resources used by the <see cref="System.IO.StreamReader"></see>, and optionally releases the managed resources.</summary>
		/// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources. </param>
		protected override void Dispose(bool disposing)
		{
			try
			{
				base.Dispose(disposing);
			}
			finally
			{
				this.stream = null;
				this.encoding = null;
			}
		}
	}
#endif

}

