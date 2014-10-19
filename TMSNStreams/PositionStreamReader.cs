using System;
using System.IO;

namespace Microsoft.TMSN.IO
{
#if !ENABLE_POSITIONSTREAMREADER
	// This is just not currently correct or performant enough to have around...
	// It could potentially be fixed up.
#else
	/// <summary>
	/// A StreamReader that supports reporting of the exact byte position.
	/// </summary>
	/// <remarks>
	/// This is actually currently a TextReader, which should be changed!
	/// There may be other rough edges.
	/// </remarks>
	public class PositionStreamReader : TextReader
	{
		private readonly string path;
		private readonly Stream stream;
		private static int bufferByteSize = 32 * 1024;
		private int bufferFillSize = 0;
		private byte[] buffer = new byte[bufferByteSize];

		long currBufferPos = 0;

		// relative to the lines read (not the what's read into the buffer)
		long currFilePos = 0;

		/// <summary>
		/// Create a PositionStreamReader for the specified file.
		/// </summary>
		/// <param name="filename">The path of the file to open</param>
		public PositionStreamReader(string filename)
		{
			path = filename;
			stream = ZStreamIn.Open(filename);
		}

		/// <summary>
		/// Create a PositionStreamReader for the specified Stream.
		/// </summary>
		/// <param name="stream">The Stream to back this reader</param>
		public PositionStreamReader(Stream stream)
		{
			path = null;
			this.stream = stream;
		}

		/// <summary>
		/// Read a single byte from the input file stream.
		/// </summary>
		/// <returns>The byte that was read, or -1 if at end of stream</returns>
		public override int Read()
		{
			// if we emptied the buffer, read new buffer, if empty return -1
			if (currBufferPos == bufferFillSize && ReadBuffer() == 0)
			{
				return -1;
			}
			
			currFilePos++;
			return (int)buffer[currBufferPos++];
		}

		private int ReadBuffer()
		{
			currBufferPos = 0;
			long pos = stream.Position;
			long len = stream.Length;
			if (len - pos < bufferByteSize)
			{
				bufferFillSize = stream.Read(buffer, 0, (int)(len-pos));
			}
			else
			{
				bufferFillSize = stream.Read(buffer, 0, bufferByteSize);
			}

			return bufferFillSize;
		}
		
		/// <summary>
		/// Get the current byte position in the file.
		/// </summary>
		public long Position
		{
			get
			{
				return currFilePos;
			}
		}
		/// <summary>
		/// Get the current byte position in the file (alias for Position).
		/// </summary>
		public long FilePosition
		{
			get { return Position; }
		}

		/// <summary>
		/// Read the next byte without advancing the position.
		/// </summary>
		/// <returns>The byte that was read, or -1 if at end of stream</returns>
		public override int Peek() 
		{
			if (currBufferPos == bufferFillSize)
			{
				return -1;
			}
			return (int) buffer[currBufferPos];
		}

		/// <summary>
		/// Seek to the byte position within the file.
		/// </summary>
		/// <param name="position">the location, relative to the start of the stream</param>
		public void Seek(long position)
		{
			Seek(position, SeekOrigin.Begin);
		}
		/// <summary>
		/// Seek to the byte position within the file.
		/// </summary>
		/// <param name="position">the location, relative to the origin</param>
		/// <param name="origin">the location to take the offset from</param>
		public void Seek(long position, SeekOrigin origin) 
		{
			stream.Seek(position, origin);

			ReadBuffer();

			switch (origin)
			{
				case SeekOrigin.Begin:
					currFilePos = position;
					break;

				case SeekOrigin.End:
					currFilePos = stream.Length + position;
					break;

				case SeekOrigin.Current:
					currFilePos += position;
					break;

				default:
					break;
			}
		}
	}

#endif

}

