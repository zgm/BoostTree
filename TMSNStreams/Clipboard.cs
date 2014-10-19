// owner: rragno

using System;
using System.IO;
using System.Collections;
using System.Collections.Specialized;
using System.Text;
using System.Threading;
using System.Runtime.InteropServices;
using System.Windows.Forms;


namespace Microsoft.TMSN.IO
{

	/// <summary>
	/// Wrapper Stream to perform Clipboard reading.
	/// </summary>
	public class ClipboardReadStream : Stream
	{
		private readonly MemoryStream str;

		private static readonly string[] textFormats = new string[]
			{
				DataFormats.UnicodeText,
				DataFormats.OemText,
				DataFormats.Text,
				DataFormats.StringFormat,
				DataFormats.CommaSeparatedValue,
				DataFormats.Html,
				DataFormats.Rtf
			};

		internal static bool IsClipboardStream(string fileName)
		{
			if (fileName == null || fileName.Length == 0)  return false;
			if (string.Compare(fileName, 0, "clip:", 0, "clip:".Length, true) != 0)  return false;
			if (fileName.Length == "clip:".Length)  return true;
			int i = "clip:".Length;
			while (i < fileName.Length && (fileName[i] == '\\' || fileName[i] == '/'))
			{
				i++;
			}
			int end = fileName.Length - 1;
			if (fileName[end] == ':')  end--;
			if (i == end + 1)  return true;
			if (end - i + 1 != "clip".Length)  return false;
			return string.Compare(fileName, i, "clip", 0, end - i + 1, true) == 0;
		}

		/// <summary>
		/// Create a stream to read from the system clipboard.
		/// The contents will be coerced to text.
		/// </summary>
		/// <exception cref="FileNotFoundException">The clipboard is empty or cannot be coerced to text.</exception>
		public ClipboardReadStream()
			: this(null)
		{
		}
		/// <summary>
		/// Create a stream to read from the system clipboard.
		/// The contents will be coerced to text.
		/// </summary>
		/// <param name="format">the format to request from the clipboard, such as those
		/// in <see cref="System.Windows.Forms.DataFormats"/>.</param>
		/// <remarks>
		/// Setting the format can change the data that is exposed. For example, if HTML is
		/// on the clipboard, the extracted text will normally be retrieved, but a format of
		/// <see cref="System.Windows.Forms.DataFormats.Html"/> will give the raw HTML.
		/// </remarks>
		/// <exception cref="FileNotFoundException">The clipboard is empty or cannot be coerced to text.</exception>
		public ClipboardReadStream(string format)
		{
			IDataObject data;
			int retry = 0;
			while (true)
			{
				try
				{
					data = Clipboard.GetDataObject();
					break;
				}
				catch (System.Runtime.InteropServices.ExternalException ex)
				{
					if (retry >= 10)
					{
						throw new FileNotFoundException("Cannot access the clipboard.", ex);
					}
					Thread.Sleep(retry * 30);
				}
				retry++;
			}

			string text = null;
			if (format != null)
			{
				// clean the format, if needed:
				format = format.Trim();
				string m = format.Replace(" ", "");
				for (int i = 0; i < textFormats.Length; i++)
				{
					string f = textFormats[i];
					if (string.Compare(m, f.Replace(" ", ""), true) == 0)
					{
						format = f;
						break;
					}
				}

				text = (string)data.GetData(format, true);
			}
			else
			{
				foreach (string f in textFormats)
				{
					text = (string)data.GetData(f, true);
					if (text != null)  break;
				}
				// try filedrop, if all else fails...
				if (text == null)
				{
					string[] fd = (string[])data.GetData(DataFormats.FileDrop, true);
					if (fd != null && fd.Length != 0)
					{
						for (int i = 0; i < fd.Length; i++)
						{
							if (fd[i].IndexOf(' ') >= 0 &&
								(fd[i][0] != '"' || fd[i][fd[i].Length] != '"'))
							{
								fd[i] = "\"" + fd[i] + "\"";
							}
						}
						//text = string.Join(" ", fd);
						text = string.Join("\n", fd);
					}
				}
			}
			if (text == null)
			{
				throw new FileNotFoundException("No text data on the clipboard.");
			}

			str = new MemoryStream(ZStreamWriter.UTF8Lenient.GetBytes(text));
		}


		/// <summary>
		/// Determine if there is text data on the clipboard.
		/// </summary>
		/// <returns>true if the clipboard has data that can be coerced to text, false otherwise</returns>
		public static bool FileExists()
		{
			try
			{
				IDataObject data;
				try
				{
					data = Clipboard.GetDataObject();
				}
				catch
				{
					try
					{
						data = Clipboard.GetDataObject();
					}
					catch
					{
						return false;
					}
				}

				foreach (string f in textFormats)
				{
					if (data.GetDataPresent(f))  return true;
				}
				// try filedrop, if all else fails...
				string[] fd = (string[])data.GetData(DataFormats.FileDrop, true);
				if (fd != null && fd.Length != 0)
				{
					return true;
				}
				return false;
			}
			catch
			{
				return false;
			}
		}

		/// <summary>
		/// Determine if there is data of the specified format on the clipboard.
		/// </summary>
		/// <param name="format">the format to check for, such as those in
		/// <see cref="System.Windows.Forms.DataFormats.Html"/></param>
		/// <returns>true if the clipboard has data of the given format, false otherwise</returns>
		public static bool FileExists(string format)
		{
			try
			{
				IDataObject data;
				try
				{
					data = Clipboard.GetDataObject();
				}
				catch
				{
					try
					{
						data = Clipboard.GetDataObject();
					}
					catch
					{
						return false;
					}
				}

				// clean the format, if needed:
				format = format.Trim();
				string m = format.Replace(" ", "");
				for (int i = 0; i < textFormats.Length; i++)
				{
					string f = textFormats[i];
					if (string.Compare(m, f.Replace(" ", ""), true) == 0)
					{
						format = f;
						break;
					}
				}

				return data.GetDataPresent(format);
			}
			catch
			{
				return false;
			}
		}

		/// <summary>
		/// Get the length of the data, in bytes.
		/// </summary>
		public override long Length
		{
			get
			{
				return str.Length;
			}
		}


		/// <summary>
		/// Get whether the stream can seek. Should be true.
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				return str.CanSeek;
			}
		}

		/// <summary>
		/// Get whether the stream can read - true.
		/// </summary>
		public override bool CanRead
		{
			get
			{
				return true;
			}
		}

		
		/// <summary>
		/// Get whether the stream can write - false.
		/// </summary>
		public override bool CanWrite
		{
			get
			{
				return false;
			}
		}

		/// <summary>
		/// Close the stream for further reading.
		/// </summary>
		public override void Close()
		{
			str.Close();
		}

		/// <summary>
		/// Seek to a new position in the data, in bytes.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public virtual long Seek(long offset)
		{
			return Seek(offset);
		}
		/// <summary>
		/// Seek to a new position in the data, in bytes.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			return str.Seek(offset, origin);
		}
		/// <summary>
		/// Get or Set the position in the data, in bytes.
		/// </summary>
		public override long Position
		{
			get
			{
				return str.Position;
			}
			set
			{
				str.Position = value;
			}
		}

		/// <summary>
		/// Begin an asynchronous read.
		/// </summary>
		/// <param name="buffer">the buffer to read into</param>
		/// <param name="offset">The zero based byte offset in <paramref name="array" /> at which to begin reading.</param>
		/// <param name="count">The maximum number of bytes to read.</param>
		/// <param name="callback">The method to be called when the asynchronous read operation is completed.</param>
		/// <param name="state">A user-provided object that distinguishes this particular asynchronous read request from other requests.</param>
		/// <returns>
		/// An <see cref="System.IAsyncResult" /> that references the asynchronous read.
		/// </returns>
		public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			return str.BeginRead(buffer, offset, count, callback, state);
		}


		/// <summary>
		/// Not supported.
		/// </summary>
		/// <param name="buffer">The buffer to write data to.</param>
		/// <param name="offset">The zero based byte offset in <paramref name="array" /> at which to begin writing.</param>
		/// <param name="count">The maximum number of bytes to write.</param>
		/// <param name="callback">The method to be called when the asynchronous write operation is completed.</param>
		/// <param name="state">A user-provided object that distinguishes this particular asynchronous write request from other requests.</param>
		/// <returns>
		/// An <see cref="System.IAsyncResult" /> that references the asynchronous write.
		/// </returns>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			throw new NotSupportedException("ClipboardReadStream cannot write.");
		}

		/// <summary>
		/// End an asynchronous read.
		/// </summary>
		/// <param name="asyncResult">the result object representing this read</param>
		/// <returns>the number of bytes read</returns>
		public override int EndRead(IAsyncResult asyncResult)
		{
			return str.EndRead(asyncResult);
		}

		/// <summary>
		/// Not supported.
		/// </summary>
		/// <param name="asyncResult"></param>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override void EndWrite(IAsyncResult asyncResult)
		{
			throw new NotSupportedException("ClipboardReadStream cannot write.");
		}

		/// <summary>
		/// Empty operation.
		/// </summary>
		public override void Flush()
		{
			
		}

		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <returns>the number of bytes read</returns>
		public override int Read(byte[] buffer, int offset, int count)
		{
			return str.Read(buffer, offset, count);
		}

		/// <summary>
		/// Read a single byte from the stream
		/// </summary>
		/// <returns>the byte read, or -1 if at end of stream</returns>
		public override int ReadByte()
		{
			return str.ReadByte();
		}

		/// <summary>
		/// Set the size of the stream - not supported.
		/// </summary>
		/// <param name="value">the length to not set</param>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override void SetLength(long value)
		{
			throw new NotSupportedException("ClipboardReadStream cannot write.");
		}

		/// <summary>
		/// Not supported.
		/// </summary>
		/// <param name="buffer"></param>
		/// <param name="offset"></param>
		/// <param name="count"></param>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException("ClipboardReadStream cannot write.");
		}

		/// <summary>
		/// Not supported.
		/// </summary>
		/// <param name="value">the byte to not write</param>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override void WriteByte(byte value)
		{
			throw new NotSupportedException("ClipboardReadStream cannot write.");
		}
	}



	/// <summary>
	/// Wrapper Stream to perform Clipboard writing.
	/// </summary>
	public class ClipboardWriteStream : Stream, IDisposable
	{
		private readonly MemoryStream str;
		private readonly string format;
		private bool stored = false;

		private static readonly string[] textFormats = new string[]
			{
				DataFormats.UnicodeText,
				DataFormats.OemText,
				DataFormats.Text,
				DataFormats.StringFormat,
				DataFormats.CommaSeparatedValue,
				DataFormats.Html,
				DataFormats.Rtf
			};

		/// <summary>
		/// Create a stream to write to the system clipboard.
		/// The data will be treated as text.
		/// </summary>
		public ClipboardWriteStream()
			: this(null)
		{
		}
		/// <summary>
		/// Create a stream to write to the system clipboard.
		/// The data will be treated as text.
		/// </summary>
		/// <param name="append">if true, append if compatable data exists; if false, overwrite</param>
		public ClipboardWriteStream(bool append)
			: this(null, append)
		{
		}
		/// <summary>
		/// Create a stream to write to the system clipboard.
		/// The data will be treated as text.
		/// </summary>
		/// <param name="format">the format to state for the clipboard, such as those
		/// in <see cref="System.Windows.Forms.DataFormats"/>.</param>
		/// <remarks>
		/// Setting the format can change the data that is stored. For example, if raw HTML is
		/// stored in the clipboard, the format will need to be set to
		/// <see cref="System.Windows.Forms.DataFormats.Html"/> to specify this to other
		/// applications.
		/// </remarks>
		private ClipboardWriteStream(string format)
			: this(format, false)
		{
		}
		/// <summary>
		/// Create a stream to write to the system clipboard.
		/// The data will be treated as text.
		/// </summary>
		/// <param name="format">the format to state for the clipboard, such as those
		/// in <see cref="System.Windows.Forms.DataFormats"/>.</param>
		/// <param name="append">if true, append if compatable data exists; if false, overwrite</param>
		/// <remarks>
		/// Setting the format can change the data that is stored. For example, if raw HTML is
		/// stored in the clipboard, the format will need to be set to
		/// <see cref="System.Windows.Forms.DataFormats.Html"/> to specify this to other
		/// applications.
		/// </remarks>
		private ClipboardWriteStream(string format, bool append)
		{
			if (format != null)
			{
				// clean the format, if needed:
				format = format.Trim();
				string m = format.Replace(" ", "");
				for (int i = 0; i < textFormats.Length; i++)
				{
					string f = textFormats[i];
					if (string.Compare(m, f.Replace(" ", ""), true) == 0)
					{
						format = f;
						break;
					}
				}
			}
			this.format = format;
			str = new MemoryStream();
			if (append)
			{
				try
				{
					using (ClipboardReadStream r = new ClipboardReadStream(format))
					{
						byte[] buffer = new byte[32768];
						for (int count = r.Read(buffer, 0, buffer.Length); count > 0; count = r.Read(buffer, 0, buffer.Length))
						{
							Write(buffer, 0, count);
						}
					}
				}
				catch
				{
					// ignore...
				}
			}
		}

		/// <summary>
		/// Get the length of the data, in bytes.
		/// </summary>
		public override long Length
		{
			get
			{
				return str.Length;
			}
		}


		/// <summary>
		/// Get whether the stream can seek. Should be true.
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				return str.CanSeek;
			}
		}

		/// <summary>
		/// Get whether the stream can read - false.
		/// </summary>
		public override bool CanRead
		{
			get
			{
				return false;
			}
		}

		
		/// <summary>
		/// Get whether the stream can write - true.
		/// </summary>
		public override bool CanWrite
		{
			get
			{
				return true;
			}
		}

		/// <summary>
		/// Close the stream for further writing and stores on the clipboard.
		/// </summary>
		/// <exception cref="InvalidOperationException">Cannot store the data to the clipboard.</exception>
		public override void Close()
		{
			if (!stored)
			{
				Flush();

				// do not close:
				////str.Close();
				GC.SuppressFinalize(this);
				stored = true;
			}
		}

		/// <summary>
		/// Close the stream for further writing and stores on the clipboard.
		/// </summary>
		/// <exception cref="InvalidOperationException">Cannot store the data to the clipboard.</exception>
		void IDisposable.Dispose()
		{
			Close();
		}

		/// <summary>
		/// Close the stream for further writing and stores on the clipboard.
		/// </summary>
		~ClipboardWriteStream()
		{
			try
			{
				Close();
			}
			catch
			{
				// ignore
			}
		}


		/// <summary>
		/// Seek to a new position in the data, in bytes.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public virtual long Seek(long offset)
		{
			return Seek(offset);
		}
		/// <summary>
		/// Seek to a new position in the data, in bytes.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			return str.Seek(offset, origin);
		}
		/// <summary>
		/// Get or Set the position in the data, in bytes.
		/// </summary>
		public override long Position
		{
			get
			{
				return str.Position;
			}
			set
			{
				str.Position = value;
			}
		}

		/// <summary>
		/// Not supported.
		/// </summary>
		/// <param name="buffer">the buffer to read into</param>
		/// <param name="offset">The zero based byte offset in <paramref name="array" /> at which to begin reading.</param>
		/// <param name="count">The maximum number of bytes to read.</param>
		/// <param name="callback">The method to be called when the asynchronous read operation is completed.</param>
		/// <param name="state">A user-provided object that distinguishes this particular asynchronous read request from other requests.</param>
		/// <returns>
		/// An <see cref="System.IAsyncResult" /> that references the asynchronous read.
		/// </returns>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			throw new NotSupportedException("ClipboardWriteStream cannot read.");
		}


		/// <summary>
		/// Not supported.
		/// </summary>
		/// <param name="buffer">The buffer to write data to.</param>
		/// <param name="offset">The zero based byte offset in <paramref name="array" /> at which to begin writing.</param>
		/// <param name="count">The maximum number of bytes to write.</param>
		/// <param name="callback">The method to be called when the asynchronous write operation is completed.</param>
		/// <param name="state">A user-provided object that distinguishes this particular asynchronous write request from other requests.</param>
		/// <returns>
		/// An <see cref="System.IAsyncResult" /> that references the asynchronous write.
		/// </returns>
		public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			// check for flush?
			for (int i = offset; i < offset + count; i++)
			{
				if (buffer[i] == (byte)'\n')
				{
					asyncFlush = true;
					break;
				}
			}
			return str.BeginWrite(buffer, offset, count, callback, state);
		}

		private bool asyncFlush = false;

		/// <summary>
		/// Not supported.
		/// </summary>
		/// <param name="asyncResult">the result object representing this read</param>
		/// <returns>the number of bytes read</returns>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override int EndRead(IAsyncResult asyncResult)
		{
			throw new NotSupportedException("ClipboardWriteStream cannot read.");
		}

		/// <summary>
		/// End an asynchronous write.
		/// </summary>
		/// <param name="asyncResult">
		/// An <see cref="System.IAsyncResult" /> that references the asynchronous write.
		/// </param>
		public override void EndWrite(IAsyncResult asyncResult)
		{
			str.EndWrite(asyncResult);
			if (asyncFlush)
			{
				asyncFlush = false;
				Flush(true);
			}
		}

		/// <summary>
		/// Copy the current contents to the clipboard, without closing the stream.
		/// </summary>
		/// <remarks>
		/// Because this method is often called haphazardly, it may be desirable to
		/// split this functionality.
		/// </remarks>
		/// <exception cref="InvalidOperationException">Cannot store the data to the clipboard.</exception>
		public override void Flush()
		{
			Flush(false);
		}
		private void Flush(bool toNewline)
		{
			//try
			//{
			long pos = str.Position;
			str.Position = 0;
			string s;
			// avoid closing or calling Dispose:
			StreamReader sr = new StreamReader(str, ZStreamWriter.UTF8Lenient);
			s = sr.ReadToEnd();
			str.Position = pos;
			if (toNewline && s.Length > 0 && s[s.Length - 1] != '\n')
			{
				int i = s.Length - 1;
				while (i >= 0 && s[i] != '\n') i--;
				if (i < 0)
				{
					s = "";
				}
				else
				{
					s = s.Substring(0, i + 1);
				}
			}

			DataObject data;
			if (format != null)
			{
				data = new DataObject();
				data.SetData(format, true, s);
			}
			else
			{
				data = new DataObject();  //DataFormats.UnicodeText, s);
				data.SetData(DataFormats.UnicodeText, true, s);

				//data.SetData(DataFormats.OemText, true, s);
				//data.SetData(DataFormats.Text, true, s);
				//data.SetData(DataFormats.StringFormat, true, s);
				////data.SetData(DataFormats.CommaSeparatedValue, true, s);
				////data.SetData(DataFormats.Html, true, s);
				////data.SetData(DataFormats.Rtf, true, s);
			}

			int retry = 0;
			while (true)
			{
				try
				{
					Clipboard.SetDataObject(data, true);
					stored = true;
					break;
				}
				catch (System.Runtime.InteropServices.ExternalException ex)
				{
					if (retry >= 10)
					{
						throw new InvalidOperationException("Cannot access the clipboard.", ex);
					}
					Thread.Sleep(retry * 30);
				}
				retry++;
			}
			//}
			//catch
			//{
			//	if (!stored)  throw;
			//}
		}

		/// <summary>
		/// Not supported.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <returns>the number of bytes read</returns>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override int Read(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException("ClipboardWriteStream cannot read.");
		}

		/// <summary>
		/// Not supported.
		/// </summary>
		/// <returns>the byte read, or -1 if at end of stream</returns>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override int ReadByte()
		{
			throw new NotSupportedException("ClipboardWriteStream cannot read.");
		}

		/// <summary>
		/// Set the size of the stream.
		/// </summary>
		/// <param name="value">the length to set, in bytes</param>
		public override void SetLength(long value)
		{
			if (value != str.Length)
			{
				str.SetLength(value);
				stored = false;
			}
		}

		/// <summary>
		/// Write data from the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to take the data from</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the number of bytes to write</param>
		public override void Write(byte[] buffer, int offset, int count)
		{
			bool hasNewline = false;
			for (int i = offset; i < offset + count; i++)
			{
				if (buffer[i] == (byte)'\n')
				{
					hasNewline = true;
					break;
				}
			}
			str.Write(buffer, offset, count);
			stored = false;
			if (hasNewline) Flush(true);
		}

		/// <summary>
		/// Write a byte.
		/// </summary>
		/// <param name="value">the byte to write</param>
		public override void WriteByte(byte value)
		{
			str.WriteByte(value);
			stored = false;
			if (value == (byte)'\n') Flush(true);
		}
	}


}

