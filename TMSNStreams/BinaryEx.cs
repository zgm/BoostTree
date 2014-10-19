using System;
using System.IO;
using System.Collections;
#if DOTNET2
using System.Collections.Generic;
#endif
using System.Text;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;


namespace Microsoft.TMSN.IO
{
	// should the serializable object be marked with attributes? Implement an interface?
	// Is ISerializable worth anything? Serializing is easy, but deserializing has a
	// problem with the strong typing. Also, how should the deserializing be done? A static
	// factory method might be best, but it is quite awkward to specify and requires either
	// reflection or dynamic code generation. An instance method requires that a null
	// constructor be used. A constructor that takes a BinaryReaderEx might be nice, but
	// no interface or base class can constrain that. ***

	#region Interfaces

	/// <summary>
	/// Interface to enable a class to be attractively deserialized from a BinaryReaderEx.
	/// </summary>
	/// <remarks>
	/// This isn't really that attractive a pattern, and a constructor is probably cleaner.
	/// The BinaryReaderEx.Read(Type type) method makes that reasonably clean.
	/// </remarks>
	public interface IBinaryReadable
	{
		/// <summary>
		/// Read into the current instance.
		/// </summary>
		/// <param name="input">The reader to read from</param>
		void Deserialize(BinaryReaderEx input);
	}
	/// <summary>
	/// Interface to enable a class to be attractively serialized to a BinaryWriterEx.
	/// </summary>
	public interface IBinaryWritable
	{
		/// <summary>
		/// Write the current instance out.
		/// </summary>
		/// <param name="output">The writer to write to</param>
		void Serialize(BinaryWriterEx output);
	}
	/// <summary>
	/// Interface to enable a class to be attractively serialized to a BinaryWriterEx
	/// and deserialized from a BinaryReaderEx.
	/// </summary>
	public interface IBinarySerializable : IBinaryReadable, IBinaryWritable
	{
	}

	#endregion



	/// <summary>
	/// BinaryReader extended to handle additional data structures.
	/// </summary>
	/// <remarks>
	/// Autodetection of options would be nice, but it would interfere with the statelessness.
	/// Perhaps a special constructor could be made that would read a header?
	/// </remarks>
	public sealed class BinaryReaderEx : BinaryReader
	{
		// defaulting this to true is a big change! It breaks backwards compatability... Should it be done??
		private bool packLengths = false;
		//private bool packBooleans = false;
		//private bool closeStream = false;

		// the transformations here should optimally be done in larger batches,,, ***

		#region Constructors

		/// <summary>
		/// Create a new BinaryReaderEx based on the given filename, with an implicit call
		/// to ZStreamIn.Open().
		/// </summary>
		/// <param name="input">The name of the file to open</param>
		public BinaryReaderEx(string input)
			: this(ZStreamIn.Open(input))
		{
			//closeStream = true;
		}

		/// <summary>
		/// Create a new BinaryReaderEx based on the given filename and encoding, with an implicit call
		/// to ZStreamIn.Open().
		/// </summary>
		/// <param name="input">The name of the file to open</param>
		/// <param name="enc">The encoding to use for text objects</param>
		public BinaryReaderEx(string input, Encoding enc)
			: this(ZStreamIn.Open(input), enc)
		{
			//closeStream = true;
		}

		/// <summary>
		/// Create a new BinaryReaderEx based on the given stream.
		/// </summary>
		/// <param name="input">The stream to read from</param>
		public BinaryReaderEx(Stream input)
			: base(input)
		{
		}

		/// <summary>
		/// Create a new BinaryReaderEx based on the given stream and encoding.
		/// </summary>
		/// <param name="input">The stream to read from</param>
		/// <param name="enc">The encoding to use for text objects</param>
		public BinaryReaderEx(Stream input, Encoding enc)
			: base(input, enc)
		{
			encoding = enc;
		}

		/// <summary>
		/// Create a new BinaryReaderEx based on the given BinaryReader.
		/// </summary>
		/// <param name="input">The BinaryReader to read from</param>
		public BinaryReaderEx(BinaryReader input)
			: this(input.BaseStream)
		{
		}

		/// <summary>
		/// Create a new BinaryReaderEx based on the given BinaryReader and encoding.
		/// </summary>
		/// <param name="input">The BinaryReader to read from</param>
		/// <param name="enc">The encoding to use for text objects</param>
		public BinaryReaderEx(BinaryReader input, Encoding enc)
			: this(input.BaseStream, enc)
		{
		}

		#endregion

		//		private static Encoding RipEncoding(BinaryReader reader)
		//		{
		//			Type type = typeof(BinaryReader);
		//			FieldInfo field = type.GetField("m_decoder", BindingFlags.NonPublic);
		//			Decoder decoder = (Decoder)field.GetValue(reader);
		//			decoder does not expose the Encoding!!
		//		}

		private readonly Encoding encoding = System.Text.Encoding.UTF8;

		/// <summary>
		/// Get the encoding used for any character input.
		/// </summary>
		public Encoding Encoding
		{
			get { return encoding; }
		}

		#region Packing

		/// <summary>
		/// Get or set whether to pack the lengths in a 7-bit multibyte encoding,
		/// to save space. This is false by default, and it must be matched by the reader.
		/// </summary>
		public bool PackLengths
		{
			get { return packLengths; }
			set { packLengths = value; }
		}

		/// <summary>
		/// Read a possibly packed integer, based on the PackLengths value.
		/// </summary>
		/// <returns>A 32-bit integer</returns>
		private int ReadInt()
		{
			if (packLengths)
			{
				return Read7BitEncodedInt();
			}
			else
			{
				return ReadInt32();
			}
		}

		/// <summary>
		/// Reads an integer from the stream that was written using WritePacked(int).
		/// </summary>
		/// <returns>A 32-bit integer</returns>
		public int ReadInt32Packed()
		{
			/*
			byte b = ReadByte();
			if (b == 0) return 0;

			int result=0;
			while (true)
			{
				if ((b & 0x80) != 0)
				{
					result |= (b & 0x7F);
					result <<= 7;
					b = ReadByte();
				}
				else
				{
					result |= b;
					break;
				}
			}
			return result;
			*/
			return Read7BitEncodedInt();
		}

		/// <summary>
		/// Reads a float in compressed format, that was written using <see cref="BinaryWriterEx.WritePacked(float)"/>.
		/// </summary>
		/// <returns>A float</returns>
		public float ReadSinglePacked()
		{
			sbyte specialByte = ReadSByte();
			if (specialByte != sbyte.MinValue)
			{
				return (float)specialByte;
			}
			else
			{
				return ReadSingle();
			}
		}

		/// <summary>
		/// Reads a double in compressed format, that was written using <see cref="BinaryWriterEx.WritePacked(double)"/>.
		/// </summary>
		/// <returns>A double</returns>
		public double ReadDoublePacked()
		{
			sbyte specialByte = ReadSByte();
			if (specialByte != sbyte.MinValue)
			{
				return (double)specialByte;
			}
			else
			{
				return ReadDouble();
			}
		}

		#endregion


		#region Stream Operations

		/// <summary>
		/// Get whether the reader is currently at the end.
		/// </summary>
		/// <remarks>
		/// This is an alias for <see cref="Eof"/>.
		/// </remarks>
		public bool EndOfStream
		{
			get
			{
				return Eof;
			}
		}
		/// <summary>
		/// Get whether the reader is currently at the end.
		/// </summary>
		/// <remarks>
		/// This is preferable to <see cref="BinaryReader.PeekChar"/>, which is a very broken method
		/// that should virtually never be used. However, this may still always return false
		/// when the <see cref="BaseStream"/> is not seekable, unfortunately. This may be fixed
		/// in future releases.
		/// </remarks>
		public bool Eof
		{
			get
			{
				try
				{
					// assume no buffering??
					return BaseStream.Position < BaseStream.Length;
				}
				catch
				{
					//try
					//{
					//    // ugh! should this be done?
					//    return (PeekChar() >= 0);
					//}
					//catch
					//{
					return false;
					//}
				}
			}
		}

		/// <summary>
		/// Get the base Stream - hide this from applications, to avoid problems!
		/// </summary>
		private new Stream BaseStream
		{
			get
			{
				return base.BaseStream;
			}
		}

		/// <summary>
		/// Hide the PeekChar from applications, since it is terrible.
		/// </summary>
		/// <returns></returns>
		private new int PeekChar()
		{
			return base.PeekChar();
		}

		/// <summary>
		/// Seek to a given position in the underlying stream.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		/// <remarks>
		/// Note that this does not guarantee that the resulting position aligns with any complex
		/// data types, and it will fail if the underlying stream is not seekable.
		/// </remarks>
		public long Seek(long offset)
		{
			return Seek(offset, SeekOrigin.Begin);
		}
		/// <summary>
		/// Seek to a given position.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		/// <remarks>
		/// Note that this does not guarantee that the resulting position aligns with any complex
		/// data types, and it will fail if the underlying stream is not seekable.
		/// </remarks>
		public long Seek(long offset, SeekOrigin origin)
		{
			return base.BaseStream.Seek(offset, origin);
		}

		#endregion


		#region IBinarySerializable

		/// <summary>
		/// Read into the given item.
		/// </summary>
		/// <param name="item">The instance to deserialize into</param>
		public void Read(IBinaryReadable item)
		{
			item.Deserialize(this);
		}

		private static readonly Type[] deserializeArgs = new Type[] { typeof(BinaryReaderEx) };
		private static readonly Type[] deserializeBaseArgs = new Type[] { typeof(BinaryReader) };
		private object[] consArgs = null;

		/// <summary>
		/// Read an object of the given type, which must have a constructor that takes
		/// a single argument of either a BinaryReaderEx or a BinaryReader.
		/// </summary>
		/// <param name="type">The type of the object to construct</param>
		/// <returns>The new object of the given type</returns>
		/// <remarks>
		/// Consider using the generic version, instead, for performance and type safety.
		/// </remarks>
		public object Read(Type type)
		{
			ConstructorInfo cons = type.GetConstructor(deserializeArgs);
			if (cons == null) cons = type.GetConstructor(deserializeBaseArgs);
			if (cons == null) throw new NotSupportedException("The type " + type.FullName + " does not have a constructor that takes a BinaryReader.");
			if (consArgs == null) consArgs = new object[] { this };
			return cons.Invoke(consArgs);
		}


		/// <summary>
		/// Read an array of objects of the given type, which must have a constructor that takes
		/// a single argument of either a BinaryReaderEx or a BinaryReader.
		/// </summary>
		/// <param name="type">The type of the object to construct</param>
		/// <returns>The new array of objects of the given type</returns>
		/// <remarks>
		/// Consider using the generic version, instead, for performance and type safety.
		/// </remarks>
		public object[] ReadArray(Type type)
		{
			return ReadArray(type, ReadInt());
		}
		private object[] ReadArray(Type type, int len)
		{
			object[] vec = new object[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = Read(type);
			}
			return vec;
		}

		#endregion


		#region Serializable

#if DOTNET2
		private static readonly object typeCheckLock = new object();
		private static Dictionary<Type, ConstructorInfo> typeConstructors = null;
		private static Dictionary<Type, ConstructorInfo> typeEmptyConstructors = null;
		private static Dictionary<Type, bool> typeSerializable = null;
		private BinaryFormatter binaryFormatter = null;

		internal static bool TypeCheck(Type t,
			out ConstructorInfo typeConstructor, out ConstructorInfo typeEmptyConstructor) //, out bool serializable)
		{
			// memoizing this choice for performance reasons is preferable!
			lock (typeCheckLock)
			{
				if (typeConstructors == null) typeConstructors = new Dictionary<Type, ConstructorInfo>();
				if (typeEmptyConstructors == null) typeEmptyConstructors = new Dictionary<Type, ConstructorInfo>();
				if (typeSerializable == null) typeSerializable = new Dictionary<Type, bool>();
				if (typeConstructors.TryGetValue(t, out typeConstructor))
				{
					typeEmptyConstructor = null;
					//serializable = false;
					return true;
				}
				if (typeEmptyConstructors.TryGetValue(t, out typeEmptyConstructor))
				{
					typeConstructor = null;
					//serializable = false;
					return true;
				}
				bool serializable;
				if (typeSerializable.TryGetValue(t, out serializable))
				{
					typeConstructor = null;
					typeEmptyConstructor = null;
					return serializable;
				}
			}
			// not looked up before...
			typeConstructor = t.GetConstructor(deserializeArgs);
			if (typeConstructor == null) typeConstructor = t.GetConstructor(deserializeBaseArgs);
			if (typeConstructor != null)
			{
				lock (typeCheckLock)
				{
					typeConstructors[t] = typeConstructor;
				}
				typeEmptyConstructor = null;
				//serializable = false;
				return true;
			}
			if (t.GetInterface("Microsoft.TMSN.IBinaryReadable") != null)
			{
				typeEmptyConstructor = t.GetConstructor(Type.EmptyTypes);
				if (typeEmptyConstructor != null)
				{
					lock (typeCheckLock)
					{
						typeEmptyConstructors[t] = typeEmptyConstructor;
					}
					typeConstructor = null;
					//serializable = false;
					return true;
				}
			}
			if ((t.Attributes & TypeAttributes.Serializable) != 0)
			{
				lock (typeCheckLock)
				{
					typeSerializable[t] = true;
				}
				typeConstructor = null;
				typeEmptyConstructor = null;
				//serializable = true;
				return true;
			}

			// failure!
			lock (typeCheckLock)
			{
				typeSerializable[t] = false;
			}
			typeConstructor = null;
			typeEmptyConstructor = null;
			//serializable = false;
			return false;
		}


		/// <summary>
		/// Read an object of the given type,
		/// which must either have a constructor that takes
		/// a single argument of either a <see cref="BinaryReaderEx"/> or a <see cref="BinaryReader"/>,
		/// have an empty constructor and implement <see cref="IBinaryReadable"/>,
		/// or be marked with the <see cref="SerializableAttribute"/> attribute.
		/// </summary>
		/// <typeparam name="T">The type of the object to construct</typeparam>
		/// <returns>The new object of the given type</returns>
		public T Read<T>()
		{
			ConstructorInfo cons, emptyCons;
			if (!TypeCheck(typeof(T), out cons, out emptyCons))
			{
				throw new NotSupportedException("Type " + typeof(T).FullName + " must either have a BinaryReader constructor, " +
					"implement IBinaryReadable, or be marked serializable.");
			}
			if (cons != null)
			{
				// constructor call
				if (consArgs == null) consArgs = new object[] { this };
				return (T)cons.Invoke(consArgs);
			}
			else if (emptyCons != null)
			{
				// IBinaryReadable
				T res = (T)cons.Invoke(Type.EmptyTypes);
				((IBinaryReadable)res).Deserialize(this);
				return res;
			}
			else
			{
				// .NET binary serialization
				if (binaryFormatter == null) binaryFormatter = new BinaryFormatter();
				Stream s;
				s = base.BaseStream;
				//s = new System.IO.Compression.DeflateStream(s, System.IO.Compression.CompressionMode.Decompress, true);
				//s = new System.IO.Compression.GZipStream(s, System.IO.Compression.CompressionMode.Decompress, true);
				//s = new GzipDecodeStream(new IgnoreCloseStream(s));
				//Xceed.Compression.Licenser.LicenseKey = "SCN10-PWMX9-BR9H8-UWNA";
				//s = new IgnoreCloseStream(s);
				//s = new BufferedStream(s, 4 * 1024 * 1024);
				//s = new Xceed.Compression.CompressedStream(s, Xceed.Compression.CompressionMethod.Deflated, Xceed.Compression.CompressionLevel.Lowest);
				//s = new System.IO.Compression.GZipStream(s, System.IO.Compression.CompressionMode.Decompress, true);
				//s = new BufferedStream(s, 4 * 1024 * 1024);
				T res = (T)binaryFormatter.Deserialize(s);
				//s.Close();
				return res;
			}
		}

		/// <summary>
		/// Read an array of objects of the given type,
		/// which must either have a constructor that takes
		/// a single argument of either a <see cref="BinaryReaderEx"/> or a <see cref="BinaryReader"/>,
		/// have an empty constructor and implement <see cref="IBinaryReadable"/>,
		/// or be marked with the <see cref="SerializableAttribute"/> attribute.
		/// </summary>
		/// <typeparam name="T">The type of the object to construct</typeparam>
		/// <returns>The new array of objects of the given type</returns>
		public T[] ReadArray<T>()
		{
			//    return ReadArray<T>(ReadInt());
			//}
			//private T[] ReadArray<T>(int len)
			//{
			//    T[] vec = new T[len];
			//    for (int i = 0; i < vec.Length; i++)
			//    {
			//        vec[i] = Read<T>();
			//    }
			//    return vec;
			//}

			ConstructorInfo cons, emptyCons;
			if (!TypeCheck(typeof(T), out cons, out emptyCons))
			{
				throw new NotSupportedException("Type " + typeof(T).FullName + " must either have a BinaryReader constructor, " +
					"implement IBinaryReadable, or be marked serializable.");
			}
			if (cons != null)
			{
				// constructor call
				if (consArgs == null) consArgs = new object[] { this };
				T[] res = new T[ReadInt()];
				for (int i = 0; i < res.Length; i++)
				{
					res[i] = (T)cons.Invoke(consArgs);
				}
				return res;
			}
			else if (emptyCons != null)
			{
				// IBinaryReadable
				T[] res = new T[ReadInt()];
				for (int i = 0; i < res.Length; i++)
				{
					IBinaryReadable item = (IBinaryReadable)cons.Invoke(Type.EmptyTypes);
					item.Deserialize(this);
					res[i] = (T)item;
				}
				return res;
			}
			else
			{
				// .NET binary serialization
				if (binaryFormatter == null) binaryFormatter = new BinaryFormatter();
				Stream s;
				s = base.BaseStream;
				//s = new System.IO.Compression.DeflateStream(base.BaseStream, System.IO.Compression.CompressionMode.Decompress, true);
				//s = new System.IO.Compression.GZipStream(base.BaseStream, System.IO.Compression.CompressionMode.Decompress, true);
				//s = new ICSharpCode.SharpZipLib.Zip.Compression.Streams.InflaterInputStream(s, new ICSharpCode.SharpZipLib.Zip.Compression.Inflater(true), 32 * 1024);
				//s = new Xceed.Compression.CompressedStream(s, Xceed.Compression.CompressionMethod.Deflated, Xceed.Compression.CompressionLevel.Lowest);
				return (T[])binaryFormatter.Deserialize(s);
			}
		}

		/// <summary>
		/// Read a matrix of objects of the given type,
		/// which must either have a constructor that takes
		/// a single argument of either a <see cref="BinaryReaderEx"/> or a <see cref="BinaryReader"/>,
		/// have an empty constructor and implement <see cref="IBinaryReadable"/>,
		/// or be marked with the <see cref="SerializableAttribute"/> attribute.
		/// </summary>
		/// <typeparam name="T">The type of the object to construct</typeparam>
		/// <returns>The new matrix of objects of the given type</returns>
		public T[][] ReadMatrix<T>()
		{
			//    T[][] mat = new T[ReadInt()][];
			//    int width = ReadInt();
			//    if (width < 0)
			//    {
			//        for (int j = 0; j < mat.Length; j++)
			//        {
			//            mat[j] = ReadArray<T>();
			//        }
			//    }
			//    else
			//    {
			//        for (int j = 0; j < mat.Length; j++)
			//        {
			//            mat[j] = ReadArray<T>(width);
			//        }
			//    }
			//    return mat;
			//}

			ConstructorInfo cons, emptyCons;
			if (!TypeCheck(typeof(T), out cons, out emptyCons))
			{
				throw new NotSupportedException("Type " + typeof(T).FullName + " must either have a BinaryReader constructor, " +
					"implement IBinaryReadable, or be marked serializable.");
			}
			if (cons != null || emptyCons != null)
			{
				// constructor call
				if (cons != null && consArgs == null) consArgs = new object[] { this };
				T[][] res = new T[ReadInt()][];
				int width = ReadInt();
				for (int j = 0; j < res.Length; j++)
				{
					int w = width >= 0 ? width : ReadInt();
					res[j] = new T[w];
					for (int i = 0; i < res[j].Length; i++)
					{
						if (cons != null)
						{
							res[j][i] = (T)cons.Invoke(consArgs);
						}
						else
						{
							IBinaryReadable item = (IBinaryReadable)cons.Invoke(Type.EmptyTypes);
							item.Deserialize(this);
							res[j][i] = (T)item;
						}
					}
				}
				return res;
			}
			else
			{
				// .NET binary serialization
				if (binaryFormatter == null) binaryFormatter = new BinaryFormatter();
				Stream s;
				s = base.BaseStream;
				//s = new System.IO.Compression.DeflateStream(base.BaseStream, System.IO.Compression.CompressionMode.Decompress, true);
				//s = new System.IO.Compression.GZipStream(base.BaseStream, System.IO.Compression.CompressionMode.Decompress, true);
				return (T[][])binaryFormatter.Deserialize(s);
			}
		}
#endif

		#endregion


		#region Primitive Types

		/// <summary>
		/// Read count bytes from the stream with index as the starting point in buffer.
		/// </summary>
		/// <param name="buffer">the buffer to read into</param>
		/// <param name="index">the index at which to start placing the bytes</param>
		/// <param name="count">the number of bytes to read</param>
		/// <returns>
		/// The number of bytes read, which will be equal to the number requested unless
		/// the end of the stream is reached, in which case the number of bytes read will be
		/// returned.
		/// </returns>
		public override int Read(byte[] buffer, int index, int count)
		{
			int countRead = base.Read(buffer, index, count);
			if (countRead != count)
			{
				while (true)
				{
					int last = countRead;
					countRead = base.Read(buffer, last + index, count - last);
					if (countRead <= 0)
					{
						//Console.WriteLine("!! failed to complete read - " + last + " / " + Backing.Length);
						break;
					}
					countRead += last;
					if (countRead == count) break;
				}
			}
			return countRead;
		}

		/// <summary>
		/// Read bytes from the stream to fill a buffer.
		/// </summary>
		/// <param name="buffer">the buffer to read into</param>
		/// <returns>
		/// The number of bytes read, which will be equal to buffer.Length unless
		/// the end of the stream is reached, in which case the number of bytes read will be
		/// returned.
		/// </returns>
		public int Read(byte[] buffer)
		{
			return Read(buffer, 0, buffer.Length);
		}


		/// <summary>
		/// Read an array of bytes.
		/// </summary>
		/// <returns>The array of bytes</returns>
		public byte[] ReadByteArray()
		{
			return ReadByteArray(ReadInt());
		}
		/// <summary>
		/// Read an array of bytes.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of bytes</returns>
		public byte[] ReadByteArray(int len)
		{
			return ReadBytes(len);
		}
		/// <summary>
		/// Read a matrix of bytes.
		/// </summary>
		/// <returns>The matrix of bytes</returns>
		public byte[][] ReadByteMatrix()
		{
			byte[][] mat = new byte[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadByteArray();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadByteArray(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of sbytes.
		/// </summary>
		/// <returns>The array of sbytes</returns>
		public sbyte[] ReadSByteArray()
		{
			return ReadSByteArray(ReadInt());
		}
		/// <summary>
		/// Read an array of sbytes.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of sbytes</returns>
		public sbyte[] ReadSByteArray(int len)
		{
			sbyte[] vec = new sbyte[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadSByte();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of sbytes.
		/// </summary>
		/// <returns>The matrix of sbytes</returns>
		public sbyte[][] ReadSByteMatrix()
		{
			sbyte[][] mat = new sbyte[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadSByteArray();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadSByteArray(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of chars.
		/// </summary>
		/// <returns>The array of chars</returns>
		public char[] ReadCharArray()
		{
			return ReadCharArray(ReadInt());
		}
		/// <summary>
		/// Read an array of chars.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of chars</returns>
		public char[] ReadCharArray(int len)
		{
			return ReadChars(len);
		}
		/// <summary>
		/// Read a matrix of chars.
		/// </summary>
		/// <returns>The matrix of chars</returns>
		public char[][] ReadCharMatrix()
		{
			char[][] mat = new char[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadCharArray();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadCharArray(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of shorts.
		/// </summary>
		/// <returns>The array of shorts</returns>
		public short[] ReadInt16Array()
		{
			return ReadInt16Array(ReadInt());
		}
		/// <summary>
		/// Read an array of shorts.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of shorts</returns>
		public short[] ReadInt16Array(int len)
		{
			short[] vec = new short[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadInt16();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of shorts.
		/// </summary>
		/// <returns>The matrix of shorts</returns>
		public short[][] ReadInt16Matrix()
		{
			short[][] mat = new short[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadInt16Array();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadInt16Array(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of ushorts.
		/// </summary>
		/// <returns>The array of ushorts</returns>
		public ushort[] ReadUInt16Array()
		{
			return ReadUInt16Array(ReadInt());
		}
		/// <summary>
		/// Read an array of ushorts.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of ushorts</returns>
		public ushort[] ReadUInt16Array(int len)
		{
			ushort[] vec = new ushort[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadUInt16();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of ushorts.
		/// </summary>
		/// <returns>The matrix of ushorts</returns>
		public ushort[][] ReadUInt16Matrix()
		{
			ushort[][] mat = new ushort[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadUInt16Array();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadUInt16Array(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of ints.
		/// </summary>
		/// <returns>The array of ints</returns>
		public int[] ReadInt32Array()
		{
			return ReadInt32Array(ReadInt());
		}
		/// <summary>
		/// Read an array of ints.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of ints</returns>
		public int[] ReadInt32Array(int len)
		{
			int[] vec = new int[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadInt32();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of ints.
		/// </summary>
		/// <returns>The matrix of ints</returns>
		public int[][] ReadInt32Matrix()
		{
			int[][] mat = new int[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadInt32Array();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadInt32Array(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of uints.
		/// </summary>
		/// <returns>The array of uints</returns>
		public uint[] ReadUInt32Array()
		{
			return ReadUInt32Array(ReadInt());
		}
		/// <summary>
		/// Read an array of uints.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of uints</returns>
		public uint[] ReadUInt32Array(int len)
		{
			uint[] vec = new uint[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadUInt32();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of uints.
		/// </summary>
		/// <returns>The matrix of uints</returns>
		public uint[][] ReadUInt32Matrix()
		{
			uint[][] mat = new uint[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadUInt32Array();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadUInt32Array(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of longs.
		/// </summary>
		/// <returns>The array of longs</returns>
		public long[] ReadInt64Array()
		{
			return ReadInt64Array(ReadInt());
		}
		/// <summary>
		/// Read an array of longs.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of longs</returns>
		public long[] ReadInt64Array(int len)
		{
			long[] vec = new long[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadInt64();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of longs.
		/// </summary>
		/// <returns>The matrix of longs</returns>
		public long[][] ReadInt64Matrix()
		{
			long[][] mat = new long[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadInt64Array();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadInt64Array(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of ulongs.
		/// </summary>
		/// <returns>The array of ulongs</returns>
		public ulong[] ReadUInt64Array()
		{
			return ReadUInt64Array(ReadInt());
		}
		/// <summary>
		/// Read an array of ulongs.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of ulongs</returns>
		public ulong[] ReadUInt64Array(int len)
		{
			ulong[] vec = new ulong[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadUInt64();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of ulongs.
		/// </summary>
		/// <returns>The matrix of ulongs</returns>
		public ulong[][] ReadUInt64Matrix()
		{
			ulong[][] mat = new ulong[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadUInt64Array();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadUInt64Array(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of floats.
		/// </summary>
		/// <returns>The array of floats</returns>
		public float[] ReadSingleArray()
		{
			return ReadSingleArray(ReadInt());
		}
		/// <summary>
		/// Read an array of floats.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of floats</returns>
		public float[] ReadSingleArray(int len)
		{
			float[] vec = new float[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadSingle();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of floats.
		/// </summary>
		/// <returns>The matrix of floats</returns>
		public float[][] ReadSingleMatrix()
		{
			float[][] mat = new float[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadSingleArray();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadSingleArray(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of doubles.
		/// </summary>
		/// <returns>The array of doubles</returns>
		public double[] ReadDoubleArray()
		{
			return ReadDoubleArray(ReadInt());
		}
		/// <summary>
		/// Read an array of doubles.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of doubles</returns>
		public double[] ReadDoubleArray(int len)
		{
			double[] vec = new double[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadDouble();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of doubles.
		/// </summary>
		/// <returns>The matrix of doubles</returns>
		public double[][] ReadDoubleMatrix()
		{
			double[][] mat = new double[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadDoubleArray();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadDoubleArray(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of decimals.
		/// </summary>
		/// <returns>The array of decimals</returns>
		public decimal[] ReadDecimalArray()
		{
			return ReadDecimalArray(ReadInt());
		}
		/// <summary>
		/// Read an array of decimals.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of decimals</returns>
		public decimal[] ReadDecimalArray(int len)
		{
			decimal[] vec = new decimal[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadDecimal();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of decimals.
		/// </summary>
		/// <returns>The matrix of decimals</returns>
		public decimal[][] ReadDecimalMatrix()
		{
			decimal[][] mat = new decimal[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadDecimalArray();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadDecimalArray(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read a BitArray.
		/// </summary>
		/// <returns>The BitArray instance</returns>
		public BitArray ReadBitArray()
		{
			return ReadBitArray(ReadInt());
		}
		/// <summary>
		/// Read a BitArray.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The BitArray instance</returns>
		public BitArray ReadBitArray(int len)
		{
			if (len == 0)
			{
				return new BitArray(0);
			}
			else
			{
				int numBytes = (int)Math.Ceiling(len / 8.0);
				byte[] bytes = ReadBytes(numBytes);
				BitArray vec = new BitArray(bytes);
				vec.Length = len;
				return vec;
			}
		}
		/// <summary>
		/// Read a matrix of bits.
		/// </summary>
		/// <returns>The matrix of bits, as an array of BitArray instances</returns>
		public BitArray[] ReadBitMatrix()
		{
			BitArray[] mat = new BitArray[ReadInt()];
			int width = ReadInt();
			for (int j = 0; j < mat.Length; j++)
			{
				mat[j] = ReadBitArray(width);
			}
			return mat;
		}

		/// <summary>
		/// Read an array of bools.
		/// </summary>
		/// <returns>The array of bools</returns>
		public bool[] ReadBooleanArray()
		{
			BitArray vec = ReadBitArray();
			bool[] res = new bool[vec.Length];
			vec.CopyTo(res, 0);
			return res;
		}
		/// <summary>
		/// Read an array of bools.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of bools</returns>
		public bool[] ReadBooleanArray(int len)
		{
			BitArray vec = ReadBitArray(len);
			bool[] res = new bool[len];
			vec.CopyTo(res, 0);
			return res;
		}
		/// <summary>
		/// Read a matrix of bools.
		/// </summary>
		/// <returns>The matrix of bools</returns>
		public bool[][] ReadBooleanMatrix()
		{
			bool[][] mat = new bool[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadBooleanArray();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadBooleanArray(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read an array of strings.
		/// </summary>
		/// <returns>The array of strings</returns>
		public string[] ReadStringArray()
		{
			return ReadStringArray(ReadInt());
		}
		/// <summary>
		/// Read an array of strings.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of strings</returns>
		public string[] ReadStringArray(int len)
		{
			string[] vec = new string[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadString();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of strings.
		/// </summary>
		/// <returns>The matrix of strings</returns>
		public string[][] ReadStringMatrix()
		{
			string[][] mat = new string[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadStringArray();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadStringArray(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read a Guid.
		/// </summary>
		/// <returns>The Guid</returns>
		public Guid ReadGuid()
		{
			return new Guid(ReadBytes(16));
		}
		/// <summary>
		/// Read an array of Guids.
		/// </summary>
		/// <returns>The array of Guids</returns>
		public Guid[] ReadGuidArray()
		{
			return ReadGuidArray(ReadInt());
		}
		/// <summary>
		/// Read an array of Guids.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of Guids</returns>
		public Guid[] ReadGuidArray(int len)
		{
			Guid[] vec = new Guid[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadGuid();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of Guids.
		/// </summary>
		/// <returns>The matrix of Guids</returns>
		public Guid[][] ReadGuidMatrix()
		{
			Guid[][] mat = new Guid[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadGuidArray();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadGuidArray(width);
				}
			}
			return mat;
		}

		/// <summary>
		/// Read a Var.
		/// </summary>
		/// <returns>The Var</returns>
		public Var ReadVar()
		{
			return new Var(ReadString());
		}
		/// <summary>
		/// Read an array of Vars.
		/// </summary>
		/// <returns>The array of Vars</returns>
		public Var[] ReadVarArray()
		{
			return ReadVarArray(ReadInt());
		}
		/// <summary>
		/// Read an array of Vars.
		/// </summary>
		/// <param name="len">the length of the array</param>
		/// <returns>The array of Vars</returns>
		public Var[] ReadVarArray(int len)
		{
			Var[] vec = new Var[len];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ReadVar();
			}
			return vec;
		}
		/// <summary>
		/// Read a matrix of Vars.
		/// </summary>
		/// <returns>The matrix of Guids</returns>
		public Var[][] ReadVarMatrix()
		{
			Var[][] mat = new Var[ReadInt()][];
			int width = ReadInt();
			if (width < 0)
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadVarArray();
				}
			}
			else
			{
				for (int j = 0; j < mat.Length; j++)
				{
					mat[j] = ReadVarArray(width);
				}
			}
			return mat;
		}

		#endregion


		//		#region IDisposable Members
		//
		//		public void Dispose()
		//		{
		//			if (closeStream)
		//			{
		//				Close();
		//			}
		//		}
		//
		//		public void Close()
		//		{
		//			try
		//			{
		//				BaseStream.Close();
		//				closeStream = false;
		//			}
		//			catch
		//			{
		//			}
		//		}
		//
		//		~BinaryReaderEx()
		//		{
		//			if (closeStream)
		//			{
		//				Close();
		//			}
		//		}
		//
		//		#endregion
	}




	/// <summary>
	/// BinaryWriter extended to handle additional data structures.
	/// </summary>
	/// <remarks>
	/// Autodetection of options would be nice, but it would interfere with the statelessness.
	/// Perhaps a special constructor could be made that would write a header?
	/// </remarks>
	public class BinaryWriterEx : BinaryWriter
	{
		#region Constructors

		/// <summary>
		/// Create a writer for the given filename.
		/// </summary>
		/// <param name="output">The file to write to</param>
		public BinaryWriterEx(string output)
			: this(ZStreamOut.Open(output))
		{
			//closeStream = true;
		}

		/// <summary>
		/// Create a writer for the given filename and encoding.
		/// </summary>
		/// <param name="output">The file to write to</param>
		/// <param name="enc">The encoding to use for string objects</param>
		public BinaryWriterEx(string output, Encoding enc)
			: this(ZStreamOut.Open(output), enc)
		{
			//closeStream = true;
		}

		/// <summary>
		/// Create a writer for the given stream.
		/// </summary>
		/// <param name="output">The stream to write to</param>
		public BinaryWriterEx(Stream output)
			: base(output)
		{
		}

		/// <summary>
		/// Create a writer for the given stream and encoding.
		/// </summary>
		/// <param name="output">The stream to write to</param>
		/// <param name="enc">The encoding to use for string objects</param>
		public BinaryWriterEx(Stream output, Encoding enc)
			: base(output, enc)
		{
			encoding = enc;
		}

		/// <summary>
		/// Create a writer for the given BinaryWriter.
		/// </summary>
		/// <param name="output">The writer to write to</param>
		public BinaryWriterEx(BinaryWriter output)
			: this(output.BaseStream)
		{
		}

		/// <summary>
		/// Create a writer for the given BinaryWriter and encoding.
		/// </summary>
		/// <param name="output">The writer to write to</param>
		/// <param name="enc">The encoding to use for string objects</param>
		public BinaryWriterEx(BinaryWriter output, Encoding enc)
			: this(output.BaseStream, enc)
		{
		}

		#endregion


		private readonly Encoding encoding = System.Text.Encoding.UTF8;

		/// <summary>
		/// Get the encoding used for any character output.
		/// </summary>
		public Encoding Encoding
		{
			get { return encoding; }
		}


		#region Packing

		private bool packLengths = false;
		//private bool closeStream = false;
		/// <summary>
		/// Get or set whether to pack the lengths in a 7-bit multibyte encoding,
		/// to save space. This is false by default, and it must be matched by the reader.
		/// </summary>
		public bool PackLengths
		{
			get { return packLengths; }
			set { packLengths = value; }
		}

		private void WriteInt(int val)
		{
			if (packLengths)
			{
				Write7BitEncodedInt(val);
			}
			else
			{
				Write(val);
			}
		}

		/// <summary>
		/// Writes a 32-bit integer in compressed format. Should be read with <see cref="BinaryReaderEx.ReadInt32Packed"/>.
		/// </summary>
		/// <param name="val">The integer to write</param>
		public void WritePacked(int val)
		{
			//			Write((byte)((val >> 16) & 0xff));
			//			if (val == 0)
			//			{
			//				Write((byte)0);
			//			}
			//			else
			//			{
			//				for (int i=4; i>=0; i--)
			//				{
			//					int remainder = val >> (7*i);
			//					if (remainder == 0) continue;
			//
			//					byte b=(byte)(remainder & 0x7F);
			//					if (i != 0)
			//						b |= 0x80;
			//					Write(b);
			//				}
			////////
			//
			//
			//				while (true)
			//				{
			//
			//					byte b = (byte)(val & (byte)(0x7F));
			//					val >>= 7;
			//					if (val == 0)
			//					{
			//						Write(b);
			//						break;
			//					}
			//					else
			//					{
			//						Write((byte)(b | (byte)(0x80)));
			//					}
			//				}
			//			}
			Write7BitEncodedInt(val);
		}

		/// <summary>
		/// Writes a float in compressed format. Compression is best if the
		/// floats tend to be small integers.
		/// </summary>
		/// <param name="val">The float to write</param>
		public void WritePacked(float val)
		{
			unchecked
			{
				// See if we can cast to sbyte and back without losing any precision
				sbyte sbVal = (sbyte)val;
				if ((float)sbVal == val && sbVal != sbyte.MinValue)
				{
					Write(sbVal);
				}
				else
				{
					Write(sbyte.MinValue);
					Write(val);
				}
			}
		}

		/// <summary>
		/// Writes a double in compressed format. Compression is best if the
		/// doubles tend to be small integers.
		/// </summary>
		/// <param name="val">The double to write</param>
		public void WritePacked(double val)
		{
			unchecked
			{
				// See if we can cast to sbyte and back without losing any precision
				sbyte sbVal = (sbyte)val;
				if ((double)sbVal == val && sbVal != sbyte.MinValue)
				{
					Write(sbVal);
				}
				else
				{
					Write(sbyte.MinValue);
					Write(val);
				}
			}
		}

		#endregion


		#region Stream Operations

		///// <summary>
		///// Get whether the reader is currently at the end.
		///// </summary>
		///// <remarks>
		///// This is an alias for <see cref="Eof"/>.
		///// </remarks>
		//public bool EndOfStream
		//{
		//    get
		//    {
		//        return Eof;
		//    }
		//}
		///// <summary>
		///// Get whether the reader is currently at the end.
		///// </summary>
		///// <remarks>
		///// This is preferable to <see cref="BinaryReader.PeekChar"/>, which is a very broken method
		///// that should virtually never be used. However, this may still always return false
		///// when the <see cref="BaseStream"/> is not seekable, unfortunately. This may be fixed
		///// in future releases.
		///// </remarks>
		//public bool Eof
		//{
		//    get
		//    {
		//        try
		//        {
		//            // assume no buffering??
		//            return BaseStream.Position < BaseStream.Length;
		//        }
		//        catch
		//        {
		//            //try
		//            //{
		//            //    // ugh! should this be done?
		//            //    return (PeekChar() >= 0);
		//            //}
		//            //catch
		//            //{
		//            return false;
		//            //}
		//        }
		//    }
		//}

		/// <summary>
		/// Get the base Stream - hide this from applications, to avoid problems!
		/// </summary>
		private new Stream BaseStream
		{
			get
			{
				return base.BaseStream;
			}
		}

		/// <summary>
		/// Seek to a given position in the underlying stream.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		/// <remarks>
		/// Note that this does not guarantee that the resulting position aligns with any complex
		/// data types, and it will fail if the underlying stream is not seekable.
		/// </remarks>
		public long Seek(long offset)
		{
			return Seek(offset, SeekOrigin.Begin);
		}
		/// <summary>
		/// Seek to a given position.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		/// <remarks>
		/// Note that this does not guarantee that the resulting position aligns with any complex
		/// data types, and it will fail if the underlying stream is not seekable.
		/// </remarks>
		public long Seek(long offset, SeekOrigin origin)
		{
			return base.BaseStream.Seek(offset, origin);
		}

		#endregion


		#region IBinarySerializable

		/// <summary>
		/// Write the item.
		/// </summary>
		/// <param name="item">The item to write</param>
		public void Write(IBinaryWritable item)
		{
			item.Serialize(this);
		}
		/// <summary>
		/// Write the array.
		/// </summary>
		/// <param name="vec">The array to write</param>
		public void Write(IBinaryWritable[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(IBinaryWritable[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix.
		/// </summary>
		/// <param name="mat">The matrix to write</param>
		public void Write(IBinaryWritable[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		#endregion


		#region Serializable

		#region Painful junk required for C# spec fault
		/// <summary>Writes a one-byte Boolean value to the current stream, with 0 representing false and 1 representing true.</summary>
		/// <param name="value">The Boolean value to write (0 or 1). </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		public new virtual void Write(bool value)
		{
			base.Write(value);
		}
		/// <summary>Writes an unsigned byte to the current stream and advances the stream position by one byte.</summary>
		/// <param name="value">The unsigned byte to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		public new virtual void Write(byte value)
		{
			base.Write(value);
		}
		///// <summary>Writes a byte array to the underlying stream.</summary>
		///// <param name="buffer">A byte array containing the data to write. </param>
		///// <exception cref="T:System.ArgumentNullException">buffer is null. </exception>
		///// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		///// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		///// <filterpriority>1</filterpriority>
		//public new virtual void Write(byte[] buffer)
		//{
		//    base.Write(value);
		//}
		/// <summary>Writes a Unicode character to the current stream and advances the current position of the stream in accordance with the Encoding used and the specific characters being written to the stream.</summary>
		/// <param name="ch">The non-surrogate, Unicode character to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.ArgumentException">ch is a single surrogate character.</exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		public new virtual void Write(char ch)
		{
			base.Write(ch);
		}
		/// <summary>Writes a two-byte signed integer to the current stream and advances the stream position by two bytes.</summary>
		/// <param name="value">The two-byte signed integer to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		public new virtual void Write(short value)
		{
			base.Write(value);
		}
		///// <summary>Writes a character array to the current stream and advances the current position of the stream in accordance with the Encoding used and the specific characters being written to the stream.</summary>
		///// <param name="chars">A character array containing the data to write. </param>
		///// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		///// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		///// <exception cref="T:System.ArgumentNullException">chars is null. </exception>
		///// <filterpriority>1</filterpriority>
		//public virtual void Write(char[] chars);
		/// <summary>Writes a decimal value to the current stream and advances the stream position by sixteen bytes.</summary>
		/// <param name="value">The decimal value to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		public new virtual void Write(decimal value)
		{
			base.Write(value);
		}
		/// <summary>Writes an eight-byte floating-point value to the current stream and advances the stream position by eight bytes.</summary>
		/// <param name="value">The eight-byte floating-point value to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		public new virtual void Write(double value)
		{
			base.Write(value);
		}
		/// <summary>Writes a four-byte signed integer to the current stream and advances the stream position by four bytes.</summary>
		/// <param name="value">The four-byte signed integer to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		public new virtual void Write(int value)
		{
			base.Write(value);
		}
		/// <summary>Writes an eight-byte signed integer to the current stream and advances the stream position by eight bytes.</summary>
		/// <param name="value">The eight-byte signed integer to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		public new virtual void Write(long value)
		{
			base.Write(value);
		}
		/// <summary>Writes a signed byte to the current stream and advances the stream position by one byte.</summary>
		/// <param name="value">The signed byte to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		[CLSCompliant(false)]
		public new virtual void Write(sbyte value)
		{
			base.Write(value);
		}
		/// <summary>Writes a four-byte floating-point value to the current stream and advances the stream position by four bytes.</summary>
		/// <param name="value">The four-byte floating-point value to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		public new virtual void Write(float value)
		{
			base.Write(value);
		}
		/// <summary>Writes a length-prefixed string to this stream in the current encoding of the <see cref="T:System.IO.BinaryWriter"></see>, and advances the current position of the stream in accordance with the encoding used and the specific characters being written to the stream.</summary>
		/// <param name="value">The value to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <exception cref="T:System.ArgumentNullException">value is null. </exception>
		/// <filterpriority>1</filterpriority>
		public new virtual void Write(string value)
		{
			base.Write(value);
		}
		/// <summary>Writes a two-byte unsigned integer to the current stream and advances the stream position by two bytes.</summary>
		/// <param name="value">The two-byte unsigned integer to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		[CLSCompliant(false)]
		public new virtual void Write(ushort value)
		{
			base.Write(value);
		}
		/// <summary>Writes a four-byte unsigned integer to the current stream and advances the stream position by four bytes.</summary>
		/// <param name="value">The four-byte unsigned integer to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		[CLSCompliant(false)]
		public new virtual void Write(uint value)
		{
			base.Write(value);
		}
		/// <summary>Writes an eight-byte unsigned integer to the current stream and advances the stream position by eight bytes.</summary>
		/// <param name="value">The eight-byte unsigned integer to write. </param>
		/// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		/// <filterpriority>1</filterpriority>
		[CLSCompliant(false)]
		public new virtual void Write(ulong value)
		{
			base.Write(value);
		}
		///// <summary>Writes a region of a byte array to the current stream.</summary>
		///// <param name="count">The number of bytes to write. </param>
		///// <param name="buffer">A byte array containing the data to write. </param>
		///// <param name="index">The starting point in buffer at which to begin writing. </param>
		///// <exception cref="T:System.ArgumentNullException">buffer is null. </exception>
		///// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		///// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		///// <exception cref="T:System.ArgumentOutOfRangeException">index or count is negative. </exception>
		///// <exception cref="T:System.ArgumentException">The buffer length minus index is less than count. </exception>
		///// <filterpriority>1</filterpriority>
		//public virtual void Write(byte[] buffer, int index, int count);
		///// <summary>Writes a section of a character array to the current stream, and advances the current position of the stream in accordance with the Encoding used and perhaps the specific characters being written to the stream.</summary>
		///// <param name="chars">A character array containing the data to write. </param>
		///// <param name="count">The number of characters to write. </param>
		///// <param name="index">The starting point in buffer from which to begin writing. </param>
		///// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
		///// <exception cref="T:System.ObjectDisposedException">The stream is closed. </exception>
		///// <exception cref="T:System.ArgumentOutOfRangeException">index or count is negative. </exception>
		///// <exception cref="T:System.ArgumentException">The buffer length minus index is less than count. </exception>
		///// <exception cref="T:System.ArgumentNullException">chars is null. </exception>
		///// <filterpriority>1</filterpriority>
		//public virtual void Write(char[] chars, int index, int count);
		#endregion

		private BinaryFormatter binaryFormatter = null;

		/// <summary>
		/// Write the item, using .NET binary serialization.
		/// </summary>
		/// <param name="item">The item to write</param>
		/// <remarks>
		/// Note that this is very large and slow, and is often best to avoid whenever possible!
		/// </remarks>
		public void Write(object item)
		{
			//if (item is byte)
			//{
			//    base.Write((byte)item);
			//    return;
			//}
			// null never matches...
			// do we want this check? we can store a single byte...
			////if (item is IBinaryWritable)
			////{
			////    Write((IBinaryWritable)item);
			////    return;
			////}

			// assume serializable:
			// .NET binary serialization
			if (binaryFormatter == null) binaryFormatter = new BinaryFormatter();
			Flush();
			Stream s;
			s = base.BaseStream;
			//s = new System.IO.Compression.DeflateStream(base.BaseStream, System.IO.Compression.CompressionMode.Compress, true);
			//s = new System.IO.Compression.GZipStream(base.BaseStream, System.IO.Compression.CompressionMode.Compress, true);
			//s = new IgnoreCloseStream(s, true);
			//s = new BufferedStream(s, 256 * 1024);
			//Xceed.Compression.Licenser.LicenseKey = "SCN10-PWMX9-BR9H8-UWNA";
			//s = new Xceed.Compression.CompressedStream(s, Xceed.Compression.CompressionMethod.Deflated, Xceed.Compression.CompressionLevel.Highest);
			//s = new System.IO.Compression.DeflateStream(s, System.IO.Compression.CompressionMode.Compress, true);
			//s = new ICSharpCode.SharpZipLib.Zip.Compression.Streams.DeflaterOutputStream(s, new ICSharpCode.SharpZipLib.Zip.Compression.Deflater(ICSharpCode.SharpZipLib.Zip.Compression.Deflater.BEST_SPEED, true), 256 * 1024);
			//s = new GzipEncodeStream(s, 1);
			//s = new BufferedStream(s, 256 * 1024);
			binaryFormatter.Serialize(s, item);
			//s.Flush();
			//s.Close();
			//s.Flush();
			//s.Close();
			//throw new Exception(item.ToString());
		}

		/// <summary>
		/// Write the array, using .NET binary serialization.
		/// </summary>
		/// <param name="vec">The array to write</param>
		/// <remarks>
		/// Note that this is very large and slow, and is often best to avoid whenever possible!
		/// </remarks>
		public void Write(object[] vec)
		{
			// null never matches...
			// do we want this check?
			////if (vec is IBinaryWritable[])
			////{
			////    Write((IBinaryWritable[])vec);
			////    return;
			////}

			// assume serializable:
			// .NET binary serialization
			if (binaryFormatter == null) binaryFormatter = new BinaryFormatter();
			Flush();
			Stream s;
			s = base.BaseStream;
			//s = new IgnoreCloseStream(new BufferedStream(s, 32 * 1024 * 1024), true);
			//s = new System.IO.Compression.DeflateStream(s, System.IO.Compression.CompressionMode.Compress, true);
			//s = new System.IO.Compression.GZipStream(s, System.IO.Compression.CompressionMode.Compress, true);
			//s = new GzipEncodeStream(new BufferedStream(new IgnoreCloseStream(s), 1024*1024));
			//Stream real = s;
			//MemoryStream ms = new MemoryStream();
			//s = ms;
			//s = new BufferedStream(s, 4096);
			//s = new IgnoreCloseStream(s);
			//s = new ICSharpCode.SharpZipLib.Zip.Compression.Streams.DeflaterOutputStream(s, new ICSharpCode.SharpZipLib.Zip.Compression.Deflater(ICSharpCode.SharpZipLib.Zip.Compression.Deflater.BEST_SPEED, true), 32 * 1024);
			//Xceed.Compression.Licenser.LicenseKey = "SCN10-PWMX9-BR9H8-UWNA";
			//s = new Xceed.Compression.CompressedStream(s, Xceed.Compression.CompressionMethod.Deflated, Xceed.Compression.CompressionLevel.Highest);
			//s = new System.IO.Compression.GZipStream(s, System.IO.Compression.CompressionMode.Compress, true);
			//s = new System.IO.Compression.DeflateStream(s, System.IO.Compression.CompressionMode.Compress, true);
			//s = new BufferedStream(s, 32 * 1024);
			binaryFormatter.Serialize(s, vec);
			//s.Flush();
			//s.Close();
			//ms.Position = 0;
			//ms.WriteTo(real);
			//real.Flush();
			//real.Close();
			//throw new Exception(vec.ToString());
		}
		/// <summary>
		/// Write the matrix, using .NET binary serialization.
		/// </summary>
		/// <param name="mat">The matrix to write</param>
		/// <remarks>
		/// Note that this is very large and slow, and is often best to avoid whenever possible!
		/// </remarks>
		public void Write(object[][] mat)
		{
			// null never matches...
			// do we want this check?
			////if (mat is IBinaryWritable[][])
			////{
			////    Write((IBinaryWritable[][])mat);
			////    return;
			////}

			// assume serializable:
			// .NET binary serialization
			if (binaryFormatter == null) binaryFormatter = new BinaryFormatter();
			Flush();
			Stream s;
			s = base.BaseStream;
			//s = new System.IO.Compression.DeflateStream(base.BaseStream, System.IO.Compression.CompressionMode.Compress, true);
			//s = new System.IO.Compression.GZipStream(base.BaseStream, System.IO.Compression.CompressionMode.Compress, true);
			binaryFormatter.Serialize(s, mat);
			//s.Flush();
			//s.Close();
			//throw new Exception(mat.ToString());
		}

		#endregion


		#region Primitive Types

		/// <summary>
		/// Write the specified bytes to the output, without writing the length.
		/// 
		/// </summary>
		/// <param name="vec">The bytes to write</param>
		/// <remarks>
		/// WARNING! This does not output the length, so it cannot be deserialized.
		/// Use <see cref="WriteArray(byte[])"/> instead to output a recoverable array.
		/// </remarks>
		public new virtual void Write(byte[] vec)
		{
			base.Write(vec);
		}
		/// <summary>
		/// Write the array to the output.
		/// This can be deserialized into a byte array, unlike the base class Write(byte[]) method.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void WriteArray(byte[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(byte[] vec)
		{
			Write(vec);
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(byte[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						WriteArray(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(sbyte[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(sbyte[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(sbyte[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the specified characters to the output.
		/// WARNING! This does not output the length, so it cannot be deserialized.
		/// Use WriteArray() instead to output a recoverable array.
		/// </summary>
		/// <param name="vec">The bytes to write</param>
		public new virtual void Write(char[] vec)
		{
			base.Write(vec);
		}
		/// <summary>
		/// Write the array to the output.
		/// This can be deserialized into a byte array, unlike the base class Write(char[]) method.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void WriteArray(char[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(char[] vec)
		{
			Write(vec);
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(char[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						WriteArray(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(short[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(short[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(short[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(ushort[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(ushort[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(ushort[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(int[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(int[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(int[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(uint[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(uint[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(uint[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(long[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(long[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(long[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(ulong[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(ulong[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(ulong[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(decimal[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(decimal[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(decimal[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(float[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(float[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(float[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(double[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(double[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(double[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the BitArray.
		/// </summary>
		/// <param name="vec">The BitArray to write, which must not be null</param>
		public void Write(BitArray vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(BitArray vec)
		{
			if (vec.Length != 0)
			{
				int numBytes = (int)Math.Ceiling(vec.Length / 8.0);
				byte[] bytes = new byte[numBytes];
				vec.CopyTo(bytes, 0);
				Write(bytes);
			}
		}
		/// <summary>
		/// Write the bit matrix.
		/// </summary>
		/// <param name="mat">The matrix to write as an array of BitArray instances, which must not be null and must not contain null rows</param>
		public void Write(BitArray[] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(bool[] vec)
		{
			Write(new BitArray(vec));
		}
		private void SaveInner(bool[] vec)
		{
			SaveInner(new BitArray(vec));
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(bool[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(string[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(string[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(string[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the Guid to the output.
		/// </summary>
		/// <param name="val">The Guid to write</param>
		public void Write(Guid val)
		{
			Write(val.ToByteArray());
		}
		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(Guid[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(Guid[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(Guid[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}

		/// <summary>
		/// Write the Var to the output.
		/// </summary>
		/// <param name="val">The Var to write</param>
		public void Write(Var val)
		{
			Write((string)val);
		}
		/// <summary>
		/// Write the array to the output.
		/// </summary>
		/// <param name="vec">The array to write, which must not be null</param>
		public void Write(Var[] vec)
		{
			WriteInt(vec.Length);
			SaveInner(vec);
		}
		private void SaveInner(Var[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Write(vec[i]);
			}
		}
		/// <summary>
		/// Write the matrix to the output.
		/// </summary>
		/// <param name="mat">The matrix to write, which must not be null and must not contain null rows</param>
		public void Write(Var[][] mat)
		{
			WriteInt(mat.Length);
			if (mat.Length == 0)
			{
				WriteInt(0);
				return;
			}
			// check for uniform row length:
			int width = mat[0].Length;
			for (int j = 1; j < mat.Length; j++)
			{
				if (mat[j].Length != width)
				{
					// non-uniform:
					WriteInt(-1);
					for (j = 0; j < mat.Length; j++)
					{
						Write(mat[j]);
					}
					return;
				}
			}
			// uniform:
			WriteInt(width);
			for (int j = 0; j < mat.Length; j++)
			{
				SaveInner(mat[j]);
			}
		}
		#endregion

	}

}

