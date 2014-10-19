using System;
using System.IO;
using System.Threading;

namespace Microsoft.TMSN.Data {
	internal class VarLenFifo {
		private static int _fifoMemSize = 20; // max is ten bytes
		private byte[] _fifo = new byte[_fifoMemSize];
		private int _nextPosition;

		public VarLenFifo() {}

		public void PushInitial(uint value) {
			_nextPosition = _fifoMemSize - 1;  // start filling at right edge
			_fifo[_nextPosition] = (byte)(0x7F & value);
		}

		public void PushInitial(ulong value) {
			_nextPosition = _fifoMemSize - 1;  // start filling at right edge

			_fifo[_nextPosition] = (byte)(0x7F & value);
		}
			
		public void PushPartial(uint value) {
			_fifo[--_nextPosition] = (byte)(0x80 | (0x7F & value));
		}

		public void PushPartial(ulong value) {
			_fifo[--_nextPosition] = (byte)(0x80 | (0x7F & value));
		}
			
		public void Write(Stream outstream) {
			//              byte[],  offset ,      count
			outstream.Write(_fifo, _nextPosition, _fifoMemSize - _nextPosition);
		}

		public void Write(BinaryWriter writer) {
			//              byte[],  offset ,      count
			writer.Write(_fifo, _nextPosition, _fifoMemSize - _nextPosition);
		}

		public int PutBytes(byte[] dest, int startIndex) {
			int numBytes = Size;
			Buffer.BlockCopy(_fifo, _nextPosition, dest, startIndex, numBytes);
			return numBytes;
		}

		public int Size {
			get {
				return _fifoMemSize - _nextPosition;
			}
		}
	}

	/// <summary>
	/// Binary writer that includes writing variable length uints.
	/// </summary>
	public class VariableLengthBinaryWriter : BinaryWriter {
		private VarLenFifo _fifo = new VarLenFifo();


		/// <summary>
		/// Binary writer capable of writing variable length encoded values
		/// </summary>
		/// <param name="outstream"></param>
		public VariableLengthBinaryWriter(Stream outstream) : base(outstream) {}

		/// <summary>
		/// Writes a variable length uint to the output stream.
		/// </summary>
		/// <param name="value">The uint to be written.</param>
		/// <returns>Number of bytes written to the output stream.</returns>
		public int WriteVariableLength(uint value) {
			if (value < 0x80) {
				base.Write((byte)value);
				return 1;
			}

			_fifo.PushInitial(value);
			do {
				value >>= 7;
				_fifo.PushPartial(value);
			} while (value >= 0x80);

			//_fifo.Write(base.BaseStream);
			_fifo.Write(this);
			return _fifo.Size;
		}

		/// <summary>
		/// Writes a variable length ulong to the output stream.
		/// </summary>
		/// <param name="value">The long to be written.</param>
		public int WriteVariableLength(ulong value) {
			if (value < 0x80) {
				base.Write((byte)value);
				return 1;
			}

			_fifo.PushInitial(value);
			do {
				value >>= 7;
				_fifo.PushPartial(value);
			} while (value >= 0x80);

			//_fifo.Write(base.BaseStream);
			_fifo.Write(this);
			return _fifo.Size;
		}
	}

	/// <summary>
	/// Binary reader that includes reading variable length encoded values.
	/// </summary>
	public class VariableLengthBinaryReader : BinaryReader {
		/// <summary>
		/// Constructor for variable length binary reader.
		/// </summary>
		/// <param name="input">The intput stream.</param>
		public VariableLengthBinaryReader(Stream input) : base(input) {
		}

		/// /// <summary>
		/// Readers a variable number of bytes as a uint.
		/// </summary>
		/// <returns>uint value written to stream.</returns>
		public uint ReadVariableLength() {
			byte byte0 = base.ReadByte();
			if (byte0 < 0x80) return (uint)byte0;

			uint readValue = 0;

			do {
				readValue <<= 7;
				readValue |= (uint)(0x7F & byte0);
				byte0 = base.ReadByte();

			} while (byte0 >= 0x80);

			readValue = (readValue << 7) | byte0;

			return readValue;
		}

		/// /// <summary>
		/// Readers a variable number of bytes as a uint.
		/// </summary>
		/// <returns>uint value written to stream.</returns>
		public uint ReadVariableLength(ref long currentPosition) {
			byte byte0 = base.ReadByte();
			currentPosition++;
			if (byte0 < 0x80) return (uint)byte0;

			uint readValue = 0;

			do {
				readValue <<= 7;
				readValue |= (uint)(0x7F & byte0);
				byte0 = base.ReadByte();
				currentPosition++;
			} while (byte0 >= 0x80);

			readValue = (readValue << 7) | byte0;

			return readValue;
		}

		/// /// <summary>
		/// Readers a variable number of bytes as a ulong.
		/// </summary>
		/// <returns>ulong value written to stream.</returns>
		public ulong ReadVariableLengthULong() {
			byte byte0 = base.ReadByte();
			if (byte0 < 0x80) return (ulong)byte0;

			ulong readValue = 0;

			do {
				readValue <<= 7;
				readValue |= (uint)(0x7F & byte0);
				byte0 = base.ReadByte();
			} while (byte0 >= 0x80);

			readValue = (readValue << 7) | byte0;

			return readValue;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="stream"></param>
		/// <returns></returns>
		public static ulong ReadVariableLengthULong(Stream stream) {
			int byte0 = stream.ReadByte();
			if (byte0 < 0x80) return (ulong)byte0;

			ulong readValue = 0;

			do {
				readValue <<= 7;
				readValue |= (uint)(0x7F & byte0);
				byte0 = stream.ReadByte();
			} while (byte0 >= 0x80);

			readValue = (readValue << 7) | (uint)byte0;

			return readValue;
		}

	}

	/// <summary>
	/// Class for converting decimals to and from variable length byte arrays.
	/// </summary>
	public class VariableLengthBitConverter {
		private VarLenFifo _fifo = null;

		/// <summary>
		/// Converts byte array to Uint32
		/// </summary>
		/// <param name="value">input byte array.</param>
		/// <param name="startIndex">offset in array at which to start conversion.</param>
		/// <returns>Uint32 value.</returns>
		public static uint ToUint32(byte[] value, int startIndex) {
			int varLength;
			return ToUint32(value, startIndex, out varLength);
		}

		/// <summary>
		/// Converts byte array to Uint32
		/// </summary>
		/// <param name="value">input byte array.</param>
		/// <param name="startIndex">offset in array at which to start conversion.</param>
		/// <param name="varLength">returns the number of bytes used by the Uint32.</param>
		/// <returns>Uint32 value.</returns>
		public static uint ToUint32(byte[] value, int startIndex, out int varLength) {
			int i = startIndex;
			byte byte0 = value[i++];
			if (byte0 < 0x80) {
				varLength = 1;
				return (uint)byte0;
			}

			uint readValue = 0;

			do {
				readValue <<= 7;
				readValue |= (uint)(0x7F & byte0);
				byte0 = value[i++];
			} while (byte0 >= 0x80);

			readValue = (readValue << 7) | byte0;

			varLength = i - startIndex;
			return readValue;
		}

		/// <summary>
		/// Converts byte array to Uint64
		/// </summary>
		/// <param name="value">input byte array.</param>
		/// <param name="startIndex">offset in array at which to start conversion.</param>
		/// <returns>Uint64 value.</returns>
		public static ulong ToUint64(byte[] value, int startIndex) {
			int varLength;
			return ToUint64(value, startIndex, out varLength);
		}

		/// <summary>
		/// Converts byte array to Uint64
		/// </summary>
		/// <param name="value">input byte array.</param>
		/// <param name="startIndex">offset in array at which to start conversion.</param>
		/// <param name="varLength">returns the number of bytes used by the Uint64.</param>
		/// <returns>Uint64 value.</returns>
		public static ulong ToUint64(byte[] value, int startIndex, out int varLength) {
			int i = startIndex;
			int byte0 = value[i++];
			if (byte0 < 0x80) {
				varLength = 1;
				return (ulong)byte0;
			}

			ulong readValue = 0;

			do {
				readValue <<= 7;
				readValue |= (uint)(0x7F & byte0);
				byte0 = value[i++];
			} while (byte0 >= 0x80);

			readValue = (readValue << 7) | (uint)byte0;

			varLength = i - startIndex;
			return readValue;
		}

		private VarLenFifo Fifo {
			get {
				if (_fifo == null) {
					_fifo = new VarLenFifo();
				}

				return _fifo;
			}
		}

		/// <summary>
		/// Converts Uint32 to byte array.
		/// </summary>
		/// <param name="value">Value to be converted</param>
		/// <returns>Byte array representing value.</returns>
		public byte[] GetBytes(uint value) {
			byte[] ret = null;

			if (value < 0x80) {
				ret = new byte[1];
				ret[0] = (byte)value;
				return ret;
			}

			Fifo.PushInitial(value);
			do {
				value >>= 7;
				Fifo.PushPartial(value);
			} while (value >= 0x80);

			ret = new byte[Fifo.Size];
			Fifo.PutBytes(ret, 0);
			
			return ret;
		}

		/// <summary>
		/// Converts Uint64 to byte array.
		/// </summary>
		/// <param name="value">Value to be converted</param>
		/// <returns>Byte array representing value.</returns>
		public byte[] GetBytes(ulong value) {
			byte[] ret = null;
			ulong inVal = value;

			if (value < 0x80) {
				ret = new byte[1];
				ret[0] = (byte)value;
				return ret;
			}

			Fifo.PushInitial(value);
			do {
				value >>= 7;
				Fifo.PushPartial(value);
			} while (value >= 0x80);

			ret = new byte[Fifo.Size];
			Fifo.PutBytes(ret, 0);
			return ret;
		}

		/// <summary>
		/// Places variable length representation of Uint32 into destination byte array.
		/// </summary>
		/// <param name="value">Value to be converted.</param>
		/// <param name="dest">Byte array to write converted bytes into to.</param>
		/// <param name="startIndex">Offset into destination at which to write bytes.</param>
		/// <returns>num bytes put into the destination</returns>
		public int PutBytes(uint value, byte[] dest, int startIndex) {
			if (value < 0x80) {
				dest[startIndex] = (byte)value;
				return 1;
			}

			Fifo.PushInitial(value);
			do {
				value >>= 7;
				Fifo.PushPartial(value);
			} while (value >= 0x80);

			Fifo.PutBytes(dest, startIndex);
			return Fifo.Size;
		}

		/// <summary>
		/// Places variable length representation of Uint64 into destination byte array.
		/// </summary>
		/// <param name="value">Value to be converted.</param>
		/// <param name="dest">Byte array to write converted bytes into to.</param>
		/// <param name="startIndex">Offset into destination at which to write bytes.</param>
		/// <returns>num bytes put into the destination</returns>
		public int PutBytes(ulong value, byte[] dest, int startIndex) {
			if (value < 0x80) {
				dest[startIndex] = (byte)value;
				return 1;
			}

			Fifo.PushInitial(value);
			do {
				value >>= 7;
				Fifo.PushPartial(value);
			} while (value >= 0x80);

			Fifo.PutBytes(dest, startIndex);
			return Fifo.Size;
		}
	}

}
