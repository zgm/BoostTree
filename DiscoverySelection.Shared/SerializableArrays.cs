using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace DiscoverySelection.Shared
{
    public class SerializableUInt64Array : SerializableObject
    {
        public ulong[] array;
        public int arraySize;

        public SerializableUInt64Array(int length)
        {
            arraySize = length;
            array = new ulong[length];
        }

        public override void Serialize(System.IO.BinaryWriter bw)
        {
            bw.Write(arraySize);
            for (int i = 0; i < arraySize; i++)
            {
                bw.Write(array[i]);
            }
        }

        public override void Deserialize(System.IO.BinaryReader br)
        {
            arraySize = br.ReadInt32();
            array = new ulong[arraySize];
            for (int i = 0; i < arraySize; i++)
            {
                array[i] = br.ReadUInt64();
            }
        }

        public override string ToString()
        {
            string str = "";
            for (int i = 0; i < arraySize; i++)
            {
                str += array[i].ToString() + ";";
            }
            return str;
        }
    }

    public class SerializableFloatArray : SerializableObject
    {
        public float[] array;
        public int arraySize;

        public SerializableFloatArray(int length)
        {
            arraySize = length;
            array = new float[length];
        }

        public override void Serialize(System.IO.BinaryWriter bw)
        {
            bw.Write(arraySize);
            for (int i = 0; i < arraySize; i++)
            {
                bw.Write(array[i]);
            }
        }

        public override void Deserialize(System.IO.BinaryReader br)
        {
            arraySize = br.ReadInt32();
            array = new float[arraySize];
            for (int i = 0; i < arraySize; i++)
            {
                array[i] = br.ReadSingle();
            }
        }

        public override string ToString()
        {
            string str = "";
            for (int i = 0; i < arraySize; i++)
            {
                str += array[i].ToString() + ";";
            }
            return str;
        }
    }

    public class SerializableDoubleArray : SerializableObject
    {
        public double[] array;
        public int arraySize;

        public SerializableDoubleArray(int length)
        {
            arraySize = length;
            array = new double[length];
        }

        public override void Serialize(System.IO.BinaryWriter bw)
        {
            bw.Write(arraySize);
            for (int i = 0; i < arraySize; i++)
            {
                bw.Write(array[i]);
            }
        }

        public override void Deserialize(System.IO.BinaryReader br)
        {
            arraySize = br.ReadInt32();
            array = new double[arraySize];
            for (int i = 0; i < arraySize; i++)
            {
                array[i] = br.ReadDouble();
            }
        }

        public override string ToString()
        {
            string str = "";
            for (int i = 0; i < arraySize; i++)
            {
                str += array[i].ToString() + ";";
            }
            return str;
        }
    }
}