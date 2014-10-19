using System;
using System.Collections;
using System.IO;
using System.IO.Compression;
using System.Runtime.Serialization.Formatters.Binary;

namespace DiscoverySelection.Shared
{
    [Serializable()]
    public abstract class BloomFilter<T>
    {
        protected BitArray hashbits;
        protected int numKeys;

        public BloomFilter(int tableSize, int nKeys)
        {
            Assert<int>.Greater(tableSize, 0, "Table size <= 0");
            Assert<int>.Greater(nKeys, 0, "Num keys <= 0");

            numKeys = nKeys;
            hashbits = new BitArray(tableSize);
        }

        public bool Test(T val)
        {
            int [] hashKeys = CreateHashes(val);
            // Test each hash key.  Return false
            //  if any one of the bits is not set.
            foreach (int hash in hashKeys)
            {
                if (!hashbits[hash])
                    return false;
            }
            // All bits set.  The item is there.
            return true;
        }

        // returns false if the item is definitely not already in the table
        // the function can return true if the item is not in the table (with a small error rate)
        public bool Add(T val)
        {
            // Initially assume that the item is in the table
            bool isIn = true;
            int [] hashKeys = CreateHashes(val);

            foreach (int hash in hashKeys)
            {
                if (!hashbits[hash])
                {
                    // One of the bits wasn't set, so show that
                    // the item wasn't in the table, and set that bit.
                    isIn = false;
                    hashbits[hash] = true;
                }
            }

            return isIn;
        }

        public void CombineWith(BloomFilter<T> another)
        {
            Assert<int>.Equal(numKeys, another.numKeys);
            Assert<int>.Equal(hashbits.Length, another.hashbits.Length);

            for (int i = 0; i < hashbits.Length; i++)
            {
                hashbits[i] |= another.hashbits[i];
            }
        }

        protected virtual int [] CreateHashes(T val)
        {
            int[] hashKeys = new int[numKeys];

            int hash1 = CreateHash1(val);
            int hash2 = CreateHash2(val);

            hashKeys[0] = Math.Abs(hash1 % hashbits.Count);

            if (numKeys > 1)
            {
                for (int i = 1; i < numKeys; i++)
                {
                    hashKeys[i] = Math.Abs((hash1 + (i * hash2)) %
                        hashbits.Count);
                }
            }

            return hashKeys;
        }

        public static void Serialize(string path, BloomFilter<T> obj)
        {
            BinaryFormatter formatter = new BinaryFormatter();
            Stream stream = File.Open(path, FileMode.Create, FileAccess.Write);

            using (stream)
            {
                formatter.Serialize(stream, obj);
            }
        }

        public static BloomFilter<T> Deserialize(string path)
        {
            BinaryFormatter formatter = new BinaryFormatter();
            Stream stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);

            BloomFilter<T> obj;

            using (stream)
            {
                obj = (BloomFilter<T>)formatter.Deserialize(stream);
            }

            return obj;
        }

        public static Byte [] Serialize(BloomFilter<T> obj)
        {
            BinaryFormatter formatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();

            formatter.Serialize(stream, obj);

            return stream.ToArray();
        }

        public static BloomFilter<T> Deserialize(Byte[] bytes)
        {
            BinaryFormatter formatter = new BinaryFormatter();
            Stream stream = new MemoryStream(bytes);

            return (BloomFilter<T>)formatter.Deserialize(stream);
        }

        protected abstract int CreateHash1(T val);

        protected abstract int CreateHash2(T val);
    }
}
