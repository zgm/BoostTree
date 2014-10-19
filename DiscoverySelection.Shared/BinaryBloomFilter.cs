using System;

namespace DiscoverySelection.Shared
{
    [Serializable()]
    public class BinaryBloomFilter : BloomFilter<byte[]>
    {
        public BinaryBloomFilter(int tableSize, int nKeys)
            : base(tableSize, nKeys)
        {
        }

        protected override int CreateHash1(byte[] val)
        {
            int hash = 0;

            for (int i = 0; i < val.Length; i++)
            {
                hash += val[i];
                hash += (hash << 6);
                hash ^= (hash >> 10);
            }

            hash += (hash << 15);
            hash ^= (hash >> 11);
            hash += (hash << 3);
            
            return hash;
        }

        protected override int CreateHash2(byte[] val)
        {
            int hash = 0;

            // a few magic values to create the second hash
            for (int i = 0; i < val.Length; i++)
            {
                hash += val[i];
                hash += (hash << 10);
                hash ^= (hash >> 6);
            }

            hash += (hash << 3);
            hash ^= (hash >> 11);
            hash += (hash << 15);

            return hash;
        }
    }
}