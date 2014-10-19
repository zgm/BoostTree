using System;
using System.IO;

namespace DiscoverySelection.Shared
{
    public abstract class SerializableObject
    {
        /// <summary>
        /// Serializes instance to BinaryWriter
        /// </summary>        
        public abstract void Serialize(BinaryWriter bw);

        /// <summary>
        /// Deserializes instance from BinaryReader
        /// </summary>
        /// <param name="br"></param>
        public abstract void Deserialize(BinaryReader br);

        public Byte[] Serialize()
        {
            MemoryStream ms = new MemoryStream();
            Byte[] result = null;
            using (BinaryWriter bw = new BinaryWriter(ms))
            {
                Serialize(bw);
                result = ms.ToArray();
            }
            return result;
        }

        public String SerializeToHex()
        {
            Byte[] bytes = Serialize();
            return Util.ByteArrayToHexString( bytes );
        }
        
        public void Deserialize(Byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0)
            {
                throw new Exception("Serialized object is null or has a zero length");
            }

            MemoryStream ms = new MemoryStream(bytes);
            using (BinaryReader br = new BinaryReader(ms))
            {
                Deserialize(br);

                if (ms.Position != bytes.Length)
                {
                    throw new Exception("Extra bytes detected when deserializing object");
                }
            }
        }

        public void DeserializeFromHex( String strHex )
        {
            Deserialize( Util.HexStringToByteArray( strHex) );
        }

    }
}
