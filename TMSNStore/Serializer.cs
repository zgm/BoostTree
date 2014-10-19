using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;


namespace Microsoft.TMSN.Data 
{
    internal class Serializer
    {

        private Serializer()
        { 
        
        }
        
        public static byte[] Serialize(Dictionary<string, int> edge)
        {
            BinaryFormatter formatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();
            formatter.Serialize(stream, edge);
            return stream.ToArray();
        }

        public static Dictionary<string, int> Deserialize(Stream stream)
        {
            BinaryFormatter formatter = new BinaryFormatter();
            stream.Position = 0;
            return (Dictionary<string,int>)formatter.Deserialize(stream);
        }
    }
}
