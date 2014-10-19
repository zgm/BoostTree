
using System;
using System.IO;
using System.Collections;
using Microsoft.TMSN.IO;

namespace Microsoft.TMSN
{
    /// <summary>
    /// generic interface that encapsulate futher processing/derivation of the data such as:
    /// generating pairs, sampleing/filtering data, hole-filling, computing scores (NDCG), testing, training ....
    /// </summary>
    public interface IDataProc<D, T> where D : IData 
    {
        //take in the data extracted from tsv file and do something with it...
        T Process(D data);
    }

    public class DataNullProc<D> : IDataProc<D, D>
        where D : IData
    {
        //default (noop) implementation
        //derive other classes for more sophisticated computation
        public D Process(D data)
        {
            return (D) data.Clone();            
        }
    }    

    //A sample Data process object that dump the data out
    public class MsnDataProc : IDataProc<MsnData, MsnData>
    {
        public MsnData Process(MsnData data)
        {
            MsnData msnData = (MsnData)data;
            
            FeatureData featureData = (FeatureData)msnData.Feature;
            MetaData metaData = (MetaData)msnData.Meta;
            LabelData labelData = (LabelData)msnData.Labels;
            
            
            for (int i = 0; i < featureData.Data.NumRows; i++)
            {
                string line = null;
                for (int j = 0; j < metaData.Data.NumCols; j++)
                {
                    line += " " + metaData.Data.GetValue(i, j);
                }

                for (int j = 0; j < labelData.Data.NumCols; j++)
                {
                    line += " " + labelData.Data.GetValue(i, j);
                }

                for (int j = 0; j < featureData.Data.NumCols; j++)
                {
                    line += " " + featureData.Data.GetValue(i,j);
                }                

                Console.WriteLine(line);
            }

            return msnData;
        }
    }
}