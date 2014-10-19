using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.TMSN;
using Microsoft.TMSN.IO;

namespace StochasticGradientBoost
{
    public interface Subset
    {
        bool Keep(int iData);
    }

    public class TopNSet : Subset
    {
        public TopNSet(LabelFeatureData subModelScore, int N)
        {
            this.Datakept = new bool[subModelScore.NumDataPoint];
            for (int i = 0; i < subModelScore.DataGroups.GroupCounts; i++)
            {
                DataGroup dataGroup = subModelScore.DataGroups[i];
                float[] scores = new float[dataGroup.cSize];
                int[] idx = new int[dataGroup.cSize];

                for (int j = 0; j < dataGroup.cSize; j++)
                {
                    this.Datakept[dataGroup.iStart + j] = false;
                    float[] features = subModelScore.GetFeature(dataGroup.iStart + j);
                    scores[j] = 0-features[0]; //sort is in increasing order
                    idx[j] = j;
                }

                Array.Sort(scores, idx);
                for (int j = 0; j < dataGroup.cSize; j++)
                {
                    if (j >= N)
                    {
                        break;
                    }
                    this.Datakept[dataGroup.iStart+idx[j]] = true;
                }                
            }
        }

        public bool Keep(int iData)
        {
            return this.Datakept[iData];
        }

        bool[] Datakept; 
    }

    public class CLabelFeatureDataSubset : CLabelFeatureData
    {
        public CLabelFeatureDataSubset(CLabelFeatureData labelFeatureData, Subset subset) 
            :base(labelFeatureData)
        {
            this.cSubSet = 0;
            this.mapTbl = new int[labelFeatureData.NumDataPoint];
            for (int i = 0; i < labelFeatureData.NumDataPoint; i++)
            {
                if (subset.Keep(i))
                {
                    this.mapTbl[this.cSubSet++] = i;
                }
            }            
        }

        // the total number of data points
        override public int NumDataPoint
        {
            get
            {
                return this.cSubSet;                
            }
        }       

        override public float GetLabel(int iDataIn)
        {
            int iData = this.mapTbl[iDataIn];
            return base.GetLabel(iData);			
        }
        
        override public int GetGroupId(int iDataIn)
        {
            int iData = this.mapTbl[iDataIn];
            return base.GetGroupId(iData);			
        }
                	
        override public DataGroups DataGroups
        {            
            get
            {
                if (null == m_dataGroups)
                {                   
                    this.m_dataGroups = new DataGroups(this);					
                }
                return m_dataGroups;
            }				
        }
       
        //iFeature is the index on the active feature not all the avaliable features
        override public float GetFeature(int iFeature, int iDataIn)
        {
            int iData = this.mapTbl[iDataIn];
            return base.GetFeature(iFeature, iData);
        }

        override public float[] GetFeature(int iDataIn)
        {
            int iData = this.mapTbl[iDataIn];
            return base.GetFeature(iData);
        }

        //map the index in the subset data to its index in the original data
        protected int[] mapTbl;
        //the total number of the data points in the subset
        private int cSubSet;
        DataGroups m_dataGroups;
                 
    }

    /// <summary>
    /// Contains label and binned feature data
    /// </summary>    
    public class LabelFeatureDataCodedSubset : CLabelFeatureDataCoded
    {
        public LabelFeatureDataCodedSubset(CLabelFeatureDataCoded labelFeatureDataCoded, Subset subset)                        
            : base(labelFeatureDataCoded)
        {
            this.cSubSet = 0;
            this.mapTbl = new int[labelFeatureDataCoded.NumDataPoint];
            for (int i = 0; i < labelFeatureDataCoded.NumDataPoint; i++)
            {
                if (subset.Keep(i))
                {
                    this.mapTbl[this.cSubSet++] = i;
                }
            }            
        }

        // the total number of data points
        override public int NumDataPoint
        {
            get
            {
                return this.cSubSet;                
            }
        }       

        override public float GetLabel(int iDataIn)
        {
            int iData = this.mapTbl[iDataIn];
            return base.GetLabel(iData);			
        }

        override public int GetGroupId(int iDataIn)
        {
            int iData = this.mapTbl[iDataIn];
            return base.GetGroupId(iData);
        }
                	
        override public DataGroups DataGroups
        {            
            get
            {
                if (null == m_dataGroups)
                {                    
                    this.m_dataGroups = new DataGroups(this);					
                }
                return m_dataGroups;
            }				
        }

        override public ushort GetFeatureCoded(int iActiveFeature, int iDataIn)
        {
            int iData = this.mapTbl[iDataIn];
            return base.GetFeatureCoded(iActiveFeature, iData);            
        }

        //map the index in the subset data to its index in the original data
        protected int[] mapTbl;
        //the total number of the data points in the subset
        private int cSubSet;
        DataGroups m_dataGroups;
    } 
}
