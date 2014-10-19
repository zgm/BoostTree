
using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Diagnostics;
using Microsoft.TMSN.IO;
using System.Text.RegularExpressions;

namespace StochasticGradientBoost
{
    [Serializable]
    class FeatureTbl
    {
        //todo (qiangwu): make featureIdxTbl.Comparer case insensitive
        Dictionary<string, int> featureIdxTbl;
        public FeatureTbl(string[] featureNames)
        {
            featureIdxTbl = new Dictionary<string, int>(featureNames.Length);
            for (int i = 0; i < featureNames.Length; i++)
            {
                featureIdxTbl.Add(featureNames[i], i);
            }
        }
        
        public int FeatureIdx(string namePrefix, string streamName, int wordIdx)
        {
            string featureName = FeatureName(namePrefix, streamName, wordIdx);

            int idx = -1;

            if (featureIdxTbl.ContainsKey(featureName))
            {
                idx = featureIdxTbl[featureName];
            }            
            
            return idx;
        }

        private string FeatureName(string namePrefix, string streamName, int wordIdx)
        {
            string featureName = namePrefix;

            if (streamName != null)
            {
                featureName += "_" + streamName;                
            }
                            
            if (wordIdx >= 0)
            {
                featureName += "_" + wordIdx;
            }
            
            return featureName;
        }

    }

    [Serializable]
    class BM25Stream
    {        
        public static BM25Stream[] Create(string[] parameters, int idxStart)
        {
            List<BM25Stream> bm25StreamList = new List<BM25Stream>(30);

            for (int i = idxStart; i < parameters.Length; i++)
            {
                string[] fields = parameters[i].Split('=');
                string name = fields[0];
                string value = fields[1];
                
                fields = name.Split(':');
                if (fields.Length != 2)
                {
                    continue;
                }

                string namePara = fields[0];
                string nameStream = fields[1];

                if (IsParameter(namePara))
                {
                    BM25Stream bm25Stream = null;
                    for (int j = 0; j < bm25StreamList.Count; j++)
                    {
                        if (string.Compare(bm25StreamList[j].nameStream, nameStream, true) == 0)
                        {
                            bm25Stream = bm25StreamList[j];
                            break;
                        }
                    }
                    if (bm25Stream == null)
                    {
                        bm25Stream = new BM25Stream(nameStream);
                        bm25StreamList.Add(bm25Stream);
                    }

                    double dblValue = double.Parse(value);
                    bm25Stream.AddParameter(namePara, dblValue);
                }
            }

            return bm25StreamList.ToArray();
        }
       
        public BM25Stream(string nameStream)
        {
            this.nameStream = nameStream;
        }

        public void WriteMSNStyle(StreamWriter wStream)
        {
            wStream.WriteLine("AverageMetaStreamLength:{0}={1}", this.nameStream, this.dblAverageLength);
            wStream.WriteLine("MetaStreamWeight:{0}={1}", this.nameStream, this.dblWeight);
            wStream.WriteLine("MetaStreamLengthNorm:{0}={1}", this.nameStream, this.dblLengthNorm);
        }
        
        public double Weight
        {
            get
            {
                return this.dblWeight;
            }
        }

        public double LengthNorm
        {
            get
            {
                return this.dblLengthNorm;
            }
        }

        public double AverageLength
        {
            get
            {
                return this.dblAverageLength;
            }
        }

        public string StreamName
        {
            get
            {
                return this.nameStream;
            }
        }

        private void AddParameter(string paramName, double value)
        {
            if (string.Compare(paramName, "AverageMetaStreamLength", true) == 0)
            {
                this.dblAverageLength = value;
            }
            else if (string.Compare(paramName, "MetaStreamWeight", true) == 0)
            {
                this.dblWeight = value;
            }
            else if (string.Compare(paramName, "MetaStreamLengthNorm", true) == 0)
            {
                this.dblLengthNorm = value;
            }
            else
            {
                throw (new Exception("Un-Recognized Stream parameter for BM25F2"));
            }
        }

        private double dblWeight;
        private double dblLengthNorm;
        private double dblAverageLength;
        private string nameStream;

        static private bool IsParameter(string nameParameter)
        {
            string[] const_strParameters = { "AverageMetaStreamLength", "MetaStreamWeight", "MetaStreamLengthNorm" };
            for (int i = 0; i<const_strParameters.Length; i++)
            {
                if (string.Compare(nameParameter, const_strParameters[i], true) == 0)
                {
                    return true;
                }
            }
            return false;
        }        
    }

    [Serializable]
    class BM25F2Transform : TransformFunction
    {
        public BM25F2Transform(string[] parameters, int idxStart)
        {
            //(1) parsing the BM25F2 section of the model configuration file            
            for (int i = idxStart; i < parameters.Length; i++)
            {
                string[] fields = parameters[i].Split('=');
                string name = fields[0];                
                if (string.Compare(name, "SaturationParameter", true) == 0)
                {
                    this.dblSaturation = double.Parse(fields[1]);
                    break;
                }                
            }
            this.bm25Streams = BM25Stream.Create(parameters, idxStart);                      
        }

        public BM25F2Transform(BM25F2Transform bm25F2Transform)
        {                    
            this.dblSaturation = bm25F2Transform.dblSaturation;
            this.bm25Streams = bm25F2Transform.bm25Streams; ;
            this.featureTbl = bm25F2Transform.featureTbl; ;
        }

        public override void WriteMSNStyle(StreamWriter wStream, string ftrName, string funcName)
        {
            if (ftrName != null)
            {
                wStream.WriteLine("Name={0}", ftrName);
            }
            wStream.WriteLine("Transform={0}", funcName);
            wStream.WriteLine("SaturationParameter={0}", this.dblSaturation);
            for (int i = 0; i < this.bm25Streams.Length; i++)
            {
                this.bm25Streams[i].WriteMSNStyle(wStream);
            }
        }

        public override object Clone()
        {
            return new BM25F2Transform(this);
        }

        public override bool SetFeatureNames(string[] FeatureNames, string featName)
        {
            this.featureTbl = new FeatureTbl(FeatureNames);
            return true;
        }

        public override float Apply(float[] inputs)
        {
            const UInt32 MAX_QUERY_WORDS = 100;             
            UInt32 cWordsInQuery = this.FeatureValueLoopup(inputs, "NumberOfWordsInQuery");

            if (cWordsInQuery > MAX_QUERY_WORDS)
            {
                //warning at least??
                cWordsInQuery = MAX_QUERY_WORDS;                
            }

            // Calculate the BM25 contribution from each word in the query
            double vBM25 = 0.0;
            for (int iWord=0; iWord<cWordsInQuery; iWord++)
            {
                vBM25 += CalculateWordBM25(iWord, inputs);
            }

            return (float)vBM25;
        }

        private double CalculateWordBM25(int iWord, float[] inputs)
        {
            // Get document frequency information            
            double dblAdjustedTermFrequency = CalculateTermFrequency(iWord, inputs);
            UInt32 dwDocCounts = CalculateDocumentCounts(iWord, inputs);

            //calculate IDF
            double dblIdf = 0.0F;
            //qiangwu: the constant c_dblTotalDocumentCounts will not normalize across all index sizes??
            if (c_dblTotalDocumentCounts/2.0 > dwDocCounts)
            {
                dblIdf = Math.Log(
                    (c_dblTotalDocumentCounts - (double)dwDocCounts + 0.5) /
                    ((double) dwDocCounts + 0.5)
                    );
            }

            double dblWordBM25 = dblIdf * dblAdjustedTermFrequency / 
                (this.dblSaturation + dblAdjustedTermFrequency);

            return dblWordBM25;
        }

        private double CalculateTermFrequency(int iWord, float[] inputs)
        {              
            // Calculate values for each metastream            
            double dblAdjustedTermFrequency = 0;            

            for (int iStream = 0; iStream < this.bm25Streams.Length; iStream++)
            {
                if (this.bm25Streams[iStream].Weight > 0)
                {
                    double dblAdjustedStreamTermFrequency = CalculateStreamTermFrequency(iWord, iStream, inputs);
                    
                    dblAdjustedTermFrequency += dblAdjustedStreamTermFrequency;
                }
            }

            return dblAdjustedTermFrequency;            
        }

        private double CalculateStreamTermFrequency(int iWord, int iStream, float[] inputs)
        {
            double dblAdjustedStreamTermFrequency = 0;

            // Get number of occurrences
            UInt32 dwTermCounts = FeatureValueLoopup(inputs, "NumberOfOccurrences", this.bm25Streams[iStream].StreamName, iWord);

            UInt32 dwStreamLength = FeatureValueLoopup(inputs, "StreamLength", this.bm25Streams[iStream].StreamName);

            if (dwTermCounts > dwStreamLength)
            {
                // Stream can't be shorter than the NumberOfOccurrences
                dwStreamLength = dwTermCounts;
            }

            if (dwTermCounts != 0)
            {
                double dblNumerator = this.bm25Streams[iStream].Weight *
                    (double)dwTermCounts;

                double dblNormalizedStreamLength =
                    dwStreamLength /
                    this.bm25Streams[iStream].AverageLength - 1.0;

                double dblDenominator = 1.0 + this.bm25Streams[iStream].LengthNorm *
                    dblNormalizedStreamLength;

                dblAdjustedStreamTermFrequency = dblNumerator / dblDenominator;
            }

            return dblAdjustedStreamTermFrequency;
        }

        private UInt32 CalculateDocumentCounts(int iWord, float[] inputs)
        {
            UInt32 dwDocCounts = 0;

            for (int iStream = 0; iStream < this.bm25Streams.Length; iStream++)
            {
                UInt32 dwDocStreamCounts = FeatureValueLoopup(inputs, "DocumentCounts", this.bm25Streams[iStream].StreamName, iWord);

                if (dwDocCounts < dwDocStreamCounts)
                {
                    dwDocCounts = dwDocStreamCounts;
                }                
            }

            UInt32 dwDocCounts1 = this.FeatureValueLoopup(inputs, "DocumentFrequency", iWord);
            if (dwDocCounts1 > 0)
            {
                dwDocCounts = dwDocCounts1;
            }

            return dwDocCounts;
        }

        private UInt32 FeatureValueLoopup(float[] inputs, string prefix)
        {
            return  FeatureValueLoopup(inputs, prefix, null, -1);
        }

        private UInt32 FeatureValueLoopup(float[] inputs, string prefix, string streamName)
        {
            return FeatureValueLoopup(inputs, prefix, streamName, -1);
        }

        private UInt32 FeatureValueLoopup(float[] inputs, string prefix, int iWord)
        {
            return FeatureValueLoopup(inputs, prefix, null, iWord);
        }

        private UInt32 FeatureValueLoopup(float[] inputs, string prefix, string streamName, int iWord)
        {             
            int iFeature = this.featureTbl.FeatureIdx(prefix, streamName, iWord);

            UInt32 featVal = 0;
            if (iFeature < 0)
            {
                if (streamName == null && iWord < 0)
                {
                    throw (new Exception("Feature " + streamName + " dose not exist"));
                }
            }
            else
            {
                featVal = (UInt32)inputs[iFeature];
            }
            return featVal;
        }

        private double dblSaturation;
        private BM25Stream[] bm25Streams;
        private FeatureTbl featureTbl;

        private const double c_dblTotalDocumentCounts = 1000000000.0;
    }

    [Serializable]
    class LogBM25F2Transform : TransformFunction
    {
        public LogBM25F2Transform(string[] parameters, int idxStart)
        {
            //(1) parsing the LogBM25F2 section of the model configuration file            
            for (int i = idxStart; i < parameters.Length; i++)
            {
                string[] fields = parameters[i].Split('=');
                string name = fields[0];                
                if (string.Compare(name, "Slope", true) == 0)
                {
                    this.dblSlope = double.Parse(fields[1]);                    
                }
                else if (string.Compare(name, "Multiplier", true) == 0)
                {
                    this.dblMultiplier = double.Parse(fields[1]);
                }
                else if (string.Compare(name, "Intercept", true) == 0)
                {
                    this.dblIntercept = double.Parse(fields[1]);
                }
            }

            this.bm25F2Transform = new BM25F2Transform(parameters, idxStart);
        }

        public LogBM25F2Transform(LogBM25F2Transform logbm25F2Transform)
        {
            this.dblSlope = logbm25F2Transform.dblSlope;
            this.dblMultiplier = logbm25F2Transform.dblMultiplier;
            this.dblIntercept = logbm25F2Transform.dblIntercept;

            this.bm25F2Transform = new BM25F2Transform(logbm25F2Transform.bm25F2Transform);
        }

        public override void WriteMSNStyle(StreamWriter wStream, string ftrName, string funcName)
        {
            this.bm25F2Transform.WriteMSNStyle(wStream, ftrName, funcName);

            wStream.WriteLine("Slope={0}", this.dblSlope);
            wStream.WriteLine("Intercept={0}", this.dblIntercept);
            wStream.WriteLine("Multiplier={0}", this.dblMultiplier);
        }

        public override object Clone()
        {
            return new LogBM25F2Transform(this);
        }

        public override bool SetFeatureNames(string[] FeatureNames, string featName)
        {
            return this.bm25F2Transform.SetFeatureNames(FeatureNames, featName);
        }

        public override float Apply(float[] inputs)
        {
            double dblBM25 = this.bm25F2Transform.Apply(inputs);
            double dblRetVal = this.dblSlope * Math.Log(this.dblMultiplier * dblBM25 + 1) + this.dblIntercept;
            return (float)dblRetVal;
        }

        private BM25F2Transform bm25F2Transform;

        private double dblSlope; 
        private double dblMultiplier;
        private double dblIntercept;
    }
}