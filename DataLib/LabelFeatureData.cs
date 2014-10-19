#define QUANTIZER_TIMER
//#define VERBOSE
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Diagnostics;
using Microsoft.TMSN;
using Microsoft.TMSN.IO;

namespace StochasticGradientBoost
{
    public enum DataFileType
    {
        TsvFile,
        BinFile,
        DpFile       
    }

    public enum DataPartitionType
    {
        Train = 0,
        Validation,
        Test,
        cTypes, // the total number of types should always be the last one
    }
    
    public class TsvFileLoader
    {
        //return a list of features specified in the inputFeatureFile
        public static string[] ReadFeatureNames(string inputFeatureFile)
        {
            List<string> activeFeatureList = new List<string>();
            StreamReader rStream = new StreamReader(inputFeatureFile);
            string feature = null;
            while ((feature = rStream.ReadLine()) != null)
            {
                activeFeatureList.Add(feature);
            }

            return activeFeatureList.ToArray();
        }

        //methods relates to object construction
        public TsvFileLoader(string tsvFileName) :
            this(CreateTsvFile(tsvFileName))
        {            
        }

        public TsvFileLoader(string tsvFileName, 
                                IParser<string> metaParser, IParser<float> labelParser, IParser<float> featureParser,
                                IGroupBoundary groupBoundary) :
            this(CreateTsvFile(tsvFileName, metaParser, labelParser, featureParser, groupBoundary))
        {            
        }

        public TsvFileLoader(string tsvFileName,
                                IParser<string> metaParser, IParser<float> labelParser, IParser<float> featureParser,
                                IGroupBoundary groupBoundary, Random r, double rangeLower, double rangeUpper) :
            this(CreateTsvFile(tsvFileName, metaParser, labelParser, featureParser, groupBoundary), r, rangeLower, rangeUpper)
        {
        }

        //this version of loader is for cluster distribution
        //take in the number of nodes, and current node number
        //we add in data only if it corresponds to our current node number
        public TsvFileLoader(RankingTSVFile<MsnData> tsvFile, Random r, double rangeLower, double rangeUpper)
        {
            DataNullProc<MsnData> dataNullPro = new DataNullProc<MsnData>();
            IDataEnum<MsnData, MsnData, DataNullProc<MsnData>> tsvDataEnum = new TsvDataStream<MsnData, MsnData, DataNullProc<MsnData>>(tsvFile, dataNullPro);

            int gId = 0;

            featureDataMatrix = new DataMatrixArray<float>();
            List<float> labelList = new List<float>();
            List<int> groupIdList = new List<int>();

            //for each query in the set of queries
            foreach (MsnData d in tsvDataEnum)
            {
                //if it falls in the "Test set" range, load it into the test set
                double rDouble = r.NextDouble();
                if (rDouble >= rangeLower && rDouble < rangeUpper)
                {
                    //load the example
                    if (featureNames == null)
                    {
                        featureNames = new string[d.Feature.NumColumns];
                        for (int i = 0; i < d.Feature.NumColumns; i++)
                        {
                            featureNames[i] = tsvFile.ColumnNames[d.Feature.Parser.columnIndex(i)];
                        }
                    }

                    //get labels
                    for (int i = 0; i < d.Labels.Data.NumRows; i++)
                    {
                        labelList.Add(d.Labels.Data.GetValue(i, 0));
                        groupIdList.Add(gId); // group index/Id					
                    }

                    //get feature data
                    featureDataMatrix.Add(d.Feature.Data);

                    gId++;
                }

                //otherwise, skip that query
            }

            int numRows = labelList.Count;
            labels = new float[numRows];
            groupId = new int[numRows];

            for (int i = 0; i < numRows; i++)
            {
                labels[i] = labelList[i];
                groupId[i] = groupIdList[i];
            }
        }

        public TsvFileLoader(RankingTSVFile<MsnData> tsvFile)
        {
            DataNullProc<MsnData> dataNullPro = new DataNullProc<MsnData>();
            IDataEnum<MsnData, MsnData, DataNullProc<MsnData>> tsvDataEnum = new TsvDataStream<MsnData, MsnData, DataNullProc<MsnData>>(tsvFile, dataNullPro);

            int gId = 0;

            featureDataMatrix = new DataMatrixArray<float>();
            List<float> labelList = new List<float>();
            List<int> groupIdList = new List<int>();

            foreach (MsnData d in tsvDataEnum)
            {
                if (featureNames == null)
                {
                    featureNames = new string[d.Feature.NumColumns];
                    for (int i = 0; i < d.Feature.NumColumns; i++)
                    {
                        featureNames[i] = tsvFile.ColumnNames[d.Feature.Parser.columnIndex(i)];
                    }
                }

                //get labels
                for (int i = 0; i < d.Labels.Data.NumRows; i++)
                {					
                    labelList.Add(d.Labels.Data.GetValue(i, 0));
                    groupIdList.Add(gId); // group index/Id					
                }

                //get feature data
                featureDataMatrix.Add(d.Feature.Data);

                gId++;
            }

            int numRows = labelList.Count;			
            labels = new float[numRows];
            groupId = new int[numRows];

            for (int i = 0; i < numRows; i++)
            {				
                labels[i] = labelList[i];
                groupId[i] = groupIdList[i];
            }           
        }

        public static RankingTSVFile<MsnData> CreateTsvFile(string tsvFileName)
        {
            return CreateTsvFile(tsvFileName, null, null, null, null);
        }

        public static RankingTSVFile<MsnData> CreateTsvFile(string tsvFileName,
                                IParser<string> metaParser, IParser<float> labelParser, IParser<float> featureParser,
                                IGroupBoundary groupBoundary)
        {
            MsnData msnData = new MsnData();

            if (metaParser != null)
            {
                msnData.Meta.Parser = metaParser;
            }
            else
            {
                msnData.Meta.Parser = DefaultMetaParser;
            }

            if (labelParser != null)
            {
                msnData.Labels.Parser = labelParser;
            }
            else
            {
                msnData.Labels.Parser = DefaultLabelParser;
            }

            if (featureParser != null)
            {
                msnData.Feature.Parser = featureParser;
            }

            RankingTSVFile<MsnData> tsvFile = new RankingTSVFile<MsnData>(tsvFileName, msnData);

            if (groupBoundary != null)
            {
                tsvFile.GroupBoundary = groupBoundary;
            }
            else
            {
                tsvFile.GroupBoundary = DefaultGroupBoundary;
            }

            return tsvFile;
        }
       
        // core data
        public string[] FeatureName
        {
            get
            {
                return featureNames;
            }
        }

        public float[] Labels
        {
            get
            {
                return labels;
            }
        }

        public int[] GroupId
        {
            get
            {
                return groupId;
            }
        }
        
        public IDataMatrix<float> Feature
        {
            get
            {
                return featureDataMatrix;
            }
        }

        // the total number of data points
        public int NumDataPoints
        {
            get
            {
                return Labels.Length;
            }
        }

        // the total number of features
        public int NumFeatures
        {
            get
            {
                return featureNames.Length;
            }
        }

        //Private data members
        private string[] featureNames;
        private float[] labels;
        private int[] groupId;

        DataMatrixArray<float> featureDataMatrix;
                    
        static private IParser<string> DefaultMetaParser
        {
            get
            {
                if (defaultMetaParser == null) 
                {
                    string[] metaNames = { "m:Source", };
                    defaultMetaParser = new MsnMetaParser(metaNames);
                }
                return defaultMetaParser;
            }
        }

        static private IParser<float> DefaultLabelParser
        {
            get
            {
                if (defaultLabelParser == null)
                {
                    string[] labelName = { "m:Label" };
                    defaultLabelParser = new MsnLabelParser(labelName);
                }
                return defaultLabelParser;
            }
        }

        static private IGroupBoundary DefaultGroupBoundary
        {
            get
            {
                if (defaultGroupBoundary == null)
                {
                    defaultGroupBoundary = new OnelineGroup();
                }
                return defaultGroupBoundary;
            }
        }

        static private IParser<string> defaultMetaParser;
        static private IParser<float> defaultLabelParser;
        static private IGroupBoundary defaultGroupBoundary;
    }

    /// <summary>
    /// properties and functions common to any LabelFeature data    
    /// </summary>
    public interface LabelFeatureCore
    {
        /// <summary>
        /// the total number of data points in the data set
        /// </summary>
        int NumDataPoint
        {
            get;           
        }
                
        /// <summary>
        /// The total number of features in the data sets
        /// The actual features can be controled/restricted by SetActiveFeatures
        /// </summary>
        int NumFeatures
        {
            get;
        }
        
        /// <summary>
        /// The name of the features in the data sets
        /// The feature index used by functions in this or derived interfaces has to respect this 
        /// - i.e. the name of the iFeature-th feature under consideration is FeatureNames[iFeature]
        /// The actual features can be controls/restricted by SetActiveFeatures
        /// </summary>
        string[] FeatureNames
        {
            get;
        }

        /// <summary>
        /// Data can be partitioned into groups - for example, all the urls in a query
        /// DataGroups enumerate all the groups and all the data points in each group
        /// </summary>
        DataGroups DataGroups
        {
            get;
        }	

        /// <summary>
        /// Look up the groupID for a data point
        /// </summary>
        /// <param name="iData"></param>
        /// <returns></returns>
        int GetGroupId(int iData);
    
        /// <summary>
        /// specify/restrict the features to be currently used - 
        /// the joint of inFeatureNames and original\existing features are the new features
        /// </summary>
        /// <param name="inFeatureNames">the feature names to be used/active</param>
        void SetActiveFeatures(string[] inFeatureNames);

        /// <summary>
        /// get the label of a data point
        /// </summary>
        /// <param name="iData">the data point index</param>
        /// <returns>the label of the data</returns>
        float GetLabel(int iData);
    }

    /// <summary>
    /// Access the original(un-quantized) feature values in the data set
    /// </summary>
    public interface LabelFeatureData : LabelFeatureCore
    {		
        /// <summary>
        /// get the feature value of the iFeature-th feature of iData-th data
        /// FeatureNames[iFeature] is the name of the feature
        /// </summary>
        /// <param name="iFeature">the feature index</param>
        /// <param name="iData">the data index</param>
        /// <returns>the feature value</returns>
        float GetFeature(int iFeature, int iData);

        /// <summary>
        /// get the entire feature vector of the iData-th data
        /// the returned feature vector corresponding to FeatureNames exactly
        /// </summary>
        /// <param name="iData">the data index</param>
        /// <returns>the feature vector returned</returns>
        float[] GetFeature(int iData);
    }

    /// <summary>
    /// Access the quantized/coded feature values in the data set
    /// Coded data set also contains the original/un-coded feature values
    /// </summary>
    public interface LabelFeatureDataCoded : LabelFeatureData
    {
        /// <summary>
        /// The number of codes of a feature
        /// </summary>
        /// <param name="iFeature">the feature index</param>
        /// <returns>the number of codes</returns>
        ushort GetCodeRange(int iFeature);

        /// <summary>
        /// get the coded feature value of the iFeature-th feature of iData-th data
        /// </summary>
        /// <param name="iFeature">the feature index</param>
        /// <param name="iData">the data index</param>
        /// <returns>the coded feature value</returns>
        ushort GetFeatureCoded(int iFeature, int iData);

        /// <summary>
        /// get the coded feature value of the iFeature-th feature of all data specified in idxData array
        /// </summary>
        /// <param name="iFeature">the feature to retrive the value for</param>
        /// <param name="idxData">the index of the data points</param>
        /// <param name="dataCoded">output coded feature values</param>
        /// <param name="cData">how many data points in the idxData array</param>
        void GetFeatureCoded(int iFeature, int[] idxData, ushort[] dataCoded, int cData);

        /// <summary>
        /// Convert the coded feature to its original value
        /// </summary>
        /// <param name="iFeature">the feature index</param>
        /// <param name="valueCode">coded value</param>
        /// <returns>the original value</returns>
        float ConvertToOrigData(int iFeature, float valueCode);

        /// <summary>
        /// Covert the original feature value to its coded value - encode
        /// </summary>
        /// <param name="iFeature">the feature index</param>
        /// <param name="value"> original feature value</param>
        /// <returns>coded value</returns>
        ushort CodeOrigData(int iFeature, float value);
    }

    /// <summary>
    /// Implement the LabelFeatureCore interface 
    /// - the properties and function common to all LabelFeatureData
    /// </summary>
    [Serializable]
    public class CLabelFeatureCore : LabelFeatureCore, IBinaryWritable
    {		
        protected CLabelFeatureCore(string[] featureNames, float[] labels, int[] groupId)
        {
            this.featureNames = featureNames;
            this.labels = labels;
            this.groupId = groupId;
        }

        protected CLabelFeatureCore(CLabelFeatureCore labelFeatureCore) :
            this(labelFeatureCore.FeatureNames, labelFeatureCore.labels, labelFeatureCore.groupId)
        {
            
            this.activeFeatureNames = labelFeatureCore.activeFeatureNames;         
            this.activeFeatureIdx = labelFeatureCore.activeFeatureIdx; 
            this.m_dataGroups = labelFeatureCore.m_dataGroups;	
        }

        //deserialize the object
        public CLabelFeatureCore(BinaryReaderEx binReaderEx)
        {                                             
            this.labels = binReaderEx.ReadSingleArray();// Read<float>();                                   
            this.groupId = binReaderEx.ReadInt32Array();
            this.featureNames = binReaderEx.ReadStringArray();

            bool factiveFeatureNamesExist = binReaderEx.ReadBoolean();
            if (factiveFeatureNamesExist)
            {
                this.activeFeatureNames = binReaderEx.ReadStringArray();
            }

            bool factiveFeatureIdxExist = binReaderEx.ReadBoolean();
            if (factiveFeatureIdxExist)
            {
                this.activeFeatureIdx = binReaderEx.ReadInt32Array();
            }            
        }

        //seralize the Object
        virtual public void Serialize(BinaryWriterEx binWriterEx)
        {
            binWriterEx.Write(this.GetType());

            binWriterEx.Write(this.labels);           
            binWriterEx.Write(this.groupId);       
            binWriterEx.Write(this.featureNames);

            binWriterEx.Write((this.activeFeatureNames != null));
            if (this.activeFeatureNames != null)
            {
                binWriterEx.Write(this.activeFeatureNames);
            }
            binWriterEx.Write((this.activeFeatureIdx != null));
            if (this.activeFeatureIdx != null)
            {
                binWriterEx.Write(this.activeFeatureIdx);
            }
        }

        // the total number of data points
        virtual public int NumDataPoint
        {
            get
            {
                return labels.Length;
            }
        }

        // specify/restrict the features to be currently used - 
        // the active features are joint of inFeatureNames and this.featureNames
        virtual public void SetActiveFeatures(string[] inFeatureNames)
        {
            // (inFeatureNames == null) <=> all the features are used
            if (inFeatureNames == null)
            {
                return;
            }

            List<int> activeIdx = new List<int>();
            for (int i = 0; i < this.featureNames.Length; i++)
            {
                for (int j = 0; j < inFeatureNames.Length; j++)
                {
                    if (string.Compare(inFeatureNames[j], this.featureNames[i], true) == 0)
                    {
                        activeIdx.Add(i);
                    }
                }
            }

            //(active-feature-set == null) <==> all the features are used
            if (activeIdx.Count == this.featureNames.Length)
            {
                this.activeFeatureIdx = null;
                this.activeFeatureNames = null;
            }
            else
            {
                this.activeFeatureIdx = activeIdx.ToArray();

                this.activeFeatureNames = new string[this.activeFeatureIdx.Length];
                for (int i = 0; i < this.activeFeatureIdx.Length; i++)
                {
                    this.activeFeatureNames[i] = this.featureNames[this.activeFeatureIdx[i]];
                }
            }            
        }        

        
        // the total number of active features
        virtual public int NumFeatures
        {
            get
            {
                return (activeFeatureNames != null)? activeFeatureNames.Length : featureNames.Length;
            }
        }

        // the name of the active features
        virtual public string[] FeatureNames
        {
            get
            {
                return (activeFeatureNames != null) ? activeFeatureNames : featureNames;               
            }
        }

        virtual public float GetLabel(int iData)
        {								
            return labels[iData];			
        }
        
        virtual public int GetGroupId(int iData)
        {										
            return groupId[iData];			
        }

        virtual public DataGroups DataGroups
        {
            get
            {
                if (null == m_dataGroups)
                {
                    m_dataGroups = new DataGroups(this);
                }
                return m_dataGroups;
            }
        }				       

        //Private data members        
        private float[] labels;
        private int[] groupId;
        
        private string[] featureNames; // names of all the features in the data set
        //(active-feature-set == null) <==> it is the same as the total feature set
        private string[] activeFeatureNames; // names of a subset of avaliable features being used
        protected int[] activeFeatureIdx; //the index of the subset of active features
        
        [NonSerialized] DataGroups m_dataGroups;
    }

    /// <summary>
    /// An (simple) implementation of LabelFeatureData where the original feature values are stored
    /// in a 2-D array - IDataMatrix - access by feature and data index
    /// </summary>
    [Serializable]
    public class CLabelFeatureData : CLabelFeatureCore, LabelFeatureData, IBinaryWritable
    {
        static public LabelFeatureData Load(string inFileName, IParser<float> featureParser, IParser<float> labelParser, IGroupBoundary dataGroupBoundary,
                                           Type outDataType, string[] activeFeatureNames, int cThreads)
        {
            return Load(inFileName, featureParser, labelParser, dataGroupBoundary, outDataType, activeFeatureNames, cThreads, true);
        }

        static public LabelFeatureData Load(string inFileName, IParser<float> featureParser, IParser<float> labelParser, IGroupBoundary dataGroupBoundary,
                                            Type outDataType, string[] activeFeatureNames, int cThreads, bool fSparseCoded)
        {
            return Load(inFileName, featureParser, labelParser, dataGroupBoundary, outDataType, activeFeatureNames, cThreads, true, fSparseCoded);
        }
        
        /// <summary>
        /// Load LabelFeatureData file - static function for both CLabelFeatureData and CLabelFeatureDataCoded
        /// load in the file and output a LabelFeatureData object that has the specified Type (type = {LabelFeatureData, LabelFeatureDataCode}
        /// </summary>
        /// <param name="inFileName">the name of the file: xxx.tsv == tsv file formation; xxx.bin == binary uncoded data format; xxx.dp == binary coded data format</param>
        /// <param name="featureParser">parser that understand the feature values</param>
        /// <param name="labelParser">parser that understand the label values</param>
        /// <param name="dataGroupBoundary">data group boundaries</param>
        /// <param name="outDatatype">the output data type LabelFeatureData or LabelFeatureDataCoded</param>
        /// <param name="activeFeatureNames">only these feature values are loaded</param>
        /// <param name="cThreads">number of threads used to code the original data</param>
        /// <returns>the desired LabelFeatureData if no errors in loading; otherwise, null</returns>
        static public LabelFeatureData Load(string inFileName, IParser<float> featureParser, IParser<float> labelParser, IGroupBoundary dataGroupBoundary,
                                            Type outDataType, string[] activeFeatureNames, int cThreads, bool fCacheCodedFeature, bool fSparseCoded)
        {
            if (inFileName == null)
            {
                return null;
            }

            string[] fields = inFileName.Split('.');
            if (fields.Length <= 0)
            {
                return null;
            }

            CLabelFeatureData labelFeatureData = null;

            string sufix = fields[fields.Length-1];
            
            if (string.Compare(sufix, "bin", true) == 0 || string.Compare(sufix, "dp", true) == 0)
            {
                BinaryReaderEx binReaderEx = new BinaryReaderEx(inFileName);
                Type t = binReaderEx.Read<Type>();
                labelFeatureData = (CLabelFeatureData)binReaderEx.Read(t);
                binReaderEx.Close();
                
                labelFeatureData.SetActiveFeatures(activeFeatureNames);
            }
            else // "tsv" or null
            {
                TsvFileLoader tsvFileLoader = new TsvFileLoader(inFileName, null, labelParser, featureParser, dataGroupBoundary);

                labelFeatureData = new CLabelFeatureData(tsvFileLoader.FeatureName, tsvFileLoader.Labels, tsvFileLoader.GroupId, tsvFileLoader.Feature);
            }

            if (outDataType.Equals(typeof(CLabelFeatureDataCoded)))
            {
                if (labelFeatureData.GetType().Equals(typeof(CLabelFeatureDataCoded)))
                {
                    if (fCacheCodedFeature)
                    {
                        ((CLabelFeatureDataCoded)labelFeatureData).EncodeFeatureValues(cThreads, fSparseCoded);
                    }
                }
                else
                {
                    //need to upgrade to coded                    
                    labelFeatureData = new CLabelFeatureDataCoded(labelFeatureData, cThreads, fCacheCodedFeature, fSparseCoded);
                }
            }           

            return labelFeatureData;
        }

        /// <summary>
        /// Load LabelFeatureData file and distribute - static function for both CLabelFeatureData and CLabelFeatureDataCoded
        /// load in the file and output a LabelFeatureData object that has the specified Type (type = {LabelFeatureData, LabelFeatureDataCode}
        /// </summary>
        /// <param name="inFileName">the name of the file: xxx.tsv == tsv file formation; xxx.bin == binary uncoded data format; xxx.dp == binary coded data format</param>
        /// <param name="featureParser">parser that understand the feature values</param>
        /// <param name="labelParser">parser that understand the label values</param>
        /// <param name="dataGroupBoundary">data group boundaries</param>
        /// <param name="outDatatype">the output data type LabelFeatureData or LabelFeatureDataCoded</param>
        /// <param name="activeFeatureNames">only these feature values are loaded</param>
        /// <param name="cThreads">number of threads used to code the original data</param>
        /// <returns>the desired LabelFeatureData if no errors in loading; otherwise, null</returns>
        static public LabelFeatureData DistributeLoad(string inFileName, IParser<float> featureParser, IParser<float> labelParser, IGroupBoundary dataGroupBoundary,
                                            Type outDataType, string[] activeFeatureNames, int cThreads, bool fCacheCodedFeature, bool fSparseCoded, Random r, double rangeLower, double rangeUpper)
        {
            if (inFileName == null)
            {
                return null;
            }

            string[] fields = inFileName.Split('.');
            if (fields.Length <= 0)
            {
                return null;
            }

            CLabelFeatureData labelFeatureData = null;

            string sufix = fields[fields.Length - 1];
            if (string.Compare(sufix, "tsv", true) == 0 || string.Compare(sufix, "gz", true) == 0)
            {
                TsvFileLoader tsvFileLoader = new TsvFileLoader(inFileName, null, labelParser, featureParser, dataGroupBoundary, r, rangeLower, rangeUpper);

                labelFeatureData = new CLabelFeatureData(tsvFileLoader.FeatureName, tsvFileLoader.Labels, tsvFileLoader.GroupId, tsvFileLoader.Feature);
            }
            else if (string.Compare(sufix, "bin", true) == 0 || string.Compare(sufix, "dp", true) == 0)
            {
                //initially, only accept tsv file.
                return null;

                //BinaryReaderEx binReaderEx = new BinaryReaderEx(inFileName);
                //Type t = binReaderEx.Read<Type>();
                //labelFeatureData = (CLabelFeatureData)binReaderEx.Read(t);
                //binReaderEx.Close();

                //labelFeatureData.SetActiveFeatures(activeFeatureNames);
            }

            if (outDataType.Equals(typeof(CLabelFeatureDataCoded)))
            {
                if (labelFeatureData.GetType().Equals(typeof(CLabelFeatureDataCoded)))
                {
                    if (fCacheCodedFeature)
                    {
                        ((CLabelFeatureDataCoded)labelFeatureData).EncodeFeatureValues(cThreads, fSparseCoded);
                    }
                }
                else
                {
                    //need to upgrade to coded                    
                    labelFeatureData = new CLabelFeatureDataCoded(labelFeatureData, cThreads, fCacheCodedFeature, fSparseCoded);
                }
            }

            return labelFeatureData;
        }
        
        public CLabelFeatureData(CLabelFeatureData labelFeatureData)
            : base(labelFeatureData)
        {                        
            this.feature = labelFeatureData.feature;
        }

        public CLabelFeatureData(string[] featureNames, float[] labels, int[] groupId, IDataMatrix<float> feature) 
            : base(featureNames, labels, groupId)
        {
            this.feature = feature;
        }

        public CLabelFeatureData(IDataMatrix<float> feature)
            : base(null, null, null)
        {
            this.feature = feature;
        }

        public CLabelFeatureData(float[][] feature)
            : base(null, null, null)
        {
            this.feature = new DataMatrixDenseRowMajor<float>(feature);
        }		    

        //iFeature is the index on the active feature not all the avaliable features
        virtual public float GetFeature(int iFeature, int iData)
        {
            int idx = (activeFeatureIdx != null) ? activeFeatureIdx[iFeature] : iFeature;
            return feature.GetValue(iData, idx);
        }

        virtual public float[] GetFeature(int iData)
        {
            //(1) getting all the features - active and inactive ones
            if (allFeatureRow == null)
            {
                allFeatureRow = new float[this.feature.NumCols];
            }

            this.feature.GetValues(iData, allFeatureRow);
             
            //all features are active => return all features
            if (activeFeatureIdx == null)
            {
                return allFeatureRow;
            }
                
            //(2) only select the active ones
            if (activeFeatureRow == null)
            {
                activeFeatureRow = new float[this.NumFeatures];
            }

            for (int i = 0; i < this.NumFeatures; i++)
            {
                activeFeatureRow[i] = allFeatureRow[activeFeatureIdx[i]];
            }
            return activeFeatureRow;
        }

        //deserialize the object
        public CLabelFeatureData(BinaryReaderEx binReaderEx)
            : base(binReaderEx)
        {                       
            this.feature = binReaderEx.Read<DataMatrixSerialized<float>>();
        }		    

        //seralize the Object
        override public void Serialize(BinaryWriterEx binWriterEx)        
        {
            //first serialize the base class
            base.Serialize(binWriterEx);
                            
            ((IBinaryWritable)this.feature).Serialize(binWriterEx);                                   
        }  

        virtual public void Save(string binFileName)
        {
#if FORMATTER
            FileStream fileStream = new FileStream(binFileName, FileMode.Create);
            BinaryFormatter formatter = new BinaryFormatter();
            formatter.Serialize(fileStream, this);
            fileStream.Close(); 
#else
            BinaryWriterEx binWriterEx = new BinaryWriterEx(binFileName);
            this.Serialize(binWriterEx);
            binWriterEx.Close(); 
#endif //FORMATTER
        }

        // the total number of data points
        override public int NumDataPoint
        {
            get
            {
                return feature.NumRows;
            }
        }

        protected IDataMatrix<float> feature;

        [NonSerialized]
        private float[] activeFeatureRow; // the feature vector of a data point correpsonding to the active features
        [NonSerialized]
        private float[] allFeatureRow; // the feature vector of all features
    }

    /// <summary>
    /// An implementation of LabelFeatureDataCode which contains the original feature data
    /// in its base-class CLabelFeatureData and also contains quantized/encoded feature data
    /// </summary>
    [Serializable]
    public class CLabelFeatureDataCoded : CLabelFeatureData, LabelFeatureDataCoded
    {        
        //seralize the Object
        override public void Save(string binFileName)
        {
#if FORMATTER
            FileStream fileStream = new FileStream(binFileName, FileMode.Create);
            BinaryFormatter formatter = new BinaryFormatter();
            formatter.Serialize(fileStream, this);
            fileStream.Close();
#else //FORMATTER
            BinaryWriterEx binWriterEx = new BinaryWriterEx(binFileName);
            this.Serialize(binWriterEx);
            binWriterEx.Close();
#endif //FORMATTER
        }

        //deserialize the object
        public CLabelFeatureDataCoded(BinaryReaderEx binReaderEx)
            : base(binReaderEx)
        {
            this.codeBook = binReaderEx.Read<CodeBook>();

            bool featureCodedExist = binReaderEx.ReadBoolean();
            if (featureCodedExist)
            {                
                this.featureCoded = binReaderEx.Read<DataMatrixSerialized<ushort>>();
            }
        }

        //seralize the Object
        override public void Serialize(BinaryWriterEx binWriterEx)
        {
            //first serialize the base class
            base.Serialize(binWriterEx);

            binWriterEx.Write(this.codeBook);

            binWriterEx.Write((this.featureCoded != null));
            if (this.featureCoded != null)
            {                
                ((IBinaryWritable)this.featureCoded).Serialize(binWriterEx);
            }
        }  

        public CLabelFeatureDataCoded(CLabelFeatureDataCoded labelFeatureDataCoded)
            : base(labelFeatureDataCoded)
        {
            this.codeBook = labelFeatureDataCoded.codeBook;
            this.featureCoded = labelFeatureDataCoded.featureCoded;
        }

        public CLabelFeatureDataCoded(CLabelFeatureData labelFeatureData, int cThreads, bool fStoreCodedFeature, bool fSparse)
            : base(labelFeatureData)
        {
            //input data
            int numRows = labelFeatureData.NumDataPoint;
            int numCols = labelFeatureData.NumFeatures;

            //compute the code book for each feature            
            this.codeBook = ComputeCodeBook(this.feature, cThreads);
            
            //encode the original feature values and store them for speed if required
            if (fStoreCodedFeature)
            {
                this.featureCoded = EncodeFeatureValues(this.feature, this.codeBook, cThreads, fSparse);
            }
        }

        public CLabelFeatureDataCoded(CLabelFeatureData labelFeatureData, int cThreads, bool fStoreCodedFeature)
            : this(labelFeatureData, cThreads, fStoreCodedFeature, true)
        {
        }

        public CLabelFeatureDataCoded(CLabelFeatureData labelFeatureData, int cThreads)
            : this(labelFeatureData, cThreads, true)
        {
        }

        virtual public ushort GetCodeRange(int iFeature)
        {
            iFeature = (activeFeatureIdx != null) ? activeFeatureIdx[iFeature] : iFeature;
            return this.codeBook.GetCodeRange(iFeature);
        }

        virtual public ushort GetFeatureCoded(int iFeature, int iData)
        {
            ushort c;

            if (featureCoded == null)
            {
                float v = this.GetFeature(iFeature, iData);
                c = this.CodeOrigData(iFeature, v);
            }
            else
            {
                iFeature = (activeFeatureIdx != null) ? activeFeatureIdx[iFeature] : iFeature;
                c = this.featureCoded.GetValue(iData, iFeature);
            }

            return c;
        }

        virtual public void GetFeatureCoded(int iFeature, int[] idxData, ushort[] dataCoded, int cData)
        {
            if (featureCoded == null)
            {
                for (int i = 0; i < cData; i++)
                {
                    float v = this.GetFeature(iFeature, idxData[i]);
                    dataCoded[i] = this.CodeOrigData(iFeature, v);                    
                }                
            }
            else
            {
                iFeature = (activeFeatureIdx != null) ? activeFeatureIdx[iFeature] : iFeature;                
                for (int i = 0; i < cData; i++)
                {                    
                    dataCoded[i] = this.featureCoded.GetValue(idxData[i], iFeature);
                }
            }            
        }

        virtual public float ConvertToOrigData(int iActiveFeature, float x)
        {
            int iFeature = (activeFeatureIdx != null) ? activeFeatureIdx[iActiveFeature] : iActiveFeature;

            return this.codeBook.ConvertToOrigData(iFeature, x);     
        }

        virtual public ushort CodeOrigData(int iActiveFeature, float x)
        {
            int iFeature = (activeFeatureIdx != null) ? activeFeatureIdx[iActiveFeature] : iActiveFeature;
    
            return this.codeBook.CodeLookup(iFeature, x);
        }

        public void EncodeFeatureValues(int cThreads, bool fSparse)
        {
            //encode the original feature values and store them for speed if required
            if (this.featureCoded == null)
            {
                this.featureCoded = this.EncodeFeatureValues(this.feature, this.codeBook, cThreads, fSparse);
            }
        }

        private CodeBook ComputeCodeBook(IDataMatrix<float> dataMatrix, int cThreads)
        {
            //perform quantization
            MatrixQuantizer matrixQuantizer = new MatrixQuantizer(dataMatrix);
            ProcessorMT processorMT = new ProcessorMT(matrixQuantizer, cThreads);

#if QUANTIZER_TIMER
            System.Diagnostics.Stopwatch timer = new System.Diagnostics.Stopwatch();
#if VERBOSE
            Console.WriteLine("Starting codebook timer...");
#endif
            timer.Start();
#endif
            processorMT.Process();
#if QUANTIZER_TIMER
            timer.Stop();
#if VERBOSE
            Console.WriteLine("Total Codebook calculation time: {0} seconds", 0.001 * timer.ElapsedMilliseconds);
#endif
#endif

            //code book
            return matrixQuantizer.codeBook;
        }

        private IDataMatrix<ushort> EncodeFeatureValues(IDataMatrix<float> dataMatrix, CodeBook codeBook, int cThreads, bool fSparse)
        {
            //construct the matrix builder given if we want to have dense or sparse representations or not
            IDataMatrixBuilderRam<ushort> dataMatrixBuilderRam;
            if (fSparse)
            {
                dataMatrixBuilderRam = new DataMatrixBuilderRamSparse<ushort>(dataMatrix.NumRows, dataMatrix.NumCols, 0);
            }
            else
            {
                dataMatrixBuilderRam = new DataMatrixBuilderRamDense<ushort>(dataMatrix.NumRows, dataMatrix.NumCols);
            }

            MatrixEncoder matrixEncoder = new MatrixEncoder(dataMatrix, codeBook, dataMatrixBuilderRam);
            ProcessorMT processorMT = new ProcessorMT(matrixEncoder, cThreads);

#if QUANTIZER_TIMER
            System.Diagnostics.Stopwatch timer = new System.Diagnostics.Stopwatch();
#if VERBOSE
            Console.WriteLine("Starting encoding timer...");
#endif
            timer.Start();
#endif

            processorMT.Process();

#if QUANTIZER_TIMER
            timer.Stop();
#if VERBOSE
            Console.WriteLine("Total encoding time: {0} seconds", 0.001 * timer.ElapsedMilliseconds);
#endif
#endif          
            return matrixEncoder.CodedMatrix;
        }

        //code book
        protected CodeBook codeBook;
        //encoded feature value
        protected IDataMatrix<ushort> featureCoded = null;        
    }

    /// <summary>
    /// An implementation of the LabelFeatureData which consists of an array of LabelFeatureData objects
    /// All the data points in the array are aggragated together and expose as a single uniform collection 
    /// of data points that have the same features
    /// </summary>
    public class CLabelFeatureDataComposite : LabelFeatureData
    {
        /// <summary>
        /// construct a single uniform LabelFeatureData from an array of such data objects
        /// Features: the features of the unified final object is the same as the features of the first object in the input array
        ///           If a feature does not exist/missing in the subsequent object, its value is set to zero
        /// DataPoints/index: the data points are aggragated together and indexed in the same order as the input object array
        /// DataGroups/index: the data groups are also agregrated together and indexed in the same order as the input object array
        /// </summary>
        /// <param name="labelFeatureDataElements"></param>
        public CLabelFeatureDataComposite(LabelFeatureData[] labelFeatureDataElements)
        {
            this.labelFeatureDataElements = labelFeatureDataElements;
            
            //number of the datapoints are the sum of the ones in the input object array
            numDataPoint = 0;
            for (int i = 0; i < this.labelFeatureDataElements.Length; i++)
            {
                if (this.labelFeatureDataElements[i] != null)
                {
                    this.numDataPoint += this.labelFeatureDataElements[i].NumDataPoint;
                }                
            }
                        
            //featureNames is set to the feature names of the first element
            this.featureNames = this.labelFeatureDataElements[0].FeatureNames;

            //index map used to look up the feature id of the original input object from that of the unified object
            this.idxMaps = new int[labelFeatureDataElements.Length][];
            this.accGroups = new int[labelFeatureDataElements.Length];
            for (int i = 0; i < this.labelFeatureDataElements.Length; i++)
            {
                this.idxMaps[i] = null;
                if (this.labelFeatureDataElements[i] != null)
                {
                    this.idxMaps[i] = new int[this.featureNames.Length];
                    for (int j = 0; j < this.featureNames.Length; j++)
                    {
                        string name = this.featureNames[j];
                        this.idxMaps[i][j] = -1;
                        int k = 0;
                        for (k = 0; k < this.labelFeatureDataElements[i].FeatureNames.Length; k++)
                        {
                            if (string.Compare(name, this.labelFeatureDataElements[i].FeatureNames[k], true) == 0)
                            {
                                this.idxMaps[i][j] = k;
                                break;
                            }
                        }

                        if (k >= this.labelFeatureDataElements[i].FeatureNames.Length)
                        {
                            Console.WriteLine("Feature " + name + " does not exist");
                        }
                    }
                }

                this.accGroups[i] = ((i==0)? 0 : this.accGroups[i-1]) + ((this.labelFeatureDataElements[i] == null)? 0 : this.labelFeatureDataElements[i].DataGroups.GroupCounts);
            }       
        }

        // the total number of data points
        virtual public int NumDataPoint
        {
            get
            {
                return this.numDataPoint;
            }
        }

        // the total number of features
        virtual public int NumFeatures
        {
            get
            {
                return this.FeatureNames.Length;                
            }
        }

        // the name of the features
        virtual public string[] FeatureNames
        {
            get
            {
                if (this.activeFeatureNames != null)
                {
                    return this.activeFeatureNames;
                }
                else
                {
                    return this.featureNames;
                }
            }
        }     

        // specify/restrict the features to be currently used 
        virtual public void SetActiveFeatures(string[] inFeatureNames)
        {
            int cActive = 0;
            for (int i=0; i<this.featureNames.Length; i++)
            {
                string name = this.featureNames[i];
                for (int j=0; j<inFeatureNames.Length; i++)
                {
                    if (string.Compare(name, inFeatureNames[j], true) == 0)
                    {
                        cActive++;
                        break;
                    }
                }                
            }

            activeFeatureNames = new string[cActive]; // names of a subset of avaliable features being used            
            activeFeatureIdx = new int[cActive];
            cActive = 0;
            for (int i=0; i<this.featureNames.Length; i++)
            {
                string name = this.featureNames[i];
                for (int j=0; j<inFeatureNames.Length; i++)
                {
                    if (string.Compare(name, inFeatureNames[j], true) == 0)
                    {
                        activeFeatureNames[cActive] = name;
                        activeFeatureIdx[cActive] = i;
                        cActive++;
                        break;
                    }
                }                
            }
        }

        virtual public float GetLabel(int iData)
        {
            int iDataLocal = -1;
            int idxEle = FindEleIdx(iData, ref iDataLocal);
            return this.labelFeatureDataElements[idxEle].GetLabel(iDataLocal);
        }
       
        virtual public float GetFeature(int iFeature, int iData)
        {
            float v = 0; //default

            int iDataLocal = -1;
            int idxEle = FindEleIdx(iData, ref iDataLocal);

            //iFeature is the index on the active feature not all the avaliable features
            if (activeFeatureIdx != null)
            {
                iFeature = activeFeatureIdx[iFeature];
            }

            //does the feature exist in the corresponding data object?
            int iFeatureLocal = this.idxMaps[idxEle][iFeature];
            if (iFeatureLocal >= 0)
            {
                v = this.labelFeatureDataElements[idxEle].GetFeature(iFeatureLocal, iDataLocal);
            }
            else
            {
                //set the feature value to zero if the feature does not exist
                v = 0;
            }

            return v;           
        }

        virtual public float[] GetFeature(int iData)
        {
            if (activeFeatureRow == null)
            {
                activeFeatureRow = new float[this.NumFeatures];
            }
            for (int i = 0; i < this.NumFeatures; i++)
            {
                activeFeatureRow[i] = this.GetFeature(i, iData);
            }
            
            return activeFeatureRow;			
        }

        virtual public int GetGroupId(int iData)
        {
            int iDataLocal = -1;
            int idxEle = FindEleIdx(iData, ref iDataLocal);

            int prev = this.accGroups[idxEle] - this.labelFeatureDataElements[idxEle].DataGroups.GroupCounts;
            return (prev + this.labelFeatureDataElements[idxEle].GetGroupId(iDataLocal));                        
        }

        virtual public DataGroups DataGroups
        {
            get
            {
                if (null == m_dataGroups)
                {
                    m_dataGroups = new DataGroups(this);
                }
                return m_dataGroups;
            }
        }		        

        int numDataPoint;

        protected LabelFeatureData[] labelFeatureDataElements;
        int[] accGroups; //the running sum number of data groups
        protected int[][] idxMaps;

        private string[] featureNames; // names of all the features in the data set
        //(active-feature-set == null) <==> it is the same as the total feature set
        private string[] activeFeatureNames; // names of a subset of avaliable features being used
        protected int[] activeFeatureIdx; //the index of the subset of active features

        float[] activeFeatureRow;

        [NonSerialized]
        DataGroups m_dataGroups;

        protected int FindEleIdx(int iData, ref int iDataLocal)
        {
            int cPrev = 0;
            int i = 0;
            for (i = 0; i < this.labelFeatureDataElements.Length; i++)
            {
                int curCounts = (this.labelFeatureDataElements[i] == null) ? 0 : this.labelFeatureDataElements[i].NumDataPoint;

                if (iData < cPrev + curCounts)
                {
                    break;
                }
                cPrev += curCounts;
            }

            iDataLocal = iData - cPrev;
            return i;
        }

    }

    /// <summary>
    /// An implementation of CLabelFeatureDataCoded object which consists of an array of CLabelFeatureData objects,
    /// where the first object is a CLabelFeatureDataCoded object
    /// </summary>
    public class CLabelFeatureDataCodedComposite : CLabelFeatureDataComposite, LabelFeatureDataCoded
    {
        //the first one id always train data, which is always coded        
        public static CLabelFeatureDataCodedComposite Create(LabelFeatureDataCoded trainLabelFeatureDataCoded, LabelFeatureData validLabelFeatureData, LabelFeatureData testLabelFeatureData)         
        {
            if(trainLabelFeatureDataCoded == null)
            {
                return null;
            }

            int cPartition = (int)DataPartitionType.cTypes;

            List<LabelFeatureData> listLabelFeatureData = new List<LabelFeatureData>(cPartition);                        
            listLabelFeatureData.Add(trainLabelFeatureDataCoded);            
            listLabelFeatureData.Add(validLabelFeatureData);       
            listLabelFeatureData.Add(testLabelFeatureData);            
            LabelFeatureData[] labelFeatureDataArray = listLabelFeatureData.ToArray();

            CLabelFeatureDataCodedComposite labelFeatureData = new CLabelFeatureDataCodedComposite(labelFeatureDataArray);
           
            int[] cDataGroups = new int[cPartition];
            for (int i = 0; i < cPartition; i++)
            {
                cDataGroups[i] = 0;
                if (labelFeatureDataArray[i] != null)
                {
                    cDataGroups[i] = labelFeatureDataArray[i].DataGroups.GroupCounts;
                }
            }

            //train/valid/test data partition
            labelFeatureData.DataGroups.PartitionData(cDataGroups);

            return labelFeatureData;
        }

        private CLabelFeatureDataCodedComposite(LabelFeatureData[] labelFeatureDataElements)
            :
            base(labelFeatureDataElements)
        {
            //the LabelFeatureDataCoded object is always the first one in the input array
            this.labelFeatureDataCoded = (LabelFeatureDataCoded)labelFeatureDataElements[0];
        }

        virtual public ushort GetCodeRange(int iFeature)
        {            
            iFeature = this.iFeatureIdx2LabelFeatureDataCoded(iFeature);

            return labelFeatureDataCoded.GetCodeRange(iFeature);
        }

        virtual public ushort CodeOrigData(int iFeature, float x)
        {
            iFeature = this.iFeatureIdx2LabelFeatureDataCoded(iFeature);

            return labelFeatureDataCoded.CodeOrigData(iFeature, x);
        }

        virtual public ushort GetFeatureCoded(int iFeature, int iData)
        {
            ushort c = 0;

            int iDataLocal = -1;
            int idxEle = FindEleIdx(iData, ref iDataLocal);
            if (idxEle == 0)
            {
                //data is from the LabelFeatureDataCoded => directly return the coded data
                iFeature = this.iFeatureIdx2LabelFeatureDataCoded(iFeature);
                c = labelFeatureDataCoded.GetFeatureCoded(iFeature, iData);
            }
            else
            {
                //otherwise, first look up its original value, and then encode the value
                float v = this.GetFeature(iFeature, iData);
                c = this.CodeOrigData(iFeature, v);
            }

            return c;            
        }

        virtual public void GetFeatureCoded(int iFeature, int[] idxData, ushort[] dataCoded, int cData)
        {
            //translate the feature index
            int iFeatureCoded = this.iFeatureIdx2LabelFeatureDataCoded(iFeature);

            //get all the data that are in LabelFeatureDataCoded
            int limit0 = this.labelFeatureDataElements[0].NumDataPoint;
            int cData0 = 0;
            for (int i = 0; i < cData; i++)
            {
                if (idxData[i] >= limit0)
                {
                    break;
                }
                cData0++;
            }
            labelFeatureDataCoded.GetFeatureCoded(iFeatureCoded, idxData, dataCoded, cData0);
            
            //get the rest of the data
            for (int j = cData0; j < cData; j++)
            {
                dataCoded[j] = this.GetFeatureCoded(iFeature, idxData[j]);
            }
        }

        virtual public float ConvertToOrigData(int iFeature, float x)
        {
            iFeature = this.iFeatureIdx2LabelFeatureDataCoded(iFeature);
            
            return labelFeatureDataCoded.ConvertToOrigData(iFeature, x);
        }

        private LabelFeatureDataCoded labelFeatureDataCoded;

        //map the active feature index of the composite object into the
        //feature index of the LabelFeatureDataCoded object
        private int iFeatureIdx2LabelFeatureDataCoded(int iFeature)
        {                        
            //map the active feature index to original feature index
            if (this.activeFeatureIdx != null)
            {
                iFeature = this.activeFeatureIdx[iFeature];
            }

            //map the feature index into that of the LabelFeatureDataCoded object,
            //which is always the first one in the input array - see the constructor.
            //It is easy to make this more flexiable later if necessary
            iFeature = this.idxMaps[0][iFeature];

            return iFeature;
        }
    }
  
    public class DataGroup
    {
        public int iStart;
        public int cSize;
        public int id;
    }

    public class DataGroups
    {
        //parsing a string separated by ':' to get the data partition amounts train:valid:test
        //This function is called by applications that needs to split data
        public static float[] DataSplit(string inStr)
        {
            string[] dataSplit = inStr.Split(':');
            float[] percentage = new float[dataSplit.Length]; //train, valid, test
            float sum = 0;
            for (int i = 0; i < percentage.Length; i++)
            {
                percentage[i] = float.Parse(dataSplit[i]);
                sum += percentage[i];
            }
            for (int i = 0; i < percentage.Length; i++)
            {
                percentage[i] = percentage[i] / sum;
            }
            return percentage;
        }

        //parsing a string separated by ':' to get the data partitions used to the application
        //This function is called by applications that needs to use particular paritions of the data
        public static DataPartitionType[] PartitionTypes(string inStr)
        {
            List<DataPartitionType> ListPartition = new List<DataPartitionType>();
            string[] portions = inStr.Split(':');
            for (int i = 0; i < portions.Length; i++)
            {
                if (string.Compare(portions[i], "Train", true) == 0)
                {
                    ListPartition.Add(DataPartitionType.Train);
                }
                else if (string.Compare(portions[i], "Valid", true) == 0)
                {
                    ListPartition.Add(DataPartitionType.Validation);
                }
                else if (string.Compare(portions[i], "Test", true) == 0)
                {
                    ListPartition.Add(DataPartitionType.Test);
                }
                else
                {
                }
            }
            return ListPartition.ToArray(); 
        }

        /// <summary>
        /// Requirements:
        /// (1) all the data points are indexed from 0 ... cDataCountsTotal
        /// (2) all data points belongs to one group are contingious
        /// </summary>
        /// <param name="groupId">maps a data point to its group ID</param>
        public DataGroups(LabelFeatureCore labelFeatureCore)
        {
            NumDataPoints = labelFeatureCore.NumDataPoint;

            List<int> listGroupId = new List<int>();
            List<int> listStartIdx = new List<int>();

            listGroupId.Add(labelFeatureCore.GetGroupId(0));
            listStartIdx.Add(0);
            int curID = labelFeatureCore.GetGroupId(0);
            for (int i = 0; i < NumDataPoints; i++)
            {
                int nextID = labelFeatureCore.GetGroupId(i);
                if (nextID != curID)
                {
                    listStartIdx.Add(i);
                    listGroupId.Add(nextID);
                    curID = nextID;
                }
            }

            groupCounts = listStartIdx.Count;
            dataGroups = new DataGroup[groupCounts];

            for (int i = 0; i < groupCounts; i++)
            {
                dataGroups[i] = new DataGroup();
                dataGroups[i].id = listGroupId[i];
                dataGroups[i].iStart = listStartIdx[i];
                if (i < groupCounts - 1)
                {
                    dataGroups[i].cSize = listStartIdx[i + 1] - listStartIdx[i];
                }
                else
                {
                    dataGroups[i].cSize = NumDataPoints - listStartIdx[i];
                }
            }
        }

        /// <summary>
        /// returns the total number of DataGroups
        /// </summary>
        public int GroupCounts
        {
            get
            {
                return groupCounts;
            }
        }

        public DataGroup this[int index]
        {
            get
            {
                return dataGroups[index];
            }
        }

        /// <summary>
        /// Partition the data input Train/Valid/Test sets
        /// partitionGroupCounts[0] are the total groups in train set
        /// partitionGroupCounts[1] are the total groups in valid set
        /// partitionGroupCounts[2] are the total groups in test set
        /// The data groups are assigned in the order of train/valid/test
        /// </summary>
        /// <param name="partitionGroupCounts"></param>
        public void PartitionData(int[] partitionGroupCounts)
        {
            //int cPartition = (int)DataPartitionType.cTypes;
            int cPartition = partitionGroupCounts.Length;
            groupPartitionIndexTbl = new int[cPartition][];

            int iStart = 0;
#if VERBOSE
            Console.Write("Train/Valid/Test query numbers: ");
#endif
            for (int i = 0; i < cPartition; i++)
            {
                int L = partitionGroupCounts[i];
                L = (L > (this.groupCounts - iStart)) ? (this.groupCounts - iStart) : L;

#if VERBOSE
                Console.Write("{0} ", L);
#endif
                int[] idx = new int[L];
                for (int j = 0; j < L; j++)
                {
                    idx[j] = iStart + j;
                }
                groupPartitionIndexTbl[i] = idx;
                iStart += L;
            }
#if VERBOSE
            Console.WriteLine();
#endif
        }

        /// <summary>
        /// Partition data into disjoint train, validation, and test data sets
        /// preserve group boundary
        /// </summary>
        /// <param name="percentage"> train/valid/test split</param>
        public void PartitionData(float[] percentage, Random r)
        {            
            int[] permuteSampleIndex = null;
            if (r != null)
            {
                permuteSampleIndex = Vector.RandomSample(groupCounts, groupCounts, r);
            }
            else
            {
                permuteSampleIndex = Vector.IndexArray(groupCounts);
            }

            PartitionData(percentage, permuteSampleIndex);           
        }

        public void PartitionData(float[] percentage, int[] permuteSampleIndex)
        {
            int cPartition = (int)DataPartitionType.cTypes;
            groupPartitionIndexTbl = new int[cPartition][];

            int iStart = 0;
#if VERBOSE
            Console.Write("Train/Valid/Test query numbers: ");
#endif
            for (int i = 0; i < cPartition; i++)
            {
                int L = (int)((percentage[i] * (float)groupCounts) + 0.5); // Bug - without the 0.5 you can run into rounding problems
#if VERBOSE
                Console.Write("{0} ", L);
#endif
                if (i == cPartition - 1)
                {
                    L = groupCounts - iStart;
                }

                int[] idx = new int[L];
                for (int j = 0; j < L; j++)
                {
                    idx[j] = permuteSampleIndex[iStart + j];
                }
                groupPartitionIndexTbl[i] = idx;
                iStart += L;
            }            
#if VERBOSE
            Console.WriteLine();
#endif
        }

        /// <summary>
        /// Retrive the data partition specifed before in actual data index
        /// </summary>
        /// <param name="pType">DataPartitionType</param>
        /// <param name="subSamplePercent">the percentage of specified partition to be returned</param>
        /// <param name="cSize">the actual number of total data points returned</param>
        /// <returns>Corresponding Data indices</returns>
        public DataSet GetDataPartition(DataPartitionType pType, float subSamplePercent, Random r)
        {
            DataSet dataSet = null;
            if (groupPartitionIndexTbl != null && pType < DataPartitionType.cTypes)
            {                
                int[] groupIndex = this.groupPartitionIndexTbl[(int)pType];

                int cGroups = groupIndex.Length;
                cGroups = (int)((float)cGroups * subSamplePercent);

                int[] sampleGroupIndex = null;
                if (r != null)
                {
                    sampleGroupIndex = Vector.RandomSample(groupIndex.Length, cGroups, r);                    
                }
                else
                {
                    sampleGroupIndex = Vector.IndexArray(cGroups);                    
                }
                for (int i = 0; i < cGroups; i++)
                {
                    sampleGroupIndex[i] = groupIndex[sampleGroupIndex[i]];
                }
                dataSet = new DataSet(this, sampleGroupIndex);                
            }

            return dataSet;
        }

        public DataSet GetDataPartition(DataPartitionType pType)
        {
            return GetDataPartition(pType, 1.0F, null);
        }

        private int NumDataPoints;
        int groupCounts;
        DataGroup[] dataGroups;        
        
        //the group index of corresponding partition
        int[][] groupPartitionIndexTbl;
    }   

    public class DataSet
    {
        public DataSet(DataGroups dataGroups, int[] groupIndex)
        {
            this.dataGroups = dataGroups;
            this.groupIndex = groupIndex;
            numSamples = 0;
            for (int i = 0; i < this.groupIndex.Length; i++)
            {
                DataGroup dataGroup = this.dataGroups[this.groupIndex[i]];
                numSamples += dataGroup.cSize;
            }

            int k = 0;
            this.dataIndex = new int[numSamples];
            for (int i = 0; i < this.groupIndex.Length; i++)
            {
                DataGroup dataGroup = this.dataGroups[this.groupIndex[i]];
                for (int j = 0; j < dataGroup.cSize; j++)
                {
                    this.dataIndex[k] = dataGroup.iStart + j;
                    k++;
                }
            }
        }

        public int NumSamples
        {
            get
            {
                return numSamples;
            }
        }

        public int[] DataIndex
        {
            get
            {
                return dataIndex;
            }
        }

        public int NumGroups
        {
            get
            {
                return groupIndex.Length;
            }
        }

        public int[] GroupIndex
        {
            get
            {
                return groupIndex;
            }
        }   
    
        /// <summary>
        /// ways to enumerate all data groups in the dataset
        /// look up the index-th DataGroup element in the data set
        /// </summary>
        /// <param name="index">the index is internal to the data set</param>
        /// <returns>the index-th element in the dataset </returns>
        public DataGroup GetDataGroup(int index)
        {
            return this.dataGroups[this.groupIndex[index]];
        }

        public void Sort()
        {
            //qiangwu: probably make it run bit faster
            Array.Sort(groupIndex);
            Array.Sort(dataIndex);            
        }

        private int numSamples;
        private DataGroups dataGroups;
        private int[] groupIndex;
        private int[] dataIndex;
    }

    /// <summary>
    /// Converting the input label value into corresponding values suitable for the application
    /// </summary>
    public interface LabelConverter
    {
        float convert(float inLabel);
    }

    public class LabelConverterNull : LabelConverter
    {
        public float convert(float inLabel)
        {
            return inLabel;
        }
    }

    public class LabelConverterMSN : LabelConverter
    {
        public LabelConverterMSN(float labelForUnlabeled)
        {
            this.labelForUnlabeled = labelForUnlabeled;
        }

        public float convert(float inLabel)
        {
            if (inLabel < 0)
            {
                return this.labelForUnlabeled;
            }
            return inLabel;
        }

        private float labelForUnlabeled = 0;
    }

}
