using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using Microsoft.TMSN.IO;

namespace Microsoft.TMSN
{
    /// <summary>
    /// The is generic parser interface for interpreting each line of data content in a TSV file 
    /// given its header (column names).
    /// </summary>   
    public abstract class IParser<T>
    {                        
        //The column names that the Parser cares about
        protected string[] m_colNames;

        protected class Column
        {
            public int inputIndex;		// index in the original TSV file     
            public int colNamesIndex;   // m_colNames[colNamesIndex] = the name of the column
        }
        protected Column[] m_Columns = null;
        protected int m_numOutputColumns = 0;
        
        /// <summary>
        /// The length of the feature vector
        /// </summary>
        public virtual int NumColumns
        {
            get
            {
                return m_numOutputColumns;
            }
        }

        /// <summary>
        /// initialize the feature data parser from the columnNames (header) of a TSV file.
        /// Based on which, the parser will inteperate each line of data in the TSV file.
        /// </summary>
        /// <param name="columnNames"> the header of the tsv file</param>
        public virtual void ReadColumnNames(string[] columnNames)
        {
            // Identify which fields are Label fields
            ArrayList colsList = new ArrayList();
            m_numOutputColumns = 0;

            for (int i = 0; i < columnNames.Length; i++)
            {
                if (IsParseColumn(columnNames[i]))
                {
                    Column col = new Column();
                    col.inputIndex = i;
                    col.colNamesIndex = ColumnNamesIdx(columnNames[i]);              
                    m_numOutputColumns += 1;
                    
                    colsList.Add(col);
                }
            }
            m_Columns = (Column[])colsList.ToArray(typeof(Column));
        }

        /// <summary>
        /// find the index of a particular feature given its column index in TSV file header
        /// i.e. dataIndex(TSVFile.GetColumnIndex("Feature_i")) returns the index of "Feature_i" in the data matrix
        /// </summary>
        /// <param name="columnIndex"> the column index of the feature in TSV file</param>
        /// <returns>the column index of the feature in feature data matrix</returns>        
        public virtual int dataIndex(int columnIndex)
        {
            for (int i = 0; i < m_Columns.Length; i++)
            {
                if (m_Columns[i].inputIndex == columnIndex)
                {
                    return i;
                }
            }
            return -1;
        }

        /// <summary>
        /// find the index of a particular feature given its name (in string) column index in TSV file        
        /// </summary>
        /// <param name="columnName"> the name of the feature in TSV file</param>
        /// <returns>the column index of the feature in feature data matrix</returns>        
        public virtual int dataIndex(string columnName)
        {
            for (int i = 0; i < m_Columns.Length; i++)
            {
                if (0 == String.Compare(columnName, m_colNames[m_Columns[i].colNamesIndex], true))
                {
                    return i;
                }               
            }
            return -1;
        }

        /// <summary>
        /// find the column index of the feature in TSV file header
        /// </summary>
        /// <param name="dataIndex">the column index of the feature in feature data matrix</param>
        /// <returns>the column index of the feature in TSV file</returns>
        public virtual int columnIndex(int dataIndex)
        {
            if (dataIndex >= 0 && dataIndex < m_Columns.Length)
            {
                return m_Columns[dataIndex].inputIndex;
            }
            return -1;
        }

        /// <summary>
        /// extract the corresponding data from a row of string in tsv file
        /// </summary>
        /// <param name="queryFields">a row of string in tsv file</param>
        /// <returns>the data represented by the string</returns>
        public virtual T[] Parse(string[] Fields)
        {
            //don't bother if there is nothing to parse
            if (0 == NumColumns)
            {
                return null;
            }

            T[] Values = new T[NumColumns];
            int currentCol = 0;
            for (int j = 0; j < m_Columns.Length; j++)
            {
                Values[currentCol] = GetValue(Fields, m_Columns[j].inputIndex);
                currentCol++;                
            }

            return Values;
        }

        protected virtual bool IsParseColumn(string columnName)
        {
            for (int i = 0; i < m_colNames.Length; i++)
            {
                if (0 == String.Compare(columnName, m_colNames[i], true))
                {
                    return true;
                }
            }
            return false;
        }

        protected virtual int ColumnNamesIdx(string columnName)
        {
            for (int i = 0; i < m_colNames.Length; i++)
            {
                if (0 == String.Compare(columnName, m_colNames[i], true))
                {
                    return i;
                }
            }
            return -1;
        }

        protected abstract T GetValue(string[] Fields, int inputIndex);                            
    }    

    /// <summary>
    /// Parsing/extracting the meta data given the current MSN Search TSV file format and column name syntax
    /// Parsing meta data into array of strings
    /// </summary>
    public class MsnMetaParser : IParser<string>
    {       
        //by default we are not keeping track of meta data due to memory cost
        //the following are the set of meta fields used in MSN Search TSV file
        //MetaNamesDefault = {"m:Query", "m:QueryId", "m:Url", "m:DocId", "m:Rating", "m:ResultType", "m:Date"};
        protected readonly string[] MetaNamesDefault = { };

        public MsnMetaParser()
        {
            m_colNames = MetaNamesDefault;
        }

        public MsnMetaParser(string[] LabelNames)
        {
            m_colNames = LabelNames;
        }

        protected override string GetValue(string[] Fields, int inputIndex)
        {
            return Fields[inputIndex];            
        }
    }

    /// <summary>
    /// Parsing/extracting the data label given the current MSN Search TSV file format and column name syntax
    /// Parsing labels into array of floats
    /// </summary>
    public class MsnLabelParser : IParser<float>
    {
        protected readonly string[] LabelNameDefault = { "m:Rating" };

        IDictionary<string, int> LabelNameValue;        
        private IDictionary<string, int> CreateLabelNameValueDefault()
        {
            IDictionary<string, int> LabelNameValue = new Dictionary<string, int>();

            //labels used for 5 points web-search
            LabelNameValue.Add("Definitive", 4);
            LabelNameValue.Add("Perfect", 4);
            LabelNameValue.Add("Excellent", 3);
            LabelNameValue.Add("Good", 2);
            LabelNameValue.Add("Fair", 1);
            LabelNameValue.Add("Bad", 0);
            LabelNameValue.Add("Detrimental", 0);

            //labels used for 3 points image-search
            LabelNameValue.Add("Relevant", 2);
            LabelNameValue.Add("HighlyRelevant", 1);
            LabelNameValue.Add("NotRelevant", 0);

            LabelNameValue.Add("Unjudged", 0);
            LabelNameValue.Add("Unknown", -1);
            LabelNameValue.Add("", -1);

            return LabelNameValue;
        }

        private IDictionary<string, int> CreateLabelNameValueCustom(string fileName)
        {
            IDictionary<string, int> LabelNameValue = new Dictionary<string, int>();

            StreamReader rStream = new StreamReader(fileName);
            string line = null;
            while ((line = rStream.ReadLine()) != null)
            {
                string[] fields = line.Split('\t');
                int value = int.Parse(fields[1]);
                LabelNameValue.Add(fields[0], value);
            }            

            return LabelNameValue;
        }

        public MsnLabelParser()  
            : this(null, null)          
        {                      
        }

        public MsnLabelParser(string[] LabelNames) 
            : this(LabelNames, null)
        {            
        }

        public MsnLabelParser(string[] LabelNames, string LabelNameValueFile)
        {                    
            this.m_colNames = (LabelNames == null) ? LabelNameDefault : LabelNames;

            if (LabelNameValueFile == null)
            {
                this.LabelNameValue = CreateLabelNameValueDefault();
            }
            else
            {
                this.LabelNameValue = CreateLabelNameValueCustom(LabelNameValueFile);
            }
        }

        protected override float GetValue(string[] Fields, int inputIndex)
        {            
            string label = Fields[inputIndex];
            int valuelInt;

            bool fExist = this.LabelNameValue.TryGetValue(label, out valuelInt);

            if (fExist)
            {
                return (float)valuelInt;
            }
            else
            {
                try
                {
                    return float.Parse(label);
                }
                catch (Exception)
                {
                    Console.WriteLine("Unable to parse label " + label + " into a float. Using 0");
                    return 0;
                }
            }           
        }
    }

    /// <summary>
    /// feature vector parser given the current MSN Search TSV file format and column name syntax
    /// override or replace this class to define new feature vector extraction behavior
    /// Parsing feature vector into array of float
    /// </summary>
    public class MsnFeatureParser : IParser<float>
    {
        class FeatureColumn : Column
        {            
            public bool binned;		//  true if this feature is a binned feature
            public int numBins;		//  if binned=true, this specifies the number of bins in the output
            public bool sparse;		// true if this feature is a set of sparse features
            public int numSparseFeatures;	// if sparse=true, this specifies the number of features            
        }                     
                
        //In this implementation all columns without "m:" prefix are consider as features
        //without explict specifing the ones to include
        public MsnFeatureParser()
        {
            m_colNames = null;
        }

        //only include the ones specified in the list
        public MsnFeatureParser(string[] FeatureNames)
        {
            m_colNames = FeatureNames;
        }

        /// <summary>
        /// initialize the feature data parser from the columnNames (header) of a TSV file.
        /// Based on which, the parser will inteperate each line of data in the TSV file.
        /// </summary>
        /// <param name="columnNames"> the header of the tsv file</param>       
        public override void ReadColumnNames(string[] columnNames)
        {
            // Identify which fields are feature fields
            ArrayList featuresList = new ArrayList();
            m_numOutputColumns = 0;
            for (int i = 0; i < columnNames.Length; i++)
            {
                if (IsParseColumn(columnNames[i]))
                {
                    FeatureColumn col = new FeatureColumn();
                    col.inputIndex = i;
                    if (IsBinnedColumn(columnNames[i]))
                    {
                        col.binned = true;
                        col.numBins = NumBinsForColumn(columnNames[i]);
                        m_numOutputColumns += col.numBins;
                    }
                    else if (IsSparseColumn(columnNames[i]))
                    {
                        col.sparse = true;
                        col.numSparseFeatures = NumFeaturesForSparseColumn(columnNames[i]);
                        m_numOutputColumns += col.numSparseFeatures;
                    }
                    else
                    {
                        m_numOutputColumns += 1;
                    }
                    featuresList.Add(col);
                }
            }
            m_Columns = (Column[])featuresList.ToArray(typeof(FeatureColumn));
        }        

        /// <summary>
        /// extract a feature vector from a row of string in tsv file
        /// </summary>
        /// <param name="queryFields">a row of string in tsv file</param>
        /// <returns>the feature vector represented by the string</returns>
        public override float[] Parse(string[] queryFields)
        {
            if (0 == NumColumns)
            {
                return null;
            }

            FeatureColumn[] featureColumns = (FeatureColumn[])m_Columns;
            bool truncatedSparseFeatures = false;

            float [] features = new float[m_numOutputColumns];
            int currentCol = 0;
            for (int j = 0; j < m_Columns.Length; j++)
            {
                // TSV Files are allowed to truncate lines if the rest of the fields are all 0
                if (featureColumns[j].binned)
                {
                    float val = GetValue(queryFields, m_Columns[j].inputIndex);
                    for (int k = 0; k < featureColumns[j].numBins; k++)
                    {
                        if (k == (int)val)
                            features[currentCol] = 1;
                        else
                            features[currentCol] = 0;
                        currentCol++;
                    }
                }
                else if (featureColumns[j].sparse)
                {
                    // This can be made more efficient by just walking along the string
                    string[] vals = queryFields[featureColumns[j].inputIndex].Split(' ');
                    foreach (string val in vals)
                    {
                        if (val.Length == 0)
                            continue;
                        int featureNum = int.Parse(val);
                        if (featureNum < featureColumns[j].numSparseFeatures)
                            features[currentCol + featureNum] = 1;
                        else
                            truncatedSparseFeatures = true;
                    }
                    currentCol += featureColumns[j].numSparseFeatures;
                }
                else
                {
                    float val = GetValue(queryFields, featureColumns[j].inputIndex);
                    features[currentCol] = val;
                    currentCol++;
                }
            }

            if (truncatedSparseFeatures)
            {
                Console.WriteLine("Warning: Some of the sparse feature values were outside of the column's range, so were dropped");
            }
            return features;
        }

        protected override float GetValue(string[] queryFields, int index)
        {
            if (queryFields.Length <= index)
                return 0F;
            if (queryFields[index].Length == 0)
                return 0F;
            try
            {
                return float.Parse(queryFields[index]);
            }
            catch (Exception)
            {
                Console.WriteLine("Caught exception while parsing float value of '" + queryFields[index] + "'");
                return 0;
            }
        }

        /// <summary>
        /// Returns whether column i is a feature column. TSV files have two types of
        /// columns: meta and feature. The meta columns are denoted by "m:" preceding the
        /// column name.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        protected override bool IsParseColumn(string Name)
        {
            if (null == m_colNames)
            {
                return !Name.StartsWith("m:");
            }
            else
            {
                return base.IsParseColumn(Name);
            }            
        }

        /// <summary>
        /// Returns whether column i is a binned column. Feature columns may either be
        /// normal or binned. A normal column just contains the floating point value of
        /// its feature. A binned column contains an integer specifying which bin has value
        /// 1, with the remaining bins having value 0. The number of bins is specified in the
        /// column name, which is "bin:numBins:name". Example: "bin:10:TopLevelDomain".
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        protected bool IsBinnedColumn(string Name)
        {
            return Name.StartsWith("bin:");            
        }
        /// <summary>
        /// Gives the number of bins for a binned column
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        protected int NumBinsForColumn(string Name)
        {
            string[] pieces = Name.Split(':');
            return int.Parse(pieces[1]);
        }

        /// <summary>
        /// Returns whether the given column is a sparse column
        ///  a sparse column has the format "sparse:numFeatures:name" such as "sparse:50000:HasWord"
        ///  the value of the column is a space separated list of features, from 0 to numFeatures-1.
        ///  The feature vector is grown by numFeatures, and each one takes value 0 if not
        ///  specified, and 1 otherwise.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        protected bool IsSparseColumn(string Name)
        {
            return Name.StartsWith("sparse:");
        }
        /// <summary>
        /// If it is a sparse Column (see IsSparseColumn()), this returns the number
        ///  of features the column refers to.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        protected int NumFeaturesForSparseColumn(string Name)
        {
            string[] pieces = Name.Split(':');
            return int.Parse(pieces[1]);
        }        
    }
}

