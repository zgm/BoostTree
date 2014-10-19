using System;
using System.IO;
using System.Collections;
using Microsoft.TMSN.IO;

namespace Microsoft.TMSN
{
    /// <summary>
    /// The generic interface for data generation:
    /// (1)understands the tsv header syntax 
    /// (2)reads and inteperates/parses each incoming data lines accordingly
    /// (3)store the parsed the data
    /// </summary>
    public interface IData : ICloneable
    {
        /// <summary>
        /// Reads and understands/inteperates the column namses (tsv header)
        /// </summary>
        /// <param name="columnNames"></param>
        void ReadColumnNames(string[] columnNames);
        /// <summary>
        /// Signaling the beginning of a new section of data
        /// </summary>
        void Reset();
        /// <summary>
        /// process and/or add a line of data content
        /// </summary>
        /// <param name="dataFields"></param>
        void ProcessLine(string[] dataFields);
        /// <summary>
        /// signaling the end of the current section has reached
        /// </summary>
        void Complete();        
    }
    
    /// <summary>
    /// The general implementation of single type of homogeneous data
    /// such as FeatureData (float[][]), metaData(string[][]), or labelData(float[][])
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class SingleData<T> : IData
    {
        protected IParser<T> m_Parser;
        protected IDataStore<T> m_dataStore;                
        
        /// <summary>
        /// initialize the data inteprater given the columnNames
        /// </summary>
        /// <param name="columnNames"></param>
        public void ReadColumnNames(string[] columnNames)
        {
            m_Parser.ReadColumnNames(columnNames);
        }

        public void Reset()
        {
            m_dataStore.Reset();
        }

        /// <summary>
        /// extracting and storing the corresponding data from a row/line of content in a TSV file
        /// </summary>
        /// <param name="dataFields"></param>
        /// <returns></returns>
        public void ProcessLine(string[] dataFields)
        {
            T[] data = m_Parser.Parse(dataFields);
            m_dataStore.Append(data);            
        }

        /// <summary>
        /// Transfering the data accumulated so far and reset the storage
        /// </summary>
        public void Complete()
        {
            m_dataStore.Complete();
        }
        
        /// <summary>
        /// retrive the extracted data matrix
        /// </summary>
        public virtual IDataStore<T> Data
        {
            get
            {
                return m_dataStore;
            }
        }        
        
        /// <summary>
        /// find the index of a column given its index in TSV file header
        /// i.e. dataIndex(TSVFile.GetColumnIndex("Name_i")) returns the index of "Name_i" in the data matrix
        /// </summary>
        /// <param name="columnIndex"> the column index of the column in TSV file</param>
        /// <returns>its column index in feature data matrix</returns>
        public virtual int dataIndex(int columnIndex)
        {
            return m_Parser.dataIndex(columnIndex);
        }

        public virtual int NumColumns
        {
            get
            {
                return m_Parser.NumColumns;
            }
        }

        /// <summary>
        /// plugging other feather data parser here for different
        /// tsv syntax or feature data generation behavior 
        /// </summary>
        public virtual IParser<T> Parser
        {
            set
            {
                m_Parser = value;
            }
            get
            {
                return m_Parser;
            }
        }

        protected SingleData()
        {
            m_Parser = null;
            m_dataStore = null;
        }

        protected SingleData(SingleData<T> data)
        {
            m_Parser = data.m_Parser;
            m_dataStore = (IDataStore<T>)data.m_dataStore.Clone();
        }

        public abstract object Clone();

        //implementing other fancy data representation such as 
        //iterating the data row by row and by column names in the future???
    }

    /// <summary>
    /// General implementation of a composite data type that can process/extract serveral different kinds
    /// of data (feature, label, meta, ...) while iterating through the tsv data file once.
    /// </summary>
    public class CompositeData : IData
    {
        protected IData[] m_dataArray = null;

        public CompositeData()
        {
            m_dataArray = null;
        }

        public CompositeData(IData[] dataArray)
        {
            m_dataArray = dataArray;
        }        

        public void ReadColumnNames(string[] columnNames)
        {
            for (int i = 0; i < m_dataArray.Length; i++)
            {
                if (m_dataArray[i] != null)
                {
                    m_dataArray[i].ReadColumnNames(columnNames);
                }
            }
        }

        public void Reset()
        {
            for (int i = 0; i < m_dataArray.Length; i++)
            {
                if (m_dataArray[i] != null)
                {
                    m_dataArray[i].Reset();
                }
            }
        }

        public void ProcessLine(string[] dataFields)
        {            
            for (int i = 0; i < m_dataArray.Length; i++)
            {
                if (m_dataArray[i] != null)
                {
                    m_dataArray[i].ProcessLine(dataFields);
                }
            }            
        }

        public void Complete()
        {
            for (int i = 0; i < m_dataArray.Length; i++)
            {
                if (m_dataArray[i] != null)
                {
                    m_dataArray[i].Complete();
                }
            }
        }

        public virtual object Clone()
        {
            IData[] dataArray = (IData[])m_dataArray.Clone();
            for (int i = 0; i < m_dataArray.Length; i++)
            {
                if (null != dataArray[i])
                {
                    if (m_dataArray[i] != null)
                    {
                        dataArray[i] = (IData)dataArray[i].Clone();
                    }
                }
            }

            return new CompositeData(dataArray);            
        }         
    }

    //Building MSN Search related data types: Feature, Label, Meta on top of previously defined basic data types
    //It should probably split into a new file since it is a higher layer of abstraction

    public class FeatureData : SingleData<float>
    {
        public FeatureData()
        {
            m_Parser = DefaultParser();
            
            //the dense version of the feature vector DataStore
            //m_dataStore = new DataStore<float>();

            //the sparse version of the feature vector DataStore
            m_dataStore = new DataStoreSparse();
        }        

        protected FeatureData(FeatureData data)
            : base(data)
        {
        }

        public override object Clone()
        {
            return new FeatureData(this);
        }

        protected IParser<float> DefaultParser()
        {
            return new MsnFeatureParser();
        }
    }

    public class MetaData : SingleData<string>
    {        
        public MetaData()
        {
            m_Parser = DefaultParser();
            m_dataStore = new DataStore<string>();
        }        

        protected IParser<string> DefaultParser()
        {
            return new MsnMetaParser();
        }

        protected MetaData(MetaData data)
            : base(data)
        {
        }

        public override object Clone()
        {
            return new MetaData(this);
        }
    }

    public class LabelData : SingleData<float>
    {
        public LabelData()
        {
            m_Parser = DefaultParser();
            m_dataStore = new DataStore<float>();
        }        

        protected IParser<float> DefaultParser()
        {
            return new MsnLabelParser();
        }

        protected LabelData(LabelData data)
            : base(data)
        {
        }

        public override object Clone()
        {
            return new LabelData(this);
        }

        public int Length
        {
            get
            {
                return m_dataStore.NumRows;
            }
        }

        public float this[int index]
        {
            get
            {
                return m_dataStore.GetValue(index, 0);
            }
        }

        public bool IsAllSame()
        {            
            for (int i = 1; i < this.Length; ++i)
            {
                if (this[i] != this[i - 1])
                {
                    return false;
                }
            }
            return true;
        }
    }

    /// <summary>
    /// Basic TSV data parsing and presentation:
    /// It consists of 3 different types: feature vector, label, and meta data for each data points
    /// Customizing the behaviors by plugging in different parser or datastore objects
    /// It contains all the information to derive other data objects (such as rankdata which contains pairs)
    /// </summary>
    public class MsnData : CompositeData
    {
        public enum IdxType {IdxFeature=0, IdxLabel, IdxMeta, IdxMax};

        static IdxType[] FullIdxType = { IdxType.IdxFeature, IdxType.IdxLabel, IdxType.IdxMeta };

        public MsnData(IdxType[] arrayTypes)
        {
            m_dataArray = new IData[(int)IdxType.IdxMax];
            for (int i=0; i<arrayTypes.Length; i++)
            {
                switch (arrayTypes[i])
                {
                    case IdxType.IdxFeature:
                        m_dataArray[(int)arrayTypes[i]] = new FeatureData();
                        break;                        
                    case IdxType.IdxLabel:
                        m_dataArray[(int)IdxType.IdxLabel] = new LabelData();
                        break;                    
                    case IdxType.IdxMeta:            
                        m_dataArray[(int)IdxType.IdxMeta] = new MetaData();
                        break;
                    default:
                        break;
                }                
            }            
        }

        public MsnData()
            : this(FullIdxType)
        {
                       
        }

        public MsnData(MsnData data)
        {
            m_dataArray = (IData[])data.m_dataArray.Clone();
            for (int i = 0; i < m_dataArray.Length; i++)
            {
                if (null != m_dataArray[i])
                {
                    m_dataArray[i] = (IData)m_dataArray[i].Clone();
                }
            }
        }

        public override object Clone()
        {
            return new MsnData(this);
        }

        public FeatureData Feature
        {
            get
            {
                return (FeatureData)m_dataArray[(int)IdxType.IdxFeature];
            }
            set
            {
                m_dataArray[(int)IdxType.IdxFeature] = value;
            }
        }

        public LabelData Labels
        {
            get
            {
                return (LabelData)m_dataArray[(int)IdxType.IdxLabel];
            }
            set
            {
                m_dataArray[(int)IdxType.IdxLabel] = value;
            }
        }

        public MetaData Meta
        {
            get
            {
                return (MetaData)m_dataArray[(int)IdxType.IdxMeta];
            }
            set
            {
                m_dataArray[(int)IdxType.IdxMeta] = value;
            }
        }
    }

}
