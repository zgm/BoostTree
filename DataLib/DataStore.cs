using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.TMSN.IO;

namespace Microsoft.TMSN
{
    public interface IDataStore<T> : ICloneable, IDataMatrix<T>
    {        
        /// <summary>
        /// Adding new data to the store
        /// </summary>
        /// <param name="newData"></param>
        void Append(T[] newData);

        /// <summary>
        /// caller signaling the end of a group/section of data
        /// </summary>
        void Complete();

        /// <summary>
        /// caller signaling the start of a new group/section of data
        /// </summary>
        void Reset();  
    }

    public class DataStore<T> : IDataStore<T>, IBinaryWritable
    {
        [NonSerialized]
        protected ArrayList m_DataList;
        protected DataMatrixDenseRowMajor<T> m_Content;

        public DataStore()
        {
            m_DataList = new ArrayList();
        }
        
        public void Serialize(BinaryWriterEx binWriterEx)
        {
            ((IBinaryWritable)this.m_Content).Serialize(binWriterEx);
        }       

        protected DataStore(DataStore<T> dataStore)
        {
            m_DataList = (ArrayList)dataStore.m_DataList.Clone();
            m_Content = (DataMatrixDenseRowMajor<T>)dataStore.m_Content.Clone();
        }

        public object Clone()
        {
            return new DataStore<T>(this);
        }

        /// <summary>
        /// Transfering the data accumulated so far and reset the storage
        /// </summary>
        public void Complete()
        {
            T[][] content = (T[][])m_DataList.ToArray(typeof(T[]));

            m_Content = new DataMatrixDenseRowMajor<T>(content);

            //qiangwu: which one is beter???
            //m_DataList.Clear();
            m_DataList = new ArrayList();
        }

        public void Reset()
        {
            m_Content = null;
        }

        public int NumRows
        {
            get
            {
                if (null != m_Content)
                {
                    return m_Content.NumRows;
                }
                return 0;
            }
        }

        public int NumCols
        {
            get
            {
                if (null != m_Content)
                {
                    return m_Content.NumCols;
                }
                return 0;
            }
        }

        public T GetValue(int iRow, int iCol)
        {
            return m_Content.GetValue(iRow, iCol);
        }

        public void GetValues(int iRow, T[] values)
        {
            m_Content.GetValues(iRow, values);
        }

        public void GetValues(int iCol, int[] idxData, T[] dataOut, int cData)
        {
            m_Content.GetValues(iCol, idxData, dataOut, cData);            
        }
        
        public void Append(T[] newData)
        {
            m_DataList.Add(newData);
        }
    }        

    [Serializable]
    public class DataStoreSparse : IDataStore<float>, IBinaryWritable
    {
        [NonSerialized]
        private ArrayList m_DataList;
        [NonSerialized]
        private int numCols = -1;

        private DataMatrixSparse<float> m_Content = null;
        
        public DataStoreSparse()
        {
            this.m_DataList = new ArrayList();            
            this.numCols = -1;
        }

        public void Serialize(BinaryWriterEx binWriterEx)
        {
            ((IBinaryWritable)this.m_Content).Serialize(binWriterEx);
        }

        protected DataStoreSparse(DataStoreSparse dataStoreSparse)
        {
            this.m_DataList = (ArrayList)dataStoreSparse.m_DataList.Clone();
            this.m_Content = (DataMatrixSparse<float>)dataStoreSparse.m_Content.Clone();
            this.numCols = dataStoreSparse.NumCols;
        }

        public object Clone()
        {
            return new DataStoreSparse(this);
        }

        /// <summary>
        /// Transfering the data accumulated so far and reset the storage
        /// </summary>
        public void Complete()
        {
            RowSparse<float>[] matrix = (RowSparse<float>[])m_DataList.ToArray(typeof(RowSparse<float>));
            this.m_Content = new DataMatrixSparse<float>(matrix, this.numCols);

            //qiangwu: which one is beter???
            //m_DataList.Clear();
            m_DataList = new ArrayList();
        }

        public void Reset()
        {
            m_Content = null;
        }

        public int NumRows
        {
            get
            {
                if (null != m_Content)
                {
                    return m_Content.NumRows;
                }
                return 0;
            }
        }

        public int NumCols
        {
            get
            {
                if (null != m_Content)
                {
                    return m_Content.NumCols;
                }
                return this.numCols;
            }
        }

        public float GetValue(int iRow, int iCol)
        {
            return m_Content.GetValue(iRow, iCol);
        }

        public void GetValues(int iRow, float[] values)
        {
            m_Content.GetValues(iRow, values);
        }

        public void GetValues(int iCol, int[] idxData, float[] dataOut, int cData)
        {
            for (int i = 0; i < cData; i++)
            {
                dataOut[i] = m_Content.GetValue(idxData[i], iCol);
            }
        }

        public void Append(float[] newData)
        {
            m_DataList.Add(new RowSparse<float>(newData, 0));
            if (this.numCols < 0)
            {
                this.numCols = newData.Length;
            }
            else
            {
                //assert(this.numCols == newData.Length);
            }
        }
    }

    /// <summary>
    /// Building a DataMatrix by appending rows of content
    /// </summary>
    /// <typeparam name="T">the data type stored in the matrix</typeparam>
    public interface IDataMatrixBuilderSerial<T>
    {
        /// <summary>
        /// Adding new rows sequentially
        /// </summary>
        /// <param name="row">a row of data to append</param>
        void Append(T[] row);

        /// <summary>
        /// caller signaling the end of the matrix building        
        /// </summary>
        /// <returns>the actual data matrix constructed/built</returns>
        IDataMatrix<T> Complete();
    }

    /// <summary>
    /// Building a DataMatrix by adding rows specified by their row index
    /// </summary>
    /// <typeparam name="T">the data type stored in the matrix</typeparam>
    public interface IDataMatrixBuilderRam<T> 
    {
        /// <summary>
        /// Adding new rows randomly specified by row index
        /// </summary>
        /// <param name="row">the row content</param>
        /// <param name="iRow">the row index</param>
        void Add(T[] row, int iRow);

        /// <summary>
        /// caller signaling the end of the matrix building        
        /// </summary>
        /// <returns>the actual data matrix constructed/built</returns>
        IDataMatrix<T> Complete();

    }

    /// <summary>
    /// Building a dense matrix - column major
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DataMatrixBuilderRamDense<T> : IDataMatrixBuilderRam<T> 
    {
        public DataMatrixBuilderRamDense(int cRows, int cCols)
        {
            this.content = new T[cCols][];
            for (int iCol = 0; iCol < cCols; iCol++)
            {
                this.content[iCol] = new T[cRows];
            }
            this.dataMatrix = new DataMatrixDenseColumnMajor<T>(this.content);
        }

        /// <summary>
        /// Adding new rows randomly specified by row index
        /// </summary>
        /// <param name="row">the row content</param>
        /// <param name="iRow">the row index</param>
        public virtual void Add(T[] row, int iRow)
        {
            for (int iCol = 0; iCol < row.Length; iCol++)
            {
                this.content[iCol][iRow] = row[iCol];
            }
        }

        /// <summary>
        /// caller signaling the end of the matrix building        
        /// </summary>
        /// <returns>the actual data matrix constructed/built</returns>
        public virtual IDataMatrix<T> Complete()
        {
            return dataMatrix;
        }

        T[][] content;
        IDataMatrix<T> dataMatrix;
    }

    /// <summary>
    /// Building a sparse matrix
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DataMatrixBuilderRamSparse<T> : IDataMatrixBuilderRam<T> where T : System.IEquatable<T>
    {
        public DataMatrixBuilderRamSparse(int cRows, int cCols, T zeroV)
        {
            this.content = new RowSparse<T>[cRows];
            this.dataMatrix = new DataMatrixSparse<T>(this.content, cCols);
            this.zeroV = zeroV;
        }

        /// <summary>
        /// Adding new rows randomly specified by row index
        /// </summary>
        /// <param name="row">the row content</param>
        /// <param name="iRow">the row index</param>
        public virtual void Add(T[] row, int iRow)
        {
            this.content[iRow] = new RowSparse<T>(row, this.zeroV);
        }

        /// <summary>
        /// caller signaling the end of the matrix building        
        /// </summary>
        /// <returns>the actual data matrix constructed/built</returns>
        public virtual IDataMatrix<T> Complete()
        {
            return dataMatrix;
        }

        RowSparse<T>[] content;
        IDataMatrix<T> dataMatrix;
        T zeroV;
    }


}