using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.TMSN.IO;

namespace Microsoft.TMSN
{    
    public interface IDataMatrix<T> 
    {
        /// <summary>
        /// get the size of the data matrix
        /// </summary>
        int NumRows
        {
            get;
        }
        int NumCols
        {
            get;
        }      

        /// <summary>
        /// retrive the extracted/stored value
        /// Did not return a flat [][] to preserve the flexibility
        /// of more complex implementation (such as sparse matrix) or
        /// tricks to deal with meta data that have a lot of duplicated strings
        /// </summary>
        /// <param name="iRow"></param>
        /// <param name="iCol"></param>
        /// <returns></returns>
        T GetValue(int iRow, int iCol);

        /// <summary>
        /// retrive the values stored in the i-th row in a flat array
        /// </summary>
        /// <param name="iRow">the row index of the data</param>
        /// <param name="values">the flat array to retrive the values of the row</param>
        void GetValues(int iRow, T[] values);

        /// <summary>
        /// retrive all the data specified in idxData stored in the iCol-th column
        /// </summary>
        /// <param name="iCol">the column index of the data to retrive</param>
        /// <param name="idxData">the data points</param>
        /// <param name="dataOut">the output data values</param>
        /// <param name="cData">the total data points</param>        
        void GetValues(int iCol, int[] idxData, T[] dataOut, int cData);       
    }

    public class DataMatrixSerialized<T> : IDataMatrix<T>, IBinaryWritable
    {
        public DataMatrixSerialized()
        {
        }

        //deserialization
        public DataMatrixSerialized(BinaryReaderEx binReaderEx)
        {            
            Type t = binReaderEx.Read<Type>();
            dataMatrix = (IDataMatrix<T>)binReaderEx.Read(t);            
        }

        //seralization
        virtual public void Serialize(BinaryWriterEx binWriterEx)
        {
            binWriterEx.Write(this.GetType());
        }

        virtual public int NumRows
        {
            get
            {
                return dataMatrix.NumRows;
            }
        }

        virtual public int NumCols
        {
            get
            {
                return dataMatrix.NumCols;
            }
        }

        virtual public T GetValue(int iRow, int iCol)
        {
            return dataMatrix.GetValue(iRow, iCol);
        }

        virtual public void GetValues(int iRow, T[] values)
        {
            dataMatrix.GetValues(iRow, values);
        }

        virtual public void GetValues(int iCol, int[] idxData, T[] dataOut, int cData)
        {
            dataMatrix.GetValues(iCol, idxData, dataOut, cData);
        }

        IDataMatrix<T> dataMatrix;
    }

    public class DataMatrixDenseRowMajor<T> : DataMatrixSerialized<T>
    {
        private T[][] matrix;
        int cRows;
        int cCols;
    
        public object Clone()
        {
            return new DataMatrixDenseRowMajor<T>(this.matrix);
        }

        public DataMatrixDenseRowMajor(T[][] matrix)
        {
            this.matrix = matrix;
            this.cRows = (matrix != null) ? matrix.Length : 0;
            this.cCols = (matrix != null && matrix[0] != null) ? matrix[0].Length : 0;
        }

        public DataMatrixDenseRowMajor(BinaryReaderEx binReaderEx)
        {
            this.cRows = binReaderEx.Read<int>();
            this.cCols = binReaderEx.Read<int>();
            this.matrix = binReaderEx.Read<T[][]>();
        }

        override public void Serialize(BinaryWriterEx binWriterEx)
        {
            base.Serialize(binWriterEx);

            binWriterEx.Write(this.GetType());
            binWriterEx.Write(this.cRows);
            binWriterEx.Write(this.cCols);
            binWriterEx.Write(matrix);            
        }

        override public int NumRows
        {
            get
            {
                return this.cRows;
            }
        }

        override public int NumCols
        {
            get
            {
                return this.cCols;
            }
        }

        override public T GetValue(int iRow, int iCol)
        {
            return matrix[iRow][iCol];
        }

        override public void GetValues(int iRow, T[] values)
        {
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = matrix[iRow][i];
            }
        }

        override public void GetValues(int iCol, int[] idxData, T[] dataOut, int cData)
        {
            for (int i = 0; i < cData; i++)
            {
                dataOut[i] = matrix[idxData[i]][iCol];
            }
        }        
    }

    public class DataMatrixDenseColumnMajor<T> : DataMatrixSerialized<T>
    {
        private T[][] matrix;
        int cRows;
        int cCols;
                            
        public DataMatrixDenseColumnMajor(BinaryReaderEx binReaderEx)
        {
            this.cRows = binReaderEx.ReadInt32();
            this.cCols = binReaderEx.ReadInt32();
            this.matrix = binReaderEx.Read<T[][]>();           
        }

        override public void Serialize(BinaryWriterEx binWriterEx)
        {
            base.Serialize(binWriterEx);

            binWriterEx.Write(this.cRows);
            binWriterEx.Write(this.cCols);
            binWriterEx.Write(matrix);            
        }   

        public object Clone()
        {
            return new DataMatrixDenseColumnMajor<T>(this.matrix);
        }

        public DataMatrixDenseColumnMajor(T[][] matrix)
        {
            this.matrix = matrix;
            this.cCols = (matrix != null) ? matrix.Length : 0;
            this.cRows = (matrix != null && matrix[0] != null) ? matrix[0].Length : 0;
        }


        override public int NumRows
        {
            get
            {
                return this.cRows;
            }
        }

        override public int NumCols
        {
            get
            {
                return this.cCols;
            }
        }

        override public T GetValue(int iRow, int iCol)
        {
            return matrix[iCol][iRow];
        }

        override public void GetValues(int iRow, T[] values)
        {
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = matrix[i][iRow];
            }
        }

        override public void GetValues(int iCol, int[] idxData, T[] dataOut, int cData)
        {
            for (int i = 0; i < cData; i++)
            {
                dataOut[i] = matrix[iCol][idxData[i]];
            }

            //unsafe
            //{
            //    fixed (T* colValPtr = matrix[iCol])
            //    fixed (T* dataOutPtr = dataOut)
            //    fixed (int* idxDataPtr = idxData)
            //    {
            //        for (int i = 0; i < cData; i++)
            //        {
            //            dataOutPtr[i] = colValPtr[idxData[i]];
            //        }
            //    }
            //}
        }
    }
    
    [Serializable]
    public class RowSparse<V> where V : IEquatable<V>
    {
        private uint[] indices;
        private V[] values;
        private static V ZeroV;
                    
        public object Clone()
        {
            return new RowSparse<V>(this);
        }

        public RowSparse(RowSparse<V> rowSparse)
        {
            this.indices = (uint[])rowSparse.indices.Clone();
            this.values = (V[])rowSparse.values.Clone();
        }

        public RowSparse(V[] denseValues, V Zero)
        {
            ZeroV = Zero;
            int cNonZeroValues = 0;
            for (int i = 0; i < denseValues.Length; i++)
            {
                if (!denseValues[i].Equals(ZeroV))
                {
                    cNonZeroValues++;
                }
            }
            this.indices = new uint[cNonZeroValues];
            this.values = new V[cNonZeroValues];

            int k = 0;
            for (int i = 0; i < denseValues.Length; i++)
            {
                if (!denseValues[i].Equals(ZeroV))
                {
                    this.indices[k] = (uint)i;
                    this.values[k] = denseValues[i];
                    k++;
                }
            }
        }

        public V GetValue(int iCol)
        {
            V v = ZeroV;
            if (this.indices == null)
            {
                return v;
            }

            int i = Array.BinarySearch<uint>(indices, (uint)iCol);
            
            if (i >= 0)
            {                
                v = this.values[i];
                Debug.Assert(this.indices[i] == iCol);
            }            
            return v;
        }

        public void GetValues(V[] outValues)
        {            
            int i = 0;
            if (this.indices != null)
            {
                for (int j = 0; j < this.indices.Length && i < outValues.Length; j++)
                {
                    while (i < this.indices[j])
                    {
                        outValues[i] = ZeroV;
                        i++;
                    }
                    outValues[i] = this.values[j];
                    i++;
                }
            }

            while (i < outValues.Length) 
            {
                outValues[i] = ZeroV;
                i++;
            }
        }
    }

    public class DataMatrixSparse<T> : DataMatrixSerialized<T> where T : IEquatable<T>
    {
        RowSparse<T>[] matrix;
        int cRows;
        int cCols;
                
        public object Clone()
        {
            return new DataMatrixSparse<T>(this.matrix, this.cCols);
        }

        public DataMatrixSparse(RowSparse<T>[] matrix, int cCols)
        {
            this.matrix = matrix;
            this.cRows = (matrix != null) ? matrix.Length : 0;
            this.cCols = cCols;
        }

        public DataMatrixSparse(BinaryReaderEx binReaderEx)
        {
            this.cRows = binReaderEx.ReadInt32();
            this.cCols = binReaderEx.ReadInt32();
            this.matrix = new RowSparse<T>[this.cRows];
            for (int i = 0; i < this.matrix.Length; i++)
            {
                this.matrix[i] = (RowSparse<T>)binReaderEx.Read<RowSparse<T>>();                
            }
        }

        override public void Serialize(BinaryWriterEx binWriterEx)
        {
            base.Serialize(binWriterEx);

            binWriterEx.Write(this.cRows);
            binWriterEx.Write(this.cCols);
            for (int i = 0; i < this.cRows; i++)
            {
                binWriterEx.Write(this.matrix[i]);
            }
        }

        override public int NumRows
        {
            get
            {
                return this.cRows;
            }
        }

        override public int NumCols
        {
            get
            {
                return this.cCols;
            }
        }

        override public T GetValue(int iRow, int iCol)
        {
            return this.matrix[iRow].GetValue(iCol);
        }

        override public void GetValues(int iRow, T[] values)
        {
            this.matrix[iRow].GetValues(values);            
        }

        override public void GetValues(int iCol, int[] idxData, T[] dataOut, int cData)
        {
            for (int i = 0; i < cData; i++)
            {
                dataOut[i] = this.matrix[idxData[i]].GetValue(iCol);
            }
        }
    }

    public class DataMatrixArray<T> : DataMatrixSerialized<T>
    {
        List<IDataMatrix<T>> listDataMtrix;
        List<int> accRowNum;

        int numRows;
        int numCols;

        public DataMatrixArray()
        {
            listDataMtrix = new List<IDataMatrix<T>>(100);
            accRowNum = new List<int>(100);

            this.numRows = 0;
            this.numCols = 0;
        }

        override public void Serialize(BinaryWriterEx binWriterEx)
        {
            base.Serialize(binWriterEx);

            binWriterEx.Write(this.listDataMtrix.Count);
            for (int i = 0; i < this.listDataMtrix.Count; i++)
            {
                ((IBinaryWritable)this.listDataMtrix[i]).Serialize(binWriterEx);
            }
        }        

        public DataMatrixArray(BinaryReaderEx binReaderEx)
        {            
            int cDataMatrix = binReaderEx.ReadInt32();

            listDataMtrix = new List<IDataMatrix<T>>(cDataMatrix);
            accRowNum = new List<int>(cDataMatrix);

            for (int i = 0; i < cDataMatrix; i++)
            {                                
                IDataMatrix<T> dataMatrix = binReaderEx.Read<DataMatrixSerialized<T>>();
                this.Add(dataMatrix);
            }
        }

        public void Add(IDataMatrix<T> dataMatrix)
        {
            this.numRows += dataMatrix.NumRows;
            this.numCols = dataMatrix.NumCols;

            listDataMtrix.Add(dataMatrix);
            accRowNum.Add(this.numRows);

            //should check the number of columns match
            Debug.Assert(this.NumCols == 0 || dataMatrix.NumCols == this.NumCols);
        }

        override public int NumRows
        {
            get
            {
                return this.numRows;
            }
        }

        override public int NumCols
        {
            get
            {
                return this.numCols;
            }
        }

        override public T GetValue(int iRow, int iCol)
        {
            int iData = -1;
            int j = ResolveIdx(iRow, ref iData);

            //look up the actual data
            return listDataMtrix[j].GetValue(iData, iCol);
        }

        override public void GetValues(int iRow, T[] values)
        {
            int iData = -1;
            int j = ResolveIdx(iRow, ref iData);

            //look up the actual data
            listDataMtrix[j].GetValues(iData, values);
        }

        override public void GetValues(int iCol, int[] idxData, T[] dataOut, int cData)
        {
            for (int i = 0; i < cData; i++)
            {
                dataOut[i] = this.GetValue(idxData[i], iCol);
            }
        }

        protected int ResolveIdx(int iRow, ref int iData)
        {
            //The zero-based index of item in the sorted List, if item is found; 
            //otherwise, a negative number that is the bitwise complement of the 
            //index of the next element that is larger than item or, if there is 
            //no larger element, the bitwise complement of Count. 
            int j = accRowNum.BinarySearch(iRow);
            j = (j < 0) ? ((0 - j) - 1) : (j + 1);

            int prevCount = accRowNum[j] - listDataMtrix[j].NumRows;

            iData = iRow - prevCount;

            return j;
        }
    }

}