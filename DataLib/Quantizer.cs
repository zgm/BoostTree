using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Threading;
using Microsoft.TMSN;

namespace StochasticGradientBoost
{
    class MatrixQuantizer : Processor
    {
        public MatrixQuantizer(IDataMatrix<float> dataMatrix)
        {
            this.dataMatrix = dataMatrix;
            this.dataMap = new float[this.dataMatrix.NumCols][];
        }

        virtual public int cJobs
        {
            get
            {
                return this.dataMatrix.NumCols;
            }
        }

        virtual public ProcessorThread CreatePerThread()
        {
            return new QuantizerThreadObj(this.dataMap, this.dataMatrix);
        }

        public CodeBook codeBook
        {
            get
            {
                if (this.m_codeBook == null)
                {
                    this.m_codeBook = new CodeBook(dataMap);
                }
                return m_codeBook;
            }
        }

        // input data
        IDataMatrix<float> dataMatrix;

        //output data
        float[][] dataMap;
        CodeBook m_codeBook;
    }

    class QuantizerThreadObj : ProcessorThread
    {
        public QuantizerThreadObj(float[][] dataMap, IDataMatrix<float> dataMatrix)
        {
            //allocating temporary data used for quantization            
            this.cDataSize = dataMatrix.NumRows;
            this.input = new float[this.cDataSize];
            this.quantizedData = new ushort[this.cDataSize]; // This is not needed by CJCB's binning
            this.sortedIndex = Vector.IndexArray(this.cDataSize);
            this.changePointData = new float[this.cDataSize];
            //this.bucketSize = new int[cDataSize];

            //input data
            this.dataMatrix = dataMatrix;
            this.dataMap = dataMap;
        }

        virtual public void SetData(int iJob)
        {
            this.iCol = iJob;
        }

        virtual public void process()
        {
            //preparing the data for the thread to consume                
            for (int j = 0; j < this.dataMatrix.NumRows; j++)
                this.input[j] = this.dataMatrix.GetValue(j, this.iCol);

            float minGap = this.initMinGap;

            Array.Sort(this.sortedIndex);
            Array.Sort(this.input, this.sortedIndex);

            int numChanges = 0;
            bool moreQuantization = true;
            while (moreQuantization)
            {
                this.quantizedData[0] = 0;
                int changePoint = 0;
                numChanges = 0;
                this.changePointData[changePoint] = this.input[0];

                for (int i = 0; i < this.cDataSize; i++)
                {
                    if (this.input[i] - this.input[changePoint] < minGap)
                    {
                        this.quantizedData[i] = this.quantizedData[changePoint];
                    }
                    else
                    {
                        this.quantizedData[i] = (ushort)(this.quantizedData[changePoint] + 1);
                        changePoint = i;
                        this.changePointData[++numChanges] = this.input[changePoint];
                        //assert numChanges == this.quantizedData[i]
                    }
                    if (this.quantizedData[i] == this.maxNumBins - 1)
                    {
                        minGap *= 2.0F;
                        break;
                    }
                    if (i == this.cDataSize - 1)
                        moreQuantization = false;
                }
            }

            //fill in the output data structures
            this.dataMap[this.iCol] = new float[numChanges + 1];
            for (int i = 0; i < numChanges + 1; i++)
                this.dataMap[this.iCol][i] = this.changePointData[i];

#if false // DEBUG
            Console.Write("Number of clusters {0}: ", numChanges + 1);
            for (int i = 0; i < numChanges + 1 && i <= 20; i++)
            {
                Console.Write("{0} ", this.changePointData[i]);
            }
            Console.WriteLine();
#endif
        }

        //local data for quantization computation
        private int cDataSize; //cDataSize == input.Length
        private float[] input;
        private int[] sortedIndex;
        private float[] changePointData;
        ushort[] quantizedData;

        //constant configurations of the quantizer
        private float initMinGap = 1e-8F;
        //KMS: change to coarser bins
        private float maxNumBins = ushort.MaxValue + 1;

        //private float maxNumBins = byte.MaxValue + 1;
        //private int minNumSamplesPerBin = 200; // REVIEW: this should be passed as an argument
                
        //input: the original data
        int iCol;
        IDataMatrix<float> dataMatrix;

        //output: code book
        float[][] dataMap;

    }

    class MatrixEncoder : Processor
    {
        public MatrixEncoder(IDataMatrix<float> dataMatrix, CodeBook codeBook, IDataMatrixBuilderRam<ushort> matrixBuilder)
        {
            this.dataMatix = dataMatrix;
            this.codeBook = codeBook;
            this.matrixBuilder = matrixBuilder;
        }

        virtual public int cJobs
        {
            get
            {
                return this.dataMatix.NumRows;
            }
        }

        virtual public ProcessorThread CreatePerThread()
        {
            return new EncoderThreadObj(this.codeBook, this.dataMatix, this.matrixBuilder);
        }

        public IDataMatrix<ushort> CodedMatrix
        {
            get
            {
                if (codedMatrix == null)
                {
                    codedMatrix = matrixBuilder.Complete();
                }
                return codedMatrix;
            }
        }

        // input data
        IDataMatrix<float> dataMatix;
        CodeBook codeBook;

        //intermediate helping object
        IDataMatrixBuilderRam<ushort> matrixBuilder;

        //output data
        IDataMatrix<ushort> codedMatrix;        
    }

    class EncoderThreadObj : ProcessorThread
    {
        public EncoderThreadObj(CodeBook codeBook, IDataMatrix<float> dataMatrix, IDataMatrixBuilderRam<ushort> matrixBuilder)
        {
            //temporary data used by encoding one data group
            this.cDataSize = dataMatrix.NumCols;
            this.codedRow = new ushort[this.cDataSize];

            //input data
            this.dataMatrix = dataMatrix;
            this.codeBook = codeBook;

            //store the encoded features
            this.matrixBuilder = matrixBuilder;
        }

        virtual public void SetData(int iJob)
        {
            this.iRow = iJob;
        }

        virtual public void process()
        {                         
            for (int iCol = 0; iCol < this.dataMatrix.NumCols; iCol++)
            {
                float x = this.dataMatrix.GetValue(this.iRow, iCol);
                codedRow[iCol] = codeBook.CodeLookup(iCol, x);
            }
            this.matrixBuilder.Add(codedRow, this.iRow);                               
        }

        //input data
        private int cDataSize; 
        private ushort[] codedRow;

        //intermediate helping object
        IDataMatrixBuilderRam<ushort> matrixBuilder;

        //input: the original data
        int iRow;
        IDataMatrix<float> dataMatrix;
        CodeBook codeBook;
    }

    [Serializable]
    public class CodeBook
    {
        public CodeBook(float[][] dataMap)
        {
            this.dataMap = dataMap;
        }

        public ushort CodeLookup(int iCol, float v)
        {
            float[] curDataMap = dataMap[iCol];
            //The index of the specified value in the specified array, if value is found. 
            //If value is not found and value is less than one or more elements in array, 
            //a negative number which is the bitwise complement of the index of the first element 
            //that is larger than value. 
            //If value is not found and value is greater than any of the elements in array, 
            //a negative number which is the bitwise complement of (the index of the last element plus 1).

            int code = Array.BinarySearch<float>(curDataMap, v);
            if (code < 0)
            {
                code = (~code) - 1;
                code = (code < 0) ? 0 : code;
            }

            return (ushort)code;
        }

        public float ConvertToOrigData(int iCol, float x)
        {           
            int lower = (ushort)Math.Floor(x);
            int upper = (ushort)Math.Ceiling(x);

            float[] curDataMap = this.dataMap[iCol];

            if (lower < 0)
                return curDataMap[0];
            else if (upper >= curDataMap.Length)
                return curDataMap[curDataMap.Length - 1];
            else
                return (curDataMap[lower] + curDataMap[upper]) / 2;
        }

        public ushort GetCodeRange(int iCol)
        {          
            return (ushort)this.dataMap[iCol].Length;
        }

        float[][] dataMap;
    }
}
