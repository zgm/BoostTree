using System;
using System.Collections.Generic;
using System.Text;

namespace StochasticGradientBoost
{
    /// <summary>
    /// Assembly of some static methods for simple (float type) vector manipulations 
    /// </summary>
    public class Vector
    {
        public static float Dot(float[] x, float[] y)
        {
            float sum = 0;
            for (int i = 0; i < x.Length & i < y.Length; i++)
                sum += x[i] * y[i];            
            return sum;
        }

        public static float Sum(float[] x)
        {
            float sum = 0;
            for (int i = 0; i < x.Length; i++)            
                sum += x[i];            
            return sum;
        }

        public static float Mean(float[] x)
        {
            if (x.Length == 0) return 0;
            return Sum(x) / x.Length;
        }

		public static float Median(float[] x)
		{
			if (x.Length == 0) return 0;

			float[] y = new float[x.Length];
			for (int i = 0; i < y.Length; i++)
				y[i] = x[i]; 

			Array.Sort(y);
			if (y.Length % 2 == 1)
				return y[(y.Length - 1) / 2];
			else
				return (y[y.Length / 2 - 1] + y[y.Length / 2]) / 2.0F; 
		}

		public static float Max(float[] x)
		{
			if (x.Length == 0) return 0;
			float maxVal = x[0]; 
			for (int i = 1; i < x.Length; i++)
			{
				if (x[i] > maxVal)
					maxVal = x[i]; 
			}
			return maxVal; 
		}

        public static float Variance(float[] x)
        {
            if (x.Length == 0) return 0;

            float mean = Mean(x);
			float var = 0;
			for (int i = 0; i < x.Length; i++)
				var += x[i] * x[i];
			var /= x.Length;

			return var - mean * mean;
        }

        public static float[] IndexCopyTo(float[] x, long[] index)
        {
            float[] y = new float[index.Length];
            for (int i = 0; i < index.Length; i++)
            {
                y[i] = x[index[i]];
            }
            return y;
        }

        public static int[] IndexArray(int n)
        {
            int[] indexArray = new int[n];
            for (int i = 0; i < n; i++)
                indexArray[i] = i;

            return indexArray;
        }

		public static float[] OnesArray(int n)
		{
			float[] ones = new float[n];
			for (int i = 0; i < n; i++)
				ones[i] = 1;
			return ones; 
		}        

        public static float[] CumSum(float[] x)
        {
            float[] y = new float[x.Length];

            y[0] = x[0];
            for (int i = 1; i < y.Length; i++)
                y[i] = y[i - 1] + x[i];       
     
            return y;
        }
        
        public static int[] RandomSample(int n, int m, Random r)
        {
            int[] indexArray = IndexArray(n);                           
            Array.Sort(Random(n, r), indexArray);            
            Array.Resize(ref indexArray, m);
            return indexArray;
        }

        public static float[] Random(int size, Random r)
        {
            if (r == null)
            {
                r = new Random();
            }

            float[] randomArray = new float[size];
            for (int i = 0; i < size; i++)
                randomArray[i] = (float)r.NextDouble();

            return randomArray;
        }


        public static void Print(float[,] x)
        {
            for (int i = 0; i < x.GetLength(0); i++)
            {
                Console.Write(i); Console.Write('\t');
                for (int j = 0; j < x.GetLength(1) - 1; j++)
                    Console.Write(x[i, j]); Console.Write('\t');
                Console.Write(x[i, x.GetLength(1) - 1]); Console.Write('\n');
            }
        }
    }
}
