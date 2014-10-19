// Simple vector/matrix utilities.
//
//
// Chris Burges, (c) Microsoft Corporation 2005

using System;
using System.Collections;
using System.IO;
using System.Diagnostics;
using System.Runtime.Serialization.Formatters.Binary;
#if !NO_ZSTREAM
using Microsoft.TMSN.IO;
#endif

namespace Microsoft.TMSN
{

	/// <summary>
	/// All array utilities collected here.
	/// </summary>
	public class ArrayUtils
	{
		public static float[][] FloatMatrix(int i, int j)
		{
			float[][] res = new float[i][];
			for (int x = 0; x < res.Length; x++)
			{
				res[x] = new float[j];
			}
			return res;
		}
		public static double[][] DoubleMatrix(int i, int j)
		{
			double[][] res = new double[i][];
			for (int x = 0; x < res.Length; x++)
			{
				res[x] = new double[j];
			}
			return res;
		}
		public static int[][] IntMatrix(int i, int j)
		{
			int[][] res = new int[i][];
			for (int x = 0; x < res.Length; x++)
			{
				res[x] = new int[j];
			}
			return res;
		}
		public static long[][] LongMatrix(int i, int j)
		{
			long[][] res = new long[i][];
			for (int x = 0; x < res.Length; x++)
			{
				res[x] = new long[j];
			}
			return res;
		}
		public static sbyte[][] SbyteMatrix(int i, int j)
		{
			sbyte[][] res = new sbyte[i][];
			for (int x = 0; x < res.Length; x++)
			{
				res[x] = new sbyte[j];
			}
			return res;
		}
		public static string[][] StringMatrix(int i, int j)
		{
			string[][] res = new string[i][];
			for (int x = 0; x < res.Length; x++)
			{
				res[x] = new string[j];
			}
			return res;
		}

		public static void Abs(double[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = Math.Abs(vec[i]);
			}
		}

		public static void Abs(double[][] mat)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = Math.Abs(mat[i][j]);
				}
			}
		}

		public static void ApplyWhiten(double[][] data, double[] meanVec, double[] sdevVec) 
		{
			ApplyWhiten(data, 0, data.Length, meanVec, sdevVec);
		}

		public static void ApplyWhiten(double[][] data, int start, int length, double[] meanVec, double[] sdevVec)
		{
			if (data.Length == 0)  return;
			if (meanVec.Length != data[0].Length || sdevVec.Length != data[0].Length)
			{
				throw new ArgumentOutOfRangeException("mean and/or sdevVec have wrong size");
			}

			for (int j = 0; j < data[0].Length; j++)
			{
				double mean = meanVec[j];
				double sdev = sdevVec[j];
				if (sdev == 0.0)
				{
					for (int i = start; i < start+length && i < data.Length; i++)
					{
						data[i][j] = 0.0;
					}
				}
				else
				{
					sdev = 1.0 / sdev;
					for (int i = start; i < start+length && i < data.Length; i++)
					{
						data[i][j] = (data[i][j] - mean) * sdev;
					}
				}
			}
		}

		public static void ApplyWhiten(float[][] data, float[] meanVec, float[] sdevVec) 
		{
			ApplyWhiten(data, 0, data.Length, meanVec, sdevVec);
		}

		public static void ApplyWhiten(float[][] data, int start, int length, float[] meanVec, float[] sdevVec)
		{
			if (data.Length == 0)  return;
			if (meanVec.Length != data[0].Length || sdevVec.Length != data[0].Length)
			{
				throw new ArgumentOutOfRangeException("mean and/or sdevVec have wrong size");
			}

			for (int j = 0; j < data[0].Length; j++)
			{
				float mean = meanVec[j];
				float sdev = sdevVec[j];
				if (sdev == 0.0)
				{
					for (int i = start; i < start+length && i < data.Length; i++)
					{
						data[i][j] = 0.0F;
					}
				}
				else
				{
					sdev = 1.0F / sdev;
					for (int i = start; i < start+length && i < data.Length; i++)
					{
						data[i][j] = (data[i][j] - mean) * sdev;
					}
				}
			}
		}



		/// <summary>
		/// Although scaling columns is slow for jag arrays, we sometimes want to use them since row
		/// based ops are much faster.
		/// </summary>
		/// <param name="data"></param>
		/// <param name="meanVec"></param>
		/// <param name="sdevVec"></param>
		public static void ApplyWhiten(float[][] data, double[] meanVec, double[] sdevVec) 
		{
			ApplyWhiten(data, 0, data.Length, meanVec, sdevVec);
		}
		public static void ApplyWhiten(float[][] data, int start, int length, double[] meanVec, double[] sdevVec)
		{
			if (length == 0 || data.Length == 0)  return;
			if (meanVec.Length != data[0].Length || sdevVec.Length != data[0].Length)
			{
				throw new ArgumentOutOfRangeException("mean and/or sdevVec have wrong size");
			}

			bool simple = true;
			for (int j = 0; j < sdevVec.Length; j++)
			{
				if ((float)sdevVec[j] == 0.0F)
				{
					simple = false;
					break;
				}
			}

			if (simple)
			{
				float[] meanF = new float[meanVec.Length];
				for (int j = 0; j < meanF.Length; j++)  meanF[j] = (float)meanVec[j];
				float[] sdevF = new float[sdevVec.Length];
				for (int j = 0; j < sdevF.Length; j++)  sdevF[j] = (float)sdevVec[j];
				ApplyWhitenSimple(data, start, length, meanF, sdevF);
			}
			else
			{
				for (int i = start; i < start+length && i < data.Length; i++)
				{
					for (int j = 0; j < data[i].Length; j++)
					{
						if (sdevVec[j] == 0.0F)
						{
							data[i][j] = 0.0F;
						}
						else
						{
							data[i][j] = ((data[i][j] - (float)meanVec[j]) / (float)sdevVec[j]);
						}
					}
				}
			}
		}

		private static void ApplyWhitenSimple(float[][] data, int start, int length, float[] meanVec, float[] sdevVec)
		{
			for (int i = start; i < start+length && i < data.Length; i++)
			{
				for (int j = 0; j < data[i].Length; j++)
				{
					data[i][j] = ((data[i][j] - meanVec[j]) / sdevVec[j]);
				}
			}
		}



		public static void ApplyWhiten(float[] data, double[] meanVec, double[] sdevVec)
		{
			if (data.Length == 0)  return;
			if (meanVec.Length != data.Length || sdevVec.Length != data.Length)
			{
				throw new ArgumentOutOfRangeException("mean and/or sdevVec have wrong size");
			}

			for (int j = 0; j < sdevVec.Length; j++)
			{
				float sdev = (float)sdevVec[j];
				if (sdev == 0.0F)
				{
					data[j] = 0.0F;
				}
				else
				{
					float mean = (float)meanVec[j];
					sdev = 1.0F / sdev;
					data[j] = ((data[j] - mean) * sdev);
				}
			}
		}

		public static void ApplyWhiten(double[] data, double mean, double sdev)
		{
			if (data.Length == 0)  return;
			if (sdev == 0.0)
			{
				for (int i = 0; i < data.Length; i++)
				{
					data[i] = 0.0;
				}
			}
			else
			{
				sdev = 1.0 / sdev;
				for (int i = 0; i < data.Length; i++)
				{
					data[i] = (data[i] - mean) * sdev;
				}
			}
		}




		/// <summary>
		/// Debugging tools
		/// </summary>
		/// <param name="vec1"></param>
		/// <param name="vec2"></param>
		public static void Compare(bool[] vec1, bool[] vec2)
		{
			if(vec1.Length != vec2.Length)
			{
				Console.WriteLine("Compare: Lengths differ: {0} {1}", vec1.Length, vec2.Length);
			}
			else
			{
				for(int i = 0; i < vec1.Length; i++)
				{
					if(vec1[i] != vec2[i])
					{
						Console.WriteLine("Compare: vector differ: element {0} :  {1} versus {2}", i, vec1[i], vec2[i]);
					}
				}
			}
		}
		public static void Compare(int[] vec1, int[] vec2)
		{
			if(vec1.Length != vec2.Length)
			{
				Console.WriteLine("Compare: Lengths differ: {0} {1}", vec1.Length, vec2.Length);
			}
			else
			{
				for(int i = 0; i < vec1.Length; i++)
				{
					if(vec1[i] != vec2[i])
					{
						Console.WriteLine("Compare: vector differ: element {0} :  {1} versus {2}", i, vec1[i], vec2[i]);
					}
				}
			}
		}
		public static void Compare(float[] vec1, float[] vec2)
		{
			if(vec1.Length != vec2.Length)
			{
				Console.WriteLine("Compare: Lengths differ: {0} {1}", vec1.Length, vec2.Length);
			}
			else
			{
				for(int i = 0; i < vec1.Length; i++)
				{
					if(vec1[i] != vec2[i])
					{
						Console.WriteLine("Compare: vector differ: element {0} :  {1} versus {2}", i, vec1[i], vec2[i]);
					}
				}
			}
		}
		public static void Compare(double[] vec1, double[] vec2)
		{
			if(vec1.Length != vec2.Length)
			{
				Console.WriteLine("Compare: Lengths differ: {0} {1}", vec1.Length, vec2.Length);
			}
			else
			{
				for(int i = 0; i < vec1.Length; i++)
				{
					if(vec1[i] != vec2[i])
					{
						Console.WriteLine("Compare: vector differ: element {0} :  {1} versus {2}", i, vec1[i], vec2[i]);
					}
				}
			}
		}

		public static void Compare(float[][] jag1, float[][] jag2)
		{
			if(jag1.Length != jag2.Length)
			{
				Console.WriteLine("Jag Compare: Lengths differ: {0} {1}", jag1.Length, jag2.Length);
			}
			else
			{
				for(int i = 0; i < jag1.Length; i++)
				{
					if(jag1[i] != null && jag2[i] != null)
					{
						Compare(jag1[i], jag2[i]);
					}
					else if((jag1[i] != null && jag2[i] == null) || (jag1[i] == null && jag2[i] != null))
					{
						Console.WriteLine("Compare: row {0}: One jag has non null element where other does not", i);
					}
				}
			}
		}

		public static void CompMeansNSdevs(float[][] data, double[] meanVec, double[] sdevVec)
		{
			int i, j, nRows = data.Length, nCols = data[0].Length;
			if (meanVec.Length != nCols || sdevVec.Length != nCols)
			{
				throw(new ArgumentOutOfRangeException("mean and/or sdev have wrong size"));
			}

			double tmp, mean, sum, sumsq, recipSize=1.0/nRows;
			for (j=0; j<nCols; j++)
			{
				sum = 0.0;
				sumsq = 0.0;
				for (i=0; i<nRows; i++)
				{
					tmp = data[i][j]; // OK, using jags is counterproductive here, but the gain elsewhere is large
					sum += tmp;
					sumsq += tmp*tmp;
				}
				mean = recipSize * sum;
				meanVec[j] = mean;
				// Over very large datasets (where e.g. all elements the same except for one), this can become negative due to
				// roundoff error.  Prevent this.
				tmp = recipSize*sumsq - mean*mean;
				if (tmp <= 0.0)
				{
					sdevVec[j]=0.0;
				}
				else
				{
					sdevVec[j] = Math.Sqrt(tmp);
				}
			}
		}

		public static void CompMeanNSdev(double[] data, out double mean, out double sdev)
		{
			double sum = 0.0;
			double sumsq = 0.0;
			for(int i = 0; i < data.Length; i++)
			{
				double tmp = data[i];
				sum += tmp;
				sumsq += tmp * tmp;
			}
			double recipSize = 1.0 / data.Length;
			mean = recipSize * sum;
			double tmp2 = recipSize * sumsq - mean * mean;
			if(tmp2 <= 0.0)
			{
				sdev = 0.0;
			}
			else
			{
				sdev = Math.Sqrt(tmp2);
			}
		}

		public static void CompMeanNSdevMinMax(double[] data, out double mean, out double sdev, out double minVal, out double maxVal)
		{
			minVal = data[0];
			maxVal = data[0];
			double sum = 0.0;
			double sumsq = 0.0;
			for(int i = 0; i < data.Length; i++)
			{
				double tmp = data[i];
				sum += tmp;
				sumsq += tmp * tmp;
				if(tmp > maxVal)
					maxVal = tmp;
				else if(tmp < minVal)
					minVal = tmp;
			}
			double recipSize = 1.0 / data.Length;
			mean = recipSize * sum;
			double tmp2 = recipSize * sumsq - mean * mean;
			if(tmp2 <= 0.0)
			{
				sdev = 0.0;
			}
			else
			{
				sdev = Math.Sqrt(tmp2);
			}
		}


		/// <summary>
		/// Hard copies.
		/// </summary>
		/// <param name="vec"></param>
		/// <returns></returns>
		public static bool[] Copy(bool[] vec)
		{
			return (bool[])vec.Clone();
		}
		public static sbyte[] Copy(sbyte[] vec)
		{
			return (sbyte[])vec.Clone();
		}
		public static int[] Copy(int[] vec)
		{
			return (int[])vec.Clone();
		}
		public static float[] Copy(float[] vec)
		{
			return (float[])vec.Clone();
		}
		public static double[] Copy(double[] vec)
		{
			return (double[])vec.Clone();
		}
		public static float[][] Copy(float[][] jag)
		{
			float[][] newJag = new float[jag.Length][];
			for(int i = 0; i < jag.Length; i++)
			{
				if(jag[i] != null)
				{
					newJag[i] = Copy(jag[i]);
				}
			}
			return newJag;
		}


		public static double Dot(float[] a, float[] b)
		{
			Debug.Assert(a.Length == b.Length);

			double ans=0.0;
			for (int i = 0; i < a.Length; i++)
			{
				ans += a[i]*b[i];
			}
			return ans;
		}
		public static double Dot(float[] a, double[] b)
		{
			Debug.Assert(a.Length == b.Length);

			double ans=0.0;
			for (int i = 0; i < a.Length; i++)
			{
				ans += a[i]*b[i];
			}
			return ans;
		}
		public static double Dot(double[] a, double[] b)
		{
			Debug.Assert(a.Length == b.Length);

			double ans=0.0;
			for (int i = 0; i < a.Length; i++)
			{
				ans += a[i]*b[i];
			}
			return ans;
		}
		public static double Dot(float[][] data, int idx, float[] vec)
		{
			Debug.Assert(data[0].Length == vec.Length);

			double ans = 0.0;
			for (int i = 0; i < vec.Length; i++)
			{
				ans += vec[i] * data[idx][i];
			}
			return ans;
		}


		// Compute Euclidean distance for every pair of rows
		public static double[][] DistMat(double[][] mat)
		{
			int nRow = mat.Length, nCol = mat[0].Length;
			double[][] ans = new double[nRow][];
			for (int i = 0; i < ans.Length; i++)  ans[i] = new double[nRow];
			double tmp, tmp2, tmp3;

			for (int i = 0; i < nRow-1; i++)
			{
				for (int j = i+1; j < nRow; j++)
				{
					tmp2 = 0.0;
					for (int k = 0; k < nCol; k++)
					{
						tmp = mat[i][k] - mat[j][k];
						tmp2 += tmp*tmp;
					}
					tmp3 = Math.Sqrt(tmp2);
					ans[i][j] = tmp3;
					ans[j][i] = tmp3;
				}
			}

			return ans;
		}

		/// <summary>
		/// Replace each col with zero mean, unit sdev.
		/// If for a given column sdev=0, the feature is mapped to zero.
		/// </summary>
		/// <param name="data"></param>
		/// <param name="meanVec"></param>
		/// <param name="recipSdevVec"></param>
		public static void FastWhiten(double[][] data)
		{
			int i, j, nRows = data.Length, nCols = data[0].Length;
			double tmp, mean, sdev, recipSdev, sum, sumsq, recipSize=1.0/nRows;
			for (j=0; j<nCols; j++)
			{
				sum = 0.0;
				sumsq = 0.0;
				for (i=0; i<nRows; i++)
				{
					tmp = data[i][j];
					sum += tmp;
					sumsq += tmp*tmp;
				}
				mean = recipSize * sum;
				double squeak = recipSize*sumsq - mean*mean; // Avoid numerical problems
				if(squeak <= 0) sdev = 0.0;
				else sdev = Math.Sqrt(squeak);
				if (sdev==0)
				{
					for (i=0; i<nRows; i++)
					{
						data[i][j] = 0.0;
					}
				}
				else
				{
					for (i=0; i<nRows; i++)
					{
						recipSdev = 1.0/sdev;
						data[i][j] = (data[i][j] - mean)*recipSdev;
					}
				}
			}
		}

		public static void FastWhiten(float[][] data)
		{
			int i, j, nRows = data.Length, nCols = data[0].Length;
			float tmp;
			double mean, sdev, recipSdev, sum, sumsq, recipSize=1.0/nRows;
			for (j=0; j<nCols; j++)
			{
				sum = 0.0;
				sumsq = 0.0;
				for (i=0; i<nRows; i++)
				{
					tmp = data[i][j];
					sum += tmp;
					sumsq += tmp*tmp;
				}
				mean = recipSize * sum;
				double squeak = recipSize*sumsq - mean*mean; // Avoid numerical problems
				if(squeak <= 0) sdev = 0.0;
				else sdev = Math.Sqrt(squeak);
				if (sdev==0)
				{
					for (i=0; i<nRows; i++)
					{
						data[i][j] = 0.0F;
					}
				}
				else
				{
					for (i=0; i<nRows; i++)
					{
						recipSdev = 1.0/sdev;
						data[i][j] = (float)((data[i][j] - mean)*recipSdev);
					}
				}
			}
		}

		/// <summary>
		/// Replace each col with zero mean, unit sdev.  Return the column mean and sdev.
		/// If for a given column sdev=0, the feature is mapped to zero.
		/// </summary>
		/// <param name="data"></param>
		/// <param name="meanVec"></param>
		/// <param name="recipSdevVec"></param>
		public static void FastWhiten(double[][] data, double[] meanVec, double[] sdevVec)
		{
			int i, j, nRows = data.Length, nCols = data[0].Length;
			if (meanVec.Length != nCols || sdevVec.Length != nCols)
			{
				throw(new ArgumentOutOfRangeException("mean and/or sdev have wrong size"));
			}

			double tmp, mean, sdev, sum, sumsq, recipSdev, recipSize=1.0/nRows;
			for (j=0; j<nCols; j++)
			{
				sum = 0.0;
				sumsq = 0.0;
				for (i=0; i<nRows; i++)
				{
					tmp = data[i][j];
					sum += tmp;
					sumsq += tmp*tmp;
				}
				mean = recipSize * sum;
				sdev = Math.Sqrt(recipSize*sumsq - mean*mean);
				if (sdev==0)
				{
					for (i=0; i<nRows; i++)
					{
						data[i][j] = 0.0;
					}
				}
				else
				{
					recipSdev = 1.0/sdev;
					for (i=0; i<nRows; i++)
					{
						data[i][j] = (data[i][j] - mean)*recipSdev;
					}
				}
				meanVec[j] = mean;
				sdevVec[j] = sdev;
			}
		}

		/// <summary>
		/// Replace each col with zero mean, unit sdev.  Return the column mean and sdev.
		/// If for a given column sdev=0, the feature is mapped to zero.
		/// </summary>
		/// <param name="data"></param>
		/// <param name="meanVec"></param>
		/// <param name="recipSdevVec"></param>
		public static void FastWhiten(float[][] data, double[] meanVec, double[] sdevVec)
		{
			int i, j, nRows = data.Length, nCols = data[0].Length;
			if (meanVec.Length != nCols || sdevVec.Length != nCols)
			{
				throw(new ArgumentOutOfRangeException("mean and/or sdev have wrong size"));
			}

			double tmp, mean, sdev, sum, sumsq, recipSdev, recipSize=1.0/nRows;
			for (j=0; j<nCols; j++)
			{
				sum = 0.0;
				sumsq = 0.0;
				for (i=0; i<nRows; i++)
				{
					tmp = data[i][j];
					sum += tmp;
					sumsq += tmp*tmp;
				}
				mean = recipSize * sum;
				sdev = Math.Sqrt(recipSize*sumsq - mean*mean);
				if (sdev==0)
				{
					for (i=0; i<nRows; i++)
					{
						data[i][j] = 0.0F;
					}
				}
				else
				{
					recipSdev = 1.0/sdev;
					for (i=0; i<nRows; i++)
					{
						data[i][j] = (float)((data[i][j] - mean)*recipSdev);
					}
				}
				meanVec[j] = mean;
				sdevVec[j] = sdev;
			}
		}



		// Robert's example of how to avoid generics
		//		public static void Fill(Array vec, object val)
		//		{
		//			for (int i = 0; i < vec.Length; i++) { vec.SetValue(val; }
		//		}
		public static void Fill(sbyte[] vec, sbyte val)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = val;
			}
		}
		public static void Fill(int[] vec, int val)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = val;
			}
		}
		public static void Fill(float[] vec, float val)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = val;
			}
		}
		public static void Fill(double[] vec, double val)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = val;
			}
		}
		public static void Fill(bool[] vec, bool val)
		{
			if (val)
			{
				for (int i = 0; i < vec.Length; i++) { vec[i] = true; }
			}
			else
			{
				Array.Clear(vec, 0, vec.Length);
			}
		}

		public static void Fill(double[][] mat, double val)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = val;
				}
			}
		}

		public static void Fill(sbyte[][] mat, sbyte val)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = val;
				}
			}
		}


		/// <summary>
		/// Compute a histogram.
		/// </summary>
		/// <param name="vecIn"></param>
		/// <returns>The bins.  First col: value at center of bin, OR at the actual value, if the bin contains
		/// only one value.  Second col: num values in bin to the
		/// left of that.</returns>
		public static float[][] Hist(float[] vec, int nBins)
		{
			if (nBins < 1)
			{
				throw(new ArgumentOutOfRangeException("nBins must be > 0"));
			}

			int i, j, nRows = vec.Length;
			float minVal, maxVal;
			float[][] ans;
			MinMax(vec, out minVal, out maxVal);
			double step = ((double)maxVal - (double)minVal)/(double)nBins;
			if (step == 0.0)
			{
				ans = FloatMatrix(1,2);
				ans[0][0] = minVal;
				ans[0][1] = (float)nRows;
			}
			else
			{

				// Probably faster ways to compute this - but this one is really safe.  Use vals and diffVals to
				// keep track of whether only one value winds up in that bin, for display purposes later (note it only
				// contains an upper bound on the number of different values that wind up in that bin, since the values
				// are not sorted).
				ans = FloatMatrix(nBins, 2);
				double pos = minVal + step;
				double[] vals = new double[nBins];
				ArrayUtils.Fill(vals, minVal-1.0F);
				double[] diffVals = new double[nBins];
				for (i=0; i<nBins-1; i++) // Last bin treated separately to avoid numerical problems
				{
					ans[i][0] = (float)pos;
					pos += step;
				}
				ans[nBins-1][0] = maxVal;

				float val;
				for (i=0; i<nRows; i++)
				{
					val = vec[i];
					for (j=0; j<nBins; j++)
					{
						if (val <= ans[j][0])
						{
							ans[j][1] = ans[j][1]+1;
							if (vals[j] != val)
							{
								vals[j] = val;
								diffVals[j] = diffVals[j]+1;
							}
							break;
						}
					}
				}


				if ((int)SumSlice(ans, 1, 1) != nRows)
				{
					throw(new Exception("Hist failed: summed bins != num elements"));
				}

				// For display, center each bin
				double halfStep = step*0.5;
				for (i=0; i<nBins; i++) // Last bin treated separately to avoid numerical problems
				{
					if (diffVals[i]==1) // Only one value in the bin, so center over that exact value
					{
						ans[i][0] = (float)vals[i];
					}
					else
					{
						ans[i][0] = ans[i][0]-(float)halfStep;
					}
				}
			}

			return ans;
		}



		private static bool CheckForBinaryFile(ref string fname)
		{
#if DEBUG_LOAD
			try
			{
#endif
			// This code tries to respond to updates to the source files.
			// That might not be best if they are commonly copied around...
			DateTime origTime;
			if (File.Exists(fname))
			{
				origTime = File.GetLastWriteTime(fname);
			}
			else
			{
				origTime = DateTime.MinValue;
			}
			if (File.Exists(fname + ".dat"))
			{
				if (File.GetLastWriteTime(fname + ".dat") >= origTime)
				{
					fname = fname + ".dat";
					return true;
				}
			}
#if !NO_ZSTREAM
			if (File.Exists(fname + ".dat.gz"))
			{
				if (File.GetLastWriteTime(fname + ".dat.gz") >= origTime)
				{
					fname = fname + ".dat.gz";
					return true;
				}
			}
			if (File.Exists(fname + ".dat.7z"))
			{
				if (File.GetLastWriteTime(fname + ".dat.7z") >= origTime)
				{
					fname = fname + ".dat.7z";
					return true;
				}
			}
#endif
			return false;
#if DEBUG_LOAD
			}
			finally
			{
				Console.Error.WriteLine("Loading: " + fname);
			}
#endif
		}




		#region Load Vector

		public static bool[] LoadBoolVec(string fname)
		{
			string[] strArr = IOUtils.ReadWholeRows(fname);
			bool[] vec = new bool[strArr.Length];
			for (int i = 0; i < strArr.Length; i++)
			{
				vec[i] = bool.Parse(strArr[i]);
			}
			return vec;
		}

		public static BitArray LoadBitArray(string fname)
		{
			return LoadBitArray(fname, true);
		}
		public static BitArray LoadBitArray(string fname, bool autoBinary)
		{
			autoBinary = autoBinary && !CheckForBinaryFile(ref fname);

			int[] sizes = IOUtils.CountRows(fname);
			bool binary = sizes.Length > 1;
			BitArray vec;

			if (binary)
			{
#if NO_ZSTREAM
				using (Stream s = new FileStream(fname, FileMode.Open))
#else
				using (Stream s = ZStreamIn.Open(fname))
#endif
				{
					while (s.ReadByte() != (byte)'\n') ;
					BinaryReader br = new BinaryReader(s);
					vec = LoadBitArray(br);
				}
			}
			else
			{
				vec = new BitArray(sizes[0]);
#if NO_ZSTREAM
				using (StreamReader sr = new StreamReader(fname))
#else
				using (StreamReader sr = ZStreamReader.Open(fname))
#endif
				{
					for (int i = 0; i < vec.Length; i++)
					{
						vec[i] = bool.Parse(sr.ReadLine());
					}
				}
				if (autoBinary)
				{
#if NO_ZSTREAM
					using (Stream s = new FileStream(fname + ".dat", FileMode.Create))
#else
					using (Stream s = ZStreamOut.Open(fname + ".dat"))
#endif
					{
						using (BinaryWriter bw = new BinaryWriter(s))
						{
							bw.Write(System.Text.Encoding.ASCII.GetBytes("@ " + vec.Length + " b\n"));
							Save(vec, bw);
							bw.Flush();
						}

						//s.Flush();
					}
				}
			}
			return vec;
		}

		public static int[] LoadIntVec(string fname)
		{
			return LoadIntVec(fname, true);
		}
		public static int[] LoadIntVec(string fname, bool autoBinary)
		{
			autoBinary = autoBinary && !CheckForBinaryFile(ref fname);

			int[] sizes = IOUtils.CountRows(fname);
			bool binary = sizes.Length > 1;
			int[] vec = new int[sizes[0]];

			if (binary)
			{
#if NO_ZSTREAM
				using (Stream s = new FileStream(fname, FileMode.Open))
#else
				using (Stream s = ZStreamIn.Open(fname))
#endif
				{
					//BinaryReader br = new BinaryReader(s);
					//while (br.ReadByte() != (byte)'\n') ;
					//for (int i = 0; i < vec.Length; i++)
					//{
					//	vec[i] = br.ReadInt32();
					//}
					byte[] row = new byte[vec.Length << 2];
					while (s.ReadByte() != (byte)'\n') ;
					s.Read(row, 0, row.Length);
					Buffer.BlockCopy(row, 0, vec, 0, row.Length);
				}
			}
			else
			{
#if NO_ZSTREAM
				using (StreamReader sr = new StreamReader(fname))
#else
				using (StreamReader sr = ZStreamReader.Open(fname))
#endif
				{
					for (int i = 0; i < vec.Length; i++)
					{
						vec[i] = int.Parse(sr.ReadLine());
					}
				}
				if (autoBinary)
				{
#if NO_ZSTREAM
					using (Stream s = new FileStream(fname + ".dat", FileMode.Create))
#else
					using (Stream s = ZStreamOut.Open(fname + ".dat"))
#endif
					{
						//						using (BinaryWriter bw = new BinaryWriter(s))
						//						{
						//							bw.Write(System.Text.Encoding.ASCII.GetBytes("@ " + vec.Length + " b\n"));
						//							//						for (int i = 0; i < vec.Length; i++)
						//							//						{
						//							//							bw.Write(vec[i]);
						//							//						}
						//							byte[] row = new byte[vec.Length << 2];
						//							Buffer.BlockCopy(vec, 0, row, 0, row.Length);
						//							bw.Write(row);
						//							bw.Flush();
						//						}

						byte[] header = System.Text.Encoding.ASCII.GetBytes("@ " + vec.Length + " b\n");
						s.Write(header, 0, header.Length);

						byte[] row = new byte[vec.Length << 2];
						Buffer.BlockCopy(vec, 0, row, 0, row.Length);
						s.Write(row, 0, row.Length);

						s.Flush();
					}
				}
			}
			return vec;
		}

		
		public static float[] LoadFloatVec(string fname)
		{
			return LoadFloatVec(fname, true);
		}
		public static float[] LoadFloatVec(string fname, bool autoBinary)
		{
			autoBinary = autoBinary && !CheckForBinaryFile(ref fname);

			int[] sizes = IOUtils.CountRows(fname);
			bool binary = sizes.Length > 1;
			float[] vec = new float[sizes[0]];

			if (binary)
			{
#if NO_ZSTREAM
				using (Stream s = new FileStream(fname, FileMode.Open))
#else
				using (Stream s = ZStreamIn.Open(fname))
#endif
				{
					//					BinaryReader br = new BinaryReader(s);
					//					while (br.ReadByte() != (byte)'\n') ;
					//					for (int i = 0; i < vec.Length; i++)
					//					{
					//						vec[i] = br.ReadSingle();
					//					}
					byte[] singleRow = new byte[vec.Length << 2];
					while (s.ReadByte() != (byte)'\n') ;
					s.Read(singleRow, 0, singleRow.Length);
					Buffer.BlockCopy(singleRow, 0, vec, 0, singleRow.Length);
				}
			}
			else
			{
#if NO_ZSTREAM
				using (StreamReader sr = new StreamReader(fname))
#else
				using (StreamReader sr = ZStreamReader.Open(fname))
#endif
				{
					for (int i = 0; i < vec.Length; i++)
					{
						vec[i] = float.Parse(sr.ReadLine());
					}
				}
				if (autoBinary)
				{
#if NO_ZSTREAM
					using (Stream s = new FileStream(fname + ".dat", FileMode.Create))
#else
					using (Stream s = ZStreamOut.Open(fname + ".dat"))
#endif
					{
						//						using (BinaryWriter bw = new BinaryWriter(s))
						//						{
						//							bw.Write(System.Text.Encoding.ASCII.GetBytes("@ " + vec.Length + " b\n"));
						//							//						for (int i = 0; i < vec.Length; i++)
						//							//						{
						//							//							bw.Write(vec[i]);
						//							//						}
						//							byte[] row = new byte[vec.Length << 2];
						//							Buffer.BlockCopy(vec, 0, row, 0, row.Length);
						//							bw.Write(row);
						//							bw.Flush();
						//						}

						byte[] header = System.Text.Encoding.ASCII.GetBytes("@ " + vec.Length + " b\n");
						s.Write(header, 0, header.Length);

						byte[] row = new byte[vec.Length << 2];
						Buffer.BlockCopy(vec, 0, row, 0, row.Length);
						s.Write(row, 0, row.Length);

						s.Flush();
					}
				}
			}
			return vec;
		}

		public static double[] LoadDoubleVec(string fname)
		{
			string[] strArr = IOUtils.ReadWholeRows(fname);
			int i, size = strArr.Length;
			double[] vec = new double[size];
			for (i=0; i<size; i++)
			{
				vec[i] = double.Parse(strArr[i]);
			}
			return vec;
		}

		public static double[] LoadDoubleVecSerial(string fname)
		{
			double[] vec;
#if NO_ZSTREAM
			using (Stream fs = new FileStream(fname, FileMode.Open))
#else
			using (Stream fs = ZStreamIn.Open(fname))
#endif
			{
				BinaryFormatter deserializer = new BinaryFormatter();
				vec = (double[])(deserializer.Deserialize(fs));
			}
			return vec;
		}

		#endregion


		#region Load Matrix

		public static sbyte[][] LoadSbyteMat(string fname)
		{
			return LoadSbyteMat(fname, true);
		}
		public static sbyte[][] LoadSbyteMat(string fname, bool autoBinary)
		{
			autoBinary = autoBinary && !CheckForBinaryFile(ref fname);

			int[] sizes = IOUtils.CountRowsNCols(fname);
			bool binary = sizes.Length > 2;
			bool compact = sizes.Length > 2 && (sizes[2] == 1);

			if (binary)
			{
				sbyte[][] mat = SbyteMatrix(sizes[0], sizes[1]);
#if NO_ZSTREAM
				using (Stream s = new FileStream(fname, FileMode.Open))
#else
				using (Stream s = ZStreamIn.Open(fname))
#endif
				{
					//					BinaryReader br = new BinaryReader(s);
					//					while (br.ReadByte() != (byte)'\n') ;
					//					for (int i = 0; i < mat.Length; i++)
					//					{
					//						if (compact)
					//						{
					//							int maskLength = (int)Math.Ceiling(mat[i].Length / 8.0);
					//							BitArray zeroMask = new BitArray(br.ReadBytes(maskLength));
					//							for (int j = 0; j < mat[i].Length; j++)
					//							{
					//								if (zeroMask[j])
					//								{
					//									mat[i][j] = br.ReadSByte();
					//								}
					//								else
					//								{
					//									mat[i][j] = (sbyte)0;
					//								}
					//							}
					//						}
					//						else
					//						{
					//							for (int j = 0; j < mat[i].Length; j++)
					//							{
					//								mat[i][j] = br.ReadSByte();
					//							}
					//						}
					//					}
					while (s.ReadByte() != (byte)'\n') ;

					if (compact)
					{
						byte[] buf = new byte[(int)Math.Ceiling(mat[0].Length / 8.0)];
						for (int i = 0; i < mat.Length; i++)
						{
							s.Read(buf, 0, buf.Length);
							BitArray zeroMask = new BitArray(buf);
							for (int j = 0; j < mat[i].Length; j++)
							{
								if (zeroMask[j])
								{
									mat[i][j] = (sbyte)s.ReadByte();
								}
								else
								{
									mat[i][j] = 0;
								}
							}
						}
					}
					else
					{
						byte[] integerRow = new byte[mat[0].Length];
						for (int i = 0; i < mat.Length; i++)
						{
							s.Read(integerRow, 0, integerRow.Length);
							//for (int j = 0; j < mat[i].Length; j++)
							//{
							//	mat[i][j] = (sbyte)integerRow[j];
							//}
							Buffer.BlockCopy(integerRow, 0, mat[i], 0, integerRow.Length);
						}
					}
				}
				return mat;
			}
			else
			{
				sbyte[][] mat = new sbyte[sizes[0]][];
				bool allSameLength = true;
#if NO_ZSTREAM
				using (StreamReader sr = new StreamReader(fname))
#else
				using (StreamReader sr = ZStreamReader.Open(fname))
#endif
				{
					for (int i = 0; i < mat.Length; i++)
					{
						string[] fields = sr.ReadLine().Split();
						mat[i] = new sbyte[fields.Length];
						for (int j = 0; j < mat[i].Length; j++)
						{
							mat[i][j] = sbyte.Parse(fields[j]);
						}
						if (allSameLength && i != 0 && mat[i].Length != mat[i-1].Length)  allSameLength = false;
					}
				}
				if (autoBinary && allSameLength)
				{
#if NO_ZSTREAM
					using (Stream s = new FileStream(fname + ".dat", FileMode.Create))
#else
					using (Stream s = ZStreamOut.Open(fname + ".dat"))
#endif
					{
						//						using (BinaryWriter bw = new BinaryWriter(s))
						//						{
						//							bw.Write(System.Text.Encoding.ASCII.GetBytes("@ " + mat.Length + " " + mat[0].Length + " b c\n"));
						//							int maskLength = (int)Math.Ceiling(mat[0].Length / 8.0);
						//							for (int i = 0; i < mat.Length; i++)
						//							{
						//								byte[] mask = new byte[maskLength];
						//								BitArray zeroMask = new BitArray(mask.Length * 8);
						//								for (int j = 0; j < mat[i].Length; j++)
						//								{
						//									zeroMask[j] = (mat[i][j] != 0.0F);
						//								}
						//								zeroMask.CopyTo(mask, 0);
						//								bw.Write(mask);
						//								for (int j = 0; j < mat[i].Length; j++)
						//								{
						//									if (zeroMask[j])
						//									{
						//										bw.Write(mat[i][j]);
						//									}
						//								}
						//							}
						//							//// non-compact:
						//							//bw.Write(System.Text.Encoding.ASCII.GetBytes("@ " + mat.Length + " " + mat[0].Length + " b\n"));
						//							//for (int i = 0; i < mat.Length; i++)
						//							//{
						//							//	for (int j = 0; j < mat[i].Length; j++)
						//							//	{
						//							//		bw.Write(mat[i][j]);
						//							//	}
						//							//}
						//							bw.Flush();
						//						}

						byte[] header = System.Text.Encoding.ASCII.GetBytes("@ " + mat.Length + " " + mat[0].Length + " b c\n");
						s.Write(header, 0, header.Length);

						int maskLength = (int)Math.Ceiling(mat[0].Length / 8.0);
						byte[] mask = new byte[maskLength];
						for (int i = 0; i < mat.Length; i++)
						{
							BitArray zeroMask = new BitArray(mask.Length * 8);
							for (int j = 0; j < mat[i].Length; j++)
							{
								zeroMask[j] = (mat[i][j] != 0.0F);
							}
							zeroMask.CopyTo(mask, 0);
							s.Write(mask, 0, mask.Length);

							for (int j = 0; j < mat[i].Length; j++)
							{
								if (zeroMask[j])
								{
									s.WriteByte((byte)mat[i][j]);
								}
							}
						}

						s.Flush();
					}
				}
				return mat;
			}
		}



		public static int[][] LoadIntMat(string fname)
		{
			return LoadIntMat(fname, true);
		}
		/// <summary>
		/// Assumes same number of cols for every row.
		/// </summary>
		/// <param name="fname"></param>
		/// <returns></returns>
		public static int[][] LoadIntMat(string fname, bool autoBinary)
		{
			autoBinary = autoBinary && !CheckForBinaryFile(ref fname);

			int[] sizes = IOUtils.CountRowsNCols(fname);
			bool binary = sizes.Length > 2;
			bool compact = sizes.Length > 3;

			if (binary)
			{
				int[][] mat = IntMatrix(sizes[0], sizes[1]);
#if NO_ZSTREAM
				using (Stream s = new FileStream(fname, FileMode.Open))
#else
				using (Stream s = ZStreamIn.Open(fname))
#endif
				{
					//					BinaryReader br = new BinaryReader(s);
					//					while (br.ReadByte() != (byte)'\n') ;
					//					for (int i = 0; i < mat.Length; i++)
					//					{
					//						for (int j = 0; j < mat[i].Length; j++)
					//						{
					//							mat[i][j] = br.ReadInt32();
					//						}
					//					}
					while (s.ReadByte() != (byte)'\n') ;

					if (compact)
					{
						byte[] integer = new byte[4];
						byte[] buf = new byte[(int)Math.Ceiling(mat[0].Length / 8.0)];
						for (int i = 0; i < mat.Length; i++)
						{
							s.Read(buf, 0, buf.Length);
							BitArray zeroMask = new BitArray(buf);
							for (int j = 0; j < mat[i].Length; j++)
							{
								if (zeroMask[j])
								{
									s.Read(integer, 0, 4);
									mat[i][j] = BitConverter.ToInt32(integer, 0);
								}
								else
								{
									mat[i][j] = 0;
								}
							}
						}
					}
					else
					{
						byte[] integerRow = new byte[mat[0].Length << 2];
						for (int i = 0; i < mat.Length; i++)
						{
							s.Read(integerRow, 0, integerRow.Length);
							//for (int j = 0; j < mat[i].Length; j++)
							//{
							//	mat[i][j] = BitConverter.ToInt32(integerRow, j << 2);
							//}
							Buffer.BlockCopy(integerRow, 0, mat[i], 0, integerRow.Length);
						}
					}
				}
				//WriteIntMat(mat, fname + ".binary_test.gz");
				return mat;
			}
			else
			{
				int[][] mat = new int[sizes[0]][];
				bool allSameLength = true;
#if NO_ZSTREAM
				using (StreamReader sr = new StreamReader(fname))
#else
				using (StreamReader sr = ZStreamReader.Open(fname))
#endif
				{
					for (int i = 0; i < mat.Length; i++)
					{
						string[] fields = sr.ReadLine().Split();
						mat[i] = new int[fields.Length];
						for (int j = 0; j < mat[i].Length; j++)
						{
							mat[i][j] = int.Parse(fields[j]);
						}
						if (allSameLength && i != 0 && mat[i].Length != mat[i-1].Length)  allSameLength = false;
					}
				}
				if (autoBinary && allSameLength)
				{
#if NO_ZSTREAM
					using (Stream s = new FileStream(fname + ".dat", FileMode.Create))
#else
					using (Stream s = ZStreamOut.Open(fname + ".dat"))
#endif
					{
						//using (StreamWriter sb = ZStreamWriter.Open(fname + ".dat.bytes"))
						//{

						//						BinaryWriter bw = new BinaryWriter(s);
						//						bw.Write(System.Text.Encoding.ASCII.GetBytes("@ " + mat.Length + " " + mat[0].Length + " b c\n"));
						//						int maskLength = (int)Math.Ceiling(mat[0].Length / 8.0);
						//						for (int i = 0; i < mat.Length; i++)
						//						{
						//							byte[] mask = new byte[maskLength];
						//							BitArray zeroMask = new BitArray(mask.Length * 8);
						//							for (int j = 0; j < mat[i].Length; j++)
						//							{
						//								zeroMask[j] = (mat[i][j] != 0.0F);
						//							}
						//							zeroMask.CopyTo(mask, 0);
						//							bw.Write(mask);
						//							for (int j = 0; j < mat[i].Length; j++)
						//							{
						//								if (zeroMask[j])
						//								{
						//									bw.Write(mat[i][j]);
						//								}
						//							}
						//						}

						//// non-compact:
						//						using (BinaryWriter bw = new BinaryWriter(s))
						//						{
						//							bw.Write(System.Text.Encoding.ASCII.GetBytes("@ " + mat.Length + " " + mat[0].Length + " b\n"));
						//							//sb.WriteLine("@ " + mat.Length + " " + mat[0].Length + " b");
						//							for (int i = 0; i < mat.Length; i++)
						//							{
						//								for (int j = 0; j < mat[i].Length; j++)
						//								{
						//									bw.Write(mat[i][j]);
						//									//byte[] bb = BitConverter.GetBytes(mat[i][j]);
						//									//for (int b = 0; b < bb.Length; b++)
						//									//{
						//									//	if (j != 0 || b != 0)  sb.Write('\t');
						//									//	sb.Write(bb[b]);
						//									//}
						//								}
						//								//sb.WriteLine();
						//							}
						//							bw.Flush();
						//						}

						byte[] header = System.Text.Encoding.ASCII.GetBytes("@ " + mat.Length + " " + mat[0].Length + " b\n");
						s.Write(header, 0, header.Length);

						byte[] integerRow = new byte[mat[0].Length << 2];
						for (int i = 0; i < mat.Length; i++)
						{
							Buffer.BlockCopy(mat[i], 0, integerRow, 0, integerRow.Length);
							s.Write(integerRow, 0, integerRow.Length);
						}

						s.Flush();
					}
					//					}
				}
				//WriteIntMat(mat, fname + ".raw_test");
				return mat;
			}
		}

		public static void WriteIntMat(int[][] mat, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter s = new StreamWriter(fname, FileMode.Create))
#else
			using (StreamWriter s = ZStreamWriter.Open(fname))
#endif
			{
				for (int i = 0; i < mat.Length; i++)
				{
					for (int j = 0; j < mat[i].Length; j++)
					{
						if (j != 0)  s.Write('\t');
						s.Write(mat[i][j]);
					}
					s.WriteLine();
				}
			}
		}



		public static float[][] LoadFloatMat(string fname)
		{
			return LoadFloatMat(fname, true);
		}
		/// <summary>
		/// Assumes same number of cols for every row.
		/// </summary>
		/// <param name="fname"></param>
		/// <returns></returns>
		public static float[][] LoadFloatMat(string fname, bool autoBinary)
		{
			autoBinary = autoBinary && !CheckForBinaryFile(ref fname);

			int[] sizes = IOUtils.CountRowsNCols(fname);
			bool binary = sizes.Length > 2;
			bool compact = sizes.Length > 2 && (sizes[2] == 1);
			if (binary)
			{
				float[][] mat = FloatMatrix(sizes[0], sizes[1]);
#if NO_ZSTREAM
				using (Stream s = new FileStream(fname, FileMode.Open))
#else
				using (Stream s = ZStreamIn.Open(fname))
#endif
				{
					//					BinaryReader br = new BinaryReader(s);
					//					while (br.ReadByte() != (byte)'\n') ;
					//					for (int i = 0; i < mat.Length; i++)
					//					{
					//						if (compact)
					//						{
					//							int maskLength = (int)Math.Ceiling(mat[i].Length / 8.0);
					//							BitArray zeroMask = new BitArray(br.ReadBytes(maskLength));
					//							for (int j = 0; j < mat[i].Length; j++)
					//							{
					//								if (zeroMask[j])
					//								{
					//									mat[i][j] = br.ReadSingle();
					//								}
					//								else
					//								{
					//									mat[i][j] = 0.0F;
					//								}
					//							}
					//						}
					//						else
					//						{
					//							for (int j = 0; j < mat[i].Length; j++)
					//							{
					//								mat[i][j] = br.ReadSingle();
					//							}
					//						}
					//					}
					while (s.ReadByte() != (byte)'\n') ;

					if (compact)
					{
						//						float[] singleRow = new float[mat[0].Length];
						//						byte[] singleBuf = new byte[singleRow.Length << 2];
						byte[] single = new byte[4];
						byte[] buf = new byte[(int)Math.Ceiling(mat[0].Length / 8.0)];
						for (int i = 0; i < mat.Length; i++)
						{
							s.Read(buf, 0, buf.Length);
							BitArray zeroMask = new BitArray(buf);

							for (int j = 0; j < mat[i].Length; j++)
							{
								if (zeroMask[j])
								{
									s.Read(single, 0, 4);
									mat[i][j] = BitConverter.ToSingle(single, 0);
								}
								// the default:
								//else
								//{
								//	mat[i][j] = 0.0F;
								//}
							}
						}

						//							int count = 0;
						//							for (int j = 0; j < zeroMask.Count; j++)  if (zeroMask[j])  count++;
						//							if (count != 0)
						//							{
						//								s.Read(singleBuf, 0, count << 2);
						//								Buffer.BlockCopy(singleBuf, 0, singleRow, 0, count << 2);
						//
						//								int index = 0;
						//								for (int j = 0; j < mat[i].Length; j++)
						//								{
						//									if (zeroMask[j])
						//									{
						//										mat[i][j] = singleRow[index++];
						//									}
						//								}
						//							}
						//						}
					}
					else
					{
						byte[] singleRow = new byte[mat[0].Length << 2];
						for (int i = 0; i < mat.Length; i++)
						{
							s.Read(singleRow, 0, singleRow.Length);
							//for (int j = 0; j < mat[i].Length; j++)
							//{
							//	mat[i][j] = BitConverter.ToSingle(singleRow, j << 2);
							//}
							Buffer.BlockCopy(singleRow, 0, mat[i], 0, singleRow.Length);
						}
					}
				}
				return mat;
			}
			else
			{
				bool allSameLength = true;

				float[][] mat = new float[sizes[0]][];
#if NO_ZSTREAM
				using (StreamReader sr = new StreamReader(fname))
#else
				using (StreamReader sr = ZStreamReader.Open(fname))
#endif
				{
					for (int i = 0; i < mat.Length; i++)
					{
						string[] fields = sr.ReadLine().Split();
						mat[i] = new float[fields.Length];
						for (int j = 0; j < fields.Length; j++)
						{
							mat[i][j] = float.Parse(fields[j]);
						}
						if (allSameLength && i != 0 && mat[i].Length != mat[i-1].Length)  allSameLength = false;
					}
				}

				if (autoBinary && allSameLength)
				{
#if NO_ZSTREAM
					using (Stream s = new FileStream(fname + ".dat", FileMode.Create))
#else
					using (Stream s = ZStreamOut.Open(fname + ".dat"))
#endif
					{
						//						using (BinaryWriter bw = new BinaryWriter(s))
						//						{
						//							bw.Write(System.Text.Encoding.ASCII.GetBytes("@ " + mat.Length + " " + mat[0].Length + " b c\n"));
						//							int maskLength = (int)Math.Ceiling(mat[0].Length / 8.0);
						//							for (int i = 0; i < mat.Length; i++)
						//							{
						//								byte[] mask = new byte[maskLength];
						//								BitArray zeroMask = new BitArray(mask.Length * 8);
						//								for (int j = 0; j < mat[i].Length; j++)
						//								{
						//									zeroMask[j] = (mat[i][j] != 0.0F);
						//								}
						//								zeroMask.CopyTo(mask, 0);
						//								bw.Write(mask);
						//								for (int j = 0; j < mat[i].Length; j++)
						//								{
						//									if (zeroMask[j])
						//									{
						//										bw.Write(mat[i][j]);
						//									}
						//								}
						//							}
						//							//// non-compact:
						//							//bw.Write(System.Text.Encoding.ASCII.GetBytes("@ " + mat.Length + " " + mat[0].Length + " b\n"));
						//							//for (int i = 0; i < mat.Length; i++)
						//							//{
						//							//	for (int j = 0; j < mat[i].Length; j++)
						//							//	{
						//							//		bw.Write(mat[i][j]);
						//							//	}
						//							//}
						//							bw.Flush();
						//						}

						byte[] header = System.Text.Encoding.ASCII.GetBytes("@ " + mat.Length + " " + mat[0].Length + " b c\n");
						s.Write(header, 0, header.Length);

						byte[] singleRow = new byte[mat[0].Length << 2];
						int maskLength = (int)Math.Ceiling(mat[0].Length / 8.0);
						byte[] mask = new byte[maskLength];
						for (int i = 0; i < mat.Length; i++)
						{
							BitArray zeroMask = new BitArray(mask.Length * 8);
							for (int j = 0; j < mat[i].Length; j++)
							{
								zeroMask[j] = (mat[i][j] != 0.0F);
							}
							zeroMask.CopyTo(mask, 0);
							s.Write(mask, 0, mask.Length);

							Buffer.BlockCopy(mat[i], 0, singleRow, 0, singleRow.Length);
							for (int j = 0; j < mat[i].Length; j++)
							{
								if (zeroMask[j])
								{
									s.Write(singleRow, j << 2, 4);
								}
							}
						}

						s.Flush();
					}
				}

				return mat;
			}
		}



		public static float[][] DecompressFloatMat(BinaryReader br)
		{
			//int[] sizes = IOUtils.CountRowsNCols(new StreamReader(br.BaseStream));
			//bool binary = sizes.Length > 2;
			//bool compact = sizes.Length > 2 && (sizes[2] == 1);
			int rows = br.ReadInt32();
			int cols = br.ReadInt32();

			float[][] mat = FloatMatrix(rows, cols);
			for (int i = 0; i < mat.Length; i++)
			{
				int maskLength = (int)Math.Ceiling(mat[i].Length / 8.0);
				BitArray zeroMask = new BitArray(br.ReadBytes(maskLength));
				for (int j = 0; j < mat[i].Length; j++)
				{
					if (zeroMask[j])
					{
						mat[i][j] = br.ReadSingle();
					}
					else
					{
						mat[i][j] = 0.0F;
					}
				}
			}
			return mat;
		}

		//		public static float[][] DecompressFloatMat(byte[] buffer)
		//		{
		//			int rows = BitConverter.ToInt32(buffer, 0);
		//			int cols = BitConverter.ToInt32(buffer, 4);
		//
		//			float[][] mat = FloatMatrix(rows, cols);
		//			int maskLength = (int)Math.Ceiling(cols / 8.0);
		//			int b = 8;
		//			for (int i = 0; i < mat.Length; i++)
		//			{
		//				//BitArray zeroMask = new BitArray(br.ReadBytes(maskLength));
		//				int s = b + maskLength;
		//				for (int j = 0; j < mat[i].Length; j++)
		//				{
		//					if ((buffer[b + (j >> 3)] & (1 << (j % 8))) != 0)
		//					{
		//						mat[i][j] = BitConverter.ToSingle(buffer, s);
		//						s += 4;
		//					}
		//					else
		//					{
		//						mat[i][j] = 0.0F;
		//					}
		//				}
		//				b = s;
		//			}
		//			return mat;
		//		}

		unsafe public static float[][] DecompressFloatMat(byte[] bufferArray)
		{
			unchecked
			{
				fixed (byte* buffer = bufferArray)
				{
					byte* b = buffer;
					int rows = *((int*)b);
					b += 4;
					int cols = *((int*)b);
					b += 4;

					float[][] mat = FloatMatrix(rows, cols);
					int maskLength = (cols + 7) >> 3;
					for (int i = 0; i < mat.Length; i++)
					{
						fixed (float* row = mat[i])
						{
							float* r = row;
							float* rEnd = r + cols;
							float* d = (float*)(b + maskLength);
							byte bit = 1;
							while (r != rEnd)
							{
								if (((*b) & bit) != 0)
								{
									*r = *(d++);
								}
								r++;
								bit <<= 1;
								if (bit == 0)
								{
									bit = 1;
									b++;
								}
							}
							b = (byte*)d;
						}
					}
					return mat;
				}
			}
		}


		// Assumes all rows are the same length
		public static void CompressFloatMat(float[][] mat, BinaryWriter bw) 
		{
			//bw.Write(System.Text.Encoding.ASCII.GetBytes("@ " + mat.Length + " " + mat[0].Length + " b c\n"));
			bw.Write(mat.Length);
			bw.Write(mat[0].Length);
			int maskLength = (int)Math.Ceiling(mat[0].Length / 8.0);
			for (int i = 0; i < mat.Length; i++)
			{
				byte[] mask = new byte[maskLength];
				BitArray zeroMask = new BitArray(mask.Length * 8);
				for (int j = 0; j < mat[i].Length; j++)
				{
					zeroMask[j] = (mat[i][j] != 0.0F);
				}
				zeroMask.CopyTo(mask, 0);
				bw.Write(mask);
				for (int j = 0; j < mat[i].Length; j++)
				{
					if (zeroMask[j])
					{
						bw.Write(mat[i][j]);
					}
				}
			}
		}

		unsafe public static byte[] CompressFloatMat(float[][] mat)
		{
			unchecked
			{
				int rows = mat.Length;
				int cols = mat[0].Length;
				int maskLength = (cols + 7) >> 3;
				int len;
				byte[] bufferArray = new byte[rows * (maskLength + (cols << 2)) + 2];
				fixed (byte* buffer = bufferArray)
				{
					byte* b = buffer;
					*((int*)b) = rows;
					b += 4;
					*((int*)b) = cols;
					b += 4;
					for (int i = 0; i < mat.Length; i++)
					{
						fixed (float* row = mat[i])
						{
							float* r = row;
							float* rEnd = row + cols;
							float* d = (float*)(b + maskLength);
							byte bit = 1;
							while (r != rEnd)
							{
								if (*r != 0.0F)
								{
									*(d++) = *r;
									*b |= bit;
								}
								r++;
								bit <<= 1;
								if (bit == 0)
								{
									bit = 1;
									b++;
								}
							}
							b = (byte*)d;
						}
					}
					len = (int)(b - buffer);
				}
				if (bufferArray.Length != len)
				{
					byte[] all = bufferArray;
					bufferArray = new byte[len];
					Buffer.BlockCopy(all, 0, bufferArray, 0, bufferArray.Length);
				}
				return bufferArray;
			}
		}




		public static RelType[][] LoadRelTypeJag(string fname)
		{
			char[] seps = new char[1] { '\t' };
			string[][] strArr = IOUtils.ReadSplitRows(fname, seps);
			RelType[][] mat = new RelType[strArr.Length][];
			for(int i = 0; i < strArr.Length; i++)
			{
				mat[i] = new RelType[strArr[i].Length];
				for(int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = (RelType)Enum.Parse(typeof(RelType), strArr[i][j], true);
				}
			}
			return mat;
		}


		public static double[][] LoadDoubleJag(string fname)
		{
			char[] seps = new char[1] { '\t' };
			string[][] strArr = IOUtils.ReadSplitRows(fname, seps);
			double[][] jag = new double[strArr.Length][];
			for(int i = 0; i < strArr.Length; i++)
			{
				jag[i] = new double[strArr[i].Length];
				for(int j = 0; j < jag[i].Length; j++)
				{
					jag[i][j] = double.Parse(strArr[i][j]);
				}
			}
			return jag;
		}


		public static float[][] LoadFloatJag(string fname)
		{
			char[] seps = new char[1] { '\t' };
			string[][] strArr = IOUtils.ReadSplitRows(fname, seps);
			float[][] jag = new float[strArr.Length][];
			for(int i = 0; i < strArr.Length; i++)
			{
				jag[i] = new float[strArr[i].Length];
				for(int j = 0; j < jag[i].Length; j++)
				{
					jag[i][j] = float.Parse(strArr[i][j]);
				}
			}
			return jag;
		}


		public static int[][] LoadIntJag(string fname)
		{
			char[] seps = new char[1] { '\t' };
			string[][] strArr = IOUtils.ReadSplitRows(fname, seps);
			int[][] jag = new int[strArr.Length][];
			for(int i = 0; i < strArr.Length; i++)
			{
				jag[i] = new int[strArr[i].Length];
				for(int j = 0; j < jag[i].Length; j++)
				{
					jag[i][j] = int.Parse(strArr[i][j]);
				}
			}
			return jag;
		}


		/// <summary>
		/// Version that only loads those rows indexed by "indices".  E.g. indices=[3,5] only loads the 4th and 6th row.
		/// NOTE: for safety, this function also loads in the exact order specificed in indices, so if indices=[5,3], the 6th
		/// row will be loaded first.  This makes it slightly tricky because we want to avoid loading the whole file.  
		/// </summary>
		/// <param name="fname"></param>
		/// <param name="indices">Is _not_ changed in place.</param>
		/// <returns></returns>
		public static float[][] LoadFloatMat(string fname, int[] indicesIn)
		{
			int[] indices = ArrayUtils.Copy(indicesIn);
			int[] sizes = IOUtils.CountRowsNCols(fname);
			int nToLoad = indices.Length;
			int nRows = sizes[0], nCols;
			float[][] mat = new float[nToLoad][];
			
			int[] crossIndex = new int[nToLoad];
			ArrayUtils.Range(crossIndex, 0, 1);
			Array.Sort(indices, crossIndex);
			bool[] loadThese = new bool[nRows];
			for (int i = 0; i < nToLoad; i++)
			{
				loadThese[indices[i]] = true;
			}

#if NO_ZSTREAM
			using (StreamReader sr = new StreamReader(fname))
#else
			using (StreamReader sr = ZStreamReader.Open(fname))
#endif
			{
				char[] seps = {' ', '\t'};
				string line;
				int ctr = 0;
				for (int i = 0; i < nRows; i++)
				{
					line = sr.ReadLine();
					if (loadThese[i])
					{
						string[] fields = line.Split(seps);
						nCols = fields.Length;
						mat[crossIndex[ctr]] = new float[nCols];
						float[] row = mat[crossIndex[ctr]];
						++ctr;
						for (int j = 0; j < nCols; j++)
						{
							row[j] = float.Parse(fields[j]);
						}
					}
				}
			}
			return mat;
		}

		#endregion




		public static void MatrixStats(float[][] mat)
		{
			int length = mat.Length * mat[0].Length;
			Console.WriteLine("" + length + " elements (" + mat.Length + " X " + mat[0].Length + ")");
			int zeroCount = 0;
			int nonPosCount = 0;
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					if (mat[i][j] <= 0)
					{
						nonPosCount++;
						if (mat[i][j] == 0.0F)
						{
							zeroCount++;
						}
					}
				}
			}
			Console.WriteLine("          zero: " + zeroCount.ToString().PadLeft(length.ToString().Length) +
				" [" + (zeroCount / (double)length).ToString("P1") + "]");
			Console.WriteLine("  non-positive: " + nonPosCount.ToString().PadLeft(length.ToString().Length) +
				" [" + (nonPosCount / (double)length).ToString("P1") + "]");
			int distinctMax = 100000;
			Console.WriteLine("  Distinct values:");
			int totalBytes = 0;
			for (int j = 0; j < mat[0].Length; j++)
			{
				Hashtable dist = new Hashtable();
				for (int i = 0; i < mat.Length; i++)
				{
					dist[mat[i][j]] = null;
					if (dist.Count > distinctMax)  break;
				}
				int distinct = dist.Count;
				int bytes = distinct == 1 ? 0 : distinct < (1 << 8) ? 1 : distinct < (1 << 16) ? 2 : 4;
				totalBytes += bytes;
				Console.WriteLine("    col " + j.ToString().PadLeft(mat[0].Length.ToString().Length) + ": " +
					(distinct > distinctMax ? "> " + distinctMax : distinct.ToString().PadLeft(mat.Length.ToString().Length)) +
					"  (" + bytes + " bytes)");
			}
			Console.WriteLine("  Bytes used: " + (4 * mat[0].Length) + "  Bytes needed: " + totalBytes);
		}


		/// <summary>
		/// Labels are assumed to be simply either 0 or X, where X should be ranked higher than 0.
		/// If there are multiple X's, only the top-scoring X contributes to the MRR.
		/// </summary>
		/// <param name="labels"></param>
		/// <param name="scores"></param>
		
		public static double ReciprocalRank(int[] labels, double[] scores)
		{
			if(labels.Length != scores.Length) throw new Exception("ReciprocalRnak: size mismatch");
			int[] labelsCP = new int[labels.Length];
			double[] negScores = new double[scores.Length];
			for(int i=0; i<labelsCP.Length; ++i)
			{
				labelsCP[i] = labels[i];
				negScores[i] = -scores[i];
			}
			Array.Sort(negScores, labelsCP);
			double mrr = 0.0, rank = 1.0;
			bool cont = true;
			for(int i=0; i<labelsCP.Length && cont; ++i)
			{
				if(labelsCP[i] != 0) 
				{
					mrr = 1.0/rank;
					cont = false;
				}
				++rank;
			}
			
			return mrr;
		}
		public static void MinMax(int[] vec, out int minVal, out int maxVal)
		{
			maxVal = vec[0];
			minVal = maxVal;
			int val;
			for (int i = 1; i < vec.Length; i++)
			{
				val = vec[i];
				if (val > maxVal)
				{
					maxVal = val;
				}
				else if (val < minVal)
				{
					minVal = val;
				}
			}
		}
		public static void MinMax(float[] vec, out float minVal, out float maxVal)
		{
			maxVal = vec[0];
			minVal = maxVal;
			float val;
			for (int i = 1; i < vec.Length; i++)
			{
				val = vec[i];
				if (val > maxVal)
				{
					maxVal = val;
				}
				else if (val < minVal)
				{
					minVal = val;
				}
			}
		}

		public static void MinMax(float[][] mat, out float minVal, out float maxVal)
		{
			maxVal = mat[0][0];
			minVal = maxVal;
			float val;
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					val = mat[i][j];
					if (val > maxVal)
					{
						maxVal = val;
					}
					else if (val < minVal)
					{
						minVal = val;
					}
				}
			}
		}

		public static void MinMaxCol(float[][] mat, int colIdx, out float minVal, out float maxVal)
		{
			maxVal = mat[0][colIdx];
			minVal = maxVal;
			float val;
			for (int i = 1; i < mat.Length; i++)
			{
				val = mat[i][colIdx];
				if (val > maxVal)
				{
					maxVal = val;
				}
				else if (val < minVal)
				{
					minVal = val;
				}
			}
		}
		public static void MinMaxAllCols(float[][] mat, out float[] minVals, out float[] maxVals)
		{
			minVals = new float[mat[0].Length];
			maxVals = new float[mat[0].Length];
			for(int j=0; j<mat[0].Length; ++j)
			{
				float maxVal = mat[0][j];
				float minVal = maxVal;
				float val;
				for (int i=1; i<mat.Length; i++)
				{
					val = mat[i][j];
					if (val > maxVal)
					{
						maxVal = val;
					}
					else if (val < minVal)
					{
						minVal = val;
					}
				}
				minVals[j] = minVal;
				maxVals[j] = maxVal;
			}
		}

		public static double Max(double[] vec)
		{
			double maxVal = vec[0];
			double val;
			for (int i = 1; i < vec.Length; i++)
			{
				val = vec[i];
				if (val > maxVal)
				{
					maxVal = val;
				}
			}
			return maxVal;
		}

		public static int Max(int[] vec)
		{
			int maxVal = vec[0];
			int val;
			for (int i = 1; i < vec.Length; i++)
			{
				val = vec[i];
				if (val > maxVal)
				{
					maxVal = val;
				}
			}
			return maxVal;
		}


		public static double Mean(float[] vec)
		{
			double ans = 0;
			for (int i = 0; i < vec.Length; i++)
			{
				ans += vec[i];
			}
			return ans / vec.Length;
		}
		/// <summary>
		/// Name is attempt to disambiguate - are we taking the mean OF the cols, giving a vector of
		/// dimension the number of rows, or the mean PER the cols, giving a vector of dim the number
		/// of cols?  Here it's the former.
		/// </summary>
		/// <param name="data"></param>
		/// <returns></returns>
		public static double[] MeanOfColVectors(float[][] data)
		{
			double[] ans = new double[data.Length];
			// assume all are same width:
			double recip = 1.0 / data[0].Length;
			for (int i = 0; i < data.Length; i++)
			{
				for (int j = 0; j < data[i].Length; j++)
				{
					ans[i] += data[i][j];
				}
				ans[i] *= recip;
			}
			return ans;
		}
		public static double[] MeanOfRowVectors(float[][] data)
		{
			// assume all are same width:
			double[] ans = new double[data[0].Length];
			double recip = 1.0 / data.Length;
			//			// assume all are same width:
			//			for (int j = 0; j < data[0].Length; j++)
			//			{
			//				double colSum = 0.0;
			//				for (int i = 0; i < data.Length; i++)
			//				{
			//					colSum += data[i][j];
			//				}
			//				ans[j] = colSum * recip;
			//			}
			for (int i = 0; i < data.Length; i++)
			{
				for (int j = 0; j < data[0].Length; j++)
				{
					ans[j] += data[i][j];
				}
			}
			for (int j = 0; j < ans.Length; j++)
			{
				ans[j] *= recip;
			}
			return ans;
		}


		public static void Mult(double[] vec, double val)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] *= val;
			}
		}

		public static void Mult(double[][] mat, double val)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] *= val;
				}
			}
		}



		public static void Print(double[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Console.Write("{0:F3} ", vec[i]);
			}
			Console.WriteLine("");
		}

		public static void Print(float[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Console.Write("{0:F3} ", vec[i]);
			}
			Console.WriteLine("");
		}

		public static void Print(int[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Console.Write("{0:F3} ", vec[i]);
			}
			Console.WriteLine("");
		}
		public static void Print(sbyte[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Console.Write("{0} ", vec[i]);
			}
			Console.WriteLine("");
		}
		public static void Print(string[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Console.Write("{0:F3} ", vec[i]);
			}
			Console.WriteLine("");
		}

		public static void Print(bool[] vec)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				Console.Write("{0:F3} ", vec[i]);
			}
			Console.WriteLine("");
		}

		public static void Print(double[][] mat)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					Console.Write("{0:F3} ", mat[i][j]);
				}
				Console.WriteLine("");
			}
		}

		public static void Print(float[][] mat)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					Console.Write("{0:F3} ", mat[i][j]);
				}
				Console.WriteLine("");
			}
		}

		public static void Print(int[][] mat)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					Console.Write("{0:D3} ", mat[i][j]);
				}
				Console.WriteLine("");
			}
		}

		public static void Print(sbyte[][] mat)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					Console.Write("{0} ", mat[i][j]);
				}
				Console.WriteLine("");
			}
		}

		public static void Print(RelType[][] mat)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					Console.Write("{0} ", mat[i][j]);
				}
				Console.WriteLine("");
			}
		}

		public static void Print(double[][] mat, int row)
		{
			for (int j = 0; j < mat[0].Length; j++)
			{
				Console.Write("{0:F3} ", mat[row][j]);
			}
			Console.WriteLine("");
		}

		public static void Print(float[][] mat, int row)
		{
			for (int j = 0; j < mat[0].Length; j++)
			{
				Console.Write("{0:F3} ", mat[row][j]);
			}
			Console.WriteLine("");
		}

		public static void Print(int[][] mat, int row)
		{
			for (int j = 0; j < mat[0].Length; j++)
			{
				Console.Write("{0:F3} ", mat[row][j]);
			}
			Console.WriteLine("");
		}


		public static void Random(bool[] vec)
		{
#if ALLOW_RANDOM
			Random(vec, new Random());
#else
			throw new Exception("Random initialized to timer!");
#endif
		}
		public static void Random(bool[] vec, Random rand)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = (rand.NextDouble() < 0.5);
			}
		}
		public static void Random(bool[] vec, Random rand, double thrsh)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = (rand.NextDouble() < thrsh);
			}
		}

		public static void Random(sbyte[][] mat, sbyte minVal, sbyte maxVal)
		{
#if ALLOW_RANDOM
			Random(mat, minVal, maxVal, new Random());
#else
			throw new Exception("Random initialized to timer!");
#endif
		}
		public static void Random(sbyte[][] mat, sbyte minVal, sbyte maxVal, Random rand)
		{
			// Is this inclusive of maxVal? ***
			if (maxVal <= minVal)  throw new ArgumentException("minVal must be less than maxVal");
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = (sbyte)rand.Next(minVal, maxVal);
				}
			}
		}

		[Obsolete("Use the version that takes a Random, constructed with the desired seed.")]
		public static void Random(float[] vec, float minVal, float maxVal, int seed)
		{
			Random(vec, minVal, maxVal, new Random(seed));
		}
		public static void Random(float[] vec, float minVal, float maxVal)
		{
#if ALLOW_RANDOM
			Random(vec, minVal, maxVal, new Random());
#else
			throw new Exception("Random initialized to timer!");
#endif
		}
		public static void Random(float[] vec, float minVal, float maxVal, Random rand)
		{
			if (maxVal <= minVal)  throw new ArgumentException("minVal must be less than maxVal");
			float delta = maxVal - minVal;
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = ((float)rand.NextDouble()) * delta + minVal;
			}
		}

		[Obsolete("Use the version that takes a Random, constructed with the desired seed.")]
		public static void Random(float[][] mat, float minVal, float maxVal, int seed)
		{
			Random(mat, minVal, maxVal, new Random(seed));
		}
		public static void Random(float[][] mat, float minVal, float maxVal)
		{
#if ALLOW_RANDOM
			Random(mat, minVal, maxVal, new Random());
#else
			throw new Exception("Random initialized to timer!");
#endif
		}
		public static void Random(float[][] mat, float minVal, float maxVal, Random rand)
		{
			if (maxVal <= minVal)  throw new ArgumentException("minVal must be less than maxVal");
			float delta = maxVal - minVal;
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = ((float)rand.NextDouble()) * delta + minVal;
				}
			}
		}

		[Obsolete("Use the version that takes a Random, constructed with the desired seed.")]
		public static void Random(double[] vec, double minVal, double maxVal, int seed)
		{
			Random(vec, minVal, maxVal, new Random(seed));
		}
		public static void Random(double[] vec, double minVal, double maxVal)
		{
#if ALLOW_RANDOM
			Random(vec, minVal, maxVal, new Random());
#else
			throw new Exception("Random initialized to timer!");
#endif
		}
		public static void Random(double[] vec, double minVal, double maxVal, Random rand)
		{
			if (maxVal <= minVal)  throw new ArgumentException("minVal must be less than maxVal");
			double delta = maxVal - minVal;
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = rand.NextDouble() * delta + minVal;
			}
		}

		[Obsolete("Use the version that takes a Random, constructed with the desired seed.")]
		public static void Random(double[][] mat, double minVal, double maxVal, int seed)
		{
			Random(mat, minVal, maxVal, new Random(seed));
		}
		public static void Random(double[][] mat, double minVal, double maxVal)
		{
#if ALLOW_RANDOM
			Random(mat, minVal, maxVal, new Random());
#else
			throw new Exception("Random initialized to timer!");
#endif
		}
		public static void Random(double[][] mat, double minVal, double maxVal, Random rand)
		{
			if (maxVal <= minVal)  throw new ArgumentException("minVal must be less than maxVal");
			double delta = maxVal - minVal;
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = rand.NextDouble() * delta + minVal;
				}
			}
		}

		public static void Range(int[] vec, int start, int step)
		{
			int val = start;
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = val;
				val += step;
			}
		}
		public static void Range(float[][] mat, float start, float step)
		{
			float val = start;
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = val;
					val += step;
				}
			}
		}
		public static void Range(int[][] mat, int start, int step)
		{
			int val = start;
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = val;
					val += step;
				}
			}
		}

		public static void Range(double[][] mat, double start, double step)
		{
			double val = start;
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = val;
					val += step;
				}
			}
		}
		public static void RangeRows(int[][] mat, int start, int step)
		{
			int val = start;
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = val;
				}
				val += step;
			}
		}

		public static void RangeRows(float[][] mat, float start, float step)
		{
			float val = start;
			for (int i = 0; i < mat.Length; i++)
			{
				for (int j = 0; j < mat[i].Length; j++)
				{
					mat[i][j] = val;
				}
				val += step;
			}
		}


		public static double[][] ReadBinaryDouble(string fname, int nRows, int nCols)
		{
			double[][] mat = DoubleMatrix(nRows, nCols);
			ReadBinaryDouble(mat, fname);
			return mat;
		}
		public static void ReadBinaryDouble(double[][] mat, string fname)
		{
#if NO_ZSTREAM
			using (Stream fs = new FileStream(fname, FileMode.Open))
#else
			using (Stream fs = ZStreamIn.Open(fname))
#endif
			{
				try
				{
					BinaryReader br = new BinaryReader(fs);
					for (int i = 0; i < mat.Length; i++)
					{
						for (int j = 0; j < mat[i].Length; j++)
						{
							mat[i][j] = br.ReadDouble();
						}
					}
				}
				catch (EndOfStreamException)
				{
					throw new IOException("ReadBinaryDouble: stream ended prematurely");
				}
				catch (ObjectDisposedException)
				{
					throw new IOException("ReadBinaryDouble: stream closed");
				}
				catch (IOException)
				{
					throw new IOException("ReadBinaryDouble: I/O exception");
				}
			}
		}

		public static float[][] ReadBinaryFloat(string fname, int nRows, int nCols)
		{
			float[][] mat = FloatMatrix(nRows, nCols);
			ReadBinaryFloat(mat, fname);
			return mat;
		}
		public static void ReadBinaryFloat(float[][] mat, string fname)
		{
#if NO_ZSTREAM
			using (Stream fs = new FileStream(fname, FileMode.Open))
#else
			using (Stream fs = ZStreamIn.Open(fname))
#endif
			{
				try
				{
					BinaryReader br = new BinaryReader(fs);
					for (int i = 0; i < mat.Length; i++)
					{
						for (int j = 0; j < mat[i].Length; j++)
						{
							mat[i][j] = br.ReadSingle();
						}
					}
				}
				catch (EndOfStreamException)
				{
					throw new IOException("ReadBinaryFloat: stream ended prematurely");
				}
				catch (ObjectDisposedException)
				{
					throw new IOException("ReadBinaryFloat: stream closed");
				}
				catch (IOException)
				{
					throw new IOException("ReadBinaryFloat: I/O exception");
				}
			}
		}


		/// <summary>
		/// Rescale data linearly from lo to hi.  If sdev is zero, map to 0.0.
		/// </summary>
		/// <param name="data"></param>
		/// <param name="lo"></param>
		/// <param name="hi"></param>
		public static void Rescale(float[] data, float lo, float hi)
		{
			int nRows = data.Length;
			float val, maxVal = data[0], minVal = maxVal;
			for (int i = 0; i < nRows; i++)
			{
				val = data[i];
				if (val > maxVal)
				{
					maxVal = val;
				}
				else if (val < minVal)
				{
					minVal = val;
				}
			}
			if (maxVal == minVal)
			{
				for (int i = 0; i < nRows; i++)
				{
					data[i] = 0.0F;
				}
			}
			else
			{

				double a = (hi - lo) / (maxVal-minVal);
				double b = hi - a * maxVal;
				for (int i = 0; i < nRows; i++)
				{
					data[i] = (float)(a * data[i] + b);
				}
			}
		}
		public static void Rescale(double[] data, double lo, double hi)
		{
			int nRows = data.Length;
			double val, maxVal = data[0], minVal = maxVal;
			for (int i = 0; i < nRows; i++)
			{
				val = data[i];
				if (val > maxVal)
				{
					maxVal = val;
				}
				else if (val < minVal)
				{
					minVal = val;
				}
			}
			if (maxVal == minVal)
			{
				for (int i = 0; i < nRows; i++)
				{
					data[i] = 0.0;
				}
			}
			else
			{

				double a = (hi - lo) / (maxVal-minVal);
				double b = hi - a * maxVal;
				for (int i = 0; i < nRows; i++)
				{
					data[i] = a * data[i] + b;
				}
			}
		}
		public static void RescaleFromMinusOneToOne(double[][] data)
		{
			int nRows = data.Length, nCols = data[0].Length;
			double val, maxVal = data[0][0], minVal = maxVal;
			for (int i = 0; i < nRows; i++)
			{
				for (int j = 0; j < nCols; j++)
				{
					val = data[i][j];
					if (val > maxVal)
					{
						maxVal = val;
					}
					else if (val < minVal)
					{
						minVal = val;
					}
				}
			}
			if (maxVal == minVal)  throw new IndexOutOfRangeException("RescaleFromMinusOneToOne: zero sdev");
			double a = 2.0/(maxVal-minVal);
			double b = 1.0-a*maxVal;
			for (int i = 0; i < nRows; i++)
			{
				for (int j = 0; j < nCols; j++)
				{
					data[i][j] = a * data[i][j] + b;
				}
			}
		}
		/// <summary>
		/// Run length encoding.
		/// </summary>
		/// <param name="x">The values to encode.</param>
		/// <returns>int[*,3]: First col: length; second: start index; third: value.</returns>
		public static float[][] RunEncode(float[] x)
		{
			if (x.Length == 0)
			{
				throw new ArgumentOutOfRangeException("vector must have at least one element");
			}
			int nPoints = x.Length;
			float[][] runs = FloatMatrix(nPoints, 3);  // First col: length; second: start index.
			int start=0, end=0;
			int runCtr = 0;
			float y, val = x[0];

			for (int i = 0; i < nPoints; i++, end++)
			{
				y = x[i];
				if (y != val)
				{
					runs[runCtr][0] = (float)(end - start);
					runs[runCtr][1] = (float)start;
					runs[runCtr][2] = val;
					start = end;
					++runCtr;
					val = y;
				}
			}

			// There'll always be a last run.
			runs[runCtr][0] = (float)(end - start);
			runs[runCtr][1] = (float)start;
			runs[runCtr][2] = val;
			++runCtr;

			if (runCtr == runs.Length)  return runs;
			//float[][] ans = FloatMatrix(runCtr,3);
			//// should check these copies! ***
			//Array.Copy(runs, 0, ans, 0, ans.Length);
			float[][] ans = new float[runCtr][];
#if ENABLE_BARTOK
			for (int i = 0; i < ans.Length; i++)
			{
				ans[i] = runs[i];
			}
#else
			Array.Copy(runs, 0, ans, 0, ans.Length);
#endif
			return ans;
		}


		public static int[][] RunEncode(int[] x)
		{
			if (x.Length == 0)
			{
				throw new ArgumentOutOfRangeException("vector must have at least one element");
			}
			int nPoints = x.Length;
			int[][] runs = IntMatrix(nPoints, 3);  // First col: length; second: start index.
			int start=0, end=0;
			int runCtr = 0;
			int y, val = x[0];

			for (int i = 0; i < nPoints; i++, end++)
			{
				y = x[i];
				if (y != val)
				{
					runs[runCtr][0] = end - start;
					runs[runCtr][1] = start;
					runs[runCtr][2] = val;
					start = end;
					++runCtr;
					val = y;
				}
			}

			// There'll always be a last run.
			runs[runCtr][0] = end - start;
			runs[runCtr][1] = start;
			runs[runCtr][2] = val;
			++runCtr;

			if (runCtr == runs.Length)  return runs;
			int[][] ans = new int[runCtr][];
#if ENABLE_BARTOK
			for (int i = 0; i < ans.Length; i++)
			{
				ans[i] = runs[i];
			}
#else
			Array.Copy(runs, 0, ans, 0, ans.Length);
#endif
			return ans;
		}

		
		public static bool SameP(bool[] v1, bool[] v2)
		{
			bool ans = true;
			int nRows = v1.Length;
			if (nRows != v2.Length)
			{
				return false;
			}
			else
			{
				for (int i = 0; i < nRows; i++)
				{
					if (v1[i] != v2[i])
					{
						ans = false;
						break;
					}
				}
			}
			return ans;
		}

		public static bool SameP(double[] v1, double[] v2)
		{
			bool ans = true;
			int nRows = v1.Length;
			if (nRows != v2.Length)
			{
				return false;
			}
			else
			{
				for (int i = 0; i < nRows; i++)
				{
					if (v1[i] != v2[i])
					{
						ans = false;
						break;
					}
				}
			}
			return ans;
		}

		public static bool SameP(sbyte[][] m1, sbyte[][] m2)
		{
			bool ans = true;
			int nRows = m1.Length, nCols = m1[0].Length;
			if (nRows != m2.Length || nCols != m2[0].Length)
			{
				return false;
			}
			else
			{
				for (int i = 0; i < nRows; i++)
				{
					for (int j = 0; j < nCols; j++)
					{
						if (m1[i][j] != m2[i][j])
						{
							ans = false;
							break;
						}
					}
				}
			}
			return ans;
		}

		public static bool SameP(float[][] m1, float[][] m2)
		{
			bool ans = true;
			int nRows = m1.Length, nCols = m1[0].Length;
			if (nRows != m2.Length || nCols != m2[0].Length)
			{
				return false;
			}
			else
			{
				for (int i = 0; i < nRows; i++)
				{
					for (int j = 0; j < nCols; j++)
					{
						if (m1[i][j] != m2[i][j])
						{
							ans = false;
							break;
						}
					}
				}
			}
			return ans;
		}


		/// <summary>
		/// Save to disk.
		/// </summary>
		/// <param name="vec"></param>
		/// <param name="fname"></param>
		public static void SaveSerial(double[] vec, string fname)
		{
#if NO_ZSTREAM
			using (Stream fs = new FileStream(fname, FileMode.Create))
#else
			using (Stream fs = ZStreamOut.Open(fname))
#endif
			{
				BinaryFormatter serializer = new BinaryFormatter();
				serializer.Serialize(fs, vec);
			}
		}

		public static void SaveSerial(double[][] mat, string fname)
		{
#if NO_ZSTREAM
			using (Stream fs = new FileStream(fname, FileMode.Create))
#else
			using (Stream fs = ZStreamOut.Open(fname))
#endif
			{
				BinaryFormatter serializer = new BinaryFormatter();
				serializer.Serialize(fs, mat);
			}
		}

		public static void SaveSerial(float[][] mat, string fname)
		{
#if NO_ZSTREAM
			using (Stream fs = new FileStream(fname, FileMode.Create))
#else
			using (Stream fs = ZStreamOut.Open(fname))
#endif
			{
				BinaryFormatter serializer = new BinaryFormatter();
				serializer.Serialize(fs, mat);
			}
		}

		public static void SaveSerial(RelType[][] mat, string fname)
		{
#if NO_ZSTREAM
			using (Stream fs = new FileStream(fname, FileMode.Create))
#else
			using (Stream fs = ZStreamOut.Open(fname))
#endif
			{
				BinaryFormatter serializer = new BinaryFormatter();
				serializer.Serialize(fs, mat);
			}
		}


		public static void SaveRow(float[][] mat, int rowIdx, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				for (int i = 0; i < mat[rowIdx].Length; i++)
				{
					sw.WriteLine(mat[rowIdx][i]);
				}
			}
		}


		public static void Save(bool[] vec, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				for (int i = 0; i < vec.Length; i++)
				{
					sw.WriteLine(vec[i].ToString());
				}
			}
		}

		public static void Save(BitArray vec, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				for (int i = 0; i < vec.Length; i++)
				{
					sw.WriteLine(vec[i].ToString());
				}
			}
		}

		public static void Save(int[] vec, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				for (int i = 0; i < vec.Length; i++)
				{
					sw.WriteLine(vec[i].ToString());
				}
			}
		}

		public static void Save(float[] vec, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				for (int i = 0; i < vec.Length; i++)
				{
					sw.WriteLine(vec[i].ToString("R"));
				}
			}
		}


		public static void Save(byte[] vec, BinaryWriter bw)
		{
			bw.Write(vec.Length);
			bw.Write(vec);
		}
		public static byte[] LoadByteVec(BinaryReader br)
		{
			byte[] vec = br.ReadBytes(br.ReadInt32());
			return vec;
		}


		public static void Save(float[] vec, BinaryWriter bw) 
		{
			bw.Write(vec.Length);
			for (int i = 0; i < vec.Length; i++)
			{
				bw.Write(vec[i]);
			}
		}
		public static float[] LoadFloatVec(BinaryReader br)
		{
			float[] vec = new float[br.ReadInt32()];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = br.ReadSingle();
			}
			return vec;
		}

		public static void Save(float[][] mat, BinaryWriter bw)
		{
			bw.Write(mat.Length);
			bw.Write(mat.Length == 0 ? 0 : mat[0].Length);
			for (int j = 0; j < mat.Length; j++)
			{
				for (int i = 0; i < mat[j].Length; i++)
				{
					bw.Write(mat[j][i]);
				}
			}
		}
		public static float[][] LoadFloatMat(BinaryReader br)
		{
			float[][] mat = new float[br.ReadInt32()][];
			int width = br.ReadInt32();
			for (int j = 0; j < mat.Length; j++)
			{
				mat[j] = new float[width];
				for (int i = 0; i < mat[j].Length; i++)
				{
					mat[j][i] = br.ReadSingle();
				}
			}
			return mat;
		}
		
		public static void Save(int[] vec, BinaryWriter bw)
		{
			bw.Write(vec.Length);
			for (int i = 0; i < vec.Length; i++)
			{
				bw.Write(vec[i]);
			}
		}
		public static int[] LoadIntVec(BinaryReader br)
		{
			int[] vec = new int[br.ReadInt32()];
			for (int i = 0; i < vec.Length; i++)
			{
				vec[i] = br.ReadInt32();
			}
			return vec;
		}


		public static void Save(BitArray vec, BinaryWriter bw) 
		{
			bw.Write(vec.Length);
			if (vec.Length != 0)
			{
				int numBytes = (int)Math.Ceiling(vec.Length / 8.0);
				byte[] bytes = new byte[numBytes];
				vec.CopyTo(bytes, 0);
				bw.Write(bytes);
			}
		}
		public static BitArray LoadBitArray(BinaryReader br)
		{
			int len = br.ReadInt32();
			if (len == 0)
			{
				return new BitArray(0);
			}
			else
			{
				int numBytes = (int)Math.Ceiling(len / 8.0);
				byte[] bytes = br.ReadBytes(numBytes);
				BitArray vec = new BitArray(bytes);
				vec.Length = len;
				return vec;
			}
		}



		public static void Save(double[] vec, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				for (int i = 0; i < vec.Length; i++)
				{
					sw.WriteLine(vec[i].ToString("R"));
				}
			}
		}

		public static void Save(string[] vec, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				for (int i = 0; i < vec.Length; i++)
				{
					sw.WriteLine(vec[i]);
				}
			}
		}
		public static void Save(ArrayList arrList, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				for (int i = 0; i < arrList.Count; i++)
				{
					sw.WriteLine((string)arrList[i]);
				}
			}
		}
		public static void Save(RelType[] vec, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				for (int i = 0; i < vec.Length; i++)
				{
					sw.WriteLine(vec[i]);
				}
			}
		}

		public static void Save(sbyte[][] mat, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				int nRows = mat.Length;
				int nCols = mat[0].Length;
				for (int i = 0; i < nRows; i++)
				{
					for (int j = 0; j < nCols-1; j++)
					{
						sw.Write(mat[i][j].ToString());
						sw.Write("\t");
					}
					sw.WriteLine(mat[i][nCols-1].ToString());
				}
			}
		}
		public static void Save(int[][] mat, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				int nRows = mat.Length;
				int nCols = mat[0].Length;
				for (int i = 0; i < nRows; i++)
				{
					for (int j = 0; j < nCols-1; j++)
					{
						sw.Write(mat[i][j].ToString());
						sw.Write("\t");
					}
					sw.WriteLine(mat[i][nCols-1].ToString());
				}
			}
		}
		public static void Save(float[][] mat, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				int nRows = mat.Length;
				int nCols;
				for (int i = 0; i < nRows; i++)
				{
					nCols = mat[i].Length;
					for (int j = 0; j < nCols-1; j++)
					{
						sw.Write(mat[i][j].ToString("R"));
						sw.Write("\t");
					}
					sw.WriteLine(mat[i][nCols-1].ToString("R"));
				}
			}
		}
		public static void Save(RelType[][] mat, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				int nRows = mat.Length;
				int nCols;
				for (int i = 0; i < nRows; i++)
				{
					nCols = mat[i].Length;
					for (int j = 0; j < nCols-1; j++)
					{
						sw.Write(mat[i][j]);
						sw.Write("\t");
					}
					sw.WriteLine(mat[i][nCols-1]);
				}
			}
		}


		/// <summary>
		/// Versions that only save the first nRows rows.
		/// </summary>
		/// <param name="mat"></param>
		/// <param name="nRows"></param>
		/// <param name="fname"></param>
		public static void Save(float[][] mat, int nRows, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				int nCols;
				for (int i = 0; i < nRows; i++)
				{
					nCols = mat[i].Length;
					for (int j = 0; j < nCols-1; j++)
					{
						sw.Write(mat[i][j].ToString("R"));
						sw.Write("\t");
					}
					sw.WriteLine(mat[i][nCols-1].ToString("R"));
				}
			}
		}

		public static void Save(sbyte[][] mat, int nRows, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				int nCols = mat[0].Length;
				for (int i = 0; i < nRows; i++)
				{
					for (int j = 0; j < nCols-1; j++)
					{
						sw.Write(mat[i][j].ToString());
						sw.Write("\t");
					}
					sw.WriteLine(mat[i][nCols-1].ToString());
				}
			}
		}


		/// <summary>
		/// Save a row (dim=0) or a column (dim=1) of a matrix.  'idx' indexes which slice to take.
		/// </summary>
		/// <param name="mat"></param>
		/// <param name="dim"></param>
		/// <param name="idx"></param>
		/// <param name="fname"></param>
		public static void SaveSlice(float[][] mat, int dim, int idx, string fname)
		{
#if NO_ZSTREAM
			using (StreamWriter sw = new StreamWriter(fname))
#else
			using (StreamWriter sw = ZStreamWriter.Open(fname))
#endif
			{
				if (dim == 0)
				{
					for (int j = 0; j < mat[0].Length; j++)
					{
						sw.WriteLine(mat[idx][j].ToString("R"));
					}
				}
				else if (dim == 1)
				{
					for (int i = 0; i < mat.Length; i++)
					{
						sw.WriteLine(mat[i][idx].ToString("R"));
					}
				}
			}
		}


		[Obsolete("Use the version that takes a Random, constructed with the desired seed.")]
		public static void Shuffle(int[] vec, int seed)
		{
			Shuffle(vec, new Random(seed));
		}
		public static void Shuffle(int[] vec)
		{
#if ALLOW_RANDOM
			Shuffle(vec, new Random());
#else
			throw new Exception("Random initialized to timer!");
#endif
		}
		public static void Shuffle(int[] vec, Random rand)
		{
			for (int i = 0; i < vec.Length; i++)
			{
				int ranIdx = rand.Next(vec.Length);
				if (ranIdx == i)  continue;
				int tmp = vec[i];
				vec[i] = vec[ranIdx];
				vec[ranIdx] = tmp;
			}
		}

		[Obsolete("Use the version that takes a Random, constructed with the desired seed.")]
		public static void ShuffleRows(int[][] mat, int seed)
		{
			ShuffleRows(mat, new Random(seed));
		}
		public static void ShuffleRows(int[][] mat)
		{
#if ALLOW_RANDOM
			ShuffleRows(mat, new Random());
#else
			throw new Exception("Random initialized to timer!");
#endif
		}
		public static void ShuffleRows(int[][] mat, Random rand)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				int ranIdx = rand.Next(mat.Length);
				if (ranIdx == i)  continue;
				// shuffle row:
				int[] tmp = mat[i];
				mat[i] = mat[ranIdx];
				mat[ranIdx] = tmp;
				// shuffle item by item:
				//for (int j = 0; j < mat[i].Length; j++)
				//{
				//	int tmp = mat[i][j];
				//	mat[i][j] = mat[ranIdx][j];
				//	mat[ranIdx][j] = tmp;
				//}
			}
		}

		[Obsolete("Use the version that takes a Random, constructed with the desired seed.")]
		public static void ShuffleRows(double[][] mat, int seed)
		{
			ShuffleRows(mat, new Random(seed));
		}
		public static void ShuffleRows(double[][] mat)
		{
#if ALLOW_RANDOM
			ShuffleRows(mat, new Random());
#else
			throw new Exception("Random initialized to timer!");
#endif
		}
		public static void ShuffleRows(double[][] mat, Random rand)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				int ranIdx = rand.Next(mat.Length);
				if (ranIdx == i)  continue;
				// shuffle row:
				double[] tmp = mat[i];
				mat[i] = mat[ranIdx];
				mat[ranIdx] = tmp;
				// shuffle item by item:
				//for (int j = 0; j < mat[i].Length; j++)
				//{
				//	double tmp = mat[i][j];
				//	mat[i][j] = mat[ranIdx][j];
				//	mat[ranIdx][j] = tmp;
				//}
			}
		}

		public static void ShuffleRows(float[][] mat, int seed)
		{
			ShuffleRows(mat, new Random(seed));
		}
		public static void ShuffleRows(float[][] mat)
		{
#if ALLOW_RANDOM
			ShuffleRows(mat, new Random());
#else
			throw new Exception("Random initialized to timer!");
#endif
		}
		public static void ShuffleRows(float[][] mat, Random rand)
		{
			for (int i = 0; i < mat.Length; i++)
			{
				int ranIdx = rand.Next(mat.Length);
				if (ranIdx == i)  continue;
				// shuffle row:
				float[] tmp = mat[i];
				mat[i] = mat[ranIdx];
				mat[ranIdx] = tmp;
				// shuffle item by item:
				//for (int j = 0; j < mat[i].Length; j++)
				//{
				//	double tmp = mat[i][j];
				//	mat[i][j] = mat[ranIdx][j];
				//	mat[ranIdx][j] = tmp;
				//}
			}
		}

		public static void ShuffleDoubleRows(float[][] mat1, double[][] mat2, int seed)
		{
			ShuffleDoubleRows(mat1, mat2, new Random(seed));
		}
		public static void ShuffleDoubleRows(float[][] mat1, double[][] mat2)
		{
#if ALLOW_RANDOM
			ShuffleRows(mat, new Random());
#else
			throw new Exception("Random initialized to timer!");
#endif
		}
		public static void ShuffleDoubleRows(float[][] mat1, double[][] mat2, Random rand)
		{
			if( mat1.Length != mat2.Length) throw new Exception("ShuffleDoubleRows: size mismatch");

			for (int i = 0; i < mat1.Length; i++)
			{
				int ranIdx = rand.Next(mat1.Length);
				if (ranIdx == i)  continue;
				float[] tmp1 = mat1[i];
				mat1[i] = mat1[ranIdx];
				mat1[ranIdx] = tmp1;
				double[] tmp2 = mat2[i];
				mat2[i] = mat2[ranIdx];
				mat2[ranIdx] = tmp2;
			}
		}


		/// <summary>
		/// Compute the sizes of, and offsets of, the blocks, where a block is defined by a contiguous set
		/// of ids.  E.g. idx = [1 1 1 1 2 2 3 3 3] would give ans = [4,0; 2,4; 3,6].  Useful for e.g. files
		/// where a column is a query ID.
		/// </summary>
		/// <param name="ids"></param>
		/// <returns></returns>
		public static int[][] SizesNOffsets(int[] ids)
		{
			ArrayList sizes = new ArrayList();
			ArrayList offsets = new ArrayList();
			int lastID = ids[0];
			int size = 0, offset = 0;
			for(int i=0; i<ids.Length; ++i)
			{
				if(ids[i] == lastID) ++size;
				else
				{
					sizes.Add(size);
					offsets.Add(offset);
					offset = i;
					size = 1;
					lastID = ids[i];
				}
			}
			sizes.Add(size);
			offsets.Add(offset);
		
			int[][] ans = ArrayUtils.IntMatrix(sizes.Count,2);
			for(int i=0; i<sizes.Count; ++i)
			{
				ans[i][0] = (int)sizes[i];
				ans[i][1] = (int)offsets[i];
			}
			return ans;
		}
		public static int[][] SizesNOffsets(string[] ids)
		{
			ArrayList sizes = new ArrayList();
			ArrayList offsets = new ArrayList();
			string lastID = ids[0];
			int size = 0, offset = 0;
			for(int i=0; i<ids.Length; ++i)
			{
				if(ids[i] == lastID) ++size;
				else
				{
					sizes.Add(size);
					offsets.Add(offset);
					offset = i;
					size = 1;
					lastID = ids[i];
				}
			}
			sizes.Add(size);
			offsets.Add(offset);
		
			int[][] ans = ArrayUtils.IntMatrix(sizes.Count,2);
			for(int i=0; i<sizes.Count; ++i)
			{
				ans[i][0] = (int)sizes[i];
				ans[i][1] = (int)offsets[i];
			}
			return ans;
		}

		// Compute Euclidean squared distance between two vectors
		public static double SquareDist(double[] v1, double[] v2)
		{
			double tmp, ans = 0.0;
			int i, nElem = v1.Length;
			if (nElem != v2.Length)
			{
				throw(new IndexOutOfRangeException("SquareDist: vectors have different lengths"));
			}
			for (i=0; i<nElem; i++)
			{
				tmp = v1[i] - v2[i];
				ans += tmp*tmp;
			}

			return ans;
		}

		// Compute Euclidean squared distance between two rows
		public static double SquareDist(double[][] mat, int idx1, int idx2)
		{
			double tmp, ans = 0.0;
			int i, nCol = mat[0].Length;
			for (i=0; i<nCol; i++)
			{
				tmp = mat[idx1][i] - mat[idx2][i]; // Rely on C# bounds check
				ans += tmp*tmp;
			}

			return ans;
		}

		// Compute Euclidean squared distance for every pair of rows
		public static double[][] SquareDistMat(double[][] mat)
		{
			int i, j, k, nRow = mat.Length, nCol = mat[0].Length;
			double[][] ans = DoubleMatrix(nRow, nRow);
			double tmp, tmp2;

			for (i=0; i<nRow-1; i++)
			{
				for (j=i+1; j<nRow; j++)
				{
					tmp2 = 0.0;
					for (k=0; k<nCol; k++)
					{
						tmp = mat[i][k] - mat[j][k];
						tmp2 += tmp*tmp;
					}
					ans[i][j] = tmp2;
					ans[j][i] = tmp2;
				}
			}

			return ans;
		}



		public static double SumSlice(float[][] mat, int dim, int idx)
		{
			double ans = 0.0;
			if (dim == 0)
			{
				for (int j = 0; j < mat[0].Length; j++)
				{
					ans += mat[idx][j];
				}
			}
			else if (dim == 1)
			{
				for (int i = 0; i < mat.Length; i++)
				{
					ans += mat[i][idx];
				}
			}
			return ans;
		}


		public static double Sum(double[][] mat)
		{
			double ans = 0.0;
			int i, j, nRow = mat.Length, nCol = mat[0].Length;
			for (i=0; i<nRow; i++)
			{
				for (j=0; j<nCol; j++)
				{
					ans += mat[i][j];
				}
			}
			return ans;
		}

		public static int Sum(int[] vec)
		{
			int ans = 0;
			for (int i = 0; i < vec.Length; i++)
			{
				ans += vec[i];
			}
			return ans;
		}
		public static int Sum(bool[] vec)
		{
			int ans = 0;
			for (int i = 0; i < vec.Length; i++)
			{
				if (vec[i])  ans++;
			}
			return ans;
		}
		public static int Sum(BitArray vec)
		{
			int ans = 0;
			for (int i = 0; i < vec.Length; i++)
			{
				if (vec[i])  ans++;
			}
			return ans;
		}

		public static double Sum(double[] vec)
		{
			double ans = 0.0;
			int i, nRow = vec.Length;
			for (i=0; i<nRow; i++)
			{
				ans += vec[i];
			}
			return ans;
		}


		/// <summary>
		/// Like array.h narrow.  However, explicitly makes a hard copy.  Assumes that data
		/// is actually square (not jagged).  'Narrow' outputs matrices with same number of dims
		/// as input, 'select' outputs a vector selected from a matrix.
		/// </summary>
		/// <param name="data"></param>
		/// <returns></returns>
		public static float[][] SubNarrowCopy(float[][] data, int nRows, int startRow)
		{
			int nCols = data[0].Length;
			float[][] ans = FloatMatrix(nRows, nCols);
			for(int i=0, k=startRow; i<nRows; ++i, ++k)
			{
				for(int j=0; j<nCols; ++j)
				{
					ans[i][j] = data[k][j];
				}
			}

			return ans;
		}
		public static sbyte[] SubNarrowCopy(sbyte[] data, int nRows, int startRow)
		{
			sbyte[] ans = new sbyte[nRows];
			for(int i=0, k=startRow; i<nRows; ++i, ++k) ans[i] = data[k];
			return ans;
		}
		public static int[] SubNarrowCopy(int[] data, int nRows, int startRow)
		{
			int[] ans = new int[nRows];
			for(int i=0, k=startRow; i<nRows; ++i, ++k) ans[i] = data[k];
			return ans;
		}
		public static float[] SubNarrowCopy(float[] data, int nRows, int startRow)
		{
			float[] ans = new float[nRows];
			for(int i=0, k=startRow; i<nRows; ++i, ++k) ans[i] = data[k];
			return ans;
		}
		public static double[] SubNarrowCopy(double[] data, int nRows, int startRow)
		{
			double[] ans = new double[nRows];
			for(int i=0, k=startRow; i<nRows; ++i, ++k) ans[i] = data[k];
			return ans;
		}
		public static string[] SubNarrowCopy(string[] data, int nRows, int startRow)
		{
			string[] ans = new string[nRows];
			for(int i=0, k=startRow; i<nRows; ++i, ++k) ans[i] = data[k];
			return ans;
		}
		public static string[] SubSelectCopy(string[][] data, int dim, int slice, int size, int start)
		{
			string[] ans = new string[size];
			if(dim == 0) // Select on rows (first dimension)
			{
				for(int i=0; i<size; ++i)
				{
					ans[i] = data[slice][i+start];
				}
			} 
			else if(dim == 1) // Select on cols
			{
				for(int i=0; i<size; ++i)
				{
					ans[i] = data[i+start][slice];
				}
			} 
			else throw new Exception("Illegal dim passed.");

			return ans;
		}

		public static int[] SubSelectCopyToInt(string[][] data, int dim, int slice, int size, int start)
		{
			int[] ans = new int[size];
			if(dim == 0) // Select on rows (first dimension)
			{
				for(int i=0; i<size; ++i)
				{
					ans[i] = int.Parse(data[slice][i+start]);
				}
			} 
			else if(dim == 1) // Select on cols
			{
				for(int i=0; i<size; ++i)
				{
					ans[i] = int.Parse(data[i+start][slice]);
				}
			} 
			else throw new Exception("Illegal dim passed.");

			return ans;
		}
		   

		public static float[][] LoadFloatJagSerial(string fname)
		{
			float[][] mat;
#if NO_ZSTREAM
			using (Stream fs = new FileStream(fname, FileMode.Open))
#else
			using (Stream fs = ZStreamIn.Open(fname))
#endif
			{
				BinaryFormatter deserializer = new BinaryFormatter();
				mat = (float[][])(deserializer.Deserialize(fs));
			}
			return mat;
		}


		//		public static double[][] LoadDoubleJagSerial(string fname)
		//		{
		//			double[][] mat;
		//			using (Stream myFileStream = ZStreamIn.Open(fname))
		//			{
		//				BinaryFormatter deserializer = new BinaryFormatter();
		//				mat = (double[][])(deserializer.Deserialize(myFileStream));
		//			}
		//			return mat;
		//		}


		//		public static RelType[][] LoadRelSerial(string fname)
		//		{
		//			RelType[][] mat;
		//			using (Stream myFileStream = ZStreamIn.Open(fname))
		//			{
		//				BinaryFormatter deserializer = new BinaryFormatter();
		//				mat = (RelType[][])(deserializer.Deserialize(myFileStream));
		//			}
		//			return mat;
		//		}



		public static void SaveFloatJagSerial(float[][] data, string fname) 
		{
#if NO_ZSTREAM
			using (Stream fs = new FileStream(fname, FileMode.Create))
#else
			using (Stream fs = ZStreamOut.Open(fname))
#endif
			{
				BinaryFormatter serializer = new BinaryFormatter();
				serializer.Serialize(fs, data);
			}
		}


		
		#region TSVprocessing
		// Concatenate the objects in an array into a tab-separated list of names.
		public static string ToTSV(float[] vec)
		{
			string ans = null;
			foreach(float val in vec)
				ans += val.ToString("R") + "\t";
			return ans.Remove(ans.Length-1, 1);
		}

		public static string ToTSV(RelType[] vec)
		{
			string ans = null;
			foreach(RelType relation in vec)
				ans += relation.ToString() + "\t";
			return ans.Remove(ans.Length-1, 1);
		}

		public static float[] TSVToFloatArr(string tsv)
		{
			string[] fields = tsv.Split('\t');
			float[] values = new float[fields.Length];
			for(int i=0; i<fields.Length; ++i)
			{
				values[i] = float.Parse(fields[i]);
			}
			return values;
		}
		public static RelType[] TSVToRelTypeArr(string tsv)
		{
			string[] fields = tsv.Split('\t');
			RelType[] values = new RelType[fields.Length];
			for(int i=0; i<fields.Length; ++i)
			{
				values[i] = (RelType)Enum.Parse(typeof(RelType), fields[i]);
			}
			return values;
		}
		#endregion

	}

}

