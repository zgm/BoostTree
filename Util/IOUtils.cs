// High level input/output methods
// Chris Burges, (c) Microsoft Corporation 2004
using System;
using System.Collections;
using System.IO;
using Microsoft.TMSN.IO;

namespace Microsoft.TMSN
{
	/// <summary>
	/// Summary description for Class1.
	/// </summary>
	public class IOUtils
	{
        /// <summary>
        /// Check if a bunch of files exists.  fnames of null are allowed (and denote 'ignore').
        /// </summary>
        /// <param name="fnames"></param>
        /// <returns></returns>
        public static bool CheckFilesExist(string[] fnames)
        {
            bool allFilesExist = true;
            for (int i = 0; i < fnames.Length; ++i)
            {
                string fname = fnames[i];
                if (fname != null)
                {
                    if (!File.Exists(fname))
                    {
                        Console.WriteLine("WARNING: File " + fname + " not found.");
                        allFilesExist = false;
                    }
                }
            }

            return allFilesExist;
        }

        /// <summary>
        /// Count the number of columns in the first row, whitespace delimited.
        /// </summary>
        /// <param name="fname"></param>
        /// <returns></returns>
        public static int CountColsFirstRow(string fname)
		{
			using (StreamReader sr = ZStreamReader.Open(fname))
			{
				string line = sr.ReadLine();
				if (line == null)  return 0;
				return line.Split().Length;
			}
		}


		public static int[] CountRows(string fname)
		{
			using (StreamReader sr = ZStreamReader.Open(fname))
			{
				string line;
				while ((line = sr.ReadLine()) != null && line.Length == 0) ;
				if (line == null)  return new int[] {0, 0};
				// allow "@" to start a size line
				if (line[0] == '@')
				{
					line = line.Substring(1).Trim();
					string[] fields = line.Split();
					if (fields.Length > 1 && fields[1].ToLower() == "b")
					{
						return new int[] { int.Parse(fields[0]), 1 };
					}
					else
					{
						return new int[] { int.Parse(fields[0]) };
					}
				}

				int nRows = 1;
				while ((line = sr.ReadLine()) != null)
				{
					nRows++;
				}
				return new int[] { nRows };
			}
		}

		/// <summary>
		/// Count number of rows, and the number of columns in the first row, in a file.  Assumes whitespace
		/// delimited data. Blank lines are counted!
		/// </summary>
		/// <param name="fname"></param>
		/// <returns>int[2]: first element is # rows, second is # cols</returns>
		public static int[] CountRowsNCols(StreamReader sr) 
		{
			string line;
			while ((line = sr.ReadLine()) != null && line.Length == 0) ;
			if (line == null)  return new int[] {0, 0};
			// allow "@" to start a size line
			if (line[0] == '@')
			{
				line = line.Substring(1).Trim();
				string[] fields = line.Split();
				if (fields.Length < 2)  throw new Exception("Malformed matrix file.");
				if (fields.Length > 2 && fields[2].ToLower() == "b")
				{
					if (fields.Length > 3 && fields[3].ToLower() == "c")
					{
						return new int[] { int.Parse(fields[0]), int.Parse(fields[1]), 1 };
					}
					else
					{
						return new int[] { int.Parse(fields[0]), int.Parse(fields[1]), 0 };
					}
				}
				else
				{
					return new int[] { int.Parse(fields[0]), int.Parse(fields[1]) };
				}
			}

			// assume first row has all columns:	
			int nCols = line.Split().Length;
			int nRows = 1;
			while ((line = sr.ReadLine()) != null)
			{
				nRows++;
			}
			return new int[] { nRows, nCols };


		}

		/// <summary>
		/// Count number of rows, and the number of columns in the first row, in a file.  Assumes whitespace
		/// delimited data. Blank lines are counted!
		/// </summary>
		/// <param name="fname"></param>
		/// <returns>int[2]: first element is # rows, second is # cols</returns>
		public static int[] CountRowsNCols(string fname)
		{
			using (StreamReader sr = ZStreamReader.Open(fname))
			{
				return CountRowsNCols(sr);
			}
		}


		/// <summary>
		/// Take a list of feature names (one per line), and create a dummy .ini file from them (every transform is no-op).
		/// </summary>
		/// <param name="fnameIn"></param>
		/// <param name="fnameOut"></param>
		public static void CreateINI(string fnameIn, string fnameOut)
		{
			string[] featureNames = IOUtils.ReadWholeRows(fnameIn);
			int nFtrs = featureNames.Length;
			using (StreamWriter sw = ZStreamWriter.Open(fnameOut))
			{
				sw.WriteLine("Inputs=" + featureNames.Length);
				sw.WriteLine();
				for (int i = 0; i < featureNames.Length; i++)
				{
					sw.WriteLine("[Input:" + (i + 1) + "]");
					sw.WriteLine("Name=" + featureNames[i]);
					sw.WriteLine("Transform=none");
					sw.WriteLine();
				}
			}
		}


		public static string GetCurrentTime()
		{
			string ans = "";
			int hr = DateTime.Now.Hour;
			int mn = DateTime.Now.Minute;
			int sc = DateTime.Now.Second;

			ans = (hr < 10) ? "0" + hr.ToString() : hr.ToString();
			ans += ":";
			ans += (mn < 10) ? "0" + mn.ToString() : mn.ToString();			
			ans += ":";
			ans += (sc < 10) ? "0" + sc.ToString() : sc.ToString();			

			return ans;
		}

		/// <summary>
		/// Compute max and min for each feature.  It is assumed that the first row contains feature names, and that all fields
		/// are separated by tabs.
		/// </summary>
		/// <param name="listFiles">List of files, together with file sizes</param>
		/// <param name="nFiles">Number of files to use to compute the maxes and mins.  May be less than
		/// the number of files listed in listFiles.</param>
		/// <param name="skipTrailingTab">Each row of the MSN Search data has a trailing tab.</param>
		public static void PrintRanges(string listFiles, int nFiles, bool skipTrailingTab)
		{
			float minVal = float.PositiveInfinity;
			float maxVal = float.NegativeInfinity;
			int i, j, nFeatures = 0;
			string[] colNames = new string[0];
			float[] mins = new float[0];
			float[] maxs = new float[0];

			string line1, line2;
			string[] fields1, fields2;
			string filename;
			// Find out how many features.  Allocate space.
			using (StreamReader sr1 = ZStreamReader.Open(listFiles))
			{
				// Use just the first file to get col names and num ftrs
				line1 = sr1.ReadLine();
				if (line1 != null)
				{
					fields1 = line1.Split('\t');
					filename = fields1[0];
					using (StreamReader sr2 = ZStreamReader.Open(filename))
					{
						line2 = sr2.ReadLine();
						if (line2 != null)
						{
							fields2 = line2.Split('\t');
							nFeatures = fields2.Length;
							if (skipTrailingTab) --nFeatures;
							colNames = new string[nFeatures];
							mins = new float[nFeatures];
							maxs = new float[nFeatures];
							for (i=0; i<nFeatures; ++i)
							{
								colNames[i] = fields2[i];
								mins[i] = minVal;
								maxs[i] = maxVal;
							}
						}
					}
				}
			}

			// Start again
			if (nFeatures != 0)
			{
				using (StreamReader sr1 = ZStreamReader.Open(listFiles))
				{
					float val;
					int fileCtr = 0;
					while (((line1 = sr1.ReadLine()) != null) && fileCtr < nFiles)
					{
						fields1 = line1.Split('\t');
						filename = fields1[0];
						Console.WriteLine("Reading file {0}", filename);
						int nDocs = int.Parse(fields1[1]);
						using (StreamReader sr2 = ZStreamReader.Open(filename))
						{
							sr2.ReadLine(); // Skip the line containing feature names
							for (i=0; i<nDocs; ++i)
							{
								line2 = sr2.ReadLine();
								if (line2 == null)
								{
									throw(new ArgumentOutOfRangeException("Stated num docs does not match"));
								}
								fields2 = line2.Split('\t');
								for (j=0; j<nFeatures; ++j)
								{
									val = float.Parse(fields2[j]);
									if (val < mins[j])
									{
										mins[j] = val;
									}
									if (val > maxs[j])
									{
										maxs[j] = val;
									}
								}
							}
						}
						++fileCtr;
					}
				}
			}

			for (i=0; i<nFeatures; ++i)
			{
				Console.WriteLine("{0,-35} {1:F3}  {2:F3}", colNames[i], mins[i], maxs[i]);
			}
		}


		/// <summary>
		/// Populate an array of strings from a file, one row per line.
		/// Blank lines and all-whitespace line are skipped, but lines are not trimmed.
		/// </summary>
		/// <param name="fname">filename</param>
		/// <returns></returns>
		public static string[] ReadWholeRows(string fname)
		{
			ArrayList res = new ArrayList();
			using (StreamReader sr = ZStreamReader.Open(fname))
			{
				string line;
				while ((line = sr.ReadLine()) != null)
				{
					if (line.Trim().Length == 0)  continue;
					res.Add(line);
				}
			}
			return (string[])res.ToArray(typeof(string));
		}

		/// <summary>
		/// Populate an array of strings from a file, one row per line.
		/// Blank lines and all-whitespace line are skipped, but lines are not trimmed.
		/// Lines starting with commentChar are skipped.
		/// </summary>
		/// <param name="fname">filename</param>
		/// <returns></returns>
		public static string[] ReadWholeRows(string fname, char commentChar)
		{
			ArrayList res = new ArrayList();
			using (StreamReader sr = ZStreamReader.Open(fname))
			{
				string line;
				while ((line = sr.ReadLine()) != null)
				{
					string trimmed = line.Trim();
					if (trimmed.Length == 0 || trimmed[0] == commentChar)  continue;
					res.Add(line);
				}
			}
			return (string[])res.ToArray(typeof(string));
		}

		/// <summary>
		/// Populate an array of newline-seperated strings from a string, one row per line.
		/// Blank lines and all-whitespace line are skipped, but lines are not trimmed.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		public static string[] ReadWholeRowsString(string input)
		{
			ArrayList res = new ArrayList();
			using (StringReader sr = new StringReader(input))
			{
				string line;
				while ((line = sr.ReadLine()) != null)
				{
					if (line.Trim().Length == 0)  continue;
					res.Add(line);
				}
			}
			return (string[])res.ToArray(typeof(string));
		}


		public static int[] ReadWholeRowsInt(string input)
		{
			string[] strArr = ReadWholeRows(input);
			int nRows = strArr.Length;
			int[] ans = new int[nRows];
			for(int i=0; i<nRows; ++i)
			{
				ans[i] = int.Parse(strArr[i]);
			}
			return ans;
		}



		// Read first line of file, return string[] of fields.
		public static string[] ReadFirstRow(string fname, char[] separators)
		{
			string[] ans;
			using (StreamReader sr = ZStreamReader.Open(fname))
			{
				string line = sr.ReadLine();
				ans = line.Split(separators);
			}
			return ans;
		}
		/// <summary>
		/// Load an ASCII file.  Fields assumed separated by any of the characters in separators.
		/// Blank lines and all-whitespace line are skipped, but lines are not trimmed.
		/// </summary>
		/// <param name="fname"></param>
		/// <param name="separators"></param>
		/// <returns>An ArrayList, one row per record.</returns>
		[Obsolete("Use ReadSplitRows to get an array back, instead.")]
		public static ArrayList ReadSplitRowsToArr(string fname, char[] separators)
		{
			ArrayList ans = new ArrayList();
			using (StreamReader sr = ZStreamReader.Open(fname))
			{
				string line;
				while ((line = sr.ReadLine()) != null)
				{
					if (line.Trim().Length == 0)  continue;
					ans.Add(line.Split(separators));
				}
			}
			return ans;
		}
		/// <summary>
		/// Load an ASCII file.  Fields assumed separated by any of the characters in separators.
		/// Blank lines and all-whitespace line are skipped, but lines are not trimmed.
		/// </summary>
		/// <param name="fname"></param>
		/// <param name="separators"></param>
		/// <returns></returns>
		public static string[][] ReadSplitRows(string fname, char[] separators)
		{
			ArrayList ans = new ArrayList();
			using (StreamReader sr = ZStreamReader.Open(fname))
			{
				string line;
				while ((line = sr.ReadLine()) != null)
				{
					if (line.Trim().Length == 0)  continue;
					ans.Add(line.Split(separators));
				}
			}
			return (string[][])ans.ToArray(typeof(string[]));
		}
		/// <summary>
		/// Version that skips the first nLinesToSkip.
		/// </summary>
		/// <param name="fname"></param>
		/// <param name="separators"></param>
		/// <param name="nLinesToSkip"></param>
		/// <returns></returns>
		public static string[][] ReadSplitRowsNSkip(string fname, char[] separators, int nLinesToSkip)
		{
			ArrayList ans = new ArrayList();
			using (StreamReader sr = ZStreamReader.Open(fname))
			{
				int lineNum=1;
				string line;
				while ((line = sr.ReadLine()) != null)
				{
					if(lineNum++ > nLinesToSkip)
					{
						if (line.Trim().Length == 0)  continue;
						ans.Add(line.Split(separators));
					}
				}
			}
			return (string[][])ans.ToArray(typeof(string[]));
		}

		/// <summary>
		/// Read ASCII file to float 2 dim array.
		/// </summary>
		/// <param name="fname"></param>
		/// <param name="separators"></param>
		/// <returns></returns>
		public static float[][] ReadSplitRowsToFloat(string fname, char[] separators)
		{
			string[][] strArr = ReadSplitRows(fname, separators);
			int nRows = strArr.Length;
			int nCols = strArr[0].Length;
			float[][] ans = ArrayUtils.FloatMatrix(nRows, nCols);
			for(int i=0; i<nRows; ++i)
			{
				for (int j=0; j<nCols; ++j) 
				{
					ans[i][j] = float.Parse(strArr[i][j]);
				}
			}
			return ans;
		}


		/// <summary>
		/// Load an ASCII file.  Fields assumed separated by any of the characters in separators.
		/// Blank lines and all-whitespace line are skipped, but lines are not trimmed.
		/// </summary>
		/// <param name="fname"></param>
		/// <param name="separator"></param>
		/// <returns></returns>
		public static string[][] ReadSplitRows(string fname, char separator)
		{
			return ReadSplitRows(fname, new char[] { separator });
		}
		/// <summary>
		/// Load an ASCII file.  Fields assumed separated by any of the characters in separators.
		/// Blank lines and all-whitespace line are skipped, but lines are not trimmed.
		/// </summary>
		/// <param name="fname"></param>
		/// <returns></returns>
		[Obsolete("Don't use this: too dangerous for those who forget that it uses all spaces, not just tabs, as separators. Use ReadSplitRows with separators as arg.")]
		public static string[][] ReadSplitRows(string fname)
		{
			ArrayList ans = new ArrayList();
			using (StreamReader sr = ZStreamReader.Open(fname))
			{
				string line;
				while ((line = sr.ReadLine()) != null)
				{
					if (line.Trim().Length == 0)  continue;
					ans.Add(line.Split());
				}
			}
			return (string[][])ans.ToArray(typeof(string[]));
		}


		/// <summary>
		/// Load a subset of the rows, namely fraction 'frac', chosen randomly and uniformly by  the random number generator
		/// initialized with 'seed', from the tab-separated data file 'fname'.
		/// Also only load those columns for which the corresponding element of 'ftrsToKeep' is non-zero.  Skip the first N rows of the
		/// file (e.g. when the first row contains header info).  Return jagged array of the kept rows and cols.  Useful for e.g. computing
		/// bins for features where loading 'fname' would be impossible, or where computing bins for it would be prohibitively slow.
		/// 
		/// The loading is very tolerant of the data in 'fname'.  If a row does not contain enough data so that every 'on' feature flagged in
		/// 'ftrsToKeep' applies, it is skipped, with a warning.  If it contains too much data (i.e. a row whose length exceeds that of 'ftrsToKeep'),
		/// the excess data is just ignored (this to support variable length file formats).  It is assumed that the former case is an error (if a flag
		/// is set, that feature shoudl exist), but the latter is not necessarily (hence no warning is issued).7
		/// </summary>
		/// <param name="fname"></param>
		/// <param name="ftrsToKeep"></param>
		/// <returns></returns>
		public static float[][] SampleFeatureMatrix(string fname, int[] ftrsToKeep, double frac, int nToSkip, int seed)
		{
			// We need to limit memory usage as much as possible, so better to do two passes through the file than use ArrayLists;
			// this has added benefit of checking for sufficient memory up front.
			int[] nRowsNCols = CountRowsNCols(fname);
			int nRows = nRowsNCols[0];

			// Make a vector flagging rows to keep.
			Random ranGen = new Random(seed);
			bool[] keepRows = new bool[nRows];
			int nRowsToKeep = 0;
			for(int i=nToSkip; i<nRows; ++i)
			{
				if(ranGen.NextDouble() < frac) 
				{
					keepRows[i] = true;
					++nRowsToKeep;
				}
			}

			// Allocate...
			int nColsToKeep = 0;
			int maxKeptCount = 0; // Index of the last kept feature, plus one
			for(int i=0; i<ftrsToKeep.Length; ++i)
			{
				if(ftrsToKeep[i] != 0) 
				{
					++nColsToKeep;
					maxKeptCount = i+1;
				}
			}
			float[][] features = ArrayUtils.FloatMatrix(nRowsToKeep, nColsToKeep);

			//... and load.
			using (StreamReader sr = ZStreamReader.Open(fname))
			{
				int rowCtr = 0;
				int colCtr;
				string line;
				for(int i=0; i<nRows; ++i)
				{
					line = sr.ReadLine();
					if(keepRows[i])
					{
						string[] fields = line.Split('\t');
						if(fields.Length >= maxKeptCount)
						{
							float[] row = features[rowCtr++];
							colCtr = 0;
							for(int j=0; j<maxKeptCount; ++j)
							{
								if(ftrsToKeep[j] != 0) row[colCtr++] = float.Parse(fields[j]); // Do we need UINT32 here?
							}
						}
						else Console.WriteLine("Warning: row length smaller than expected according to ftrsToKeep: dropping");
					}
				}
			}

			return features;
		}


		public static void WriteStringsToFile(string[] strArr, string fname)
		{
			using (StreamWriter sw = ZStreamWriter.Open(fname))
			{
				for (int i = 0; i < strArr.Length; i++)
				{
					sw.WriteLine(strArr[i]);
				}
			}
		}
		public static void WriteStringsToFile(string[][] str, string fname)
		{
			using (StreamWriter sw = ZStreamWriter.Open(fname))
			{
				for (int i = 0; i < str.Length; i++)
				{
					if (str[i].Length != 0)
					{
						sw.WriteLine(str[i][0]);
						for (int j = 1; j < str[i].Length; j++)
						{
							sw.Write('\t');
							sw.Write(str[i][j]);
						}
					}
					sw.WriteLine();
				}
			}
		}


		// Write string[] to tab-separated line
		public static void WriteTSLine(string[] line)
		{
			for(int i=0; i<line.Length-1; ++i) Console.Write("{0}\t", line[i]);
			Console.WriteLine(line[line.Length-1]);
		}
		public static void WriteTSLine(string[] line, StreamWriter sw)
		{
			for(int i=0; i<line.Length-1; ++i) sw.Write("{0}\t", line[i]);
			sw.WriteLine(line[line.Length-1]);
		}


		/// <summary>
		/// Print mem usage to stderr
		/// </summary>
		public static void PrintMem()
		{
			double heapSize = GC.GetTotalMemory(true) / (1024.0 * 1024.0);
#if true
			double procSize = System.Diagnostics.Process.GetCurrentProcess().PrivateMemorySize64 / (1024.0 * 1024.0);
#else
			double procSize = System.Diagnostics.Process.GetCurrentProcess().PrivateMemorySize / (1024.0 * 1024.0);
#endif
			Console.Error.WriteLine("Heap: {0}, Process: {1}", heapSize.ToString("0.00"), procSize.ToString("0.00"));
		}
	}
}
