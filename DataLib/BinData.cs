// This is a new and simpler version of BinarizeData.cs.  It's quite different (e.g. RelType does not have LT or GTE).
// The guiding philosophy is that there are two kinds of value: those which occur much more frequently than you'd
// expect based on the mean histogram bar height, and the rest.  E.g. one feature may be 0 most of the time, and take a
// few other values the rest of the time, and 0 has a special meaning.
//
// The first kind of value is assigned to an EQ bin; the second, to LTE or GT bins.  The latter is only used for the last
// bin, to make sure that values that are larger than what occured in the training set are captured, although it's placed
// amongst the data.  So e.g. the histogram bars ****** might be binned as **|**||**, where the first two relations
// are LTE, and the last, GT.
//
// The idea is that a particular feature value is binarized in two passes: in the first, the equals bins are checked, and in
// the second, the other  bins are checked.  The design decision is that EQ means something quite different from the other bins;
// that is, for example, in the case where you have two EQ bins next to each other, but with a few bars in between, it still makes
// sense to capture the in-between bars with an LTE bin that may lie to the right of the second EQ bin.
//
// Clearly it's important that any code that uses this thresholding scheme test the EQ bins first (actually, it should rely on
// methods in this class to do so).
//
// The code takes one parameter, nBins.
//
// nBins is the requested number of bins; if there are fewer bars than nBins, then each bar gets its own bin.
// Furthermore we allow as many EQ bins as needed, and then attempt to split the remaining bars amongst nBins, so you can get
// more than nBins bins.  The idea is that the EQ bins are just separate and should not impact the bandwith you can apply to the
// remaining data.  Thus, a given feature can wind up with less than nBins bins, nBins bins, or more than nBins bins.
// (Believe me, this is less confusing than what I was doing before.)
//
// If more that 1/4 of all the points fall in one run, then that run is given its own EQ bin.
//
// Why have EQ bins at all, rather than just using LTEs, and one GT, everywhere?  Because some values are special: if you used LTEs
// you'd run the risk of lumping in other values with them.  Thus this algorithm maps every value to 1 in some bin, somewhere, and
// this is assumed by some functions.
//
// Chris Burges, (c) Microsoft 2005
#define DEBUG

using System;
using System.Collections;


namespace Microsoft.TMSN
{

    public enum RelType
    {
        EQ,
        LTE,
        GT,
    }

    public class BinData
    {
        public float[][] allThresholds;
        public RelType[][] allRelations;
        public int nBinaryFeatures;

        /// <summary>
        /// Compute the thresholds and relations from data.
        /// </summary>
        /// <param name="ftrsVecs">Each row is a feature vector.</param>
        /// <param name="nBins">Desired, target num bins per feature.</param>
        public BinData(float[][] ftrVecs, int[] nBins)
        {
#if DEBUG
            if (nBins.Length != ftrVecs[0].Length)
            {
                throw(new ArgumentException("nBins has wrong number of elements"));
            }
#endif
			
            // Every column is treated separately.
            int nPoints = ftrVecs.Length;
            int nFtrs = ftrVecs[0].Length;
            allThresholds = new float[nFtrs][];
            allRelations = new RelType[nFtrs][];
            float[] col = new float[nPoints];
            for (int i=0; i<nFtrs; ++i)
            {
                for (int j=0; j<nPoints; ++j)
                {
                    col[j] = ftrVecs[j][i];
                }
                float[] thresholds;
                RelType[] rel;
                CompThresholds(col, nBins[i], out thresholds, out rel);
                allThresholds[i] = thresholds;
                allRelations[i] = rel;
                //GC.Collect();
            }
            CompNFtrs();
        }

        /// <summary>
        /// Same, but use same nBins for all features
        /// </summary>
        /// <param name="ftrVecs"></param>
        /// <param name="nBins"></param>
        public BinData(float[][] ftrVecs, int nBins)
        {
            // Every column is treated separately.
            int nPoints = ftrVecs.Length;
            int nFtrs = ftrVecs[0].Length;
            allThresholds = new float[nFtrs][];
            allRelations = new RelType[nFtrs][];
            float[] col = new float[nPoints];
            for (int i=0; i<nFtrs; ++i)
            {
                for (int j=0; j<nPoints; ++j)
                {
                    col[j] = ftrVecs[j][i];
                }
                float[] thresholds;
                RelType[] rel;
                CompThresholds(col, nBins, out thresholds, out rel);
                allThresholds[i] = thresholds;
                allRelations[i] = rel;
                //GC.Collect();
            }
            CompNFtrs();
        }


        /// <summary>
        /// Load the thresholds and relations from files.  To be used in test phase (e.g. there's no int[] nBins).
        /// </summary>
        /// <param name="allThresholdsFname"></param>
        /// <param name="allRelationsFname"></param>
        public BinData(string allThresholdsFname, string allRelationsFname)
        {
            allThresholds = ArrayUtils.LoadFloatMat(allThresholdsFname);
            allRelations = ArrayUtils.LoadRelTypeJag(allRelationsFname);
            CompNFtrs();
        }


        /// <summary>
        /// Attempt to place nValsPerBin in each bin.  If no bins result, a warning is issued.
        /// </summary>
        private void AddThresholds(float[][] runs, int nPointsPerBin, ArrayList thresholds, ArrayList relations)
        {
#if DEBUG
            if (nPointsPerBin < 1)
            {
                throw new ArgumentOutOfRangeException("You must request at least one point per bin.");
            }
#endif

            // Last run is always in last bin.
            int counts = 0;
            for (int i = 0; i < runs.Length-1; i++)
            {
                counts += (int)runs[i][0];
                if (counts > nPointsPerBin)
                {
                    float thrsh = 0.5F * (runs[i][2] + runs[i+1][2]);
                    counts = 0;
                    thresholds.Add(thrsh);
                    relations.Add(RelType.LTE);
                }
            }

            if(thresholds.Count == 0 && runs.Length > 1) // Can happen when the very last run is huge, but only if the bin size threshold for EQ is set differently from how it's currently set... so this is just for safety.
            {
                float thrsh = 0.5F * (runs[runs.Length-1][2] + runs[runs.Length-2][2]);
                thresholds.Add(thrsh);
                relations.Add(RelType.LTE);
            }
            // Since we're rounding up, this may have left a small number of guys in the last bin
            // (to the right of the last '<=').  If so, merge with previous bin, unless this is the first bin.
            else if (counts+runs[runs.Length-1][0] < nPointsPerBin/5.0 && thresholds.Count > 1)
            {
                thresholds.RemoveAt(thresholds.Count-1);
                relations.RemoveAt(relations.Count-1);
            }

            // The very last threshold is always both an <=, and a > threshold
            if (thresholds.Count > 0)
            {
                thresholds.Add(thresholds[thresholds.Count-1]);
                relations.Add(RelType.GT);
            }
            else 
            {
                Console.WriteLine("AddThresholds: Warning: no bins added");
            }
        }


        /// <summary>
        /// Map a feature vector to an sbyte vector of {0,1}, based on the thresholds
        /// and types of relations.
        /// </summary>
        /// <param name="ftrVec"></param>
        /// <param name="binFtrVecs"></param>
        public sbyte[] BinFtrVec(float[] ftrVec)
        {
            if (ftrVec.Length != allThresholds.Length || ftrVec.Length != allRelations.Length)
            {
                throw new ArgumentException("allRelations and allThresholds don't match ftrVec length");
            }
            sbyte[] binFtrVec = new sbyte[nBinaryFeatures];

            int start = 0, offset;
            for (int i = 0; i < ftrVec.Length; i++)
            {
                if (allThresholds[i].Length > 1)
                {
                    offset = MapNumToBin(ftrVec[i], allThresholds[i], allRelations[i]);
                    if(offset != -1) binFtrVec[start+offset] = (sbyte)1;
                    start += allThresholds[i].Length;
                }
            }
            return binFtrVec;
        }

        /// <summary>
        /// Map a feature vector to an sbyte vector of {0,1}, based on the thresholds
        /// and types of relations.  Assumes that the passed binFtrVec has the correct
        /// size.
        /// </summary>
        /// <param name="ftrVec"></param>
        /// <param name="binFtrVecs"></param>
        public void BinFtrVec(float[] ftrVec, sbyte[] binFtrVec)
        {

#if DEBUG
            if (ftrVec.Length != allThresholds.Length || ftrVec.Length != allRelations.Length)
            {
                throw new ArgumentException("allRelations and allThresholds don't match ftrVec length");
            }
            if (binFtrVec.Length != nBinaryFeatures) throw new Exception("binFtrVec has wrong size");
#endif

            ArrayUtils.Fill(binFtrVec,0);
            int start = 0, offset;
            for (int i = 0; i < ftrVec.Length; i++)
            {
                offset = MapNumToBin(ftrVec[i], allThresholds[i], allRelations[i]);
                if(offset != -1) binFtrVec[start+offset] = (sbyte)1;
                start += allThresholds[i].Length;
            }
        }

        /// <summary>
        /// Map a matrix of feature vectors to an sbyte vector of {0,1}, based on the thresholds
        /// and types of relations.  Note that some rows of 'thresholds' may be of size 1, which means
        /// that the data used to construct the thresholds took only one value, so that feature will be ignored
        /// (even when in test data it may take more than one value).
        /// </summary>
        /// <param name="ftrVecs"></param>
        /// <param name="allThresholds"></param>
        /// <param name="allRelations"></param>
        /// <param name="binFtrVecs"></param>
        /// <param name="debugLevel"></param>
        public sbyte[][] BinFtrVecs(float[][] ftrVecs)
        {
            int start, offset;
            int nRows = ftrVecs.Length;
            int nFtrs = ftrVecs[0].Length;
            if (nFtrs != allThresholds.Length || nFtrs != allRelations.Length)
            {
                throw new ArgumentException("allRelations and allThresholds don't match ftrVecs length");
            }
            sbyte[][] binFtrVecs = ArrayUtils.SbyteMatrix(nRows, nBinaryFeatures);
            for (int i = 0; i < nRows; i++)
            {
                start = 0;
                float[] ftrVec = ftrVecs[i];
                for (int j = 0; j < nFtrs; j++)
                {
                    if (allThresholds[j].Length > 1)
                    {
                        offset = MapNumToBin(ftrVec[j], allThresholds[j], allRelations[j]);
                        if(offset != -1) binFtrVecs[i][start+offset] = (sbyte)1;
                        start += allThresholds[j].Length;
                    }
                }
            }
            return binFtrVecs;
        }

        public int GetNumBins(int feature)
        {
            return allThresholds[feature].Length;
        }

        /// <summary>
        /// Compute the number of binarized features that a given binarization will generate.  'allRelations'
        /// is passed just to check that it has the correct sizes.  Single threshold features are discarded.
        /// </summary>
        /// <param name="allThresholds"></param>
        /// <param name="allRelations"></param>
        /// <returns></returns>
        private void CompNFtrs()
        {
            nBinaryFeatures = 0;
            for (int i = 0; i < allThresholds.Length; i++)
            {
                if (allThresholds[i].Length > 1)  // Will discard single valued features
                {
                    nBinaryFeatures += allThresholds[i].Length;
                }
#if DEBUG
                if (allRelations[i].Length != allThresholds[i].Length)
                {
                    Console.WriteLine("Row number {0}, allThresholds.Length = {1}, allRelations.Length = {2}",
                        i, allRelations[i].Length, allThresholds[i].Length);
                    throw new ArgumentException("allRelations and allThresholds have different lengths");
                }
#endif
            }
        }


        /// <summary>
        /// NOTE: col is sorted in place - make a copy.  Compute a number of thresholds to
        /// use for binarization.  Attempt to use nBins.  See notes at top of this file, also
        /// headers of the called functions, for the algorithm.  Fills <thresholds> and <relations>,
        /// which are in 1-1 correspondence.
        /// </summary>
        public void CompThresholds(float[] col, int nBins, out float[] thresholds, out RelType[] relations)
        {
            int nPoints = col.Length;
            ArrayList thrsh = new ArrayList();
            ArrayList relns = new ArrayList();
#if DEBUG
            if (nBins<=0 || nBins > nPoints)
            {
                Console.WriteLine("nBins = {0}, nPoints = {1}", nBins, nPoints);
                throw new ArgumentException("nBins: illegal value");
            }
#endif

            Array.Sort(col);
            float[][] runs = ArrayUtils.RunEncode(col);
            //			Console.WriteLine("");
            //			ArrayUtils.Print(col);
            // ArrayUtils.Print(runs);

            // Step (1): The case of fewer runs than requested nBins.
            if (runs.Length <= nBins) AddThrshRlnsForAllRuns(runs, thrsh, relns);
            else
            {
                // Step (2): Populate the EQ bins.  Since the EQ bins should always be checked first, we will always put them first
                // in the list of relations + thresholds.
                bool[] EQRunsFlags = FlagEQRuns(runs);
                int nNonEQRuns = 0;
                for(int i=0; i<runs.Length; ++i)
                {
                    if(EQRunsFlags[i])
                    {
                        thrsh.Add(runs[i][2]);
                        relns.Add(RelType.EQ);
                    }
                    else ++nNonEQRuns;
                }


                if(nNonEQRuns > 0)
                {
                    float[][] truncRuns = new float[nNonEQRuns][];
                    int nTruncRuns=0;
                    for (int i=0; i<runs.Length; ++i)
                    {
                        if (!EQRunsFlags[i]) truncRuns[nTruncRuns++] = runs[i];
                    }
                    AddThrshRelnsForBlocksOfRuns(truncRuns, nBins, thrsh, relns);
                }
            }

#if DEBUG
            if(thrsh.Count != relns.Count) throw new Exception("thresholds, relations have different sizes");
#endif

            thresholds = new float[thrsh.Count];
            relations = new RelType[relns.Count];
            for (int i = 0; i < thrsh.Count; i++)
            {
                thresholds[i] = (float)thrsh[i];
                relations[i] = (RelType)relns[i];
            }

#if DEBUG
            RelType last = relations[relations.Length-1];
            if (last != RelType.EQ && last != RelType.GT) throw new Exception("relations violate GT or EQ at end rule");
            for (int i=0; i<relations.Length-1; ++i)
            {
                if (relations[i] == RelType.GT) throw new Exception("relations violate GT or EQ at end rule");
            }
#endif
        }



        /// <summary>
        /// Map a feature value to the offset in a binary vector (so that the binary vector should be
        /// set to 'one' at that offset).  Note that the EQ relations are always checked first; we conservatively
        /// allow the EQ relations to appear anywhere here, but usually they should be first.  However the LTE
        /// have to be in order.  This function assumes that there is only one GT and that it is at the end.  Note
        /// that every value gets mapped to 1 in some bin, somewhere.
        /// 
        /// It is possible that no conditions in <relations> are met.  If this occurs, return -1, to flag the calling
        /// routine to fill all slots with 0.  However, if this does occur, you need to check why this is happening and
        /// make sure that you want it to happen (e.g. all relations are of type EQ, a value falls in between, and that's OK).
        /// Here's one reason why this is a good idea: if the <relations> and <thresholds> were computed using all the
        /// training data, then if we added an extra condition to handle data _not_ in the training data, the corresponding
        /// weight in a neural net should remain at 0 anyway: otherwise, the effect on the net would be essentially random
        /// for that pattern.
        /// </summary>
        /// <param name="val"></param>
        /// <param name="thresholds"></param>
        /// <param name="relations"></param>
        /// <returns></returns>
        public static int MapNumToBin(float val, float[] thresholds, RelType[] relations)
        {
            int ans = -1;
            bool found = false;

#if DEBUG
            if (relations.Length != thresholds.Length)
            {
                throw new ArgumentOutOfRangeException("Must be same number of thresholds as relations");
            }
#endif

            for (int i=0; i<thresholds.Length; ++i)
            {
                RelType reln = relations[i];
                float thrsh = thresholds[i];
                if (reln == RelType.EQ && val == thrsh)
                {
                    ans = i;
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                for (int i=0; i<thresholds.Length; ++i)
                {
                    RelType reln = relations[i];
                    float thrsh = thresholds[i];
                    if (reln == RelType.LTE && val <= thrsh)
                    {
                        ans = i;
                        found = true;
                        break;
                    }
                }
            }

            if (!found && relations[relations.Length-1] == RelType.GT && val > thresholds[thresholds.Length-1])
            {
                ans = relations.Length-1;
                found = true;
            }

            if (!found)
            {
                // Missed all tests.  Flag as 'set all slots to 0'.
                Console.WriteLine("MapNumToBin: WARNING: No condition met.  Flagging as all zeros.");
                ans = -1;
            }

            return ans;
        }


        /// <summary>
        /// Drawing histograms of very large amounts of data, and comparing against the chosen bins, is frought with peril.
        /// Because of the gross quantization, the histograms and corresponding bins can look completely wrong when in fact
        /// they are correct.  It's much safer to draw the histogram using the computed thresholds and relations, that is,
        /// for each relation, compute a count of the number of examples that fall into the corresponding bin.  When coupled
        /// with the Matlab viewer, this provides a direct visual check that the binning code is working as expected.  Also,
        /// this function is written for streaming use, so you can compute histograms for enormous files.  Thus <counters>
        /// should be initialized to the right size and to all zeros, since it's incremented for each feature vector.
        /// </summary>
        /// <param name="ftrVec"></param>
        /// <param name="counters"></param>
        public void IncrementHist(float[] ftrVec, int[][] counters)
        {
#if DEBUG
            if (counters.Length != allThresholds.Length)
            {
                throw new Exception("MakeHist: <counters> has the wrong size");
            }
            if (ftrVec.Length != allThresholds.Length)
            {
                throw new Exception("MakeHist: <ftrVec> has the wrong size");
            }
#endif
            for (int i=0; i<ftrVec.Length; ++i)
            {
                int binIdx = MapNumToBin(ftrVec[i], allThresholds[i], allRelations[i]);
                if(binIdx != -1) counters[i][binIdx] = counters[i][binIdx] + 1;
            }
        }


        public void Print()
        {

            for (int i=0; i<allThresholds.Length; ++i)
            {
                for (int j=0; j<allThresholds[i].Length; ++j)
                {
                    switch(allRelations[i][j])
                    {
                        case RelType.LTE:
                            Console.Write("<= {0} ", allThresholds[i][j]);
                            break;
                        case RelType.EQ:
                            Console.Write("= {0} ", allThresholds[i][j]);
                            break;
                        case RelType.GT:
                            Console.Write("> {0} ", allThresholds[i][j]);
                            break;
                    }
                }
                Console.WriteLine();
            }
        }


        /// <summary>
        /// Populate the EQ bins.  Since the EQ bins should always be checked first, they should always appear first
        /// in the list of relations.  Algorithm: compute the histogram height assuming data is spread evenly over all nBins bins.
        /// If longest run(s) length is greater than 1/4 total number of points, flag as EQ run.
        /// </summary>
        /// <param name="runs"></param>
        /// <returns></returns>
        public bool[] FlagEQRuns(float[][] runs)
        {
            bool[] EQRunsFlags = new bool[runs.Length];
            int totLen = 0;
            for (int i=0; i<runs.Length; ++i)
                totLen += (int)runs[i][0];
            double barHtThrsh = 0.25*(double)totLen;
            int len;
            if(runs.Length == 1)
            {
                Console.WriteLine("Warning: feature has single (useless) run: attaching EQ bin");
                EQRunsFlags[0] = true;
            }
            else 
            {
                for (int i=0; i<runs.Length; ++i)
                {
                    len = (int)runs[i][0];
                    if(len > barHtThrsh) 
                        EQRunsFlags[i] = true;
                }
            }
            return EQRunsFlags;
        }


        /// <summary>
        /// Called when fewer runs than requested nBins.  If there are zero runs, throw an exception.  If there is
        /// just one run, issue a warning, but attach an EQ bins and proceed.
        /// For more, put a LTE threshold between each histogram bar, and a GT threshold also at the last LTE threshold.
        /// Note that for two runs, we still allot an LTE/GT pair, rather than an EQ, even though
        /// it costs us one more bin, since we want a non-zero value presented to the net for both contingencies, since both
        /// may be informative.  Using just an EQ would present a bunch of values as zeros, and those will not update the weights.
        /// It is assumed elsewhere (e.g. in BinFtrVec) that every feature value gets mapped to one _somewhere_.
        /// </summary>
        /// <param name="runs"></param>
        void AddThrshRlnsForAllRuns(float[][] runs, ArrayList thrsh, ArrayList relns)
        {
            if (runs.Length == 0)
            {
                throw new ArgumentOutOfRangeException("Must have at least one run.");
            }
            else if (runs.Length == 1)
            {
                Console.WriteLine("Warning: feature has single (useless) run: attaching EQ bin");
                thrsh.Add(runs[0][2]);
                relns.Add(RelType.EQ);
            }
            else // Put thresholds between the histogram bars.
            {
                for (int i = 1; i < runs.Length; i++)
                {
                    thrsh.Add(0.5F*(runs[i][2]+runs[i-1][2]));
                    relns.Add(RelType.LTE);
                }
                thrsh.Add(0.5F * (runs[runs.Length-1][2] + runs[runs.Length-2][2]));
                relns.Add(RelType.GT);
            }
        }


        /// <summary>
        /// Put an LTE as equally spaced as possible, but between consecutive bars.  Add a GT at the last LTE.
        /// Assumes EQ runs have been removed.
        /// </summary>
        /// <param name="truncRuns"></param>
        /// <param name="nBins"></param>
        void AddThrshRelnsForBlocksOfRuns(float[][] runs, int nBins, ArrayList thrsh, ArrayList relns)
        {
            int nPoints = 0;
            for (int i=0; i<runs.Length; ++i) nPoints += (int)runs[i][0];

            int nPointsPerBin = (int)Math.Round((double)nPoints/(double)nBins);
            if (nPointsPerBin < 1) nPointsPerBin = 1;

            AddThresholds(runs, nPointsPerBin, thrsh, relns);
        }

    }
}
