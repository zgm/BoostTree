// C. Burges, Fall 2006
//
// Please do not submit changes without code review with CBurges.
using System;
using System.Collections;
using System.Text;
using System.Diagnostics;

namespace Microsoft.TMSN
{
    public class FindStepLib
    {
        public bool convex;
        public bool verbose;
        public bool alphaPos = true; // Only search for positive alphas.  If false, only search for negative.  Only applies for non-convex search.
        private const double maxStep = 100; // Max step size, for non-convex search
        public double MaxStep { get { return maxStep; } }
        private Random random;


        public FindStepLib(bool convex, Random random, bool verbose)
        {
            this.convex = convex;
            this.random = random;
            this.verbose = verbose;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="qc1">the scores for all the queries computed from the existing system</param>
        /// <param name="qc2">the score for all the queries computed from the newly added tree</param>
        /// <param name="queryIdxActive">the set of "active queries" that we use to find the optimal combination</param>
        /// <param name="bestMeanNDCGGain"></param>
        /// <returns></returns>
        public double FindStep(QueryCollection qc1, QueryCollection qc2, int[] queryIdxActive, out double bestMeanNDCGGain)
        {
            PairRankedItems.Reset();

            // (queryIdxActive == null) <=> using all the queries in qc1/qc2
            int cActiveQueries = (queryIdxActive == null) ? qc1.NQueries : queryIdxActive.Length;

            if(qc1.Count != qc2.Count)
                throw new Exception("Input files must have same number of rows.");
            if(qc1.NQueries != qc2.NQueries)
                throw new Exception("Input files must have same number of queries.");
           
            if (qc1.NQueries < cActiveQueries)
                throw new Exception("Active queries must be less than all the queries.");
            DCGScorer dcg = new DCGScorer();

            // The relabeling must be done before FillRankedItems is called // REVIEW: ??
            long nPairs; // Only used for debug
            int nDocs;   // ditto
            CountDocsNAllPairs(qc1, qc2, queryIdxActive, out nPairs, out nDocs); // ditto
            int rankedItemCtr = 0;
            //PairRankedItems pri = null;
            int nQueries = 0;
            int nSkippedQueries = 0;
            for (int i = 0; i < cActiveQueries; ++i)
            {
                int qIdx = (queryIdxActive == null) ? i : queryIdxActive[i];
                Query query1 = qc1.queries[qIdx];
                Query query2 = qc2.queries[qIdx];
                // We discard the array itself each time, but the object pointers persist.
                // Also: discard any queries that have maxDCG = 0.
                RankedItem[] thisRankedItems = FillRankedItems(query1, query2, dcg, random);
                if(thisRankedItems != null)
                {
                    FillRanks(thisRankedItems);
                    //pri = FillPairRankedItems(thisRankedItems, convex, maxStep, ref rankedItemCtr);
                    FillPairRankedItems(thisRankedItems, convex, alphaPos, maxStep, ref rankedItemCtr); // This forms a linked list.
                    ++nQueries;
                }
                else
                {
                    ++nSkippedQueries;
                }
            }

            PairRankedItems[] pairRankedItems = PRI_ListToArray();
            if (alphaPos)
            {
                Array.Sort(pairRankedItems, new SortPairRankedItemsIncreasing()); // First value closest to zero, next more positive
            }
            else
            {
                Array.Sort(pairRankedItems, new SortPairRankedItemsDecreasing()); // First value still closest to zero, next more negative
            }
            // Now that we have the sorted values of alpha: compute which global alpha gives best NDCG gain.
            double bestAlpha;
            FindBestAlpha(pairRankedItems, dcg, nQueries, out bestAlpha, out bestMeanNDCGGain);

            if (verbose)
            {
                Console.WriteLine("{0} queries total, {1} skipped queries, {2} docs", nQueries + nSkippedQueries, nSkippedQueries, nDocs);
                Console.WriteLine("Tot. # pairs = {0}, num. pairs in computation = {1}", nPairs, pairRankedItems.Length);
                // For the convex combination, it's tempting to rescale alpha so that the first weight is one.  But this is not always possible:
                // it may need to be -1.
                Console.WriteLine("Best mean NDCG Gain = {0}, best alpha = {1}", bestMeanNDCGGain, bestAlpha);

                // Check that the gain is correct.
                qc1.ComputeNDCGs();
                qc2.ComputeNDCGs();
                double firstFactor = convex ? 1.0 - bestAlpha : 1.0;
                QueryCollection qc = QueryCollection.LinearlyCombine(firstFactor, qc1, bestAlpha, qc2);
                qc.ComputeNDCGs();
                Console.WriteLine("NON-TRUNC: First NDCG = {0:F4}/{1:F4}, second = {2:F4}/{3:F4}, combined = {4:F4}/{5:F4}",
                                  qc1.NonTruncNDCG_pes, qc1.NonTruncNDCG_opt, qc2.NonTruncNDCG_pes, qc2.NonTruncNDCG_opt,
                                  qc.NonTruncNDCG_pes, qc.NonTruncNDCG_opt);
                Console.WriteLine("    TRUNC: First NDCG = {0:F4}/{1:F4}, second = {2:F4}/{3:F4}, combined = {4:F4}/{5:F4}",
                                  qc1.TruncNDCG_pes, qc1.TruncNDCG_opt, qc2.TruncNDCG_pes, qc2.TruncNDCG_opt,
                                  qc.TruncNDCG_pes, qc.TruncNDCG_opt);
            }


            return bestAlpha;
        }


        public double BestCombinedMeanNDCG(QueryCollection qc1, QueryCollection qc2)
        {
            double bestNDCGGain;
            double alpha = FindStep(qc1, qc2, null, out bestNDCGGain);
            QueryCollection qc = QueryCollection.LinearlyCombine(1.0, qc1, alpha, qc2);
            qc.ComputeNDCGs();


            if (verbose)
            {
                // Print all three NDCGs: previous model, current model, combination
                qc1.ComputeNDCGs();
                qc2.ComputeNDCGs();
                Console.WriteLine("Previous model: NDCG = {0:F6}-{1:F6}-{2:F6}, NDCG@{3} = {4:F6}-{5:F6}-{6:F6}",
                                  qc1.NonTruncNDCG_pes, qc1.NonTruncNDCG_mean, qc1.NonTruncNDCG_opt, DCGScorer.truncLevel,
                                  qc1.TruncNDCG_pes, qc1.TruncNDCG_mean, qc1.TruncNDCG_opt);
                Console.WriteLine("Current model: NDCG = {0:F6}-{1:F6}-{2:F6}, NDCG@{3} = {4:F6}-{5:F6}-{6:F6}",
                                  qc2.NonTruncNDCG_pes, qc2.NonTruncNDCG_mean, qc2.NonTruncNDCG_opt, DCGScorer.truncLevel,
                                  qc2.TruncNDCG_pes, qc2.TruncNDCG_mean, qc2.TruncNDCG_opt);
                Console.WriteLine("Combined model: NDCG = {0:F6}-{1:F6}-{2:F6}, NDCG@{3} = {4:F6}-{5:F6}-{6:F6}",
                                  qc.NonTruncNDCG_pes, qc.NonTruncNDCG_mean, qc.NonTruncNDCG_opt, DCGScorer.truncLevel,
                                  qc.TruncNDCG_pes, qc.TruncNDCG_mean, qc.TruncNDCG_opt);
                Console.WriteLine("alpha = {0}", alpha);
            }

            return qc.NonTruncNDCG_mean;
        }


        /// <summary>
        /// query1 and query2 must be the same query (but with different scores).  The urls must be in the same order.  However they need not be sorted.
        /// </summary>
        /// <param name="query1"></param>
        /// <param name="query2"></param>
        /// <param name="dcg"></param>
        /// <param name="truncLevel"></param>
        /// <returns>Null if this query has zero maxDCG.  Else, a rankedItem array, sorted by the scores in query1.</returns>
        static public RankedItem[] FillRankedItems(Query query1, Query query2, DCGScorer scorer, Random ran)

        {
            if(query1.Length != query2.Length)
                throw new Exception("Query length mismatch.");
            if(query1.QueryID != query2.QueryID)
                throw new Exception("Queries have differnt IDs.");
            int length = query1.Length;
            double maxDCG = query1.MaxNonTruncDCG;
            if(maxDCG == 0.0)
                return null;
            RankedItem[] rankedItems = new RankedItem[length];
            double[] scores1 = query1.scores;
            double[] scores2 = query2.scores;
            string QID = query1.QueryID;
            for(int i = 0; i < length; ++i)
            {
                float label = query1.Labels[i];
                if(label != query2.Labels[i])
                    throw new Exception("FillRankedItems: label mismatch.");
                rankedItems[i] = new RankedItem((double)DCGScorer.scoresMap[(int)label] / maxDCG, scores1[i], scores2[i], label);//, QueryID);
            }

            if (rankedItems != null)
            {
                SortNJitter(rankedItems, ran);
            }
            return rankedItems;
        }


        /// <summary>
        /// Add jitter to those items and only those items with duplicate scores, to reduce single crossing points
        /// as alpha sweeps.  However, although this helps a lot, it is not sufficient: consider the case where scores1 = {1,2,3}
        /// and scores2 = {3,2,1}.  They all meet in the middle.  This is fixed in FindBestAlpha.  Algorithmically speaking
        /// this is not needed, but we add jitter here to significantly reduce overall computational cost when the amount of
        /// degeneracy is high.
        ///
        /// Returns with rankedItems sorted by the (jittered) score1's.
        /// </summary>
        /// <param name="rankedItems"></param>
        /// <param name="ran"></param>
        static public void SortNJitterOld(RankedItem[] rankedItems, Random ran)
        {
            // We only have to sort score2 to add jitter, and if needed, we don't need to re-sort.
            double scale = 1e-6;
            for (int i = 0; i < rankedItems.Length; ++i)
            {
                rankedItems[i].score = rankedItems[i].score2;
            }
            Array.Sort(rankedItems);
            for (int i = 0, j = 1; i < rankedItems.Length - 1; ++i, ++j)
            {
                if (rankedItems[i].score2 == rankedItems[j].score2)
                {
                    rankedItems[i].score2 += scale * (ran.NextDouble() - 0.5); // leave 'j' alone for next comparison
                }
            }


            bool needResorting = false;
            for (int i = 0; i < rankedItems.Length; ++i)
            {
                rankedItems[i].score = rankedItems[i].score1;
            }
            Array.Sort(rankedItems);
            for (int i = 0, j = 1; i < rankedItems.Length - 1; ++i, ++j)
            {
                if (rankedItems[i].score1 == rankedItems[j].score1)
                {
                    rankedItems[i].score1 += scale * (ran.NextDouble() - 0.5);
                    rankedItems[i].score = rankedItems[i].score1;
                    needResorting = true;
                }
            }
            if (needResorting)
                Array.Sort(rankedItems);

        }

        /// <summary>
        /// This version jitters everything, in an attempt to prevent any degeneracy.  This also effectively picks a particular ranking
        /// for the NDCG baseline, if there is degeneracy.  The only case where this returns null is when all the scores (before or after or
        /// both) are the same, in which case we simply should return 1.0 for the optimal alpha, since the lines will never cross (and if
        /// all initial scores are zero, chances are we've started with an empty model and should just take the scores of the first trained model,
        /// which here corresponds to scores2).
        /// </summary>
        /// <param name="rankedItems"></param>
        /// <param name="ran"></param>
        static public void SortNJitter(RankedItem[] rankedItems, Random rand)
        {
            double scale = 1e-6;
            double val;
            double max1 = double.NegativeInfinity, min1 = double.PositiveInfinity, max2 = double.NegativeInfinity, min2 = double.PositiveInfinity;
            for (int i = 0; i < rankedItems.Length; ++i)
            {
                val = rankedItems[i].score1;
                if(val > max1)
                    max1 = val;
                if(val < min1)
                    min1 = val;
                val = rankedItems[i].score2;
                if(val > max2)
                    max2 = val;
                if(val < min2)
                    min2 = val;
            }
            
            for (int i = 0; i < rankedItems.Length; ++i)
            {
                val = rankedItems[i].score1;
                double ranVal = (2.0 * rand.NextDouble() - 1.0) * scale;
                double mult = (max1 == min1) ? 1.0 : (max1 - min1); // A very degenerate case, but we must handle it
                if (val == 0.0)
                {
                    rankedItems[i].score1 = mult * ranVal;
                }
                else
                {
                    rankedItems[i].score1 = val * (1.0 + ranVal);
                }

                val = rankedItems[i].score2;
                ranVal = (2.0 * rand.NextDouble() - 1.0) * scale;
                mult = (max2 == min2) ? 1.0 : (max2 - min2); // A very degenerate case, but we must handle it
                if (val == 0.0)
                {
                    rankedItems[i].score2 = mult * ranVal;
                }
                else
                {
                    rankedItems[i].score2 = val * (1.0 + ranVal);
                }

                rankedItems[i].score = rankedItems[i].score1;
            }

            Array.Sort(rankedItems);
        }


        static public void FillRanks(RankedItem[] rankedItems)
        {
            for(int i = 0; i < rankedItems.Length; ++i)
            {
                rankedItems[i].rank = i;
            }
        }


        static public PairRankedItems[] PRI_ListToArray()
        {
            int ctr = 0;
            PairRankedItems priPtr = PairRankedItems.mostRecent;
            while(priPtr != null)
            {
                ++ctr;
                priPtr = priPtr.previous;
            }

            PairRankedItems[] pairRankedItems = new PairRankedItems[ctr];

            ctr = 0;
            priPtr = PairRankedItems.mostRecent;
            while(priPtr != null)
            {
                pairRankedItems[ctr++] = priPtr;
                priPtr = priPtr.previous;
            }

            return pairRankedItems;
        }


        /// <summary>
        /// Find every pair, compute alpha, and store the pair in pairRankedItems.  Items are assumed already ranked.
        /// In fact the convex version (limited to [0,1]) is equivalent to the non-convex version (allowing infinite steps)
        /// (just rescale by 1/(1-alpha)).  However the latter is useful because (1) we can easily control step size and (2)
        /// the old weights are left unchanged.
        /// </summary>
        /// <param name="rankedItems"></param>
        /// <param name="convex"></param>
        /// <param name="maxStep">max step to use for non-convex version.</param>
        /// <param name="ctr"></param>
        /// <returns></returns>
        static public PairRankedItems FillPairRankedItems(RankedItem[] rankedItems, bool convex, bool alphaPos, double maxStep, ref int ctr)
        {
            PairRankedItems pri = null;
            for(int i = 0; i < rankedItems.Length - 1; ++i)
            {
                RankedItem x = rankedItems[i];
                for(int j = i + 1; j < rankedItems.Length; ++j)
                {
                    RankedItem y = rankedItems[j];

                    // See boostingNotes.docx.
                    if(convex)
                    {
                        // The convex combination version: (1-alpha)*score1 + alpha*score2.  Disadvantage: previously computed weights
                        // keep changing.
                        if(x.score2 - x.score1 + y.score1 - y.score2 != 0)
                        {
                            // Note ranks go inversely with score
                            double alpha = ( y.score1 - x.score1 ) / ( x.score2 - x.score1 + y.score1 - y.score2 );
                            if(alpha >= 0.0 && alpha <= 1.0)
                            {
                                pri = new PairRankedItems(x, y);
                                pri.alpha = alpha;
                            }
                        }
                    }
                    else
                    {
                        // The score1 + alpha*score2 version.  Advantage: leaves previously computed weights the same.
                        if(x.score2 != y.score2)
                        {
                            double alpha = ( x.score1 - y.score1 ) / ( y.score2 - x.score2 );
                            // alpha=0 corresponds to original rank order for both convex and non-convex combinations
                            if( (alphaPos && alpha >= 0.0 && alpha <= maxStep) ||
                                (!alphaPos && alpha <= 0.0 && alpha >= -maxStep) )
                            {
                                pri = new PairRankedItems(x, y);
                                pri.alpha = alpha;
                            }
                        }
                    }

                }
            }

            return pri;
        }


        /// <summary>
        /// The only reason we pass both data1 and data2 is to change the labels in the second one, too.
        /// </summary>
        /// <param name="data1"></param>
        /// <param name="data2"></param>
        /// <param name="labelForUnlabeled"></param>
        /// <returns></returns>
        //static public void Relabel(IRankDataCollection data1, IRankDataCollection data2, float labelForUnlabeled)
        //{
        //    IEnumerator ienumData1 = data1.GetEnumerator();
        //    IEnumerator ienumData2 = data2.GetEnumerator();

        //    while(ienumData1.MoveNext())
        //    {
        //        ienumData2.MoveNext();
        //        RankData query1 = (RankData)ienumData1.Current;
        //        RankData query2 = (RankData)ienumData2.Current;
        //        for(int i = 0; i < query1.Labels.Length; ++i)
        //        {
        //            if(query1.Labels[i] == -1)
        //                query1.Labels[i] = labelForUnlabeled;
        //            if(query2.Labels[i] == -1)
        //                query2.Labels[i] = labelForUnlabeled;
        //        }
        //    }
        //}


        /// <summary>
        /// Debug only: how does the total number of pairs compare with the number of pairs used in the computation?
        /// </summary>
        /// <param name="data1"></param>
        /// <param name="data2"></param>
        /// <param name="labelForUnlabeled"></param>
        /// <returns></returns>
        public void CountDocsNAllPairs(QueryCollection qc1, QueryCollection qc2, int[] queryIdxActive, out long nPairs, out int nDocs)
        {            
            nPairs = 0;
            nDocs = 0;

            int cActiveQuery = (queryIdxActive == null) ? qc1.queries.Length : queryIdxActive.Length;

            for (int i = 0; i < cActiveQuery; ++i)
            {
                int qIdx = (queryIdxActive == null) ? i : queryIdxActive[i];
                int tot = qc1.queries[qIdx].Labels.Length;
                Debug.Assert(tot == qc2.queries[qIdx].Labels.Length, "query collection size mismatch");
                nDocs += tot;
                nPairs += ( tot * ( tot - 1 ) ) / 2;
            }
        }


        /// <summary>
        /// Loop through every alpha.  If both labels are the same, just swap ranks (and no change to NDCG).
        /// If not, still swap ranks, and compute cumulative delta NDCG.  Keep track of that alpha that gave the
        /// best NDCG.  Also treat as a special case alpha=0 (which may give the best result, and which may not
        /// be one of the listed alphas, since those always correspond to swapping points).
        /// </summary>
        /// <param name="pairRankedItems">Assumed sorted by alpha, with the value closest to zero first. WARNING: SIDE EFFECTS on RankedItems.</param>
        /// <param name="dcg"></param>
        /// <param name="bestAlpha"></param>
        /// <param name="bestNDCGGain"></param>
        void FindBestAlpha(PairRankedItems[] pairRankedItems, DCGScorer scorer, int nQueries, out double bestAlpha, out double bestMeanNDCGGain)
        {
            bestAlpha = 0.0;
            double bestNDCGGain = 0.0;
            int bestIndex = 0;
            double NDCGGain = 0.0; // This really is a gain in a gain (again)
            double[] markups = DCGScorer.discounts; // Position dependent part of NDCG


            // Rely on jittering to take care of degeneracy.
            int loopLength = pairRankedItems.Length;
            //int degCtr = 0;
            //while (loopLength != 0)
            //{
            for (int i = 0; i < loopLength; ++i)
            {
                PairRankedItems pairRankedItem = pairRankedItems[i];
                RankedItem x = pairRankedItem.item1;
                RankedItem y = pairRankedItem.item2;
                int rankx = x.rank;
                int ranky = y.rank;
                if (rankx != ranky + 1 && rankx != ranky - 1)
                {
                    throw new Exception("FindBestAlpha: degenerate scores encountered.");
                    //pairRankedItems[degCtr++] = pairRankedItem;
                    //Console.WriteLine("Warning: we've hit a degenerate pair: QueryID {0}", x.QueryID);
                    //Console.WriteLine("QueryID: {0} s1_1 {1} s1_2 {2} s2_1 {3} s2_2 {4} rank1 {5} rank2 {6} crossing error...", x.QueryID, x.score1, y.score1, x.score2, y.score2, rankx, ranky);
                }
                else
                {
                    if (x.label != y.label)
                    {
                        double ndcgx = x.ndcgWt;
                        double ndcgy = y.ndcgWt;
                        double markupx = markups[rankx];
                        double markupy = markups[ranky];
                        NDCGGain += (ndcgx - ndcgy) * (markupy - markupx);
                        if (NDCGGain > bestNDCGGain)
                        {
                            bestNDCGGain = NDCGGain;
                            bestIndex = i;
                        }
                    }

                    // Positions swap only if in the open interval (not at the edges), otherwise could get a spurious gain
                    if ((convex && pairRankedItem.alpha != 0.0 && pairRankedItem.alpha != 1.0) ||
                         (!convex && pairRankedItem.alpha != 0.0 && pairRankedItem.alpha < maxStep && pairRankedItem.alpha > -maxStep))
                    {
                        x.rank = ranky;
                        y.rank = rankx;
                    }
                }
            }

//				if(degCtr > 0)
//					Console.WriteLine("Num degenerates = {0}", degCtr);
//				loopLength = degCtr;
//				degCtr = 0;
//			}

            // Put the best alpha half way between that found (which is on the border) and the next, unless it's the last.
            if(bestIndex < pairRankedItems.Length - 1)
                bestAlpha = 0.5 * ( pairRankedItems[bestIndex].alpha + pairRankedItems[bestIndex + 1].alpha );
            else if(bestIndex == pairRankedItems.Length - 1)
                bestAlpha = pairRankedItems[bestIndex].alpha;
            else // The passed pairRankedItems array could be empty.
                bestAlpha = 0.0;

            bestMeanNDCGGain = bestNDCGGain / (double)nQueries;
        }
    }
}
