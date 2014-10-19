// C. Burges, Fall 2006
//
// This is a simplified version of RankData, RankDataCollection, etc.  It loads everything into memory.  Written
// because I needed a simple class to manipulate query data in memory. These classes are aimed at loading query data from disk;
// XQuery, on the other hand, are XDAg specific (e.g. they contain an array of AnnotatedSamples, which know about thresholds, which
// is a DAG-specific thing).
// 
// If we were strictly encapsulating, we might want to keep everything re. NDCG separate from this class.  But, it is
// extremely convenient to (1) compute the needed max DGCs at query construct time, and (2) have a method that allows
// a query to compute its own NDCG, or a query collection to compute its mean NDCG. So, here we view NDCG as simply a
// property of a query.
//
// However, note that this code gives slightly different results from the old ranker code (RankData, and IRankDataCollection).
// The latter can introduce errors of about one part in 1e-6 (actually, a max of 9e-7 when tested over a thousand queries) 
// when computing the feature vector with GetFeatureVector, whereas this code loads exactly the floats that were saved.
//
// Note: label remapping is handled in QueryRow (for those queries that use this class), and in Query (those those that don't);
// scoreForDegenerateQuery (an empty query is one with no pairs) is handled in Query, and skipDegenerate (i.e. skip when maxDCG = 0)
// is handled in QueryCollection.
//
// Please do not submit changes without code review with cburges.
// #define USE_BM25_TO_BREAK_DEGENERACY
using System;
using System.IO;
using System.Collections;
using System.Text;
using System.Diagnostics;

namespace Microsoft.TMSN
{

    /// <summary>
    /// Contain all meta header information for queries: finds which column contains the queryID, etc.
    /// Assumes that all meta columns come before all non-meta columns.
    /// </summary>
    public class QueryRowHeader
    {
        static private string metaPrefix = "m:";
        static public string MetaPrefix { get { return metaPrefix; } }
        static private string queryHeader = "m:queryid";
        static public string QueryHeader { get { return queryHeader; } }
        static private string ratingHeader = "m:rating";
        static public string RatingHeader { get { return ratingHeader; } }

#if USE_BM25_TO_BREAK_DEGENERACY       
        static public string BM25Header = "loglinear(bm25f)";
#endif

        static private char separator = '\t';
        static public char Separator { get { return separator; } }

        private string[] m_headers = null;

        private short m_queryIDIdx = -1;
        private short m_ratingIdx = -1;
        private short m_firstFtrIdx = -1;
        private short m_FeatureCount = -1;

#if USE_BM25_TO_BREAK_DEGENERACY
        private short degenBreak_idx = -1;
        public short DegenBreak_idx { get { return degenBreak_idx; } } 
#endif

        public short queryIDIdx { get { return m_queryIDIdx; } }

        public short ratingIdx { get { return m_ratingIdx; } }
        public short firstFtrIdx { get { return m_firstFtrIdx; } }
        public short FeatureCount { get { return m_FeatureCount; } }

        public QueryRowHeader(string[] headers)
        {
            short firstFtrIdx = 0;

            m_headers = headers;
            bool queryIdIdxFound = false;
            bool ratingIdxFound = false;

#if USE_BM25_TO_BREAK_DEGENERACY
            bool bm25FIdxFound = false;
#endif

            for (short i = 0; i < headers.Length; ++i)
            {
                string header = headers[i].ToLower();
                if (header == QueryRowHeader.QueryHeader)
                {
                    m_queryIDIdx = i;
                    queryIdIdxFound = true;
                }
                else if (header == QueryRowHeader.RatingHeader)
                {
                    m_ratingIdx = i;
                    ratingIdxFound = true;
                }

#if USE_BM25_TO_BREAK_DEGENERACY
                else if(header == BM25Header)
                {
                    degenBreak_idx = i;
                    bm25FIdxFound = true;
                }
#endif

                if (header.StartsWith(QueryRowHeader.MetaPrefix))
                    ++firstFtrIdx;
            }
            if (!queryIdIdxFound)
                throw new Exception(QueryRowHeader.QueryHeader + " header missing");
            if(!ratingIdxFound)
                throw new Exception(QueryRowHeader.RatingHeader + " header missing");

#if USE_BM25_TO_BREAK_DEGENERACY
            if (!bm25FIdxFound)
                throw new Exception(QueryRowHeader.BM25Header + " header missing");
#endif

            m_firstFtrIdx = firstFtrIdx;
            m_FeatureCount = (short)((short)headers.Length - firstFtrIdx);

#if USE_BM25_TO_BREAK_DEGENERACY
            degenBreak_idx -= firstFtrIdx;
#endif
        }

        public string ColumnName(int iFeature)
        {
            return m_headers[iFeature + firstFtrIdx];
        }
    }


    /// <summary>
    /// A QueryRow is a simply a row from the features matrix, containing the feature vectors and the label.
    /// As query rows are constructed, a linked list is grown.  This is just a simple way to handle loading
    /// rows when we don't know a priori how many will be loaded.
    /// Note that label remapping is handled here (for those queries that use this class).
    /// </summary>
    public class QueryRow // label and features
    {
        internal QueryRow previous; // automatically construct a linked list.
        static internal QueryRow mostRecent = null;
        private float label;
        public float Label { get { return label; } }
        private float[] features;
        public float[] Features { get { return features; } }


        public QueryRow(float label, float[] features, float labelForUnlabeled)
        {
            this.label = (label == -1) ? labelForUnlabeled : label;
            this.features = features;
            previous = mostRecent;
            mostRecent = this;
        }


        /// <summary>
        /// Generate a random QueryRow, with label chosen uniformly at random according to the 'priors',
        /// and with scores chosen to be (label + scale*uniform(-1,1)).
        /// Labels here always take values in [0,...,nLabels-1] (no 'unlabeled' data).
        /// </summary>
        /// <param name="priors">Assumed in order worst label first</param>
        /// <param name="nDocs"></param>
        /// <returns></returns>
        public QueryRow(float[] priors, double scale, int nFtrs, Random rangen)
        {
            double ran = rangen.NextDouble();
            double cdf = 0.0;
            label = -1; // This is just for the computation - labels here always take values in [0,...,nLabels-1].
            for (int i = 0; i < priors.Length; ++i)
            {
                cdf += priors[i];
                if (ran <= cdf)
                {
                    if (label == -1)
                        label = (float)i;
                    // don't break, so can check the cdf1
                }
            }
            if ((float)cdf != 1.0)
                throw new Exception("priors don't sum to one.");

            features = new float[nFtrs];
            for (int i = 0; i < nFtrs; ++i)
            {
                features[i] = (float)((double)label + scale * (2.0 * rangen.NextDouble() - 1.0));
            }
        }


        static internal void Reset()
        {
            mostRecent = null;
        }
    }
    

    /// <summary>
    /// A Query contains an array of labels, a jagged array of feature vectors, and the query's max DCG values.
    /// As 'Query's are loaded they are added to a linked list of 'Query's.
    /// </summary>
    public class Query
    {
        internal Query previous; // automatically construct a linked list
        static internal Query mostRecent = null;
        public string QueryID;
        private float[] labels; // Could use array of QueryRows, but it's handy to have ftrVectors as a jagged array
        public float[] Labels { get { return labels; } }
        private float[][] ftrVectors; // REVIEW: Don't use for scores any more
        public float[][] FtrVectors { get { return ftrVectors; } }
        public double[] scores;  // Test scores
        public double scoreForDegenerateQuery;
        private double[] scoresCp; // Local copy for sorting
        public int[] ranks; // Ranks starting at zero, when urls ordered by scores.
        
        private int length;
        public int Length { get { return length; } }

        private double maxNonTruncDCG;
        public double MaxNonTruncDCG { get { return maxNonTruncDCG; } }
        private double maxTruncDCG;
        public double MaxTruncDCG { get { return maxTruncDCG; } }

        private double nonTruncNDCG_opt; // Optimistic - same scores given best label ordering
        public double NonTruncNDCG_opt { get { return nonTruncNDCG_opt; } }
        private double truncNDCG_opt;
        public double TruncNDCG_opt { get { return truncNDCG_opt; } }

        private double nonTruncNDCG_pes; // Pessimistic - same scores given worst label ordering
        public double NonTruncNDCG_pes { get { return nonTruncNDCG_pes; } }
        private double truncNDCG_pes;
        public double TruncNDCG_pes { get { return truncNDCG_pes; } }

        private double nonTruncNDCG_mean; // Expectation over uniform random samplings of the docs with same score
        public double NonTruncNDCG_mean { get { return nonTruncNDCG_mean; } }
        private double truncNDCG_mean;
        public double TruncNDCG_mean { get { return truncNDCG_mean; } }


        #region CONSTRUCTORS
        /// <summary>
        /// Assumes all query data has been prepared in the linked list of QueryRows, except features and scores (which don't live
        /// in individual queries).
        /// </summary>
        /// <param name="QID"></param>
        /// <param name="dcg">Can be null if desired (in which case max DCGs won't be computed).</param>
        public Query(string QID, double scoreForDegenerateQuery)
        {
            this.QueryID = QID;
            this.scoreForDegenerateQuery = scoreForDegenerateQuery;
            this.length = 0;
            QueryRow ptr = QueryRow.mostRecent;
            while(ptr != null)
            {
                ++length;
                ptr = ptr.previous;
            }

            ftrVectors = new float[length][];
            scores = new double[length];
            scoresCp = new double[length];
            ranks = new int[length];
            labels = new float[length];
            ptr = QueryRow.mostRecent;
            int ctr = length-1; // stick to original order
            while(ptr != null)
            {
                labels[ctr] = ptr.Label;
                ftrVectors[ctr] = ptr.Features;
                --ctr;
                ptr = ptr.previous;
            }

            // Reset QueryRow
            QueryRow.Reset();

            previous = mostRecent;
            mostRecent = this;

            // Fill DCGs
            DCGScorer dcg = new DCGScorer();
            FillMaxDCGs(dcg);
        }


        /// <summary>
        /// Create a query independently of the QueryRow linked list.  The latter is not changed, if it exists.
        /// Labels corresponding to 'unlabeled' are set to labelForUnlabeled so that max DCGs can be computed.
        /// </summary>
        /// <param name="QID"></param>
        /// <param name="labels"></param>
        /// <param name="ftrVectors"></param>
        public Query(string QID, float[] labels, float[][] ftrVectors, double[] scores, float labelForUnlabeled,
                     double scoreForDegenerateQuery)            
        {
            QueryID = QID;
            this.scoreForDegenerateQuery = scoreForDegenerateQuery;
            this.labels = labels;
            this.ftrVectors = ftrVectors;
            this.scores = scores;
            length = labels.Length;
            scoresCp = new double[length];
            ranks = new int[length];

            // Compute max DCGs
            FixUnlabeledRows(labelForUnlabeled);
            DCGScorer dcg = new DCGScorer();
            FillMaxDCGs(dcg);
        }

        public Query(string QID, float[] labels, float[][] ftrVectors, double[] scores, float labelForUnlabeled,
                     double scoreForDegenerateQuery, bool dcgFlag)
        {
            QueryID = QID;
            this.scoreForDegenerateQuery = scoreForDegenerateQuery;
            this.labels = labels;
            this.ftrVectors = ftrVectors;
            this.scores = scores;
            length = labels.Length;
            scoresCp = new double[length];
            ranks = new int[length];

            // Compute max DCGs
            FixUnlabeledRows(labelForUnlabeled);
            DCGScorer dcg = new DCGScorer();
            if(dcgFlag)
                FillMaxDCGs(dcg);
        }

        /// <summary>
        /// If maxDCG and maxTruncDCG have been passed, it's safe to assume that the labels have already been remapped,
        /// hence labelForUnlabeled is not needed.
        /// </summary>
        /// <param name="QID"></param>
        /// <param name="labels"></param>
        /// <param name="scores"></param>
        /// <param name="maxDCG"></param>
        /// <param name="maxTruncDCG"></param>
        public Query(Query q, float[] labels, float[][] ftrVectors, double[] scores)
        {
            if (ftrVectors.Length != 0 && labels.Length != ftrVectors.Length) // Allow for zero-sized feature vectors (for queries that contain only scores)
                throw new Exception("Query constructor: size mismatch");
            QueryID = q.QueryID;
            this.scoreForDegenerateQuery = q.scoreForDegenerateQuery;
            this.maxNonTruncDCG = q.maxNonTruncDCG;
            this.maxTruncDCG = q.maxTruncDCG;
            this.labels = labels;
            this.ftrVectors = ftrVectors;
            this.scores = scores;
            this.maxNonTruncDCG = q.maxNonTruncDCG;
            this.maxTruncDCG = q.maxTruncDCG;
            length = labels.Length;
        }

        /// <summary>
        /// Generate a random query.  No need to pass labelForUnlabeled here, since the random queries are all given labels.
        /// Still pass scoreForDegenerateQuery just in case some query gets all labels the same.
        /// Scores are not set here.
        /// </summary>
        /// <param name="priors"></param>
        /// <param name="scale"></param>
        /// <param name="nScores"></param>
        /// <param name="rangen"></param>
        /// <param name="nRows"></param>
        public Query(float[] priors, double scale, int nScores, int nRows, string queryID, Random rangen)
        {
            length = nRows;
            QueryID = queryID;

            QueryRow[] qr = new QueryRow[length];
            ftrVectors = new float[length][];
            scores = new double[length];
            labels = new float[length];
            for (int i = 0; i < nRows; ++i)
            {
                qr[i] = new QueryRow(priors, scale, nScores, rangen);
                labels[i] = qr[i].Label;
                ftrVectors[i] = qr[i].Features;
            }

            DCGScorer dcg = new DCGScorer();
            FillMaxDCGs(dcg);
        }
        #endregion // CONSTRUCTORS

        /// <summary>
        /// update the scores corresponding to the query
        /// </summary>
        /// <param name="scores">the input score of query</param>
        /// <param name="idxStart">scores[idxStart, ..., idxStart+length] are the scores to set</param>
        public void UpdateScores(float[] scores, int idxStart)
        {
            for (int i = 0; i < this.length; ++i)
            {
                this.scores[i] = scores[i+idxStart];
            }
        }

        public void FixUnlabeledRows(float labelForUnlabeled)
        {
            for (int i = 0; i < labels.Length; ++i)
            {
                if (labels[i] == -1)
                    labels[i] = labelForUnlabeled;
            }
        }

        public void FillMaxDCGs(DCGScorer dcg)
        {
            // Faster to compute the histogram just once
            int[] hist = new int[dcg.NLabels];
            for (int i = 0; i < labels.Length; ++i)
            {
                ++hist[(int)labels[i]];
            }
            maxNonTruncDCG = dcg.ComputeMaxDCG(hist);
            maxTruncDCG = dcg.ComputeMaxTruncDCG(hist);
        }


        /// <summary>
        /// Compute both truncated and non-truncated NDCG for this query.
        /// </summary>
        /// <returns></returns>
        public void ComputeNDCGs()
        {
            if (maxNonTruncDCG == 0.0) // Then also maxTruncDCG = 0.0, and this query is degenerate.
            { // Note: these may be skipped in the NDCG computation for a bunch of queries - but that is handled in QueryCollection.
                Debug.Assert(maxTruncDCG == 0.0, "maxNonTruncDCG = 0 should imply maxTruncDCG = 0");
                nonTruncNDCG_opt = scoreForDegenerateQuery;
                nonTruncNDCG_pes = scoreForDegenerateQuery;
                nonTruncNDCG_mean = scoreForDegenerateQuery;
                truncNDCG_opt = scoreForDegenerateQuery;
                truncNDCG_pes = scoreForDegenerateQuery;
                truncNDCG_mean = scoreForDegenerateQuery; 
            }
            else
            {
                DCGScorer dcg = new DCGScorer();
                double truncDCG_pes, nonTruncDCG_pes, truncDCG_opt, nonTruncDCG_opt, truncDCG_mean, nonTruncDCG_mean;
                dcg.ComputeDCGs(true, scores, labels, out truncDCG_pes, out nonTruncDCG_pes);
                dcg.ComputeDCGs(false, scores, labels, out truncDCG_opt, out nonTruncDCG_opt);
                dcg.ComputeMeanDCGs(scores, labels, out truncDCG_mean, out nonTruncDCG_mean);
                truncNDCG_pes = truncDCG_pes / maxTruncDCG;
                truncNDCG_opt = truncDCG_opt / maxTruncDCG;
                truncNDCG_mean = truncDCG_mean / maxTruncDCG;
                nonTruncNDCG_pes = nonTruncDCG_pes / maxNonTruncDCG;
                nonTruncNDCG_opt = nonTruncDCG_opt / maxNonTruncDCG;
                nonTruncNDCG_mean = nonTruncDCG_mean / maxNonTruncDCG;
            }
        }

        /// <summary>
        /// The absolute value of the change in NDCG if we swap the current ranks of two data points.  It is assumed that the indices
        /// are of the points ordered by score; this should be done upstream because AbsDeltaNDCG is likely to be called in a loop for the
        /// same query.
        /// </summary>
        /// <param name="idx1">first data point</param>
        /// <param name="idx2">second data point</param>
        /// <returns>the change in NDCG</returns>
        public float AbsDeltaNDCG(int idx1, int idx2)
        {
            double gain1 = DCGScorer.scoresMap[(int)labels[idx1]];
            double gain2 = DCGScorer.scoresMap[(int)labels[idx2]];
            double discount1 = DCGScorer.discounts[ranks[idx1]];
            double discount2 = DCGScorer.discounts[ranks[idx2]];
            double deltaNDCG = ((gain1 - gain2) * (discount1 - discount2)) / maxTruncDCG;
            double absDeltaNDCG = deltaNDCG < 0.0 ? -deltaNDCG : deltaNDCG;

            return (float)absDeltaNDCG;
        }

        //kms: Added for Google Clone training.
        public float AbsDeltaPosition(int idx1, int idx2)
        {
            double gain1 = DCGScorer.scoresMapGoogle[(int)labels[idx1]];
            double gain2 = DCGScorer.scoresMapGoogle[(int)labels[idx2]];
            double discount1 = DCGScorer.discounts[ranks[idx1]];
            double discount2 = DCGScorer.discounts[ranks[idx2]];
            double deltaNDCG = ((discount1 - discount2) * (gain1 - gain2));
            double absDeltaNDCG = deltaNDCG < 0.0 ? -deltaNDCG : deltaNDCG;

            return (float)absDeltaNDCG;
        }

        /// <summary>
        /// Compute the 'ranks' array: indices of the queries sorted by score
        /// </summary>
        public void ComputeRank()
        {
            ArrayUtils.Range(ranks, 0, 1);
            Array.Copy(scores, scoresCp, scores.Length);
            Array.Sort(scoresCp, ranks, new ReverseComparer());

            for (int i = 0; i < ranks.Length; i++)
            {
                scoresCp[ranks[i]] = i;
            }

            for (int i = 0; i < ranks.Length; i++)
            {
                ranks[i] = (int)scoresCp[i];
            }
        }

        public void Save(StreamWriter sw)
        {
            for (int i = 0; i < length; ++i)
            {
                sw.Write("{0}\t{1}", QueryID, labels[i]);
                float[] ftrVector = ftrVectors[i];
                for (int j = 0; j < ftrVector.Length; ++j)
                {
                    sw.Write("\t{0}", ftrVector[j]);
                }
                sw.WriteLine();
            }
        }

        /// <summary>
        /// </summary>
        /// <returns>true if all labels are the same</returns>
        public bool AllRowsSameLabel()
        {
            bool allSame = true;
            for (int i = 1; i < labels.Length; ++i)
            {
                if (labels[i] != labels[i - 1])
                    allSame = false;
            }
            return allSame;
        }


        /// <summary>
        /// Zero out the scores
        /// </summary>
        public void ZeroOutScores()
        {
            for(int i = 0; i < scores.Length; ++i)
            {
                scores[i] = 0.0;
            }
        }


        static internal void Reset()
        {
            mostRecent = null;
        }
    }


    public class QueryCollection : IEnumerable
    {
        public IEnumerator GetEnumerator() // Thanks, 2.0 - but why doesnt' IEnumerable<Query>, IEnumerator<Query> work here?
        {
            for(int i = 0; i < queries.Length; ++i)
            {
                yield return queries[i];
            }
        }

        private double nonTruncNDCG_opt;
        public double NonTruncNDCG_opt { get { return nonTruncNDCG_opt; } }
        private double truncNDCG_opt;
        public double TruncNDCG_opt { get { return truncNDCG_opt; } }

        private double nonTruncNDCG_pes;
        public double NonTruncNDCG_pes { get { return nonTruncNDCG_pes; } }
        private double truncNDCG_pes;
        public double TruncNDCG_pes { get { return truncNDCG_pes; } }

        private double nonTruncNDCG_mean;
        public double NonTruncNDCG_mean { get { return nonTruncNDCG_mean; } }
        private double truncNDCG_mean;
        public double TruncNDCG_mean { get { return truncNDCG_mean; } }


        private int nRows; // Not including the header
        public int Count { get { return nRows; } }  // The total number of rows, for all queries, not including any header
        private int nQueries;
        public int NQueries { get { return nQueries; } } 
        public Query[] queries;

#if USE_BM25_TO_BREAK_DEGENERACY
        private static int degenBreak_idx = -1; // This is only filled when data is read from a tsv file.
        public int DegenBreak_idx { get { return degenBreak_idx; } } 
#endif

        private bool skipDegenerateQueries; // queries that have zero max DCG
        public bool SkipDegenerateQueries { get { return skipDegenerateQueries; } } 
        private double scoreForDegenerateQueries;
        public double ScoreForDegenerateQueries { get { return scoreForDegenerateQueries; } } 

        public short FeatureCount { get { return (short)queries[0].FtrVectors[0].Length; } } // Don't use the global FeatureCount, it may not be up to date


        #region CONSTRUCTORS

        /// <summary>
        /// A degenerate query is one with all the labels the same.
        /// </summary>
        /// <param name="fname"></param>
        /// <param name="skipDegenerate"></param>
        public QueryCollection(string fname, float labelForUnlabeled, bool skipDegenerateQueries, double scoreForDegenerateQuery)
        {
            this.skipDegenerateQueries = skipDegenerateQueries;
            this.scoreForDegenerateQueries = scoreForDegenerateQuery;

            using(StreamReader sr = new StreamReader(fname))
            {				
                string[] headers = sr.ReadLine().Split(QueryRowHeader.Separator);
                QueryRowHeader queryRowHeader = new QueryRowHeader(headers);

#if USE_BM25_TO_BREAK_DEGENERACY
                degenBreak_idx = queryRowHeader.DegenBreak_idx;
#endif
                
                nRows = 0;
                string row = sr.ReadLine();
                string[] splitRow;
                string lastQID = null;
                string QID = null;
                Query.Reset();
                while(row != null)
                {
                    splitRow = row.Split(QueryRowHeader.Separator);
                    QID = splitRow[queryRowHeader.queryIDIdx];
                    if(QID != lastQID && nRows != 0)
                    {
                        new Query(lastQID, scoreForDegenerateQuery);
                        ++nQueries;
                    }

                    string rating = splitRow[queryRowHeader.ratingIdx];
                    float label;
                    switch(rating)
                    {
                        case "Definitive":
                            label = 4;
                            break;
                        case "Perfect":
                            label = 4;
                            break;
                        case "Excellent":
                            label = 3;
                            break;
                        case "Good":
                            label = 2;
                            break;
                        case "Fair":
                            label = 1;
                            break;
                        case "Bad":
                            label = 0;
                            break;
                        case "Detrimental":
                            label = 0;
                            break;
                        case "Unknown":
                            label = -1;
                            break;
                        case "": // Unlabeled (in RatedNRandom)
                            label = -1;
                            break;
                        case "HighlyRelevant": // ImageSearch - happily the only label that overlaps with MSNSearch("Detrimental") gets the same score
                            label = 2;
                            break;
                        case "Relevant":
                            label = 1;
                            break;
                        case "NotRelevant":
                            label = 0;
                            break;
                        case "Unjudged":
                            label = 0;
                            break;
                        default:
                            try
                            {
                                label = float.Parse(rating);
                            }
                            catch(Exception)
                            {
                                Console.WriteLine("Unable to parse rating " + rating + " into an float. Using 0");
                                label = 0;
                            }
                            break;
                    }

                    // Convention is: if row has shorter length than number of headers, the missing values are all zero.
                    //float[] ftrVector = new float[queryRowHeader.FeatureCount];
                    float[] ftrVector = new float[queryRowHeader.FeatureCount];
                    //for(int j = 0, i = queryRowHeader.firstFtrIdx; j < queryRowHeader.FeatureCount; ++i, ++j)
                    for (int j = 0, i = queryRowHeader.firstFtrIdx; i < splitRow.Length; ++i, ++j)
                    {
                        string val = splitRow[i];
                        ftrVector[j] = ( val == String.Empty ) ? 0.0F : float.Parse(val);
                    }
                    new QueryRow(label, ftrVector, labelForUnlabeled);
                    lastQID = QID;
                    row = sr.ReadLine();
                    ++nRows;
                }
                new Query(QID, scoreForDegenerateQuery);
                ++nQueries;
            }

            // Finally construct the array, in the original order
            queries = new Query[nQueries];
            Query ptr = Query.mostRecent;
            int ctr = nQueries-1;
            while(ptr != null)
            {
                queries[ctr--] = ptr;
                ptr = ptr.previous;
            }


            // No need to fix unlabeled rows, since QueryRow does that.  (Any unlabeled rows must be fixed before degenerates are removed).
            FixUnlabeledRows(labelForUnlabeled);

            // For training, for efficiency, we might want to skip queries that have no pairs.
            if (skipDegenerateQueries)
            {
                int nonDegCtr = 0;
                int newNRows = 0;
                for (int i = 0; i < nQueries; ++i)
                {
                    Query q = queries[i];
                    if (!q.AllRowsSameLabel())
                    {
                        queries[nonDegCtr++] = q;
                        newNRows += q.Length;
                    }
                }
                if (nonDegCtr < nQueries)
                {
                    Query[] newQueries = new Query[nonDegCtr]; // wish C# had direct way to shorten array
                    Array.Copy(queries, newQueries, nonDegCtr);
                    nRows = newNRows;
                    nQueries = nonDegCtr;
                    queries = newQueries;
                }
            }
            
            Query.Reset(); // Prepare for next load
        }


        public QueryCollection(Query[] queries, bool skipDegenerateQueries, double scoreForDegenerateQueries)
        {
            this.skipDegenerateQueries = skipDegenerateQueries;
            this.scoreForDegenerateQueries = scoreForDegenerateQueries;
            this.queries = queries;
            nQueries = queries.Length;
            nRows = 0;
            for(int i = 0; i < nQueries; ++i)
            {
                nRows += queries[i].Length;
            }
        }

        /// <summary>
        /// update the scores for all query-url pairs
        /// </summary>
        /// <param name="scores">the scores are in the same quer-url order of the queries</param>
        public void UpdateScores(float[] scores)
        {
            int idxStart = 0;
            for (int i = 0; i < nQueries; ++i)
            {
                queries[i].UpdateScores(scores, idxStart);
                idxStart += queries[i].Length;
            }
        }

        /// <summary>
        /// Generate a random query collection for testing.  The scores are correlated with the labels, and the labels are distributed
        /// according to priors (priors[0] = P(Bad), etc.).
        /// </summary>
        /// <param name="nQueries"></param>
        /// <param name="priors"></param>
        /// <param name="scale"></param>
        /// <param name="nScores"></param>
        /// <param name="nRowsPerQuery"></param>
        /// <param name="rangen"></param>
        public QueryCollection(int nQueries, float[] priors, double scale, int nScores, int nRowsPerQuery, Random rangen)
        {
            this.nQueries = nQueries;
            nRows = nRowsPerQuery * nQueries;
            queries = new Query[nQueries];
            for (int i = 0; i < nQueries; ++i)
            {
                queries[i] = new Query(priors, scale, nScores, nRowsPerQuery, i.ToString(), rangen);
            }
        }

        #endregion // CONSTRUCTORS

        /// <summary>
        /// Labels are hard copied.  Feature vectors are left unallocated. 
        /// Space is allocated for a single column of scores.
        /// </summary>
        /// <param name="qc"></param>
        /// <returns></returns>
        public QueryCollection CopyEmptyQueryCollection()
        {
            Query[] newQueries = new Query[nQueries];
            for (int i = 0; i < nQueries; ++i)
            {
                Query q = queries[i];
                float[][] ftrVectors = ArrayUtils.FloatMatrix(0, 0);
                double[] scores = new double[q.Length];
                float[] newLabels = ArrayUtils.Copy(q.Labels);
                newQueries[i] = new Query(q, newLabels, ftrVectors, scores);
            }

            return new QueryCollection(newQueries, this.skipDegenerateQueries, scoreForDegenerateQueries);
        }


        /// <summary>
        /// Hard copy.
        /// </summary>
        /// <returns></returns>
        public QueryCollection CopyQueryCollection(bool skipDegenerateQueries, double scoreForDegenerateQueries)
        {
            Query[] newQueries = new Query[nQueries];
            for (int i = 0; i < nQueries; ++i)
            {
                Query q = queries[i];
                float[][] ftrVectors = ArrayUtils.Copy(q.FtrVectors);
                double[] scores = ArrayUtils.Copy(q.scores);
                float[] newLabels = ArrayUtils.Copy(q.Labels);
                newQueries[i] = new Query(q, newLabels, ftrVectors, scores);
            }

            return new QueryCollection(newQueries, skipDegenerateQueries, this.scoreForDegenerateQueries);
        }



        public void FixUnlabeledRows(float labelForUnlabeled)
        {
            for(int i = 0; i < nQueries; ++i)
            {
                queries[i].FixUnlabeledRows(labelForUnlabeled);
            }
        }


        /// <summary>
        /// Use one function rather than operators, since it's much more efficient.
        /// </summary>
        /// <param name="alpha"></param>
        /// <param name="qc1"></param>
        /// <param name="beta"></param>
        /// <param name="qc2"></param>
        /// <returns></returns>
        static public QueryCollection LinearlyCombine(double alpha, QueryCollection qc1, double beta, QueryCollection qc2)
        {
            QueryCollection qc = qc1.CopyEmptyQueryCollection();
            for (int i = 0; i < qc.nQueries; ++i)
            {
                double[] scores1 = qc1.queries[i].scores;
                double[] scores2 = qc2.queries[i].scores;
                double[] scoresCombined = qc.queries[i].scores;
                if (scores1.Length != scores2.Length)
                    throw new Exception("LinearlyCombine: size mismatch");
                for (int j = 0; j < scores1.Length; ++j)
                {
                    scoresCombined[j] = alpha * scores1[j] + beta * scores2[j];
                }
            }

            return qc;
        }


        /// <summary>
        /// Same, but replace the scores in qc2 with the results.
        /// </summary>
        /// <param name="alpha"></param>
        /// <param name="qc1"></param>
        /// <param name="beta"></param>
        /// <param name="qc2"></param>
        public static void LinearlyCombineNUpdate(double alpha, QueryCollection qc1, double beta, QueryCollection qc2, QueryCollection qcTarget)
        {
#if DEBUG
            if(qc1.nQueries != qc2.nQueries || qc1.NQueries != qcTarget.NQueries)
                throw new Exception("LinearlyCombineNUpdate: queries size mismatch");
#endif
            for (int iQuery = 0; iQuery < qc1.nQueries; ++iQuery)
            {
                double[] scores1 = qc1.queries[iQuery].scores;
                double[] scores2 = qc2.queries[iQuery].scores;
                double[] scores3 = qcTarget.queries[iQuery].scores;
#if DEBUG
                if (scores1.Length != scores2.Length || scores1.Length != scores3.Length)
                    throw new Exception("LinearlyCombineNUpdate: scores size mismatch");
#endif
                for (int iScore = 0; iScore < scores1.Length; ++iScore)
                {
                    scores3[iScore] = alpha * scores1[iScore] + beta * scores2[iScore];
                }
            }
        }


        /// <summary>
        /// Same, but linearly combine several vectors into one, and increment qcTarget accordingly.
        /// It is up to the calling code to zero out qcTarget if desired.
        /// </summary>
        /// <param name="weights"></param>
        /// <param name="qcArray"></param>
        /// <param name="start">Index of first query to use.</param>
        /// <param name="end">Index of last query to use.</param>
        /// <param name="qc2"></param>
        /// <returns></returns>
        static public void LinearlyCombineNIncrement(double[] weights, QueryCollection[] qcArray, int start, int end, QueryCollection qcTarget)
        {
            if(start > end || start < 0 || end >= qcArray.Length)
                throw new Exception("Illegal indices passed to LinearlyCombine");
            if(weights.Length != qcArray.Length)
                throw new Exception("weights must have same length as qcArray");

            for(int j = start; j <= end; ++j)
            {
                QueryCollection qcSource = qcArray[j];
                double wt = weights[j];
                for(int i = 0; i < qcTarget.nQueries; ++i)
                {
                    double[] scoresSource = qcSource.queries[i].scores;
                    double[] scoresTarget = qcTarget.queries[i].scores;
                    if(scoresSource.Length != scoresTarget.Length)
                        throw new Exception("LinearlyCombine: scores size mismatch");
                    for(int k = 0; k < scoresSource.Length; ++k)
                    {
                        scoresTarget[k] += wt * scoresSource[k];
                    }
                }
            }
        }


        /// <summary>
        /// Zero out the scores
        /// </summary>
        public void ZeroOutScores()
        {
            for(int iQuery = 0; iQuery < nQueries; ++iQuery)
            {
                queries[iQuery].ZeroOutScores();
            }
        }


        /// <summary>
        /// 'scores' must contain a score for every url, in the order in which they are stored in the query collection.
        /// </summary>
        /// <param name="scores"></param>
        public void AssignScores(double[] scores)
        {
            int iScore = 0;
            for (int iQuery = 0; iQuery < nQueries; ++iQuery)
            {
                Query q = queries[iQuery];
                for (int iSample = 0; iSample < q.Length; ++iSample)
                {
                    q.scores[iSample] = scores[iScore];
                    ++iScore;
                }
            }
            if (iScore != Count)
                throw new Exception("AssignScores: size mismatch");
        }


        /// <summary>
        /// Assign scores from the iFtr'th feature.
        /// </summary>
        /// <param name="iFtr"></param>
        public void AssignScoresFromFeature(int iFtr)
        {
            for (int iQuery = 0; iQuery < nQueries; ++iQuery)
            {
                Query q = queries[iQuery];
                for (int iSample = 0; iSample < q.Length; ++iSample)
                {
                    q.scores[iSample] = q.FtrVectors[iSample][iFtr];
                }
            }
        }


        public void AssignScoresFrom(QueryCollection qc)
        {
            if (qc.NQueries != NQueries)
                throw new Exception("QueryCollection.AssignScores: query array length mismatch");
            for (int iQuery = 0; iQuery < NQueries; ++iQuery)
            {
                Query qIn = qc.queries[iQuery];
                Query q = queries[iQuery];
                if (qIn.scores.Length != q.scores.Length)
                    throw new Exception("QueryCollection.AssignScores: query scores length mismatch");
                for (int iScore = 0; iScore < q.Length; ++iScore)
                {
                    q.scores[iScore] = qIn.scores[iScore];
                }
            }
        }


        /// <summary>
        /// Create a random subsample of size nSamples, without replacement.
        /// </summary>
        /// <param name="nSamples"></param>
        /// <param name="rangen"></param>
        /// <returns></returns>
        public QueryCollection Sample(int nSamples, Random rangen)
        {
            if (nSamples > nQueries)
                throw new Exception("nSamples cannot exceed total number of queries.");

            int[] indices = new int[nQueries];
            ArrayUtils.Range(indices, 0, 1);
            float[] vals = new float[nQueries];
            ArrayUtils.Random(vals, 0, 1, rangen);
            Array.Sort(vals, indices);

            Query[] qs = new Query[nSamples];
            for (int i = 0; i < nSamples; ++i)
            {
                qs[i] = queries[indices[i]];
            }

            return new QueryCollection(qs, true, 0.0);
        }


        public void ComputeNDCGs()
        {
            nonTruncNDCG_opt = 0.0;
            truncNDCG_opt = 0.0;
            nonTruncNDCG_pes = 0.0;
            truncNDCG_pes = 0.0;
            nonTruncNDCG_mean = 0.0;
            truncNDCG_mean = 0.0;
            double qCtr = 0.0;
            for(int iQuery = 0; iQuery < queries.Length; ++iQuery)
            {
                Query q = queries[iQuery];
                q.ComputeNDCGs();
                if(q.MaxNonTruncDCG == 0.0)
                {
                    Debug.Assert(q.MaxTruncDCG == 0.0, "ComputeNDCGs: nonTruncNDCG = 0 should imply truncNDCG = 0");
                    if(!skipDegenerateQueries)
                    {
                        nonTruncNDCG_opt += scoreForDegenerateQueries;
                        nonTruncNDCG_pes += scoreForDegenerateQueries;
                        nonTruncNDCG_mean += scoreForDegenerateQueries;
                        truncNDCG_opt += scoreForDegenerateQueries;
                        truncNDCG_pes += scoreForDegenerateQueries;
                        truncNDCG_mean += scoreForDegenerateQueries;
                        ++qCtr;
                    }
                }
                else
                {
                    nonTruncNDCG_opt += q.NonTruncNDCG_opt;
                    nonTruncNDCG_pes += q.NonTruncNDCG_pes;
                    nonTruncNDCG_mean += q.NonTruncNDCG_mean;
                    truncNDCG_opt += q.TruncNDCG_opt;
                    truncNDCG_pes += q.TruncNDCG_pes;
                    truncNDCG_mean += q.TruncNDCG_mean;
                    ++qCtr;
                }
            }
            if(qCtr != 0.0)
            {
                double recipQctr = 1.0/qCtr;
                nonTruncNDCG_opt *= recipQctr;
                truncNDCG_opt *= recipQctr;
                nonTruncNDCG_pes *= recipQctr;
                truncNDCG_pes *= recipQctr;
                nonTruncNDCG_mean *= recipQctr;
                truncNDCG_mean *= recipQctr;
            }
        }


        public void Save(string fname)
        {
            using (StreamWriter sw = new StreamWriter(fname))
            {
                sw.Write("{0}\t{1}", QueryRowHeader.QueryHeader, QueryRowHeader.RatingHeader);
                for (int i = 0; i < FeatureCount; ++i)
                {
                    sw.Write("\tscore{0}", i);
                }
                sw.WriteLine();

                for (int i = 0; i < nQueries; ++i)
                {
                    queries[i].Save(sw);
                }
            }
        }

    }

}
