using System;
using System.IO;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using Microsoft.TMSN;

namespace StochasticGradientBoost
{     
    public abstract class Metrics
    {
        protected class Result
        {
            public Result(float[][] metrics, int Id)
            {
                this.metrics = (float[][])metrics.Clone();
                for (int i = 0; i < metrics.Length; i++)
                {                                            
                    this.metrics[i] = (float[])metrics[i].Clone();                    
                }
                this.id = Id;
            }

            public float[][] Metrics
            {
                get
                {
                    return metrics;
                }
            }

            public int Id
            {
                get
                {
                    return id;
                }
            }

            float[][] metrics;
            int id;
        }

        public Metrics(LabelFeatureCore labelFeatureCore, DataPartitionType[] dataTypes) : this(labelFeatureCore, null, dataTypes)
        {
        }

        public Metrics(LabelFeatureCore labelFeatureCore, LabelConverter labelConvert, DataPartitionType[] dataTypes)
        {
            this.labelFeatureCore = labelFeatureCore;
            this.dataTypes = dataTypes;
            this.optimalID = -1;

            this.labels = new float[labelFeatureCore.NumDataPoint];

            if (labelConvert != null)
            {
                for (int i = 0; i < labelFeatureCore.NumDataPoint; i++)
                {
                    this.labels[i] = labelConvert.convert(labelFeatureCore.GetLabel(i));
                }
            }
            else
            {
                for (int i = 0; i < labelFeatureCore.NumDataPoint; i++)
                {
                    this.labels[i] = labelFeatureCore.GetLabel(i);
                }
            }

            this.dataSegments = new int[(int)DataPartitionType.cTypes][];

            this.metricsCur = new float[(int)DataPartitionType.cTypes][];            
            for (int i = 0; i < (int)DataPartitionType.cTypes; i++)
            {
                metricsCur[i] = new float[SIZE];
            }

            foreach (DataPartitionType dataType in dataTypes)
            {
                DataSet dataSet = labelFeatureCore.DataGroups.GetDataPartition(dataType);
                int[] dataSegment = dataSet.DataIndex;
                if (dataSegment != null)
                {
                    dataSegments[(int)dataType] = dataSegment;
                }
                else
                {
                    //we will fill in the non-existing data sections with 0
                    //throw new Exception("data partition does not exist");
                }
            }

            this.results_list = new List<Result>();
        }

        //TODO: make the following property abstract
        virtual public int SIZE
        {
            get
            {
                return -1;
            }
        }

        /// <summary>
        /// computes error metrics given the results predicted by a model
        /// </summary>
        /// <param name="scorePredict">the score of each label for each data point predicted by the model</param>
        /// <param name="labelPredict">the label of each data point predicted by the model</param>
        public void ComputeMetrics(float[][] scorePredict, int id, bool IsOptimal)        
        {
            foreach (DataPartitionType dataType in dataTypes)
            {
                ComputeMetrics(this.labels, scorePredict, dataType, metricsCur[(int)dataType]);
            }            
            results_list.Add(new Result(metricsCur, id));

            if (IsOptimal)
            {
                this.optimalID = id;
            }
        }        
        
        public string ResultsStr(int id)
        {
            int i;
            for (i = 0; i < results_list.Count; i++)
            {
                if (id == results_list[i].Id)
                {
                    break;
                }
            }

            if (id < results_list.Count)
            {
                return ResultsStr(results_list[i].Metrics, id);
            }
            else
            {
                return null;
            }
        }

        public float[][] ResultsStrMatrix(int id)
        {
            int i;
            for (i = 0; i < results_list.Count; i++)
            {
                if (id == results_list[i].Id)
                {
                    break;
                }
            }

            //if (id < results_list.Count)
            //{
                return results_list[i].Metrics;
            //}
            //else
            //{
            //    return null;
            //}
        }

        public void SaveAllResults(string fileName)
        {
            StreamWriter stream = new StreamWriter(new FileStream(fileName, FileMode.Create));

            stream.WriteLine(ResultsHeaderStr());
            for (int i = 0; i < results_list.Count; i++)
            {
                stream.WriteLine(ResultsStr(results_list[i].Metrics, results_list[i].Id));
            }

            stream.Close();
        }

        virtual public void SaveScores(string fileName, float[][] scores)
        {
        }

        static protected void SaveProbScores(string fileName, float[][] scores)
        {
            StreamWriter stream = new StreamWriter(new FileStream("Prob.txt", FileMode.Create));
            {
                int j = 0;
                for (j = 0; j < scores.Length - 1; j++)
                {
                    stream.Write("P" + j.ToString() + "\t");
                }
                stream.Write("P" + j.ToString() + "\n");
            }

            int cData = scores[0].Length;

            for (int j = 0; j < cData; j++)
            {
                for (int i = 0; i < scores.Length - 1; i++)
                {
                    stream.Write(scores[i][j]);
                    stream.Write('\t');
                }
                stream.Write(scores[scores.Length - 1][j]); stream.Write('\n');
            }
            stream.Close();
        }

        abstract public string ResultsHeaderStr();

        abstract public int GetBest(DataPartitionType dataType, ref float result);

        abstract protected void ComputeMetrics(float[] labels, float[][] classprob, DataPartitionType dataType, float[] metrics);

        abstract protected string ResultsStr(float[][] metrics, int id);
        
        protected LabelFeatureCore labelFeatureCore;
        protected DataPartitionType[] dataTypes;
        protected int[][] dataSegments;
        protected float[] labels;

        protected float[][] metricsCur;

        protected List<Result> results_list;

        protected int optimalID;
    }

    public class PrecRecall : Metrics    
    {
        protected enum PrecRecallType
        {
            PRECESSION = 0,
            RECALL,
            CLASSERR,
            CTYPES
        }

        //dataType==null <=> all the data are used in one partition
        public PrecRecall(LabelFeatureData labelFeatureData, LabelConverter labelConvert, DataPartitionType[] dataTypes)
            : base(labelFeatureData, labelConvert, dataTypes)
        {                                   
            this.stats = new int[2, 2];
        }

        override public int SIZE
        {
            get
            {
                return (int)PrecRecallType.CTYPES;
            }
        }

        override public string ResultsHeaderStr()
        {            
            return "Iter\t TrainPrec\t TrainRecall\t TrainErr\t ValidPrec\t ValidRecall\t ValidErr\t TestPrec\t TestRecall\t TestErr";
        }

        /// <summary>
        /// Get the optimal metric value and its corresponding iteration ID
        /// </summary>
        /// <param name="dataType">in: the data type we are interested in</param>
        /// <param name="metricType">in: the metric type we are interested in</param>
        /// <param name="result">out: the actual value of the metric</param>
        /// <returns>the iteration Index/ID the produces the minimal of the metricType on dataType</returns>
        override public int GetBest(DataPartitionType dataType, ref float result)
        {            
            //int idMin = results_list[results_list.Count-1].Id;
            //we cannot really compare precison/recall
            return -1;
        }        

        override protected string ResultsStr(float[][] metrics, int id)
        {
            string result = id.ToString();
            if (id == optimalID)
            {
                result += "*";
            }
            result += "\t";
            result += metrics[(int)DataPartitionType.Train][(int)PrecRecallType.PRECESSION].ToString() + "\t";
            result += metrics[(int)DataPartitionType.Train][(int)PrecRecallType.RECALL].ToString() + "\t";
            result += metrics[(int)DataPartitionType.Train][(int)PrecRecallType.CLASSERR].ToString() + "\t";

            result += metrics[(int)DataPartitionType.Validation][(int)PrecRecallType.PRECESSION].ToString() + "\t";
            result += metrics[(int)DataPartitionType.Validation][(int)PrecRecallType.RECALL].ToString() + "\t";
            result += metrics[(int)DataPartitionType.Validation][(int)PrecRecallType.CLASSERR].ToString() + "\t";

            result += metrics[(int)DataPartitionType.Test][(int)PrecRecallType.PRECESSION].ToString() + "\t";
            result += metrics[(int)DataPartitionType.Test][(int)PrecRecallType.RECALL].ToString() + "\t";
            result += metrics[(int)DataPartitionType.Test][(int)PrecRecallType.CLASSERR].ToString();
            return result;
        }

        override protected void ComputeMetrics(float[] labels, float[][] classprob, DataPartitionType dataType, float[] metrics)
        {
            int[] predict = ConvertClassProbToLabels(classprob, 0.5F);
            int[] index = dataSegments[(int)dataType];

            for (int i = 0; i < 2; i++)
            {
                for (int j = 0; j < 2; j++)
                {
                    stats[i, j] = 0;
                }
            }
            
            if (index != null)
            {
                for (int i = 0; i < index.Length; i++)
                {
                    stats[(int)labels[index[i]],predict[index[i]]]++;                    
                }
            }
            else
            {
                for (int i = 0; i < index.Length; i++)
                {
                    stats[(int)labels[i],predict[i]]++;
                }
            }

            metrics[(int)PrecRecallType.PRECESSION] = (float)stats[1, 1] / (float)(stats[0, 1] + stats[1, 1]);
            metrics[(int)PrecRecallType.RECALL] = (float)stats[1, 1] / (float)(stats[1, 0] + stats[1, 1]);
            metrics[2] = (float)(stats[1, 0] + stats[0, 1]) / (float)(stats[0, 0] + stats[0, 1] + stats[1, 0] + stats[1, 1]);
        }

        //covert two class probabilites to predicted labels
        protected static int[] ConvertClassProbToLabels(float[][] classProb, float threshhold)
        {
            int[] symbols = new int[classProb[0].Length];

            int numClass = classProb.Length;
            int numSamples = classProb[0].Length;

            for (int i = 0; i < numSamples; i++)
            {
                if (classProb[1][i] > threshhold)
                {
                    symbols[i] = 1;
                }                
            }
            return symbols;
        }

        override public void SaveScores(string fileName, float[][] scores)
        {
            SaveProbScores(fileName, scores);
        }

        private int[,] stats;        
    }

    public class ClassError : Metrics
    {
        //dataType==null <=> all the data are used in one partition
        public ClassError(LabelFeatureCore labelFeatureCore, LabelConverter labelConvert, DataPartitionType[] dataTypes)
            : base(labelFeatureCore, labelConvert, dataTypes)
        {
        }

        override public int SIZE
        {
            get
            {
                return 1;
            }
        }

        override public string ResultsHeaderStr()
        {
            return "Iter\t TrainError\t ValidError\t TestError";
        }

        /// Get the optimal metric value and its corresponding iteration ID
        /// </summary>
        /// <param name="dataType">in: the data type we are interested in (train/valid/test)</param>
        /// <param name="result">out: the actual value of the metric</param>
        /// <returns>the iteration Index/ID the produces the minimal of the metricType on dataType</returns>
        override public int GetBest(DataPartitionType dataType, ref float result)
        {
            int idMin = -1;
            if (results_list.Count > 0)
            {
                result = results_list[0].Metrics[(int)dataType][0];
                idMin = results_list[0].Id;

                for (int i = 0; i < results_list.Count; i++)
                {
                    if (result > results_list[i].Metrics[(int)dataType][0])
                    {
                        result = results_list[i].Metrics[(int)dataType][0];
                        idMin = i;
                    }
                }
            }
            return idMin;
        }

        override protected string ResultsStr(float[][] metrics, int id)
        {
            string result = id.ToString();
            if (id == optimalID)
            {
                result += "+";
            }
            result += "\t";
            result += metrics[(int)DataPartitionType.Train][0].ToString() + "\t";
            result += metrics[(int)DataPartitionType.Validation][0].ToString() + "\t";
            result += metrics[(int)DataPartitionType.Test][0].ToString();
            return result;
        }

        override protected void ComputeMetrics(float[] labels, float[][] classprob, DataPartitionType dataType, float[] metrics)
        {
            int[] predict = ConvertClassProbToLabels(classprob);

            int[] index = dataSegments[(int)dataType];
            int err = 0;
            double recip = 1.0;
            if (index != null)
            {
                for (int i = 0; i < index.Length; i++)
                {
                    if (labels[index[i]] <= 0 && predict[index[i]] == 0)
                        continue;
                    if (labels[index[i]] != predict[index[i]])
                        err++;
                }
                recip = 1.0 / ((double)index.Length + float.Epsilon);
            }
            else
            {
                for (int i = 0; i < predict.Length; i++)
                {
                    if (labels[i] <= 0 && predict[i] == 0)
                        continue;
                    if (labels[i] != predict[i])
                        err++;
                }
                recip = 1.0 / (double)(predict.Length + float.Epsilon);
            }
            // REVIEW: the following line looks like a bug to me (CJCB): index could be null; and you want to use predict.Length instead anyway,
            // if the 'else' holds
            //metrics[0] = err / (index.Length + float.Epsilon); 
            metrics[0] = (float)(err * recip);
        }

        //covert class probabilites to predicted labels: the function will be move to some other classes
        protected static int[] ConvertClassProbToLabels(float[][] classProb)
        {
            int[] symbols = new int[classProb[0].Length];

            int numClass = classProb.Length;
            int numSamples = classProb[0].Length;

            for (int i = 0; i < numSamples; i++)
            {
                float maxProb = classProb[0][i];
                symbols[i] = 0;
                for (int j = 1; j < numClass; j++)
                {
                    if (maxProb < classProb[j][i])
                    {
                        maxProb = classProb[j][i];
                        symbols[i] = j;
                    }
                }
            }
            return symbols;
        }
   
        override public void SaveScores(string fileName, float[][] scores)
        {
            SaveProbScores(fileName, scores);
        }
        
    }

    public class RankPair
    {
        public RankPair(int idxH, int idxL)
        {
            this.IdxH = idxH;
            this.IdxL = idxL;
        }

        public int IdxH; // index of the data that has higher ranking
        public int IdxL; // index of the data that has lower ranking
        //public int LabelH; // the higher ranking label
        //public int LabelL; // the lower ranking label

        public static double CrossEntropy(float scoreH_minus_scoreL)
        {
            //CrossEntropyCost(2.0) = -log(1+exp(2.0*(scoreL-scoreH)).
            double twice_oL_minus_oH = 2.0 * (-scoreH_minus_scoreL);
            double prob = 1.0 / (1 + Math.Exp(twice_oL_minus_oH));
            double result = -Math.Log(prob);
            return result;
        }

        /// <summary>
        /// Compute the cross-entropy gradient of a pair
        /// </summary>
        /// <param name="scoreH_minus_scoreL">the score of the data that has higher relevancy minus the score of lower relevancy data</param>
        /// <returns></returns>
        public static float CrossEntropyDerivative(float scoreH_minus_scoreL)
        {
            float lambda = 0.0F;

            // Here, lambda is the negative gradient of CrossEntropyCost(2.0) = log(1+exp(2.0*(scoreL-scoreH)).
            float twice_oL_minus_oH = 2.0F * (-scoreH_minus_scoreL);
            if (twice_oL_minus_oH > 0.0)
            {
                lambda = (float)(2.0F / (1.0F + Math.Exp(-twice_oL_minus_oH)));
            }
            else
            {
                double exp_alpha_x = Math.Exp(twice_oL_minus_oH);
                lambda = (float)(2.0F * exp_alpha_x / (1.0F + exp_alpha_x));
            }

            return lambda;

            //double prob = 1.0 / (1 + Math.Exp(twice_oL_minus_oH));
            //lambda = 2.0 * (1 - prob);            

            //this.pseudoResponses[k][i] = this.classLabelsMatrix[k][i] - this.classProb[k][i];
        }

        public static float CrossEntropy2ndDerivative(float scoreH_minus_scoreL)
        {
            float twice_oL_minus_oH = 2.0F * (-scoreH_minus_scoreL);
            float prob = (float)(1.0F / (1.0F + Math.Exp(twice_oL_minus_oH)));
            float weight = 4.0F * prob * (1.0F - prob); // Note that this is always positive!
            //this.weights[k][i] = this.classProb[k][i] * (1 - this.classProb[k][i]);
            return weight;
        }

    }

    public class RankPairGenerator
    {
        public RankPairGenerator(DataGroup dataGroup, float[] rating)
        {
            this.dataGroup = dataGroup;
            this.rating = rating;
        }

        public IEnumerator GetEnumerator()
        {
            int end = dataGroup.iStart + dataGroup.cSize;
            for (int i = dataGroup.iStart; i < end; i++)
            {
                for (int j = i; j < end; j++)
                {
                    if (rating[i] > rating[j])
                    {
                        yield return new RankPair(i, j);
                    }
                    else if (rating[j] > rating[i])
                    {
                        yield return new RankPair(j, i);
                    }
                }
            }
        }

        private DataGroup dataGroup;
        private float[] rating;
    }

    public class NDCG_OLD // REVIEW: need to unify with DCGScorer
    {
        //static float[] defaultScoresMap = new float[5] { 0, 3, 7, 15, 31 };

        public static float[] DefaultScoresMap
        {
            get { return DCGScorer.scoresMap; }
        }

        public NDCG_OLD()
            : this(true, 0.0F)
        {
        }

        //dataType==null <=> all the data are used in one partition
        public NDCG_OLD(bool dropEmptyQueries, float scoreForEmptyQuery)
        {            
            //this.ndcgAt = ndcgAt;
            this.dropEmptyQueries = dropEmptyQueries;  // These should not be hardwired - r
            this.scoreForEmptyQuery = scoreForEmptyQuery;

            //int size = scoresMapIn.Length;			
            //scoresMap = new float[size];
            //Array.Copy(scoresMapIn, 0, scoresMap, 0, size);
            scoresMap = DefaultScoresMap;

            int maxNObjects = 1000;
            rankCoeffs = new float[maxNObjects];
            double numer = Math.Log(2.0);
            for (int i = 0; i < maxNObjects; i++)
            {
                // Note top ranked object has rank 1, not 0
                rankCoeffs[i] = (float)(numer / Math.Log(2 + i));
            }
        }

        public float ComputeNDCG(DataGroup query, float[] labels, float[] scores, int ndcgAt)
        {
            float dcg = ComputeDCG(query, labels, scores, ndcgAt);
            float maxDcg = ComputeMaxDCG(query, labels, scores, ndcgAt);

            float ndcg = 0.0F;
            if (maxDcg == 0.0)
            {
                ndcg = (dropEmptyQueries ? 0.0F : scoreForEmptyQuery); // THis is a bug: if we dropEmptyQueries, we don't count them at all
            }
            else
            {
                ndcg = (dcg / maxDcg);
            }
            return ndcg;
        }

        protected float ComputeDCG(DataGroup query, float[] labels, float[] scores, int ndcgAt)
        {
            float dcg = 0.0F;
            int last = Math.Min(ndcgAt, query.cSize);
            float[] workScores = new float[query.cSize];
            Array.Copy(scores, query.iStart, workScores, 0, query.cSize);

            int[] workLabels = new int[query.cSize];
            Array.Copy(labels, query.iStart, workLabels, 0, query.cSize);
            Array.Sort(workScores, workLabels);

            for (int j = 0; j < last; j++)
            {
                int label = workLabels[query.cSize - j - 1];
                if (label < 0)
                    label = 0;
                dcg += scoresMap[label] * rankCoeffs[j];
            }
            return dcg;
        }

        protected float ComputeMaxDCG(DataGroup query, float[] labels, float[] scores, int ndcgAt)
        {
            float[] workLabels = new float[query.cSize];
            Array.Copy(labels, query.iStart, workLabels, 0, query.cSize);
            Array.Sort(workLabels);

            float maxDCG = 0.0F;
            int last = Math.Min(ndcgAt, query.cSize);
            for (int i = 0; i < last; i++)
            {
                float label = workLabels[query.cSize - i - 1];
                if (label < 0)
                    label = 0;
                maxDCG += scoresMap[(int)label] * rankCoeffs[i];
            }
            return maxDCG;
        }

        private bool dropEmptyQueries;
        private float scoreForEmptyQuery;

        float[] rankCoeffs;
        float[] scoresMap;
    }

    public class NDCG
    {
        public NDCG(bool dropEmptyQueries, float scoreForEmptyQuery, int ndcgAt)
        {            
            this.dropEmptyQueries = dropEmptyQueries;
            this.scoreForEmptyQuery = scoreForEmptyQuery;
            dcgScorer = new DCGScorer();
            DCGScorer.truncLevel = ndcgAt;
        }

        public bool ComputeNDCGs(DataGroup query, float[] labels, float[] scores, out double meanTruncNDCG, out double pessTruncNDCG, out double optiTruncNDCG)
        {
            double meanTruncDCG, pessTruncDCG, optiTruncDCG;
            ComputeTruncDCGs(query, labels, scores, out meanTruncDCG, out pessTruncDCG, out optiTruncDCG);
            double maxTruncDCG;
            bool emptyQuery = ComputeMaxTruncDCG(query, labels, out maxTruncDCG); // REVIEW: this should be done once, in a query constructor

            if (emptyQuery)
            {
                // Note that empty queries, if they are dropped, must be taken care of in the calling code.  However we must assign something here.
                if (dropEmptyQueries)
                {
                    meanTruncNDCG = double.NegativeInfinity;
                    pessTruncNDCG = double.NegativeInfinity;
                    optiTruncNDCG = double.NegativeInfinity;
                }
                else
                {
                    meanTruncNDCG = scoreForEmptyQuery;
                    pessTruncNDCG = scoreForEmptyQuery;
                    optiTruncNDCG = scoreForEmptyQuery;
                }
            }
            else
            {
                double recipMaxTruncDCG = 1.0 / maxTruncDCG; // maxTruncDCG cannot be zero for non-empty queries, unless more than one label is assigned zero gain
                meanTruncNDCG = meanTruncDCG * recipMaxTruncDCG;
                pessTruncNDCG = pessTruncDCG * recipMaxTruncDCG;
                optiTruncNDCG = optiTruncDCG * recipMaxTruncDCG;
            }

            return emptyQuery;
        }

        protected void ComputeTruncDCGs(DataGroup query, float[] labels, float[] scores, out double meanTruncDCG, out double pessTruncDCG, out double optiTruncDCG)
        {
            double[] workScores = new double[query.cSize]; // REVIEW: these should be allocated once, in the query object
            Array.Copy(scores, query.iStart, workScores, 0, query.cSize);
            float[] workLabels = new float[query.cSize];
            Array.Copy(labels, query.iStart, workLabels, 0, query.cSize);

            double nonTruncDCG;
            dcgScorer.ComputeMeanDCGs(workScores, workLabels, out meanTruncDCG, out nonTruncDCG);
            bool pessimistic = true;
            dcgScorer.ComputeDCGs(pessimistic, workScores, workLabels, out pessTruncDCG, out nonTruncDCG);
            pessimistic = false;
            dcgScorer.ComputeDCGs(pessimistic, workScores, workLabels, out optiTruncDCG, out nonTruncDCG);
        }

        // REVIEW: This should be done once per query, in the query constructor
        protected bool ComputeMaxTruncDCG(DataGroup query, float[] labels, out double maxTruncDCG)
        {
            float[] workLabels = new float[query.cSize];
            float firstLabel = labels[query.iStart];
            bool emptyQuery = true;
            for (int iLabelTo = 0, iLabelFrom = query.iStart; iLabelTo < query.cSize; ++iLabelTo, ++iLabelFrom)
            {
                if (labels[iLabelFrom] != firstLabel)
                    emptyQuery = false;
                workLabels[iLabelTo] = labels[iLabelFrom];
            }
            maxTruncDCG = dcgScorer.ComputeMaxTruncDCG(workLabels);

            return emptyQuery;
        }

        // REMOVE WHEN DEBUGGED
        //private AnnotatedScore[] GetAnnScores(DataGroup query, int[] labels, float[] scores, int ndcgAt)
        //{
        //    AnnotatedScore[] annScores = new AnnotatedScore[query.cSize]; // REVIEW: this should be allocated once and stored inside a query object
        //    for (int iLabelTo = 0, iLabelFrom = query.iStart; iLabelTo < query.cSize; ++iLabelFrom, ++iLabelTo)
        //    {
        //        // Assume labels have already been converted: REVIEW: should be done once, in query constructor
        //        annScores[iLabelTo] = new AnnotatedScore(scores[iLabelFrom], (float)labels[iLabelFrom]);
        //    }

        //    return annScores;
        //}

        private bool dropEmptyQueries;
        public bool DropEmptyQueries { get { return dropEmptyQueries; } }
        float scoreForEmptyQuery;
        public float ScoreForEmptyQuery { get { return scoreForEmptyQuery; } }
        DCGScorer dcgScorer;
    }

    public class NDCGMultiClass : Metrics
    {
        protected enum NDCGType
        {
            meanTruncNDCG = 0,
            pessTruncNDCG,
            optiTruncNDCG,
            CLASSERR,
            CTYPES
        }        

        //dataType==null <=> all the data are used in one partition
        public NDCGMultiClass(LabelFeatureCore labelFeatureCore, LabelConverter labelConvert, DataPartitionType[] dataTypes, int ndcgAt,
                              bool dropEmptyQueries, float scoreForEmptyQuery)
            : base(labelFeatureCore, labelConvert, dataTypes)
        {
            this.ndcg = new NDCG(dropEmptyQueries, scoreForEmptyQuery, ndcgAt);
        }

        override public int SIZE
        {
            get
            {
                return (int)NDCGType.CTYPES;
            }
        }

        override public string ResultsHeaderStr()
        {
            return "Iter  ClassErr:Trn/Vld/Tst   TrNDCG:Pess/Mean/Opt   VdNDCG:Pess/Mean/Opt   TsNDCG:Pess/Mean/Opt";
        }

        /// <summary>
        /// Get the optimal metric value and its corresponding iteration ID
        /// </summary>
        /// <param name="dataType">in: the data type we are interested in</param>
        /// <param name="metricType">in: the metric type we are interested in</param>
        /// <param name="result">out: the actual value of the metric</param>
        /// <returns>the iteration Index/ID the produces the minimal of the metricType on dataType</returns>
        override public int GetBest(DataPartitionType dataType, ref float result)
        {
            int idMax = -1;
            if (results_list.Count > 0)
            {
                result = results_list[0].Metrics[(int)dataType][(int)NDCGType.meanTruncNDCG];
                idMax = results_list[0].Id;

                for (int i = 0; i < results_list.Count; i++)
                {
                    if (result < results_list[i].Metrics[(int)dataType][0])
                    {
                        result = results_list[i].Metrics[(int)dataType][0];
                        idMax = i;
                    }
                }
            }
            return idMax;
        }

        override protected string ResultsStr(float[][] metrics, int id)
        {
            //double all = metrics[(int)DataPartitionType.Train][(int)NDCGType.NDCG] * 0.7 + metrics[(int)DataPartitionType.Validation][(int)NDCGType.NDCG] * 0.1 + metrics[(int)DataPartitionType.Test][(int)NDCGType.NDCG] * 0.2;
            string result = id.ToString();
            if (id == optimalID)
            {
                result += "*";
            }
            string fstr = "F4"; // format string
            result += "   "; 
            result += metrics[(int)DataPartitionType.Train][(int)NDCGType.CLASSERR].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)NDCGType.CLASSERR].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)NDCGType.CLASSERR].ToString(fstr) + "   ";

            result += metrics[(int)DataPartitionType.Train][(int)NDCGType.pessTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Train][(int)NDCGType.meanTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Train][(int)NDCGType.optiTruncNDCG].ToString(fstr) + "   ";

            result += metrics[(int)DataPartitionType.Validation][(int)NDCGType.pessTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)NDCGType.meanTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)NDCGType.optiTruncNDCG].ToString(fstr) + "   ";

            result += metrics[(int)DataPartitionType.Test][(int)NDCGType.pessTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)NDCGType.meanTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)NDCGType.optiTruncNDCG].ToString(fstr);

            return result;
        }

        override protected void ComputeMetrics(float[] labels, float[][] classprob, DataPartitionType dataType, float[] metrics)
        {
            //results initialization            
            for (int i = 0; i < metrics.Length; i++)
            {
                metrics[i] = 0;
            }

            //(1) compute error rate                        
            DataSet dataSet = this.labelFeatureCore.DataGroups.GetDataPartition(dataType);
            int[] index = dataSet.DataIndex;
            if (index != null)
            {
                int[] predict = ConvertClassProbToLabels(classprob);
                int err = 0;
                for (int i = 0; i < index.Length; i++)
                {
                    if (labels[index[i]] <= 0 && predict[index[i]] == 0)
                        continue;
                    if (labels[index[i]] != predict[index[i]])
                        err++;
                }
                metrics[(int)NDCGType.CLASSERR] = (float)(err) / (index.Length + float.Epsilon);
            }

            //(2) Compute NDCGs
            int[] groupIndex = dataSet.GroupIndex;
            if (groupIndex != null && groupIndex.Length>0)
            {
                float[] scores = ConvertProbToScore(classprob);

                double totalMeanNDCG = 0;
                double totalPessNDCG = 0;
                double totalOptiNDCG = 0;
                double cQueries = 0;

                //compute the NDCG for each query/group
                double meanTruncNDCG, pessTruncNDCG, optiTruncNDCG;
                for (int i = 0; i < groupIndex.Length; i++)
                {
                    DataGroup query = this.labelFeatureCore.DataGroups[groupIndex[i]];
                    //float ndcg = this.ndcg.ComputeNDCG(query, labels, scores, this.ndcgAt);
                    bool emptyQuery = ndcg.ComputeNDCGs(query, labels, scores, out meanTruncNDCG, out pessTruncNDCG, out optiTruncNDCG);

                    if (!emptyQuery || (emptyQuery && !ndcg.DropEmptyQueries))
                    {
                        totalMeanNDCG += meanTruncNDCG;
                        totalPessNDCG += pessTruncNDCG;
                        totalOptiNDCG += optiTruncNDCG;
                        cQueries++;
                    }
                }
                metrics[(int)NDCGType.meanTruncNDCG] = (float)(totalMeanNDCG / cQueries);
                metrics[(int)NDCGType.pessTruncNDCG] = (float)(totalPessNDCG / cQueries);
                metrics[(int)NDCGType.optiTruncNDCG] = (float)(totalOptiNDCG / cQueries);
            }
        }

        override public void SaveScores(string fileName, float[][] classprob)
        {
            StreamWriter stream = new StreamWriter(new FileStream(fileName, FileMode.Create));

            stream.WriteLine("m:QueryId\t" + "m:Rating\t" + "Score");
            {
                float[] scores = ConvertProbToScore(classprob);

                for (int i = 0; i < this.labelFeatureCore.NumDataPoint; i++)
                {
                    stream.WriteLine(this.labelFeatureCore.GetGroupId(i).ToString() + "\t" + this.labelFeatureCore.GetLabel(i).ToString() + "\t" + scores[i].ToString());
                }
            }
            stream.Close();
        }
      
        //covert class probabilities to score for ranking: the function will be moved to some other class
        protected static float[] ConvertProbToScore(float[][] prob)
        {
            bool considerVariance = false;
            bool considerEntropy = false;
            int numClass = prob.Length;
            int numSamples = prob[0].Length;

            float[] scores = new float[numSamples];

            double maxEntropy = Math.Log(numClass, 2);

            for (int i = 0; i < numSamples; i++)
            {
                if (considerVariance == true)
                {
                    float mean = 0;
                    float mean2 = 0;
                    for (int j = 1; j < numClass; j++)
                    {
                        mean += prob[j][i] * j;
                        mean2 += prob[j][i] * j * j;
                    }
                    // have to minimize the effect of variance, using a small factor (e.g., 1/20)
                    // should we use mean - std ? or mean +std? This is interesting question
                    // Initially I thought mean - std is reasonable, but on the second thought... 
                    scores[i] = mean - (float)Math.Sqrt(mean2 - mean * mean) / 20;
                }
                else if (considerEntropy == true)
                {
                    scores[i] = 0;
                    double H = 0; // entropy 
                    for (int j = 0; j < numClass; j++)
                    {
                        float p = prob[j][i];
                        H += -p * Math.Log(p + double.Epsilon, 2);
                        scores[i] += p * j;
                    }
                    scores[i] -= (float)(H / maxEntropy / 5);
                }
                else // use the expected relevance
                {
                    scores[i] = 0;
                    for (int j = 1; j < numClass; j++)
                        scores[i] += prob[j][i] * j;
                }
            }
            return scores;
        }

        //convert class probabilites to predicted labels: the function will be move to some other classes
        protected static int[] ConvertClassProbToLabels(float[][] classProb)
        {
            int[] symbols = new int[classProb[0].Length];

            int numClass = classProb.Length;
            int numSamples = classProb[0].Length;

            for (int i = 0; i < numSamples; i++)
            {
                float maxProb = classProb[0][i];
                symbols[i] = 0;
                for (int j = 1; j < numClass; j++)
                {
                    if (maxProb < classProb[j][i])
                    {
                        maxProb = classProb[j][i];
                        symbols[i] = j;
                    }
                }
            }
            return symbols;
        }

        private NDCG ndcg;
    }

    public class NDCGPairwise : Metrics
    {
        protected enum NDCGPairwiseType
        {
            PairCrossEnt = 0, //pair-wise cross-entropy
            PairError, //pair-wise pair error rate            
            meanTruncNDCG,
            pessTruncNDCG,
            optiTruncNDCG,
            CTYPES
        }

        public NDCGPairwise(LabelFeatureCore labelFeatureCore, LabelConverter labelConvert, DataPartitionType[] dataTypes,
            int ndcgAt, bool dropEmptyQueries, float scoreForEmptyQuery)
            : base(labelFeatureCore, labelConvert, dataTypes)
        {
            this.ndcg = new NDCG(dropEmptyQueries, scoreForEmptyQuery, ndcgAt);
        }

        override public int SIZE
        {
            get
            {
                return (int)NDCGPairwiseType.CTYPES;
            }
        }

        override public string ResultsHeaderStr()
        {
            return "Iter  CrossEnt:Trn/Vld/Tst   PairErrr:Trn/Vld/Tst   TrNDCG:Pess/Mean/Opt   VdNDCG:Pess/Mean/Opt   TsNDCG:Pess/Mean/Opt";
        }

        /// <summary>
        /// Get the optimal metric value and its corresponding iteration ID
        /// </summary>
        /// <param name="dataType">in: the data type we are interested in</param>
        /// <param name="metricType">in: the metric type we are interested in</param>
        /// <param name="result">out: the actual value of the metric</param>
        /// <returns>the iteration Index/ID the produces the minimal of the metricType on dataType</returns>
        override public int GetBest(DataPartitionType dataType, ref float result)
        {
            int idMax = -1;
            if (results_list.Count > 0)
            {
                int index = (int)NDCGPairwiseType.meanTruncNDCG;
                result = results_list[0].Metrics[(int)dataType][index];
                idMax = results_list[0].Id;

                for (int i = 0; i < results_list.Count; i++)
                {
                    if (result < results_list[i].Metrics[(int)dataType][index])
                    {
                        result = results_list[i].Metrics[(int)dataType][index];
                        idMax = i;
                    }
                }
            }
            return idMax;
        }

        override protected string ResultsStr(float[][] metrics, int id)
        {
            //double all = metrics[(int)DataPartitionType.Train][(int)NDCGType.NDCG] * 0.7 + metrics[(int)DataPartitionType.Validation][(int)NDCGType.NDCG] * 0.1 + metrics[(int)DataPartitionType.Test][(int)NDCGType.NDCG] * 0.2;
            string result = id.ToString("000");
            if (id == optimalID)
            {
                result += "*";
            }
            string fstr = "F4"; // format string
            result += "   ";
            result += metrics[(int)DataPartitionType.Train][(int)NDCGPairwiseType.PairCrossEnt].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)NDCGPairwiseType.PairCrossEnt].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)NDCGPairwiseType.PairCrossEnt].ToString(fstr) + "   ";

            result += metrics[(int)DataPartitionType.Train][(int)NDCGPairwiseType.PairError].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)NDCGPairwiseType.PairError].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)NDCGPairwiseType.PairError].ToString(fstr) + "   ";

            result += metrics[(int)DataPartitionType.Train][(int)NDCGPairwiseType.pessTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Train][(int)NDCGPairwiseType.meanTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Train][(int)NDCGPairwiseType.optiTruncNDCG].ToString(fstr) + "   ";

            result += metrics[(int)DataPartitionType.Validation][(int)NDCGPairwiseType.pessTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)NDCGPairwiseType.meanTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)NDCGPairwiseType.optiTruncNDCG].ToString(fstr) + "   ";

            result += metrics[(int)DataPartitionType.Test][(int)NDCGPairwiseType.pessTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)NDCGPairwiseType.meanTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)NDCGPairwiseType.optiTruncNDCG].ToString(fstr);

            return result;
        }

        override protected void ComputeMetrics(float[] labels, float[][] inScores, DataPartitionType dataType, float[] metrics)
        {
            //results initialization            
            for (int i = 0; i < metrics.Length; i++)
            {
                metrics[i] = 0;
            }

            //(1) Compute NDCG, pairwise error, and cross-entropy
            DataSet dataSet = this.labelFeatureCore.DataGroups.GetDataPartition(dataType);
            int[] groupIndex = dataSet.GroupIndex;
            if (groupIndex != null && groupIndex.Length > 0)
            {
                float[] scores = inScores[0];

                double totalMeanNDCG = 0;
                double totalPessNDCG = 0;
                double totalOptiNDCG = 0;
                double cQueries = 0;

                double totalErrRate = 0;
                double totalCrossEnt = 0;
                //compute the NDCG for each query/group
                double meanTruncNDCG, pessTruncNDCG, optiTruncNDCG;
                for (int i = 0; i < groupIndex.Length; i++)
                {
                    DataGroup query = this.labelFeatureCore.DataGroups[groupIndex[i]];
                    RankPairGenerator rankPairs = new RankPairGenerator(query, labels);
                    double cErr = 0;
                    double CrossEnt = 0;
                    double cPairs = 0;
                    foreach (RankPair rankPair in rankPairs)
                    {
                        float scoreH_minus_scoreL = scores[rankPair.IdxH] - scores[rankPair.IdxL];
                        CrossEnt += RankPair.CrossEntropy(scoreH_minus_scoreL);
                        if (scoreH_minus_scoreL <= 0)
                        {
                            cErr++;
                        }
                        cPairs++;
                    }

                    ndcg.ComputeNDCGs(query, labels, scores, out meanTruncNDCG, out pessTruncNDCG, out optiTruncNDCG);
                    if (cPairs > 0) // equivalent to !emptyQuery
                    {
                        //   float ndcg = this.ndcg.ComputeNDCG(query, labels, scores, ndcgAt);
                        totalMeanNDCG += meanTruncNDCG;
                        totalPessNDCG += pessTruncNDCG;
                        totalOptiNDCG += optiTruncNDCG;
                        totalErrRate += (cErr / cPairs);
                        totalCrossEnt += (CrossEnt / cPairs);
                        cQueries++;
                    }
                    else
                    {
                        if (!ndcg.DropEmptyQueries)
                        {
                            totalMeanNDCG += meanTruncNDCG;
                            totalPessNDCG += pessTruncNDCG;
                            totalOptiNDCG += optiTruncNDCG;
                            totalErrRate += 0.0F;
                            totalCrossEnt += 0.0F;
                            cQueries++;
                        }
                    }
                }
                metrics[(int)NDCGPairwiseType.meanTruncNDCG] = (float)(totalMeanNDCG / cQueries);
                metrics[(int)NDCGPairwiseType.pessTruncNDCG] = (float)(totalPessNDCG / cQueries);
                metrics[(int)NDCGPairwiseType.optiTruncNDCG] = (float)(totalOptiNDCG / cQueries);
                metrics[(int)NDCGPairwiseType.PairCrossEnt] = (float)(totalCrossEnt / cQueries);
                metrics[(int)NDCGPairwiseType.PairError] = (float)(totalErrRate / cQueries);
            }
        }

        override public void SaveScores(string fileName, float[][] InScores)
        {
            StreamWriter stream = new StreamWriter(new FileStream(fileName, FileMode.Create));

            stream.WriteLine("m:QueryId\t" + "m:Rating\t" + "Score");
            {
                float[] scores = InScores[0];

                for (int i = 0; i < this.labelFeatureCore.NumDataPoint; i++)
                {
                    stream.WriteLine(this.labelFeatureCore.GetGroupId(i).ToString() + "\t" + this.labelFeatureCore.GetLabel(i).ToString() + "\t" + scores[i].ToString());
                }
            }
            stream.Close();
        }

        private NDCG ndcg;
    }

    public class NDCGPairwiseVTest : Metrics
    {
        protected enum NDCGPairwiseType
        {
            PairCrossEnt = 0, //pair-wise cross-entropy
            PairError, //pair-wise pair error rate            
            meanTruncNDCG,
            pessTruncNDCG,
            optiTruncNDCG,
            CTYPES
        }

        public NDCGPairwiseVTest(LabelFeatureCore labelFeatureCore, LabelConverter labelConvert, DataPartitionType[] dataTypes,
            int ndcgAt, bool dropEmptyQueries, float scoreForEmptyQuery)
            : base(labelFeatureCore, labelConvert, dataTypes)
        {
            this.ndcg = new NDCG(dropEmptyQueries, scoreForEmptyQuery, ndcgAt);
        }

        override public int SIZE
        {
            get
            {
                return (int)NDCGPairwiseType.CTYPES;
            }
        }

        override public string ResultsHeaderStr()
        {
            return "Iter  CrossEnt:Trn/Vld/Tst   PairErr:Trn/Vld/Tst   VdNDCG:Pess/Mean/Opt   TsNDCG:Pess/Mean/Opt";
        
        }

        /// <summary>
        /// Get the optimal metric value and its corresponding iteration ID
        /// </summary>
        /// <param name="dataType">in: the data type we are interested in</param>
        /// <param name="metricType">in: the metric type we are interested in</param>
        /// <param name="result">out: the actual value of the metric</param>
        /// <returns>the iteration Index/ID the produces the minimal of the metricType on dataType</returns>
        override public int GetBest(DataPartitionType dataType, ref float result)
        {
            int idMax = -1;
            int index;
            if (results_list.Count > 0)
            {
                if ((int)dataType == 0) //training data
                {
                    index = (int)NDCGPairwiseType.PairError;
                }
                else
                {
                    index = (int)NDCGPairwiseType.meanTruncNDCG;
                }
                result = results_list[0].Metrics[(int)dataType][index];
                idMax = results_list[0].Id;

                for (int i = 0; i < results_list.Count; i++)
                {
                    if (result < results_list[i].Metrics[(int)dataType][index])
                    {
                        result = results_list[i].Metrics[(int)dataType][index];
                        idMax = i;
                    }
                }
            }
            return idMax;
        }

        override protected string ResultsStr(float[][] metrics, int id)
        {
            //double all = metrics[(int)DataPartitionType.Train][(int)NDCGType.NDCG] * 0.7 + metrics[(int)DataPartitionType.Validation][(int)NDCGType.NDCG] * 0.1 + metrics[(int)DataPartitionType.Test][(int)NDCGType.NDCG] * 0.2;
            string result = id.ToString("000");
            if (id == optimalID)
            {
                result += "*";
            }
            string fstr = "F4"; // format string
            result += "   ";
            result += metrics[(int)DataPartitionType.Train][(int)NDCGPairwiseType.PairCrossEnt].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)NDCGPairwiseType.PairCrossEnt].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)NDCGPairwiseType.PairCrossEnt].ToString(fstr) + "   ";

            result += metrics[(int)DataPartitionType.Train][(int)NDCGPairwiseType.PairError].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)NDCGPairwiseType.PairError].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)NDCGPairwiseType.PairError].ToString(fstr) + "   ";

            result += metrics[(int)DataPartitionType.Validation][(int)NDCGPairwiseType.pessTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)NDCGPairwiseType.meanTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)NDCGPairwiseType.optiTruncNDCG].ToString(fstr) + "   ";

            result += metrics[(int)DataPartitionType.Test][(int)NDCGPairwiseType.pessTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)NDCGPairwiseType.meanTruncNDCG].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)NDCGPairwiseType.optiTruncNDCG].ToString(fstr);

            return result;
        }

        override protected void ComputeMetrics(float[] labels, float[][] inScores, DataPartitionType dataType, float[] metrics)
        {
            //results initialization            
            for (int i = 0; i < metrics.Length; i++)
            {
                metrics[i] = 0;
            }

            //(1) Compute NDCG, pairwise error, and cross-entropy
            DataSet dataSet = this.labelFeatureCore.DataGroups.GetDataPartition(dataType);
            int[] groupIndex = dataSet.GroupIndex;
            if (groupIndex != null && groupIndex.Length>0 )
            {
                if ((int)dataType != 0)
                {
                    float[] scores = inScores[0];

                    double totalMeanNDCG = 0;
                    double totalPessNDCG = 0;
                    double totalOptiNDCG = 0;
                    double cQueries = 0;

                    double totalErrRate = 0;
                    double totalCrossEnt = 0;
                    //compute the NDCG for each query/group
                    double meanTruncNDCG, pessTruncNDCG, optiTruncNDCG;
                    for (int i = 0; i < groupIndex.Length; i++)
                    {
                        DataGroup query = this.labelFeatureCore.DataGroups[groupIndex[i]];
                        RankPairGenerator rankPairs = new RankPairGenerator(query, labels);
                        double cErr = 0;
                        double CrossEnt = 0;
                        double cPairs = 0;
                        foreach (RankPair rankPair in rankPairs)
                        {
                            float scoreH_minus_scoreL = scores[rankPair.IdxH] - scores[rankPair.IdxL];
                            CrossEnt += RankPair.CrossEntropy(scoreH_minus_scoreL);
                            if (scoreH_minus_scoreL <= 0)
                            {
                                cErr++;
                            }
                            cPairs++;
                        }

                        ndcg.ComputeNDCGs(query, labels, scores, out meanTruncNDCG, out pessTruncNDCG, out optiTruncNDCG);
                        if (cPairs > 0) // equivalent to !emptyQuery
                        {
                            //   float ndcg = this.ndcg.ComputeNDCG(query, labels, scores, ndcgAt);
                            totalMeanNDCG += meanTruncNDCG;
                            totalPessNDCG += pessTruncNDCG;
                            totalOptiNDCG += optiTruncNDCG;
                            totalErrRate += (cErr / cPairs);
                            totalCrossEnt += (CrossEnt / cPairs);
                            cQueries++;
                        }
                        else
                        {
                            if (!ndcg.DropEmptyQueries)
                            {
                                totalMeanNDCG += meanTruncNDCG;
                                totalPessNDCG += pessTruncNDCG;
                                totalOptiNDCG += optiTruncNDCG;
                                totalErrRate += 0.0F;
                                totalCrossEnt += 0.0F;
                                cQueries++;
                            }
                        }
                    }
                    metrics[(int)NDCGPairwiseType.meanTruncNDCG] = (float)(totalMeanNDCG / cQueries);
                    metrics[(int)NDCGPairwiseType.pessTruncNDCG] = (float)(totalPessNDCG / cQueries);
                    metrics[(int)NDCGPairwiseType.optiTruncNDCG] = (float)(totalOptiNDCG / cQueries);
                    metrics[(int)NDCGPairwiseType.PairCrossEnt] = (float)(totalCrossEnt / cQueries);
                    metrics[(int)NDCGPairwiseType.PairError] = (float)(totalErrRate / cQueries);
                }
                else
                {
                    float[] scores = inScores[0];

                    double cQueries = 0;

                    double totalErrRate = 0;
                    double totalCrossEnt = 0;

                    for (int i = 0; i < groupIndex.Length; i++)
                    {
                        DataGroup query = this.labelFeatureCore.DataGroups[groupIndex[i]];
                        RankPairGenerator rankPairs = new RankPairGenerator(query, labels);
                        double cErr = 0;
                        double CrossEnt = 0;
                        double cPairs = 0;
                        foreach (RankPair rankPair in rankPairs)
                        {
                            float scoreH_minus_scoreL = scores[rankPair.IdxH] - scores[rankPair.IdxL];
                            CrossEnt += RankPair.CrossEntropy(scoreH_minus_scoreL);
                            if (scoreH_minus_scoreL <= 0)
                            {
                                cErr++;
                            }
                            cPairs++;
                        }

                        if (cPairs > 0) // equivalent to !emptyQuery
                        {
                            totalErrRate += (cErr / cPairs);
                            totalCrossEnt += (CrossEnt / cPairs);
                            cQueries++;
                        }
                        else
                        {
                            if (!ndcg.DropEmptyQueries)
                            {
                                totalErrRate += 0.0F;
                                totalCrossEnt += 0.0F;
                                cQueries++;
                            }
                        }
                    }
                    metrics[(int)NDCGPairwiseType.meanTruncNDCG] = 0.0F;
                    metrics[(int)NDCGPairwiseType.pessTruncNDCG] = 0.0F;
                    metrics[(int)NDCGPairwiseType.optiTruncNDCG] = 0.0F;
                    metrics[(int)NDCGPairwiseType.PairCrossEnt] = (float)(totalCrossEnt / cQueries);
                    metrics[(int)NDCGPairwiseType.PairError] = (float)(totalErrRate / cQueries);
                }
            }
        }

        override public void SaveScores(string fileName, float[][] InScores)
        {
            StreamWriter stream = new StreamWriter(new FileStream(fileName, FileMode.Create));

            stream.WriteLine("m:QueryId\t" + "m:Rating\t" + "Score");
            {
                float[] scores = InScores[0];

                for (int i = 0; i < this.labelFeatureCore.NumDataPoint; i++)
                {
                    stream.WriteLine(this.labelFeatureCore.GetGroupId(i).ToString() + "\t" + this.labelFeatureCore.GetLabel(i).ToString() + "\t" + scores[i].ToString());
                }
            }
            stream.Close();
        }

        private NDCG ndcg;
    }

    
    public class Pairwise : Metrics
    {
        protected enum PairwiseType
        {
            PairCrossEnt = 0, //pair-wise cross-entropy
            PairError, //pair-wise pair error rate            
            CTYPES
        }

        public Pairwise(LabelFeatureCore labelFeatureCore, LabelConverter labelConvert, DataPartitionType[] dataTypes,
            int ndcgAt, bool dropEmptyQueries, float scoreForEmptyQuery)
            : base(labelFeatureCore, labelConvert, dataTypes)
        {
            this.ndcg = new NDCG(dropEmptyQueries, scoreForEmptyQuery, ndcgAt);
        }

        override public int SIZE
        {
            get
            {
                return (int)PairwiseType.CTYPES;
            }
        }

        override public string ResultsHeaderStr()
        {
            return "Iter  CrossEnt:Trn/Vld/Tst   PairErrr:Trn/Vld/Tst";
        }

        /// <summary>
        /// Get the optimal metric value and its corresponding iteration ID
        /// </summary>
        /// <param name="dataType">in: the data type we are interested in</param>
        /// <param name="metricType">in: the metric type we are interested in</param>
        /// <param name="result">out: the actual value of the metric</param>
        /// <returns>the iteration Index/ID the produces the minimal of the metricType on dataType</returns>
        override public int GetBest(DataPartitionType dataType, ref float result)
        {
            int idMax = -1;
            if (results_list.Count > 0)
            {
                int index = (int)PairwiseType.PairError;
                result = results_list[0].Metrics[(int)dataType][index];
                idMax = results_list[0].Id;

                for (int i = 0; i < results_list.Count; i++)
                {
                    if (result < results_list[i].Metrics[(int)dataType][index])
                    {
                        result = results_list[i].Metrics[(int)dataType][index];
                        idMax = i;
                    }
                }
            }
            return idMax;
        }

        override protected string ResultsStr(float[][] metrics, int id)
        {
            //double all = metrics[(int)DataPartitionType.Train][(int)NDCGType.NDCG] * 0.7 + metrics[(int)DataPartitionType.Validation][(int)NDCGType.NDCG] * 0.1 + metrics[(int)DataPartitionType.Test][(int)NDCGType.NDCG] * 0.2;
            string result = id.ToString("000");
            if (id == optimalID)
            {
                result += "*";
            }
            string fstr = "F4"; // format string
            result += "   ";
            result += metrics[(int)DataPartitionType.Train][(int)PairwiseType.PairCrossEnt].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)PairwiseType.PairCrossEnt].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)PairwiseType.PairCrossEnt].ToString(fstr) + "   ";

            result += metrics[(int)DataPartitionType.Train][(int)PairwiseType.PairError].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Validation][(int)PairwiseType.PairError].ToString(fstr) + "/";
            result += metrics[(int)DataPartitionType.Test][(int)PairwiseType.PairError].ToString(fstr) + "   ";

            return result;
        }

        override protected void ComputeMetrics(float[] labels, float[][] inScores, DataPartitionType dataType, float[] metrics)
        {
            //results initialization            
            for (int i = 0; i < metrics.Length; i++)
            {
                metrics[i] = 0;
            }

            //(1) Compute NDCG, pairwise error, and cross-entropy
            DataSet dataSet = this.labelFeatureCore.DataGroups.GetDataPartition(dataType);
            int[] groupIndex = dataSet.GroupIndex;
            if (groupIndex != null && groupIndex.Length>0)
            {
                float[] scores = inScores[0];

                
                double cQueries = 0;

                double totalErrRate = 0;
                double totalCrossEnt = 0;
               
                for (int i = 0; i < groupIndex.Length; i++)
                {
                    DataGroup query = this.labelFeatureCore.DataGroups[groupIndex[i]];
                    RankPairGenerator rankPairs = new RankPairGenerator(query, labels);
                    double cErr = 0;
                    double CrossEnt = 0;
                    double cPairs = 0;
                    foreach (RankPair rankPair in rankPairs)
                    {
                        float scoreH_minus_scoreL = scores[rankPair.IdxH] - scores[rankPair.IdxL];
                        CrossEnt += RankPair.CrossEntropy(scoreH_minus_scoreL);
                        if (scoreH_minus_scoreL <= 0)
                        {
                            cErr++;
                        }
                        cPairs++;
                    }

                    
                    if (cPairs > 0) // equivalent to !emptyQuery
                    {
                    
                        totalErrRate += (cErr / cPairs);
                        totalCrossEnt += (CrossEnt / cPairs);
                        cQueries++;
                    }
                    else
                    {
                        if (!ndcg.DropEmptyQueries)
                        {
                            totalErrRate += 0.0F;
                            totalCrossEnt += 0.0F;
                            cQueries++;
                        }
                    }
                }

                metrics[(int)PairwiseType.PairCrossEnt] = (float)(totalCrossEnt / cQueries);
                metrics[(int)PairwiseType.PairError] = (float)(totalErrRate / cQueries);
            }
        }

        override public void SaveScores(string fileName, float[][] InScores)
        {
            StreamWriter stream = new StreamWriter(new FileStream(fileName, FileMode.Create));

            stream.WriteLine("m:QueryId\t" + "m:Rating\t" + "Score");
            {
                float[] scores = InScores[0];

                for (int i = 0; i < this.labelFeatureCore.NumDataPoint; i++)
                {
                    stream.WriteLine(this.labelFeatureCore.GetGroupId(i).ToString() + "\t" + this.labelFeatureCore.GetLabel(i).ToString() + "\t" + scores[i].ToString());
                }
            }
            stream.Close();
        }

        private NDCG ndcg;
    }

    /// <summary>
    /// Abstract class for L_1 and L_2 error
    /// </summary>
    public abstract class L_N : Metrics
    {
        //dataType==null <=> all the data are used in one partition
        public L_N(LabelFeatureCore labelFeatureCore, DataPartitionType[] dataTypes)
            : base(labelFeatureCore, dataTypes)
        {
        }

        override public int SIZE
        {
            get
            {
                return 1;
            }
        }

        //public abstract string ResultsHeaderStr();

        /// Get the optimal metric value and its corresponding iteration ID
        /// </summary>
        /// <param name="dataType">in: the data type we are interested in</param>
        /// <param name="result">out: the actual value of the metric</param>
        /// <returns>the iteration Index/ID the produces the minimal of the metricType on dataType</returns>
        override public int GetBest(DataPartitionType dataType, ref float result)
        {
            int idMin = -1;
            if (results_list.Count > 0)
            {
                result = results_list[0].Metrics[(int)dataType][0];
                idMin = results_list[0].Id;

                for (int i = 0; i < results_list.Count; i++)
                {
                    if (result > results_list[i].Metrics[(int)dataType][0])
                    {
                        result = results_list[i].Metrics[(int)dataType][0];
                        idMin = i;
                    }
                }
            }
            return idMin;
        }

        override protected string ResultsStr(float[][] metrics, int id)
        {
            string result = id.ToString();
            if (id == optimalID)
            {
                result += "+";
            }
            result += "\t";
            result += metrics[(int)DataPartitionType.Train][0].ToString("0.0000000") + "\t";
            result += metrics[(int)DataPartitionType.Validation][0].ToString("0.0000000") +"\t";
            result += metrics[(int)DataPartitionType.Test][0].ToString();
            return result;
        }

        override public void SaveScores(string fileName, float[][] InScores)
        {
            StreamWriter stream = new StreamWriter(new FileStream(fileName, FileMode.Create));

            stream.WriteLine("m:Rating\t" + "Score");
            {
                float[] scores = InScores[0];

                for (int i = 0; i < this.labelFeatureCore.NumDataPoint; i++)
                {
                    stream.WriteLine(this.labelFeatureCore.GetLabel(i).ToString() + "\t" + scores[i].ToString());
                }
            }
            stream.Close();
        }

        //protected abstract void ComputeMetrics(float[] labels, float[][] scorePredict, DataPartitionType dataType, float[] metrics);
    }

    public class L2Error : L_N
    {
        //dataType==null <=> all the data are used in one partition
        public L2Error(LabelFeatureCore labelFeatureCore, DataPartitionType[] dataTypes)
            : base(labelFeatureCore, dataTypes)
        {
        }

        override public string ResultsHeaderStr()
        {
            return "Iter\tTrainL2Error\tValidL2Error\tTestL2Error";
        }

        override protected void ComputeMetrics(float[] labels, float[][] scorePredict, DataPartitionType dataType, float[] metrics)
        {
            float[] scores = scorePredict[0];
            int[] index = dataSegments[(int)dataType];
            double err = 0;
            double recip = 1.0;
            if (index != null)
            {
                for (int i = 0; i < index.Length; i++)
                {
                    double thisErr = labels[index[i]] - scores[index[i]];
                    err += thisErr * thisErr;
                }
                recip = 1.0 / ((double)index.Length + float.Epsilon);
            }
            else
            {
                for (int i = 0; i < scores.Length; i++)
                {
                    double thisErr = labels[i] - scores[i];
                    err += thisErr * thisErr;
                }
                recip = 1.0 / ((double)scores.Length + float.Epsilon);
            }
            metrics[0] = (float)Math.Sqrt(err * recip); // Only one kind of metric for MSE (namely MSE)
        }
    }

    public class L1Error : L_N
    {
        //dataType==null <=> all the data are used in one partition
        public L1Error(LabelFeatureCore labelFeatureCore, DataPartitionType[] dataTypes)
            : base(labelFeatureCore, dataTypes)
        {
        }

        override public string ResultsHeaderStr()
        {
            return "Iter\tTrainL1Error\tValidL1Error\tTestL1Error";
        }

        override protected void ComputeMetrics(float[] labels, float[][] scorePredict, DataPartitionType dataType, float[] metrics)
        {
            float[] scores = scorePredict[0];
            int[] index = dataSegments[(int)dataType];
            double err = 0;
            double recip = 1.0;
            if (index != null)
            {
                for (int i = 0; i < index.Length; i++)
                {
                    double delta = labels[index[i]] - scores[index[i]];
                    double thisErr = delta > 0 ? delta : -delta;
                    err += thisErr;
                }
                recip = 1.0 / ((double)index.Length + float.Epsilon);
            }
            else
            {
                for (int i = 0; i < scores.Length; i++)
                {
                    double delta = labels[i] - scores[i];
                    double thisErr = delta > 0 ? delta : -delta;
                    err += thisErr;
                }
                recip = 1.0 / ((double)scores.Length + float.Epsilon);
            }
            metrics[0] = (float)(err * recip);
        }
    }


}
