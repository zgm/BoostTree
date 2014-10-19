using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Diagnostics;
using Microsoft.TMSN;

namespace StochasticGradientBoost
{
    public class RandomSampler
    {       
        public RandomSampler(Random r)
        {
            this.r = r;            
        }
       
        public float SampleRate
        {
            set
            {
                sampleRate = value;
            }

            get
            {
                return sampleRate;
            }
        }

        public int SampleSize
        {
            get
            {
                return (int)(this.n * this.sampleRate);
            }
        }

        public int TotalSize
        {
            get
            {
                return this.n;
            }
        }

        //randomly shuffle the mapping
        public void Shuffle(int totalSize)
        {
            this.n = totalSize;
            if (this.r != null && (this.mapTbl == null || this.n > this.mapTbl.Length))
            {
                this.mapTbl = new int[this.n];
            }

            if (this.mapTbl != null)
            {
                for (int i = 0; i < this.mapTbl.Length; i++)
                {
                    this.mapTbl[i] = i;
                }

                if (this.mapTbl != null)
                {
                    for (int i = 1; i < this.n; i++)
                    {
                        // Note we need: r.Next(0, i+1), and not r.Next(0, i)  
                        // and consider sawp with itself for equal distribution.
                        // This is important for small totalSizes like 2. andrzejp, 2009-08-14
                        
                        int j = r.Next(0, i+1);
                        //swap mapTbl[i] <-> mapTbl[j]
                        int t = this.mapTbl[i];
                        this.mapTbl[i] = this.mapTbl[j];
                        this.mapTbl[j] = t;
                    }
                }
            }
        }

        /// <summary>
        /// return the object ID of the i-th sample
        /// </summary>
        /// <param name="i">the i-th sampled item</param>
        /// <returns>the original ID/index of the sampled item</returns>
        public int idxLookup(int i)
        {
            if (this.mapTbl != null)
            {
                return this.mapTbl[i];
            }
            else
            {
                return i;
            }
        }

        int n;
        float sampleRate;

        int[] mapTbl;
        Random r;
    }

    public class DataFeatureSampleRate
    {

        public DataFeatureSampleRate(float sampleFeatureRate, float sampleDataRate, float sampleDataGroupRate)
        {
            this.sampleFeatureRate = sampleFeatureRate;
            this.sampleDataRate = sampleDataRate;
            this.sampleDataGroupRate = sampleDataGroupRate;
        }        

        /// <summary>
        /// randomly select a portion of the training data groups for current iteration of boosting
        /// </summary>
        /// <param name="iIter">the current iteration number</param>
        /// <returns>the portion of training data group to be used for training</returns>
        public float SampleDataGroupRate(int iIter)
        {
            return this.sampleDataGroupRate;
        }

        /// <summary>
        /// sub-sample a portion of the training data when spliting a node for current iteration of boosting
        /// </summary>
        /// <param name="iIter">the current iteration number</param>
        /// <returns>the portion of training data to be used for training</returns>
        public float SampleDataRate(int iIter)
        {
            return this.sampleDataRate;
        }

        /// <summary>
        /// sub-sample a portion of the features when spliting a node for current iteration of boosting
        /// </summary>
        /// <param name="iIter">the current iteration number</param>
        /// <returns>the portion of features to be used for training</returns>
        public float SampleFeatureRate(int iIter)
        {
            return sampleFeatureRate;
        }       

        // The fraction of randomly selection data groups for each iteration
        [NonSerialized]
        private float sampleDataGroupRate;
        // The fraction of (randomly chosen) data to use at each split.
        [NonSerialized]
        private float sampleDataRate;
        // The fraction of (randomly chosen) features to use at each split.
        [NonSerialized]
        private float sampleFeatureRate;
    }

    [Serializable]
    public class RegressionTree
    {
        public RegressionTree(LabelFeatureDataCoded labelFeatureDataCoded, BoostTreeLoss boostTreeLoss, int iTree, int[] workIndex,
                              RandomSampler featureSampler, RandomSampler dataSampler,
                              int maxTreeSize, int minNumSamples,
                              IFindSplit findSplit, TempSpace tempSpace)
        {
            this.labelFeatureDataCoded = labelFeatureDataCoded;                        
            this.workIndex = workIndex;
            this.numFeatures = labelFeatureDataCoded.NumFeatures;
            this.maxTreeSize = maxTreeSize;
            this.featureImportance = new float[this.numFeatures];
            this.minNumSamples = minNumSamples;       
            
            //distributed setting
            this.adjustFactor = 1.0F;

            InitTempSpace(tempSpace);
            BuildRegressionTree(boostTreeLoss, iTree, findSplit, dataSampler, featureSampler);
            GC.Collect(); // hope for the best!!!
        }

        public void PredictFunValue(LabelFeatureDataCoded data, ref float[] funValue)
        {            
            for (int i = 0; i < funValue.Length; i++)
            {
                int node = 0;
                bool nextDataPoint = false;
                while (nextDataPoint == false)
                {
                    if (this.tree[node].isTerminal)
                    {
                        funValue[i] = this.tree[node].regionValue;
                        nextDataPoint = true;
                        continue;
                    }

                    if (data.GetFeatureCoded(this.tree[node].split, i) <= this.tree[node].splitValueCoded)
                        node = this.tree[node].leftChild;
                    else
                        node = this.tree[node].rightChild;
                }
            }
        }

        public void PredictFunValueNKeepScores(LabelFeatureData data, int[] Train2TestIdx, float[] funValue, float[] keepScores)
        {
            Debug.Assert(data.NumDataPoint == funValue.Length);
            for (int i = 0; i < data.NumDataPoint; i++)
            {
                funValue[i] = PredictFunValue(data.GetFeature(i), Train2TestIdx);
                keepScores[i] = funValue[i];
            }
        }


        public void PredictFunValue(LabelFeatureData data, int[] Train2TestIdx, float[] funValue)
        {
            Debug.Assert(data.NumDataPoint == funValue.Length);
            for (int i = 0; i < data.NumDataPoint; i++)
            {
                funValue[i] = PredictFunValue(data.GetFeature(i), Train2TestIdx);

                //int node = 0;
                //bool nextDataPoint = false;
                //while (nextDataPoint == false)
                //{
                //    if (this.tree[node].isTerminal)
                //    {
                //        funValues[i] = this.tree[node].regionValue;
                //        nextDataPoint = true;
                //        continue;
                //    }

                //    if (data[i][this.tree[node].split] <= this.tree[node].splitValue)
                //        node = this.tree[node].leftChild;
                //    else
                //        node = this.tree[node].rightChild;
                //}
            }
        }

        ///distributed version of Predicting Function Value, for use with uncoded data, but 
        ///data is guaranteed to have same order of features since it is training data that was
        ///split across nodes. Do not use function if unknown ordering of features.
        public void PredictFunValue(LabelFeatureDataCoded data, bool b, ref float[] funValue)
        {
            Debug.Assert(data.NumDataPoint == funValue.Length);
            for (int i = 0; i < data.NumDataPoint; i++)
            {
                funValue[i] = PredictFunValue(data.GetFeature(i));
            }
        }

        //for distributed version, when ordering of features is guaranteed
        public float PredictFunValue(float[] dataPoint)
        {
            int i = 0;
            while (true)
            {
                if (this.tree[i].isTerminal)
                    return this.tree[i].regionValue;

                //assuming idx is same since train set was split for distribution
                int idx = this.tree[i].split;
                float val = (idx < 0) ? 0 : dataPoint[idx];
                if (val <= this.tree[i].splitValue)
                    i = this.tree[i].leftChild;
                else
                    i = this.tree[i].rightChild;
            }
        } 

        public float PredictFunValue(float[] dataPoint, int[] Train2TestIdx)
        {
            int i = 0;
            while (true)
            {
                if (this.tree[i].isTerminal)
                    return this.tree[i].regionValue;

                int idx = (Train2TestIdx == null) ? this.tree[i].split : Train2TestIdx[this.tree[i].split];
                float val = (idx < 0) ? 0 : dataPoint[idx];
                if (val <= this.tree[i].splitValue)
                    i = this.tree[i].leftChild;
                else
                    i = this.tree[i].rightChild;
            }
        }     

        public float[] FeatureImportance
        {
            get { return this.featureImportance; }
        }

        /// <summary>
        /// Mutiply the tree terminal value/response by the adjFactor
        /// - additional optimozation step
        /// </summary>
        /// <param name="adjFactor">the multiplication factor on the leaf nodes</param>
        public void AdjustResponse(float adjFactor)
        {            
            if (adjFactor == 1.0f)
                return;
            for (int i = 0; i < this.tree.Length; i++)
            {
                if (this.tree[i] != null && this.tree[i].isTerminal)
                {
                    this.tree[i].regionValue *= adjFactor;
                }
            }
        }

        private void BuildRegressionTree(BoostTreeLoss boostTreeLoss, int iTree, IFindSplit findSplit, RandomSampler featureSampler, RandomSampler dataSampler)
        {
            this.responses = boostTreeLoss.PseudoResponse(iTree);
            
            TreeNode root = new TreeNode();
            root.isTerminal = true;
            root.dataPoints = Vector.IndexArray(this.workIndex.Length);   

            this.tree = new TreeNode[2 * maxTreeSize - 1];
            this.tree[0] = root; 

            for (int i = 0; i < maxTreeSize - 1; i++)
            {
                float maxGain = -1;
                int bestRegion = -1;

                TreeNode leftNode = new TreeNode();
                TreeNode rightNode = new TreeNode();

                //qiangwu: compute the best split for new nodes
                //         We only need to explore the last two nodes because they are and only they are new nodes i.e.
                //         for (int j = 2*i; j >= 0; j--)
                for (int j = 0; j < 2 * i + 1; j++)
                {
                    TreeNode curNode = this.tree[j];
                   
                    //qiangwu: (assert curNode.split<0 && curNode.isTerminal) <==> (2*i-1 <= j <= 2*i)
                    if (curNode.split<0 && curNode.isTerminal && curNode.dataPoints.Length >= this.minNumSamples)
                    {
                        dataSampler.Shuffle(curNode.dataPoints.Length);
                        featureSampler.Shuffle(this.numFeatures);

                        Split bestSplit = findSplit.FindBestSplit(this.labelFeatureDataCoded, this.responses, curNode.dataPoints, this.workIndex, featureSampler, dataSampler, this.minNumSamples);   

                        //qiangwu: the only way (bestSplit.feature < 0) not slippint is because this.dataColRange[dim]=1 for all
                        //         dimensions. I.e. the values all of data points in every dimension are the same (or in one bin)                        
                        if (bestSplit.feature >= 0)
                        {
                            curNode.split = bestSplit.feature;
                            curNode.gain = (float)bestSplit.gain;
                            curNode.splitValueCoded = bestSplit.iThresh + 0.2F; // add 0.2 to avoid boundary check or floating point rounding
                            curNode.splitValue = this.labelFeatureDataCoded.ConvertToOrigData(curNode.split, curNode.splitValueCoded);
                            //SplitOneDim(curNode.dataPoints, regionSplitDim, regionSplitPoint, out curNode.leftPoints, out curNode.rightPoints);
                        }
                    }
                    if (curNode.gain > maxGain)
                    {
                        maxGain = curNode.gain;
                        bestRegion = j;
                    }
                }

                if (bestRegion == -1)
                    break;

                TreeNode bestNode = this.tree[bestRegion];

                SplitOneDim(bestNode.dataPoints, bestNode.split, (int)bestNode.splitValueCoded, out bestNode.leftPoints, out bestNode.rightPoints);

                leftNode.isTerminal = true; leftNode.parent = bestRegion;
                leftNode.dataPoints = bestNode.leftPoints;

                rightNode.isTerminal = true; rightNode.parent = bestRegion;
                rightNode.dataPoints = bestNode.rightPoints;

                this.tree[2 * i + 1] = leftNode; this.tree[2 * i + 2] = rightNode;

                this.featureImportance[bestNode.split] += bestNode.gain; 

                bestNode.leftChild = 2 * i + 1;
                bestNode.rightChild = 2 * i + 2;
                bestNode.isTerminal = false;
                bestNode.gain = -1;
                bestNode.dataPoints = null;
                bestNode.leftPoints = null;
                bestNode.rightPoints = null;
                GC.Collect(); // hope for the best. 
            }

            //qiangwu: compute the response of newly created region (node)
            for (int i = 0; i < this.tree.Length; i++)
            {
                if (this.tree[i] != null && this.tree[i].isTerminal)
                {
                    Debug.Assert(this.tree[i].dataPoints.Length >= this.minNumSamples, "Regression Tree split has problems");
                    float v = boostTreeLoss.Response(this.tree[i].dataPoints, this.workIndex, iTree);
                    //round the regional value to 5 decimal point 
                    //to remove/alleviate the differences due to floating point precision
                    //so that different algorithms produces the same model/results
#if ROUND
                    this.tree[i].regionValue = (float)Math.Round(v, 5);
#else
                    this.tree[i].regionValue = v;
#endif //ROUND
                    this.tree[i].dataPoints = null;
                    this.tree[i].leftPoints = null;
                    this.tree[i].rightPoints = null;
                    GC.Collect();
                }
            }            
        }
        
        // Permform actual splitting
        private void SplitOneDim(int[] dataPoints, int dim, int splitPoint, out int[] leftPoints, out int[] rightPoints)
        {

            Array.Clear(this.isLeft, 0, dataPoints.Length); 

            int numLeftPoints = 0; 
            for (int i = 0; i < dataPoints.Length; i++)
            {
                int ind = this.workIndex[dataPoints[i]];
                int loc = this.labelFeatureDataCoded.GetFeatureCoded(dim, ind);
                if (loc <= splitPoint)
                {
                    this.isLeft[i] = true;
                    numLeftPoints++;
                }
            }

            Debug.Assert(numLeftPoints >= this.minNumSamples && (dataPoints.Length-numLeftPoints) >= this.minNumSamples, "Regression Tree split has probelms");

            /*
            leftPoints = new int[numLeftPoints];
            rightPoints = new int[dataPoints.Length-numLeftPoints];
            int leftIndex = 0;
            int rightIndex = 0; 
            for (int i = 0; i < dataPoints.Length; i++)
            {
                if (this.isLeft[i])
                    leftPoints[leftIndex++] = dataPoints[i];
                else
                    rightPoints[rightIndex++] = dataPoints[i];
            }*/

            
            // reuse datapoints[], to save some memory, as GC.collect() does not work.
            leftPoints = new int[numLeftPoints];
            rightPoints = dataPoints; 
            int leftIndex = 0;
            int rightIndex = 0;
            for (int i = 0; i < dataPoints.Length; i++)
            {
                if (this.isLeft[i])
                    leftPoints[leftIndex++] = dataPoints[i];
                else
                    rightPoints[rightIndex++] = dataPoints[i];
            }
            Array.Resize(ref rightPoints, dataPoints.Length - numLeftPoints);
            
            dataPoints = null;
            
        }

        // not used. 
        // For (efficiently) computing mean and std  of the reponses. 
        private void ComputeMeanStd(int dim, int[] dataPoints, out float mean, out float std)
        {
            mean = 0;
            float mean2 = 0;
            for (int i = 0; i < dataPoints.Length; i++)
            {
                int ind = this.workIndex[dataPoints[i]];
                float resp = this.responses[ind];
                mean += resp;
                mean2 += resp * resp;
            }

            mean /= dataPoints.Length;
            mean2 /= dataPoints.Length;

            std = (float)Math.Sqrt(mean2 - mean * mean);
        }

        private void InitTempSpace(TempSpace tempSpace)
        {
            this.isLeft = tempSpace.isLeft; 
        }

        /// <summary>
        /// the number of nodes in the regresion tree
        /// </summary>
        public int NumNodes
        {
            get { return tree.Length; }
        }

        /// <summary>
        /// the number of leaves in the regression tree
        /// </summary>
        public int NumTerminalNodes
        {
            get { return (tree.Length + 1) / 2; }
        }

        /// <summary>
        /// output each tree in the format used by Powerset's parser
        /// </summary>
        public void WritePSStyle(int iTree, StreamWriter wStream, string[] ColumnNames)
        {
            wStream.WriteLine("\tSplitVar\tSplitCodePred\tLeftNode\tRightNode\tMissingNode");
            int cNodes = tree.Length;
            for (int iNode = 0; iNode < cNodes; iNode++)
            {
                if (this.tree[iNode] == null)
                {
                    continue;
                }

                if (tree[iNode].isTerminal)
                {
                    wStream.WriteLine("{0}\t-1\t{1}\t-1\t-1\t-1",iNode, tree[iNode].regionValue * 1e6);
                }
                else
                {
                    wStream.WriteLine("{0}\t{1}\t{2}\t{3}\t{4}\t-1\t{5}", iNode, tree[iNode].split, tree[iNode].splitValue, tree[iNode].leftChild, tree[iNode].rightChild, ColumnNames[tree[iNode].split]);
                }
            }
#if DEBUG
            wStream.Flush();
#endif
        }

        public void WriteMSNStyle(int iTree, StreamWriter wStream, string[] ColumnNames)
        {
            const string CStrTreeName = "AnchorMostFrequent";

            wStream.WriteLine("[Input:{0}]", iTree);
            wStream.WriteLine("Name={0}", CStrTreeName);
            wStream.WriteLine("Transform=DecisionTree");

            int cNodes = tree.Length;
            for (int iNode = 0; iNode < cNodes; iNode++)
            {
                if (this.tree[iNode] == null)
                {
                    continue;
                }

                if (tree[iNode].isTerminal)
                {
                    wStream.WriteLine("NodeType:{0}=Value", iNode);
                    wStream.WriteLine("NodeValue:{0}={1}", iNode, tree[iNode].regionValue);
                }
                else
                {
                    wStream.WriteLine("NodeType:{0}=Branch", iNode);
                    wStream.WriteLine("NodeDecision:{0}={1}", iNode, ColumnNames[tree[iNode].split]);
#if INT_FEATUREVALUE
                    if (tree[iNode].splitValue < 0 || tree[iNode].splitValue > 4294967296)
                    {
                        throw new Exception("splitValue has to be UInt32");
                    }
                    else
                    {
                        wStream.WriteLine("NodeThreshold:{0}={1}", iNode, (UInt32)tree[iNode].splitValue);
                    }
#else
                    wStream.WriteLine("NodeThreshold:{0}={1}", iNode, tree[iNode].splitValue);
#endif // INT_FEATUREVALUE
                    wStream.WriteLine("NodeLTE:{0}={1}", iNode, tree[iNode].leftChild);
                    wStream.WriteLine("NodeGT:{0}={1}", iNode, tree[iNode].rightChild);
                }
                wStream.WriteLine();
            }
        }

        public DTNode[] CreateDTNodes(string[] ColumnNames)
        {
            int cNodes = tree.Length;
            DTNode[] dtNodes = new DTNode[cNodes];
            for (int iNode = 0; iNode < cNodes; iNode++)
            {
                if (this.tree[iNode] != null)
                {
                    dtNodes[iNode] = tree[iNode].CreateDTNode(iNode, ColumnNames);
                }
            }
            return dtNodes;
        }

        [Serializable]
        private class TreeNode
        {
            public int parent = -1;
            public int leftChild = -1;
            public int rightChild = -1;
            public bool isTerminal = false;
            public int[] dataPoints = null;
            public int split = -1;

            public float splitValue;
            public float splitValueCoded;
            public float regionValue;
            public float gain = -1;

            public int[] leftPoints = null;
            public int[] rightPoints = null;

            public DTNode CreateDTNode(int iNode, string[] ColumnNames)
            {
                DTNode node = new DTNode(iNode);
                if (!isTerminal)
                {
                    node.Add("NodeType", "Branch");
                    node.Add("NodeDecision", ColumnNames[this.split]);
                    node.Add("NodeThreshold", this.splitValue.ToString());
                    node.Add("NodeLTE", this.leftChild.ToString());
                    node.Add("NodeGT", this.rightChild.ToString());
                }
                else
                {
                    node.Add("NodeType", "Value");
                    node.Add("NodeValue", this.regionValue.ToString());
                }
                return node;
            }
        }        
       
        private float[] featureImportance;
        [NonSerialized] private LabelFeatureDataCoded labelFeatureDataCoded;

        [NonSerialized] private float[] responses;        
        
        [NonSerialized] private int[] workIndex; 
        [NonSerialized] private int numFeatures;
        [NonSerialized] private int maxTreeSize;
        [NonSerialized] private int minNumSamples=15;   // not very sensitive     
        
        [NonSerialized] bool[] isLeft;

        private TreeNode[] tree;

        //adding below for distributed setting
        private float adjustFactor;
        public float AdjustFactor { get { return adjustFactor; } set { adjustFactor = value;} }
    }
}

