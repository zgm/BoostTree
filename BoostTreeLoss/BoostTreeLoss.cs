/// REVIEW: CJCB - we should have a separate interface for regression, classification,
/// and a base interface (there is a lot of stuff that classification needs, that regression does not - e.g. one tree per iteration, 
/// and no mapping of outputs to probs).  However this means also redesigning the BoostTree class.
//#define DoResponseAdjust
using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.TMSN;

namespace StochasticGradientBoost
{
    /// TODO: qiangwu - BoostTreeLoss needs to be serialized
    /// <summary>
    /// Encapsulates/defines the loss function we are optimizing on a set of regression trees
    /// </summary>    
    public interface BoostTreeLoss
    {        
        /// <summary>
        /// The number of trees per boosting iteration.  For classification, its the number of classes (or 1, for two classes);
        /// for regression, it's one.
        /// </summary>                    
        int NumTreesPerIteration
        {
            get;
        }

        /// <summary>
        /// Reset the state of a BoostTreeLoss object to accomodate training/testing on data
        /// </summary>
        /// <param name="testData"></param>
        void Reset(int cSamples);        
        
        /// <summary>
        /// The model scores of all the data points.  For classification, the first index indexes the class, and the second, the data point.
        /// For regression, the first index is always zero.  
        /// </summary>
        float[][] ModelScores
        {
            get;
        }
            
        /// <summary>
        /// Compute and store scores of the input data given the submodel (or its scores).
        /// Results are stored in ModelScores. 
        /// It is used in both training and testing phases.
        /// If subModelScore is not null, then it is simply copied to ModelScores; else if model and labelFeatureData are both non-null,
        /// the model is evaluated to populate ModelScores; else ModelScores is populated with a default value.
        /// </summary>
        /// <param name="model">the model to be evaluated</param>
        /// <param name="labelFeatureData">input data</param>
        /// <param name="subModelScore">precomputed model scores on input data</param>
        void ModelEval(Model model, LabelFeatureData labelFeatureData, LabelFeatureData subModelScore);


        /// <summary>
        /// Accumulate the functional values of all the data points from a given tree into classFunValue
        /// </summary>
        /// <param name="funValueGain">Array of size numSamples: contains the functional value gain produced by the iTree-th tree in the current iteration</param>
        /// <param name="adjFactor">Adjust/multiply the functional value gain produced by the iTree-th tree</param>
        /// <param name="iTree">Indexes the set of numClasses trees in a given iteration</param>
        void AccFuncValueGain(float[] funValueGain, float adjFactor, int iTree);

        /// <summary>
        /// Computes an optimal adjustment factor of the new tree - which is muliplied with the response value at each leaf node.  This is done
        /// using findStep (from XRank).  Note that the optimal value is found by treating the new tree as a new ranker, to be combined with the previous
        /// ranker, which consists of a bunch of trees.
        /// </summary>
        /// <param name="funcValueGain">the functional value returned by the current tree for all data points</param>
        /// <returns>the multiplication factor that will minimize the overall cost</returns>
        float ComputeResponseAdjust(float[] funcValueGain);
        
        
        /// <summary>
        /// Get the pseudo response corresponding to the ith tree,
        /// which is used to construct the boosted tree
        /// </summary>
        /// <param name="iTree">the index of the tree</param>
        /// <returns>the pseudo response of the ith tree for all data points</returns>
        float[] PseudoResponse(int iTree);

        /// <summary>
        /// Reducing the training data used to build a boosted tree
        /// </summary>
        /// <param name="inDataSet">The current work training data set</param>
        /// <param name="k">The index of the tree to be built</param>
        /// <param name="m">The iteration of the current boosting process</param>
        /// <returns>The data index used to build the boosted tree after triming</returns>
        int[] TrimIndex(DataSet inDataSet, int k, int m);

        /// <summary>
        /// Convert the functional value produced by the set of boosted regression trees to the final scores of a model
        /// Function values are the raw outputs from the regression trees; 
        /// The model scores are derived from the function values for the purpose of the underline application
        /// </summary>
        /// <param name="FuncValue">input function value of the model</param>
        /// <param name="ModelScore">output model scores</param>
        void FuncValuesToModelScores();

        /// <summary>
        /// Covert the model scores into the funational values corresponding to the set of boosted regression trees
        /// </summary>
        /// <param name="ModelScore">input scores produces by the model</param>
        /// <param name="FuncValue">output functional values</param>
        void ModelScoresToFuncValues();

        /// <summary>
        /// Compute the point-wise pseudo-response of the loss function to be optimized
        /// It is used to build the decision tree - except from computing the response value of a leaf node
        /// </summary>
        /// <param name="dataSet">all training data</param>
        void ComputePseudoResponse(DataSet dataSet);

        /// <summary>
        /// computes the response for a sub-region of data specified by dataPoints that optimonize the Loss        
        /// </summary>
        /// <param name="dataPonits"> The index of the data falling into a leaf node
        /// <param name="workIndex"> The index of all the data points to build a tree
        /// <param name="iTree">The index of the tree for the leaf node that the response is computed</param>
        /// <returns>the response of the sub-region/leaf-node on the ith tree</returns>
        float Response(int[] dataPoints, int[] workIndex, int iTree);        
    }


    /// <summary>
    /// The encapsulate loss function for mutli-class classification
    /// used in building iterative boosted regression tree
    /// </summary>
    [Serializable]
    public class McBoostTreeLoss : BoostTreeLoss
    {
        
        /// <summary>
        /// Constructor of McBoostTreeLoss
        /// </summary>
        /// <param name="dp">the input data used to train boosted regression trees</param>
        /// <param name="learnRate">the input learning rate specified in training</param>
        public McBoostTreeLoss(LabelFeatureDataCoded labelFeatureDataCoded, LabelConverter labelConvert,
                               float learnRate)
        {
            this.learnRate = learnRate;
            
            this.numClass = 0;
            this.numSamples = labelFeatureDataCoded.NumDataPoint;
            this.classLabels = new int[labelFeatureDataCoded.NumDataPoint];
            for (int i = 0; i < this.numSamples; i++)
            {
                this.classLabels[i] = (int)labelConvert.convert(labelFeatureDataCoded.GetLabel(i));
                if (this.classLabels[i] + 1 > this.numClass)
                    this.numClass = this.classLabels[i] + 1;
            }            

            //qiangwu: we probably don't need a matrix to store the label information though it is more convinent coding-wise
            this.classLabelsMatrix = BulidClassLabelsMatrix(this.classLabels, this.numClass);

            this.classProb = new float[this.numClass][];
            this.classFunValue = new float[this.numClass][];
            this.pseudoResponses = new float[this.numClass][];
            this.weights = new float[this.numClass][];          

            for (int k = 0; k < this.numClass; k++)
            {                
                this.classProb[k] = new float[this.numSamples];
                this.classFunValue[k] = new float[this.numSamples];
                this.pseudoResponses[k] = new float[this.numSamples];
                this.weights[k] = new float[this.numSamples];
            }
            
            //float[] weightsByLabel = WeightsByLabel(this.classLabels);            
            //float[] probWeights = new float[this.numSamples];
        }       

        /// <summary>
        /// reset the state of a BoostTreeLoss object to accomodate training/testing on data
        /// </summary>
        /// <param name="testData"></param>
        public void Reset(int cSamples)
        {
            this.numSamples = cSamples;

            if (this.classProb == null)
            {
                this.classProb = new float[this.numClass][];
            }
            if (this.classFunValue == null)
            {
                this.classFunValue = new float[this.numClass][];
            }
            if (this.pseudoResponses == null)
            {
                this.pseudoResponses = new float[this.numClass][];
            }
            if (this.weights == null)
            {
                this.weights = new float[this.numClass][];          
            }            

            for (int k = 0; k < this.numClass; k++)
            {
                //for testing only <-> no need to for pseudo response
                //this.pseudoResponses[k] = new float[this.numSamples];
                if (this.classProb[k] == null || this.classProb[k].Length < this.numSamples)
                {
                    this.classProb[k] = new float[this.numSamples];
                }
                if (this.classFunValue[k] == null || this.classFunValue[k].Length < this.numSamples)
                {
                    this.classFunValue[k] = new float[this.numSamples];
                }
                if (this.pseudoResponses[k] == null || this.pseudoResponses[k].Length < this.numSamples)
                {
                    this.pseudoResponses[k] = new float[this.numSamples];
                }
                if (this.weights[k] == null || this.weights[k].Length < this.numSamples)
                {
                    this.weights[k] = new float[this.numSamples];
                }

                for (int i = 0; i < this.numSamples; i++)
                {
                    this.classProb[k][i] = 0.0F;
                    this.classFunValue[k][i] = 0.0F;
                    this.pseudoResponses[k][i] = 0.0F;
                    this.weights[k][i] = 0.0F;
                }
            }
        
        }

        /// <summary>
        /// How many sets of boosted trees we are building
        /// </summary>
        public int NumTreesPerIteration
        {
            get
            {
                if (this.numClass > 2)
                {
                    return this.numClass;
                }
                else
                {
                    return this.numClass - 1;
                }
            }
        }

        /// <summary>
        /// The model scores of all the data points
        /// </summary>
        public float[][] ModelScores
        {
            get
            {
                return this.classProb;
            }
        }

        /// <summary>
        /// Compute and store the scores of the input data given a model
        /// </summary>
        /// <param name="model">the model to be evaluated</param>
        /// <param name="labelFeatureData">input data</param>        
        /// <param name="subModelScore">pre-computed scores for the input data</param>    
        public void ModelEval(Model model, LabelFeatureData labelFeatureData, LabelFeatureData subModelScore)
        {            
            if (subModelScore != null)
            {
                Debug.Assert(this.numSamples == subModelScore.NumDataPoint); 
                for (int i = 0; i < this.numSamples; i++)
                {                    
                    for (int k = 0; k < this.numClass; k++)
                    {
                        this.ModelScores[k][i] = subModelScore.GetFeature(k, i);
                    }
                }
            }
            else if (model != null && labelFeatureData != null) // REVIEW by CJCB: this was 'if', I think it should be 'else if' so I changed it
            {
                Debug.Assert(this.numSamples == labelFeatureData.NumDataPoint);
                float[] scores = new float[this.numClass];
                for (int i = 0; i < this.numSamples; i++)
                {                    
                    model.Evaluate(labelFeatureData.GetFeature(i), scores);
                    for (int k = 0; k < this.numClass; k++)
                    {
                        this.ModelScores[k][i] = scores[k];
                    }
                }
            }
            else
            {
                //TODO: qiangwu - should we always requre subModel to exist 
                //to make the default model value computation explicit??? 
                //It is probabaly safer that way revist the issue later
                float score = (float)1.0 / (float)this.numClass;
                for (int i = 0; i < this.numSamples; i++)
                {
                    for (int k = 0; k < this.numClass; k++)
                    {
                        this.ModelScores[k][i] = score;
                    }
                }
            }
        }     

        /// <summary>
        /// computes an optiomal adjustment factor of the new tree - which is muliplied with the response value at each leaf node
        /// </summary>
        /// <param name="funcValueGain">the functional value returned by the current tree for all data points</param>
        /// <returns>the multiplication factor that will minimize the overall cost</returns>
        public float ComputeResponseAdjust(float[] funcValueGain)
        {
            return 1.0F;
        }

        /// <summary>
        /// Accumulate the functional values of all the data points from a given tree into classFunValue
        /// </summary>
        /// <param name="funValueGain">Array of size numSamples: contains the functional value gain produced by the iTree-th tree in the current iteration</param>
        /// <param name="adjFactor">Adjust/multiply the functional value gain produced by the iTree-th tree</param>
        /// <param name="iTree">Indexes the set of numClasses trees in a given iteration</param>
        public void AccFuncValueGain(float[] funValueGain, float adjFactor, int k)
        {
            int numSamples = funValueGain.Length;

            for (int i = 0; i < this.numSamples; i++)
                this.classFunValue[k][i] += (funValueGain[i] * adjFactor);


            // no need to compute for the second class for binary classfication
            // qiangwu: in theory we don't need to compute the last regression tree and the classFunValue for the last class,
            //          because the sum of all classFunValue should be zero. PingLi has found that actually computing the last tree
            //          produces better results (may be due to WeightTrim or precison issues?). It is probably interesting to summ
            //          all classFunValue cross all classes and check how far it deviates from zero?
            //qiangwu: PingLi has found that we don't have to compute the last tree when numClass = 2.
            //         PingLi was exploring using a sequence of binary classifiers to realize the fact that the classes are ordered.
            //         However, it has not produce improvement yet
            if (this.numClass == 2)
            {
                for (int i = 0; i < this.numSamples; i++)
                    this.classFunValue[1][i] = -this.classFunValue[0][i];                             
            }            
        }
       

        /// <summary>
        /// Get the pseudo response corresponding to the ith tree,
        /// which is used to construct the boosted tree
        /// </summary>
        /// <param name="iTree">the index of the tree</param>
        /// <returns>the pseudo response of the ith tree</returns>
        public float[] PseudoResponse(int iTree)
        {
            return this.pseudoResponses[iTree];
        }

        /// <summary>
        /// Reducing the training data used to build a boosted tree
        /// Influece trimming discard a small fraction (lower quantile) of the samples. 
        /// This was proposed previously as a heuristic for speeding up training. 
        /// I happen to notice that trimming also helps for better NDCG. 
        /// Intutively, when the weight p*(1-p) is small, this sample is already classified pretty well, and
        /// further efforts could be wasted. In any case, for ranking, we do not really need perfect classifications.
        /// </summary>
        /// <param name="inDataSet">The current work training data set</param>
        /// <param name="k">The index of the tree to be built</param>
        /// <param name="m">The iteration of the current boosting process</param>
        /// <returns>The data index used to build the boosted tree after triming</returns>
        public int[] TrimIndex(DataSet inDataSet, int k, int m)
        {
            const float maxTrimRate = 0.8F;  // At least keeps 1-0.8 samples after trimming
            const int minNonTrimIter = 30;   // only perform weight trimming after 30 iterations.

            // weight trimming plays a beneficial role for NDCG. 
            // For now, it is probably safe to use 0.10F-0.20F. 

            float trimQuantile = 0.10F; //Make it scalable to any number of classes

            //float[] trimQuantile = { 0.10F, 0.10F, 0.10F, 0.10F, 0.10F };
            //private float[] trimQuantile = { 0.15F, 0.15F, 0.15F, 0.15F, 0.15F};
            //private float[] trimQuantile = { 0.2F, 0.2F, 0.2F, 0.2F, 0.2F };
            //private float[] trimQuantile = { 0.25F, 0.25F, 0.25F, 0.25F, 0.25F };
            //private float[] trimQuantile = { 0.3F, 0.3F, 0.3F, 0.3F, 0.3F };
            //private float[] trimQuantile = { 0.25F, 0.20F, 0.15F, 0.10F, 0.05F };
            //private float[] trimQuantile = { 0.30F, 0.25F, 0.20F, 0.15F, 0.10F };	
            //private float[] trimQuantile = { 0.35F, 0.30F, 0.25F, 0.20F, 0.15F };
            //private float[] trimQuantile = { 0.40F, 0.35F, 0.30F, 0.25F, 0.20F };				
            //private float[] trimQuantile = { 0.45F, 0.40F, 0.35F, 0.30F, 0.25F };            

            // Weight-trimming discards a portion of samples in the lower quantile of p*(1-p)
            // I find this not only speeds up the computations but also outputs better results. 
            int[] trimIndex;
            int[] index = inDataSet.DataIndex;
            if (m < minNonTrimIter || (this.numClass == 2 && k == 1))
            {
                trimIndex = index;
            }
            else
            {
                float[] weightsL = new float[index.Length];
                float sumWeights = 0;

                for (int i = 0; i < index.Length; i++)
                {
                    weightsL[i] = this.weights[k][index[i]];
                    sumWeights += weightsL[i];
                }

                int[] weightIndex = Vector.IndexArray(index.Length);
                Array.Sort(weightsL, weightIndex);

                int trimLen = 0;
                float partialSumWeights = 0;
                float targetSumWeights = trimQuantile * sumWeights - float.Epsilon;
                while (partialSumWeights < targetSumWeights && trimLen < index.Length * maxTrimRate)
                    partialSumWeights += weightsL[trimLen++];

                trimIndex = new int[index.Length - trimLen];

                for (int i = 0; i < trimIndex.Length; i++)
                    trimIndex[i] = index[weightIndex[i + trimLen]];

                // We find empirically that sorting the indexes actually speeds up accessing the data matrix
                Array.Sort(trimIndex);

                if (m >= minNonTrimIter)
                    Console.WriteLine("\t" + k.ToString() + "\t" + (1.0 * trimIndex.Length / index.Length).ToString());
            }
            return trimIndex;
        }
        
        /// <summary>
        /// Convert the functional value produced by the set of boosted regression trees to the final scores of the model
        /// (i.e. to a probability for each class, for each sample).
        /// Sigmoid transform of the function value into class prob.
        /// </summary>
        /// <param name="FuncValue">input function value of the model</param>
        /// <param name="ModelScore">output model scores</param>
        public void FuncValuesToModelScores()
        {
            float maxFunValue = 500;  // to prevent overflow

            for (int i = 0; i < this.numSamples; i++)
            {
                float rowExpSum = 0;
                for (int j = 0; j < this.numClass; j++)
                {
                    this.classProb[j][i] = (float)Math.Exp(Math.Min(this.classFunValue[j][i], maxFunValue));
                    rowExpSum += this.classProb[j][i];
                }
                for (int j = 0; j < this.numClass; j++)
                {
                    this.classProb[j][i] /= rowExpSum;
                }
            }
        }  

        /// <summary>
        /// Convert the model scores into the functional values corresponding to the set of boosted regression trees
        /// Logit transform of the class prob into the function value.
        /// </summary>
        /// <param name="ModelScore">input scores produces by the model</param>
        /// <param name="FuncValue">output functional values</param>
        public void ModelScoresToFuncValues()
        {
            float minProbValue = (float)1e-6; // to prevent overflow

            for (int i = 0; i < this.numSamples; i++)
            {
                float rowLogSum = 0;
                for (int j = 0; j < this.numClass; j++)
                {
                    rowLogSum += (float)Math.Log(Math.Max(this.classProb[j][i], minProbValue));
                }
                rowLogSum /= (float)numClass;
                for (int j = 0; j < numClass; j++)
                {
                    this.classFunValue[j][i] = (float)Math.Log(Math.Max(this.classProb[j][i], minProbValue)) - rowLogSum;
                }
            }
        }

        /// <summary>
        /// Compute the point-wise pseudo-response of the loss function to be optimized
        /// It is used to build the decision tree - except from computing the response value of a leaf node
        /// </summary>
        /// <param name="dataSet">all training data</param>
        public void ComputePseudoResponse(DataSet dataSet)
        {
            for (int k = 0; k < this.numClass; k++)
            {        
                for (int j = 0; j < dataSet.NumSamples; j++)
                {
                    int i = dataSet.DataIndex[j];                                        
                    this.pseudoResponses[k][i] = this.classLabelsMatrix[k][i] - this.classProb[k][i];
                    // qiangwu: pingli has assured me that the following are equvalent:
                    // qiangwu: weights[i] = abs(responses[k][i])(1-abs(responses[k][i])); 
                    // qiangwu: which is shown in the paper - algorithms 6: Lk-TreeBoost
                    this.weights[k][i] = this.classProb[k][i] * (1 - this.classProb[k][i]);
                }
            }
        }

        /// <summary>
        /// computes the response for a sub-region of data specified by dataPoints that optimonize the Loss        
        /// </summary>
        /// <param name="iTree">The index of the tree for the leaf node that the response is computed</param>
        /// <returns>the response of the sub-region/leaf-node on the ith tree</returns>
        public float Response(int[] dataPoints, int[] workIndex, int iTree)
        {
            float value;
            // classification
            
            float tmp1 = 0, tmp2 = 0;
            for (int j = 0; j < dataPoints.Length; j++)
            {
                int orgInd = workIndex[dataPoints[j]];
                tmp1 += this.pseudoResponses[iTree][orgInd];
                tmp2 += this.weights[iTree][orgInd];
            }

            const float maxFunVal = 5;    // not very sensitive, mainly for numerical stability  

            value = (tmp1 + float.Epsilon) / (tmp2 + float.Epsilon) * this.FunctionValueModifier;
            if (value > maxFunVal)
                value = maxFunVal;
            else if (value < -maxFunVal)
                value = -maxFunVal;
            
            // regression 
            //{
            //    float[] y = new float[this.tree[i].dataPoints.Length];
            //    for (int j = 0; j < y.Length; j++)
            //    {
            //        int orgInd = this.workIndex[this.tree[i].dataPoints[j]];
            //        y[j] = weights[orgInd];
            //    }

            //    if (model == 1) // L1 regression
            //        value = Vector.Median(y);
            //    else  // L2 regression
            //        value = Vector.Mean(y);
            //    value *= this.funValModifier;
            //}

            return value;
        }

        /// <summary>
        /// the shrinkage of the iterative boosting algorithm
        /// </summary>
        /// <param name="learnRate">the input learning rate of the overall boosting algorithm</param>
        /// <returns>the actual shrinkage given the learning rate</returns>
        private float FunctionValueModifier
        {
            get
            {
                return this.learnRate * (this.numClass - 1) / this.numClass;
            }
        }

        private float[][] BulidClassLabelsMatrix(int[] labels, int numClass)
        {
            float[][] classLabelsMatrix = new float[numClass][];

            for (int i = 0; i < numClass; i++)
            {
                classLabelsMatrix[i] = new float[labels.Length];
            }

            for (int i = 0; i < labels.Length; i++)
            {
                classLabelsMatrix[labels[i]][i] = 1;
            }
            return classLabelsMatrix;
        }

        // modify the loss function slightly. v<=0.2 seems at least no harm. 
        private float[][] BulidClassLabelsMatrix(int[] labels, int numClass, float v)
        {
            float[][] classLabelsMatrix = new float[numClass][];
            for (int i = 0; i < numClass; i++)
            {
                classLabelsMatrix[i] = new float[labels.Length];
            }

            for (int i = 0; i < labels.Length; i++)
            {
                if (labels[i] <= 0)
                {
                    classLabelsMatrix[0][i] = 1 - v; classLabelsMatrix[1][i] = v;
                }
                if (labels[i] == 1)
                {
                    classLabelsMatrix[0][i] = v; classLabelsMatrix[1][i] = 1 - 2 * v; classLabelsMatrix[2][i] = v;
                }
                if (labels[i] == 2)
                {
                    classLabelsMatrix[1][i] = v; classLabelsMatrix[2][i] = 1 - 2 * v; classLabelsMatrix[3][i] = v;
                }
                if (labels[i] == 3)
                {
                    classLabelsMatrix[2][i] = v; classLabelsMatrix[3][i] = 1 - 2 * v; classLabelsMatrix[4][i] = v;
                }
                if (labels[i] == 4)
                {
                    classLabelsMatrix[3][i] = v; classLabelsMatrix[4][i] = 1 - v;
                }
            }
            return classLabelsMatrix;
        }        

        private float learnRate = 0.01F;

        private int numClass;

        [NonSerialized] private int numSamples;

        [NonSerialized] private int[] classLabels;
        [NonSerialized] private float[][] classLabelsMatrix;

        [NonSerialized] private float[][] classProb;
        [NonSerialized] private float[][] classFunValue;
        [NonSerialized] private float[][] pseudoResponses;
        [NonSerialized] private float[][] weights;
        
        //currently not used        
        //private bool labelWeighting = false;
        //private float[] WeightsByLabel(int[] labels)
        //{
        //    bool expWeighting = false;
        //    float[] weights = new float[labels.Length];

        //    // exponential weighting (i.e, 2^r-1) is too aggressive 
        //    if (expWeighting)
        //    {
        //        for (int i = 0; i < labels.Length; i++)
        //        {
        //            if (labels[i] == 4)
        //                weights[i] = 31;
        //            else if (labels[i] == 3)
        //                weights[i] = 15;
        //            else if (labels[i] == 2)
        //                weights[i] = 7;
        //            else if (labels[i] == 1)
        //                weights[i] = 3;
        //            else
        //                weights[i] = 1;
        //        }
        //    }
        //    else
        //    {
        //        for (int i = 0; i < labels.Length; i++)
        //        {
        //            weights[i] = labels[i] + 1;

        //            if (weights[i] < 1)
        //                weights[i] = 1;
        //        }
        //    }
        //    return weights;
        //}
    }

    /// <summary>
    /// The encapsulate the ranknet loss function used in building iterative boosted regression tree
    /// </summary>
    [Serializable]
    public class BoostTreeRankNetLoss : BoostTreeLoss
    {
        /// <summary>
        /// Constructor of BoostTreeRankNetLoss
        /// </summary>
        /// <param name="dp">the input data used to train boosted regression trees</param>
        /// <param name="learnRate">the input learning rate specified in training</param>
        public BoostTreeRankNetLoss(LabelFeatureDataCoded labelFeatureDataCoded, LabelConverter labelConvert,
                                    float learnRate)
        {
            this.learnRate = learnRate;

            this.numSamples = labelFeatureDataCoded.NumDataPoint;
            this.labels = new float[labelFeatureDataCoded.NumDataPoint];

            for (int i = 0; i < labelFeatureDataCoded.NumDataPoint; i++)
            {
                this.labels[i] = labelConvert.convert(labelFeatureDataCoded.GetLabel(i));
            }

            this.score = new float[this.numSamples];
            this.funValue = new float[this.numSamples];
            this.pseudoResponses = new float[this.numSamples];
            this.weights = new float[this.numSamples];

            this.labelFeatureDataCoded = labelFeatureDataCoded;
        }

        /// <summary>
        /// How many sets of boosted trees we are building
        /// </summary>                    
        public int NumTreesPerIteration
        {
            get
            {
                return 1;
            }
        }

        /// <summary>
        /// reset the state of a BoostTreeLoss object to accomodate training/testing on data
        /// </summary>
        /// <param name="testData"></param>
        public void Reset(int cSamples)
        {
            this.numSamples = cSamples;

            this.score = new float[this.numSamples];
            this.funValue = new float[this.numSamples];
            this.pseudoResponses = new float[this.numSamples];
            this.weights = new float[this.numSamples];
        }

        /// <summary>
        /// The model scores of all the data points
        /// </summary>
        public float[][] ModelScores
        {
            get
            {
                float[][] modelScores = new float[1][];
                modelScores[0] = this.score;
                return modelScores;
            }
        }

        /// <summary>
        /// Compute and store the scores of the input data given a model
        /// </summary>
        /// <param name="model">the model to be evaluated</param>
        /// <param name="data">input data</param>
        public void ModelEval(Model model, float[][] data)
        {
            Debug.Assert(this.numSamples == data.Length);

        }

        /// <summary>
        /// Compute and store the scores of the input data given a model
        /// </summary>
        /// <param name="model">the model to be evaluated</param>
        /// <param name="labelFeatureData">input data</param>        
        /// <param name="subModelScore">pre-computed scores for the input data</param>    
        public void ModelEval(Model model, LabelFeatureData labelFeatureData, LabelFeatureData subModelScore)
        {
            if (subModelScore != null)
            {
                Debug.Assert(this.numSamples == subModelScore.NumDataPoint);
                for (int i = 0; i < this.numSamples; i++)
                {
                    this.score[i] = subModelScore.GetFeature(0, i);
                }
            }
            else if (model != null && labelFeatureData != null)
            {
                Debug.Assert(this.numSamples == labelFeatureData.NumDataPoint);
                float[] scores = new float[1];
                for (int i = 0; i < this.numSamples; i++)
                {
                    model.Evaluate(labelFeatureData.GetFeature(i), scores);
                    this.score[i] = scores[0];
                }
            }
            else
            {
                for (int i = 0; i < this.numSamples; i++)
                {
                    this.score[i] = 0.0F;
                }
            }
        }

        /// <summary>
        /// computes an optiomal adjustment factor of the new tree - which is muliplied with the response value at each leaf node
        /// </summary>
        /// <param name="funcValueGain">the functional value returned by the current tree for all data points</param>
        /// <returns>the multiplication factor that will minimize the overall cost</returns>
        public float ComputeResponseAdjust(float[] funcValueGain)
        {
            return 1.0F;
        }

        /// <summary>
        /// Accumulate the functional values of all the data points from a given tree into classFunValue
        /// </summary>
        /// <param name="funValueGain">Array of size numSamples: contains the functional value gain produced by the iTree-th tree in the current iteration</param>
        /// <param name="adjFactor">Adjust/multiply the functional value gain produced by the iTree-th tree</param>
        /// <param name="iTree">Indexes the set of numClasses trees in a given iteration</param>
        public void AccFuncValueGain(float[] funValueGain, float adjFactor, int iTree)
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.funValue[i] += (funValueGain[i] * adjFactor);
            }
        }

        /// <summary>
        /// Get the pseudo response corresponding to the ith tree,
        /// which is used to construct the boosted tree
        /// </summary>
        /// <param name="iTree">the index of the tree</param>
        /// <returns>the pseudo response of the ith tree</returns>
        public float[] PseudoResponse(int iTree)
        {
            return this.pseudoResponses;
        }

        /// <summary>
        /// Reducing the training data used to build a boosted tree
        /// </summary>
        /// <param name="inDataSet">The current work training data set</param>
        /// <param name="k">The index of the tree to be built</param>
        /// <param name="m">The iteration of the current boosting process</param>
        /// <returns>The data index used to build the boosted tree after triming</returns>
        public int[] TrimIndex(DataSet inDataSet, int k, int m)
        {
            return inDataSet.DataIndex;
        }

        /// <summary>
        /// Convert the functional value produced by the set of boosted regression trees a final scores of a model
        /// </summary>
        public void FuncValuesToModelScores()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.score[i] = this.funValue[i];
            }
        }        

        /// <summary>
        /// Covert the model scores into the funational values corresponding to the set of boosted regression trees
        /// </summary>
        public void ModelScoresToFuncValues()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.funValue[i] = this.score[i];
            }
        }

        /// <summary>
        /// Compute the point-wise pseudo-response of the loss function to be optimized
        /// It is used to build the decision tree - except from computing the response value of a leaf node
        /// </summary>        
        /// <param name="dataSet">all training data</param>
        public void ComputePseudoResponse(DataSet dataSet)
        {
            ResetParameters();
            for (int qIdx = 0; qIdx < dataSet.NumGroups; qIdx++)
            {
                DataGroup query = dataSet.GetDataGroup(qIdx);
                RankPairGenerator rankPairs = new RankPairGenerator(query, this.labels);
                foreach (RankPair rankPair in rankPairs)
                {
                    float scoreH_minus_scoreL = this.score[rankPair.IdxH] - this.score[rankPair.IdxL];
                    float gradient = RankPair.CrossEntropyDerivative(scoreH_minus_scoreL);

                    this.pseudoResponses[rankPair.IdxH] += gradient;
                    this.pseudoResponses[rankPair.IdxL] -= gradient;

                    float weight = RankPair.CrossEntropy2ndDerivative(this.score[rankPair.IdxH] - this.score[rankPair.IdxL]);

                    this.weights[rankPair.IdxH] += weight;
                    this.weights[rankPair.IdxL] += weight;

                }
                //this.labelFeatureData.PartitionData;
            }
        }

        /// <summary>
        /// computes the response for a sub-region of data specified by dataPoints that optimonize the Loss        
        /// </summary>
        /// <param name="iTree">The index of the tree for the leaf node that the response is computed</param>
        /// <returns>the response of the sub-region/leaf-node on the ith tree</returns>
        public float Response(int[] dataPoints, int[] workIndex, int iTree)
        {
            return LineSearchResponse(dataPoints, workIndex, iTree);
        }

        /// <summary>
        /// return the response approximated by line-step
        /// </summary>
        /// <param name="dataPoints"></param>
        /// <param name="workIndex"></param>
        /// <param name="iTree"></param>
        /// <returns></returns>
        private float LineSearchResponse(int[] dataPoints, int[] workIndex, int iTree)
        {
            float value;

            float tmp1 = 0, tmp2 = 0;
            for (int j = 0; j < dataPoints.Length; j++)
            {
                int orgInd = workIndex[dataPoints[j]];
                tmp1 += this.pseudoResponses[orgInd];
                tmp2 += this.weights[orgInd];
            }

            const float MaxFunVal = 5;
            value = (tmp1 + float.Epsilon) / (tmp2 + float.Epsilon) * this.FunctionValueModifier;
            if (value > MaxFunVal)
                value = MaxFunVal;
            else if (value < -MaxFunVal)
                value = -MaxFunVal;

            return value;
        }

        private float learnRate = 0.0F;

        /// <summary>
        /// the shrinkage of the iterative boosting algorithm
        /// </summary>
        /// <param name="learnRate">the input learning rate of the overall boosting algorithm</param>
        /// <returns>the actual shrinkage given the learning rate</returns>
        private float FunctionValueModifier
        {
            get
            {
                return this.learnRate / 2;
            }
        }

        private void ResetParameters()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.weights[i] = 0.0F;
                this.pseudoResponses[i] = 0.0F;
            }
        }

        //the entire data set
        [NonSerialized]
        private LabelFeatureDataCoded labelFeatureDataCoded = null;
        //the number of sample in the above data set
        [NonSerialized]
        private int numSamples = 0;

        //the labels of all data points
        [NonSerialized]
        private float[] labels = null;
        //the model socres corresponding to all data points
        [NonSerialized]
        private float[] score = null;
        //the function values produced by the boosted trees for all data pounts
        [NonSerialized]
        private float[] funValue = null;
        //the output pseudo-response for all data points 
        [NonSerialized]
        private float[] pseudoResponses = null;
        [NonSerialized]
        private float[] weights = null;

    }

    public enum StepSizeType
    {
        LineSearch = 0,
        Average,
        Median,
    };

    /// <summary>
    /// The encapsulate the lambda loss function
    /// used in building iterative boosted regression tree
    /// </summary>
    [Serializable]
    public class BoostTreeLambdaLoss : BoostTreeLoss
    {        
        /// <summary>
        /// Constructor of BoostTreeRankNetLoss
        /// </summary>
        /// <param name="dp">the input data used to train boosted regression trees</param>
        /// <param name="learnRate">the input learning rate specified in training</param>
        public BoostTreeLambdaLoss(LabelFeatureDataCoded labelFeatureDataCoded, LabelConverter labelConvert, 
                                   float learnRate, float labelWeight, StepSizeType stepSizeType, FindStepLib fs,
                                   float labelForUnlabeled, double scoreForDegenerateQuery, int truncLevel)
        {
            this.learnRate = learnRate;
            this.labelWeight = labelWeight;
            
            this.labelForUnlabeled = labelForUnlabeled;
            this.scoreForDegenerateQuery = scoreForDegenerateQuery;
            this.truncLevel = truncLevel;

            this.labels = new float[labelFeatureDataCoded.NumDataPoint];

            for (int i = 0; i < labelFeatureDataCoded.NumDataPoint; i++)
            {
                this.labels[i] = labelConvert.convert(labelFeatureDataCoded.GetLabel(i));
            }

            this.numSamples = labels.Length;

            this.score = new float[this.numSamples];
            this.funValue = new float[this.numSamples];
            this.pseudoResponses = new float[this.numSamples];
            this.weights = new float[this.numSamples];

            this.labelFeatureDataCoded = labelFeatureDataCoded;

            //data member to compute the optimal adjustment factor (step size) for leaf nodes response
            this.qcAccum = this.CreateQueryCollection();
            this.qcCurrent = this.CreateQueryCollection();
            this.fs = fs;
            this.stepSizeType = stepSizeType;
        }

        /// <summary>
        /// How many sets of boosted trees we are building
        /// </summary>                    
        public int NumTreesPerIteration
        {
            get
            {
                return 1;
            }
        }

        /// <summary>
        /// reset the state of a BoostTreeLoss object to accomodate training/testing on data
        /// </summary>
        /// <param name="testData"></param>
        public void Reset(int cSamples)
        {
            this.numSamples = cSamples;

            this.score = new float[this.numSamples];
            this.funValue = new float[this.numSamples];
            this.pseudoResponses = new float[this.numSamples];
            this.weights = new float[this.numSamples];
        }

        /// <summary>
        /// The model scores of all the data points
        /// </summary>
        public float[][] ModelScores
        {
            get
            {
                float[][] modelScores = new float[1][];
                modelScores[0] = this.score;
                return modelScores;
            }
        }

        /// <summary>
        /// Compute and store the scores of the input data given a model
        /// </summary>
        /// <param name="model">the model to be evaluated</param>
        /// <param name="labelFeatureData">input data</param>        
        /// <param name="subModelScore">pre-computed scores for the input data</param>    
        public void ModelEval(Model model, LabelFeatureData labelFeatureData, LabelFeatureData subModelScore)
        {
            if (subModelScore != null)
            {
                Debug.Assert(this.numSamples == subModelScore.NumDataPoint);
                for (int i = 0; i < this.numSamples; i++)
                {
                    this.score[i] = subModelScore.GetFeature(0, i);
                }
            }
            else if (model != null && labelFeatureData != null)
            {
                Debug.Assert(this.numSamples == labelFeatureData.NumDataPoint);
                float[] scores = new float[1];
                for (int i = 0; i < this.numSamples; i++)
                {
                    model.Evaluate(labelFeatureData.GetFeature(i), scores);
                    this.score[i] = scores[0];
                }
            }
            else
            {
                for (int i = 0; i < this.numSamples; i++)
                {
                    this.score[i] = 0.0F;
                }
            }
        }

        /// <summary>
        /// Accumulate the functional values of all the data points from a given tree into classFunValue
        /// </summary>
        /// <param name="funValueGain">Array of size numSamples: contains the functional value gain produced by the iTree-th tree in the current iteration</param>
        /// <param name="adjFactor">Adjust/multiply the functional value gain produced by the iTree-th tree</param>
        /// <param name="iTree">Indexes the set of numClasses trees in a given iteration</param>
        public void AccFuncValueGain(float[] funValueGain, float adjFactor, int iTree)
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.funValue[i] += (funValueGain[i] * adjFactor);
            }
        }

        /// <summary>
        /// Get the pseudo response corresponding to the ith tree,
        /// which is used to construct the boosted tree
        /// </summary>
        /// <param name="iTree">the index of the tree</param>
        /// <returns>the pseudo response of the ith tree</returns>
        public float[] PseudoResponse(int iTree)
        {
            return this.pseudoResponses;
        }

        /// <summary>
        /// Reducing the training data used to build a boosted tree
        /// </summary>
        /// <param name="inDataSet">The current work training data set</param>
        /// <param name="k">The index of the tree to be built</param>
        /// <param name="m">The iteration of the current boosting process</param>
        /// <returns>The data index used to build the boosted tree after triming</returns>
        public int[] TrimIndex(DataSet inDataSet, int k, int m)
        {
#if !DoTrimIndex
            return inDataSet.DataIndex;
#else
            const float maxTrimRate = 0.8F;
            const int minNonTrimIter = 30;

            float trimQuantile = 0.10F; //Make it scalable to any number of classes
            //float[] trimQuantile = { 0.10F, 0.10F, 0.10F, 0.10F, 0.10F };

            // Weight-trimming discards a portion of samples in the lower quantile of data respect to weights
            // I find this not only speeds up the computations but also outputs better results. 
            int[] trimIndex;
            int[] index = inDataSet.DataIndex;
            if (m < minNonTrimIter)
            {
                trimIndex = index;
            }
            else
            {
                float[] weightsL = new float[index.Length];
                float sumWeights = 0;

                for (int i = 0; i < index.Length; i++)
                {
                    weightsL[i] = this.weights[index[i]];
                    sumWeights += weightsL[i];
                }

                int[] weightIndex = Vector.IndexArray(index.Length);
                Array.Sort(weightsL, weightIndex);

                int trimLen = 0;
                float partialSumWeights = 0;
                float targetSumWeights = trimQuantile * sumWeights - float.Epsilon;
                while (partialSumWeights < targetSumWeights && trimLen < index.Length * maxTrimRate)
                    partialSumWeights += weightsL[trimLen++];

                trimIndex = new int[index.Length - trimLen];

                for (int i = 0; i < trimIndex.Length; i++)
                    trimIndex[i] = index[weightIndex[i + trimLen]];

                // We find empirically that sorting the indexes actually speeds up accessing the data matrix
                Array.Sort(trimIndex);

                if (m >= minNonTrimIter)
                    Console.WriteLine("\t" + k.ToString() + "\t" + (1.0 * trimIndex.Length / index.Length).ToString());
            }
            return trimIndex;     
#endif //!DoTrimIndex
        }

        /// <summary>
        /// Convert the functional value produced by the set of boosted regression trees a final scores of a model
        /// </summary>
        public void FuncValuesToModelScores()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.score[i] = this.funValue[i];
            }
        }

        /// <summary>
        /// Covert the model scores into the funational values corresponding to the set of boosted regression trees
        /// </summary>
        public void ModelScoresToFuncValues()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.funValue[i] = this.score[i];
            }
        }

        /// <summary>
        /// Compute the point-wise pseudo-response of the loss function to be optimized
        /// It is used to build the decision tree - except from computing the response value of a leaf node
        /// </summary>    
        /// <param name="dataSet">all training data</param>
        public void ComputePseudoResponse(DataSet dataSet)
        {
            // Reset/(zero out) pseudoResponse and weights for a new iteration 
            ResetParameters();
            
            for (int qIdx = 0; qIdx < dataSet.NumGroups; qIdx++)
            {
                DataGroup queryGroup = dataSet.GetDataGroup(qIdx);
                RankPairGenerator rankPairs = new RankPairGenerator(queryGroup, this.labels);
                Query query = this.qcAccum.queries[dataSet.GroupIndex[qIdx]]; ;
                query.UpdateScores(this.score, queryGroup.iStart);
                query.ComputeRank();
                foreach (RankPair rankPair in rankPairs)
                {
                    float scoreH_minus_scoreL = this.score[rankPair.IdxH] - this.score[rankPair.IdxL];
                    //compute the cross-entropy gradient of the pair
                    float gradient = RankPair.CrossEntropyDerivative(scoreH_minus_scoreL);
                    //compute the absolute change in NDCG if we swap the pair in the current ordering
                    float absDeltaNDCG = AbsDeltaNDCG(rankPair, queryGroup, query);

                    // Marginalize the pair-wise gradient to get point wise gradient.  The point with higher relevance label (IdxH) always gets
                    // a positive push (i.e. upwards).
                    this.pseudoResponses[rankPair.IdxH] += gradient * absDeltaNDCG;                    
                    this.pseudoResponses[rankPair.IdxL] -= gradient * absDeltaNDCG;
                    
                    // Note that the weights are automatically always positive
                    float weight = absDeltaNDCG * RankPair.CrossEntropy2ndDerivative(this.score[rankPair.IdxH] - this.score[rankPair.IdxL]);
                    this.weights[rankPair.IdxH] += weight;
                    this.weights[rankPair.IdxL] += weight;
                }
            }
            
            for (int i = 0; i < dataSet.NumSamples; i++)
            {
                int dataIdx = dataSet.DataIndex[i];
                //incorporating the gradient of the label
                this.pseudoResponses[dataIdx] = (1 - this.labelWeight) * this.pseudoResponses[dataIdx] + this.labelWeight * (this.labels[dataIdx] - this.score[dataIdx]);
                this.weights[dataIdx] = (1 - this.labelWeight) * this.weights[dataIdx] + this.labelWeight * 1;
            }            
        }

        private float AbsDeltaNDCG(RankPair rankPair, DataGroup queryGroup, Query query)
        {
            int idx1 = rankPair.IdxL - queryGroup.iStart;
            int idx2 = rankPair.IdxH - queryGroup.iStart;
            return query.AbsDeltaNDCG(idx1, idx2);
        }

        private Query CreateQuery(DataGroup queryGroup, float[] inLabels, float[] inScores,
                                  float labelForUnlabeled, double scoreForDegenerateQuery)
        {
            string QID = queryGroup.id.ToString();
            float[] labels = new float[queryGroup.cSize];
            double[] scores = new double[queryGroup.cSize];
            int end = queryGroup.iStart + queryGroup.cSize;
            for (int i = queryGroup.iStart; i < end; i++)
            {
                labels[i - queryGroup.iStart] = (float)inLabels[i];
                scores[i - queryGroup.iStart] = inScores[i];
            }

            DCGScorer.truncLevel = this.truncLevel;
            Query query = new Query(QID, labels, null, scores, labelForUnlabeled, scoreForDegenerateQuery);
            return query;
        }

        private QueryCollection CreateQueryCollection()
        {
            int cQueris = this.labelFeatureDataCoded.DataGroups.GroupCounts;
            Query[] queries = new Query[cQueris];
            for (int qIdx = 0; qIdx < cQueris; qIdx++)
            {
                DataGroup queryGroup = this.labelFeatureDataCoded.DataGroups[qIdx];
                Query query = CreateQuery(queryGroup, this.labels, this.score, this.labelForUnlabeled, this.scoreForDegenerateQuery);
                queries[qIdx] = query;
            }

            bool skipDegenerateQueries = true;

            QueryCollection qc = new QueryCollection(queries, skipDegenerateQueries, this.scoreForDegenerateQuery);

            return qc;
        }

        /// <summary>
        /// computes the response for a sub-region of data specified by dataPoints that optimize the Loss        
        /// </summary>
        /// <param name="iTree">The index of the tree for the leaf node that the response is computed</param>
        /// <returns>the response of the sub-region/leaf-node on the ith tree</returns>
        public float Response(int[] dataPoints, int[] workIndex, int iTree)
        {
            float v;
            switch (this.stepSizeType)
            {
                case StepSizeType.LineSearch:
                    v = LineSearchResponse(dataPoints, workIndex, iTree);
                    break;
                case StepSizeType.Median:
                    v = MedianResponse(dataPoints, workIndex, iTree);
                    break;
                case StepSizeType.Average:
                    v = AverageResponse(dataPoints, workIndex, iTree);
                    break;
                default:
                    v = LineSearchResponse(dataPoints, workIndex, iTree);
                    break;
            }
            return v;
        }
        /// <summary>
        /// return the response approximated by line-step
        /// </summary>
        /// <param name="dataPoints"></param>
        /// <param name="workIndex"></param>
        /// <param name="iTree"></param>
        /// <returns></returns>
        private float LineSearchResponse(int[] dataPoints, int[] workIndex, int iTree)
        {
            float value;

            float tmp1 = 0, tmp2 = 0;
            for (int j = 0; j < dataPoints.Length; j++)
            {
                int orgInd = workIndex[dataPoints[j]];
                tmp1 += this.pseudoResponses[orgInd];
                tmp2 += this.weights[orgInd];
            }

            const float MaxFunVal = 5;
            value = (tmp1 + float.Epsilon) / (tmp2 + float.Epsilon) * this.FunctionValueModifier;
            if (value > MaxFunVal)
                value = MaxFunVal;
            else if (value < -MaxFunVal)
                value = -MaxFunVal;

            return value;
        }

        /// <summary>
        /// return the median pseudo-response value of all the data points in the leaf node region
        /// </summary>
        /// <param name="dataPoints"></param>
        /// <param name="workIndex"></param>
        /// <param name="iTree"></param>
        /// <returns></returns>
        private float MedianResponse(int[] dataPoints, int[] workIndex, int iTree)
        {
            float value;

            float[] tmp = new float[dataPoints.Length];

            for (int j = 0; j < dataPoints.Length; j++)
            {
                int orgInd = workIndex[dataPoints[j]];
                tmp[j] = this.pseudoResponses[orgInd];
            }
            Array.Sort(tmp);

            int i = dataPoints.Length / 2;

            if (i >= dataPoints.Length - 1)
            {
                value = tmp[i];
            }
            else
            {
                value = (tmp[i] + tmp[i + 1]) / 2;
            }

            value = value * this.FunctionValueModifier;
            return value;
        }

        /// <summary>
        /// return the average pseudo-response value of all the data points in the leaf node region
        /// </summary>
        /// <param name="dataPoints"></param>
        /// <param name="workIndex"></param>
        /// <param name="iTree"></param>
        /// <returns></returns>
        private float AverageResponse(int[] dataPoints, int[] workIndex, int iTree)
        {
            float value = 0.0F;

            for (int j = 0; j < dataPoints.Length; j++)
            {
                int orgInd = workIndex[dataPoints[j]];
                value += this.pseudoResponses[orgInd];
            }

            value = (float)(value / dataPoints.Length) * this.FunctionValueModifier;

            return value;
        }

        /// <summary>
        /// Computes an optimal adjustment factor of the new tree - which is muliplied with the response value at each leaf node
        /// </summary>
        /// <param name="funcValueGain">the functional value returned by the current tree for all data points</param>
        /// <returns>the multiplication factor that will minimize the overall cost</returns>
        public float ComputeResponseAdjust(float[] funcValueGain)
        {
            double alpha = 1.0;

#if DoResponseAdjust //flag to turn on/off the step-size search code        
            //Train, Validation, Test,
            //select which data set used to compute the optimal combination
            DataSet dataSetActive = this.labelFeatureDataCoded.DataGroups.GetDataPartition(DataPartitionType.Validation);            

            //qcAccum Contains accumulated scores of all previous trees (or models) for the training set           
            this.qcAccum.UpdateScores(this.score);
            //qcCurren Contains the scores (e.g. mean or median) that the new tree gives to the training set        
            this.qcCurrent.UpdateScores(funcValueGain);
           
            double bestMeanNDCGGain;

            alpha = this.fs.FindStep(qcAccum, qcCurrent, dataSetActive.GroupIndex, out bestMeanNDCGGain);

            // If alpha is too small the whole learning will get stuck: in that case, we should rely on the Newton step size (alpha = 1);
            // on the other hand we know that the optimal step here is small (alpha = 1 is wrong) so go with a lower bound instead.
            // There is likely a better way to do this.
            double thresh = 0.1;
            if (alpha < thresh)
            {
                //Console.WriteLine("Desired alpha = {0}: resetting to {1}", alpha, thresh);
                alpha = thresh;
            }
            
            //else
            //{
            //    Console.WriteLine("alpha = {0}", alpha);
            //}

            //alpha *= 0.1; // Effective learning rate.  Experimental only!
            Console.WriteLine("alpha = {0}", alpha);
#endif // DoResponseAdjust

            return (float)alpha;
        }        

        /// <summary>
        /// the shrinkage of the iterative boosting algorithm
        /// </summary>
        /// <param name="learnRate">the input learning rate of the overall boosting algorithm</param>
        /// <returns>the actual shrinkage given the learning rate</returns>
        private float FunctionValueModifier
        {
            get
            {
                return this.learnRate / 2;
            }
        }

        private void ResetParameters()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.weights[i] = 0.0F;
                this.pseudoResponses[i] = 0.0F;
            }
        }

        //the entire data set
        [NonSerialized]
        private LabelFeatureDataCoded labelFeatureDataCoded = null;
        //the number of sample in the above data set
        [NonSerialized]
        private int numSamples = 0;
        //the labels of all data points
        [NonSerialized]
        private float[] labels = null;
        //the model socres corresponding to all data points
        [NonSerialized]
        private float[] score = null;
        //the function values produced by the boosted trees for all data pounts
        [NonSerialized]
        private float[] funValue = null;
        //the output pseudo-response for all data points 
        [NonSerialized]
        private float[] pseudoResponses = null;
        [NonSerialized]
        private float[] weights = null;

        [NonSerialized]
        private float labelForUnlabeled;
        [NonSerialized]
        private double scoreForDegenerateQuery;
        [NonSerialized]
        private int truncLevel = 100;

        [NonSerialized]
        private float learnRate = 0.0F;

        [NonSerialized]
        private float labelWeight = 0.0F;

        [NonSerialized]
        private StepSizeType stepSizeType = StepSizeType.LineSearch;

        // data members used to compute optimal adjustment factor of newly computed regression tree
        //Todo(qiangwu): currently, I am directly using the existing QueryColletion and QueryData objects;
        //I will do more mergering/refactoring of these classes later on - it shouls be simple since all
        //functions related to QueryCollection and QueryData are tightly encapsulated so that only a few
        //will be effected when I do refactoring later on.
        //qcAccum Contains accumulated scores of all previous trees (or models) for the training set           
        [NonSerialized]
        QueryCollection qcAccum;
        //qcCurren Contains the scores (e.g. mean or median) that the new tree gives to the training set   
        [NonSerialized]
        QueryCollection qcCurrent;
        //module to compute the optimal combination/adjustment factor
        [NonSerialized]
        FindStepLib fs;
    }

    /// <summary>
    /// The encapsulate the lambda loss function
    /// used in building iterative boosted regression tree
    /// but don't include the gains and maxDCG.  For use when training on Google data.
    /// </summary>
    [Serializable]
    public class BoostTreeNoGainLambdaLoss : BoostTreeLoss
    {
        /// <summary>
        /// Constructor of BoostTreeRankNetLoss
        /// </summary>
        /// <param name="dp">the input data used to train boosted regression trees</param>
        /// <param name="learnRate">the input learning rate specified in training</param>
        public BoostTreeNoGainLambdaLoss(LabelFeatureDataCoded labelFeatureDataCoded, LabelConverter labelConvert,
                                   float learnRate, float labelWeight, StepSizeType stepSizeType, FindStepLib fs,
                                   float labelForUnlabeled, double scoreForDegenerateQuery, int truncLevel)
        {
            this.learnRate = learnRate;
            this.labelWeight = labelWeight;

            this.labelForUnlabeled = labelForUnlabeled;
            this.scoreForDegenerateQuery = scoreForDegenerateQuery;
            this.truncLevel = truncLevel;

            this.labels = new float[labelFeatureDataCoded.NumDataPoint];

            for (int i = 0; i < labelFeatureDataCoded.NumDataPoint; i++)
            {
                this.labels[i] = labelConvert.convert(labelFeatureDataCoded.GetLabel(i));
            }

            this.numSamples = labels.Length;

            this.score = new float[this.numSamples];
            this.funValue = new float[this.numSamples];
            this.pseudoResponses = new float[this.numSamples];
            this.weights = new float[this.numSamples];

            this.labelFeatureDataCoded = labelFeatureDataCoded;

            //data member to compute the optimal adjustment factor (step size) for leaf nodes response
            this.qcAccum = this.CreateQueryCollection();
            this.qcCurrent = this.CreateQueryCollection();
            this.fs = fs;
            this.stepSizeType = stepSizeType;
        }

        /// <summary>
        /// How many sets of boosted trees we are building
        /// </summary>                    
        public int NumTreesPerIteration
        {
            get
            {
                return 1;
            }
        }

        /// <summary>
        /// reset the state of a BoostTreeLoss object to accomodate training/testing on data
        /// </summary>
        /// <param name="testData"></param>
        public void Reset(int cSamples)
        {
            this.numSamples = cSamples;

            this.score = new float[this.numSamples];
            this.funValue = new float[this.numSamples];
            this.pseudoResponses = new float[this.numSamples];
            this.weights = new float[this.numSamples];
        }

        /// <summary>
        /// The model scores of all the data points
        /// </summary>
        public float[][] ModelScores
        {
            get
            {
                float[][] modelScores = new float[1][];
                modelScores[0] = this.score;
                return modelScores;
            }
        }

        /// <summary>
        /// Compute and store the scores of the input data given a model
        /// </summary>
        /// <param name="model">the model to be evaluated</param>
        /// <param name="labelFeatureData">input data</param>        
        /// <param name="subModelScore">pre-computed scores for the input data</param>    
        public void ModelEval(Model model, LabelFeatureData labelFeatureData, LabelFeatureData subModelScore)
        {
            if (subModelScore != null)
            {
                Debug.Assert(this.numSamples == subModelScore.NumDataPoint);
                for (int i = 0; i < this.numSamples; i++)
                {
                    this.score[i] = subModelScore.GetFeature(0, i);
                }
            }
            else if (model != null && labelFeatureData != null)
            {
                Debug.Assert(this.numSamples == labelFeatureData.NumDataPoint);
                float[] scores = new float[1];
                for (int i = 0; i < this.numSamples; i++)
                {
                    model.Evaluate(labelFeatureData.GetFeature(i), scores);
                    this.score[i] = scores[0];
                }
            }
            else
            {
                for (int i = 0; i < this.numSamples; i++)
                {
                    this.score[i] = 0.0F;
                }
            }
        }

        /// <summary>
        /// Accumulate the functional values of all the data points from a given tree into classFunValue
        /// </summary>
        /// <param name="funValueGain">Array of size numSamples: contains the functional value gain produced by the iTree-th tree in the current iteration</param>
        /// <param name="adjFactor">Adjust/multiply the functional value gain produced by the iTree-th tree</param>
        /// <param name="iTree">Indexes the set of numClasses trees in a given iteration</param>
        public void AccFuncValueGain(float[] funValueGain, float adjFactor, int iTree)
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.funValue[i] += (funValueGain[i] * adjFactor);
            }
        }

        /// <summary>
        /// Get the pseudo response corresponding to the ith tree,
        /// which is used to construct the boosted tree
        /// </summary>
        /// <param name="iTree">the index of the tree</param>
        /// <returns>the pseudo response of the ith tree</returns>
        public float[] PseudoResponse(int iTree)
        {
            return this.pseudoResponses;
        }

        /// <summary>
        /// Reducing the training data used to build a boosted tree
        /// </summary>
        /// <param name="inDataSet">The current work training data set</param>
        /// <param name="k">The index of the tree to be built</param>
        /// <param name="m">The iteration of the current boosting process</param>
        /// <returns>The data index used to build the boosted tree after triming</returns>
        public int[] TrimIndex(DataSet inDataSet, int k, int m)
        {
#if !DoTrimIndex
            return inDataSet.DataIndex;
#else
            const float maxTrimRate = 0.8F;
            const int minNonTrimIter = 30;
            
            float trimQuantile = 0.10F; //Make it scalable to any number of classes
            //float[] trimQuantile = { 0.10F, 0.10F, 0.10F, 0.10F, 0.10F };

            // Weight-trimming discards a portion of samples in the lower quantile of data respect to weights
            // I find this not only speeds up the computations but also outputs better results. 
            int[] trimIndex;
            int[] index = inDataSet.DataIndex;
            if (m < minNonTrimIter)
            {
                trimIndex = index;
            }
            else
            {
                float[] weightsL = new float[index.Length];
                float sumWeights = 0;

                for (int i = 0; i < index.Length; i++)
                {
                    weightsL[i] = this.weights[index[i]];
                    sumWeights += weightsL[i];
                }

                int[] weightIndex = Vector.IndexArray(index.Length);
                Array.Sort(weightsL, weightIndex);

                int trimLen = 0;
                float partialSumWeights = 0;
                float targetSumWeights = trimQuantile * sumWeights - float.Epsilon;
                while (partialSumWeights < targetSumWeights && trimLen < index.Length * maxTrimRate)
                    partialSumWeights += weightsL[trimLen++];

                trimIndex = new int[index.Length - trimLen];

                for (int i = 0; i < trimIndex.Length; i++)
                    trimIndex[i] = index[weightIndex[i + trimLen]];

                // We find empirically that sorting the indexes actually speeds up accessing the data matrix
                Array.Sort(trimIndex);

                if (m >= minNonTrimIter)
                    Console.WriteLine("\t" + k.ToString() + "\t" + (1.0 * trimIndex.Length / index.Length).ToString());
            }
            return trimIndex;     
#endif //!DoTrimIndex
        }

        /// <summary>
        /// Convert the functional value produced by the set of boosted regression trees a final scores of a model
        /// </summary>
        public void FuncValuesToModelScores()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.score[i] = this.funValue[i];
            }
        }

        /// <summary>
        /// Covert the model scores into the funational values corresponding to the set of boosted regression trees
        /// </summary>
        public void ModelScoresToFuncValues()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.funValue[i] = this.score[i];
            }
        }

        /// <summary>
        /// Compute the point-wise pseudo-response of the loss function to be optimized
        /// It is used to build the decision tree - except from computing the response value of a leaf node
        /// </summary>    
        /// <param name="dataSet">all training data</param>
        public void ComputePseudoResponse(DataSet dataSet)
        {
            // Reset/(zero out) pseudoResponse and weights for a new iteration 
            ResetParameters();

            for (int qIdx = 0; qIdx < dataSet.NumGroups; qIdx++)
            {
                DataGroup queryGroup = dataSet.GetDataGroup(qIdx);
                RankPairGenerator rankPairs = new RankPairGenerator(queryGroup, this.labels);
                Query query = this.qcAccum.queries[dataSet.GroupIndex[qIdx]]; ;
                query.UpdateScores(this.score, queryGroup.iStart);
                query.ComputeRank();
                foreach (RankPair rankPair in rankPairs)
                {
                    float scoreH_minus_scoreL = this.score[rankPair.IdxH] - this.score[rankPair.IdxL];
                    //compute the cross-entropy gradient of the pair
                    float gradient = RankPair.CrossEntropyDerivative(scoreH_minus_scoreL);
                    //compute the absolute change in NDCG if we swap the pair in the current ordering
                    float absDeltaPosition = AbsDeltaPosition(rankPair, queryGroup, query);

                    // Marginalize the pair-wise gradient to get point wise gradient.  The point with higher relevance label (IdxH) always gets
                    // a positive push (i.e. upwards).
                    this.pseudoResponses[rankPair.IdxH] += gradient * absDeltaPosition;
                    this.pseudoResponses[rankPair.IdxL] -= gradient * absDeltaPosition;

                    // Note that the weights are automatically always positive
                    float weight = absDeltaPosition * RankPair.CrossEntropy2ndDerivative(this.score[rankPair.IdxH] - this.score[rankPair.IdxL]);
                    this.weights[rankPair.IdxH] += weight;
                    this.weights[rankPair.IdxL] += weight;
                }
            }

            for (int i = 0; i < dataSet.NumSamples; i++)
            {
                int dataIdx = dataSet.DataIndex[i];
                //incorporating the gradient of the label
                this.pseudoResponses[dataIdx] = (1 - this.labelWeight) * this.pseudoResponses[dataIdx] + this.labelWeight * (this.labels[dataIdx] - this.score[dataIdx]);
                this.weights[dataIdx] = (1 - this.labelWeight) * this.weights[dataIdx] + this.labelWeight * 1;
            }
        }

        private float AbsDeltaPosition(RankPair rankPair, DataGroup queryGroup, Query query)
        {
            int idx1 = rankPair.IdxL - queryGroup.iStart;
            int idx2 = rankPair.IdxH - queryGroup.iStart;
            return query.AbsDeltaPosition(idx1, idx2);
        }

        private Query CreateQuery(DataGroup queryGroup, float[] inLabels, float[] inScores,
                                  float labelForUnlabeled, double scoreForDegenerateQuery)
        {
            string QID = queryGroup.id.ToString();
            float[] labels = new float[queryGroup.cSize];
            double[] scores = new double[queryGroup.cSize];
            int end = queryGroup.iStart + queryGroup.cSize;
            for (int i = queryGroup.iStart; i < end; i++)
            {
                labels[i - queryGroup.iStart] = (float)inLabels[i];
                scores[i - queryGroup.iStart] = inScores[i];
            }

            DCGScorer.truncLevel = this.truncLevel;
            //Query query = new Query(QID, labels, null, scores, labelForUnlabeled, scoreForDegenerateQuery);
            Query query = new Query(QID, labels, null, scores, labelForUnlabeled, scoreForDegenerateQuery, false);
            return query;
        }

        private QueryCollection CreateQueryCollection()
        {
            int cQueris = this.labelFeatureDataCoded.DataGroups.GroupCounts;
            Query[] queries = new Query[cQueris];
            for (int qIdx = 0; qIdx < cQueris; qIdx++)
            {
                DataGroup queryGroup = this.labelFeatureDataCoded.DataGroups[qIdx];
                Query query = CreateQuery(queryGroup, this.labels, this.score, this.labelForUnlabeled, this.scoreForDegenerateQuery);
                queries[qIdx] = query;
            }

            bool skipDegenerateQueries = true;

            QueryCollection qc = new QueryCollection(queries, skipDegenerateQueries, this.scoreForDegenerateQuery);

            return qc;
        }

        /// <summary>
        /// computes the response for a sub-region of data specified by dataPoints that optimize the Loss        
        /// </summary>
        /// <param name="iTree">The index of the tree for the leaf node that the response is computed</param>
        /// <returns>the response of the sub-region/leaf-node on the ith tree</returns>
        public float Response(int[] dataPoints, int[] workIndex, int iTree)
        {
            float v;
            switch (this.stepSizeType)
            {
                case StepSizeType.LineSearch:
                    v = LineSearchResponse(dataPoints, workIndex, iTree);
                    break;
                case StepSizeType.Median:
                    v = MedianResponse(dataPoints, workIndex, iTree);
                    break;
                case StepSizeType.Average:
                    v = AverageResponse(dataPoints, workIndex, iTree);
                    break;
                default:
                    v = LineSearchResponse(dataPoints, workIndex, iTree);
                    break;
            }
            return v;
        }
        /// <summary>
        /// return the response approximated by line-step
        /// </summary>
        /// <param name="dataPoints"></param>
        /// <param name="workIndex"></param>
        /// <param name="iTree"></param>
        /// <returns></returns>
        private float LineSearchResponse(int[] dataPoints, int[] workIndex, int iTree)
        {
            float value;

            float tmp1 = 0, tmp2 = 0;
            for (int j = 0; j < dataPoints.Length; j++)
            {
                int orgInd = workIndex[dataPoints[j]];
                tmp1 += this.pseudoResponses[orgInd];
                tmp2 += this.weights[orgInd];
            }

            const float MaxFunVal = 5;
            value = (tmp1 + float.Epsilon) / (tmp2 + float.Epsilon) * this.FunctionValueModifier;
            if (value > MaxFunVal)
                value = MaxFunVal;
            else if (value < -MaxFunVal)
                value = -MaxFunVal;

            return value;
        }

        /// <summary>
        /// return the median pseudo-response value of all the data points in the leaf node region
        /// </summary>
        /// <param name="dataPoints"></param>
        /// <param name="workIndex"></param>
        /// <param name="iTree"></param>
        /// <returns></returns>
        private float MedianResponse(int[] dataPoints, int[] workIndex, int iTree)
        {
            float value;

            float[] tmp = new float[dataPoints.Length];

            for (int j = 0; j < dataPoints.Length; j++)
            {
                int orgInd = workIndex[dataPoints[j]];
                tmp[j] = this.pseudoResponses[orgInd];
            }
            Array.Sort(tmp);

            int i = dataPoints.Length / 2;

            if (i >= dataPoints.Length - 1)
            {
                value = tmp[i];
            }
            else
            {
                value = (tmp[i] + tmp[i + 1]) / 2;
            }

            value = value * this.FunctionValueModifier;
            return value;
        }

        /// <summary>
        /// return the average pseudo-response value of all the data points in the leaf node region
        /// </summary>
        /// <param name="dataPoints"></param>
        /// <param name="workIndex"></param>
        /// <param name="iTree"></param>
        /// <returns></returns>
        private float AverageResponse(int[] dataPoints, int[] workIndex, int iTree)
        {
            float value = 0.0F;

            for (int j = 0; j < dataPoints.Length; j++)
            {
                int orgInd = workIndex[dataPoints[j]];
                value += this.pseudoResponses[orgInd];
            }

            value = (float)(value / dataPoints.Length) * this.FunctionValueModifier;

            return value;
        }

        /// <summary>
        /// Computes an optimal adjustment factor of the new tree - which is muliplied with the response value at each leaf node
        /// </summary>
        /// <param name="funcValueGain">the functional value returned by the current tree for all data points</param>
        /// <returns>the multiplication factor that will minimize the overall cost</returns>
        public float ComputeResponseAdjust(float[] funcValueGain)
        {
            double alpha = 1.0;

#if DoResponseAdjust //flag to turn on/off the step-size search code        
            //Train, Validation, Test,
            //select which data set used to compute the optimal combination
            DataSet dataSetActive = this.labelFeatureDataCoded.DataGroups.GetDataPartition(DataPartitionType.Validation);            

            //qcAccum Contains accumulated scores of all previous trees (or models) for the training set           
            this.qcAccum.UpdateScores(this.score);
            //qcCurren Contains the scores (e.g. mean or median) that the new tree gives to the training set        
            this.qcCurrent.UpdateScores(funcValueGain);
           
            double bestMeanNDCGGain;

            alpha = this.fs.FindStep(qcAccum, qcCurrent, dataSetActive.GroupIndex, out bestMeanNDCGGain);

            // If alpha is too small the whole learning will get stuck: in that case, we should rely on the Newton step size (alpha = 1);
            // on the other hand we know that the optimal step here is small (alpha = 1 is wrong) so go with a lower bound instead.
            // There is likely a better way to do this.
            double thresh = 0.1;
            if (alpha < thresh)
            {
                //Console.WriteLine("Desired alpha = {0}: resetting to {1}", alpha, thresh);
                alpha = thresh;
            }
            
            //else
            //{
            //    Console.WriteLine("alpha = {0}", alpha);
            //}

            //alpha *= 0.1; // Effective learning rate.  Experimental only!
            Console.WriteLine("alpha = {0}", alpha);
#endif // DoResponseAdjust

            return (float)alpha;
        }

        /// <summary>
        /// the shrinkage of the iterative boosting algorithm
        /// </summary>
        /// <param name="learnRate">the input learning rate of the overall boosting algorithm</param>
        /// <returns>the actual shrinkage given the learning rate</returns>
        private float FunctionValueModifier
        {
            get
            {
                return this.learnRate / 2;
            }
        }

        private void ResetParameters()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.weights[i] = 0.0F;
                this.pseudoResponses[i] = 0.0F;
            }
        }

        //the entire data set
        [NonSerialized]
        private LabelFeatureDataCoded labelFeatureDataCoded = null;
        //the number of sample in the above data set
        [NonSerialized]
        private int numSamples = 0;
        //the labels of all data points
        [NonSerialized]
        private float[] labels = null;
        //the model socres corresponding to all data points
        [NonSerialized]
        private float[] score = null;
        //the function values produced by the boosted trees for all data pounts
        [NonSerialized]
        private float[] funValue = null;
        //the output pseudo-response for all data points 
        [NonSerialized]
        private float[] pseudoResponses = null;
        [NonSerialized]
        private float[] weights = null;

        [NonSerialized]
        private float labelForUnlabeled;
        [NonSerialized]
        private double scoreForDegenerateQuery;
        [NonSerialized]
        private int truncLevel = 100;

        [NonSerialized]
        private float learnRate = 0.0F;

        [NonSerialized]
        private float labelWeight = 0.0F;

        [NonSerialized]
        private StepSizeType stepSizeType = StepSizeType.LineSearch;

        // data members used to compute optimal adjustment factor of newly computed regression tree
        //Todo(qiangwu): currently, I am directly using the existing QueryColletion and QueryData objects;
        //I will do more mergering/refactoring of these classes later on - it shouls be simple since all
        //functions related to QueryCollection and QueryData are tightly encapsulated so that only a few
        //will be effected when I do refactoring later on.
        //qcAccum Contains accumulated scores of all previous trees (or models) for the training set           
        [NonSerialized]
        QueryCollection qcAccum;
        //qcCurren Contains the scores (e.g. mean or median) that the new tree gives to the training set   
        [NonSerialized]
        QueryCollection qcCurrent;
        //module to compute the optimal combination/adjustment factor
        [NonSerialized]
        FindStepLib fs;
    }

    /// <summary>
    /// Class for least squares (L1) and least absolute deviation (L2) regression (CJCB)
    /// </summary>
    [Serializable]
    public abstract class BoostTreeRegressionLoss : BoostTreeLoss
    {      
        public BoostTreeRegressionLoss(LabelFeatureDataCoded labelFeatureDataCoded, 
                                       float learnRate)
        {
            this.learnRate = learnRate;
  
            this.numSamples = labelFeatureDataCoded.NumDataPoint;
            this.labels = new float[labelFeatureDataCoded.NumDataPoint];

            //for (int i = 0; i < labelFeatureDataCoded.NumDataPoint; i++)
            //{
            //    this.labels[i] = labelConvert.convert(labelFeatureDataCoded.GetLabel(i));
            //}
            for (int i = 0; i < labelFeatureDataCoded.NumDataPoint; i++)
            {
                this.labels[i] = labelFeatureDataCoded.GetLabel(i);
            }

            this.scores = new float[this.numSamples];
            this.funValues = new float[this.numSamples];
            this.pseudoResponses = new float[this.numSamples];
            //this.weights = new float[this.numSamples];

            this.labelFeatureDataCoded = labelFeatureDataCoded;
        }

        /// <summary>
        /// How many sets of boosted trees we are building
        /// </summary>                    
        public int NumTreesPerIteration
        {
            get
            {
                return 1;
            }
        }

        /// <summary>
        /// reset the state of a BoostTreeLoss object to accomodate training/testing on data
        /// </summary>
        /// <param name="testData"></param>
        public void Reset(int cSamples)
        {
            this.numSamples = cSamples;

            this.scores = new float[this.numSamples];
            this.funValues = new float[this.numSamples];
            this.pseudoResponses = new float[this.numSamples];
            //this.weights = new float[this.numSamples];
        }

        /// <summary>
        /// The model scores of all the data points
        /// </summary>
        public float[][] ModelScores
        {
            get
            {
                float[][] modelScores = new float[1][];
                modelScores[0] = this.scores;
                return modelScores;
            }
        }

        /// <summary>
        /// Compute and store the scores of the input data given a model
        /// </summary>
        /// <param name="model">the model to be evaluated</param>
        /// <param name="data">input data</param>
        public void ModelEval(Model model, float[][] data)
        {
            Debug.Assert(this.numSamples == data.Length);

        }

        /// <summary>
        /// Compute and store the scores of the input data given a model
        /// </summary>
        /// <param name="model">the model to be evaluated</param>
        /// <param name="labelFeatureData">input data</param>        
        /// <param name="subModelScore">pre-computed scores for the input data</param>    
        public void ModelEval(Model model, LabelFeatureData labelFeatureData, LabelFeatureData subModelScore)
        {
            if (subModelScore != null)
            {
                Debug.Assert(this.numSamples == subModelScore.NumDataPoint);
                for (int i = 0; i < this.numSamples; i++)
                {
                    this.scores[i] = subModelScore.GetFeature(0, i);
                }
            }
            else if (model != null && labelFeatureData != null)
            {
                Debug.Assert(this.numSamples == labelFeatureData.NumDataPoint);
                float[] scores = new float[1];
                for (int i = 0; i < this.numSamples; i++)
                {
                    model.Evaluate(labelFeatureData.GetFeature(i), scores);
                    this.scores[i] = scores[0];
                }
            }
            else
            {
                for (int i = 0; i < this.numSamples; i++)
                {
                    this.scores[i] = 0.0F;
                }
            }
        }

        /// <summary>
        /// computes an optiomal adjustment factor of the new tree - which is muliplied with the response value at each leaf node
        /// </summary>
        /// <param name="funcValueGain">the functional value returned by the current tree for all data points</param>
        /// <returns>the multiplication factor that will minimize the overall cost</returns>
        public float ComputeResponseAdjust(float[] funcValueGain)
        {
            return 1.0F;
        }

        /// <summary>
        /// Accumulate the functional values of all the data points from a given tree into classFunValue
        /// </summary>
        /// <param name="funValueGain">Array of size numSamples: contains the functional value gain produced by the iTree-th tree in the current iteration</param>
        /// <param name="adjFactor">Adjust/multiply the functional value gain produced by the iTree-th tree</param>
        /// <param name="iTree">Indexes the set of numClasses trees in a given iteration</param>
        public void AccFuncValueGain(float[] funValueGain, float adjFactor, int iTree)
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.funValues[i] += (funValueGain[i] * adjFactor);
            }
        }

        /// <summary>
        /// Get the pseudo response corresponding to the ith tree,
        /// which is used to construct the boosted tree
        /// </summary>
        /// <param name="iTree">the index of the tree</param>
        /// <returns>the pseudo response of the ith tree</returns>
        public float[] PseudoResponse(int iTree)
        {
            return this.pseudoResponses;
        }

        /// <summary>
        /// Reducing the training data used to build a boosted tree
        /// </summary>
        /// <param name="inDataSet">The current work training data set</param>
        /// <param name="k">The index of the tree to be built</param>
        /// <param name="m">The iteration of the current boosting process</param>
        /// <returns>The data index used to build the boosted tree after triming</returns>
        public int[] TrimIndex(DataSet inDataSet, int k, int m)
        {
            return inDataSet.DataIndex;
        }

        /// <summary>
        /// Convert the functional value produced by the set of boosted regression trees a final scores of a model
        /// </summary>
        public void FuncValuesToModelScores()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.scores[i] = this.funValues[i];
            }
        }
       
        /// <summary>
        /// Covert the model scores into the funational values corresponding to the set of boosted regression trees
        /// </summary>
        public void ModelScoresToFuncValues()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                this.funValues[i] = this.scores[i];
            }
        }

        /// <summary>
        /// Compute the point-wise pseudo-response of the loss function to be optimized
        /// It is used to build the decision tree - except from computing the response value of a leaf node
        /// </summary>       
        /// <param name="dataSet">all training data</param>
        public abstract void ComputePseudoResponse(DataSet dataSet);

        /// <summary>
        /// computes the response for a sub-region of data specified by dataPoints that optimonize the Loss        
        /// </summary>
        /// <param name="iTree">The index of the tree for the leaf node that the response is computed</param>
        /// <returns>the response of the sub-region/leaf-node on the ith tree</returns>
        public float Response(int[] dataPoints, int[] workIndex, int iTree)
        {
            return LineSearchResponse(dataPoints, workIndex, iTree);
        }

        /// <summary>
        /// Compute the least squares line search and the region value, which is the mean of the data values in that region.
        /// Note that no separate line search is needed for least squares - computing the mean region value already minimizes
        /// the objective fn.
        /// </summary>
        /// <param name="dataPoints"></param>
        /// <param name="workIndex"></param>
        /// <param name="iTree"></param>
        /// <returns></returns>
        protected abstract float LineSearchResponse(int[] dataPoints, int[] workIndex, int iTree);

        /// <summary>
        /// the shrinkage of the iterative boosting algorithm
        /// </summary>
        /// <param name="learnRate">the input learning rate of the overall boosting algorithm</param>
        /// <returns>the actual shrinkage given the learning rate</returns>
        //private float FunctionValueModifier
        //{
        //    get
        //    {
        //        return this.learnRate / 2;
        //    }
        //}

        protected void ResetParameters()
        {
            for (int i = 0; i < this.numSamples; i++)
            {
                //this.weights[i] = 0.0F;
                this.pseudoResponses[i] = 0.0F;
            }
        }

        //the entire data set
        [NonSerialized]
        private LabelFeatureDataCoded labelFeatureDataCoded = null;
        //the number of sample in the above data set
        [NonSerialized]
        private int numSamples = 0;

        protected float learnRate = 0.0F;

        //the labels of all data points
        [NonSerialized]
        protected float[] labels = null;
        //the model scores corresponding to all data points
        [NonSerialized]
        private float[] scores = null;
        //the function values produced by the boosted trees for all data pounts
        [NonSerialized]
        protected float[] funValues = null;
        //the output pseudo-response for all data points 
        [NonSerialized]
        protected float[] pseudoResponses = null;
        //[NonSerialized]
        //private float[] weights = null;

    }

    [Serializable]
    public class BoostTreeRegressionL2Loss : BoostTreeRegressionLoss
    {
        public BoostTreeRegressionL2Loss(LabelFeatureDataCoded labelFeatureDataCoded,
                                       float learnRate)
            : base(labelFeatureDataCoded, learnRate)
        {
        }

        public override void ComputePseudoResponse(DataSet dataSet)
        {
            this.ResetParameters();

            for (int j = 0; j < dataSet.NumSamples; j++)
            {
                int iDoc = dataSet.DataIndex[j];
                this.pseudoResponses[iDoc] = this.labels[iDoc] - this.funValues[iDoc];
            }
        }

        protected override float LineSearchResponse(int[] dataPoints, int[] workIndex, int iTree)
        {
            float value = 0;
            float h = 0;
            for (int j = 0; j < dataPoints.Length; j++)
            {
                int orgInd = workIndex[dataPoints[j]];
                float ybar_i = this.pseudoResponses[orgInd];
                h += ybar_i;
            }
            h /= dataPoints.Length;

            const float MaxFunVal = 5;
            value = h * this.learnRate;
            if (value > MaxFunVal)
                value = MaxFunVal;
            else if (value < -MaxFunVal)
                value = -MaxFunVal;

            return value;
        }
    }

    [Serializable]
    public class BoostTreeRegressionL1Loss : BoostTreeRegressionLoss
    {
        public BoostTreeRegressionL1Loss(LabelFeatureDataCoded labelFeatureDataCoded,
                                       float learnRate)
            : base(labelFeatureDataCoded, learnRate)
        {
        }

        public override void ComputePseudoResponse(DataSet dataSet)
        {
            this.ResetParameters();

            for (int j = 0; j < dataSet.NumSamples; j++)
            {
                int iDoc = dataSet.DataIndex[j];
                // Careful: set the gradient at zero to zero.  Oterhwise we're asking the tree to correct for something that's not an error.
                float delta = this.labels[iDoc] - this.funValues[iDoc];
                if (delta > 0)
                    this.pseudoResponses[iDoc] = 1;
                else if (delta < 0)
                    this.pseudoResponses[iDoc] = -1;
                else
                    this.pseudoResponses[iDoc] = 0;
            }
        }

        protected override float LineSearchResponse(int[] dataPoints, int[] workIndex, int iTree)
        {
            float value = 0;
            float h = 0;

            if (dataPoints.Length == 0)
                return 0;

            float[] weightedDeltas = new float[dataPoints.Length]; // REVIEW - this is slow: allocate once? - CJCB
//            Console.Write("Pseudoresponses: ");
            for (int j = 0; j < dataPoints.Length; j++)
            {
                int orgInd = workIndex[dataPoints[j]];
                float ybar_i = this.pseudoResponses[orgInd];
//                Console.Write(" {0:F4}", ybar_i);
                h += ybar_i;
                weightedDeltas[j] = this.labels[orgInd] - this.funValues[orgInd];
            }

            if (h == 0)
                return 0; // Then line step, learn rate don't matter

            h /= dataPoints.Length;
//            Console.WriteLine();

            // See Friedman99 p. 6.  The sum is minimized when rho * h is the median of the labels - fn values.  
            //if (h < 0)
            //    for (int j = 0; j < dataPoints.Length; ++j)
            //        weightedDeltas[j] = -weightedDeltas[j];

            const float MaxFunVal = 5;
            float rho_h = Vector.Median(weightedDeltas); // rho times h


//            Console.WriteLine("rho times h = {0}", rho_h);
//            Console.WriteLine("Num data points = {0}", dataPoints.Length);
//            for (int j = 0; j < dataPoints.Length; ++j)
//                Console.Write("{0:F2} ", weightedDeltas[j]);
//            Console.WriteLine();
            
            value = rho_h * this.learnRate;
            if (value > MaxFunVal)
                value = MaxFunVal;
            else if (value < -MaxFunVal)
                value = -MaxFunVal;

            return value;
        }
    }

}
