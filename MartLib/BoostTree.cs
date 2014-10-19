//#define GET_PER_DOC_PER_ITER_SCORES
#define VERBOSE
using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.TMSN;



namespace StochasticGradientBoost
{
    /// <summary>
    /// Class for building boosting tree
    /// We implement the stochastic gradient boosting algorithm developed by Jerry Friedman
    /// </summary>
    [Serializable]
    public class BoostTree
    {
        /// <summary>
        /// Retrieve training data, class labels and other information
        /// </summary>
        private void UnpackData()
        {
            if (this.labelFeatureDataCoded == null)
                return;

            this.numFeatures = this.labelFeatureDataCoded.NumFeatures;
            this.numSamples = this.labelFeatureDataCoded.NumDataPoint;

            this.dataColRange = new int[this.numFeatures];
            for (int i = 0; i < this.numFeatures; i++)
                this.dataColRange[i] = this.labelFeatureDataCoded.GetCodeRange(i);
        }

        // The data are preprocessed (quantized) to avoid sorting
        public BoostTree(LabelFeatureDataCoded labelFeatureDataCoded, LabelFeatureData subModelScore,
                        Model subModel, BoostTreeLoss boostTreeLoss,
                        string saveTreeBinFile, string saveTreeTextFile)
        {
            this.labelFeatureDataCoded = labelFeatureDataCoded;
            this.subModelScore = subModelScore;

            UnpackData();

            this.subModel = subModel;
            this.boostTreeLoss = boostTreeLoss;
            this.featureNames = labelFeatureDataCoded.FeatureNames;

            this.saveTreeBinFile = saveTreeBinFile;
            this.saveTreeTextFile = saveTreeTextFile;
            this.saveTreeXmlFile = saveTreeTextFile + ".xml";
        }

        public void Build(Metrics metrics, DataFeatureSampleRate dataFeatureSampleRate,
                          int maxTreeSize, int minNumSamples, int numIter,
                          int cThreads, Random r)
        {
            this.regressionTrees = new RegressionTree[numIter, this.boostTreeLoss.NumTreesPerIteration];

            this.tempSpace = new TempSpace(this.numSamples);

            BuildBoostTree(metrics, this.boostTreeLoss, dataFeatureSampleRate, maxTreeSize, minNumSamples, numIter, cThreads, r);
            SaveBoostTree();
        }

        public void DistributedBuild(Metrics metrics, DataFeatureSampleRate dataFeatureSampleRate,
                          int maxTreeSize, int minNumSamples, int numIter,
                          int cThreads, Random r)
        {
            this.regressionTrees = new RegressionTree[numIter, this.boostTreeLoss.NumTreesPerIteration];

            this.tempSpace = new TempSpace(this.numSamples);

            DistributedBuildBoostTree(metrics, this.boostTreeLoss, dataFeatureSampleRate, maxTreeSize, minNumSamples, numIter, cThreads, r);
            SaveBoostTree();
        }

        public void Initialize(Metrics metrics, int numIter, int cThreads)
        {
            this.regressionTrees = new RegressionTree[numIter, this.boostTreeLoss.NumTreesPerIteration];

            this.tempSpace = new TempSpace(this.numSamples);

            //moved the following functions out
            //DistributedBuildBoostTree(metrics, this.boostTreeLoss, dataFeatureSampleRate, maxTreeSize, minNumSamples, numIter, cThreads, r);
            //SaveBoostTree();

            //float minValidationErr = 100;
            //float[] funValueGain = new float[this.numSamples];

            //(1) compute scores produced by the sub-model
            boostTreeLoss.ModelEval(this.subModel, this.labelFeatureDataCoded, this.subModelScore);

            //(2) compute the corresponding function values;
            boostTreeLoss.ModelScoresToFuncValues();

            //(3) compute the metrics of the sub-model
            int m = optIter = 0;

            metrics.ComputeMetrics(boostTreeLoss.ModelScores, m, false);
#if VERBOSE
            Console.WriteLine(metrics.ResultsHeaderStr());
            Console.WriteLine(metrics.ResultsStr(m));
#endif
            //(4) creat samplers to sub-sampl the features and data during node spliting
            //RandomSampler featureSampler = new RandomSampler(r);
            //RandomSampler dataSampler = new RandomSampler(r);

            //(5) creat the object that does node splitting
#if SINGLE_THREAD
            // single-threaded
             this.findSplit = new FindSplitSync();
#else
            // multi-threaded
            this.findSplit = new FindSplitAsync(cThreads);
#endif //SINGLE_THREAD

        }

        public Model SubModel
        {
            set
            {
                subModel = value;
            }
            get
            {
                return subModel;
            }
        }

        public BoostTreeLoss BoostTreeLoss
        {
            get
            {
                return boostTreeLoss;
            }
        }

        public bool SetFeatureNames(string[] featureNames)
        {
            bool fReMap = false;
            if (this.FeatureNames.Length != featureNames.Length)
            {
                fReMap = true;
            }

            for (int i = 0; i < featureNames.Length && !fReMap; i++)
            {
                if (this.FeatureNames[i] != featureNames[i])
                {
                    fReMap = true;
                }
            }

            if (fReMap)
            {
                this.Train2TestIdx = new int[this.FeatureNames.Length];
                for (int i = 0; i < Train2TestIdx.Length; i++)
                {
                    this.Train2TestIdx[i] = -1; //the feature does not exis in test data
                    for (int j = 0; j < featureNames.Length; j++)
                    {
                        if (string.Compare(this.FeatureNames[i], featureNames[j], true) == 0)
                        {
                            this.Train2TestIdx[i] = j;
                        }
                    }
                }
            }

            if (this.SubModel != null)
            {
                return this.SubModel.SetFeatureNames(featureNames);
            }

            return true;
        }

        /// <summary>
        /// This method implements the main functionality of stochastic gradient boosting, for distributed computing
        /// </summary>
        private void DistributedBuildBoostTree(Metrics metrics, BoostTreeLoss boostTreeLoss, DataFeatureSampleRate dataFeatureSampleRate,
                                    int maxTreeSize, int minNumSamples, int numIter,
                                    int cThreads, Random r)
        {
            float minValidationErr = 100;

            float[] funValueGain = new float[this.numSamples];

            //(1) compute scores produced by the sub-model
            boostTreeLoss.ModelEval(this.subModel, this.labelFeatureDataCoded, this.subModelScore);

            //(2) compute the corresponding function values;
            boostTreeLoss.ModelScoresToFuncValues();

            //(3) compute the metrics of the sub-model
            int m = optIter = 0;
            metrics.ComputeMetrics(boostTreeLoss.ModelScores, m, false);
#if VERBOSE
            Console.WriteLine(metrics.ResultsHeaderStr());
            Console.WriteLine(metrics.ResultsStr(m));
#endif
            //(4) creat samplers to sub-sampl the features and data during node spliting
            RandomSampler featureSampler = new RandomSampler(r);
            RandomSampler dataSampler = new RandomSampler(r);

            //(5) creat the object that does node splitting
#if SINGLE_THREAD
            // single-threaded
             this.findSplit = new FindSplitSync();
#else
            // multi-threaded
            this.findSplit = new FindSplitAsync(cThreads);
#endif //SINGLE_THREAD

            //(6) Iteratively building boosted trees
            for (m = 0; m < numIter; m++)
            {
                //returns array of regression trees (one per class k) for this iteration
                RegressionTree[] candidateTree = GetNextWeakLearner(m, funValueGain, metrics,boostTreeLoss,dataFeatureSampleRate, dataSampler, featureSampler, maxTreeSize,minNumSamples,cThreads,r);

                AddWeakLearner(candidateTree, funValueGain, m, metrics, boostTreeLoss, dataFeatureSampleRate, maxTreeSize, minNumSamples, cThreads, r);

                //compute the metrics of the current system
                boostTreeLoss.FuncValuesToModelScores();
                metrics.ComputeMetrics(boostTreeLoss.ModelScores, m + 1, false);
#if VERBOSE
                Console.WriteLine(metrics.ResultsStr(m + 1));
#endif
                //keep track of the best (minimal Error) iteration on the Validation data set
                this.optIter = metrics.GetBest(DataPartitionType.Validation, ref minValidationErr);

                if ((m + 1) % 5 == 0)  // save the tree every 5 iterations
                    SaveBoostTree();
            }

            if (this.findSplit != null)
            {
                this.findSplit.Cleanup();
            }
        }

        public RegressionTree[] GetNextWeakLearner(int m, float[] funValueGain, Metrics metrics, BoostTreeLoss boostTreeLoss, DataFeatureSampleRate dataFeatureSampleRate, RandomSampler dataSampler, RandomSampler featureSampler,
                                    int maxTreeSize, int minNumSamples, int cThreads, Random r)
        {
            // select a fraction of data groups for this iteration
            float sampleRate = dataFeatureSampleRate.SampleDataGroupRate(m);
            DataSet workDataSet = this.labelFeatureDataCoded.DataGroups.GetDataPartition(DataPartitionType.Train, sampleRate, r);
            workDataSet.Sort();  // sorting gains some noticable speedup.

            // compute the pseudo response of the current system
            boostTreeLoss.ComputePseudoResponse(workDataSet);

            //set the data and feature sampling rate for node spliting in this iteration
            featureSampler.SampleRate = dataFeatureSampleRate.SampleFeatureRate(m);
            dataSampler.SampleRate = dataFeatureSampleRate.SampleDataRate(m);

            // fit a residual model (regression trees) from the pseudo response
            // to compensate the error of the current system

            RegressionTree[] newTree = new RegressionTree[boostTreeLoss.NumTreesPerIteration];

            for (int k = 0; k < boostTreeLoss.NumTreesPerIteration; k++)
            {
                //only use the important data points if necessary
                int[] trimIndex = boostTreeLoss.TrimIndex(workDataSet, k, m);

                //build a regression tree according to the pseduo-response
                newTree[k] = new RegressionTree(this.labelFeatureDataCoded, boostTreeLoss, k, trimIndex,
                                                                dataSampler, featureSampler, maxTreeSize, minNumSamples, this.findSplit, this.tempSpace);

                //compute the function value of all data points produced by the newly generated regression tree
                newTree[k].PredictFunValue(this.labelFeatureDataCoded, ref funValueGain);

                //try to do a more global optimalization - refine the leaf node response of a decision tree
                //by looking at all the training data points, instead of only the ones falling into the regaion.
                //Here we are estimate and apply a global mutiplication factor for all leaf nodes
                float adjFactor = (m > 0) ? boostTreeLoss.ComputeResponseAdjust(funValueGain) : 1.0F;

                //apply the multiplication factor to the leaf nodes of the newly generated regression tree
                newTree[k].AdjustResponse(adjFactor);
                newTree[k].AdjustFactor = adjFactor;
            }

            //return the k regression trees
            return newTree;
        }

        public void AddWeakLearner(RegressionTree[] candidateTree, float[] funValueGain, int m, Metrics metrics, BoostTreeLoss boostTreeLoss, DataFeatureSampleRate dataFeatureSampleRate, int maxTreeSize, int minNumSamples, int cThreads, Random r)
        {
            //update the function value for all data points given the new regression tree
            for (int i = 0; i < boostTreeLoss.NumTreesPerIteration; i++)
            {
                candidateTree[i].PredictFunValue(this.labelFeatureDataCoded, true, ref funValueGain);

                this.regressionTrees[m, i] = candidateTree[i];
                boostTreeLoss.AccFuncValueGain(funValueGain, candidateTree[i].AdjustFactor, i);
            }
        }

        public double EvaluateWeakLearner(RegressionTree[] candidateTree, float[] funValueGain, Metrics metrics, BoostTreeLoss boostTreeLoss, int id)
        {
            float[][] scores = new float[boostTreeLoss.NumTreesPerIteration][];
            for (int k = 0; k < boostTreeLoss.NumTreesPerIteration; k++)
            {
                scores[k] = new float[funValueGain.GetLength(0)];
                for (int i = 0; i < funValueGain.GetLength(0); i++)
                {
                    scores[k][i] = 0.0F;
                }
            }

            double result = 0.0;

            for (int k = 0; k < boostTreeLoss.NumTreesPerIteration; k++)
            {

                candidateTree[k].PredictFunValue(this.labelFeatureDataCoded, true, ref funValueGain);

                //we hard code here that k=0 (not performing classification)
                //kms: this is a bit of hack...only will really work for non-classification currently
                // upgrade to have a per loss function evaluation
                for (int i = 0; i < funValueGain.GetLength(0); i++)
                {
                    scores[k][i] =  boostTreeLoss.ModelScores[k][i] +(funValueGain[i] * candidateTree[k].AdjustFactor);
                }
            }

            //need to update id so we have unique id.  For now, we take M + m + 1;
            //assume only want NDCGPairwise for now
            metrics.ComputeMetrics(scores, id, false);
            //NDCGPairwiseType = 2;
            result = metrics.ResultsStrMatrix(id)[(int)DataPartitionType.Train][2];
            //Console.WriteLine(result);

            return result;
        }

        /// <summary>
        /// This method implements the main functionality of stochastic gradient boosting
        /// </summary>
        private void BuildBoostTree(Metrics metrics, BoostTreeLoss boostTreeLoss, DataFeatureSampleRate dataFeatureSampleRate,
                                    int maxTreeSize, int minNumSamples, int numIter,
                                    int cThreads, Random r)
        {
            float minValidationErr = 100;

            float[] funValueGain = new float[this.numSamples];

            //(1) compute scores produced by the sub-model
            boostTreeLoss.ModelEval(this.subModel, this.labelFeatureDataCoded, this.subModelScore);

            //(2) compute the corresponding function values;
            boostTreeLoss.ModelScoresToFuncValues();

            //(3) compute the metrics of the sub-model
            int m = optIter = 0;
            metrics.ComputeMetrics(boostTreeLoss.ModelScores, m, false);

#if VERBOSE
            Console.WriteLine(metrics.ResultsHeaderStr());
            Console.WriteLine(metrics.ResultsStr(m));
#endif
            //(4) creat samplers to sub-sampl the features and data during node spliting
            RandomSampler featureSampler = new RandomSampler(r);
            RandomSampler dataSampler = new RandomSampler(r);

            //(5) creat the object that does node splitting
#if SINGLE_THREAD
            // single-threaded
             this.findSplit = new FindSplitSync();
#else
            // multi-threaded
            this.findSplit = new FindSplitAsync(cThreads);
#endif //SINGLE_THREAD

            //(6) Iteratively building boosted trees
            for (m = 0; m < numIter; m++)
            {
                // selecting a fraction of data groups for each iteration
                float sampleRate = dataFeatureSampleRate.SampleDataGroupRate(m);
                DataSet workDataSet = this.labelFeatureDataCoded.DataGroups.GetDataPartition(DataPartitionType.Train, sampleRate, r);
                workDataSet.Sort();  // sorting gains some noticable speedup.

                // compute the pseudo response of the current system
                boostTreeLoss.ComputePseudoResponse(workDataSet);

                //set the data and feature sampling rate for node spliting in this iteration
                featureSampler.SampleRate = dataFeatureSampleRate.SampleFeatureRate(m);
                dataSampler.SampleRate = dataFeatureSampleRate.SampleDataRate(m);

                // fit a residual model (regression trees) from the pesuso response
                // to compensate the error of the current system
                for (int k = 0; k < boostTreeLoss.NumTreesPerIteration; k++)
                {
                    //only use the important data points if necessary
                    int[] trimIndex = boostTreeLoss.TrimIndex(workDataSet, k, m);

                    //build a regression tree according to the pseduo-response
                    this.regressionTrees[m, k] = new RegressionTree(this.labelFeatureDataCoded, boostTreeLoss, k, trimIndex,
                                                                    dataSampler, featureSampler, maxTreeSize, minNumSamples, this.findSplit, this.tempSpace);

                    //compute the function value of all data points produced by the newly generated regression tree
                    this.regressionTrees[m, k].PredictFunValue(this.labelFeatureDataCoded, ref funValueGain);

                    //try to do a more global optimalization - refine the leaf node response of a decision tree
                    //by looking at all the training data points, instead of only the ones falling into the regaion.
                    //Here we are estimate and apply a global mutiplication factor for all leaf nodes
                    float adjFactor = (m>0) ? boostTreeLoss.ComputeResponseAdjust(funValueGain) : 1.0F;

                    //apply the multiplication factor to the leaf nodes of the newly generated regression tree
                    this.regressionTrees[m, k].AdjustResponse(adjFactor);

                    //update the function value for all data points given the new regression tree
                    boostTreeLoss.AccFuncValueGain(funValueGain, adjFactor, k);
                }

                //compute the metrics of the current system
                boostTreeLoss.FuncValuesToModelScores();
                metrics.ComputeMetrics(boostTreeLoss.ModelScores, m + 1, false);
#if VERBOSE
                Console.WriteLine(metrics.ResultsStr(m+1));
#endif
                //keep track of the best (minimal Error) iteration on the Validation data set
                this.optIter = metrics.GetBest(DataPartitionType.Validation, ref minValidationErr);

                if ((m+1) % 5 == 0)  // save the tree every 5 iterations
                    SaveBoostTree();
            }

            if (this.findSplit != null)
            {
                this.findSplit.Cleanup();
            }
        }

        public void Predict(LabelFeatureData labelFeatureData, int numIter,
                            BoostTreeLoss boostTreeLoss,
                            Metrics metrics, //reporting the error for each iteration if the following are set
                            bool silent // If true, only report results on the last iteration
                            )
        {
            if (numIter > this.TotalIter)
                numIter = this.TotalIter;

            boostTreeLoss.Reset(labelFeatureData.NumDataPoint);

            //(1) compute the probabilities produced by the sub-model
            boostTreeLoss.ModelEval(this.subModel, labelFeatureData, null);

            //(2) compute the corresponding function values;
            boostTreeLoss.ModelScoresToFuncValues();

            if (metrics != null)
            {
                metrics.ComputeMetrics(boostTreeLoss.ModelScores, 0, this.optIter == 0);
#if VERBOSE
                Console.WriteLine(metrics.ResultsHeaderStr());
                Console.WriteLine(metrics.ResultsStr(0));
#endif
            }

            //(3) accumulate the function values for each boosted regression tree
            int numSamples = labelFeatureData.NumDataPoint;
            float[] funValueGain = new float[numSamples];

#if GET_PER_DOC_PER_ITER_SCORES
            float[][] saveScores = ArrayUtils.FloatMatrix(numIter+2, labelFeatureData.NumDataPoint); // We will take transpose when we print
            for (int i = 0; i < labelFeatureData.NumDataPoint; ++i)
            {
                saveScores[0][i] = labelFeatureData.GetGroupId(i);
                saveScores[1][i] = labelFeatureData.GetLabel(i);
            }
#endif

            for (int m = 0; m < numIter; m++)
            {
                // fit a residual model (regression trees) from the pesuso response
                // to compensate the error of the current system
                for (int k = 0; k < boostTreeLoss.NumTreesPerIteration; k++)
                {
                    if (this.regressionTrees[m, 0] == null)
                        break;
#if GET_PER_DOC_PER_ITER_SCORES
                    this.regressionTrees[m, k].PredictFunValueNKeepScores(labelFeatureData, this.Train2TestIdx, funValueGain, saveScores[m+2]);
#else
                    this.regressionTrees[m, k].PredictFunValue(labelFeatureData, this.Train2TestIdx, funValueGain);
#endif
                    boostTreeLoss.AccFuncValueGain(funValueGain, 1.0f, k);
                }


                if (metrics != null)
                {
                    //compute the metrics of the current system
                    boostTreeLoss.FuncValuesToModelScores();
                    metrics.ComputeMetrics(boostTreeLoss.ModelScores, m + 1, this.optIter == m + 1);
                    if(m==numIter-1 || !silent)
                        Console.WriteLine(metrics.ResultsStr(m + 1));
                }
            }

#if GET_PER_DOC_PER_ITER_SCORES
            using (StreamWriter sw = new StreamWriter("allScores.tsv"))
            {
                sw.Write("m:QueryID\tm:Rating"); // Write the header (with no tab at the end!)
                for (int j = 1; j < numIter+1; ++j)
                    sw.Write("\tFtr_" + j.ToString("0000"));
                sw.WriteLine();
                for (int j = 0; j < labelFeatureData.NumDataPoint; ++j)
                {
                    sw.Write("{0}\t{1}", saveScores[0][j], saveScores[1][j]); // Write the query ID and label
                    for (int m = 2; m < numIter + 2; ++m)
                        sw.Write("\t{0:G6}", saveScores[m][j]);
                    sw.WriteLine();
                }
            }
#endif

            if (metrics == null)
            {
                boostTreeLoss.FuncValuesToModelScores();
            }
            else
                metrics.SaveScores("DataScores.txt", boostTreeLoss.ModelScores);
        }

        public void Predict(LabelFeatureData testData, int numIter,
                                BoostTreeLoss boostTreeLoss
                                )
        {
            Predict(testData, numIter, boostTreeLoss, null, false);
        }

        public void Predict(LabelFeatureData testData, int numIter, Metrics metrics)
        {
            Predict(testData, numIter, this.boostTreeLoss, metrics, false);
        }

        public void Predict(LabelFeatureData testData, int numIter, Metrics metrics, bool silent)
        {
            Predict(testData, numIter, this.boostTreeLoss, metrics, silent);
        }

        public float[][] Predict(LabelFeatureData testData, int numIter)
        {
            Predict(testData, numIter, this.boostTreeLoss, null, false);

            return this.boostTreeLoss.ModelScores;
        }

        public float[][] Predict(LabelFeatureData testData)
        {
            return Predict(testData, this.optIter);
        }

        public float ValidationSampleRate
        {
            get { return this.ValidationSampleRate; }
            set
            {
                if (value >= 0.05F && value <= 0.75F)
                    this.ValidationSampleRate = value;
            }
        }

        public void SetOptIter(int optIter)
        {
            if (optIter < this.TotalIter && optIter > 0)
                this.optIter = optIter;
        }

        public int GetOptIter()
        {
            return this.optIter;
        }

        public int TotalIter
        {
            get
            {
                int numTrees = (this.regressionTrees == null)? 0 : this.regressionTrees.GetLength(0);
                return numTrees;
            }
        }

        public void SaveBoostTree()
        {
            SaveBoostTree(saveTreeBinFile);
            WriteMSNStyle(saveTreeTextFile, FeatureNames, this.TotalIter);
            WritePSStyle(saveTreeXmlFile, FeatureNames, this.TotalIter);
        }

        public NNModelMSN CovertToNNModelMSN(int cIter, bool fConvThresh2Int)
        {
            NNModelMSN sMartMSN = null;
            NNModelMSN subModelMSN = (NNModelMSN)this.SubModel;

            DTNode[,][] dtNodes = this.CreateDTNodes(this.FeatureNames, cIter);

            foreach (DTNode[] nodes in dtNodes)
            {
                foreach (DTNode node in nodes)
                {
                    if (node != null)
                    {
                        node.IsThresholdInt = fConvThresh2Int;
                    }
                }
            }

            sMartMSN = new NNModelMSN(subModelMSN, dtNodes);


            return sMartMSN;
        }

        public void SaveBoostTree(string saveTreeFileIn)
        {
            FileStream fileStream = new FileStream(saveTreeFileIn, FileMode.Create);
            BinaryFormatter formatter = new BinaryFormatter();
            formatter.Serialize(fileStream, this);
            fileStream.Close();
        }

        public static BoostTree Read(string saveTreeFile)
        {
            FileStream fileStream = new FileStream(saveTreeFile, FileMode.Open, FileAccess.Read, FileShare.Read);
            BinaryFormatter formatter = new BinaryFormatter();

            BoostTree boostTree = (BoostTree)formatter.Deserialize(fileStream);
            fileStream.Close();
            return boostTree;
        }

        /// <summary>
        /// output the model in the XML format Powerset uses
        /// </summary>
        public void WritePSStyle(string outFileName, string[] ColumnNames, int cIter)
        {
            if (outFileName == null)
            {
                return;
            }

            FileStream file = new FileStream(outFileName, FileMode.Create);
            StreamWriter wStream = new StreamWriter(file);

            int cClasses = this.boostTreeLoss.NumTreesPerIteration;
            int cTrees = this.regressionTrees.Length;
            cIter = (cTrees / cClasses) < cIter ? (cTrees / cClasses) : cIter;

            //count the number of non-null trees;
            int index = 0;
            for (int i = 0; i < cIter; i++)
            {
                for (int j = 0; j < cClasses; j++)
                {
                    if (this.regressionTrees[i, j] != null)
                    {
                        index++;
                    }
                }
            }

            wStream.WriteLine("<gbm use_missing_value=\"false\" comparison_operator=\"&lt;=\" scale_factor=\"1e6\">\n<name>LambdaMART_model</name>\n<features>");
            for (int iFeatures = 0; iFeatures < ColumnNames.Length; iFeatures++)
            {
                wStream.WriteLine("<feature num=\"{0}\">{1}</feature>", iFeatures, ColumnNames[iFeatures]);
            }
            wStream.WriteLine("</features>");

            wStream.WriteLine("<initf>0.0</initf>\n<numtrees>{0}</numtrees>\n<trees>", index);
#if DEBUG
            wStream.Flush();
#endif
            //write all the inputs
            index = 1;
            for (int i = 0; i < cIter; i++)
            {
                for (int j = 0; j < cClasses; j++)
                {
                    if (this.regressionTrees[i, j] != null)
                    {
                        wStream.WriteLine("<tree num=\"{0}\">", index);
                        this.regressionTrees[i, j].WritePSStyle(index++, wStream, ColumnNames);
                        wStream.WriteLine("</tree>");
                    }
                }
            }
            wStream.WriteLine("</trees></gbm>");
            wStream.Close();

            file.Close();
        }

        public void WriteMSNStyle(string outFileName, string[] ColumnNames, int cIter)
        {
            if (outFileName == null)
            {
                return;
            }

            FileStream file = new FileStream(outFileName, FileMode.Create);
            StreamWriter wStream = new StreamWriter(file);

            int cClasses = this.boostTreeLoss.NumTreesPerIteration;
            int cTrees = this.regressionTrees.Length;
            cIter = (cTrees / cClasses) < cIter ? (cTrees / cClasses) : cIter;

            //using 2-layer:
            //first layer does the score summation for the boosted trees in each class
            //second layer does the probability and dot-product to compute the final score
            const int CLAYER = 2;

            //count the number of non-null trees;
            int index = 0;
            for (int i = 0; i < cIter; i++)
            {
                for (int j = 0; j < cClasses; j++)
                {
                    if (this.regressionTrees[i, j] != null)
                    {
                        index++;
                    }
                }
            }

            wStream.WriteLine("[NeuralNet]");
            wStream.WriteLine("Layers={0}", CLAYER);
            wStream.WriteLine("Inputs={0}", index);
            wStream.WriteLine();

            //write all the inputes
            index = 1;
            for (int i = 0; i < cIter; i++)
            {
                for (int j = 0; j < cClasses; j++)
                {
                    if (this.regressionTrees[i, j] != null)
                    {
                        this.regressionTrees[i, j].WriteMSNStyle(index++, wStream, ColumnNames);
                    }
                }
            }

            //write layer1
            int cNode_1Layer = cClasses;
            wStream.WriteLine("[Layer:1]");// each first layer node correspondig to a class
            wStream.WriteLine("Nodes={0}", cNode_1Layer);
            wStream.WriteLine();

            //write all the nodes in the first layer
            for (int j = 0; j < cNode_1Layer; j++)
            {
                wStream.WriteLine("[Node:{0}:{1}]", 1, j + 1);
                //weight:0 is the threshold/bias which is set to zero
                wStream.WriteLine("Weight:0=0.0");

                //weights for real inputs are indexed from 1
                for (int i = 0; i < index - 1; i++)
                {
                    float weight = (i % cNode_1Layer == j) ? (float)1.0 : (float)0.0;
                    wStream.WriteLine("Weight:{0}={1}", i + 1, weight);
                }
                wStream.WriteLine("Type=linear");
                wStream.WriteLine();
            }

            //write all the 2-layer node
            wStream.WriteLine("[Layer:2]");// 2-layer convert scores to probabilities then to final score
            wStream.WriteLine("Nodes=1");
            wStream.WriteLine();
            wStream.WriteLine("[Node:2:1]");

            //weight:0 is the threshold/bias which is set to zero
            wStream.WriteLine("Weight:0=0.0");
            //weights indexed from 1, index 0 is the biase??
            for (int i = 1; i <= cNode_1Layer; i++)
            {
                float weight = (float)(i - 1);
                wStream.WriteLine("Weight:{0}={1}", i, weight);
            }
            wStream.WriteLine("Type=Logistic"); //MART

            wStream.Close();

            file.Close();
        }

        public DTNode[,][] CreateDTNodes(string[] ColumnNames, int cIter)
        {
            int cClasses = this.boostTreeLoss.NumTreesPerIteration;
            int cTrees = this.regressionTrees.Length;
            cIter = (cTrees / cClasses) < cIter ? (cTrees / cClasses) : cIter;
            DTNode[,][] nodes = new DTNode[cIter, cClasses][];

            for (int i = 0; i < cIter; i++)
            {
                for (int j = 0; j < cClasses; j++)
                {
                    if (this.regressionTrees[i, j] != null)
                    {
                        nodes[i, j] = this.regressionTrees[i, j].CreateDTNodes(this.featureNames);
                    }
                }
            }

            return nodes;
        }

        public void SummarizeFeatureImporance(int cFinalIter, string fileName)
        {
            //allocate the accumulators to compute feature importance if necessary
            if (this.importance == null)
            {
                this.importance = new float[this.boostTreeLoss.NumTreesPerIteration + 1][];
                for (int k = 0; k < this.boostTreeLoss.NumTreesPerIteration + 1; k++)
                {
                    this.importance[k] = new float[this.numFeatures];
                }
            }
            if (this.order == null)
            {
                this.order = new int[this.boostTreeLoss.NumTreesPerIteration + 1][];
                for (int k = 0; k < this.boostTreeLoss.NumTreesPerIteration + 1; k++)
                {
                    this.order[k] = Vector.IndexArray(this.numFeatures);
                }
            }

            //reset the feature importance accumulators
            for (int k = 0; k < this.boostTreeLoss.NumTreesPerIteration + 1; k++)
            {
                Array.Sort(this.order[k]);
                for (int i = 0; i < this.numFeatures; i++)
                {
                    this.importance[k][i] = 0;
                }
            }

            //accumulate the feature importance for each feature for each set of trees
            for (int iter = 0; iter < cFinalIter; iter++)
            {
                for (int k = 0; k < this.boostTreeLoss.NumTreesPerIteration; k++)
                {
                    float[] curFeatureImportance = this.regressionTrees[iter, k].FeatureImportance;
                    for (int i = 0; i < this.numFeatures; i++)
                    {
                        this.importance[k][i] = (this.importance[k][i] * iter + curFeatureImportance[i]) / (iter + 1);
                    }
                }
            }

            //accumulate the overall importance for each feature
            for (int i = 0; i < this.numFeatures; i++)
            {
                for (int k = 0; k < this.boostTreeLoss.NumTreesPerIteration; k++)
                {
                    this.importance[this.boostTreeLoss.NumTreesPerIteration][i] += this.importance[k][i];
                }
            }

            //compute the relative importance for each feature
            for (int k = 0; k < this.boostTreeLoss.NumTreesPerIteration + 1; k++)
            {
                Array.Sort(this.importance[k], this.order[k]);
                float maxVal = (float)Math.Sqrt(this.importance[k][this.numFeatures - 1]);
                for (int i = 0; i < this.numFeatures; i++)
                {
                    this.importance[k][i] = (float)Math.Sqrt(this.importance[k][i]) / maxVal;
                }
            }

            //output feature importance
            if (fileName != null)
            {
                StreamWriter stream = new StreamWriter(new FileStream(fileName, FileMode.Create));
                for (int i = importance[0].Length - 1; i >= 0; i--)
                {
                    for (int k = 0; k < importance.Length; k++)
                    {
                        stream.Write(this.featureNames[order[k][i]] + "\t" + this.importance[k][i] + "\t");
                    }
                    stream.WriteLine();
                }
                stream.Close();
            }
        }

        public string[] FeatureNames
        {
            get
            {
                return featureNames;
            }

        }

        private int optIter;
        public int OptIter { get { return optIter; } set { optIter = value;} }

        private int numSamples;
        public int NumSamples { get { return numSamples; } }

        private int numFeatures;

        private string[] featureNames = null;

        private RegressionTree[,] regressionTrees;
        private string saveTreeBinFile;
        private string saveTreeTextFile;
        private string saveTreeXmlFile;

        private Model subModel = null;
        private BoostTreeLoss boostTreeLoss = null;

        [NonSerialized] private float[][] importance;
        [NonSerialized] private int[][] order;

        [NonSerialized] LabelFeatureDataCoded labelFeatureDataCoded;
        [NonSerialized] LabelFeatureData subModelScore;

        [NonSerialized]	private int[] dataColRange;

        [NonSerialized] private int[] Train2TestIdx = null;

        [NonSerialized]	TempSpace tempSpace;
        [NonSerialized] public IFindSplit findSplit;
    }

    /// <summary>
    /// Provide re-usable memeory space for the RegressionTree Class
    /// We could add more to this temp space.
    /// </summary>
    public class TempSpace
    {
        public TempSpace(int numSamples)
        {
            this.isLeft = new bool[numSamples];
        }
        public bool[] isLeft;
    }

    [Serializable]
    public class MartModel : Model
    {
        public MartModel(string saveTreeFile)
        {
            boostTree = BoostTree.Read(saveTreeFile);
        }

        //implement Model interface
        override public bool SetFeatureNames(string[] featureNames)
        {
            return boostTree.SetFeatureNames(featureNames);
        }

        override public float Evaluate(float[] features)
        {
            return (float)0.0;
        }

        override public bool Evaluate(float[] features, float[] results)
        {
            float[][] testData = new float[1][];
            testData[0] = features;
            LabelFeatureData labelFeatureData = new CLabelFeatureData(testData);

            float[][] prob = boostTree.Predict(labelFeatureData);

            for (int i = 0; i < results.Length; i++)
            {
                results[i] = prob[i][0];
            }

            return true;
        }

        private BoostTree boostTree = null;
    }

}
