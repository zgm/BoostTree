using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Microsoft.TMSN;
using Microsoft.TMSN.CommandLine;

namespace StochasticGradientBoost
{
    class TrainArgs
    {
        [Microsoft.TMSN.CommandLine.Argument(Microsoft.TMSN.CommandLine.ArgumentType.Required)]
        public string trainFile = null;

        [Microsoft.TMSN.CommandLine.Argument(Microsoft.TMSN.CommandLine.ArgumentType.Required)]
        public string binaryTreeFile = null;

        public string textTreeFile = null;

        public string activeFeatureFile = null;

        public int seed = 0; // Default seed.  Pass -1 to call Random().

        public string validFile = null;
        public string testFile = null;

        public string subModelScore = null;

        [Microsoft.TMSN.CommandLine.Argument(Microsoft.TMSN.CommandLine.ArgumentType.Required)]
        public int cLeafNodes = 0;

        [Microsoft.TMSN.CommandLine.Argument(Microsoft.TMSN.CommandLine.ArgumentType.Required)]
        public float learnRate = 0;

        [Microsoft.TMSN.CommandLine.Argument(Microsoft.TMSN.CommandLine.ArgumentType.Required)]
        public int numIter = 0;

        public int cThreads = 16;

        //the minimal number of data points in a leaf node
        public int minNumSamples = 15;

        //the portion of data groups to sub-sample for each iteration
        public float sampleDataGroupRate = 1.0F;
        //the random portion of feature used to split a node
        public float sampleFeatureRate = 1.0F;
        //the random portion of data used to split a node
        public float sampleDataRate = 1.0F;

        public string subModel = null;
        public int cLayer = 0;

        public string labelName = "m:Rating";
        public string labelNameValueFile = null;

        public bool SparseCoded = true;

        public TrainArgs(string[] args)
        {
            if (!Microsoft.TMSN.CommandLine.Parser.ParseArgumentsWithUsage(args, this))
            {
                Environment.Exit(-1);
            }
        }

    }

    /// <summary>
    /// Train a boost tree, and 
    /// Save the tree (which contains many small regression trees) 
    /// </summary>
    public class ClassificationTrain
    {
        /// <summary>
        /// Main Program 
        /// </summary>
        /// <param name="args">
        /// Should contain five parameters in the order of 
        /// file name for training source data (DataProcess class),
        /// file name for saving the tree
        /// tree size, i.e., maximum number of terminal nodes, usually 16 - 20
        /// learning rate, usually 0.02 - 0.06
        /// number of iterations, usallay >500(can be pruned later)
        /// </param>
        public static void Main(string[] args)
        {
            try
            {
                TrainArgs cmd = new TrainArgs(args);

                Random r = null;
                if (cmd.seed == -1)
                {
                    r = new Random();
                }
                else
                {
                    r = new Random(cmd.seed);
                }

                string[] activeFeatureNames = null;
                //read and process only a subset of activated features as specified in the activeFeatureFile
                if (cmd.activeFeatureFile != null)
                {
                    activeFeatureNames = TsvFileLoader.ReadFeatureNames(cmd.activeFeatureFile);
                }

                //feature parser: special module that understand MSN style value encoding
                MsnFeatureParser featureParser = new MsnFeatureParser(activeFeatureNames);

                //the column name for label: 
                string[] labelName = { cmd.labelName };
                //label/rating parser: 
                IParser<float> RateParser = new MsnLabelParser(labelName, cmd.labelNameValueFile);

                //data boundary: no boundary
                OnelineGroup noBounadry = new OnelineGroup();

                //Load coded data if exist
                LabelFeatureDataCoded trainLabelFeatureDataCoded = (CLabelFeatureDataCoded)CLabelFeatureData.Load(cmd.trainFile, featureParser, RateParser, noBounadry, typeof(CLabelFeatureDataCoded), activeFeatureNames, cmd.cThreads, cmd.SparseCoded);
                LabelFeatureData validLabelFeatureData = (CLabelFeatureData)CLabelFeatureData.Load(cmd.validFile, featureParser, RateParser, noBounadry, typeof(CLabelFeatureData), activeFeatureNames, cmd.cThreads, cmd.SparseCoded);
                LabelFeatureData testLabelFeatureData = (CLabelFeatureData)CLabelFeatureData.Load(cmd.testFile, featureParser, RateParser, noBounadry, typeof(CLabelFeatureData), activeFeatureNames, cmd.cThreads, cmd.SparseCoded);

                //build composite data - an aggregated data object that keeps tract of train/valid/test data
                CLabelFeatureDataCodedComposite labelFeatureDataCoded = CLabelFeatureDataCodedComposite.Create(trainLabelFeatureDataCoded, validLabelFeatureData, testLabelFeatureData);                                            

                //initial submodel to boost on
                Model subModel = null;
                if (cmd.subModel != null && cmd.subModel.Length > 0)
                {
                    if (cmd.cLayer > 0)
                    {
                        string[] layerNames = new string[cmd.cLayer];
                        for (int i = 0; i < cmd.cLayer; i++)
                        {
                            string num = "";
                            if (cmd.cLayer > 1)
                            {
                                num = (i + 1).ToString();
                            }
                            layerNames[i] = cmd.subModel + "layer" + num + ".txt";
                        }
                        subModel = new NNModel(layerNames);
                    }
                    else
                    {
                        string iniName = cmd.subModel + ".ini";
                        subModel = NNModelMSN.Create(iniName);
                    }
                }

                if (subModel != null && !subModel.SetFeatureNames(labelFeatureDataCoded.FeatureNames))
                {
                    Console.WriteLine("Fail to initialize specified submodel - training with empty submodel");
                    subModel = null;
                }

                LabelConverter labelConvert = new LabelConverterNull();

                LabelFeatureData subModelScore = null;
                McBoostTreeLoss boostTreeLoss = new McBoostTreeLoss(labelFeatureDataCoded, labelConvert, cmd.learnRate);
                BoostTree boostTree = new BoostTree(labelFeatureDataCoded, subModelScore, subModel, boostTreeLoss,
                                                cmd.binaryTreeFile, cmd.textTreeFile);

                //set up the error metrics that we like to keep tract of during testing                       
                DataPartitionType[] dataTypes = { DataPartitionType.Train, DataPartitionType.Validation, DataPartitionType.Test };
              
                //dp.LabelFeatureData is the data we are evaluating
                Metrics metrics = new ClassError(labelFeatureDataCoded, labelConvert, dataTypes);

                DataFeatureSampleRate dataFeatureSampleRate = new DataFeatureSampleRate(cmd.sampleFeatureRate, cmd.sampleDataRate, cmd.sampleDataGroupRate);

                boostTree.Build(metrics, dataFeatureSampleRate, cmd.cLeafNodes, cmd.minNumSamples, cmd.numIter, cmd.cThreads, r);

                metrics.SaveAllResults("TrainingErrHistory.txt");

            }
            catch (Exception exc)
            {
                Console.WriteLine(exc.Message);
            }
        }            
    }
}
