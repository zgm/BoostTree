using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Microsoft.TMSN;
using Microsoft.TMSN.CommandLine;
using System.Diagnostics;

namespace StochasticGradientBoost
{
	/// <summary>
	/// Test/validate the trained boost tree. 
	/// It is probably better to move the data-reading part to other classes such as DataProcess. 
	/// But we are going to change the interface later anyway. 
	/// </summary>
	class ClassificationTest
	{
        public class TestArgs
        {            
            [Microsoft.TMSN.CommandLine.Argument(Microsoft.TMSN.CommandLine.ArgumentType.Required)]
            public string binaryTreeFile = null;            
            
            public int numIter = 0; // If not specified, the optIter found during training will be used, and test results for other trees will not be printed.
            public bool silent = true; // ...unless this is set to false, in which case stats are printed for every set of trees.

            public string inputFile = null;
            public string activeFeatureFile = null;
       
            public int seed = 7;
            public int cThreads = 16;

            public string metric = "ErrRate"; //{"ErrRate", "PrecRecall"}
            
            public string labelName = "m:Rating";
            public string labelNameValueFile = null;

            public TestArgs(string[] args)
            {
                if (!Microsoft.TMSN.CommandLine.Parser.ParseArgumentsWithUsage(args, this))
                {
                    Environment.Exit(-1);
                }
            }
        }        

		/// <summary>
		/// Main Program       
		/// </summary>
		/// <param name="args">
		/// There should be at least two input parameters from command line:
		/// file name of the stored boost tree, and 
		/// file name of the source test/validation data 
		/// </param>
		public static void Main(string[] args)
        {
            TestArgs cmd = new TestArgs(args);

            Random r = new Random(cmd.seed);

            //Load the model first
            BoostTree boostTree = BoostTree.Read(cmd.binaryTreeFile);
            if (boostTree == null)
            {
                Debug.Assert(false, "Fail to load model");
                Console.WriteLine("Fail to load model " + cmd.binaryTreeFile);
                return;
            }

            int numIter = cmd.numIter;
            if (cmd.numIter == 0) // If iteration not specified, use the optimal validation iteration found during training
            {
                numIter = boostTree.OptIter;
            }

            //compute and output the feature importance for the specified number of iterations
//            boostTree.SummarizeFeatureImporance(numIter, "featureImportance.txt"); 

            string[] activeFeatureNames = null;
            //read and process only a subset of activated features as specified in the activeFeatureFile
            if (cmd.activeFeatureFile != null)
            {
                activeFeatureNames = TsvFileLoader.ReadFeatureNames(cmd.activeFeatureFile);
            }

            //feature parser: special module that understand MSN style value encoding
            MsnFeatureParser featureParser = new MsnFeatureParser(activeFeatureNames);

            //the column name for label: values to regress to
            string[] labelName = { cmd.labelName };
            //label/rating parser: special module that understand regression value
            IParser<float> RateParser = new MsnLabelParser(labelName, cmd.labelNameValueFile);

            //data boundary: every row of data is by itself / all data is in one group / no data groups
            OnelineGroup noBoundary = new OnelineGroup();

            //Load coded data if exist           
            LabelFeatureData labelFeatureData = (CLabelFeatureData)CLabelFeatureData.Load(cmd.inputFile, featureParser, RateParser, noBoundary, typeof(CLabelFeatureData), activeFeatureNames, cmd.cThreads);                                  

            if (!boostTree.SetFeatureNames(labelFeatureData.FeatureNames))
            {
                Debug.Assert(false, "Sub-model failed to initialize");
                Console.WriteLine("Sub-model failed to initialize, program exits");
                return;
            }

            //All data are for test
            float[] percentage = DataGroups.DataSplit("0:0:10"); //"Train:Valid:Test"         
            labelFeatureData.DataGroups.PartitionData(percentage, r);

            //Specify the data partitions to be tested                      
            DataPartitionType[] dataTypes = DataGroups.PartitionTypes("Train:Valid:Test"); //using all data as default          
              
            LabelConverter labelConvert = new LabelConverterNull();
            //set up the error metrics that we like to keep tract of during testing   
            //dp.LabelFeatureData is the data we are evaluating
            Metrics metrics;
            if (string.Compare(cmd.metric, "ErrRate", true) == 0)
            {
                metrics = new ClassError(labelFeatureData, labelConvert, dataTypes);
            }
            else if (string.Compare(cmd.metric, "PrecRecall", true) == 0)
            {
                metrics = new PrecRecall(labelFeatureData, labelConvert, dataTypes);
            }
            else
            {
                metrics = new ClassError(labelFeatureData, labelConvert, dataTypes);
            }

            boostTree.Predict(labelFeatureData, numIter, metrics, cmd.silent);

            // Output the testing error history. This should at least help validate the optimal
            // number of iterations, although it is probably better that we use NDCG history 
            // for the optimum. 
            metrics.SaveAllResults("testErrHistory.txt");                                 
        }
	}
}
