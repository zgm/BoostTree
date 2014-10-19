using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using Microsoft.TMSN;

namespace StochasticGradientBoost
{
	/// <summary>
	/// Read a cleaned TSV file and convert the data into ushort[][] format. 
	/// The TSV file is assumed to have the MSN format: 
	/// columns starts with "m:" are meta data, labels column has the name "m:Label"
	/// the rest of the columns are features
	/// The program outputs a file (binary formatted class) with suffix .dp, from which one 
	/// can recover the data file ushort[][], the labels, as well as the query IDs. 
	/// </summary>    
	[Serializable]
	public class DataProcess
	{
		class DataProcArgs
		{
			[Microsoft.TMSN.CommandLine.Argument(Microsoft.TMSN.CommandLine.ArgumentType.Required)]
			public string tsvFile = null;

			[Microsoft.TMSN.CommandLine.Argument(Microsoft.TMSN.CommandLine.ArgumentType.Required)]
			public string binFile = null;

			[Microsoft.TMSN.CommandLine.Argument(Microsoft.TMSN.CommandLine.ArgumentType.Required)]
			public string binFileCoded = null;
		   
			//the name of the input file which contains a list of selected features to be processed
			public string activeFeatureFile = null;

			public bool storeCodedFeature = false;

			public bool fCodedFeatureSparse = false;

            public string labelName = "m:Rating";
            public string labelNameValueFile = null;

            public bool queryBoundary = true;

			public int cThreads = 16;

			public DataProcArgs(string[] args)
			{
				if (!Microsoft.TMSN.CommandLine.Parser.ParseArgumentsWithUsage(args, this))
				{
					Environment.Exit(-1);
				}
			}
		}

		static void Main(string[] args)
		{
			DataProcArgs cmd = new DataProcArgs(args);

            IGroupBoundary boundary = null;

            if (cmd.queryBoundary)
            {
                //we need to keep tract of the queries for ranking
                boundary = new QueryBoundary();
            }
            else
            {
                //data boundary: no boundary
                boundary = new OnelineGroup();
            }

            string[] labelName = { cmd.labelName };
			IParser<float> RateParser = new MsnLabelParser(labelName, cmd.labelNameValueFile);

			Console.WriteLine("Loading data from tsv file " + cmd.tsvFile);
			 
			MsnFeatureParser featureParser = null;
			//read and process only a subset of activated features as specified in the activeFeatureFile
			if (cmd.activeFeatureFile != null)
			{
				string[] FeatureNames = TsvFileLoader.ReadFeatureNames(cmd.activeFeatureFile);
				featureParser = new MsnFeatureParser(FeatureNames);
			}

            TsvFileLoader tsvFileLoader = new TsvFileLoader(cmd.tsvFile, null, RateParser, featureParser, boundary);
			Console.WriteLine("Finishing loading the tsv file");

			Console.WriteLine("Create LabelFeatureData uncoded ...");
			CLabelFeatureData labelFeatureData = new CLabelFeatureData(tsvFileLoader.FeatureName, tsvFileLoader.Labels, tsvFileLoader.GroupId, tsvFileLoader.Feature);

			Console.WriteLine("Save LabelFeatureData uncoded ...");
			if (cmd.binFile != null)
			{
				labelFeatureData.Save(cmd.binFile);
			}
			
			Console.WriteLine("Create LabelFeatureData coded ...");
			CLabelFeatureDataCoded labelFeatureDataCoded = new CLabelFeatureDataCoded(labelFeatureData, cmd.cThreads, cmd.storeCodedFeature, cmd.fCodedFeatureSparse);

			Console.WriteLine("Save LabelFeatureData coded ...");
			if (cmd.binFileCoded != null)
			{
				labelFeatureDataCoded.Save(cmd.binFileCoded);
			}
		}       
	}

}