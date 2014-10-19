// C. Burges, Fall 2006
//
// Take advantage of the fact than when combining two sets of scores, there are only a finite number of step sizes to
// examine (and that number is equal to the total number of pairs).  Lemma: after ordering by step size, and when examining
// every step size in order, pairs will always be adjacent.  However, as opposed to RankNet training, here we need all pairs
// (not just pairs between differently labeled items) to keep track of rank positions.  Thus we may need to use a subsample of
// data here, which should be fine.
//
// The code supports search for negative step sizes: set fs.alphaPos to false.  Useful for debugging (if you're following a gradient
// correctly, and considering small enough step sizes, the optimal alpha should be positive).
//
// A good way to think about this algorithm is: what happens as alpha sweeps from its smallest to its largest value?
//
// There's a built in BVT, by running SelfTest.
//
// Please do not submit changes without code review with CBurges.
using System;
using System.Collections;
using System.Text;
using Microsoft.TMSN.CommandLine;

namespace Microsoft.TMSN
{
	class CommandLineArguments
	{
		public string firstScoresFile = null;
		public string secondScoresFile = null;

        // Self test.
        public bool selfTest = false;

		// Randomization
		public int seed = 0;

		// DCG items
        public int truncLevel = 10;
		public float labelForUnlabeled = 0;
		public double scoreForDegenerateQuery = 1.0;
		public bool skipDegenerateQueries = true;

		// The kind of combination used
		public bool convex = false;
		public bool verbose = false;
	}

	class Program
	{
		static void Main(string[] args)
		{
			try
			{
				CommandLineArguments cmd = new CommandLineArguments();
				if(CommandLine.Parser.ParseArgumentsWithUsage(args, cmd))
				{
                    Random random = new Random(cmd.seed);
                    FindStepLib fs = new FindStepLib(cmd.convex, random, cmd.verbose);
                    DCGScorer.truncLevel = cmd.truncLevel;

                    if (cmd.selfTest)
                    {
                        //fs.alphaPos = false; // Set to false to search for the optimal negative alpha.  Default is true.
                        SelfTest(10, 100, fs);
                    }
                    else
                    {
                        QueryCollection qc1 = new QueryCollection(cmd.firstScoresFile, cmd.labelForUnlabeled,
                                                                  cmd.skipDegenerateQueries, cmd.scoreForDegenerateQuery);
                        QueryCollection qc2 = new QueryCollection(cmd.secondScoresFile, cmd.labelForUnlabeled,
                                                                  cmd.skipDegenerateQueries, cmd.scoreForDegenerateQuery);

                        // Assume that the first 'feature' is in fact the scores
                        qc1.AssignScoresFromFeature(0);
                        qc2.AssignScoresFromFeature(0);

                        double bestGain;

                        fs.FindStep(qc1, qc2, null, out bestGain);
                    }
                    
					
#if DEBUG // Force console to stick around
					Console.WriteLine("...Press Enter to terminate program...");
					Console.ReadLine();
#endif
				}
			}
			catch(Exception exc)
			{
				Console.WriteLine(exc.Message);
			}
		}



		/// <summary>
		/// Generate two QueryCollections containing randomly generated scores and labels (although they share
		/// all the same labels, as though one dataset tested on two models).  The scores are loosely
		/// correlated with labels.  Then, compute the best linear combination.  Finally compare the claimed NDCG gain
		/// with the NDCG gain computed directly.  The relative frequencies of the labels are taken from the May 2005
		/// training set: 
		/// 
		/// Perfect:	0.0204
		/// Excellent: 	0.0523
		/// Good:		0.2714
		/// Fair:		0.2855
		/// Bad:		0.3704
		///
		/// Note we use random features to make it very unlikely that there will be any degeneracy: so the claimed delta NDCG
		/// should be what's actually measured by taking the linear combination that FindStep proposes.
		/// </summary>
		/// <param name="qc1"></param>
		/// <param name="qc2"></param>
		/// <param name="nDocsPerQuery"></param>
		static void SelfTest(int nQueries, int nDocsPerQuery, FindStepLib fs)
		{
			Random rangen = new Random(0);
			float[] priors = new float[5];
			priors[0] = 0.3704F; // bads first
			priors[1] = 0.2855F; 
			priors[2] = 0.2714F;
			priors[3] = 0.0523F;
			priors[4] = 0.0204F;
			double scale1 = 10.0;
            double scale2 = 20.0;
			int nScores = 1;
			QueryCollection qc1 = new QueryCollection(nQueries, priors, scale1, nScores, nDocsPerQuery, rangen);
			// Must share labels
			QueryCollection qc2 = qc1.CopyEmptyQueryCollection();
			for(int i = 0; i < qc2.queries.Length; ++i)
			{
                Query q1 = qc1.queries[i];
                Query q2 = qc2.queries[i];
				for(int j = 0; j < q1.Length; ++j)
				{
                    double label = (double) q1.Labels[j];
                    if (q2.Labels[j] != label)
                        throw new Exception("Labels mismatch.");
                    q1.scores[j] = (float)(label + scale1 * (2.0 * rangen.NextDouble() - 1.0));
                    q2.scores[j] = (float)(label + scale2 * (2.0 * rangen.NextDouble() - 1.0));
				}

			}

			double bestMeanNDCGGain;
            // We will only check for positive alphas.
		    double alpha = fs.FindStep(qc1, qc2, null, out bestMeanNDCGGain); // prints out the best NDCG gain
            Console.WriteLine("Optimal alpha = {0}", alpha);

            double firstFactor = fs.convex ? (1.0 - alpha) : 1.0;

			qc1.ComputeNDCGs();
			double initialNDCG_pes = qc1.NonTruncNDCG_pes;
			double initialNDCG_opt = qc1.NonTruncNDCG_opt;
			Console.WriteLine("Initial nonTruncNDCG = {0}-{1}", initialNDCG_pes, initialNDCG_opt);
			QueryCollection qc = QueryCollection.LinearlyCombine(firstFactor, qc1, alpha, qc2);
            qc.ComputeNDCGs();
			double finalNDCG_pes = qc.NonTruncNDCG_pes;
			double finalNDCG_opt = qc.NonTruncNDCG_opt;
			Console.WriteLine("Final nonTruncNDCG = {0}-{1}", finalNDCG_pes, finalNDCG_opt);

			Console.WriteLine("Type RETURN for exhaustive search");
			Console.ReadLine();
            double bestFound = 0.0;
            double maxAlpha = fs.convex ? 1.0 : fs.MaxStep;
            double alphaFactor = fs.alphaPos ? 1.0 : -1.0;
			for(int i = 0; i < 10001; ++i)
			{
				alpha = alphaFactor * (double)(i * maxAlpha) / 10000.0;
                qc = QueryCollection.LinearlyCombine(firstFactor, qc1, alpha, qc2);
                qc.ComputeNDCGs();
                if (qc.NonTruncNDCG_opt != qc.NonTruncNDCG_pes)
                    throw new Exception("Self test requires no degeneracy");
                double finalNDCG_mean = qc.NonTruncNDCG_mean;
				if(finalNDCG_mean > bestFound)
				{
					Console.WriteLine("Best NDCG found so far with search: alpha = {0}, NDCG = {1}", alpha, finalNDCG_mean);
					bestFound = finalNDCG_mean;
				}
			}

		}
	}
}
