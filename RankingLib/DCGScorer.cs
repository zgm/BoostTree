// C. Burges, January 2007.
//
// Please do not submit changes without code review with cburges.
using System;
using System.Collections;
using System.Diagnostics;

namespace Microsoft.TMSN
{
	/// <summary>
	/// A cleaned up and simplified version of the DCG computations in RankMeasures.cs.
	/// Note that this code only computes DCGs, not NDCGs.  Two kinds of pathology can occur with 
	/// NDCG: for a given query, all the labels can be the same, or worse, they can all be zero (giving
	/// zero maxNonTruncDCG).  It is left up to the calling code to decide what to do with such queries.
	/// This class assumes that any mapping from (negative) labels to LabelForUnlabeled has been done
	/// upstream (to avoid multiple checks).
	/// </summary>
	public class DCGScorer
	{
		private const int nLabels = 5;
		public int NLabels { get { return nLabels; } }
		public static int truncLevel;  // Global, defined only here.  Allow classes to access this without an instance
		public readonly static float[] scoresMap = new float[nLabels] {0, 3, 7, 15, 31};
        public readonly static float[] scoresMapGoogle = new float[10] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
		private static int maxTruncLevel = 10000;  // The max expected truncation level.
		public static double[] discounts; // The one used in this instantiation
		public static double[] cDiscounts; // Cumulative sum of discounts

		
		/// <summary>
		/// Precompute the discount values.
		/// </summary>
		static DCGScorer()
		{
			discounts = new double[maxTruncLevel];
			cDiscounts = new double[maxTruncLevel];
			double discount;
			double cDiscount = 0.0;
			for(int i = 0; i < maxTruncLevel; i++)
			{
				// Note top ranked object has rank 1, not 0.  Note log base does not matter.
				discount = 1.0 / Math.Log(2 + i);
				cDiscount += discount;
				discounts[i] = discount;
				cDiscounts[i] = cDiscount;
			}
		}


		/// <summary>
		/// Note that this constructor is kept as light as possible: it is likely to be called upon often.
		/// </summary>
		public DCGScorer()
		{
		}


		/// <summary>
		/// WARNING: For speed, it is assumed that we can sort the scores and labels in place.  Creating scratch space for this in the static
		/// constructor won't work with threading, and creating scratch space in the other constructor is very expensive, since may be called
		/// often.  It is left to the user to make copies beforehand if necessary.
		/// </Summary>
		/// <param name="scores"></param>
		/// <param name="labels"></param>
		/// <param name="truncNDCG"></param>
		/// <param name="nonTruncNDCG"></param>
		public void ComputeDCGs(bool pessimistic, double[] scores, float[] labels, out double truncDCG, out double nonTruncDCG)
		{
			Debug.Assert(scores.Length == labels.Length, "scores, labels length mismatch");
			AnnotatedScore[] annScores = new AnnotatedScore[scores.Length];
			for(int i = 0; i < annScores.Length; ++i)
			{
				annScores[i] = new AnnotatedScore(scores[i], labels[i]);
			}
			ComputeDCGs(pessimistic, annScores, out truncDCG, out nonTruncDCG);
		}


		public void ComputeMeanDCGs(double[] scores, float[] labels, out double truncDCG, out double nonTruncDCG)
		{
			Debug.Assert(scores.Length == labels.Length, "scores, labels length mismatch");
			AnnotatedScore[] annScores = new AnnotatedScore[scores.Length];
			for(int i = 0; i < annScores.Length; ++i)
			{
				annScores[i] = new AnnotatedScore(scores[i], labels[i]);
			}
			ComputeMeanDCGs(annScores, out truncDCG, out nonTruncDCG);
		}


		public void ComputeDCGs(bool pessimistic, AnnotatedScore[] annScores, out double truncDCG, out double nonTruncDCG)
		{
			if (pessimistic)
				Array.Sort(annScores, new PessimisticAnnotatedScoreComparer());
			else
				Array.Sort(annScores, new OptimisticAnnotatedScoreComparer());

			truncDCG = 0.0;
			nonTruncDCG = 0.0;
			for (int rank = 0; rank < annScores.Length; ++rank)
			{
				double markup = DCGScorer.discounts[rank];
				double gain = DCGScorer.scoresMap[(int)annScores[rank].label];
				if (rank < DCGScorer.truncLevel)
				{
					truncDCG += markup * gain;
				}
				nonTruncDCG += markup * gain;
			}
		}


		/// <summary>
		/// If a bunch of urls have the same score, the mean NDCG for that bunch is the mean gain, times the total markup.
		/// Best to call this after ComputeOptimisticDCGs, since then the sorted order will be the same.  Note that handling
		/// the truncated NDCG is tricky: if a block of urls with equal score, overlaps the truncation level, then you still
		/// have to compute the mean gain over the full block, and split the result to get the contribution to the truncated NDCG.
		/// </summary>
		/// <param name="annScores"></param>
		/// <param name="meanTruncDCG"></param>
		/// <param name="meanNonTruncDCG"></param>
		public void ComputeMeanDCGs(AnnotatedScore[] annScores, out double meanTruncDCG, out double meanNonTruncDCG)
		{
			Array.Sort(annScores, new OptimisticAnnotatedScoreComparer());

			meanTruncDCG = 0.0;
			meanNonTruncDCG = 0.0;
			double score;
			double lastScore = annScores[0].score;
			double samePatchCtr = 0.0;
			double gain = 0.0;
			double markupTrunc = 0.0;
			double markupNonTrunc = 0.0;
			double deltaDCG;

			int truncLevel = DCGScorer.truncLevel < annScores.Length ? DCGScorer.truncLevel : annScores.Length;
			for (int rank = 0; rank < truncLevel; ++rank)
			{
				score = annScores[rank].score;
				if (score != lastScore)
				{
					deltaDCG = ( gain / samePatchCtr ) * markupTrunc;
					meanTruncDCG += deltaDCG;
					meanNonTruncDCG += deltaDCG;
					samePatchCtr = 1.0;
					gain = 0.0;
					markupTrunc = 0.0;
					lastScore = score;
				}
				else
				{
					++samePatchCtr;
				}
				markupTrunc += DCGScorer.discounts[rank];
				gain += DCGScorer.scoresMap[(int)annScores[rank].label];
			}

			bool doneTruncComp = false;
			markupNonTrunc = markupTrunc;
			for (int rank = truncLevel; rank < annScores.Length; ++rank)
			{
				score = annScores[rank].score;
				if (score != lastScore)
				{
					if(!doneTruncComp)
					{
						meanTruncDCG += ( gain / samePatchCtr ) * markupTrunc;
						doneTruncComp = true;
					}
					meanNonTruncDCG += ( gain / samePatchCtr ) * markupNonTrunc;
					samePatchCtr = 1.0;
					gain = 0.0;
					markupNonTrunc = 0.0;
					lastScore = score;
				}
				else
				{
					++samePatchCtr;
				}
				markupNonTrunc += DCGScorer.discounts[rank];
				gain += DCGScorer.scoresMap[(int)annScores[rank].label];
			}
			if(!doneTruncComp)
			{
				meanTruncDCG += ( gain / samePatchCtr ) * markupTrunc;
			}
			meanNonTruncDCG += ( gain / samePatchCtr ) * markupNonTrunc;
		}


		/// <summary>
		/// Fastest to compute if no truncLevel.  Could also do with markupsHelper with a trunc level, but you'd have to check and backtrack.
		/// </summary>
		/// <param name="labelHist"></param>
		/// <param name="truncLevel"></param>
		/// <returns></returns>
		public double ComputeMaxDCG(int[] labelHist)
		{
			double maxDCG = 0.0;
			int level = 0;
			double startDiscount;
			for (int i = nLabels - 1; i >= 0; --i)
			{
				int nDocs = labelHist[i];
				startDiscount = (level == 0) ? 0.0 : cDiscounts[level - 1];
				if (nDocs > 0)
				{
					int newLevel = level + nDocs;
					maxDCG += scoresMap[i] * (cDiscounts[newLevel - 1] - startDiscount);
					level = newLevel;
				}
			}

			return maxDCG;
		}

		public double ComputeMaxDCG(float[] labels)
		{
			int[] labelHist = new int[nLabels];

			for (int i = 0; i < labels.Length; ++i)
				++labelHist[(int)labels[i]];

			return ComputeMaxDCG(labelHist);
		}


		/// <summary>
		/// Cheaper than sorting by label and then computing DCG.
		/// </summary>
		/// <param name="annScores"></param>
		/// <param name="truncLevel"></param>
		/// <returns>Maximum DCG value for a query.</returns>
		public double ComputeMaxTruncDCG(float[] labels)
		{
			int[] labelHist = new int[nLabels];

			for (int i = 0; i < labels.Length; ++i)
				++labelHist[(int)labels[i]];

			return ComputeMaxTruncDCG(labelHist);
		}


		public double ComputeMaxTruncDCG(int[] labelHist)
		{
			double maxDCG = 0.0;

			int level = 0;
			for(int i = nLabels - 1; i >= 0; --i)
			{
				double score = scoresMap[i];
				for(int j = 0; j < labelHist[i] && level < truncLevel; ++j)
				{
					maxDCG += score * discounts[level];
					++level;
				}
				if(level == truncLevel) break;
			}

			return maxDCG;
		}

		public double ComputeMinDCG(int[] labelHist)
		{
			double minDCG = 0.0;
			int level = 0;
			double startDiscount;
			for (int i = 0; i < nLabels; ++i)
			{
				int nDocs = labelHist[i];
				startDiscount = (level == 0) ? 0.0 : cDiscounts[level - 1];
				if (nDocs > 0)
				{
					int newLevel = level + nDocs;
					minDCG += scoresMap[i] * (cDiscounts[newLevel - 1] - startDiscount);
					level = newLevel;
				}
			}
			return minDCG;
		}


		public double ComputeMinTruncDCG(int[] labelHist)
		{
			double minDCG = 0.0;
			int level = 0;
			for(int i = 0; i < nLabels; ++i)
			{
				double score = scoresMap[i];
				for(int j = 0; j < labelHist[i] && level < truncLevel; ++j)
				{
					minDCG += score * discounts[level];
					++level;
				}
				if(level == truncLevel)
					break;
			}
			return minDCG;
		}

		public double ComputeMinDCG(int[] labelHist, int offset)
		{
			double minDCG = 0.0;
			int level = offset;
			double startDiscount;
			for(int i = 0; i < nLabels; ++i)
			{
				int nDocs = labelHist[i];
				startDiscount = (level == 0) ? 0.0 : cDiscounts[level - 1];
				if(nDocs > 0)
				{
					int newLevel = level + nDocs;
					minDCG += scoresMap[i] * (cDiscounts[newLevel - 1] - startDiscount);
					level = newLevel;
				}
			}
			return minDCG;
		}

		public double ComputeMinTruncDCG(int[] labelHist, int startLevel)
		{
			double minDCG = 0.0;

			int level = startLevel;
			if (level >= truncLevel)
				return minDCG;

			for (int i = 0; i < nLabels; ++i)
			{
				double score = scoresMap[i];
				for (int j = 0; j < labelHist[i] && level < truncLevel; ++j)
				{
					minDCG += score * discounts[level];
					++level;
				}
				if (level == truncLevel)
					break;
			}

			return minDCG;
		}


		/// <summary>
		/// Compute two DCG values: the full one, and the one truncated at 'truncLevel', but starting at level 'startLevel'.  These are
		/// incremental DCGs (in the sense that they could be added to the DCG for those samples ranked higher than startLevel).
		/// </summary>
		/// <param name="labelHist"></param>
		/// <param name="startLevel"></param>
		/// <param name="truncLevel"></param>
		/// <param name="minIncrFullDCG"></param>
		/// <param name="minIncrTruncDCG"></param>
		public void ComputeMinFullAndTruncDCG(int[] labelHist, int startLevel, out double minIncrFullDCG, out double minIncrTruncDCG)
		{
			minIncrFullDCG = 0.0;
			minIncrTruncDCG = 0.0;
			int level = startLevel;
			if (truncLevel == int.MaxValue)
			{
				for (int i = 0; i < nLabels; ++i)
				{
					double score = scoresMap[i];
					for (int j = 0; j < labelHist[i]; ++j)
					{
						minIncrFullDCG += score * discounts[level];
						++level;
					}
				}
				minIncrTruncDCG = minIncrFullDCG;
			}
			else
			{
				for (int i = 0; i < nLabels; ++i)
				{
					double score = scoresMap[i];
					for (int j = 0; j < labelHist[i]; ++j)
					{
						double delta = score * discounts[level];
						minIncrFullDCG += delta;
						if (level < truncLevel)
							minIncrTruncDCG += delta;
						++level;
					}
				}
			}
		}


		public void ComputeMeanFullAndTruncDCG(int[] labelHist, int startLevel, out double meanIncrFullDCG, out double meanIncrTruncDCG)
		{
			meanIncrFullDCG = 0.0;
			meanIncrTruncDCG = 0.0;
			int level = startLevel;

			double tot = 0.0;
			double meanLabelScore = 0.0;
			for (int i = 0; i < labelHist.Length; ++i)
			{
				tot += labelHist[i];
				meanLabelScore += (double)labelHist[i] * scoresMap[i];
			}

			if (tot > 0.0) // Else, leave at zero.
				meanLabelScore = meanLabelScore / tot;


			double cumulativeFullMarkups = 0.0;
			double cumulativeTruncMarkups = 0.0;
			double markup;
			if (truncLevel == int.MaxValue)
			{
				for (int i = 0; i < labelHist.Length; ++i)
				{
					for (int j = 0; j < labelHist[i]; ++j)
					{
						cumulativeFullMarkups += discounts[level];
						++level;
					}
				}
				meanIncrFullDCG = meanLabelScore * cumulativeFullMarkups;
				meanIncrTruncDCG = meanIncrFullDCG;
			}
			else
			{
				for (int i = 0; i < labelHist.Length; ++i)
				{
					for (int j = 0; j < labelHist[i]; ++j)
					{
						markup = discounts[level];
						cumulativeFullMarkups += markup;
						if (level < truncLevel)
							cumulativeTruncMarkups += markup;
						++level;
					}
				}
				meanIncrFullDCG = meanLabelScore * cumulativeFullMarkups;
				meanIncrTruncDCG = meanLabelScore * cumulativeTruncMarkups;
			}
		}

	}

}
