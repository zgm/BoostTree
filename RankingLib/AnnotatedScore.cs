// C. Burges, Summer 2006.  Please do not submit changes without code review.
using System;
using System.Collections;

namespace Microsoft.TMSN
{
	/// <summary>
	/// A class to hold the score, the label, and the index into the original list, for a given
	/// url result (for a given query).  There are two comparers: one to sort by score, the other, by label.
	/// To be used with the standard array(AnnotatedScores[], Comparer).
	/// </summary>
	public class AnnotatedScore
	{
		public double score;
		public float label;
		public int srcIdx; // The index in the original list of this item.

		public AnnotatedScore()
		{
			score = 0.0;
			label = -1;
			srcIdx = -1;
		}


		public AnnotatedScore(double score, float label, int srcIdx)
		{
			this.score = score;
			this.label = label;
			this.srcIdx = srcIdx;
		}


		public AnnotatedScore(double score, float label) : this(score, label, -1) 
		{
		}


		/// <summary>
		/// Self-evident.
		/// </summary>
		/// <param name="vals"></param>
		/// <param name="labels"></param>
		/// <returns></returns>
		static public AnnotatedScore[] Populate(double[] vals, float[] labels)
		{
			AnnotatedScore[] annScores = new AnnotatedScore[vals.Length];
			for (int i=0; i<vals.Length; ++i)
			{
				annScores[i] = new AnnotatedScore(vals[i], labels[i], i);
			}
			return annScores;
		}


		static public void DeepCopy(AnnotatedScore[] asIn, AnnotatedScore[] asOut)
		{
			if(asIn.Length != asOut.Length) throw new Exception("AnnotatedScore: DeepCopy: size mismatch");

			for(int i=0; i<asIn.Length; ++i)
			{
				asOut[i].score = asIn[i].score;
				asOut[i].label = asIn[i].label;
				asOut[i].srcIdx = asIn[i].srcIdx;
			}
		}
	}



	/// <summary>
	/// Sorts by label, in reverse order.so that a call to Array.Sort results in most positive first
	/// </summary>
	public class LabelComparer : IComparer
	{
		int IComparer.Compare(Object x, Object y) 
		{
			AnnotatedScore ao1, ao2;
			if(x is AnnotatedScore && y is AnnotatedScore)
			{
				ao1 = x as AnnotatedScore;
				ao2 = y as AnnotatedScore;
			} 
			else
			{
				throw new Exception("LabelComparer: object not of type AnnotatedScore");
			}
				
			return ao1.label > ao2.label ? -1 : (ao1.label == ao2.label ? 0 : 1);
		}
	}


	/// <summary>
	/// Sorts by score, so that a call to Array.Sort results in most positive first
	/// </summary>
	public class ScoreComparer : IComparer
	{
		int IComparer.Compare(Object x, Object y)
		{
			AnnotatedScore ao1, ao2;
			if(x is AnnotatedScore && y is AnnotatedScore)
			{
				ao1 = x as AnnotatedScore;
				ao2 = y as AnnotatedScore;
			}
			else
			{
				throw new Exception("LabelComparer: object not of type AnnotatedScore");
			}

			return ao1.score > ao2.score ? -1 : ( ao1.score == ao2.score ? 0 : 1 );
		}
	}


	/// <summary>
	/// Sorts by score, so that a call to Array.Sort results in: most positive scores first, and within a degenerate block
	/// (same scores), sort by label, most positive first.  Useful for e.g. computing the most optimistic NDCG.
	/// </summary>
	public class OptimisticAnnotatedScoreComparer : IComparer
	{
		int IComparer.Compare(Object x, Object y)
		{
			AnnotatedScore ao1, ao2;
			if(x is AnnotatedScore && y is AnnotatedScore)
			{
				ao1 = x as AnnotatedScore;
				ao2 = y as AnnotatedScore;
			}
			else
			{
				throw new Exception("LabelComparer: object not of type AnnotatedScore");
			}

			int ans;
			if(ao1.score > ao2.score)
				ans = -1;
			else if(ao1.score < ao2.score)
				ans = 1;
			else
			{
				if(ao1.label > ao2.label)
					ans = -1;
				else if(ao1.label < ao2.label)
					ans = 1;
				else
					ans = 0;
			}
			return ans;
		}
	}


	/// <summary>
	/// Sorts by score, so that a call to Array.Sort results in: most positive scores first, and within a degenerate block
	/// (same scores), sort by label, smallest first.  Useful for e.g. computing the most pessimistic NDCG.
	/// </summary>
	public class PessimisticAnnotatedScoreComparer : IComparer
	{
		int IComparer.Compare(Object x, Object y)
		{
			AnnotatedScore ao1, ao2;
			if(x is AnnotatedScore && y is AnnotatedScore)
			{
				ao1 = x as AnnotatedScore;
				ao2 = y as AnnotatedScore;
			}
			else
			{
				throw new Exception("LabelComparer: object not of type AnnotatedScore");
			}

			int ans;
			if(ao1.score > ao2.score)
				ans = -1;
			else if(ao1.score < ao2.score)
				ans = 1;
			else
			{
				if(ao1.label < ao2.label)
					ans = -1;
				else if(ao1.label > ao2.label)
					ans = 1;
				else
					ans = 0;
			}
			return ans;
		}
	}

}
