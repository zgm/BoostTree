// C. Burges, Fall 2006
//
// Please do not submit changes without code review with CBurges.
using System;
using System.Collections;
using System.Text;

namespace Microsoft.TMSN
{
	public class RankedItem : IComparable
	{
		//static public int seed;
		//static public Random ran;
		//private static double scale;
		public double ndcgWt; // The gain divided by maxDCG for that query (_not_ the contribution to NDCG)
		public double score;
		public double score1;
		public double score2;
		public int rank;
		public float label;
//		public string QID; // For debugging only

		#region Constructors
		//static RankedItem()
		//{
		//    seed = 0;
		//    ran = new Random(seed);
		//    scale = 1e-8;
		//}

		public RankedItem(double ndcgWt, double score1, double score2, float label) //, string QID)
		{
			this.ndcgWt = ndcgWt;
			this.score1 = score1;
			this.score2 = score2;
			this.label = label;
			//			this.QID = QID;   // DEBUG ONLY
		}
		#endregion

		#region IComparable Members
		int IComparable.CompareTo(object obj)
		{
			return CompareTo((RankedItem)obj);
		}

		public int CompareTo(RankedItem other)
		{
			return score > other.score ? -1 : ( score < other.score ? 1 : 0 );
		}
		#endregion

		//public void setSeed(int seedIn)
		//{
		//    seed = seedIn;
		//}
	}

	public class PairRankedItems : IComparable
	{
		public RankedItem item1;
		public RankedItem item2;
		public double alpha; // The combination score at which they switch
		public PairRankedItems previous; // automatically construct linked list
		static public PairRankedItems mostRecent = null;

		#region Constructors
		public PairRankedItems(RankedItem r1, RankedItem r2)
		{
			item1 = r1;
			item2 = r2;
			previous = mostRecent;
			mostRecent = this;
		}
		#endregion

		#region IComparable Members
		int IComparable.CompareTo(object obj)
		{
			return CompareTo((PairRankedItems)obj);
		}

		public int CompareTo(PairRankedItems other)
		{
			return alpha > other.alpha ? 1 : ( alpha < other.alpha ? -1 : 0 );
		}
		#endregion

		static public void Reset()
		{
			mostRecent = null;
		}
	}

    public class SortPairRankedItemsIncreasing : IComparer
    {
        int IComparer.Compare(Object xO, Object yO)
        {
            PairRankedItems x = (PairRankedItems)xO;
            PairRankedItems y = (PairRankedItems)yO;

            int ans;
            if (x.alpha > y.alpha)
                ans = 1;
            else if (x.alpha < y.alpha)
                ans = -1;
            else
                ans = 0;
            return ans;
        }
    }

    public class SortPairRankedItemsDecreasing : IComparer
    {
        int IComparer.Compare(Object xO, Object yO)
        {
            PairRankedItems x = (PairRankedItems)xO;
            PairRankedItems y = (PairRankedItems)yO;

            int ans;
            if (x.alpha > y.alpha)
                ans = -1;
            else if (x.alpha < y.alpha)
                ans = 1;
            else
                ans = 0;
            return ans;
        }
    }


}
