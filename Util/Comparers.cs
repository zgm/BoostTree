using System;
using System.Collections;
using System.Text;

namespace Microsoft.TMSN
{

    // REVIEW: These should be generic-ised!
    public class ReverseComparer : IComparer
    {
        int IComparer.Compare(Object xO, Object yO)
        {
            double x = (double)xO;
            double y = (double)yO;

            int ans;
            if (x > y)
                ans = -1;
            else if (x < y)
                ans = 1;
            else
                ans = 0;
            return ans;
        }
    }

    public class FloatReverseComparer : IComparer
    {
        int IComparer.Compare(Object xO, Object yO)
        {
            float x = (float)xO;
            float y = (float)yO;

            int ans;
            if (x > y)
                ans = -1;
            else if (x < y)
                ans = 1;
            else
                ans = 0;
            return ans;
        }
    }

}
