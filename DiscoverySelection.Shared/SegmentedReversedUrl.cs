using System;
using System.Collections.Generic;
using System.Text;

namespace DiscoverySelection.Shared
{
    /// <summary>
    /// SegmentedReversedUrl:
    /// members in class SegmentedReversedUrl are the key columns of URL repository v2.
    /// Add more comments here:
    /// 
    /// </summary>
    public class SegmentedReversedUrl
    {
        private String reversedDomain;

        public String ReversedDomain
        {
            get { return reversedDomain; }
        }

        private String reversedHostSuffix;

        public String ReversedHostSuffix
        {
            get { return reversedHostSuffix; }
            set { reversedHostSuffix = value; }
        }


        private String l1PathSuffix;

        public String L1PathSuffix
        {
            get { return l1PathSuffix; }
            set { l1PathSuffix = value; }
        }

        private String urlSuffix;

        public String UrlSuffix
        {
            get { return urlSuffix; }
            set { urlSuffix = value; }
        }

        public SegmentedReversedUrl(string reversedDomain,
                                    string reversedHostSuffix,
                                    string l1PathSuffix,
                                    string urlSuffix)
        {
            this.reversedDomain = reversedDomain;
            this.reversedHostSuffix = reversedHostSuffix;
            this.l1PathSuffix = l1PathSuffix;
            this.urlSuffix = urlSuffix;
        }

        public string ReversedHost
        {
            get { return ReversedDomain + reversedHostSuffix; }
        }

        public string ReversedL1Path
        {
            get { return ReversedHost + l1PathSuffix; }
        }

        // to improve perf
        public string ReversedUrl
        {
            get { return reversedDomain + reversedHostSuffix + l1PathSuffix + urlSuffix; }
        }

    }
}
