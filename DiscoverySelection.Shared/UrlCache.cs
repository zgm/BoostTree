//******************************************************************************************************************************************
// Copyright (c) Microsoft Corporation.
//
// @File: UrlCache.cs
//
// Owner: liefuliu
// Purpose:
//	     Segmented resersed Url, schema with 4 key columns as domain, host suffix, L1 path suffix and Url suffix, 
//  are widely used in Bing,especially in index coverage applications. Most of Urls are stored in order of the 
//  segmented resevered Url.
//       Converting a segmented reversed Url to into an integral, either reversed or not, requiring the string 
// concatenation which is runtime costly. UrlCache is to optmized such kind of computation, based on Url similarity, 
//  or the domain, host and l1path locality equally
//       Considering there are 2 Urls, where 4 key columns are
//       (d1, h1, l1, u1)
//       (d1, h1, l1, u2)
//       In order to get the integral reversed Url, typically we need to 
//          string Url1 = d1 + h1 + l1 + u1;
//          string Url2 = d1 + h1 + l1 + u2;
//	     
//       The general ideal is leveraging Url cache, to safe the computation cost of (d1 + h1 + l1) when calculate Url2, in case these 3 columns are equal ot those 
// of the previous - Url 1
//******************************************************************************************************************************************

using System;
using System.Collections.Generic;
using System.Text;

namespace DiscoverySelection.Shared
{
    public abstract class UrlCache
    {
        #region Constructors

        public UrlCache()
        {
            
        }

        #endregion Constructors

        #region Public Methods
        public String PushUrlAndFetch(String reversedDomain, 
                         String reversedHostSuffix, 
                         String l1PathSuffix,
                         String UrlSuffix)
        {
            Boolean updateDomain = false;
            Boolean updateHost = false;
            Boolean updateL1Path = false;

            if (reversedDomain == lastReversedDomain)
            {
                if (reversedHostSuffix == lastReversedHostSuffix)
                {
                    if (l1PathSuffix == lastL1PathSuffix)
                    {

                    }
                    else
                    {
                        updateL1Path = true;
                    }
                }
                else
                {
                    updateHost = true;
                    updateL1Path = true;

                }
            }
            else
            {
                updateDomain = true;
                updateHost = true;
                updateL1Path = true;
            }

            if (updateDomain)
            {
                Common.Assert(updateHost);
                lastReversedDomain = reversedDomain;
                OnDomainChanged();
            }

            if (updateHost)
            {
                Common.Assert(updateL1Path);

                lastReversedHostSuffix = reversedHostSuffix;
                OnHostChanged();
            }

            if (updateL1Path)
            {
                lastL1PathSuffix = l1PathSuffix;
                OnL1PathChanged();
            }

            return Fetch(UrlSuffix);
        }

        #endregion Public Methods

        #region Protected Methods
        protected abstract String Fetch(string UrlSuffix);

        #endregion Protected Methods


        #region Protected Methods

        protected abstract void OnDomainChanged();
        protected abstract void OnHostChanged();
        protected abstract void OnL1PathChanged();

        #endregion Protected Methods

        #region Private Methods

        #endregion Private Methods

        #region Protected Fields

        protected String lastReversedDomain = null;
        protected String lastReversedHostSuffix = null;
        protected String lastL1PathSuffix = null;

        #endregion Protected Fields
    }


    public abstract class UrlCacheOptimizedForReversedUrl : UrlCache
    {
        protected override void OnDomainChanged()
        {

        }

        protected override void OnHostChanged()
        {
            lastReversedHost = lastReversedDomain + lastReversedHostSuffix;
        }

        protected override void OnL1PathChanged()
        {
            lastReversedL1Path = lastReversedHost + lastL1PathSuffix;
        }

        protected override String Fetch(string UrlSuffix)
        {
            return lastReversedL1Path + UrlSuffix;
        }

        String lastReversedHost = null;
        String lastReversedL1Path = null;
    }

    public class UrlCacheOptimizedForOrganicUrl : UrlCache
    {
        protected override void OnDomainChanged()
        {

        }

        protected override void OnHostChanged()
        {
            
        }

        protected override void OnL1PathChanged()
        {
            lastL1Path = UrlReverser.ReverseBack(lastReversedDomain + lastReversedHostSuffix + lastL1PathSuffix);
        }

        protected override String Fetch(string UrlSuffix)
        {
            return lastL1Path + UrlSuffix;
        }


        #region Private

        String lastL1Path = null;

        #endregion 
    }
}
