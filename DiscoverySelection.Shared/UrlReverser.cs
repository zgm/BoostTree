using System;
using System.Collections.Generic;
using System.Text;
using DiscoverySelection.Shared;

namespace DiscoverySelection.Shared
{
    public class UrlReverser : IInitializable
    {
        private const String tldFileParamName = "tldfile";

        private const String schemeHttp   = "http";
        private const String schemeHttps  = "https";
        private const String schemeSuffix = "://";
        private const Char portDelimeter  = ':';
        public const Char HostDelimeter  = '.';
        public const Char PathDelimeter  = '/';
        private static Char[] L1PathDelimeters = {'/', '?'};

        public const Int32 MaxPortLength = 5; // Corresponds to the valid port number range of 0-65535

        public const Char SchemeHttpEncodingSymbol  = 'h';
        private const Char schemeHttpsEncodingSymbol = 's';
        public const Char PortSpacer = '-';

        public const Int32 ProtocolPrefixLength = MaxPortLength + 1;

        private Dictionary<String, Boolean> tlds = new Dictionary<String, Boolean>();
        private Dictionary<String, Boolean> reversedtlds = new Dictionary<String, Boolean>();

        private Int32 maxTLDSegs = 0;

        public UrlReverser()
        {
        }

        public UrlReverser(String tldfn)
        {
            LoadTLDs(tldfn);
        }

        #region IInitializable Members

        public void Initialize(Dictionary<String, String> parameters)
        {
            if (parameters == null)
            {
                throw new Exception("Parameters dictionary is not specified");
            }

            if (!parameters.ContainsKey(tldFileParamName))
            {
                throw new Exception("tldfilename parameter not found");
            }

            LoadTLDs(parameters[tldFileParamName]);
        }

        #endregion

        /// <summary>
        /// Read in a list of TLD's, one TLD per line
        /// All TLD's must start with a period, for example ".com".
        /// This function will lowercase all TLD's.
        /// </summary>
        /// <param name="tldfn">Text file with list of TLD</param>
        private void LoadTLDs(String tldfn)
        {
            tlds.Clear();
            reversedtlds.Clear();

            using (System.IO.TextReader tr = new System.IO.StreamReader(tldfn))
            {
                String line = null;

                Char[] delim = { HostDelimeter };

                while ((line = tr.ReadLine()) != null)
                {
                    if (line.Length == 0)
                    {
                        continue;
                    }

                    String[] segs = line.Split(delim, StringSplitOptions.RemoveEmptyEntries);

                    Common.Assert(segs.Length <= 2, "Invalid TLD read: " + line);

                    String tld = String.Empty;
                    String reversedtld = String.Empty;

                    for (Int32 i = segs.Length - 1; i >= 0; i--)
                    {
                        tld += segs[i] + HostDelimeter;
                        reversedtld += segs[segs.Length - 1 - i ] + HostDelimeter;
                    }

                    string lowerTld = tld.ToLower();
                    tlds.Add(lowerTld, false);

                    string lowerreversedtld = reversedtld.ToLower();
                    reversedtlds.Add(lowerreversedtld, false);

                    maxTLDSegs = Math.Max(maxTLDSegs, segs.Length);
                }
            }
        }

        /// <summary>
        /// Reverses a key (URL, Domain, Host, L1 Path)
        /// </summary>
        /// <param name="url">Key to reverse</param>
        /// <returns>Reversed key, null on error or invalid URL</returns>
        public static String Reverse(String key)
        {
            if (String.IsNullOrEmpty(key))
            {
                return null;
            }

            Int32 schemeEndIndex = key.IndexOf(schemeSuffix);
            if (schemeEndIndex == -1 || schemeEndIndex + 3 == key.Length)
            {
                return null;
            }

            Int32 hostStartIndex = schemeEndIndex + 3;

            String scheme = key.Substring(0, schemeEndIndex);
            
            if (scheme != schemeHttp && scheme != schemeHttps)
            {
                return null;
            }

            Int32 hostEndIndex = key.IndexOf(PathDelimeter, hostStartIndex);
            if (hostEndIndex == -1)
            {
                hostEndIndex = key.Length;
            }
            
            if (hostEndIndex == hostStartIndex)
            {
                return null;
            }

            String hostport = key.Substring(hostStartIndex, hostEndIndex - hostStartIndex);

            String[] byColon = hostport.Split(portDelimeter);

            String host = null;
            String port = null;
            if (byColon.Length == 2)
            {
                host = byColon[0];
                port = byColon[1];

                if (port.Length == 0 || port.Length > MaxPortLength)
                {
                    return null;
                }
            }
            else if (byColon.Length > 2 || byColon.Length == 0)
            {
                return null;
            }
            else
            {
                host = byColon[0];
            }

            String[] byDot = host.Split(HostDelimeter);

            if (byDot.Length < 2)
            {
                // Hosts consisting of one or zero segments
                return null;
            }

            Boolean isIP = false;

            // Check if the right-most host segment is number
            UInt16 dummy;
            if (UInt16.TryParse(byDot[byDot.Length - 1], out dummy))
            {
                if ((byDot.Length == 4) && IsIPHost(byDot[0], byDot[1], byDot[2], byDot[3]))
                {
                    isIP = true;
                }
                else
                {
                    // Url is not a full valid IP address but it's right-most host segment is number - discard
                    return null;
                }
            }

            StringBuilder reversedUrl = new StringBuilder();

            // Output scheme encoding symbol

            if (scheme == schemeHttp)
            {
                reversedUrl.Append(SchemeHttpEncodingSymbol);
            }
            else
            {
                reversedUrl.Append(schemeHttpsEncodingSymbol);
            }

            // Output port number

            if (port != null)
            {
                reversedUrl.Append(port);
                reversedUrl.Append(PortSpacer, MaxPortLength - port.Length);                
            }
            else
            {
                reversedUrl.Append(PortSpacer, MaxPortLength);
            }

            if (isIP)
            {
                reversedUrl.Append(host);
                reversedUrl.Append(HostDelimeter);
            }
            else
            {
                for (Int32 i = byDot.Length - 1; i >= 0; i--)
                {
                    if (byDot[i].Length == 0)
                    {
                        return null;
                    }
                    reversedUrl.Append(byDot[i]);
                    reversedUrl.Append(HostDelimeter);
                }
            }

            if (hostEndIndex != key.Length)
            {
                reversedUrl.Append(key.Substring(hostEndIndex));
            }

            return reversedUrl.ToString();
        }

        /// <summary>
        /// Reverses URL, Domain or Host back
        /// Expects string that was properly reversed by Reverse function
        /// </summary>        
        /// <returns>Reversed back string, null on error or invalid string</returns>
        public static String ReverseBack(String domain, String hostSuffix, String l1Path, String urlSuffix)
        {
            if (domain == null || hostSuffix == null || l1Path == null || urlSuffix == null)
            {
                return null;
            }

            return ReverseBack(domain + hostSuffix + l1Path + urlSuffix);
        }

        /// <summary>
        /// Reverses URL, Domain or Host back
        /// Expects string that was properly reversed by Reverse function
        /// </summary>        
        /// <returns>Reversed back string, null on error or invalid string</returns>
        public static String ReverseBack(String reversedKey)
        {
            if (String.IsNullOrEmpty(reversedKey))
            {
                return null;
            }

            if (reversedKey.Length < ProtocolPrefixLength)
            {
                return null;
            }

            String port = null;
            String scheme = null;

            if (reversedKey[0] == SchemeHttpEncodingSymbol)
            {
                scheme = schemeHttp;
            }
            else if (reversedKey[0] == schemeHttpsEncodingSymbol)
            {
                scheme = schemeHttps;
            }
            else
            {
                return null;
            }

            Int32 portEndIndex = reversedKey.IndexOf(PortSpacer);

            if (portEndIndex == -1 || portEndIndex > (ProtocolPrefixLength) - 1)
            {
                portEndIndex = 1 + MaxPortLength;
            }

            if (portEndIndex > 1)
            {
                port = reversedKey.Substring(1, portEndIndex - 1);
            }

            Int32 hostStartIndex = ProtocolPrefixLength;

            if (hostStartIndex >= reversedKey.Length)
            {
                return null;
            }

            Int32 hostEndIndex = reversedKey.IndexOf(PathDelimeter, hostStartIndex);
            if (hostEndIndex == -1)
            {
                hostEndIndex = reversedKey.Length;
            }

            if (hostEndIndex == hostStartIndex)
            {
                return null;
            }

            String host = reversedKey.Substring(hostStartIndex, hostEndIndex - hostStartIndex - 1);

            String[] byDot = host.Split(HostDelimeter);

            Boolean isIP = byDot.Length == 4 && IsIPHost(byDot[0], byDot[1], byDot[2], byDot[3]);

            StringBuilder url = new StringBuilder();

            url.Append(scheme);
            url.Append(schemeSuffix);

            if (isIP)
            {
                url.Append(host);
            }
            else
            {
                for (Int32 i = byDot.Length - 1; i >= 0; i--)
                {
                    if (byDot[i].Length == 0)
                    {
                        return null;
                    }
                    if (i != byDot.Length - 1)
                    {
                        url.Append(HostDelimeter);
                    }
                    url.Append(byDot[i]);
                }
            }

            if (port != null)
            {
                url.Append(portDelimeter);
                url.Append(port);
            }

            if (hostEndIndex != reversedKey.Length)
            {
                url.Append(reversedKey.Substring(hostEndIndex, reversedKey.Length - hostEndIndex));
            }

            return url.ToString();
        }

        /// <summary>
        /// ReverseDomainHostBack:
        /// To reverse the domain/host back, with no scheme and no port
        /// h80---com.bing. ==>bing.com rather than http://bing.com:80
        /// </summary>
        /// <param name="reversedKey"></param>
        /// <returns></returns>
        public static string ReverseDomainHostBack(string reversedKey)
        {
            if (String.IsNullOrEmpty(reversedKey))
            {
                return null;
            }

            if (reversedKey.Length < ProtocolPrefixLength)
            {
                return null;
            }

            String port = null;
            
            if (reversedKey[0] == SchemeHttpEncodingSymbol)
            {
            }
            else if (reversedKey[0] == schemeHttpsEncodingSymbol)
            {
            }
            else
            {
                return null;
            }

            Int32 portEndIndex = reversedKey.IndexOf(PortSpacer);

            if (portEndIndex == -1 || portEndIndex > (ProtocolPrefixLength) - 1)
            {
                portEndIndex = 1 + MaxPortLength;
            }

            if (portEndIndex > 1)
            {
                port = reversedKey.Substring(1, portEndIndex - 1);
            }

            Int32 hostStartIndex = ProtocolPrefixLength;

            if (hostStartIndex >= reversedKey.Length)
            {
                return null;
            }

            Int32 hostEndIndex = reversedKey.IndexOf(PathDelimeter, hostStartIndex);
            if (hostEndIndex == -1)
            {
                hostEndIndex = reversedKey.Length;
            }

            if (hostEndIndex == hostStartIndex)
            {
                return null;
            }

            String host = reversedKey.Substring(hostStartIndex, hostEndIndex - hostStartIndex - 1);

            String[] byDot = host.Split(HostDelimeter);

            Boolean isIP = byDot.Length == 4 && IsIPHost(byDot[0], byDot[1], byDot[2], byDot[3]);

            StringBuilder url = new StringBuilder();

            if (isIP)
            {
                url.Append(host);
            }
            else
            {
                for (Int32 i = byDot.Length - 1; i >= 0; i--)
                {
                    if (byDot[i].Length == 0)
                    {
                        return null;
                    }
                    if (i != byDot.Length - 1)
                    {
                        url.Append(HostDelimeter);
                    }
                    url.Append(byDot[i]);
                }
            }


            if (hostEndIndex != reversedKey.Length)
            {
                url.Append(reversedKey.Substring(hostEndIndex, reversedKey.Length - hostEndIndex));
            }

            return url.ToString();
        }


        /// <summary>
        /// Given the host segmented by dots (.), determine whether the host is an IP host
        /// </summary>
        /// <returns></returns>
        public static Boolean IsIPHost(String seg1, String seg2, String seg3, String seg4)
        {

            Boolean isIP = false;
            
            UInt16 dummy;
            if (UInt16.TryParse(seg1, out dummy) &&
                UInt16.TryParse(seg2, out dummy) &&
                UInt16.TryParse(seg3, out dummy) &&
                UInt16.TryParse(seg4, out dummy))
            {
                isIP = true;
            }

            return isIP;
        }

        /// <summary>
        /// Extract domain, host, L1 path from properly reversed URL string
        /// </summary>
        /// <param name="reversedUrl">Reversed URL string as input</param>
        /// <param name="domain">Extracted domain</param>
        /// <param name="host">Extracted host</param>
        /// <param name="l1Path">Extracted L1 path</param>
        public void ExtractDomainHostPath(String reversedUrl, out String domain, out String host, out String l1Path)
        {
            String prefix = reversedUrl.Substring(0, ProtocolPrefixLength);
            String url = reversedUrl.Substring(ProtocolPrefixLength);
            Int32 pos = url.IndexOf(PathDelimeter);

            domain = null; // just to avoid compile warning

            if (pos == -1)
            {
                host = url;
                l1Path = null;
            }
            else
            {
                host = url.Substring(0, pos);

                String[] segs = url.Substring(pos).Split(L1PathDelimeters);

                if (segs.Length < 3)
                {
                    l1Path = null;
                }
                else
                {
                    l1Path = reversedUrl.Substring(0, ProtocolPrefixLength + pos + 1 + segs[1].Length + 1);
                }
            }

            // Extract domain from host

            String[] hostsegs = host.Split(new Char[]{HostDelimeter}, StringSplitOptions.RemoveEmptyEntries);

            if (hostsegs.Length == 4 && IsIPHost(hostsegs[0], hostsegs[1], hostsegs[2], hostsegs[3]))
            {
                // IP Host
                domain = host;
            }
            // 0, 1, or 2 segments -- return original
            else if (hostsegs.Length <= 2)
            {
                domain = host;
            }
            else
            {
                Boolean tldFound = false;

                // hostsegs.Length >= 3
                for (Int32 i = Math.Min(maxTLDSegs, hostsegs.Length - 2); i > 0; i--)
                {
                    String tld = hostsegs[0] + HostDelimeter;

                    for (Int32 j = 1; j <= i; j++)
                    {
                        tld += hostsegs[j] + HostDelimeter;
                    }

                    if (tlds.ContainsKey(tld))
                    {
                        domain = String.Format("{0}{1}{2}", tld, hostsegs[i + 1], HostDelimeter);
                        tldFound = true;
                        break;
                    }
                }

                if (!tldFound)
                {
                    // tld prefix not found, so just return the first two segments
                    domain = String.Format("{0}{1}{2}{3}", hostsegs[0], HostDelimeter, hostsegs[1], HostDelimeter);
                }
            }

            host = prefix + host;
            domain = prefix + domain;
        }

        /// <summary>
        /// Extracts domain, host, and 1st level path from a given URL.
        /// </summary>
        /// <param name="url">URL (not a reversed one). Should be well-formed and starting with a schema (http:// or https:// are accepted).</param>
        /// <param name="domain">An extracted domain if extraction was successful. E.g. for "http://www.foo.com/1.html" it will contain "foo.com".</param>
        /// <param name="host">An extracted domain if extraction was successful. E.g. for "http://www.foo.com/1.html" it will contain "www.foo.com".</param>
        /// <param name="l1path">An extracted domain if extraction was successful. E.g. for "http://www.foo.com/1.html" it will contain null, for "http://www.foo.com/images/1.jpg" - "www.foo.com/images/".</param>
        /// <returns></returns>
        public bool ExtractDomainHostPathFromUrl(String url, out String domain, out String host, out String l1path)
        {
            // initializing output parameters
            domain = null;
            host = null;
            l1path = null;

            String port = null;

            // return all null values for null or empty urls
            if (String.IsNullOrEmpty(url))
            {
                return false;
            }

            Int32 schemeSuffixIdx = url.IndexOf(schemeSuffix);
            if (schemeSuffixIdx == -1 || schemeSuffixIdx + schemeSuffix.Length == url.Length)
            {
                // returning if no scheme or there is nothing except for scheme
                return false;
            }

            String scheme = url.Substring(0, schemeSuffixIdx);
            if (scheme != schemeHttp && scheme != schemeHttps)
            {
                // not supported schema
                return false;
            }

            int hostIdx = schemeSuffixIdx + schemeSuffix.Length;
            int hostEndIdx = url.IndexOf(PathDelimeter, hostIdx);
            if (hostEndIdx == hostIdx)
            {
                return false;
            }

            if (hostEndIdx == -1)
            {
                hostEndIdx = url.Length;
            }

            String hostport = url.Substring(hostIdx, hostEndIdx - hostIdx);
            String[] hostportByPortDelim = hostport.Split(portDelimeter);

            if (hostportByPortDelim.Length == 0 || hostportByPortDelim.Length > 2)
            {
                // malformed url
                return false;
            }
            else if (hostportByPortDelim.Length == 1)
            {
                host = hostport;
            }
            else if (hostportByPortDelim.Length == 2)
            {
                if (hostportByPortDelim[1].Length == 0 
                    || hostportByPortDelim[1].Length > MaxPortLength
                    || hostportByPortDelim[0].Length == 0)
                {
                    return false;
                }

                host = hostportByPortDelim[0];
                port = hostportByPortDelim[1];
            }

            if (hostEndIdx != -1 && hostEndIdx < url.Length - 1)
            {
                string[] l1pathSegs = url.Substring(hostEndIdx + 1).Split(L1PathDelimeters);
                if (l1pathSegs.Length > 1)
                {
                    l1path = String.Format("{0}{1}{2}{3}",
                                           host, PathDelimeter, l1pathSegs[0], url[hostEndIdx + l1pathSegs[0].Length + 1]);
                }
            }

            String[] hostByDelim = host.Split(HostDelimeter);
            if (hostByDelim.Length <= 2)
            {
                // malformed url
                domain = host;
            }
            else if (hostByDelim.Length == 4 && IsIPHost(hostByDelim[0], hostByDelim[1], hostByDelim[2], hostByDelim[3]))
            {
                domain = host;
            }
            else
            {
                bool tldFound = false;
                for (int i = Math.Min(maxTLDSegs, hostByDelim.Length - 1); i > 0; i--)
                {
                    string tld = HostDelimeter + hostByDelim[hostByDelim.Length - 1];
                    for (int j = 1; j < i; j++)
                    {
                        tld = String.Format("{0}{1}{2}", HostDelimeter, hostByDelim[hostByDelim.Length - 1 - j], tld);
                    }

                    if (tlds.ContainsKey(ReverseTld(tld)))
                    {
                        tldFound = true;
                        domain = String.Format("{0}{1}", hostByDelim[hostByDelim.Length - i - 1], tld);
                        break;
                    }
                }

                if (!tldFound)
                {
                    domain = String.Format("{0}{1}{2}",
                                           hostByDelim[hostByDelim.Length - 2], HostDelimeter, hostByDelim[hostByDelim.Length - 1]);
                }
            }

            return true;
        }

        private String ReverseTld(String tld)
        {
            if (String.IsNullOrEmpty(tld))
            {
                return tld;
            }

            String[] segments = tld.Split(new char[] { HostDelimeter }, StringSplitOptions.RemoveEmptyEntries);

            StringBuilder sb = new StringBuilder();
            for (int i = segments.Length - 1; i > -1; i--)
            {
                sb.Append(segments[i] + HostDelimeter);
            }

            return sb.ToString();
        }

        /// <summary>
        /// ExtractSegmentedReversedUrl
        /// </summary>
        /// <param name="key"></param>
        /// <param name="reversed"></param>
        /// <param name="srUrl"></param>
        /// <returns></returns>
        public SegmentedReversedUrl ExtractSegmentedReversedUrl(String reversedKey, bool reversed)
        {
            SegmentedReversedUrl srUrl = null;

            if (!reversed)
            {
                reversedKey = UrlReverser.Reverse(reversedKey);
            }

            if (String.IsNullOrEmpty(reversedKey))
            {
                return srUrl;
            }

            if (reversedKey.Length < ProtocolPrefixLength)
            {
                return srUrl;
            }

            String port = null;
            
            if (reversedKey[0] == SchemeHttpEncodingSymbol)
            {
            }
            else if (reversedKey[0] == schemeHttpsEncodingSymbol)
            {
            }
            else
            {
                return srUrl;
            }

            Int32 portEndIndex = reversedKey.IndexOf(PortSpacer);

            if (portEndIndex == -1 || portEndIndex > (ProtocolPrefixLength) - 1)
            {
                portEndIndex = 1 + MaxPortLength;
            }

            if (portEndIndex > 1)
            {
                port = reversedKey.Substring(1, portEndIndex - 1);
            }

            // Start to parse host here

            Int32 hostStartIndex = ProtocolPrefixLength;

            if (hostStartIndex >= reversedKey.Length)
            {
                return srUrl;
            }

            Int32 hostEndIndex = reversedKey.IndexOf(PathDelimeter, hostStartIndex);
            if (hostEndIndex == -1)
            {
                hostEndIndex = reversedKey.Length;
            }

            if (hostEndIndex == hostStartIndex)
            {
                return srUrl;
            }

            String host = reversedKey.Substring(hostStartIndex, hostEndIndex - hostStartIndex);
            String[] byDot = host.Split(new char[]{HostDelimeter}, StringSplitOptions.RemoveEmptyEntries);
            Boolean isIP = byDot.Length == 4 && IsIPHost(byDot[0], byDot[1], byDot[2], byDot[3]);

            int domainEndIndex = hostStartIndex;

            if (isIP)
            {
                domainEndIndex = hostEndIndex;
            }
            else if (byDot.Length <= 2)
            {
                domainEndIndex = hostEndIndex;
            }
            else
            {
                // Get domain 
                bool tldfound = false;

                for (; domainEndIndex < hostEndIndex; ++domainEndIndex)
                {
                    if (reversedKey[domainEndIndex] != HostDelimeter)
                    {
                        continue;
                    }
                    else
                    {

                        string suspiciousTld = reversedKey.Substring(hostStartIndex, domainEndIndex - hostStartIndex + 1);

                        if (tlds.ContainsKey(suspiciousTld))
                        {
                            tldfound = true;
                        }
                        else
                        {
                            if (tldfound)
                            {
                                // The sub string up to the last HostDelimeter is the tld
                                // so that the sub string up to here is the domain

                                ++domainEndIndex;
                                break;
                            }
                        }

                    }
                }

                // tld prefix not found, so just treat the first two segments as the domain
                if (!tldfound)
                {
                    domainEndIndex = hostStartIndex;
                    domainEndIndex += byDot[0].Length + 1 + byDot[1].Length + 1;
                }
            }

            
            // Find L1Path
            int L1EndIndex = -1;
            int L1StartIndex = -1;
            int UrlEndIndex = reversedKey.Length;

            if (hostEndIndex == reversedKey.Length)
            {
                L1EndIndex = 0;
                L1StartIndex = 0;
                UrlEndIndex = 0;
            }
            else
            {
                L1StartIndex = hostEndIndex;
                L1EndIndex = reversedKey.IndexOfAny(L1PathDelimeters, L1StartIndex + 1);

                if (L1EndIndex == -1)
                {
                    L1EndIndex = L1StartIndex;
                }

                ++L1EndIndex;
            }

            String domain = reversedKey.Substring(0, domainEndIndex );
            String hostSuffix = reversedKey.Substring(domainEndIndex, hostEndIndex - domainEndIndex);
            String l1PathSuffix = reversedKey.Substring(L1StartIndex, L1EndIndex - L1StartIndex);
            String urlSuffix = reversedKey.Substring(L1EndIndex, UrlEndIndex - L1EndIndex);

            // Sanity check for segments
            // will potentially hurt the performance, so don't use it in production
            /*
            if (l1PathSuffix.Length > 0)
            {
                Common.Assert(l1PathSuffix[0] == '/',
                    "Assertion failed: L1PathSuffix doesn't start with /, L1PathSuffix = {0}, Key = {1}",
                    l1PathSuffix,
                    reversedKey);

                
                Common.Assert(l1PathSuffix[l1PathSuffix.Length - 1] == L1PathDelimeters[0] ||
                              l1PathSuffix[l1PathSuffix.Length - 1] == L1PathDelimeters[1],
                    "Assertion failed: L1PathSuffix doesn't end with /, L1PathSuffix = {0}, Key = {1}",
                    l1PathSuffix,
                    reversedKey);
            }
            */


            srUrl = new SegmentedReversedUrl(domain,
                                          hostSuffix,
                                          l1PathSuffix,
                                          urlSuffix);

            return srUrl;
        }
    }
}