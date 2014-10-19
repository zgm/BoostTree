using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using System.Collections;


namespace DiscoverySelection.Shared
{
    // tiers for depth allocation
    public enum LocalTier
    {
        MinSmallIndex = 0, // if the score is above MinSmallIndex, then the URL will be assigned to the small index
        MinBigIndex,
        MinZ1Index,
        MinIntIndex,
        MinZ2Index,
        MaxSmallIndex, // if the score is below MaxSmallIndex, then the URL will be dropped from the small index
        MaxBigIndex,
        MaxZ1Index,
        MaxIntIndex,
        MaxZ2Index,
        NumLocalTier
    };

    // used to replace LocalTier
    public enum DepthAllocationTier
    {
        MinSmallIndex=0,
        MinZ1Index,
        MinZ2Index,
        MaxSmallIndex,
        MaxZ1Index,
        MaxZ2Index,
        NumTier
    };

    // value of each entry is a bitmap of the 5 tiers: Small Big Z1 Intl Z2 Drop
    // 0 represent it can't be allocated to that tier
    // 1 represent it is possible to be alloated to that tier
    [Flags]
    public enum AllocationTier : byte
    {
        SmallIndex = 0x20,              // (100000)     // Top 3B
        AtLeastBigIndex = 0x30,         // (110000)     // Top 10B
        AtLeastZ1Index = 0x38,          // (111000)     // Top 18.5B
        AtLeastIntIndex = 0x3C,         // (111100)     // Top 20B
        AtLeastZ2Index = 0x3E,          // (111110)     // Top 50B
        Undecided = 0x3F,               // (111111)
        BigIndexOnly = 0x10,            // (010000)
        BigIndexOrZ1Index = 0x18,       // (011000)
        BigOrZ1OrIntIndex = 0x1C,       // (011100)
        BigOrZ1OrIntOrZ2Index = 0x1E,   // (011110)
        AtMostBigIndex = 0x1F,          // (011111)
        Z1IndexOnly = 0x08,             // (001000)
        Z1IndexOrIntIndex = 0x0C,       // (001100)
        Z1OrIntOrZ2Index = 0x0E,        // (001110)
        AtMostZ1Index =0x0F,            // (001111)
        IntIndexOnly = 0x04,            // (000100)
        IntIndexOrZ2Index = 0x06,       // (000110)
        AtMostIntIndex =0x07,           // (000111)
        Z2IndexOnly = 0x02,             // (000010)
        AtMostZ2Index = 0x03,           // (000011)
        Dropped = 0x01                  // (000001)
    };

    // k/u/d decision for each url, per segment
    // keep can be extened to keep_in_small, keep_in_z1, etc. later
    public enum SegmentAllocationTier
    {
        Unknown = 0,    //no segment tier is assigned to this url. It's different from Undecided.
        Drop,   
        Undecided,
        Keep
    };


    /// <summary>
    /// enum bit mask to represent crawl rules and crawl job types
    /// </summary>
    public enum CrawlContext : byte
    {
        Unknown = 0,
        // rule type used to generate crawl candidates
        // each rule should ideally have a score/priority for cutoffs
        // do not rearrange rules
        Rule_ClickedUrl =1,
        Rule_Ie8Hub =2,
        Rule_WebmapFrontier =3, // with a threshold on inlinkcount
        Rule_OutlinkHub =4,    //with a  cutoff on rawpagerank, outlink count
        Rule_TbHub=5,
        Rule_NewDomainHost=6,
        Rule_AllDomainHost=7,
        Rule_SitemapUrl=8,
        Rule_DirectoryIndex=9,  //folder prefix  or index page
        Rule_CrawlInjection0=10,
        Rule_CrawlInjection1=11,
        Rule_CrawlInjection2=12,
        Rule_CrawlInjection3=13,
        Rule_SitemapFile=14,
        Rule_CrawlSchedulerInjection=15,
        Num_CrawlRules,     // size of (scored) crawl rules list
        // scores will not be stored in CrawlScoreInfo for bits beyond this

        Src_HubCrawl=35,
        Src_FreshCrawl=36,
        // sc jobs flipped to dc by DSC internal job handler PU
        //subset of SC: current depth <= 2 are counted as SC
        Flag_SCflipSC=37,
        //subset of DC: current depth > 2 will are counted as DC
        Flag_SCflipDC=38,
        Job_UnknownForumCrawl = 39,
        Job_RecrawlExpired = 40,
        Job_ShallowCrawl2=41,
        Job_ShallowCrawl1=42,
        Job_ShallowCrawl=43,
        Job_DeepCrawl= 44,
        Job_IntelligentDeepCrawl = 45,
        Job_SitemapCrawl= 46,
        Job_KnownForumCrawl = 47,
        // DSCv3 accepts 48 bits from CrawlSelection
        // rules list size is dynamic
        // so non-rule info is coded bottom up from bit 47

        // bits from here on will be used by Crawl Selection and not passed on to DSCv3
        Flag_ExcludeDomainHost=48, //these 3 flags are for new domain selection
        Flag_ForNewDomainCS=49,
        Flag_ForNewHostCS=50,     //candidates for new domain/host crawl selection
        Flag_HasCrawlPolicy = 51,
        Flag_IsDomainHostHomePage = 52,
        JobPolicy_Frequency = 53,
        JobPolicy_Depth = 54,
        JobPolicy_Priority = 55,
        Flag_HasCrawlHistory = 56,
        Flag_CrawledByDP = 57,
        // until 63 only
    }

    [Flags]
    public enum CrawlSourceFlag : ulong
    {
        None                 =0,
        DeepCrawl            =0x1,
        Recrawl          =0x2,
        ShallowCrawl         =0x4,
        Flip                 =0x8,
        SocialDiscovery  =0x10,
        SuperFreshDiscoveryFrd   =0x20,
        LocalSearch      =0x40,
        ForumCrawl       =0x80,
        SFDiscovery      =0x100,
        MMVideo          =0x200,
        TigerDiscovery       =0x400,
        HermesDiscovery  =0x800,
        FastCrawl            =0x1000,
        RecrawlExpansion     =0x2000,
        Foreign          =0x4000,
        ContentStore         =0x8000,
        ForceInjection       =0x10000,
        ForceRecrawl         =0x20000,
        RejectIfNoConvert    =0x40000,
        Webmap           =0x80000,
        HiddenWeb            =0x100000,
        RssDiscovery         =0x200000,
        FastCrawl2       =0x400000,
        ShallowCrawlDiscovery=0x800000,
        ForumIndex       =0x1000000,
        RecrawlDiscovery     =0x2000000,
        FrontierDiscovery    =0x4000000,
        HubCrawl             =0x8000000,   
        ShallowCrawlSeeds    =0x10000000,
        FastCrawlDiscovery   =0x20000000,
        DFSelectionDiscovery=0x40000000,
        HiddenWebDiscovery   =0x80000000,
    }


    public enum CrawlSelectionOutputTypes : byte
    {
        ScoredUrl = 0,
        NewAllDomainHosts,
        CrawlScores,
        CrawlJobPolicy,
        DebugLog,
        SitemapFile,

    }

    [Flags]
    public enum DeepCrawlV3SummaryTypes : byte
    {
        Unknown = 0x0,
        DownloadSuccess = 0x1,
        Discovery = 0x2,
        Redirect = 0x4,
        DownloadFailure = 0x8,
        ShallowCrawlExpired = 0x10
    }

    
    /// <summary>
    /// Referer type for a url. ~(InLinkPage, SourcePage)
    /// </summary>
    [Flags]
    public enum UrlSourceType : uint
    {
        Unknown = 0x0,
        //  NON serp web page
        Browse = 0x1,
        //Any Serp including Baidu, Daum, Naver, Yandex
        Serp = 0x2,
        Google = 0x4,   
        Bing = 0x8,
        Yahoo = 0x10,
        DirectVisit = 0x20, // known to be a direct click  nav256
        FaceBook = 0x40,
        Twitter = 0x80,
        // domain/host home page
        HomePage = 0x100,
        // not serp
        OutsideDomain = 0x200,
        // never spammed whitelist from IQ
        // Trusted = 0x400,

    }
    
    public interface IInitializable
    {
        void Initialize(Dictionary<String, String> parameters);
    }

    public interface ISerializable
    {
        Byte[] Serialize();
        void Deserialize(Byte[] bytes);
    }


    public static class Common
    {
        public static void Assert(Boolean condition, String messageFormat, params Object[] Args)
        {
            if (!condition)
            {
                string message = String.Format(messageFormat, Args);
                throw new Exception(String.Format("Assertion: {0}", message));
            }
        }

        public static void Assert(Boolean condition, String message)
        {
            if (!condition)
            {
                throw new Exception(String.Format("Assertion: {0}", message));
            }
        }

        public static void Assert(Boolean condition)
        {
            if (!condition)
            {
                throw new Exception("Assertion failed.");
            }
        }

        private static Char scopeArgPrefix = '-';

        public static Dictionary<String, String> ScopeArgsToDictionary(String[] args)
        {
            Dictionary<String, String> dict = new Dictionary<String, String>();

            if (args == null)
            {
                return dict;
            }

            for (Int32 i = 0; i < args.Length; i++)
            {
                String argName = args[i];

                if (String.IsNullOrEmpty(argName))
                {
                    continue;
                }

                Assert(argName[0] == scopeArgPrefix, String.Format("Argument {0} doesn't start with prefix \"{1}\"",
                    argName, scopeArgPrefix));

                Assert(i != args.Length - 1, String.Format("No value specified for argument {0}", argName));

                String argValue = args[++i];

                dict.Add(argName.Substring(1).ToLower(), argValue);
            }

            if (dict.ContainsKey("forcetofail") && Boolean.Parse(dict["forcetofail"]))
            {
                Assert(false, "The job is forced to fail due to parameter forcetofail is set to true");
            }

            return dict;
        }
    }

    public static class Helpers
    {
        public static T Create<T>(Dictionary<String, String> parameters) where T : class, new()
        {
            T instance = new T();
            if (instance is IInitializable)
            {
                ((IInitializable)instance).Initialize(parameters);
            }
            return instance;
        }

        public static T CreateSingleton<T>(Dictionary<String, String> parameters) where T : class, new()
        {
            T instance = SingletonHolder<T>.Instance;
            if (instance is IInitializable)
            {
                ((IInitializable)instance).Initialize(parameters);
            }
            return instance;
        }

        public static T CreateSerializableObject<T>(byte[] serializedData) where T : class, ISerializable, new()
        {
            T metadata = null;

            if (serializedData != null)
            {
                metadata = new T();
                metadata.Deserialize(serializedData);
            }

            return metadata;
        }
    }


    public class ConfigStreamReader : StreamReader
    {

        public ConfigStreamReader(string fn)
            : base(fn)
        {

        }

        /// <summary>
        /// Removes empty lines
        /// Trim white spaces on start and end of lines
        /// Ignores lines starting with ; or #
        /// </summary>
        /// <returns></returns>
        public override string ReadLine()
        {
            string line = null;

            do
            {
                line = base.ReadLine();
                if (line != null)
                {
                    line = line.Trim();
                }

                // || shortcuts so the '#' comparison will be okay
            } while (line != null && (line.Length == 0 || line[0] == '#' || line[0] == ';' || line[0] == '['));

            return line;
        }

    }

    public class HierarchicConfigStreamReader : StreamReader
    {
        public HierarchicConfigStreamReader(string fn)
            : base(fn)
        {

        }

        /// Difference from ConfigStreamReader is that it keeps lines starting with '['
        /// </summary>
        /// <returns></returns>
        public override string ReadLine()
        {
            string line = null;

            do
            {
                line = base.ReadLine();
                if (line != null)
                {
                    line = line.Trim();
                }
            } while (line != null && (line.Length == 0 || line[0] == '#' || line[0] == ';'));

            return line;
        }

    }


    public class Util
    {
        public static int GetLangCode( string lang )
        {
            LanguageCode langC = LanguageCode.None;
            try
            {
                langC = (LanguageCode) Enum.Parse(typeof(LanguageCode), lang, true) ;
            }
            catch( Exception )
            {
            }
            return (int) langC;
        }
        
        public static UInt32 ParseConfigUInt32(string expectedKey, string input)
        {
            string[] segs = input.Split('=');
            Common.Assert(segs.Length == 2, "Key parse error");
            Common.Assert(segs[0] == expectedKey, "Key not match");

            return UInt32.Parse(segs[1]);
        }

        public static Single ParseConfigSingle(string expectedKey, string input)
        {
            string[] segs = input.Split('=');
            Common.Assert(segs.Length == 2, "Key parse error");
            Common.Assert(segs[0] == expectedKey, "Key not match");

            return Single.Parse(segs[1]);
        }

        //1F2A --> (31,42)
        public static byte[] HexStringToByteArray(String strHex)
        {
            int len = strHex.Length;
            if( String.IsNullOrEmpty( strHex ) )    return null;
            
            if( (len % 2) != 0) throw new Exception("HexStringToByteArray: ill formed hex string [" + strHex + "]");

            byte[] bytes = new byte[len / 2];

            for (int i = 0; i < len; i += 2)
            {
                bytes[i/2] = Convert.ToByte(strHex.Substring(i, 2), 16);
            }

            return bytes;
        }
        //(31,42) --> 1F2A
        public static String  ByteArrayToHexString(byte[] bytes)
        {
            if( bytes == null ) return String.Empty;
            
            int len = bytes.Length;
            StringBuilder sb = new StringBuilder();
            
            for (int i = 0; i < len; i++)
            {
                sb.Append(String.Format( "{0:X2}", bytes[i] ));
            }

            return sb.ToString();
        }

        public static Byte [] Serialize(object obj)
        {
            BinaryFormatter formatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();

            try
            {
                formatter.Serialize(stream, obj);
            }
            catch (SerializationException e) 
            {
                Console.WriteLine("Serialization error: " + e.Message);
                throw;
            }

            return stream.ToArray();
        }

        public static object Deserialize(Byte[] bytes)
        {
            object obj = null;
            BinaryFormatter formatter = new BinaryFormatter();
            Stream stream = new MemoryStream(bytes);
            try
            {
                obj = formatter.Deserialize(stream);
            }
            catch (SerializationException e) 
            {
                Console.WriteLine("Deserialization error: " + e.Message);
                throw;
            }

            return obj;
        }


        private static readonly DateTime baseDate = DateTime.Parse("01/01/2009");
        
        public static DateTime GetDateFromVersion(UInt32 version)
        {
            return baseDate.AddDays(version);
        }

        public static UInt16 GetVersionOfToday()
        {
            TimeSpan interval = DateTime.Today - baseDate;
            return (UInt16)(interval.Days);
        }

        public static UInt16 GetDayVersion(string date)
        {
            try
            {
                return GetDayVersion(DateTime.Parse(date));
            }
            catch
            {
                throw new Exception(String.Format("date string is either not a valid format, or a earlier version than 2009-01-01: {0}", date));
            }
        }

        public static UInt16 GetDayVersion(DateTime date)
        {
            try
            {
                TimeSpan interval = date - baseDate;
                return interval.Days <= 0 ? (UInt16)0 : (UInt16)(interval.Days);
            }
            catch
            {
                throw new Exception(String.Format("date string is either not a valid format, or a earlier version than 2009-01-01: {0}", date));
            }
        }

        public static bool IsHomePage(string url)
        {
            try
            {
                Uri baseUri = new Uri(url);
                int num = baseUri.Segments.GetLength(0);
                return num == 1 &&
                    baseUri.Query == string.Empty &&
                    baseUri.Fragment == string.Empty;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static AllocationTier AllocationTierMerge(AllocationTier left, AllocationTier right)
        {
            // take the common part of the two tiers
            byte common = (byte)(left & right);
            return (common > 0) ? (AllocationTier)common : AllocationTier.Undecided;
        }

        public static FileFormatType GetFileFormatFromDocType( UInt32?  DocumentType )
        {
            
            FileFormatType format = FileFormatType.Unknown;
            
            DocType docType = (DocType) (DocumentType??0);
            
            switch( docType )
            {
                case DocType.dt_HTML : format = FileFormatType.html;
                                        break;
                case DocType.dt_PDF : format = FileFormatType.pdfps;
                                        break;
                case DocType.dt_PS : format = FileFormatType.pdfps;
                                        break;
                case DocType.dt_IMAGE : format = FileFormatType.image;
                                        break;
                case DocType.dt_VIDEO : format = FileFormatType.video;
                                        break;
                case DocType.dt_AUDIO : format = FileFormatType.audio;
                                        break;
                case DocType.dt_TEXT : format = FileFormatType.text;
                                        break;
                case DocType.dt_DOC : format = FileFormatType.word;
                                        break;
                case DocType.dt_PPT : format = FileFormatType.ppt;
                                        break;
                case DocType.dt_XLS: format = FileFormatType.excel;
                                        break;
                case DocType.dt_RTF : format = FileFormatType.word;
                                        break;
                case DocType.dt_XML : format = FileFormatType.xml;
                                        break;
                case DocType.dt_RSS : format = FileFormatType.feed;
                                        break;
                case DocType.dt_SWF : format = FileFormatType.flash;
                                        break;
                case DocType.dt_SITEMAP: format = FileFormatType.sitemap;
                                        break;
                default:    break;
            }
            
            return format;
        }

        public static Dictionary<String, Boolean> URLListFileToDictionary(String filename)
        {
            Dictionary<String, Boolean> dictURL = new Dictionary<String, Boolean>();

            using (StreamReader sr = new StreamReader(filename))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    line = line.Trim();

                    if (line == String.Empty)
                    {
                        continue;
                    }

                    if (line.StartsWith("http") || line.StartsWith("https"))
                    {
                        line = UrlReverser.Reverse(line);
                    }

                    if (!dictURL.ContainsKey(line))
                    {
                        dictURL.Add(line, true);
                    }
                }
            }

            return dictURL;
        }


        /// verified hosts/domains with no content OR totally spam/junk, but have insanely large #of host/L1paths
        public static bool IsExcludableDomainHostRevKey(string domainR, string hostSuffix)
        {
            if( hostSuffix == "www." ) return false;
             if ( domainR.StartsWith("h-----com.live", StringComparison.InvariantCultureIgnoreCase) && hostSuffix.Contains("cid-" ) ) return true;

             string [] domainBlackList = {
                "h-----uk.co.immospy.",
                "h-----com.firstareadrom.",
                "h-----cc.co.",
                "h-----us.craigslist-jobs.",
                "h-----com.craigslist-real-estate.",
                "h-----com.coffeerabbit.",
                "h-----com.rentals-craigslist.",
                "h-----com.craigslist-cars.",
            };    
            
            foreach( string badDomain in domainBlackList )
            {
                if( domainR == badDomain  ) return true;

            }         
             return false;            
        }
        
    }

    public class Assert<T> where T : IComparable<T>
    {
        public static void Equal(T left, T right, String message)
        {
            Common.Assert(left.CompareTo(right) == 0, "{0}: {1} != {2}", message, left, right);
        }

        public static void NE(T left, T right, String message)
        {
            Common.Assert(left.CompareTo(right) != 0, "{0}: {1} == {2}", message, left, right);
        }

        public static void Greater(T left, T right, String message)
        {
            Common.Assert(left.CompareTo(right) > 0, "{0}: {1} <= {2}", message, left, right);
        }

        public static void GE(T left, T right, String message)
        {
            Common.Assert(left.CompareTo(right) >= 0, "{0}: {1} < {2}", message, left, right);
        }

        public static void Less(T left, T right, String message)
        {
            Common.Assert(left.CompareTo(right) < 0, "{0}: {1} >= {2}", message, left, right);
        }

        public static void LE(T left, T right, String message)
        {
            Common.Assert(left.CompareTo(right) <= 0, "{0}: {1} > {2}", message, left, right);
        }

        public static void Equal(T left, T right)
        {
            Common.Assert(left.CompareTo(right) == 0, "{0} != {1}", left, right);
        }

        public static void NE(T left, T right)
        {
            Common.Assert(left.CompareTo(right) != 0, "{0} == {1}", left, right);
        }

        public static void Greater(T left, T right)
        {
            Common.Assert(left.CompareTo(right) > 0, "{0} <= {1}", left, right);
        }

        public static void GE(T left, T right)
        {
            Common.Assert(left.CompareTo(right) >= 0, "{0} < {1}", left, right);
        }

        public static void Less(T left, T right)
        {
            Common.Assert(left.CompareTo(right) < 0, "{0} >= {1}", left, right);
        }

        public static void LE(T left, T right)
        {
            Common.Assert(left.CompareTo(right) <= 0, "{0} > {1}", left, right);
        }
    }
}
