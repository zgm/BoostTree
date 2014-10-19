using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.IO;

namespace DiscoverySelection.Shared
{
    // static classes can't implement interfaces, but we want to
    public static class Configs // : IInitializable
    {
        public static SelectionConfig Selection = null;
        private static Object selectionConfigLock = new Object();
        private static string selectionParamKey = "selectionconfig";

        public static CrawlSelectionConfig CrawlSelection = null;
        private static Object crawlSelectionConfigLock = new Object();
        private static string crawlSelectionParamKey = "crawlselectionconfig";

        public static SegmentSelectionConfig SegmentSelection = null;
        private static Object segmentSelectionConfigLock = new Object();
        private static string segmentSelectionParamKey = "segmentselectionconfig";
        

        public static void Initialize(Dictionary<String, String> parameters)
        {
            lock (selectionConfigLock)
            {
                if (Selection == null && parameters.ContainsKey(selectionParamKey))
                {
                    Selection = Helpers.CreateSingleton<SelectionConfig>(parameters);
                }
            }

            lock (crawlSelectionConfigLock)
            {
                if (CrawlSelection == null && parameters.ContainsKey(crawlSelectionParamKey))
                {
                    CrawlSelection = Helpers.CreateSingleton<CrawlSelectionConfig>(parameters);
                }
            }

            lock (segmentSelectionConfigLock)
            {
                if (SegmentSelection == null && parameters.ContainsKey(segmentSelectionParamKey))
                {
                    SegmentSelection = Helpers.CreateSingleton<SegmentSelectionConfig>(parameters);
                }
            }
        }
    }




    public class SelectionConfig : IInitializable
    {
        // All public fields will be considered a configuration key
        public String ServeSelectClassifier = "Linear";
        public String ServeSelectFtTransformFn = "ServeFtTransform.txt";
        public String ServeSelectClassifierFn = "ServeClassifier.txt";
        public UInt32 ServeSelectBoostNumIter = 0;


        // MinScore should be >= RankHistogramStart
        public Single MinScore = 0.001f;

        public Double RankHistogramStart = 0.001;
        public Double RankHistogramStep = 1.001;
        public UInt32 RankHistogramSize = 100000;
        public Boolean EnableCurveFitting = true;

        public UInt64 TierSizeSmall = 3000000000;
        public UInt64 TierSizeSmallBig = 10000000000;
        public UInt64 TierSizeSmallBigZ1 = 14000000000;
        public UInt64 TierSizeSmallBigZ1Int = 17000000000;
        public UInt64 TierSizeSmallBigZ1IntZ2 = 20000000000;
        public UInt64 TierSizeSmallBigZ1IntZ2BeforeDedup = 21000000000;

        public UInt32 DemoteBinShiftSmall = 3;
        public UInt32 DemoteBinShiftBig = 2;
        public UInt32 DemoteBinShiftZ1 = 2;
        public UInt32 DemoteBinShiftInt = 2;
        public Double AllocationDomainRankRatioBig = 100;
        public Double AllocationDomainRankRatioZ1 = 200;

        public Double InjectionPointSmall = 0.7;
        public Double InjectionPointBig = 0.65;
        public Double InjectionPointZ1 = 0.8;
        public Double InjectionPointInt = 0.8;
        public Double InjectionPointZ2 = 0.8;

        public Boolean EnableDepthAllocation = true;
        public Boolean EnableUrlAllocation = true; /// EnableUrlAllocation is by default off if EnableDepthAllocation=false
        public Boolean EnableDepthAllocationSensor = true;


        public String ScorePostTransform = "X";

        // WARNING: Do not enable in production without manager approval.
        public Boolean SelectExclusiveTBIE8MetricsUrls = false;

        // Any URL not from any of the following sensor types will not be selected
        // See SensorTypes and SensorMask
        public String SelectUrlsFromSensors = "Sitemap|Injection|URLFeatures|DocumentFeatures|DeepCrawl|Fex|Registration";

        /// PS item: 561511
        /// This is a work around for feature improving the bottom 20B selection on Feb, 2011
        /// The work around is to disable the URLs from top 20B selection which only come from DeepCrawl, DeltaWebmap and
        /// other non-selection candidate sensor
        public Boolean ExcludeSomeSensorsFromTop20B = false;
        public String SelectTop20BUrlsFromSensors = String.Empty;
        // End of Ps item: 561511


        // Don't turn this on except for testing
        public Boolean SelectSpamUrls = false;
        public Boolean SelectBlockedExtensionUrls = false;

        public Boolean DropPreviousRankSessions = false;
        public Boolean UseMinRawPageRank = false;
        public Single MinRawPageRank = 0;
        public Boolean UseMinCurveFitPageRank = false;
        public UInt32 MinCurveFitPageRank = 0;

        public Boolean PreferClickedUrls = true;

        public Boolean UseServeCustomScore = false;
        public Boolean UseCrawlCustomScore = false;

        public Boolean EnableRankSpreading = true;
        public Boolean EnableDemotionFromRKAllocation = true;
        public Boolean EnablePromotionFromRKAllocation = true;
        public Boolean EnableSitemapInjection = true;
        // via SitemapsV3 pipeline
        public Boolean EnableTrustedSitemapUrlInjection = true;

        // Default Domain Min Quota at top 14B
        public UInt32 DefaultDomainMinQuota = 0;

        public String ClickBoostTier = "Undecided";
        public String SuperFreshTier = "Undecided";
        public UInt32 SuperFreshReserveAge = 120;

        public String SocialUrlsTier = "Undecided";
        public UInt32 SocialUrlsReserveAge = 30;

        public Boolean EnableAllocationRulesFromUrlFilter = false;
        public Boolean EnableRegistrationAllocation = true;
        public UInt32 RegistrationValidAge = 180;

        public Single FexUUMin = 0;
        public Single TbUUMin = 0;
        public Single Ie8UUMin = 0;

        public Boolean EnableHostHomepageBoosting = true;
        public Boolean EnableSerpBoost = true;
        public Boolean SerpBoostEnUsOnly = false;

        public String SafeClickedUrlsReserveTier = "Undecided";
        public UInt32 SafeClickedUrlsReserveAge = 0;
        public Single MinTBClickCountForReservation = 0;
        public Single MinIE8ClickCountForReservation = 0;
        public Single MinFEXClickCountForReservation = 0;

        public String Market = "global";

        public Single MarketVoteRatio = 0;

        public Boolean EnableMarketAllocation = true;

        public Double IndexPolitenessDefaultQpsMean = 4.0;
        public Double IndexPolitenessDefaultQpsStdev = 0.0;
        public Boolean EnableIndexPoliteness = true;
        public Double IndexPolitenessSLAFromDPinDays = 90;
        public Double IndexPolitenessQpsStdevMulFactor = 0.675; // z-value 0.675 for75%ile
        public Double IndexPolitenessQpsScaleFactor = 1.1;

        public String ZHCNClickBoostTier = "Undecided";
        public Boolean EnableZHCNSerpBoost = true;

        public Boolean EnablePolitenessFilter = false;

        public UInt64 IndexSizeBeforePolitenessFilter = 50000000000;
        public Double InjectionPointZ2WithPolitenssFilter = 0.7;

        public Boolean EnableSiteModeling = false;

        public Double InjectionPointForBottomWithPolitenssFilter = 0.66;
        public Double InjectionPointForBottom = 0.9;

        public String ExpressUrlsReserveTier = "Undecided";
        public UInt16 ExpressReserveNumSessions = 0;

        public UInt64 DepthAllocationMinSmallIndexQuota = 0;
        public UInt64 DepthAllocationMinZ1IndexQuota = 10000000000;
        public UInt64 DepthAllocationMinZ2IndexQuota = 20000000000;

        public Boolean EnablePatternBasedDepthAllocation = true;
        public UInt16 MaxRawScoreRange = 30;

        public Boolean AllConfigsMustExist = false;

        public SelectionConfig()
        {

        }
        // quickly lookup quota given QPS stats for a host
        public UInt64 GetIndexPolitenessQuota(Double QpsMean, Double QpsStdev)
        {
            return (UInt64)((QpsMean + IndexPolitenessQpsStdevMulFactor * QpsStdev) * IndexPolitenessQpsScaleFactor *
                    IndexPolitenessSLAFromDPinDays * (86400L));
        }

        // default quota for hosts without qps config  at greater than 30 million  in effect
        public UInt64 GetIndexPolitenessQuota()
        {
            return GetIndexPolitenessQuota(IndexPolitenessDefaultQpsMean, IndexPolitenessDefaultQpsStdev);
        }

        public void Initialize(Dictionary<String, String> parameters)
        {
            string key = "selectionconfig";
            Common.Assert(parameters.ContainsKey(key));

            String selectionConfigFileName = Path.GetFileName(parameters[key]);

            Initialize(selectionConfigFileName);
        }

        public void Initialize(string fn)
        {
            FieldInfo[] fields = typeof(SelectionConfig).GetFields(BindingFlags.Public | BindingFlags.Instance);
            bool[] configFound = new bool[fields.Length];
            for (int i = 0; i < configFound.Length; i++)
            {
                configFound[i] = false;
            }

            Dictionary<string, uint> fieldpos = new Dictionary<string, uint>();

            for (uint i = 0; i < fields.Length; i++)
            {
                fieldpos[fields[i].Name] = i;
            }

            using (TextReader reader = new ConfigStreamReader(fn))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    int pos = line.IndexOf('=');
                    Common.Assert(pos > 0, "Invalid configuration line: \"" + line + "\"");

                    string key = line.Substring(0, pos);
                    string value = line.Substring(pos + 1);

                    if (fieldpos.ContainsKey(key))
                    {
                        Object obj = null;
                        uint keypos = fieldpos[key];

                        Common.Assert(!configFound[keypos], "Duplicate configuration key found in configuration file.");

                        if (fields[keypos].FieldType == typeof(String))
                        {
                            obj = value;
                        }
                        else if (fields[keypos].FieldType == typeof(Double))
                        {
                            obj = Double.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(Single))
                        {
                            obj = Single.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(UInt16))
                        {
                            obj = UInt16.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(Int32))
                        {
                            obj = Int32.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(UInt32))
                        {
                            obj = UInt32.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(UInt64))
                        {
                            obj = UInt64.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(Int64))
                        {
                            obj = Int64.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(Boolean))
                        {
                            obj = Boolean.Parse(value);
                        }
                        else
                        {
                            throw new Exception("Unknown configuration type.");
                        }

                        fields[keypos].SetValue(this, obj);
                        configFound[keypos] = true;
                    }
                }
            }

            if (AllConfigsMustExist)
            {
                for (int i = 0; i < configFound.Length; i++)
                {
                    Common.Assert(configFound[i], "Config key not found in config file: " + fields[i].Name);
                }
            }
        }

        public void WriteToFile(string fn)
        {
            using (TextWriter writer = File.CreateText(fn))
            {
                FieldInfo[] fields = typeof(SelectionConfig).GetFields(BindingFlags.Public | BindingFlags.Instance);

                foreach (FieldInfo field in fields)
                {
                    writer.WriteLine("{0}={1}", field.Name, field.GetValue(this));
                }
            }
        }
    }



    public class CrawlSelectionConfig : IInitializable
    {

        // MinScore should be >= RankHistogramStart
        public Single MinScore = 0.001f;




        // WARNING: Do not enable in production without manager approval.
        public Boolean SelectExclusiveTBIE8MetricsUrls = false;

        // Any URL not from any of the following sensor types will not be selected
        // See SensorTypes and SensorMask
        public String SelectUrlsFromSensors = "Sitemap|Injection|URLFeatures|DocumentFeatures|DeepCrawl|Fex|Registration";
        public String EnableJobSubmissionTypes = "DeepCrawl|ShallowCrawl"; //ForumCrawl|SitemapCrawl|IntelligentDeepCrawl
        public String RulesEnabledForScoring = "ClickedUrl|Ie8Hub|WebmapFrontier|OutlinkHub|TbHub|NewDomainHost|DomainHost|DirectoryIndex|CrawlInjection0|CrawlInjection1|CrawlInjection2|CrawlInjection3";
        public String RulesEnabledForCrawling = "ClickedUrl|Ie8Hub|WebmapFrontier|OutlinkHub|TbHub|NewDomainHost|DomainHost|DirectoryIndex|CrawlInjection0|CrawlInjection1|CrawlInjection2|CrawlInjection3";
        //|RecrawlExpired|SitemapUrl
        // Don't turn this on except for testing
        public Boolean SelectSpamUrls = false;
        public Boolean SelectBlockedExtensionUrls = false;

        public Boolean DropPreviousRankSessions = false;
        public Boolean UseMinRawPageRank = false;
        public Single MinRawPageRank = 0;
        public Boolean UseMinCurveFitPageRank = false;
        public UInt32 MinCurveFitPageRank = 0;
        public Boolean EnableRangeMapLookupForPartitionID = false;
        public UInt32 ProcessTimeLimitInMinutes = 270;



        public UInt32 SuperFreshReserveAge = 120;


        public UInt32 RegistrationValidAge = 180;

        public Single FexUUMin = 0;
        public Single TbUUMin = 0;
        public Single Ie8UUMin = 0;

        public String DefaultCrawlJobConfig = "14,0,100";
        public Boolean DropScoringUrlsCrawledByDP = false;

        //          SEO?page
        public UInt32 OutlinkHub_MinOutlinks = 150;
        public UInt32 OutlinkHub_MinPageRankCF = 128;
        public String OutlinkHub_CrawlJobConfig = "3,0,50";
        public UInt32 OutlinkHub_MinInlinks = 100;


        public UInt32 ClickedUrl_MinInlinkCount = 2;
        public UInt32 ClickedUrl_MinClickScore = 10;
        public Boolean ClickedUrl_IsFrontier = true;
        public UInt32 ClickedUrl_MinPageRankCF = 112;
        public String ClickedUrl_CrawlJobConfig = "7,0,90";

        public Single Ie8Hub_MinScore = 0.01F;
        public String Ie8Hub_CrawlJobConfig = "3,0,50";

        public Single TbHub_MinScore = 0.01F;
        public String TbHub_CrawlJobConfig = "3,0,50";

        public UInt32 WebmapFrontier_MinPageRankCF = 112;
        public UInt32 WebmapFrontier_MinInlinkCount = 5;
        public String WebmapFrontier_CrawlJobConfig = "14,1,100";

        public String SitemapUrl_CrawlJobConfig = "7,0,50";
        public UInt32 SitemapUrl_MinPageRankCF = 112;
        public Boolean SitemapUrl_DropCrawledByDP = false;
        public UInt32 SitemapUrl_MinPageRankCFTrusted = 563;
        public String SitemapFile_CrawlJobConfig = "5,30,50";
        // score based on today - (disc | crawled | failed )
        public UInt16 SitemapFile_CrawlHistoryInDays = 10;

        public String NewDomainHost_CrawlJobConfig = "1,1,5";
        public String AllDomainHost_CrawlJobConfig = "14,1,100";

        public Boolean DirectoryIndex_IsFrontier = true;
        public UInt32 DirectoryIndex_MinPageRankCF = 128;
        public String DirectoryIndex_CrawlJobConfig = "7,1,50";
        public String DirectoryIndex_PageNames = "index,main,home";
        public UInt32 DomainHost_DaysToExcludeCrawlFailure = 30;
        public UInt32 ShallowCrawl_DaysToExcludeCrawlFailure = 4;
        public Boolean DropCrawlingsUrlsCrawledByDP = true;
        public UInt32 ShallowCrawl_MinPriority = 1024;
        public UInt32 ShallowCrawl_MaxPriority = 0;
        public UInt16 ShallowCrawl_FilterType = 1;
        public String ShallowCrawl_JobNamePrefix = "SC.Prod-";
        public UInt64 ShallowCrawl_ListSize_DepthZero = 1000000000;
        public UInt64 ShallowCrawl_ListSize_DepthOne = 50000000;
        public UInt64 ShallowCrawl_ListSize_DepthTwo = 10000000;
        public Single ShallowCrawl_DiscoveryRatio = 5.0F;

        public UInt16 CrawlSchedulerInjection_DefaultFrequency = 3;
        public UInt64 SitemapCrawl_SubmissionSize = 10000000;
        public String SitemapCrawl_JobNamePrefix = "SM.";
        public UInt32 SitemapCrawl_TargetDepth = 31;

        public CrawlSelectionConfig()
        {

        }

        public void ValidateConfig()
        {

        }

        public void Initialize(Dictionary<String, String> parameters)
        {
            string key = "crawlselectionconfig";
            Common.Assert(parameters.ContainsKey(key));

            String crawlSelectionConfigFileName = Path.GetFileName(parameters[key]);

            Initialize(crawlSelectionConfigFileName);
        }

        public void Initialize(string fn)
        {
            FieldInfo[] fields = typeof(CrawlSelectionConfig).GetFields(BindingFlags.Public | BindingFlags.Instance);
            bool[] configFound = new bool[fields.Length];
            for (int i = 0; i < configFound.Length; i++)
            {
                configFound[i] = false;
            }

            Dictionary<string, uint> fieldpos = new Dictionary<string, uint>();

            for (uint i = 0; i < fields.Length; i++)
            {
                fieldpos[fields[i].Name] = i;
            }

            using (TextReader reader = new ConfigStreamReader(fn))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    int pos = line.IndexOf('=');
                    Common.Assert(pos > 0, "Invalid configuration line: \"" + line + "\"");

                    string key = line.Substring(0, pos);
                    string value = line.Substring(pos + 1);

                    if (fieldpos.ContainsKey(key))
                    {
                        Object obj = null;
                        uint keypos = fieldpos[key];

                        Common.Assert(!configFound[keypos], "Duplicate configuration key found in configuration file.");

                        if (fields[keypos].FieldType == typeof(String))
                        {
                            obj = value;
                        }
                        else if (fields[keypos].FieldType == typeof(Double))
                        {
                            obj = Double.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(Single))
                        {
                            obj = Single.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(Int32))
                        {
                            obj = Int32.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(UInt16))
                        {
                            obj = UInt16.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(UInt32))
                        {
                            obj = UInt32.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(UInt64))
                        {
                            obj = UInt64.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(Int64))
                        {
                            obj = Int64.Parse(value);
                        }
                        else if (fields[keypos].FieldType == typeof(Boolean))
                        {
                            obj = Boolean.Parse(value);
                        }
                        else
                        {
                            throw new Exception("Unknown configuration type.");
                        }

                        fields[keypos].SetValue(this, obj);
                        configFound[keypos] = true;
                    }
                }
            }

            for (int i = 0; i < configFound.Length; i++)
            {
                Common.Assert(configFound[i], "Config key not found in config file: " + fields[i].Name);
            }
        }

        public void WriteToFile(string fn)
        {
            using (TextWriter writer = File.CreateText(fn))
            {
                FieldInfo[] fields = typeof(CrawlSelectionConfig).GetFields(BindingFlags.Public | BindingFlags.Instance);

                foreach (FieldInfo field in fields)
                {
                    writer.WriteLine("{0}={1}", field.Name, field.GetValue(this));
                }
            }
        }
    }





    /*
     *  SegmentSelectionConfig
     */

    /*
     *  NOTE: If you want to add new parameter:
     *  
     *      1- To add global parameter, search for the comment "Add Global Parameters here"
     *      2- To add segment group parameter, search for the comment "Add Segment Group Parameters here"
     *      3- To add segment parameter, search for the comment "Add Segment Parameters here" 
     */

    public interface ISegmentSelectionConfig
    {
        void Initialize(Dictionary<String, String> parameters);
        void Initialize(string fn);

        /*
         *  Getters
         */
        int getEnabledSegmentCount(SegmentGroupType segmentGroup);
        List<SegmentGroupType> getEnabledSegmentGroups();
        List<string> GetEnabledSegmentsInSegmentGroup(SegmentGroupType segmentGroup);
        List<int> GetEnabledSegmentsInSegmentGroupInt(SegmentGroupType segmentGroup);
        List<MarketSegType> getEnabledMarkets();
        List<QueryIntentType> getEnabledQueryIntents();
        List<FileFormatType> getEnabledFileFormats();
        List<PageLayoutType> getEnabledPageLayouts();
        List<VisitType> getEnabledVisitTypes();

        bool getGlobalEnabled();
        string getGlobalName();
        Double getGlobalMinCutoff_SegLikelihood();
        Double getGlobalMinCutoff_PostReRank();
        long getGlobalMinQuota();
        long getGlobalMaxQuota();
        Double getGlobalMinScore();
        Double getGlobalRankHistogramStart();
        Double getGlobalRankHistogramStep();
        long getGlobalRankHistogramSize();
        bool getGlobalEnableCurveFitting();
        bool getGlobalEnableRankSpreading();
        long getGlobalTierSizeSmallBigZ1IntZ2();
        bool getGlobalSAInTop20B();

        bool getSegmentGroupEnabled(int segmentGroup);
        string getSegmentGroupName(int segmentGroup);
        Double getSegmentGroupMinCutoff_SegLikelihood(int segmentGroup);
        Double getSegmentGroupMinCutoff_PostReRank(int segmentGroup);
        long getSegmentGroupMinQuota(int segmentGroup);
        long getSegmentGroupMaxQuota(int segmentGroup);
        Double getSegmentGroupMinScore(int segmentGroup);
        Double getSegmentGroupRankHistogramStart(int segmentGroup);
        Double getSegmentGroupRankHistogramStep(int segmentGroup);
        long getSegmentGroupRankHistogramSize(int segmentGroup);
        bool getSegmentGroupEnableCurveFitting(int segmentGroup);
        bool getSegmentGroupEnableRankSpreading(int segmentGroup);
        long getSegmentGroupTierSizeSmallBigZ1IntZ2(int segmentGroup);
        bool getSegmentGroupSAInTop20B(int segmentGroup);

        bool getSegmentEnabled(int segmentGroup, int segment);
        string getSegmentName(int segmentGroup, int segment);
        Double getSegmentMinCutoff_SegLikelihood(int segmentGroup, int segment);
        Double getSegmentMinCutoff_PostReRank(int segmentGroup, int segment);
        long getSegmentMinQuota(int segmentGroup, int segment);
        long getSegmentMaxQuota(int segmentGroup, int segment);
        Double getSegmentMinScore(int segmentGroup, int segment);
        Double getSegmentRankHistogramStart(int segmentGroup, int segment);
        Double getSegmentRankHistogramStep(int segmentGroup, int segment);
        long getSegmentRankHistogramSize(int segmentGroup, int segment);
        bool getSegmentEnableCurveFitting(int segmentGroup, int segment);
        bool getSegmentEnableRankSpreading(int segmentGroup, int segment);
        long getSegmentTierSizeSmallBigZ1IntZ2(int segmentGroup, int segment);
        bool getSegmentSAInTop20B(int segmentGroup, int segment);

        /*
         *  Setters
         */
        void setGlobalParam(string paramName, bool paramValue);
        void setGlobalParam(string paramName, int paramValue);
        void setGlobalParam(string paramName, long paramValue);
        void setGlobalParam(string paramName, double paramValue);
        void setGlobalParam(string paramName, string paramValue);

        void setSegmentGroupParam(int segmentGroup, string paramName, bool paramValue);
        void setSegmentGroupParam(int segmentGroup, string paramName, int paramValue);
        void setSegmentGroupParam(int segmentGroup, string paramName, long paramValue);
        void setSegmentGroupParam(int segmentGroup, string paramName, double paramValue);
        void setSegmentGroupParam(int segmentGroup, string paramName, string paramValue);

        void setSegmentParam(int segmentGroup, int segment, string paramName, bool paramValue);
        void setSegmentParam(int segmentGroup, int segment, string paramName, int paramValue);
        void setSegmentParam(int segmentGroup, int segment, string paramName, long paramValue);
        void setSegmentParam(int segmentGroup, int segment, string paramName, double paramValue);
        void setSegmentParam(int segmentGroup, int segment, string paramName, string paramValue);
    }

    public class SegmentSelectionConfigConstants
    {
        public const double INVALID_PARAM_DOUBLE = -1;
        public const int INVALID_PARAM_INT = -1;
        public const string INVALID_PARAM_STRING = "";
    }

    public class ConfigEntry : System.Object
    {
        public int segmentGroup = -1;
        public int segment = -1;
        public string paramName = "";
        public string paramValue = "";

        public ConfigEntry() { }

        public ConfigEntry(int segmentGroup,
                           int segment, 
                           string paramName, 
                           string paramValue)
        {
            this.segmentGroup = segmentGroup;
            this.segment = segment;
            this.paramName = paramName;
            this.paramValue = paramValue;
        }
        
        public override bool Equals(System.Object obj)
        {
            ConfigEntry c = obj as ConfigEntry;

            return (c.segmentGroup == this.segmentGroup) &&
                   (c.segment == this.segment) &&
                   (c.paramName == this.paramName);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }

    public class SegmentParameters
    {
        private Dictionary<string, string> parametersMap = new Dictionary<string, string>();

        private FieldInfo[] fields = null;
        private Dictionary<string, uint> fieldpos = null;

        /*
         * Add Segment Parameters here as public members..
         */  
        public bool enabled = false;
        public string name = "";
        public Double MinCutoff_SegLikelihood=0.5;
        // x*y plot of raw score range * prob (0,1)
        //http://www.wolframalpha.com/input/?i=plot%28+x*y+%29%2C+x%3D[0%2C1]%2Cy%3D[4%2C30]
        public Double MinCutoff_PostReRank=6; //7.6*80%

        public long MinQuota = 0;
        public long MaxQuota = 0;
        public Double MinScore=0.001;   //MinScore should be >= RankHistogramStart
        public Double RankHistogramStart=0.001;
        public Double RankHistogramStep=1.001;
        //Histogram max score = RankHistogramStart * RankHistogramStep^RankHistogramSize
        //0.001 * 1.001^12000 = ~162, so 12000 bins should be enough
        //Maximum score set to ~40 in ScorePostTransform below
        public long RankHistogramSize=12000;
        public bool EnableCurveFitting=true;
        public bool EnableRankSpreading=true;
        public long TierSizeSmallBigZ1IntZ2=10000000000;
        public bool SAInTop20B = false;

        public SegmentParameters()
        {
            fields = typeof(SegmentParameters).GetFields(BindingFlags.Public | BindingFlags.Instance);
            fieldpos = new Dictionary<string, uint>();

            for (uint i = 0; i < fields.Length; i++)
            {
                fieldpos[fields[i].Name] = i;
            }
        }

        public void AddParameter(ConfigEntry configEntry)
        {
            if (parametersMap.ContainsKey(configEntry.paramName))
            {
                parametersMap.Remove(configEntry.paramName);
            }

            parametersMap[configEntry.paramName] = configEntry.paramValue;
        }

        public void parseParameters()
        {
            foreach (KeyValuePair<string, string> pair in parametersMap)
            {
                if (fieldpos.ContainsKey(pair.Key))
                {
                    Object obj = null;
                    uint keypos = fieldpos[pair.Key];

                    if (fields[keypos].FieldType == typeof(String))
                    {
                        obj = pair.Value;
                    }
                    else if (fields[keypos].FieldType == typeof(Double))
                    {
                        obj = Double.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(Single))
                    {
                        obj = Single.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(Int32))
                    {
                        obj = Int32.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(UInt16))
                    {
                        obj = UInt16.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(UInt32))
                    {
                        obj = UInt32.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(UInt64))
                    {
                        obj = UInt64.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(Int64))
                    {
                        obj = Int64.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(Boolean))
                    {
                        obj = Boolean.Parse(pair.Value);
                    }
                    else
                    {
                        throw new Exception("Unknown configuration type.");
                    }

                    fields[keypos].SetValue(this, obj);
                }
            }
        }


        public bool getEnabled()
        {
            return enabled;
        }
        public string getName()
        {
            return name;
        }
        public Double getMinCutoff_SegLikelihood()
        {
            return MinCutoff_SegLikelihood;
        }
        public Double getMinCutoff_PostReRank()
        {
            return MinCutoff_PostReRank;
        }
        public long getMinQuota()
        {
            return MinQuota;
        }
        public long getMaxQuota()
        {
            return MaxQuota;
        }
        public Double getMinScore()
        {
            return MinScore;
        }
        public Double getRankHistogramStart()
        {
            return RankHistogramStart;
        }
        public Double getRankHistogramStep()
        {
            return RankHistogramStep;
        }
        public long getRankHistogramSize()
        {
            return RankHistogramSize;
        }
        public bool getEnableCurveFitting()
        {
            return EnableCurveFitting;
        }
        public bool getEnableRankSpreading()
        {
            return EnableRankSpreading;
        }
        public long getTierSizeSmallBigZ1IntZ2()
        {
            return TierSizeSmallBigZ1IntZ2;
        }
        public bool getSAInTop20B()
        {
            return SAInTop20B;
        }
    }

    public class SegmentGroup
    {
        private SegmentGroupType segmentGroupType;
        private Dictionary<string, string> segmentGroupParametersMap = new Dictionary<string, string>();
        private List<SegmentParameters> segmentsList = new List<SegmentParameters>();

        private FieldInfo[] fields;
        private Dictionary<string, uint> fieldpos;

        /*
         * Add Segment Group Parameters here as public members.
         * Note: Naming convention is: Group{ParameterName}
         */
        // These are Group specific params that are not propagated
        public bool GroupEnabled = false;

        // These are node specific params eligble for propagation down stream
        public bool enabled = false;
        public string name = "";
        public Double MinCutoff_SegLikelihood = 0.5;
        public Double MinCutoff_PostReRank = 6; //7.6*80%
        public long MinQuota = 0;
        public long MaxQuota = 0;
        public Double MinScore = 0.001;   //MinScore should be >= RankHistogramStart
        public Double RankHistogramStart = 0.001;
        public Double RankHistogramStep = 1.001;
        public long RankHistogramSize = 12000;
        public bool EnableCurveFitting = true;
        public bool EnableRankSpreading = true;
        public long TierSizeSmallBigZ1IntZ2 = 10000000000;
        public bool SAInTop20B = false;


        public SegmentGroup(SegmentGroupType segmentGroupType)
        {
            this.segmentGroupType = segmentGroupType;

            int segmentsListSize;

            switch (segmentGroupType)
            { 
                case SegmentGroupType.MarketSegType:
                    segmentsListSize = (int)MarketSegType.NumTypes;
                    break;
                case SegmentGroupType.QueryIntentType:
                    segmentsListSize = (int)QueryIntentType.NumTypes;
                    break;
                case SegmentGroupType.FileFormatType:
                    segmentsListSize = (int)FileFormatType.NumTypes;
                    break;
                case SegmentGroupType.PageLayoutType:
                    segmentsListSize = (int)PageLayoutType.NumTypes;
                    break;
                case SegmentGroupType.VisitType:
                    segmentsListSize = (int)VisitType.NumTypes;
                    break;
                default:
                    segmentsListSize = -1;
                    break;
            }

            for (int i = 0; i < segmentsListSize; ++i)
            {
                segmentsList.Add(new SegmentParameters());
            }
        }

        public void AddParameter(ConfigEntry configEntry)
        {
            if (configEntry.segmentGroup == (int)this.segmentGroupType ||
                configEntry.segmentGroup == -1 )
            {
                if (segmentGroupParametersMap.ContainsKey(configEntry.paramName))
                {
                    segmentGroupParametersMap.Remove(configEntry.paramName);
                }

                segmentGroupParametersMap[configEntry.paramName] = configEntry.paramValue;

                AddParameterToSegment(configEntry);
            }
        }


        public void AddParameterToSegment(ConfigEntry configEntry)
        {
            // Add the parameter to all segments
            if (configEntry.segment == -1)
            {
                for (int i = 0; i < segmentsList.Count; ++i)
                {
                    segmentsList[i].AddParameter(configEntry);
                }
            }

            // Add the parameter only to the specified market
            else
            {
                segmentsList[configEntry.segment].AddParameter(configEntry);
            }
        }
        
        public void parseParameters()
        {
            fields = typeof(SegmentGroup).GetFields(BindingFlags.Public | BindingFlags.Instance);
            fieldpos = new Dictionary<string, uint>();

            for (uint i = 0; i < fields.Length; i++)
            {
                fieldpos[fields[i].Name] = i;
            }

            foreach (KeyValuePair<string, string> pair in segmentGroupParametersMap)
            {
                if (fieldpos.ContainsKey(pair.Key))
                {
                    Object obj = null;
                    uint keypos = fieldpos[pair.Key];

                    if (fields[keypos].FieldType == typeof(String))
                    {
                        obj = pair.Value;
                    }
                    else if (fields[keypos].FieldType == typeof(Double))
                    {
                        obj = Double.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(Single))
                    {
                        obj = Single.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(Int32))
                    {
                        obj = Int32.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(UInt16))
                    {
                        obj = UInt16.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(UInt32))
                    {
                        obj = UInt32.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(UInt64))
                    {
                        obj = UInt64.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(Int64))
                    {
                        obj = Int64.Parse(pair.Value);
                    }
                    else if (fields[keypos].FieldType == typeof(Boolean))
                    {
                        obj = Boolean.Parse(pair.Value);
                    }
                    else
                    {
                        throw new Exception("Unknown configuration type.");
                    }

                    fields[keypos].SetValue(this, obj);
                }
            }

            for (int i = 0; i < segmentsList.Count; ++i)
            {
                segmentsList[i].parseParameters();
            }
        }


        public bool getGroupEnabled()
        {
            return GroupEnabled;
        }
        
        public bool getEnabled()
        {
            return enabled;
        }
        public string getName()
        {
            return name;
        }
        public Double getMinCutoff_SegLikelihood()
        {
            return MinCutoff_SegLikelihood;
        }
        public Double getMinCutoff_PostReRank()
        {
            return MinCutoff_PostReRank;
        }
        public long getMinQuota()
        {
            return MinQuota;
        }
        public long getMaxQuota()
        {
            return MaxQuota;
        }
        public Double getMinScore()
        {
            return MinScore;
        }
        public Double getRankHistogramStart()
        {
            return RankHistogramStart;
        }
        public Double getRankHistogramStep()
        {
            return RankHistogramStep;
        }
        public long getRankHistogramSize()
        {
            return RankHistogramSize;
        }
        public bool getEnableCurveFitting()
        {
            return EnableCurveFitting;
        }
        public bool getEnableRankSpreading()
        {
            return EnableRankSpreading;
        }
        public long getTierSizeSmallBigZ1IntZ2()
        {
            return TierSizeSmallBigZ1IntZ2;
        }
        public bool getSAInTop20B()
        {
            return SAInTop20B;
        }


        public bool getSegmentEnabled(int segment)
        {
            return segmentsList[segment].getEnabled();
        }
        public string getSegmentName(int segment)
        {
            return segmentsList[segment].getName();
        }
        public Double getSegmentMinCutoff_SegLikelihood(int segment)
        {
            return segmentsList[segment].getMinCutoff_SegLikelihood();
        }
        public Double getSegmentMinCutoff_PostReRank(int segment)
        {
            return segmentsList[segment].getMinCutoff_PostReRank();
        }
        public long getSegmentMinQuota(int segment)
        {
            return segmentsList[segment].getMinQuota();
        }
        public long getSegmentMaxQuota(int segment)
        {
            return segmentsList[segment].getMaxQuota();
        }
        public Double getSegmentMinScore(int segment)
        {
            return segmentsList[segment].getMinScore();
        }
        public Double getSegmentRankHistogramStart(int segment)
        {
            return segmentsList[segment].getRankHistogramStart();
        }
        public Double getSegmentRankHistogramStep(int segment)
        {
            return segmentsList[segment].getRankHistogramStep();
        }
        public long getSegmentRankHistogramSize(int segment)
        {
            return segmentsList[segment].getRankHistogramSize();
        }
        public bool getSegmentEnableCurveFitting(int segment)
        {
            return segmentsList[segment].getEnableCurveFitting();
        }
        public bool getSegmentEnableRankSpreading(int segment)
        {
            return segmentsList[segment].getEnableRankSpreading();
        }
        public long getSegmentTierSizeSmallBigZ1IntZ2(int segment)
        {
            return segmentsList[segment].getTierSizeSmallBigZ1IntZ2();
        }
        public bool getSegmentSAInTop20B(int segment)
        {
            return segmentsList[segment].getSAInTop20B();
        }
    }

    public class SegmentSelectionConfig : IInitializable, ISegmentSelectionConfig
    {
        // Parameters
        private List<ConfigEntry> globalParametersMap = new List<ConfigEntry>();
        private List<ConfigEntry> segmentGroupParametersMap = new List<ConfigEntry>();
        private List<ConfigEntry> segmentParametersMap = new List<ConfigEntry>();

        // Segment Group Objects
        Dictionary<int, SegmentGroup> segmentGroupMap = new Dictionary<int, SegmentGroup>();

        private FieldInfo[] fields = null;
        Dictionary<string, uint> fieldpos = null;

        /*
         * Add Global Parameters here as public members..
         * Note: Naming convention is: Global{ParameterName}
         */

         
        // These are node specific params eligble for propagation down stream
        public bool enabled = false;
        public string name = "";
        public Double MinCutoff_SegLikelihood = 0.5;
        public Double MinCutoff_PostReRank = 6; //7.6*80%
        public long MinQuota = 0;
        public long MaxQuota = 0;
        public Double MinScore = 0.001;   //MinScore should be >= RankHistogramStart
        public Double RankHistogramStart = 0.001;
        public Double RankHistogramStep = 1.001;
        public long RankHistogramSize = 12000;
        public bool EnableCurveFitting = true;
        public bool EnableRankSpreading = true;
        public long TierSizeSmallBigZ1IntZ2 = 10000000000;
        public bool SAInTop20B = false;


        
        public SegmentType SegEnabledMask = new SegmentType();
        public SegmentScores SegEnabled = new SegmentScores();
        public SegmentScores SegLikelihoodCutoff = new SegmentScores();
        public SegmentScores SegPostReRankCutoff = new SegmentScores();

        private string SEGMENT_TYPE_SEPARATOR = "::";

        public SegmentSelectionConfig()
        {
        }

        public void Initialize(Dictionary<String, String> parameters)
        {
            string key = "segmentselectionconfig";
            Common.Assert(parameters.ContainsKey(key));

            String segmentSelectionConfigFileName = Path.GetFileName(parameters[key]);

            Initialize(segmentSelectionConfigFileName);
        }

        public void Initialize(string fn)
        {
            segmentGroupMap.Add((int)SegmentGroupType.MarketSegType, new SegmentGroup(SegmentGroupType.MarketSegType));
            segmentGroupMap.Add((int)SegmentGroupType.QueryIntentType, new SegmentGroup(SegmentGroupType.QueryIntentType));
            segmentGroupMap.Add((int)SegmentGroupType.FileFormatType, new SegmentGroup(SegmentGroupType.FileFormatType));
            segmentGroupMap.Add((int)SegmentGroupType.PageLayoutType, new SegmentGroup(SegmentGroupType.PageLayoutType));
            segmentGroupMap.Add((int)SegmentGroupType.VisitType, new SegmentGroup(SegmentGroupType.VisitType));

            using (TextReader reader = new HierarchicConfigStreamReader(fn))
            {
                string line;
                string currentSegmentGroup = "";
                string currentSegment = "";

                while ((line = reader.ReadLine()) != null)
                {
                    ConfigEntry entry = new ConfigEntry();

                    if (line.StartsWith("[") && line.EndsWith("]"))
                    {
                        // Global parameters
                        if (line.Equals("[Main]"))
                        {
                            continue;
                        }

                        // Segment Group section, e.g. [Market] 
                        else if (!line.Contains(SEGMENT_TYPE_SEPARATOR))
                        {
                            currentSegmentGroup = line.Substring(1, (line.Length - 2));
                            currentSegment = "";
                        }

                        // Segment subsection, e.g. [Market*en_us]
                        else
                        {
                            int SegmentSeparatorCharIndex = line.IndexOf(SEGMENT_TYPE_SEPARATOR);

                            currentSegmentGroup = line.Substring(1, (SegmentSeparatorCharIndex - 1));
                            currentSegment = line.Substring((SegmentSeparatorCharIndex + 2), (line.Length - SegmentSeparatorCharIndex - 3));
                        }
                    }

                    // Line format is: paramName=value     
                    else
                    {
                        int pos = line.IndexOf('=');
                        Common.Assert(pos > 0, "Invalid configuration line: \"" + line + "\"");

                        string paramName = line.Substring(0, pos);
                        string paramValue = line.Substring(pos + 1);

                        if (!(currentSegmentGroup == ""))
                        {
                            entry.segmentGroup = (int)Enum.Parse(typeof(SegmentGroupType), currentSegmentGroup, true);   
                        }

                        if (!(currentSegment == ""))
                        {
                            entry.segment = getIndexForSegment((SegmentGroupType)entry.segmentGroup, currentSegment);
                        }
                        
                        entry.paramName = paramName;
                        entry.paramValue = paramValue;

                        if (entry.segmentGroup == -1 && entry.segment == -1)
                        {
                            if (globalParametersMap.Contains(entry))
                            {
                                globalParametersMap.Remove(entry);
                            }

                            globalParametersMap.Add(entry);
                        }

                        else if (entry.segmentGroup != -1 && entry.segment == -1)
                        {
                            if (segmentGroupParametersMap.Contains(entry))
                            {
                                segmentGroupParametersMap.Remove(entry);
                            }

                            segmentGroupParametersMap.Add(entry);
                        }

                        else if (entry.segmentGroup != -1 && entry.segment != -1)
                        {
                            if (segmentParametersMap.Contains(entry))
                            {
                                segmentParametersMap.Remove(entry);
                            }

                            segmentParametersMap.Add(entry);
                        }
                    }
                }
            }

            /*
             *  At this point, all Parameters are read.
             *  Now, SegmentParameter objects will inherit the parameters from those lists obeying the rule:
             *  
             *  if segmentParameter exists --> use segment parameter
             *  else if segmentGroupParameter exists --> use segmentGroupParameter
             *  else if globalParameter exists --> use globalParameter
             *  else --> use defaultValue
             */

            propogateParameters();
            parseParameters();
            SegEnabledMask = createEnabledMask();
            SegEnabled.Reset();
            SegEnabled.Aggregate(SegEnabledMask);
            initMinCutoffFilters();
        }

        /// <summary>
        /// Create cutoffs as SegmentScores objects so they can by applied by Segments UDT
        /// </summary>
        public void  initMinCutoffFilters( )
        {
            SegLikelihoodCutoff.Reset();
            SegPostReRankCutoff.Reset();

            if (getSegmentGroupEnabled((int)SegmentGroupType.MarketSegType))
            {
                for (int i = 0; i < (int) MarketSegType.NumTypes; ++i)
                {
                    SegLikelihoodCutoff.markets[i] = (getSegmentMinCutoff_SegLikelihood( (int) SegmentGroupType.MarketSegType, i));
                    SegPostReRankCutoff.markets[i] = (getSegmentMinCutoff_PostReRank( (int) SegmentGroupType.MarketSegType, i));
                }
            }

            if (getSegmentGroupEnabled((int)SegmentGroupType.QueryIntentType))
            {
                for (int i = 0; i < (int)QueryIntentType.NumTypes; ++i)
                {
                    SegLikelihoodCutoff.querySegs[i] = (getSegmentMinCutoff_SegLikelihood( (int) SegmentGroupType.QueryIntentType, i));
                    SegPostReRankCutoff.querySegs[i] = (getSegmentMinCutoff_PostReRank( (int) SegmentGroupType.QueryIntentType, i));
                }
            }

            if (getSegmentGroupEnabled((int)SegmentGroupType.FileFormatType))
            {
                for (int i = 0; i < (int)FileFormatType.NumTypes; ++i)
                {
                    SegLikelihoodCutoff.fileFormats[i] = (getSegmentMinCutoff_SegLikelihood( (int) SegmentGroupType.FileFormatType, i));
                    SegPostReRankCutoff.fileFormats[i] = (getSegmentMinCutoff_PostReRank( (int) SegmentGroupType.FileFormatType, i));
               }
            }

            if (getSegmentGroupEnabled((int)SegmentGroupType.PageLayoutType))
            {
                for (int i = 0; i < (int)PageLayoutType.NumTypes; ++i)
                {
                    SegLikelihoodCutoff.pageLayouts[i] = (getSegmentMinCutoff_SegLikelihood( (int) SegmentGroupType.PageLayoutType, i));
                    SegPostReRankCutoff.pageLayouts[i] = (getSegmentMinCutoff_PostReRank( (int) SegmentGroupType.PageLayoutType, i));

                }
            }

            if (getSegmentGroupEnabled((int)SegmentGroupType.VisitType))
            {
                for (int i = 0; i < (int)VisitType.NumTypes; ++i)
                {
                    SegLikelihoodCutoff.referrers[i] = (getSegmentMinCutoff_SegLikelihood( (int) SegmentGroupType.VisitType, i));
                    SegPostReRankCutoff.referrers[i] = (getSegmentMinCutoff_PostReRank( (int) SegmentGroupType.VisitType, i));
                }
            }

        }

        /// <summary>
        /// Create mask  as SegmentType object so it can by applied by Segments UDT 
        /// </summary>
        public SegmentType createEnabledMask( )
        {
            SegmentType segEnabledMask = new SegmentType();

            if (getSegmentGroupEnabled((int)SegmentGroupType.MarketSegType))
            {

                for (int i = 0; i < (int) MarketSegType.NumTypes; ++i)
                {
                    segEnabledMask.markets[i] = (getSegmentEnabled((int) SegmentGroupType.MarketSegType, i));
                }
            }

            if (getSegmentGroupEnabled((int)SegmentGroupType.QueryIntentType))
            {
                for (int i = 0; i < (int)QueryIntentType.NumTypes; ++i)
                {
                    segEnabledMask.querySegs[i] = (getSegmentEnabled((int)SegmentGroupType.QueryIntentType, i));
                }
            }

            if (getSegmentGroupEnabled((int)SegmentGroupType.FileFormatType))
            {
                for (int i = 0; i < (int)FileFormatType.NumTypes; ++i)
                {
                    segEnabledMask.fileFormats[i] = (getSegmentEnabled((int)SegmentGroupType.FileFormatType, i));
                }
            }

            if (getSegmentGroupEnabled((int)SegmentGroupType.PageLayoutType))
            {
                for (int i = 0; i < (int)PageLayoutType.NumTypes; ++i)
                {
                    segEnabledMask.pageLayouts[i] = (getSegmentEnabled((int)SegmentGroupType.PageLayoutType, i));
                }
            }

            if (getSegmentGroupEnabled((int)SegmentGroupType.VisitType))
            {
                for (int i = 0; i < (int)VisitType.NumTypes; ++i)
                {
                    if (getSegmentEnabled((int)SegmentGroupType.VisitType, i))
                    {
                        segEnabledMask.SetSourceType(((VisitType)i).ToString());
                    }
                }
            }

            return segEnabledMask;
        }
        
        void propogateParameters()
        {
            // First, propogate global parameters
            foreach (ConfigEntry entry in globalParametersMap)
            {
                foreach (KeyValuePair<int, SegmentGroup> segmentGroupPair in segmentGroupMap)
                {
                    segmentGroupPair.Value.AddParameter(entry);
                }
            }

            // Second, propogate segment group parameters
            foreach (ConfigEntry entry in segmentGroupParametersMap)
            {
                foreach (KeyValuePair<int, SegmentGroup> segmentGroupPair in segmentGroupMap)
                {
                    segmentGroupPair.Value.AddParameter(entry);
                }
            }


            // Third, propogate segment parameters
            foreach (ConfigEntry entry in segmentParametersMap)
            {
                foreach (KeyValuePair<int, SegmentGroup> segmentGroupPair in segmentGroupMap)
                {
                    segmentGroupPair.Value.AddParameter(entry);
                }
            }
        }


        private void parseParameters()
        {
            fields = typeof(SegmentSelectionConfig).GetFields(BindingFlags.Public | BindingFlags.Instance);

            fieldpos = new Dictionary<string, uint>();

            for (uint i = 0; i < fields.Length; i++)
            {
                fieldpos[fields[i].Name] = i;
            }

            foreach (ConfigEntry entry in globalParametersMap)
            {
                if (fieldpos.ContainsKey(entry.paramName))
                {
                    Object obj = null;
                    uint keypos = fieldpos[entry.paramName];

                    if (fields[keypos].FieldType == typeof(String))
                    {
                        obj = entry.paramValue;
                    }
                    else if (fields[keypos].FieldType == typeof(Double))
                    {
                        obj = Double.Parse(entry.paramValue);
                    }
                    else if (fields[keypos].FieldType == typeof(Single))
                    {
                        obj = Single.Parse(entry.paramValue);
                    }
                    else if (fields[keypos].FieldType == typeof(Int32))
                    {
                        obj = Int32.Parse(entry.paramValue);
                    }
                    else if (fields[keypos].FieldType == typeof(UInt16))
                    {
                        obj = UInt16.Parse(entry.paramValue);
                    }
                    else if (fields[keypos].FieldType == typeof(UInt32))
                    {
                        obj = UInt32.Parse(entry.paramValue);
                    }
                    else if (fields[keypos].FieldType == typeof(UInt64))
                    {
                        obj = UInt64.Parse(entry.paramValue);
                    }
                    else if (fields[keypos].FieldType == typeof(Int64))
                    {
                        obj = Int64.Parse(entry.paramValue);
                    }
                    else if (fields[keypos].FieldType == typeof(Boolean))
                    {
                        obj = Boolean.Parse(entry.paramValue);
                    }
                    else
                    {
                        throw new Exception("Unknown configuration type.");
                    }

                    fields[keypos].SetValue(this, obj);
                }
            }

            foreach (KeyValuePair<int, SegmentGroup> segmentGroup in segmentGroupMap)
            {
                segmentGroup.Value.parseParameters();
            }
        }

        private int getIndexForSegment(SegmentGroupType segmentGroup, string segment)
        {
            int index = -1;

            switch (segmentGroup)
            {
                case SegmentGroupType.MarketSegType:
                    MarketSegType marketSegType;
                    try
                    {
                        marketSegType = (MarketSegType)Enum.Parse(typeof(MarketSegType), segment, true);
                    }
                    catch (Exception e)
                    {
                        throw new Exception(String.Format("Parse error: {0}, Cannot parse {1} into {2}", e.ToString(), segment, SegmentGroupType.MarketSegType.ToString()));
                    }

                    index = (int)marketSegType;

                    break;

                case SegmentGroupType.QueryIntentType:
                    QueryIntentType queryIntentType;
                    try
                    {
                        queryIntentType = (QueryIntentType)Enum.Parse(typeof(QueryIntentType), segment, true);
                    }
                    catch (Exception e)
                    {
                        throw new Exception(String.Format("Parse error: {0}, Cannot parse {1} into {2}", e.ToString(), segment, SegmentGroupType.QueryIntentType.ToString()));
                    }

                    index = (int)queryIntentType;
                    break;

                case SegmentGroupType.FileFormatType:
                    FileFormatType fileFormatType;
                    try
                    {
                        fileFormatType = (FileFormatType)Enum.Parse(typeof(FileFormatType), segment, true);
                    }
                    catch (Exception e)
                    {
                        throw new Exception(String.Format("Parse error: {0}, Cannot parse {1} into {2}", e.ToString(), segment, SegmentGroupType.FileFormatType.ToString()));
                    }

                    index = (int)fileFormatType;
                    break;

                case SegmentGroupType.PageLayoutType:
                    PageLayoutType pageLayoutType;
                    try
                    {
                        pageLayoutType = (PageLayoutType)Enum.Parse(typeof(PageLayoutType), segment, true);
                    }
                    catch (Exception e)
                    {
                        throw new Exception(String.Format("Parse error: {0}, Cannot parse {1} into {2}", e.ToString(), segment, SegmentGroupType.PageLayoutType.ToString()));
                    }

                    index = (int)pageLayoutType;
                    break;

                case SegmentGroupType.VisitType:
                    VisitType visitType;
                    try
                    {
                        visitType = (VisitType)Enum.Parse(typeof(VisitType), segment, true);
                    }
                    catch (Exception e)
                    {
                        throw new Exception(String.Format("Parse error: {0}, Cannot parse {1} into {2}", e.ToString(), segment, SegmentGroupType.VisitType.ToString()));
                    }

                    index = (int)visitType;
                    break;

                default:
                    break;
            }

            return index;
        }

        
        // Implementations of the interface methods
         

        /*
         *  Getters
         */ 
        public int getEnabledSegmentCount(SegmentGroupType segmentGroup)
        {
            int count = -1;

            switch (segmentGroup)
            { 
                case SegmentGroupType.MarketSegType:
                    count = getEnabledMarkets().Count;
                    break;

                case SegmentGroupType.QueryIntentType:
                    count = getEnabledQueryIntents().Count;
                    break;

                case SegmentGroupType.FileFormatType:
                    count = getEnabledFileFormats().Count;
                    break;

                case SegmentGroupType.PageLayoutType:
                    count = getEnabledPageLayouts().Count;
                    break;

                case SegmentGroupType.VisitType:
                    count = getEnabledVisitTypes().Count;
                    break;

                default:
                    break;
            }

            return count;
        }

        public List<SegmentGroupType> getEnabledSegmentGroups() 
        {
            List<SegmentGroupType> enabledSegmentGroups = new List<SegmentGroupType>();

            for (int i = 0; i < (int)SegmentGroupType.NumTypes; ++i)
            {
                if (getEnabledSegmentCount((SegmentGroupType)i) > 0)
                { 
                    enabledSegmentGroups.Add((SegmentGroupType)i);
                }
            }

            return enabledSegmentGroups;
        }

        public List<string> GetEnabledSegmentsInSegmentGroup(SegmentGroupType segmentGroup)
        {
            List<string> enabledSegments = new List<string>();

            switch (segmentGroup)
            { 
                case SegmentGroupType.MarketSegType:
                    List<MarketSegType> enabledMarkets = getEnabledMarkets();

                    foreach (MarketSegType market in enabledMarkets)
                    {
                        enabledSegments.Add(market.ToString());
                    }
                    break;

                case SegmentGroupType.QueryIntentType:
                    List<QueryIntentType> enabledQueryIntents = getEnabledQueryIntents();

                    foreach (QueryIntentType qIntent in enabledQueryIntents)
                    {
                        enabledSegments.Add(qIntent.ToString());
                    }
                    break;

                case SegmentGroupType.FileFormatType:
                    List<FileFormatType> enabledFileFormats = getEnabledFileFormats();

                    foreach (FileFormatType fFormat in enabledFileFormats)
                    {
                        enabledSegments.Add(fFormat.ToString());
                    }
                    break;

                case SegmentGroupType.PageLayoutType:
                    List<PageLayoutType> enabledPageLayouts = getEnabledPageLayouts();

                    foreach (PageLayoutType pageLayout in enabledPageLayouts)
                    {
                        enabledSegments.Add(pageLayout.ToString());
                    }
                    break;

                case SegmentGroupType.VisitType:
                    List<VisitType> enabledVisitTypes = getEnabledVisitTypes();

                    foreach (VisitType vType in enabledVisitTypes)
                    {
                        enabledSegments.Add(vType.ToString());
                    }
                    break;

                default:
                    enabledSegments = null;
                    break;
            }

            return enabledSegments;
        }

        public List<int> GetEnabledSegmentsInSegmentGroupInt(SegmentGroupType segmentGroup)
        {
            List<int> enabledSegments = new List<int>();

            switch (segmentGroup)
            {
                case SegmentGroupType.MarketSegType:
                    List<MarketSegType> enabledMarkets = getEnabledMarkets();

                    foreach (MarketSegType market in enabledMarkets)
                    {
                        enabledSegments.Add((int)market);
                    }
                    break;

                case SegmentGroupType.QueryIntentType:
                    List<QueryIntentType> enabledQueryIntents = getEnabledQueryIntents();

                    foreach (QueryIntentType qIntent in enabledQueryIntents)
                    {
                        enabledSegments.Add((int)qIntent);
                    }
                    break;

                case SegmentGroupType.FileFormatType:
                    List<FileFormatType> enabledFileFormats = getEnabledFileFormats();

                    foreach (FileFormatType fFormat in enabledFileFormats)
                    {
                        enabledSegments.Add((int)fFormat);
                    }
                    break;

                case SegmentGroupType.PageLayoutType:
                    List<PageLayoutType> enabledPageLayouts = getEnabledPageLayouts();

                    foreach (PageLayoutType pageLayout in enabledPageLayouts)
                    {
                        enabledSegments.Add((int)pageLayout);
                    }
                    break;

                case SegmentGroupType.VisitType:
                    List<VisitType> enabledVisitTypes = getEnabledVisitTypes();

                    foreach (VisitType vType in enabledVisitTypes)
                    {
                        enabledSegments.Add((int)vType);
                    }
                    break;

                default:
                    enabledSegments = null;
                    break;
            }

            return enabledSegments;
        }

        public List<MarketSegType> getEnabledMarkets()
        {
            List<MarketSegType> enabledMarkets = new List<MarketSegType>();

            for(int i=0; i<(int)MarketSegType.NumTypes; ++i)
            {
                if (segmentGroupMap[(int)SegmentGroupType.MarketSegType].getSegmentEnabled(i))
                {
                    enabledMarkets.Add((MarketSegType)i);
                }
            }

            return enabledMarkets;
        }

        public List<QueryIntentType> getEnabledQueryIntents()
        {
            List<QueryIntentType> enabledQueryIntents = new List<QueryIntentType>();

            for (int i = 0; i < (int)QueryIntentType.NumTypes; ++i)
            {
                if (segmentGroupMap[(int)SegmentGroupType.QueryIntentType].getSegmentEnabled(i))
                {
                    enabledQueryIntents.Add((QueryIntentType)i);
                }
            }

            return enabledQueryIntents;
        }

        public List<FileFormatType> getEnabledFileFormats()
        {
            List<FileFormatType> enabledFileFormats = new List<FileFormatType>();

            for (int i = 0; i < (int)FileFormatType.NumTypes; ++i)
            {
                if (segmentGroupMap[(int)SegmentGroupType.FileFormatType].getSegmentEnabled(i))
                {
                    enabledFileFormats.Add((FileFormatType)i);
                }
            }

            return enabledFileFormats;
        }

        public List<PageLayoutType> getEnabledPageLayouts()
        {
            List<PageLayoutType> enabledPageLayouts = new List<PageLayoutType>();

            for (int i = 0; i < (int)PageLayoutType.NumTypes; ++i)
            {
                if (segmentGroupMap[(int)SegmentGroupType.PageLayoutType].getSegmentEnabled(i))
                {
                    enabledPageLayouts.Add((PageLayoutType)i);
                }
            }

            return enabledPageLayouts;
        }

        
        public List<VisitType> getEnabledVisitTypes()
        {
            List<VisitType> enabledVisitTypes = new List<VisitType>();

            for (int i = 0; i < (int)VisitType.NumTypes; ++i)
            {
                if (segmentGroupMap[(int)SegmentGroupType.VisitType].getSegmentEnabled(i))
                {
                    enabledVisitTypes.Add((VisitType)i);
                }
            }

            return enabledVisitTypes;
        }


        // Segment Group parameter getters
        public bool getGlobalEnabled()
        {
            return enabled;
        }
        public string getGlobalName()
        {
            return name;
        }
        public Double getGlobalMinCutoff_SegLikelihood()
        {
            return MinCutoff_SegLikelihood;
        }
        public Double getGlobalMinCutoff_PostReRank()
        {
            return MinCutoff_PostReRank;
        }
        public long getGlobalMinQuota()
        {
            return MinQuota;
        }
        public long getGlobalMaxQuota()
        {
            return MaxQuota;
        }
        public Double getGlobalMinScore()
        {
            return MinScore;
        }
        public Double getGlobalRankHistogramStart()
        {
            return RankHistogramStart;
        }
        public Double getGlobalRankHistogramStep()
        {
            return RankHistogramStep;
        }
        public long getGlobalRankHistogramSize()
        {
            return RankHistogramSize;
        }
        public bool getGlobalEnableCurveFitting()
        {
            return EnableCurveFitting;
        }
        public bool getGlobalEnableRankSpreading()
        {
            return EnableRankSpreading;
        }
        public long getGlobalTierSizeSmallBigZ1IntZ2()
        {
            return TierSizeSmallBigZ1IntZ2;
        }
        public bool getGlobalSAInTop20B()
        {
            return SAInTop20B;
        }





        // Segment Group parameter getters
        public bool getSegmentGroupEnabled(int segmentGroup)
        {
            // this doesnt get "enabled"  default setting for the entire group
            // but returns a Group level only parameter : "GroupEnabled"
            return segmentGroupMap[segmentGroup].getGroupEnabled();
        }
        public string getSegmentGroupName(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getName();
        }
        public Double getSegmentGroupMinCutoff_SegLikelihood(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getMinCutoff_SegLikelihood();
        }
        public Double getSegmentGroupMinCutoff_PostReRank(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getMinCutoff_PostReRank();
        }
        public long getSegmentGroupMinQuota(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getMinQuota();
        }
        public long getSegmentGroupMaxQuota(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getMaxQuota();
        }
        public Double getSegmentGroupMinScore(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getMinScore();
        }
        public Double getSegmentGroupRankHistogramStart(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getRankHistogramStart();
        }
        public Double getSegmentGroupRankHistogramStep(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getRankHistogramStep();
        }
        public long getSegmentGroupRankHistogramSize(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getRankHistogramSize();
        }
        public bool getSegmentGroupEnableCurveFitting(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getEnableCurveFitting();
        }
        public bool getSegmentGroupEnableRankSpreading(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getEnableRankSpreading();
        }
        public long getSegmentGroupTierSizeSmallBigZ1IntZ2(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getTierSizeSmallBigZ1IntZ2();
        }
        public bool getSegmentGroupSAInTop20B(int segmentGroup)
        {
            return segmentGroupMap[segmentGroup].getSAInTop20B();
        }
        

        // Segment parameter getters

        public bool getSegmentEnabled(int segmentGroup, int segment) 
        {
            return segmentGroupMap[segmentGroup].getSegmentEnabled(segment);
        }
        public string getSegmentName(int segmentGroup, int segment) 
        {
            return segmentGroupMap[segmentGroup].getSegmentName(segment);
        }
        public Double getSegmentMinCutoff_SegLikelihood(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentMinCutoff_SegLikelihood(segment);
        }
        public Double getSegmentMinCutoff_PostReRank(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentMinCutoff_PostReRank(segment);
        }
        public long getSegmentMinQuota(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentMinQuota(segment);
        }
        public long getSegmentMaxQuota(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentMaxQuota(segment);
        }
        public Double getSegmentMinScore(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentMinScore(segment);
        }
        public Double getSegmentRankHistogramStart(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentRankHistogramStart(segment);
        }
        public Double getSegmentRankHistogramStep(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentRankHistogramStep(segment);
        }
        public long getSegmentRankHistogramSize(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentRankHistogramSize(segment);
        }
        public bool getSegmentEnableCurveFitting(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentEnableCurveFitting(segment);
        }
        public bool getSegmentEnableRankSpreading(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentEnableRankSpreading(segment);
        }
        public long getSegmentTierSizeSmallBigZ1IntZ2(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentTierSizeSmallBigZ1IntZ2(segment);
        }
        public bool getSegmentSAInTop20B(int segmentGroup, int segment)
        {
            return segmentGroupMap[segmentGroup].getSegmentSAInTop20B(segment);
        }






        /*
         *  Setters
         */

        public void setGlobalParam(string paramName, bool paramValue)
        {
            ConfigEntry entry = new ConfigEntry(-1, -1, paramName, paramValue.ToString());
            if (globalParametersMap.Contains(entry))
            {
                globalParametersMap.Remove(entry);
            }

            globalParametersMap.Add(entry);

            propogateParameters();
            parseParameters();
        }

        public void setGlobalParam(string paramName, int paramValue)
        {
            ConfigEntry entry = new ConfigEntry(-1, -1, paramName, paramValue.ToString());
            if (globalParametersMap.Contains(entry))
            {
                globalParametersMap.Remove(entry);
            }

            globalParametersMap.Add(entry);

            propogateParameters();
            parseParameters();
        }

        public void setGlobalParam(string paramName, long paramValue)
        {
            ConfigEntry entry = new ConfigEntry(-1, -1, paramName, paramValue.ToString());
            if (globalParametersMap.Contains(entry))
            {
                globalParametersMap.Remove(entry);
            }

            globalParametersMap.Add(entry);

            propogateParameters();
            parseParameters();
        }

        public void setGlobalParam(string paramName, double paramValue)
        {
            ConfigEntry entry = new ConfigEntry(-1, -1, paramName, paramValue.ToString());
            if (globalParametersMap.Contains(entry))
            {
                globalParametersMap.Remove(entry);
            }

            globalParametersMap.Add(entry);

            propogateParameters();
            parseParameters();
        }

        public void setGlobalParam(string paramName, string paramValue)
        {
            ConfigEntry entry = new ConfigEntry(-1, -1, paramName, paramValue.ToString());
            if (globalParametersMap.Contains(entry))
            {
                globalParametersMap.Remove(entry);
            }

            globalParametersMap.Add(entry);

            propogateParameters();
            parseParameters();
        }

        // Segment group param setters
        public void setSegmentGroupParam(int segmentGroup, string paramName, bool paramValue)
        {
            ConfigEntry entry = new ConfigEntry(segmentGroup, -1, paramName, paramValue.ToString());

            if (segmentGroupParametersMap.Contains(entry))
            {
                segmentGroupParametersMap.Remove(entry);
            }
            segmentGroupParametersMap.Add(entry);

            segmentGroupMap[segmentGroup].AddParameter(entry);

            propogateParameters();
            parseParameters();
        }

        public void setSegmentGroupParam(int segmentGroup, string paramName, int paramValue)
        {
            ConfigEntry entry = new ConfigEntry(segmentGroup, -1, paramName, paramValue.ToString());

            if (segmentGroupParametersMap.Contains(entry))
            {
                segmentGroupParametersMap.Remove(entry);
            }
            segmentGroupParametersMap.Add(entry);

            segmentGroupMap[segmentGroup].AddParameter(entry);

            propogateParameters();
            parseParameters();
        }

        public void setSegmentGroupParam(int segmentGroup, string paramName, long paramValue)
        {
            ConfigEntry entry = new ConfigEntry(segmentGroup, -1, paramName, paramValue.ToString());

            if (segmentGroupParametersMap.Contains(entry))
            {
                segmentGroupParametersMap.Remove(entry);
            }
            segmentGroupParametersMap.Add(entry);

            segmentGroupMap[segmentGroup].AddParameter(entry);

            propogateParameters();
            parseParameters();
        }

        public void setSegmentGroupParam(int segmentGroup, string paramName, double paramValue)
        {
            ConfigEntry entry = new ConfigEntry(segmentGroup, -1, paramName, paramValue.ToString());

            if (segmentGroupParametersMap.Contains(entry))
            {
                segmentGroupParametersMap.Remove(entry);
            }
            segmentGroupParametersMap.Add(entry);

            segmentGroupMap[segmentGroup].AddParameter(entry);

            propogateParameters();
            parseParameters();
        }

        public void setSegmentGroupParam(int segmentGroup, string paramName, string paramValue)
        {
            ConfigEntry entry = new ConfigEntry(segmentGroup, -1, paramName, paramValue.ToString());

            if (segmentGroupParametersMap.Contains(entry))
            {
                segmentGroupParametersMap.Remove(entry);
            }
            segmentGroupParametersMap.Add(entry);

            segmentGroupMap[segmentGroup].AddParameter(entry);

            propogateParameters();
            parseParameters();
        }



        public void setSegmentParam(int segmentGroup, int segment, string paramName, bool paramValue)
        {
            ConfigEntry entry = new ConfigEntry(segmentGroup, segment, paramName, paramValue.ToString());

            if (segmentParametersMap.Contains(entry))
            {
                segmentParametersMap.Remove(entry);
            }
            segmentParametersMap.Add(entry);

            segmentGroupMap[segmentGroup].AddParameterToSegment(entry);

            propogateParameters();
            parseParameters();
        }

        public void setSegmentParam(int segmentGroup, int segment, string paramName, int paramValue)
        {
            ConfigEntry entry = new ConfigEntry(segmentGroup, segment, paramName, paramValue.ToString());

            if (segmentParametersMap.Contains(entry))
            {
                segmentParametersMap.Remove(entry);
            }
            segmentParametersMap.Add(entry);

            segmentGroupMap[segmentGroup].AddParameterToSegment(entry);

            propogateParameters();
            parseParameters();
        }

        public void setSegmentParam(int segmentGroup, int segment, string paramName, long paramValue)
        {
            ConfigEntry entry = new ConfigEntry(segmentGroup, segment, paramName, paramValue.ToString());

            if (segmentParametersMap.Contains(entry))
            {
                segmentParametersMap.Remove(entry);
            }
            segmentParametersMap.Add(entry);

            segmentGroupMap[segmentGroup].AddParameterToSegment(entry);

            propogateParameters();
            parseParameters();
        }

        public void setSegmentParam(int segmentGroup, int segment, string paramName, double paramValue)
        {
            ConfigEntry entry = new ConfigEntry(segmentGroup, segment, paramName, paramValue.ToString());

            if (segmentParametersMap.Contains(entry))
            {
                segmentParametersMap.Remove(entry);
            }
            segmentParametersMap.Add(entry);

            segmentGroupMap[segmentGroup].AddParameterToSegment(entry);

            propogateParameters();
            parseParameters();
        }

        public void setSegmentParam(int segmentGroup, int segment, string paramName, string paramValue)
        {
            ConfigEntry entry = new ConfigEntry(segmentGroup, segment, paramName, paramValue.ToString());

            if (segmentParametersMap.Contains(entry))
            {
                segmentParametersMap.Remove(entry);
            }
            segmentParametersMap.Add(entry);

            segmentGroupMap[segmentGroup].AddParameterToSegment(entry);

            propogateParameters();
            parseParameters();
        }

    }
}


