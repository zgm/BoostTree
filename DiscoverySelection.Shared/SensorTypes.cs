using System;
using System.Collections.Generic;
using System.Text;

namespace DiscoverySelection.Shared
{
    public enum SensorTypes
    {
        Ie8,
        Tb,
        Sitemap,
        Injection,
        URLFeatures,
        DocumentFeatures,
        Experimental,
        DeepCrawl,  // includes both deep crawl and shallow crawl
        Fex,
        Registration,
        DepthAllocation,
        DomainFeatures,
        CustomScore,
        HostFeatures,
        MetricsSensor,
        SitemapFile,
        ClickBoost,
        SafeUrls,
        DataMining,
        SuperFresh,
        CrawlFailure,
        DeepCrawlV3,  // includes both deep crawl and shallow crawl
        CrawlInjection,
        Ie8Hub,
        TbHub,
        SitemapUrlsV3,
        SerpUrls,
        DeltaWebmap,
        Video,
        RankFile,
        ExpressRankFile,
        MetricsSensorV2,
        GSerpUrls,
        Social,
        PageStaticRank,
        ImageClick,
        IndexProbe,
        Ie8VisitInfo,
        Segments,
        Ie9,
        GrapheXFeatures,
        GraphExDomainFeatures,
        GraphExHostFeatures

        // Always add new types at the end

        // WARNING: Max 64 types supported.  See SensorMask
    };
}
