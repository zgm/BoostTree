using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

// add SitemapShared in namespace to avoid name collision until we
// remove dependency on lightspeed dlls
//
// Those enums are shared with Stephan's sitemap fetcher
namespace DiscoverySelection.Shared.SitemapShared
{
    public enum SitemapChangeFreq : byte 
    { 
        unknown = 0, 
        always = 1, 
        hourly = 2, 
        daily = 3, 
        weekly = 4, 
        monthly = 5, 
        yearly = 6, 
        never = 7 
    }

    public enum SitemapCrawlStatusType : byte
    {
        Empty,
        Redirect,
        Failed,
        DNSFailed,
        ExceededLimit,
        Success,
        // This is a transient status  used by scheduler alone. Not intended for fetcher
        RecentlySubmitted=128,
        TrustedSitemapInjection=129,
    }

    public enum SitemapParseStatusType : byte
    {
        Empty,
        Unsupported,
        ParsingError,
        Success,
    }

    [Flags]
    public enum SitemapFileSensorSourceType : uint
    {
        Default = 0x0,
        WMProfileDelete = 0x1,
        WMProfile = 0x2,
        WMSubmit = 0x4,
        Robots = 0x8,
        RK = 0x10,
        RKStats = 0x20,
        Manual = 0x40,
        SitemapIndex = 0x80,
        Trusted = 0x100,
        TrustedSitemapIndex = 0x200,
    }

    public enum SitemapFileType : byte
    {
        Unknown,
        Xml,
        XmlIndex,
        Txt,
        Atom,
        Rss,
        Html,
        XmlBaidu,
    }

    public enum SitemapUrlValidationState : byte
    {
        Empty,
        Success,
        InvalidUrl,
        SecurityIssue,
    }

    public class SitemapShared
    {
        //some value corresponding to something very old (not realistically possible)
        public const Int16 LAST_MOD_DEFAULT = -9999;
    }

}
