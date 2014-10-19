
using System;

/// <summary>
/// Enums, UDT for handling DocType 
/// </summary>
namespace DiscoverySelection.Shared
{
    //FROM: private\shared\common\src\DocumentTypes.h
    public enum DocType : byte
    {
        dt_UNKNOWN = 0,
        dt_HTML = 1,
        dt_TEXT = 2,
        dt_PDF = 3,
        dt_DOC = 4,
        dt_PPT = 5,
        dt_XLS = 6,
        dt_PS = 7,
        dt_RTF = 8,
        dt_YP = 9,
        dt_WP = 10,
        dt_SHOWTIMES = 11,
        dt_XML = 12,
        dt_RSS = 13,
        dt_NEWSCLUSTER = 14,
        dt_BLOGCLUSTER = 15,
        dt_NNTPPOST = 16,
        dt_DWF = 17,
        dt_SITEMAP = 18,
        dt_SWF = 20,

        // Multimedia added fields
        dt_VIDEO_DEPRECATED = 19,
        dt_RTVIDEO = 19,
        dt_NORMVIDEO_DEPRECATED = 22,
        dt_STVIDEO_DEPRECATED = 23,

        // Twitter related documents
        dt_TWEET = 24,
        dt_TWEETCLUSTER = 25,

        // Multimedia added fields
        dt_IMAGE = 100,
        dt_AUDIO = 101,
        dt_VIDEO = 102,
        dt_EPISODE = 103,
        dt_MMQUERYASSOC = 104,
        dt_NORMVIDEO = 105,// the type of video which has been processed (normalized) by mediabot
        dt_STVIDEO   = 106,// the type of video when processed using static thumbnail image alone by CB chunkbuilder
        dt_PODCAST = 107,
        dt_ARTIST = 108,
        dt_ALBUM = 109,
        dt_SONG = 110,
        dt_LYRIC = 111,
    };
}