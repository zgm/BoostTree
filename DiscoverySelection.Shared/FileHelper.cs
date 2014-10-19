using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.IO;
using System.Text.RegularExpressions;

namespace DiscoverySelection.Shared
{
    public class FileHelper
    {
        public static bool DeserializeSplitMergeManifestFile(String manifestFileString,
                                           out bool bSplit,
                                           out bool bMerge)
        {
            Regex r = new Regex("issplit=(?<1>\\w+),ismerge=(?<2>\\w+)",
                RegexOptions.Multiline | RegexOptions.IgnoreCase);

            Match matchOccurence = r.Match(manifestFileString);

            bSplit = false;
            bMerge = false;

            if (matchOccurence.Success)
            {
                String isSplitString = matchOccurence.Groups[1].ToString();

                if (!Boolean.TryParse(isSplitString, out bSplit))
                {
                    return false;
                }

                String isMergeString = matchOccurence.Groups[2].ToString();

                if (!Boolean.TryParse(isMergeString, out bMerge))
                {
                    return false;
                }

                return true;
            }
            else
            {
                return false;
            }

        }

        public static String SerializeSplitMergeManifestFile(bool bSplit, bool bMerge)
        {
            String manifestString = String.Format("issplit={0},ismerge={1}",
                                        bSplit,
                                        bMerge);

            return manifestString;
        }
                                            
    }
}
