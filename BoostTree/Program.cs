using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Selection.Core;



namespace BoostTree
{
    class Program
    {
        static void Main(string[] args)
        {
            long DaySec = ((long)(DateTime.Parse("2014-01-21") - DateTime.Parse("1970-1-1")).TotalSeconds);
            long Day = ((long)(DateTime.Parse("2014-01-23") - DateTime.Parse("1970-1-1")).TotalDays);

            IFeatureTransform fttransform = new FeatureTransform("D:\\work\\Project\\BoostTree\\BoostTree\\bin\\Debug\\ServeFtTransformTree.V3.NoTransformation.txt");
            IClassifier classifier = new BoostTreeClassifier("D:\\work\\Project\\BoostTree\\BoostTree\\bin\\Debug\\SelectionModel.V3.dat", Enum.GetNames(typeof(featureList)), 0);

            float?[] features = new float?[(int)featureList.NumFields];
            for (int i = 0; i < (int)featureList.NumFields; i++) features[i] = 0.0f;

            StreamReader sr = new StreamReader("D:\\work\\Project\\BoostTree\\BoostTree\\bin\\Debug\\selection.txt", Encoding.Default);
            string line;
            while ((line = sr.ReadLine()) != null)
            {
                var data = line.Split('\t');
                for (int i = 0; i < (int)featureList.NumFields; i++)
                {
                    if (i == 10)
                    {
                        features[i] = (DaySec - TryParseLong(data[i + 4], 0)) / 86400.0f;
                    }
                    else if (i == 45)
                    {
                        if (string.IsNullOrEmpty(data[i + 4])) features[i] = Day;
                        else features[i] = Day - TryParseLong(data[i+4], 0);
                    }else features[i] = ConvertNullToZero(data[i + 4]);
                }

                float[] tfeatures = fttransform.Transform(features);
                double score = classifier.Classify(tfeatures);
                Console.WriteLine(score);
                Console.WriteLine(ConvertScoreTo64K(score));
            }

            Console.WriteLine();
        }

        public enum featureList
        {
            pageRank = 0, domainRank,
            //DA Features 4 Dim
            viewCount, userRating, userRatingCount, commentCount,
            RawPageRank, InLinkCount, IntraDomainInLinkCount, CurveFitPageRank32, pubDate,
            Block, BlockIn32days, BlockIn16days,
            //URL Features 51 Dim
            vmIeDataWeightedDecay1, vmIeDataWeightedDecay4, vmIeDataWeightedDecay8, vmIeDataWeightedDecay16, vmIeDataWeightedDecay32, vmIeDataWeightedDecay64,
            mIeDataWeightedDecay1, mIeDataWeightedDecay4, mIeDataWeightedDecay8, mIeDataWeightedDecay16, mIeDataWeightedDecay32, mIeDataWeightedDecay64,
            mIEBrowseDataWeightedDecay1, mIEBrowseDataWeightedDecay4, mIEBrowseDataWeightedDecay8, mIEBrowseDataWeightedDecay16, mIEBrowseDataWeightedDecay32, mIEBrowseDataWeightedDecay64,

            BingViewDataWeightedDecay1, BingViewDataWeightedDecay4, BingViewDataWeightedDecay8, BingViewDataWeightedDecay16, BingViewDataWeightedDecay32, BingViewDataWeightedDecay64,
            BingClickDataWeightedDecay1, BingClickDataWeightedDecay4, BingClickDataWeightedDecay8, BingClickDataWeightedDecay16, BingClickDataWeightedDecay32, BingClickDataWeightedDecay64,
            Hovers, viewCountLastUpdateTime,
            viewCountDataWeightedDecay1, viewCountDataWeightedDecay4, viewCountDataWeightedDecay8, viewCountDataWeightedDecay16, viewCountDataWeightedDecay32, viewCountDataWeightedDecay64,

            //Click Features 3 Dim
            GClickHub, GClickAuth, BClickHub, BClickAuth,
            //Special field to signify end: 61.
            NumFields
        }

        public static long TryParseLong(string val, long defaultval)
        {
            double dval = 0.0;
            if (double.TryParse(val, out dval))
                return (long)dval;
            return defaultval;
        }
        public static ulong TryParseULong(string strUlong)
        {
            long retUlong = 0;
            long.TryParse(strUlong, out retUlong);
            return (ulong)retUlong;
        }
        public static float ConvertNullToZero(string str)
        {
            float numValue = 0.0f;
            if (!string.IsNullOrEmpty(str))
            {
                float.TryParse(str, out numValue);
            }
            return numValue;
        }

        static double c_maxExpectedStaticRank = 1.1;
        static double c_maxMsnRank = 65535.0;
        static double c_msnRankFactor = c_maxMsnRank / (Math.Log(Math.Log(c_maxExpectedStaticRank + Math.Exp(1.0))));
        public static int ConvertScoreTo64K(double rank)
        {
            double db = Math.Log(Math.Log(rank + Math.Exp(1.0))) * c_msnRankFactor;
            if (db < 0.0)
            {
                db = 0.0;
            }
            else if (db > c_maxMsnRank)
            {
                db = c_maxMsnRank;
            }
            return ((int)db);
        }
    }
}
