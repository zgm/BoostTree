using System;
using System.Collections.Generic;
using System.Text;

namespace DiscoverySelection.Shared
{
    public class DatedUrls
    {
        public static string ExtractDate(string url)
        {
            bool founddate;

            string date = ExtractDate(url, true, out founddate);

            return founddate ? date : null;
        }

        // WARNING: Only works for 2011 and 2012 URLs to reduce false positives
        private static string ExtractDate(string url, bool isfirst, out bool founddate)
        {
            founddate = false;

            if (url.Length < 15) return null;

            if (url.Contains("?")) return null;

            //   /2010-10-01/
            //pos 0123456789
            int pos = url.IndexOf("2011");

            if (pos < 1 || pos > url.Length - 11)
                pos = url.IndexOf("2012");

            if (pos < 1 || pos > url.Length - 11) return null;

            if (Char.IsLetterOrDigit(url[pos-1]) || Char.IsLetterOrDigit(url[pos+10])) return null;

            char sep = url[pos+4];

            if (sep != '.' && sep != '-' && sep != '/' && sep != '_') return null;

            if (url[pos+7] != url[pos+4]) return null;

            if ( !Char.IsDigit(url[pos+5]) || !Char.IsDigit(url[pos+6])
              || !Char.IsDigit(url[pos+8]) || !Char.IsDigit(url[pos+9])) return null;

            int month = 0;

            if (!int.TryParse(url.Substring(pos+5, 2), out month) || month <= 0 || month > 12) return null;

            int day = 0;
            if (!int.TryParse(url.Substring(pos+8, 2), out day) || day <= 0 || month > 31) return null;

            int year = 0;

            if (!int.TryParse(url.Substring(pos, 4), out year) || year < 2011 || year > 2012) return null;

            string ret = String.Format("{0:D4}{1:D2}{2:D2}", year, month, day);

            founddate = true;

            bool nextfounddate;

            string restdate = ExtractDate(url.Substring(pos + 10), false, out nextfounddate);

            if (isfirst)
            {
                if (nextfounddate)
                {
                    founddate = false;
                    return null;
                }
            }

            founddate |= nextfounddate;
            return ret;
        }


        public static int? QuantizeAge(int? age)
        {
            if (age == null) return age;

            int a = age.Value;

            if (a <= 20) return a;

            if (a <= 30) return ((int) (age / 2)) * 2;

            if (a <= 80) return ((int) (age / 5)) * 5;

            if (a <= 120) return ((int) (age / 10)) * 10;

            return ((int) (age / 15)) * 15;
        }

        public static DateTime GetDate(int date)
        {
            int y = date / 10000;
            int m = (date - (y * 10000)) / 100;
            int d = date - (y * 10000) - m * 100;

            return new DateTime(y, m, d);
        }

        public static int? DiffDate(int date1, int date2)
        {
            try {
                return GetDate(date2).Subtract(GetDate(date1)).Days;
            }
            catch (Exception)
            {
                return null;
            }
        }

    }
}

