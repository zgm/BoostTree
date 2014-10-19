using System;
using System.Collections.Generic;
using System.Text;
using System.IO;


namespace DiscoverySelection.Shared
{

    public class SitePrefixTable : IInitializable
    {
        public Dictionary<string, double> table = new Dictionary<string, double>();
        public UrlReverser reverser = null;
        
        public bool getReversedSite = false;
        public bool isDefined = false;
        
        private Object InitializeLock = new Object();

        public SitePrefixTable()
        {
        }

        public void Initialize(string sitePrefixTableFile, string tldFile, bool isRev)
        {
            getReversedSite = isRev;
            table = LoadPrefixTable( sitePrefixTableFile );
            reverser = new UrlReverser(tldFile);
            isDefined = true;
        }

        #region IInitializable Members

        public void Initialize(Dictionary<String, String> parameters)
        {
            
            string tldfile = "tld.txt";
            string sitePrefixTableFile = "PrefixTable.txt";
            string getReversedSiteString = "false";

            lock( InitializeLock )
            {
                try
                {
                    if (!parameters.TryGetValue("getreversedsite", out getReversedSiteString ))
                    {
                        getReversedSiteString = "false";
                        Console.WriteLine( " getreversedsite missing. Default set to " + getReversedSiteString );
                    }                

                    if (!parameters.TryGetValue("tldfile", out tldfile ))
                    {
                        tldfile = "tld.txt";
                        Console.WriteLine( " tldfile missing. Default set to " + tldfile );
                    }
                    

                    if (!parameters.TryGetValue("siteprefixtable", out sitePrefixTableFile ))
                    {
                        sitePrefixTableFile = "PrefixTable.txt";
                        Console.WriteLine( " siteprefixtable  missing. Default set to "  +  sitePrefixTableFile  );
                    }   
                    
                    getReversedSite = bool.Parse( getReversedSiteString );
                    table = LoadPrefixTable( sitePrefixTableFile );
                    reverser = new UrlReverser(tldfile);
                    isDefined = true;
                
                }
                catch( Exception e )
                {
                    isDefined =false;
                    throw new Exception( "SitePrefixTable : Initialization error : " + e.ToString() );
                }
            }          
        }

        #endregion
        public static Dictionary<string, double> LoadPrefixTable( string filenameInput )
        {
            string filename = Path.GetFileName( filenameInput );
            if (!File.Exists(filename))
            {
                throw new Exception(filename + " is missing.");
            }

            Dictionary<string, double> result = new Dictionary<string, double>();

            using (StreamReader reader = new StreamReader(filename))
            {
                string line = null;
                while ((line = reader.ReadLine()) != null)
                {
                    try
                    {
                        string entry = (line.Trim());
                        string [] tokens = line.Split( '\t' );
                        if( tokens.Length != 2  )
                        {
                            continue;
                        }
                        result.Add(tokens[0], double.Parse( tokens[1] ) );
                    }
                    catch( Exception e)
                    {
                        throw new Exception( line + "  | " + e.ToString() );
                    }
                }
            }

            return result;
        }
        

        public string MapSegmentedUrlToSite( SegmentedReversedUrl segUrl)
        {
            String domain, host, l1Path;
            domain =  segUrl.ReversedDomain;
            host = segUrl.ReversedHost;
            l1Path = segUrl.ReversedL1Path ;
            string site = null;
            
            if( !isDefined )
            {
               throw new Exception("SitePrefixTable is not defined" );
            }


            try
            {

               // same filtering of host/l1path  in construction of SitePrefixTable
               // normalization of  prefixe dupes
               {
                   if( !String.IsNullOrEmpty(l1Path ))
                   {
                       l1Path = l1Path.Trim().Replace( " ", "%20" );
                       if( host == l1Path || l1Path == (host + "/") || l1Path == (host + "//"))
                       {
                           l1Path = "";
                       }
                   }

                   if( !String.IsNullOrEmpty(host))
                   {
                       
                       if( host == domain || host == (domain + "/") || host == (domain + "//"))
                       {
                           host = "";
                       }
                   }
               }

               if(!String.IsNullOrEmpty(l1Path) && table.ContainsKey( l1Path ))
               {
                   site =  l1Path;
               }
               else if( !String.IsNullOrEmpty(host) && table.ContainsKey( host ))
               {
                   site =  host;
               }
               else
               {
                   site = domain;
               }


               if( !getReversedSite)
               {
                   site = UrlReverser.ReverseBack( site );
               }
            }
            catch(Exception)
            {
               return null;
            }
            return site;

        }
        
        
        public string MapUrlToSite( string url, bool isUrlReversed )
        {

            string site = null;

            if( !isDefined )
            {
                throw new Exception("SitePrefixTable is not defined" );
            }
            
            try
            {
                SegmentedReversedUrl segUrl = reverser.ExtractSegmentedReversedUrl(url, isUrlReversed);
                return MapSegmentedUrlToSite( segUrl  );
            }
            catch(Exception)
            {
                site = null;
            }
            return site;
        }


        
        public bool ContainsKey( string key, bool isUrlReversed, out double value )
        {
            string revKey = key;
            value = 0;
            
            if( String.IsNullOrEmpty(key) )  return false;
            
            if( !isDefined )
            {
                throw new Exception("SitePrefixTable is not defined" );
            }
            
            if( !isUrlReversed )
            {
                revKey = UrlReverser.Reverse( revKey );
            }
            
            if( table.ContainsKey( revKey ) )
            {
                value = table[revKey];
                return true;
            }
            return false;
        }

        public void WriteToFile(string fn)
        {
            using (TextWriter writer = File.CreateText(fn))
            {
                foreach( string key in table.Keys )
                {
                    writer.WriteLine("{0}\t{1}", key, table[key]);
                }
            }
        }


    }
}
