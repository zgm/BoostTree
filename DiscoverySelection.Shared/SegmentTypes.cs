using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Reflection;

/// <summary>
/// Enums, UDT for handling Segments 
/// NOTE: This is used by UrlMetadata.SegmentsSensorData + sensors: QuerySeg 
///       Redeploy dependent services for consistency.       
/// </summary>
namespace DiscoverySelection.Shared
{
    /// enums for each orthogonal segment type like intent, market, file_format, page_format
    public enum SegmentGroupType : int
    {
        Unknown=0,
        QueryIntentType,
        MarketSegType,
        FileFormatType,
        VisitType,
        PageLayoutType,

        
        //APPEND HERE
        // NOTE: This is used by UrlMetadata.SegmentsSensorData + sensors: QuerySeg 
        //       Redeploy dependent services for consistency.
        NumTypes,
    }
    
    /// items ordered by freq, to reduced encoding length.
    /// APPEND new items before NumTypes
    public enum QueryIntentType : int
    {
        Other=0,
        Navigational,
        Commerce,
        Name,
        NameNonCeleb,
        Adult,
        Local,
        UrlQuery,
        Music,
        QuestionPattern,
        Travel,
        Autos,
        Download,
        Tech,
        WikipediaReference,
        ConsumerElectronics,
        MovieShowtimes,
        Recipes,
        Sports,
        MovieTitle,
        QandA,
        HowTo,
        Book,
        Finance,
        Dictionary,
        ThingsTodo,
        Hotel,
        Health,
        Restaurant,
        TvShows,
        Seasonal,
        Maps,
        List,
        Flight,
        Celebrities,
        ClothesAndShoes,
        VideoGames,
        TravelGuide,
        RealEstate,
        OnlineGames,
        Education,
        MovieTheater,
        University,
        Jobs,
        FlightStatus,
        Events,
        Weather,
        Nightlife,
        RadioStations,
        AppIntent,
        Bus,
        Nutrition,
        TechDownload,
        TechHelp,
        NamePlus,
        
        // APPEND HERE
        // NOTE: This is used by UrlMetadata.SegmentsSensorData + sensors: QuerySeg 
        //       Redeploy dependent services for consistency.
        NumTypes,
    }
    
    // To list only markets that need allocation/tracking
    // List till ja_us: Markets with size greater than 1% @ InIndexMarketStats
    // BlackBook Top7 revenue: UK, JA, FR (70%), DE, AU, CA, CN (17%)
    // The markets are exact mappings to country/language labels in IndexProbe data.
    // <lang>_<country> 
    public enum MarketSegType : int
    {
        Other=0,
        en_us,
        zh_chs_cn, // per InIndex  lang flag zh_chs, In index as of 02/2012 -- 1B 
        ja_jp,
        en_gb,
        fr_fr,
        de_de,  //In index as of 02/2012 -- 452M
        es_us,
        pt_br,  //In index as of 02/2012 -- 336,812,764
        es_es,
        ru_ru,
        it_it,
        tr_tr,
        pl_pl,
        ja_us,
        // additional markets outside 1% cutoff but in GR P1
        ko_kr,  //In index as of 02/2012 -- 74M
        en_in,
        en_au,
        en_ca,
        zh_cn,  //It doesn't map to IndexProbe labels. Use zh_chs_cn and zh_cht_cn instead.
        // secondary markets in top countries
        en_jp,
        fr_ca,
        en_cn,
        en_ru,
        en_kr,

        //zh-tw, zh-hk
        zh_cht_us,  //In index as of 02/2012 -- 52,860,667 
        zh_cht_tw,  //In index as of 02/2012 -- 155,572,741 
        zh_cht_hk,  //In index as of 02/2012 -- 32,473,397 
        zh_cht_cn,  //In index as of 02/2012 -- 6,575,529 
        zh_chs_us,  //In index as of 02/2012 -- 85,153,638 
        zh_chs_hk,  //In index as of 02/2012 -- 10,021,100 
        
        en_ph,  //In index as of 02/2012 -- 10,802,592
        es_mx,  //In index as of 02/2012 -- 78,839,790

        //APPEND HERE,  DONT remove pre-existing entries
        // NOTE: This is used by UrlMetadata.SegmentsSensorData + sensors: QuerySeg 
        //       Redeploy dependent services for consistency.
        NumTypes,
    }

    // format classifiers TBD
    // Signal : Align with DP/DC metaword : DocumentType (enum \private\shared\common\src\DocumentTypes.h)
    // http://sharepoint/sites/CoreSearch/Teams/Relevance/Backend/DocumentConversion/Shared Documents/FileTypeDetectionPR.xlsx
    // Extension based format type detection is very unreliable for non-html docs
    public enum FileFormatType : int
    {
        Unknown=0,
        html,
        image, 
        video, 
        pdfps, // pdf, ps
        // office triad
        word, // includes doc, docx, odt
        ppt, //ppt, pptx, odp
        excel, // xls, ods
        text, 
        xml, // xml , 
        audio,
        flash, //swf
        email, //outlook
        feed, //rss
        sitemap, //
        robots, //robots.txt

        
        //APPEND HERE,  DONT remove pre-existing entries
        // NOTE: This is used by UrlMetadata.SegmentsSensorData + sensors: QuerySeg 
        //       Redeploy dependent services for consistency.
        
        NumTypes,
    }
    
    // signals from DU : TBD
    public enum PageLayoutType : int
    {
        Unknown=0,
        discussion,
        blog,
        forum,
        profile,    // user homepage/profilepage , typically for NameNonCeleb

        //APPEND HERE,  DONT remove pre-existing entries
        // NOTE: This is used by UrlMetadata.SegmentsSensorData + sensors: QuerySeg 
        //       Redeploy dependent services for consistency.
        
        NumTypes,
    }

    // bit # of flags in UrlSourceType
    public enum VisitType : int
    {
        Unknown = 0,
        //  NON serp web page
        Browse,
        //Any Serp including Baidu, Daum, Naver, Yandex
        Serp,
        Google,   
        Bing,
        Yahoo,
        DirectVisit, // known to be a direct click  nav256
        FaceBook,
        Twitter,
        // source url: domain/host home page
        HomePage,
        // source: not serp, not same domain
        OutsideDomain,
        // never spammed whitelist from IQ
        // Trusted = 0x400,

        //APPEND HERE,  DONT remove pre-existing entries
        // NOTE: This is used by UrlMetadata.SegmentsSensorData + sensors: QuerySeg 
        //       Redeploy dependent services for consistency.
        
        NumTypes,
    }

    

    //to support more than 64 bits in a mask
    //Enum with NumTypes as a member necessary
    public class BitMask<T> : SerializableObject
        where T : struct, IComparable, IConvertible
    {
        // BitArray is a sealed class
        public BitArray Bits = null;
        
        //31 bit mask x 8  (save last bit for extending mask)
        private const int MAXLEN = 256-8;
        private static int Size = GetLength();
        private Type enumType = typeof(T);

        public int Length
        {
            get
            {
                return Size;
            }
        }
        // return true if any bit is true
        public bool Any 
        {
            get 
            {
                return AnyBit();
            }
        }

        public bool Equals( BitMask<T> right )
        {
            if( (Length != (right.Length)) || (enumType != right.enumType))   return false;
            return( this.ToString() == right.ToString() );
        }

        public bool AnyBit()
        {
            if( Bits != null )
            {
                for( int b=0; b<Bits.Length; b++ )
                {
                    if(Bits[b])
                    {
                        return true;
                    }
                }
            }
            return false;
        }

       
        public static int GetLength( )
        {
            int Len = 0;
            if( !typeof(T).IsEnum )
            {
                throw new ArgumentException("Not an EnumType mask :" + typeof(T));
            }
            try
            {
                Len = GetEnumVal(ParseEnum("NumTypes"));
            }
            catch( Exception )
            {
                throw new ArgumentException("NumTypes undefined : " + typeof(T) );
            }
            return Len;
        }
        
        public BitMask( )
        {
            
            
            if( Length >= MAXLEN || Length == 0 )
            {
                throw new ArgumentException("BitMask Length exceeded :" + Length );
            }
            
            
        }

        public void InitializeBits()
        {
            Bits = new BitArray( Length  );
            Bits.Length = Length;
        }
        public void Reset()
        {
            if( Bits != null )
            {
                Bits.SetAll(false);
            }
        }
        public static T ParseEnum( String eName )
        {
            try
            {
                return (T) Enum.Parse( typeof(T), eName, true );
            }
            catch(Exception )
            {
                // Console.WriteLine( "Parse Error: " + eName + " is not a member of " + typeof(T) );
                // 0 is assumed to be Unknown or Other in all enums
                return GetValEnum(0);
            }
        }


        public bool this[int index]
        {
          set{ 
                if(Bits == null )
                {
                    InitializeBits();
                }
                Bits[index] = value; 
             }
          get{ 
                if( Bits == null || index >= Length || index < 0)
                {
                    return false;
                }
                return Bits[index]; 
             }
        }

        // core set function
        public void Set( int bit, bool val )
        {
            this[bit]= val;
        }
        
        // Set overloads
        public void Set( String eName )
        {
            Set( eName, true );
        }
        public void Set( String eName, bool val )
        {
            Set(ParseEnum(eName), val);
        }
        
        public void Set( T item )
        {
            Set(item, true );
        }

        public void Set( T item, bool val )
        {
            Set( GetEnumVal( item ), val);
        }

        // Get 
        public bool Get(int bit )
        {
            return this[ bit ];
        }

        // Get overloads
        public bool Get( String eName )
        {
            return Get( GetEnumVal( ParseEnum( eName) ));
        }
        
        public bool Get( T item )
        {
            return Get( GetEnumVal( item ));
        }


        public static int GetEnumVal( T eVal )
        {
            return (int) eVal.GetType().GetField("value__").GetValue(eVal);
        }

        public static T GetValEnum( int val )
        {
            T eVal = (T) Enum.ToObject( typeof(T), val );
            return eVal;
        }

        public int ByteLength 
        {
            get 
            {
                return (((Length-1) >> 3) + 1);
            }
        }
        public byte[] ToByteArray( )
        {
            if( Bits == null )  return null;
            
            byte[] bytes = new byte[ ByteLength ];
            Bits.CopyTo( bytes, 0 );
            return bytes;
        }

        public void FromByteArray( byte[] byteArr )
        {
            if( byteArr == null )
            {
                Reset();
            }
            Bits = new BitArray( byteArr );
            Bits.Length = Length;
        }

        //Name1,Name2
        public override string ToString()
        {
            if( Bits == null )  return String.Empty;
            
            StringBuilder sb = new StringBuilder();
            
            for(int b=0; b<Bits.Length; b++ )
            {
                if( Bits.Get(b) )
                {
                    T val = GetValEnum( b );
                    sb.Append( val.ToString() + "," );
                }
            }
            String bitStr = sb.ToString();
            if( bitStr.EndsWith(","))
            {
                bitStr = bitStr.Substring(0, bitStr.Length-1);
            }
            return bitStr;
        }

        // csv of enabled bit names
        public static BitMask<T> ParseFromToString( string bitNameStr )
        {
            BitMask<T> bitMask = new BitMask<T>();
            string[] bitNames = bitNameStr.Split(new char[]{','},StringSplitOptions.RemoveEmptyEntries);
            foreach( string bitName in bitNames )
            {
                if(!String.IsNullOrEmpty(bitName.Trim()))
                {
                    bitMask.Set( bitName.Trim() );
                }
            }
            return bitMask;
        }

        public static BitMask<T> Merge( BitMask<T> left, BitMask<T>  right )
        {
            if( left == null )  return right;
            if( right == null ) return left;
            
            if( (left.Length != (right.Length)) || (left.enumType != right.enumType))
            {
                throw new Exception( "BitMask<T>: left & right size/type mismatch" );
            }
            if( left.Bits == null )
            {
                left.Bits = right.Bits;
            }
            else if( right.Bits != null )
            {
                left.Bits.Or( right.Bits );
            }
            return left;
        }

        public static BitMask<T> MergeAnd(  BitMask<T> left, BitMask<T> right )
        {
            if( right == null ) return left;
            
            if( (left.Length != (right.Length)) || (left.enumType != right.enumType))
            {
                throw new Exception( "BitMask<T>: left & right size/type mismatch" );
            }
            if( left.Bits == null )
            {
                left.Bits = right.Bits;
            }
            else if( right.Bits != null )
            {
                left.Bits.And( right.Bits );
            }
            return left;
        }
        public override void Serialize( BinaryWriter bw )
        {
            // bit(k) is 1 iff any bit in byte(k) is 1
            // 2nd degree bit index
            UInt32 mask = 0;
            byte[] byteArr = null;
            if( AnyBit() )
            {
                byteArr = ToByteArray();
                for( int b=0; b<byteArr.Length; b++ )
                {
                    if(byteArr[b] > 0)
                    {
                        mask |= (1U << b );
                    }
                }
            }
            bw.Write(mask);
            if( mask > 0 )
            {
                for( int b=0; b<byteArr.Length; b++ )
                {
                    if( (mask & (1U << b )) > 0 )
                    {
                        bw.Write(byteArr[b]);
                    }
                }
            }
        }

        public override void Deserialize( BinaryReader br )
        {
            UInt32 mask = 0;
            
            mask = br.ReadUInt32();
            
            //Console.WriteLine( String.Format("0x{0:X}", mask ));
            if( mask > 0 )
            {
                byte[] byteArr = new byte[ByteLength];
                for( int b=0; b<ByteLength; b++ )
                {
                    if( (mask & (1U << b )) > 0 )
                    {
                        byteArr[b] = br.ReadByte();
                    }
                }
                FromByteArray( byteArr );
            }
            
        }

    }


    public class BitMaskValues<EnumT> : SerializableObject
                        where EnumT : struct, IComparable, IConvertible
    {
        public double []bitValues = null;
        public bool SerializeToByte = false;
        public bool SerializeToSingle = false;
        public double minCutoff = 0;
        public UInt64 aggCount = 0;
        public double sampling = 0;
        
        private static int Size = BitMask<EnumT>.GetLength();
        private Type enumType = typeof(EnumT); 

        public int Length
        {
            get{    return Size; }
        }
        public double MinValueCutOff
        {
            set{    minCutoff = value; }
            get{    return minCutoff; }
        }

        public BitMaskValues( )
        {
            Reset();
        }
        public void Reset()
        {
            
            
            if( bitValues != null )
            {
                ResetBitValues();
            }
            SerializeToByte = false;
            minCutoff = 0;
            aggCount = 0;
            sampling = 0;
        }

        public void ResetBitValues()
        {
            if( bitValues == null ) return;
            
            for(int v=0; v<bitValues.Length; v++ )
            {
                bitValues[v] = 0;
            }
        }

        public void InitializeBitValues()
        {
            bitValues = new double[Length];
        }

        public double this[int index]
        {
            set
            { 
                
                if( index <0 || index >= Length )
                {
                    // silent exception handling
                    return;
                }

                if( bitValues == null )
                {
                    InitializeBitValues();
                }
                
                bitValues[index] = value; 
            }
            get
            {
                if( (bitValues == null) || (index >= Length) || (index <0) )
                {
                    // silent exception handling
                    return 0D;
                }
                return bitValues[index]; 
            }
        }

        public bool Any
        {
            get
            {
                if( bitValues == null ) return false;
                
                for(int v=0; v<bitValues.Length; v++ )
                {
                   if(bitValues[v] > minCutoff )
                   {
                        return true;
                   }
                }
                return false;
            }
        }


        public void ApplyMinCutoff( BitMaskValues<EnumT> MinCutOffVals )
        {
            
            minCutoff = 0;

            if( bitValues == null ) return;

            if( MinCutOffVals == null ||  MinCutOffVals.bitValues == null ) return;
            
            for(int v=0; v<bitValues.Length; v++ )
            {
               if(bitValues[v] < MinCutOffVals[v] )
               {
                    bitValues[v] = 0;
               }
            }
    
        }

        public void ApplyMinCutoff( double MinCutOffVal )
        {
            
            minCutoff = MinCutOffVal;

            if( bitValues == null ) return;
            
            for(int v=0; v<bitValues.Length; v++ )
            {
               if(bitValues[v] < minCutoff )
               {
                    bitValues[v] = 0;
               }
            }
    
        }

        public void Scale( BitMaskValues<EnumT> right )
        {
            if( right == null )
            {
                throw new Exception( "Scale filter arg is null" );
            }
            
            if(enumType != right.enumType)
            {
                throw new Exception( "BitMaskValues enumType mismatch" );
            }
            

            if( bitValues == null ) return;
            
            for(int v=0; v<bitValues.Length; v++ )
            {
               bitValues[v] *= right[v];
            }
        }
        public bool Equals( BitMaskValues<EnumT> right )
        {
            if( right == null ) return false;
            if( (Length != (right.Length)) || (enumType != right.enumType))   return false;
            return( this.ToString() == right.ToString() );
        }

        
        

        public void Aggregate( BitMask<EnumT> data )
        {
            if(data == null || !data.Any) return;

            if( bitValues == null )
            {
                InitializeBitValues();
            }
            
            for(int v=0; v<bitValues.Length; v++ )
            {
                bitValues[v] += (data[v]) ? 1 : 0;
            }
            aggCount++;
        }

        public void Aggregate( BitMaskValues<EnumT> data )
        {
            if(data == null || !data.Any) return;

            if( bitValues == null )
            {
                InitializeBitValues();
            }
            
            for(int v=0; v<bitValues.Length; v++ )
            {
                bitValues[v] += (data[v]);
            }
            aggCount += data.aggCount;
        }

        // Can only be called once
        public void Update( UInt64 urlCount, double minSamplingReq, double penaltyFactor )
        {
            if( aggCount < 1 ) return;
            
            sampling = aggCount*1.0D/Math.Max(1, urlCount);
            double estimateAdjustFactor = SegmentUtils.SampleEstimateAdjustFactor( sampling , minSamplingReq, penaltyFactor);

            if( bitValues == null ) return;
            
            if( aggCount < 5 && urlCount > 10 ) 
            {
                estimateAdjustFactor = 0;
            }
            
            // agg here is #seeds for a SegmentGroupType
            // use a safer limit   , for stricter limits need to make it configurable per SegmentGroupType
            if( aggCount > 10000 )
            {
                estimateAdjustFactor = 1.0;
            }

            for(int v=0; v<bitValues.Length; v++ )
            {
                bitValues[v] *= estimateAdjustFactor/aggCount;
            }
        }

        // Can only be called once
        public void Propagate( BitMaskValues<EnumT> dataAtPar, double minSamplingReq )
        {
            if( dataAtPar == null || dataAtPar.bitValues == null  )     return;

            if( bitValues == null )
            {
                InitializeBitValues();
            }

            double alpha = SegmentUtils.WeightedBySampling( sampling, dataAtPar.sampling, minSamplingReq);

            // agg here is #seeds for a SegmentGroupType
            // use a safer limit   , for stricter limits need to make it configurable per SegmentGroupType
            if( aggCount > 10000 )
            {
                alpha = 1.0;
            }
            
            for(int v=0; v<bitValues.Length; v++ )
            {
                bitValues[v] = bitValues[v]*alpha + (1-alpha)*dataAtPar[v];
            }
            
            if( alpha <= 0 )
            {
                sampling = dataAtPar.sampling;
            }
        }


        
        public void NormalizeByAggValues( )
        {
            if( bitValues == null ) return;
            
            double sum = 0;
            
            for(int v=0; v<bitValues.Length; v++ )
            {
                sum += bitValues[v];
            }
            sum = (Math.Abs(sum) < 1e-6) ? 1 : sum;
            for(int v=0; v<bitValues.Length; v++ )
            {
                bitValues[v] /= sum;
            }
        }

        public void NormalizeByAggCount( )
        {
            if( aggCount <= 1 ) return;
            if( bitValues == null ) return;
            for(int v=0; v<bitValues.Length; v++ )
            {
                bitValues[v] /= aggCount*1.0D;
            }
        }

        public void Scale( double factor )
        {
            if( bitValues == null ) return;
            for(int v=0; v<bitValues.Length; v++ )
            {
                bitValues[v] *= factor;
            }
        }
        

        public BitMask<EnumT> UpdateMask( double minVal)
        {
            if( bitValues == null ) return null;
            BitMask<EnumT> bitMask = new BitMask<EnumT>();
            for(int v=0; v<Length; v++ )
            {
                bitMask[v] = (bitValues[v] > minVal );
            }
            return bitMask;
        }

        public void SerializeBitValues( BinaryWriter bw, BitMask<EnumT> bMask )
        {
            if( bitValues == null || bMask == null ) return;
            
            for(int v=0; v<bMask.Length; v++ )
            {
                if(bMask[v])
                {
                    if( SerializeToByte )
                    {
                        byte val = (byte) (Math.Max(0D,Math.Min(1D,bitValues[v]))*255D);
                        bw.Write( val );
                    }
                    else if( SerializeToSingle )
                    {
                        Single valS = (Single)bitValues[v];
                        bw.Write( valS );
                    }
                    else
                    {
                        bw.Write(bitValues[v]);
                    }
                }
            }
        }

        public void DeserializeBitValues( BinaryReader br, BitMask<EnumT> bMask )
        {
            if( bMask == null )
            {
                bitValues = null;
                return;
            }
            if( bitValues == null )
            {
                InitializeBitValues();
            }
            for(int v=0; v<bMask.Length; v++ )
            {
                bitValues[v] = 0;
                if(bMask[v])
                {
                    if( SerializeToByte )
                    {
                        bitValues[v] = (double) br.ReadByte()/255.0;
                    }
                    else if( SerializeToSingle )
                    {
                        bitValues[v] = (double) br.ReadSingle();
                    }
                    else
                    {
                        bitValues[v] = br.ReadDouble();
                    }
                }
            }
        }
        public override void Serialize( BinaryWriter bw )
        {
            UInt16  mask=0;
            SerializeToSingle = true;
            BitMask<EnumT> bitMask = null;
            
            if( Any )
            {
                bitMask = UpdateMask( minCutoff );
                mask |= 0x1;
            }
            if( SerializeToByte )
            {
                mask |= 0x2;
            }
            if( aggCount > 0 )
            {
                mask |= 0x4;
            }
            if( sampling > 0 )
            {
                mask |= 0x8;
            }
            if( minCutoff > 0 )
            {
                mask |= 0x10;
            }
            if( SerializeToSingle  )
            {
                mask |= 0x20;
            }
            bw.Write(mask);
            if( ( mask & 0x1 ) > 0 )
            {
                bitMask.Serialize( bw );
                SerializeBitValues( bw, bitMask );
            }
            if( (mask & 0x4 ) > 0 )
            {
                bw.Write(aggCount);
            }
            if( (mask & 0x8 ) > 0 )
            {
                bw.Write( (Single) sampling);
            }
            if( (mask & 0x10 ) > 0 )
            {
                bw.Write( (Single) minCutoff);
            }
        }

        public override void Deserialize( BinaryReader br )
        {
            Reset();
            UInt16 mask = br.ReadUInt16();
            SerializeToByte = ((mask & 0x2 ) > 0 );
            SerializeToSingle = ((mask & 0x20 ) > 0 );
            if( ( mask & 0x1 ) > 0 )
            {
                BitMask<EnumT> bitMask = new BitMask<EnumT>();
                bitMask.Deserialize( br );
                DeserializeBitValues( br, bitMask );
            }
            if( (mask & 0x4 ) > 0 )
            {
                aggCount = br.ReadUInt64();
            }
            if( (mask & 0x8 ) > 0 )
            {
                sampling = (double) br.ReadSingle();
            }
            if( (mask & 0x10 ) > 0 )
            {
                minCutoff = (double) br.ReadSingle();
            }
        }


        public static double[] MergeValues(int Len, double[] left, double[] right )
        {
            if( left == null )  return right;
            if( right == null ) return left;

            for(int v=0; v<Len; v++ )
            {
                left[v] = Math.Max( left[v], right[v] );
            }
            return left;
        }

        public static BitMaskValues<EnumT> Merge( BitMaskValues<EnumT> left, BitMaskValues<EnumT> right )
        {
            if( left == null )  return right;
            if( right == null ) return left;

            left.SerializeToByte |= right.SerializeToByte;
            left.bitValues = BitMaskValues<EnumT>.MergeValues( BitMask<EnumT>.GetLength(), left.bitValues, right.bitValues );
            
            left.aggCount = Math.Max( left.aggCount, right.aggCount );
            left.sampling = Math.Max( left.sampling, right.sampling );
            return left;
        }

        public static int enumSize_QueryIntentType = (int)QueryIntentType.NumTypes;
        public static int enumSize_MarketSegType = (int)MarketSegType.NumTypes;
        public static int enumSize_FileFormatType = (int)FileFormatType.NumTypes;
        public static int enumSize_VisitType = (int)VisitType.NumTypes;
        public static int enumSize_PageLayoutType = (int)PageLayoutType.NumTypes;

        public static BitMaskValues<EnumT> Merge( BitMaskValues<EnumT> left, BitMaskValues<EnumT> right, int segType )
        {
            if( left == null )  return right;
            if( right == null ) return left;

            left.SerializeToByte |= right.SerializeToByte;

            int segTypeLen = 0;

            if (segType == (int)SegmentGroupType.QueryIntentType)
            {
                segTypeLen = enumSize_QueryIntentType;
            }
            else if (segType == (int)SegmentGroupType.MarketSegType)
            {
                segTypeLen = enumSize_MarketSegType;
            }
            else if (segType == (int)SegmentGroupType.FileFormatType)
            {
                segTypeLen = enumSize_FileFormatType;
            }
            else if (segType == (int)SegmentGroupType.VisitType)
            {
                segTypeLen = enumSize_VisitType;
            }
            else if (segType == (int)SegmentGroupType.PageLayoutType)
            {
                segTypeLen = enumSize_PageLayoutType;
            }

            left.bitValues = BitMaskValues<EnumT>.MergeValues( segTypeLen, left.bitValues, right.bitValues );
            
            left.aggCount = Math.Max( left.aggCount, right.aggCount );
            left.sampling = Math.Max( left.sampling, right.sampling );
            return left;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            if( aggCount > 0 )
            {
                sb.Append( string.Format( "{0}={1:N}, ", "SeedUrlNum", aggCount ) );
            }
            if( sampling > 0 )
            {
                sb.Append( string.Format( "{0}={1:N5}, ", "SeedDensity", sampling ) );
            }
            if( minCutoff > 0 )
            {
                sb.Append( string.Format( "{0}={1:N5}, ", "MinVal", minCutoff ) );
            }

            if( bitValues != null )
            {
                for(int b=0; b<bitValues.Length; b++ )
                {
                    if( bitValues[b] > 0.01 )
                    {
                        EnumT val = BitMask<EnumT>.GetValEnum( b );
                        sb.Append( string.Format( "{0}={1:N3}, ", val.ToString(), bitValues[b] ) );
                    }
                }
            }
            
            return  sb.ToString();
        }
    }
    
    ///<summary>
    /// A class to encapsulate all segments related BIT flags info for a Url
    ///</summary>
    public class SegmentType : SerializableObject
    {

        public BitMask<MarketSegType> markets = new BitMask<MarketSegType>();
        public BitMask<QueryIntentType> querySegs = new BitMask<QueryIntentType>();
        public BitMask<FileFormatType> fileFormats = new BitMask<FileFormatType>();
        public BitMask<PageLayoutType> pageLayouts = new BitMask<PageLayoutType>();
        public UrlSourceType referrers = UrlSourceType.Unknown;
        public BitMask<VisitType>  referrersmask = null;

        public SegmentType()
        {
        }

        public BitMask<VisitType> referrersMask
        {
            get
            {
                if(referrersmask == null )
                {
                    referrersmask = new BitMask<VisitType>();
                }
                else
                {
                    referrersmask.Reset();
                }
                UInt32 bits = (UInt32) referrers;
                if( bits != 0 )
                {
                    for( int bit = 1; (bit < (int)VisitType.NumTypes) && (bits > 0 ) ; bit++ )
                    {
                        if( ( bits & 0x1 ) > 0  )
                        {
                            referrersmask[ bit ] = true;
                        }
                        bits = bits >> 1;
                    }
                }
                return referrersmask;
            }
        }
        public void SetSourceType( string sourceTypeStr )
        {
            UrlSourceType srcType = (UrlSourceType) Enum.Parse( typeof(UrlSourceType), sourceTypeStr, true );
            referrers |= srcType ;
        }

        public void SetSourceType( UrlSourceType srcType )
        {
            referrers |= srcType ;
        }

        public static SegmentType Merge( SegmentType left, SegmentType right )
        {
            if( left == null )  return right;
            if( right == null ) return left;

            left.markets = BitMask<MarketSegType>.Merge( left.markets, right.markets );
            left.querySegs = BitMask<QueryIntentType>.Merge( left.querySegs, right.querySegs );
            left.fileFormats = BitMask<FileFormatType>.Merge( left.fileFormats, right.fileFormats );
            left.pageLayouts = BitMask<PageLayoutType>.Merge( left.pageLayouts, right.pageLayouts );
            left.referrers =  left.referrers | right.referrers ;

            return left;
        }

        public static SegmentType MergeAnd( SegmentType left, SegmentType right )
        {
            if( left == null )  return right;
            if( right == null ) return left;

            left.markets = BitMask<MarketSegType>.MergeAnd( left.markets, right.markets );
            left.querySegs = BitMask<QueryIntentType>.MergeAnd( left.querySegs, right.querySegs );
            left.fileFormats = BitMask<FileFormatType>.MergeAnd( left.fileFormats, right.fileFormats );
            left.pageLayouts = BitMask<PageLayoutType>.MergeAnd( left.pageLayouts, right.pageLayouts );
            left.referrers =  left.referrers & right.referrers ;

            return left;
        }

        public void Reset()
        {
            markets.Reset() ;
            querySegs.Reset() ;
            fileFormats.Reset() ;
            pageLayouts.Reset() ;
            referrers = UrlSourceType.Unknown;;
        }
        public override String ToString()
        {
            StringBuilder sb = new StringBuilder();
            if( markets.Any )
            {
                sb.Append(String.Format("Market:{0}",markets.ToString()) );
            }
            if( querySegs.Any )
            {
                sb.Append(String.Format(",QuerySegment:{0}",querySegs.ToString()) );
            }
            if( fileFormats.Any )
            {
                sb.Append(String.Format(",FileFormat:{0}",fileFormats.ToString()) );
            }
            if( pageLayouts.Any )
            {
                sb.Append(String.Format(",PageLayout:{0}",pageLayouts.ToString()) );
            }
            if( referrers != UrlSourceType.Unknown)
            {
                sb.Append(String.Format(",Referrer:{0}",referrers.ToString()) );
            }


            return sb.ToString();
        }

        public bool Equals( SegmentType right )
        {
            if( right == null )     return false;
            return( this.ToString() == right.ToString() );
        }

        public bool Any
        {   
            get 
            {
                return ( markets.Any || querySegs.Any || fileFormats.Any || pageLayouts.Any || ( referrers != UrlSourceType.Unknown ) );
            }
        }

        
        public override void Serialize( BinaryWriter bw )
        {
            // save last bit for mask2 xtn
            UInt16 mask = 0;
            if( markets.Any )
            {
                mask |= 0x1;
            }
            if( querySegs.Any )
            {
                mask |= 0x2;
            }
            if( fileFormats.Any )
            {
                mask |= 0x4;
            }
            if( pageLayouts.Any )
            {
                mask |= 0x8;
            }
            if( referrers != UrlSourceType.Unknown )
            {
                mask |= 0x10;
            }
            bw.Write(mask);
            //Console.WriteLine( String.Format( "Szn mask :{0:X}", mask ));

            if( (mask & 0x1) > 0 )
            {
                markets.Serialize(bw);
            }
            if( (mask & 0x2) > 0 )
            {
                querySegs.Serialize(bw);
            }
            if( (mask & 0x4) > 0 )
            {
                fileFormats.Serialize(bw);
            }
            if( (mask & 0x8) > 0 )
            {
                pageLayouts.Serialize(bw);
            }
            if( (mask & 0x10) > 0 )
            {
                bw.Write((UInt32) referrers);
            }
        }

        public override void Deserialize( BinaryReader br )
        {
            UInt16 mask = br.ReadUInt16();
            //Console.WriteLine( String.Format( "DeSzn mask :{0:X}", mask ));
            if( (mask & 0x1) > 0 )
            {
                markets.Deserialize(br);
            }
            if( (mask & 0x2) > 0 )
            {
                querySegs.Deserialize(br);
            }
            if( (mask & 0x4) > 0 )
            {
                fileFormats.Deserialize(br);
            }
            if( (mask & 0x8) > 0 )
            {
                pageLayouts.Deserialize(br);
            }
            if( (mask & 0x10) > 0 )
            {
                referrers = (UrlSourceType) br.ReadUInt32();
            }
        }
    }

    ///<summary>
    /// A class to encapsulate all segments related scores info for a Url/ Node
    ///</summary>
    public class SegmentScores : SerializableObject
    {

        public BitMaskValues<MarketSegType> markets = new BitMaskValues<MarketSegType>();
        public BitMaskValues<QueryIntentType> querySegs = new BitMaskValues<QueryIntentType>();
        public BitMaskValues<FileFormatType> fileFormats = new BitMaskValues<FileFormatType>();
        public BitMaskValues<PageLayoutType> pageLayouts = new BitMaskValues<PageLayoutType>();
        public BitMaskValues<VisitType> referrers = new BitMaskValues<VisitType>();
        public UInt64 aggCount = 0;

        public SegmentScores()
        {
        }

        public UInt64 AggCount
        {
            get 
            {
                return aggCount;
            }
        }

        public double GetScore(int segmentGroupID, int segmentID)
        {
            try
            {
                SegmentGroupType segGroup = (SegmentGroupType) segmentGroupID;
                switch( segGroup )
                {
                    case SegmentGroupType.MarketSegType : return markets[segmentID];
                    case SegmentGroupType.QueryIntentType : return querySegs[segmentID];
                    case SegmentGroupType.VisitType: return referrers[segmentID];
                    case SegmentGroupType.FileFormatType: return fileFormats[segmentID];
                    case SegmentGroupType.PageLayoutType : return pageLayouts[segmentID];
                    default : break;
                }
            }
            catch(Exception)
            {
            }

            return 0D;
        }

        public bool SetScore(int segmentGroupID, int segmentID, double segScore)
        {
            bool isDone = true;
            
            try
            {
                SegmentGroupType segGroup = (SegmentGroupType) segmentGroupID;
                switch( segGroup )
                {
                    case SegmentGroupType.MarketSegType : 
                        markets[segmentID] = segScore;
                        break;
                    case SegmentGroupType.QueryIntentType : 
                        querySegs[segmentID] = segScore;
                        break;
                    case SegmentGroupType.VisitType: 
                        referrers[segmentID] = segScore;
                        break;
                    case SegmentGroupType.FileFormatType: 
                        fileFormats[segmentID] = segScore;
                        break;
                    case SegmentGroupType.PageLayoutType : 
                        pageLayouts[segmentID] = segScore;
                        break;
                    default : 
                        isDone = false;
                        break;
                }
            }
            catch(Exception)
            {
                isDone = false;
            }

            return isDone;
        }

        public void Scale( double factor )
        {
            SerializeAllToByte(false);
            markets.Scale(factor);
            querySegs.Scale(factor);
            fileFormats.Scale(factor);
            pageLayouts.Scale(factor);
            referrers.Scale(factor);
        }

        public void SerializeAllToByte( bool enable )
        {
            markets.SerializeToByte = enable;
            querySegs.SerializeToByte = enable;
            fileFormats.SerializeToByte = enable;
            pageLayouts.SerializeToByte = enable;
            referrers.SerializeToByte = enable;
        }

        public void Aggregate( SegmentType SegmentBits )
        {
            if(SegmentBits == null )    return;
            markets.Aggregate( SegmentBits.markets );
            querySegs.Aggregate( SegmentBits.querySegs );
            fileFormats.Aggregate( SegmentBits.fileFormats );
            pageLayouts.Aggregate( SegmentBits.pageLayouts );
            referrers.Aggregate( SegmentBits.referrersMask );
            aggCount ++;
        }

        public void Aggregate( SegmentScores SegmentCounts )
        {
            if(SegmentCounts == null )    return;
            markets.Aggregate( SegmentCounts.markets );
            querySegs.Aggregate( SegmentCounts.querySegs );
            fileFormats.Aggregate( SegmentCounts.fileFormats );
            pageLayouts.Aggregate( SegmentCounts.pageLayouts );
            referrers.Aggregate( SegmentCounts.referrers );
            aggCount += SegmentCounts.aggCount;
        }

        public void ApplyMinCutoff( SegmentScores SegmentCutOff )
       {

            if(SegmentCutOff == null )    return;
            markets.ApplyMinCutoff( SegmentCutOff.markets );
            querySegs.ApplyMinCutoff( SegmentCutOff.querySegs );
            fileFormats.ApplyMinCutoff( SegmentCutOff.fileFormats );
            pageLayouts.ApplyMinCutoff( SegmentCutOff.pageLayouts );
            referrers.ApplyMinCutoff( SegmentCutOff.referrers );

       }

        public void ApplyMinCutoff( double cutOff )
        {

            markets.ApplyMinCutoff( cutOff );
            querySegs.ApplyMinCutoff( cutOff );
            fileFormats.ApplyMinCutoff( cutOff );
            pageLayouts.ApplyMinCutoff( cutOff );
            referrers.ApplyMinCutoff( cutOff );

        }

        public void Scale( SegmentScores SegmentCounts )
        {
            if(SegmentCounts == null )    return;
            SerializeAllToByte(false);
            markets.Scale( SegmentCounts.markets );
            querySegs.Scale( SegmentCounts.querySegs );
            fileFormats.Scale( SegmentCounts.fileFormats );
            pageLayouts.Scale( SegmentCounts.pageLayouts );
            referrers.Scale( SegmentCounts.referrers );
            
        }

        public void Update( )
        {
            Update( aggCount, 0.001, 2 );
        }

        public void Update( UInt64 urlCount )
        {
            Update( urlCount, 0.001, 2 );
        }

        public void Update( UInt64 urlCount, double minSamplingReq, double penaltyFactor )
        {
            //minSamplingReq to be read from config (InitSampling)
            markets.Update( urlCount, minSamplingReq*10, penaltyFactor  );
            querySegs.Update( urlCount, minSamplingReq, penaltyFactor );
            fileFormats.Update( urlCount, minSamplingReq*10, penaltyFactor );
            pageLayouts.Update( urlCount, minSamplingReq, penaltyFactor );
            referrers.Update( urlCount, minSamplingReq*10, penaltyFactor );
        }

        public void Propagate( SegmentScores SegmentCountsAtPrior, double minSamplingReq )
        {
            markets.Propagate( SegmentCountsAtPrior.markets, minSamplingReq*10 );
            querySegs.Propagate( SegmentCountsAtPrior.querySegs, minSamplingReq );
            fileFormats.Propagate( SegmentCountsAtPrior.fileFormats, minSamplingReq *10);
            pageLayouts.Propagate( SegmentCountsAtPrior.pageLayouts, minSamplingReq );
            referrers.Propagate( SegmentCountsAtPrior.referrers, minSamplingReq*10 );
        }

        public void Update( UInt64 urlCount, BitMaskValues<SegmentGroupType> minSamplingReq, double penaltyFactor )
        {
            //minSamplingReq to be read from config (InitSampling)
            markets.Update( urlCount, minSamplingReq[(int)SegmentGroupType.MarketSegType], penaltyFactor  );
            querySegs.Update( urlCount, minSamplingReq[(int)SegmentGroupType.QueryIntentType], penaltyFactor );
            fileFormats.Update( urlCount, minSamplingReq[(int)SegmentGroupType.FileFormatType], penaltyFactor );
            pageLayouts.Update( urlCount, minSamplingReq[(int)SegmentGroupType.PageLayoutType], penaltyFactor );
            referrers.Update( urlCount, minSamplingReq[(int)SegmentGroupType.VisitType], penaltyFactor );
        }

        public void Propagate( SegmentScores SegmentCountsAtPrior, BitMaskValues<SegmentGroupType> minSamplingReq )
        {
            markets.Propagate( SegmentCountsAtPrior.markets, minSamplingReq[(int)SegmentGroupType.MarketSegType] );
            querySegs.Propagate( SegmentCountsAtPrior.querySegs, minSamplingReq[(int)SegmentGroupType.QueryIntentType] );
            fileFormats.Propagate( SegmentCountsAtPrior.fileFormats, minSamplingReq[(int)SegmentGroupType.FileFormatType]);
            pageLayouts.Propagate( SegmentCountsAtPrior.pageLayouts, minSamplingReq[(int)SegmentGroupType.PageLayoutType] );
            referrers.Propagate( SegmentCountsAtPrior.referrers, minSamplingReq[(int)SegmentGroupType.VisitType] );
        }

        
        public static SegmentScores Merge( SegmentScores left, SegmentScores right )
        {
            if( left == null )  return right;
            if( right == null ) return left;

            left.markets = BitMaskValues<MarketSegType>.Merge(left.markets, right.markets, (int)SegmentGroupType.MarketSegType);
            left.querySegs = BitMaskValues<QueryIntentType>.Merge(left.querySegs, right.querySegs, (int)SegmentGroupType.QueryIntentType);
            left.fileFormats = BitMaskValues<FileFormatType>.Merge(left.fileFormats, right.fileFormats, (int)SegmentGroupType.FileFormatType);
            left.pageLayouts = BitMaskValues<PageLayoutType>.Merge(left.pageLayouts, right.pageLayouts, (int)SegmentGroupType.PageLayoutType);
            left.referrers =  BitMaskValues<VisitType>.Merge(left.referrers, right.referrers, (int)SegmentGroupType.VisitType);

            //Revisit this step on use case basis   
            left.aggCount = Math.Max(left.aggCount, right.aggCount);
            return left;
        }

        public void NormalizeByAggCount()
        {
            markets.NormalizeByAggCount();
            querySegs.NormalizeByAggCount();
            fileFormats.NormalizeByAggCount();
            pageLayouts.NormalizeByAggCount();
            referrers.NormalizeByAggCount();
        }

        public void Reset()
        {
            markets.Reset();
            querySegs.Reset();
            fileFormats.Reset();
            pageLayouts.Reset();
            referrers.Reset();
            aggCount = 0;
        }
        public override String ToString()
        {
            StringBuilder sb = new StringBuilder();
            
            if( aggCount > 0)
            {
                sb.Append(String.Format("TotalSeedNum:{0:N3}",aggCount) );
            }

            if( markets.Any )
            {
                sb.Append(String.Format(",Market:{{{0}}}",markets.ToString()) );
            }
            if( querySegs.Any )
            {
                sb.Append(String.Format(",QuerySegment:{{{0}}}",querySegs.ToString()) );
            }
            if( fileFormats.Any )
            {
                sb.Append(String.Format(",FileFormat:{{{0}}}",fileFormats.ToString()) );
            }
            if( pageLayouts.Any )
            {
                sb.Append(String.Format(",PageLayout:{{{0}}}",pageLayouts.ToString()) );
            }
            if( referrers.Any)
            {
                sb.Append(String.Format(",Referrer:{{{0}}} ",referrers.ToString()) );
            }


            return sb.ToString();
        }

        public bool Equals( SegmentScores right )
        {
            if( right == null )     return false;
            return( this.ToString() == right.ToString() );
        }

        public bool Any
        {   
            get 
            {
                return ( markets.Any || querySegs.Any || fileFormats.Any || pageLayouts.Any || ( referrers.Any ) );
            }
        }

        
        public override void Serialize( BinaryWriter bw )
        {
            // save last bit for mask2 xtn
            UInt16 mask = 0;
            if( markets.Any )
            {
                mask |= 0x1;
            }
            if( querySegs.Any )
            {
                mask |= 0x2;
            }
            if( fileFormats.Any )
            {
                mask |= 0x4;
            }
            if( pageLayouts.Any )
            {
                mask |= 0x8;
            }
            if( referrers.Any )
            {
                mask |= 0x10;
            }

            if( aggCount > 0 )
            {
                mask |= 0x20;
            }
            
            bw.Write(mask);


            if( (mask & 0x1) > 0 )
            {
                markets.Serialize(bw);
            }
            if( (mask & 0x2) > 0 )
            {
                querySegs.Serialize(bw);
            }
            if( (mask & 0x4) > 0 )
            {
                fileFormats.Serialize(bw);
            }
            if( (mask & 0x8) > 0 )
            {
                pageLayouts.Serialize(bw);
            }
            if( (mask & 0x10) > 0 )
            {
                referrers.Serialize(bw);
            }
            if( (mask & 0x20 ) > 0 )
            {
                bw.Write(aggCount);
            }
        }

        public override void Deserialize( BinaryReader br )
        {
            Reset();
            UInt16 mask = br.ReadUInt16();
            //Console.WriteLine( String.Format( "DeSzn mask :{0:X}", mask ));
            if( (mask & 0x1) > 0 )
            {
                markets.Deserialize(br);
            }
            if( (mask & 0x2) > 0 )
            {
                querySegs.Deserialize(br);
            }
            if( (mask & 0x4) > 0 )
            {
                fileFormats.Deserialize(br);
            }
            if( (mask & 0x8) > 0 )
            {
                pageLayouts.Deserialize(br);
            }
            if( (mask & 0x10) > 0 )
            {
                referrers.Deserialize(br);
            }
            if( (mask & 0x20 ) > 0 )
            {
                aggCount = br.ReadUInt64();
            }
        }
    }

    public static class SegmentUtils
    {
        /// a penalty on sample mean based on signal sampling, whenever less than min
        /// FeatureEstimate * SamplingPenalty
        /// PLOT  http://www.wolframalpha.com/input/?i=exp%28-2*%281-y%2F0.01%29%29+with+y+from+0+to+0.01
        public static double SampleEstimateAdjustFactor( double samplingActual , double minSamplingReq , double penaltyFactor)
        {
            minSamplingReq = Math.Max( minSamplingReq, 1e-6 );
            if(samplingActual > minSamplingReq ) return 1.0D;
            
            double diffSampling =  (1 - samplingActual/minSamplingReq);
            return Math.Exp( -penaltyFactor*diffSampling );
            
        }

        public static double SampleEstimateAdjustFactor(double samplingActual )
        {
            return SampleEstimateAdjustFactor( samplingActual, 0.01, 2 );
        }

        /// Est(feature) * alpha + Est(feature-regulalizer)* (1-alpha )
        /// Graph Eg: Est(featureAtNode) * alpha + Est(featureAtParentNode)* (1-alpha )
        /// for default alpha = 0.5 ==> pass (0.5, 1, 1 )
        /// whenever sampling is more than min, alpha is 1 ==. NO WEIGHTING
        public static double WeightedBySampling( double samplingActual, double samplingAtPrior, double minSamplingReq )
        {
            if( samplingActual >= minSamplingReq )
            {
                return 1D;
            }
            minSamplingReq = Math.Max( minSamplingReq, 1e-6 );
            samplingActual = Math.Min( minSamplingReq, Math.Max( samplingActual, minSamplingReq*1e-6 ));
            samplingAtPrior = Math.Min( minSamplingReq, Math.Max( samplingAtPrior, minSamplingReq*1e-6 ));
            return samplingActual/Math.Min(samplingAtPrior+samplingActual, minSamplingReq);
        }

       public static double WeightedBySampling( double samplingActual, double samplingAtPrior)
       {
           return WeightedBySampling( samplingActual, samplingAtPrior, 0.01 );
       }

       public static bool IsUrlBlogClassifier( string Domain, string HostSuffix, string L1Suffix, string UrlSuffix )
       {
            //TBD expand to top 10 blog hosts
            return ( (Domain.Contains("com.livejournal." ) )//&& !HostSuffix.Contains("www."))
                    || ( Domain.Contains("com.blogspot." ) )
                    || ( Domain.Contains("com.wordpress." ) )
                    || ( Domain.Contains("com.xanga." ) )
                    || ( Domain.Contains("com.typepad." ) )
                    || ( Domain.Contains("com.tumblr." ) )
                    );
       }

        public static double SegMinProbCutoff = 0;
        public static bool ConfigRead = false;
        private static Object ConfigInitLock = new Object();
        public static void InitSegmentConfigSettings(SegmentSelectionConfig SegmentsConfig ) 
        {
            lock( ConfigInitLock )
            {
                if(SegmentsConfig != null )
                {
                    SegMinProbCutoff = SegmentsConfig.getGlobalMinCutoff_SegLikelihood();
                }
                ConfigRead = true;
            }
        }
        
        public static SegmentScores SegmentsPreSelectionFilter( SegmentScores SegScores, SegmentSelectionConfig SegmentsConfig ) 
        {
            if(SegScores == null)   return null;

            if(SegmentsConfig == null)
            {
                throw new Exception( "SegmentsConfig is not defined" );
            }

            if(!ConfigRead)
            {
                InitSegmentConfigSettings(SegmentsConfig);
            }

            //only enabled segs have 1 and others have 0, hence removed
            // element by element multiplication  filter  a.*b
            SegScores.Scale(SegmentsConfig.SegEnabled);

            // this filter is to reduce "candidates" for segments selection
            // we should also use histogram of P(url/seg) to decide select top N urls per segment as candidates for allocation
            SegScores.ApplyMinCutoff(SegmentsConfig.SegLikelihoodCutoff);

            SegScores.SerializeAllToByte(true);
            
            return SegScores;
        }
       
    }

    // Switch to Bond eventually
    // Serializable versions of built in data types
    // types added upon requirement
    public class SerialDataType<T> : SerializableObject
                where T : struct, IComparable, IFormattable, IConvertible
    {
        private T val;
        private Type valType = typeof(T);



        public T Value
        {
            get
            {
                return val;
            }

            set
            {
                val = (T) value;
            }
        }

        public override String ToString()
        {
            return val.ToString();
        }
        public override void Serialize( BinaryWriter bw )
        {
            if( val is UInt32 )
            {
                //bw.Write((UInt32) Convert.ChangeType( val, typeof(UInt32)));
                bw.Write( (UInt32) (Object) val );
            }
            else if( val is Single )
            {
                bw.Write( (Single) (Object) val );
            }
            else if( val is Byte )
            {
                bw.Write( (Byte) (Object) val );
            }
            else if( val is UInt16 )
            {
                bw.Write( (UInt16) (Object) val );
            }
            else
            {
                throw new Exception( "SerialTypes : Szn : Undefined data type: " + valType );
            }
        }

        public override void Deserialize( BinaryReader br )
        {

            if( val is UInt32 )
            {
                //val = (T) Convert.ChangeType( br.ReadUInt32() , valType);
                val = (T) (Object) br.ReadUInt32() ;
            }
            else if( val is Single )
            {
                val = (T) (Object) br.ReadSingle() ;
            }
            else if( val is Byte )
            {
                val = (T) (Object) br.ReadByte() ;
            }
            else if( val is UInt16 )
            {
                val = (T) (Object) br.ReadUInt16() ;
            }
            else
            {
                throw new Exception( "SerialTypes : DeSzn : Undefined data type: " + valType );
            }
        }
        
    }





}
