using System;

namespace DiscoverySelection.Shared
{
    [Flags]
    public enum LanguageCode : uint
    {
        None = 0,
        EN = 0x1,
        JA = 0x2,
        PT = 0x4,
        FR = 0x8,
        ES = 0x10,
        IT = 0x20,
        TR = 0x40,
        DE = 0x80,
        TH = 0x100,
        NL = 0x200,
        ZH = 0x400,
        RU = 0x800,
        AR = 0x1000,
        PL = 0x2000,
        SV = 0x4000,
        DA = 0x8000,
        RO = 0x10000,
        EL = 0x20000,
        VI = 0x40000,
        HU = 0x80000,
        FI = 0x100000,
        IW = 0x200000,
        BG = 0x400000,
        NO = 0x800000,
        LT = 0x1000000,
        SK = 0x2000000,
        CS = 0x4000000,
        HR = 0x8000000,
        NB = 0x10000000,
        KO = 0x20000000,
        SL = 0x40000000,
        Other = 0x80000000
    };

    [Flags]
    public enum CountryCode : uint
    {
        None = 0,
        US = 0x1,
        JP = 0x2,
        BR = 0x4,
        FR = 0x8,
        GB = 0x10,
        ES = 0x20,
        MX = 0x40,
        IT = 0x80,
        TR = 0x100,
        DE = 0x200,
        CA = 0x400,
        WW = 0x800,
        TH = 0x1000,
        NL = 0x2000,
        AU = 0x4000,
        AR = 0x8000,
        TW = 0x10000,
        XL = 0x20000,
        RU = 0x40000,
        IN = 0x80000,
        SA = 0x100000,
        CN = 0x200000,
        PL = 0x400000,
        PT = 0x800000,
        CO = 0x1000000,
        XA = 0x2000000,
        HK = 0x4000000,
        SE = 0x8000000,
        DK = 0x10000000,
        BE = 0x20000000,
        CL = 0x40000000,
        Other = 0x80000000
    };
}
