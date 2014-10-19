// String handling.
//
//
// Chris Burges, (c) Microsoft Corporation 2004
using System;
using System.IO;
using System.Collections;


namespace Microsoft.TMSN
{
	public struct StringCount
	{
		public readonly string String;
		public readonly int    Count;

		public StringCount(string str, int count)
		{
			String = str;
			Count = count;
		}

		public static implicit operator string(StringCount sc)
		{
			return sc.String;
		}

		public static implicit operator int(StringCount sc)
		{
			return sc.Count;
		}
	}

	public sealed class StringUtils
	{
		/// <summary>
		/// Check if a string is representing a number.
		/// </summary>
		public static bool IsANumber(string s)
		{
			if (s == null || s.Length == 0)  return false;
			bool digitFound = false;
			bool decFound = false;
			for (int i = 0; i < s.Length; i++)
			{
				if (s[i] >= '0' && s[i] <= '9')
				{
					digitFound = true;
				}
				else
				{
					if (s[i] != ',')
					{
						if (s[i] != '.' || decFound)  return false;
						decFound = true;
					}
				}
			}
			return digitFound;
		}

	}
}
