using System;
using System.IO;
using System.Collections;
using Microsoft.TMSN.IO;

namespace Microsoft.TMSN
{    
    /// <summary>
    /// GroupBoundary is an generic interface used to detect the starting row/line for a new group
    /// It is used by TSV file to segement consequtive rows into different groups
    /// subclass this class to customarize/define any grouping behavior based on the comparison between
    /// consequtive lines in a tsv file
    /// </summary>
    public interface IGroupBoundary
    {
        /// <summary>
        /// Read header information to decide which columns in the input rows are used to 
        /// detect the goup boundary.
        /// In the query case, the QueryID column is used
        /// </summary>
        /// <param name="ColumnNames"></param>
        void ReadColumnNames(string[] columnNames);

        /// <summary>
        /// Set the starting line of the current group
        /// </summary>
        /// <param name="FirstLine"></param>
        void FirstItem(string[] FirstLine);

        /// <summary>
        /// Detect if the current line is the starting of a new group
        /// </summary>
        /// <param name="CurLine"></param>
        /// <returns></returns>
        bool NewGroup(string[] CurLine);
    }

    public class QueryBoundary : IGroupBoundary
    {
        private int m_groupColIdx = -1;
        private string m_FirstVal = null;
        private string m_GroupColumnName = "m:QueryId";

        public QueryBoundary()
        {
        }

        public QueryBoundary(string GroupColumnName)
        {
            m_GroupColumnName = GroupColumnName;            
        }
        
        /// <summary>
        /// Constructor uses header information to decide which columns in the input rows are used to 
        /// detect the goup boundary.
        /// In the query case, the QueryID column is used
        /// </summary>
        /// <param name="ColumnNames"></param>
        public void ReadColumnNames(string[] columnNames)
        {
            for (int i = 0; i < columnNames.Length; i++)
            {
                string featureName = columnNames[i];
                if (string.Compare(featureName, m_GroupColumnName, true) == 0)
                {
                    m_groupColIdx = i;
                    break;
                }
            }
        }

        /// <summary>
        /// Set the starting line of the current group
        /// </summary>
        /// <param name="FirstLine"></param>
        public void FirstItem(string[] FirstLine)
        {
            if (m_groupColIdx >= 0)
            {
                m_FirstVal = FirstLine[m_groupColIdx];
            }
        }

        /// <summary>
        /// Detect if the current line is the starting of a new group
        /// </summary>
        /// <param name="CurLine"></param>
        /// <returns></returns>
        public bool NewGroup(string[] CurLine)
        {
            if (m_groupColIdx < 0)
            {
                return false;
            }

            string curVal = CurLine[m_groupColIdx];
            return (!(string.CompareOrdinal(curVal, m_FirstVal) == 0));
        }        
    }

    /// <summary>
    /// a simple class that makes every line a group
    /// It may not have any real use except testing the coding
    /// </summary>
    public class OnelineGroup : IGroupBoundary
    {        
        public void ReadColumnNames(string[] columnNames)
        {
        }
        
        public void FirstItem(string[] FirstLine)
        {
        }

        public bool NewGroup(string[] CurLine)
        {
            return true;
        }
    }
}
