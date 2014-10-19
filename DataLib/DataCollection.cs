using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.TMSN;

namespace Microsoft.TMSN
{
    public interface IDataEnum<D, T, P> : IEnumerable
        where D : IData
        where P : IDataProc<D, T>, new()
    {
    }

    public class TsvDataStream<D, T, P> : IDataEnum<D, T, P>, ICloneable
        where D : IData
        where P : IDataProc<D, T>, new()
    {
        protected RankingTSVFile<D> m_tsvFile;
        protected IDataProc<D, T> m_dataProc;
        public TsvDataStream(RankingTSVFile<D> tsvFile, IDataProc<D, T> dataProc)
        {
            m_tsvFile = tsvFile;
            m_dataProc = dataProc;
        }

        public IEnumerator GetEnumerator()
        {
            return new DataEnumerator(m_tsvFile, m_dataProc);
        }

        private class DataEnumerator : IEnumerator
        {
            private readonly IEnumerator m_groupFieldsEnumerator;
            protected IDataProc<D, T> m_dataProc;

            bool firstMoveNext = true;
            bool lastMoveNext = false;

            private D m_data;

            public DataEnumerator(RankingTSVFile<D> tsvFile, IDataProc<D, T> dataProc)
            {
                m_groupFieldsEnumerator = tsvFile.GetGroupEnumerator();
                m_dataProc = dataProc;
            }

            public void Reset()
            {
                m_groupFieldsEnumerator.Reset();
                firstMoveNext = true;
                lastMoveNext = false;
                if (m_data != null)
                    m_data.Reset();
            }

            object IEnumerator.Current
            {
                get
                {
                    return m_dataProc.Process(m_data);
                }
            }

            public bool MoveNext()
            {
                if (lastMoveNext)
                {
                    if (m_data != null)
                        m_data.Reset();
                    return false;
                }

                if (firstMoveNext)
                {
                    bool success = m_groupFieldsEnumerator.MoveNext();
                    if (!success)
                        return false;
                    firstMoveNext = false;
                }

                m_data = (D)((D)m_groupFieldsEnumerator.Current).Clone();
                if (!m_groupFieldsEnumerator.MoveNext())
                {
                    lastMoveNext = true;
                }

                /*
                 m_groupBoundary.FirstItem(fields);                							
                
                do
                {                   
                    m_data.ProcessLine(fields);  
                    if (!fieldsEnumerator.MoveNext())
                    {
                        // That was the last row, so this set is the last we'll be returning
                        lastMoveNext = true;                                                
                        break;
                    }
                    fields = (string[])fieldsEnumerator.Current;
                } while (!m_groupBoundary.NewGroup(fields));

                m_data.Complete();
                */

                return true;
            }
        }

        public object Clone()
        {
            return new TsvDataStream<D, T, P>(this);
        }

        protected TsvDataStream(TsvDataStream<D, T, P> dataStream)
        {
            m_tsvFile = dataStream.m_tsvFile;
            m_dataProc = dataStream.m_dataProc;
        }
    }

    /// <summary>
    /// Interface for shuffleable objects
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class IShuffle<T>
    {      
        protected T[] m_allData = null;

        /// <summary>
        /// only shuffle the objects in the current collection
        /// </summary>
        /// <param name="rand"></param>
        public virtual void ShuffleShallow(Random rand)
        {
            if (rand == null) return;
            int nonemptyCount = m_allData.Length;
            // Shuffle the order of the queries
            // only shuffle the non-empty ones? **
            for (int i = 0; i < nonemptyCount; i++)
            {
                int swapWith = rand.Next(nonemptyCount);
                if (swapWith != i)
                {
                    T tmp = m_allData[swapWith];
                    m_allData[swapWith] = m_allData[i];
                    m_allData[i] = tmp;
                }
            }
        }

        /// <summary>
        /// recursively applying shuffle to all shuffable objects in the current collection
        /// </summary>
        /// <param name="rand"></param>
        public virtual void Shuffle(Random rand)
        {
            ShuffleShallow(rand);
            int nonemptyCount = m_allData.Length;
            for (int i = 0; i < nonemptyCount; i++)
            {
                if (m_allData[i] is IShuffle<T>)
                {
                    //compilation error???
                    //((IShuffle<T>)m_allData[i]).Shuffle(rand);
                }
            }
        }

    }
        
    public class TsvDataCollection<D, T, P> : IShuffle<T>, IDataEnum<D, T, P>, ICloneable
        where D : IData
        where P : IDataProc<D, T>, new()
    {       
        protected RankingTSVFile<D> m_tsvFile;
        protected IDataProc<D, T> m_dataProc;
        public TsvDataCollection(RankingTSVFile<D> tsvFile, IDataProc<D, T> dataProc)
        {
            m_tsvFile = tsvFile;
            m_dataProc = dataProc;

#if IM1
            m_allData = new T[1024];                        
            int count = 0;
            foreach (D data in tsvFile.GroupFieldsEnumeration)
            {
                if (count == m_allData.Length)
                {
                    T[] old = m_allData;
                    m_allData = new T[old.Length + (old.Length >> 1)];
                    Array.Copy(old, 0, m_allData, 0, old.Length);
                }
                m_allData[count] = m_dataProc.Process(data);
                
                count++;                
            }

            m_allData = m_allData[0, count]; 
#else
            ArrayList aList = new ArrayList();
            foreach (D data in tsvFile.GroupFieldsEnumeration)
            {

                aList.Add((T)(m_dataProc.Process(data)));
            }
            m_allData = (T[])(aList.ToArray(typeof(T)));

#endif //IM1
        }

        public IEnumerator GetEnumerator()
        {
            return m_allData.GetEnumerator();
        }

        public virtual T[] RawData
        {
            get
            {
                return m_allData;
            }
        }

        public virtual int Count
        {
            get
            {
                return m_allData.Length;
            }
        }

        public object Clone()
        {
            return new TsvDataCollection<D, T, P>(m_tsvFile, m_dataProc);
        }
    }    
}