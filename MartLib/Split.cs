using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.TMSN;

namespace StochasticGradientBoost
{
    public class Split
    {
        public double gain;
        public int feature;
        public int iThresh;

        public Split()
        {
            Init();            
        }

        public void Init()
        {
            gain = 0.0F;
            feature = -1;
            iThresh = -1;
        }

        public static bool operator >(Split split1, Split split2)
        {
            return (split1.gain > split2.gain || //always pick better ones
                                   (split1.gain == split2.gain &&
                                       ((split1.feature < split2.feature) || // using features that have smaller id to break ties <=> features with smaller id is better??
                                        (split1.feature == split2.feature && split1.iThresh < split2.iThresh) // needs to reconsider
                                       )
                                   )
                               );            
        }

        public static bool operator <(Split split1, Split split2)
        {
            return !(split1.gain > split2.gain || //always pick better ones
                                   (split1.gain == split2.gain &&
                                       ((split1.feature < split2.feature) || // using features that have smaller id to break tides <=> features with smaller id is better??
                                        (split1.feature == split2.feature && split1.iThresh < split2.iThresh) // needs to reconsider
                                       )
                                   )
                               );
        }

        public void CopyTo(Split destSplit)
        {
            destSplit.gain = gain;
            destSplit.feature = feature;
            destSplit.iThresh = iThresh;
        }
    }

    /// <summary>
    /// Compute the best split for all data as specified by list {iList, iStartPre}
    /// and all features in [iStartFeature, iEndFeature]	
    /// </summary>
    public class FindSplitObj
    {
        //global data shared by all threads
        [NonSerialized] protected LabelFeatureDataCoded LabelFeatureDataCoded;
        [NonSerialized] protected float[] responses;
        [NonSerialized] protected int[] dataPoints;
        [NonSerialized] protected int[] workIndex;

        [NonSerialized] protected RandomSampler featureSampler;
        [NonSerialized] protected int iStart = -1;
        [NonSerialized] protected int iEnd = -1;

        [NonSerialized] protected RandomSampler dataSampler;

        [NonSerialized] int minNumSamples;

        //local variables for this thread to compute the best split
        [NonSerialized] protected double[] regionSum;
        [NonSerialized] protected int[] regionCount;
        //local variables used to prefetch the feature values
        [NonSerialized] int[] idxDataPrefetch;
        [NonSerialized] ushort[] dataCodedPrefetch;
        [NonSerialized] const int cSizePrefectch = 10000;

        //best split information: results of FindSplitThreadObj.Find
        [NonSerialized] public Split bestSplit;
        [NonSerialized] public Split curSplit;

        public FindSplitObj()
        {
            this.regionSum = new double[ushort.MaxValue + 1];
            this.regionCount = new int[ushort.MaxValue + 1];

            this.idxDataPrefetch = new int[FindSplitObj.cSizePrefectch];
            this.dataCodedPrefetch = new ushort[FindSplitObj.cSizePrefectch];
            
            bestSplit = new Split();
            curSplit = new Split();
        }

        public void SetData(LabelFeatureDataCoded LabelFeatureDataCoded, float[] responses,
                            int[] dataPoints, int[] workIndex,
                            int iStart, int iEnd, RandomSampler featureSampler,
                            RandomSampler dataSampler, int minNumSamples)
        {
            this.LabelFeatureDataCoded = LabelFeatureDataCoded;
            this.responses = responses;

            this.dataPoints = dataPoints;
            this.workIndex = workIndex;
                                    
            this.iStart = iStart;
            this.iEnd = iEnd;
            this.featureSampler = featureSampler;

            this.dataSampler = dataSampler;

            this.minNumSamples = minNumSamples;
        }		

        //for all the features [iStart, iEnd] find the best splitting index of the best feature
        //that has the most MSE gain
        virtual public void Find()
        {
            this.bestSplit.Init();

            for (int iIdx = iStart; iIdx < iEnd; ++iIdx)
            {
                int iFeature = this.featureSampler.idxLookup(iIdx);
                int len = this.LabelFeatureDataCoded.GetCodeRange(iFeature);
                if (this.dataPoints.Length >= this.minNumSamples && len > 1)
                {
                    for (int i = 0; i < len; i++)
                    {
                        this.regionSum[i] = 0;
                        this.regionCount[i] = 0;
                    }

                    unsafe
                    {
                        fixed (int* idxDataPrefetchPtr = this.idxDataPrefetch)
                        fixed (ushort* dataCodedPrefetchPtr = this.dataCodedPrefetch)
                        {
                            //int[] idxDataPrefetch = new int[FindSplitObj.cSizePrefectch];
                            //ushort[] dataCodedPrefetch = new ushort[FindSplitObj.cSize];
                            int cPrefetched = 0; //nothing has fetched
                            int j = 0; //the current element in the prefetched array

                            Debug.Assert(this.dataSampler.TotalSize == dataPoints.Length, "Wrong data sampler");

                            for (int i = 0; i < this.dataSampler.SampleSize; i++)
                            {
                                if (j >= cPrefetched)
                                {
                                    //pre-fetching the data of the next cData points

                                    //(1) how many data points to prefetch
                                    cPrefetched = (i + FindSplitObj.cSizePrefectch) > dataPoints.Length ? (dataPoints.Length - i) : FindSplitObj.cSizePrefectch;

                                    //(2) the actual index of these data points
                                    for (int k = 0; k < cPrefetched; k++)
                                    {                                        
                                        idxDataPrefetchPtr[k] = this.workIndex[dataPoints[this.dataSampler.idxLookup(i + k)]];
                                    }

                                    //(3) getting them
                                    this.LabelFeatureDataCoded.GetFeatureCoded(iFeature, this.idxDataPrefetch, this.dataCodedPrefetch, cPrefetched);

                                    //reset the current element in the prefetched array
                                    j = 0;
                                }

                                int loc = dataCodedPrefetchPtr[j]; // this.LabelFeatureDataCoded.GetFeatureCoded(iFeature, ind);
                                this.regionCount[loc]++;
                                float resp = this.responses[idxDataPrefetchPtr[j]];
                                this.regionSum[loc] += resp;
                                j++;
                            }
                        }
                    }                
                    
                    // if split on boundary of bin 0 => bin 0 goes left, bin 1, ... goes right; split on boundary of bin 1 => bin 0, 1 left and bin 2, ... go right 
                    for (int i = 1; i < len; i++)
                    {
                        regionSum[i] += regionSum[i - 1];
                        regionCount[i] += regionCount[i - 1];
                    }

                    double total = regionSum[len - 1] * regionSum[len - 1] / regionCount[len - 1];
                    
                    for (int i = 0; i < len - 1; i++)
                    {                        
                        if (//make sure the split sub-regions have more than minimal number of samples
                            regionCount[i] >= this.minNumSamples &&
                            (regionCount[len - 1] - regionCount[i]) >= this.minNumSamples)
                        {
                            double gain = -total + regionSum[i] * regionSum[i] / (regionCount[i] + float.Epsilon) + (regionSum[len - 1] - regionSum[i]) * (regionSum[len - 1] - regionSum[i]) / (regionCount[len - 1] - regionCount[i] + float.Epsilon);

                            curSplit.gain = gain;
                            curSplit.iThresh = i;
                            curSplit.feature = iFeature;

                            if (curSplit > bestSplit)                            
                            {
                                curSplit.CopyTo(bestSplit);             
                            }
                        }                        
                    }
                }
            }                                                    			
        }
    }

    /// <summary>
    /// Adding the syncronization constructs so that the bestsplit computation can be
    /// done in separate threads
    /// </summary>
    public class FindSplitObj_Thread : FindSplitObj
    {
        //events used to communicate with main thread
        ManualResetEvent startEvent;
        ManualResetEvent doneEvent;

        public FindSplitObj_Thread(ManualResetEvent startEvent, ManualResetEvent doneEvent)
            : base()
        {
            this.startEvent = startEvent;
            this.doneEvent = doneEvent;
        }

        override public void Find()
        {
            while (true)
            {
                startEvent.WaitOne();
                startEvent.Reset();
                base.Find();
                doneEvent.Set();
            }
        }
    }

    /// <summary>
    /// Generic interface for computing the best split for a list of data points
    /// </summary>
    public interface IFindSplit
    {       
        Split FindBestSplit(LabelFeatureDataCoded labelFeatureDataCoded, float[] responses,
                            int[] dataPoints, int[] workIndex, RandomSampler featureSampler,
                            RandomSampler dataSampler, int minNumSamples);            
        void Cleanup();		
    }

    public class FindSplitSync : IFindSplit
    {		
        FindSplitObj findSplitObj = null;        

        public FindSplitSync()
        {
            findSplitObj = new FindSplitObj();
        }

        public Split FindBestSplit(LabelFeatureDataCoded labelFeatureDataCoded, float[] responses,
                            int[] dataPoints, int[] workIndex, RandomSampler featureSampler,
                            RandomSampler dataSampler, int minNumSamples)
        {			                                                 
            findSplitObj.SetData(labelFeatureDataCoded, responses,                 
                                 dataPoints, workIndex,
                                 0, featureSampler.SampleSize, featureSampler, dataSampler,
                                 minNumSamples);
            findSplitObj.Find();

            return findSplitObj.bestSplit;						
        }
        

        public void Cleanup()
        {
            //NOOP;
        }
    }

    public class FindSplitAsync : IFindSplit
    {		
        protected List<FindSplitObj_Thread> findSplitThreadObjList = null;
        protected List<Thread> findSplitThreadList = null;

        ManualResetEvent[] StartEvents = null;
        ManualResetEvent[] DoneEvents = null;       

        int max_Threads = 0;
        int cThreads = 0;

        Split bestSplit = null;

        public int MAX_THREADS
        {
            get
            {
                if (max_Threads == 0)
                {
                    max_Threads = Environment.ProcessorCount; //using all processors avaliable
                }
                return max_Threads;
            }
        }

        public FindSplitAsync(int cThreads)
        {
            this.bestSplit = new Split();

            //setting up threads to compute the best splits		
            this.cThreads = (cThreads > MAX_THREADS) ? MAX_THREADS : cThreads;
            
            this.findSplitThreadObjList = new List<FindSplitObj_Thread>(this.cThreads);
            this.findSplitThreadList = new List<Thread>(this.cThreads);
                        
            this.StartEvents = new ManualResetEvent[this.cThreads];
            this.DoneEvents = new ManualResetEvent[this.cThreads];

            for (int i = 0; i < this.cThreads; i++)
            {                
                this.StartEvents[i] = new ManualResetEvent(false);
                this.DoneEvents[i] = new ManualResetEvent(true);

                FindSplitObj_Thread findSplitThreadObj = new FindSplitObj_Thread(StartEvents[i], DoneEvents[i]);
                this.findSplitThreadObjList.Add(findSplitThreadObj);

                ThreadStart threadStart = new ThreadStart(findSplitThreadObj.Find);
                Thread thread = new Thread(threadStart);
                this.findSplitThreadList.Add(thread);

                thread.Start();
            }
        }

        private void InitThreads()
        {
            this.bestSplit.Init();
            for (int i = 0; i < this.cThreads; i++)
            {
                FindSplitObj_Thread threadObj = findSplitThreadObjList[i];
                threadObj.bestSplit.Init();                
            }     
        }

        public Split FindBestSplit(LabelFeatureDataCoded labelFeatureDataCoded, float[] responses,
                            int[] dataPoints, int[] workIndex, RandomSampler featureSampler,
                            RandomSampler dataSampler, int minNumSamples)
        {
            InitThreads();

            for (int i = 0; i < featureSampler.SampleSize; i++)
            {
                //wait for any of the thread to finish
                int iThread = WaitHandle.WaitAny(this.DoneEvents);
                DoneEvents[iThread].Reset();
                                
                FindSplitObj_Thread threadObj = findSplitThreadObjList[iThread];

                //update the bestSplit given the result of just finished thread
                if (threadObj.bestSplit > bestSplit)
                {
                    threadObj.bestSplit.CopyTo(bestSplit);
                }

                //assign the data to the thread 
                threadObj.SetData(labelFeatureDataCoded, responses,
                                 dataPoints, workIndex, i, i+1, featureSampler, dataSampler, minNumSamples);                               

                //set the thread into motion
                StartEvents[iThread].Set();
            }            

            WaitHandle.WaitAll(DoneEvents);
            
            for (int i = 0; i < this.cThreads; i++)
            {                
                FindSplitObj_Thread threadObj = findSplitThreadObjList[i];
                
                if (threadObj.bestSplit > bestSplit)
                {
                    threadObj.bestSplit.CopyTo(bestSplit);
                }                
            }     
            
            return bestSplit;
        }       

        public void Cleanup()
        {
            foreach (Thread t in findSplitThreadList)
            {
                t.Abort();
            }
        }
    }
}
