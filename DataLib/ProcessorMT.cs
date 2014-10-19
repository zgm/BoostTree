using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Threading;
using Microsoft.TMSN;

namespace StochasticGradientBoost
{
    /// <summary>
    /// overall processor:
    /// (1) keeping track of input dat and output data
    /// </summary>
    interface Processor
    {
        int cJobs
        {
            get;            
        }

        ProcessorThread CreatePerThread();
    }

    /// <summary>
    /// per-thread processor
    /// </summary>
    interface ProcessorThread
    {
        void SetData(int iJob);

        void process();
    }

    /// <summary>
    /// Multithread processors
    /// </summary>
    class ProcessorMT
    {         
        public ProcessorMT(Processor processor, int cThreads)
        {
            this.processor = processor;

            //(1) setting up threads to compute the best splits		
            this.cThreads = (cThreads > MAX_THREADS) ? MAX_THREADS : cThreads;
            this.cThreads = (this.cThreads > processor.cJobs) ? processor.cJobs : this.cThreads;

            this.processorThreadObjs = new ProcessorThreadObj[this.cThreads];
            this.processorThreads = new Thread[this.cThreads];

            this.StartEvents = new ManualResetEvent[this.cThreads];
            this.DoneEvents = new ManualResetEvent[this.cThreads];

            for (int i = 0; i < this.cThreads; i++)
            {                
                ProcessorThread processorThread = processor.CreatePerThread();

                this.StartEvents[i] = new ManualResetEvent(false);
                this.DoneEvents[i] = new ManualResetEvent(true);

                ProcessorThreadObj processorThreadObj = new ProcessorThreadObj(StartEvents[i], DoneEvents[i], processorThread);
                this.processorThreadObjs[i] = processorThreadObj;

                ThreadStart threadStart = new ThreadStart(processorThreadObj.Process);
                Thread thread = new Thread(threadStart);
                this.processorThreads[i] = thread;

                thread.Start();
            }
        }

        public void Process()
        {            
            //(1) using the threads to process the columns             
            for (int i = 0; i < processor.cJobs; i++)
            {
                //wait for any of the thread to finish
                int iThread = WaitHandle.WaitAny(this.DoneEvents);
                DoneEvents[iThread].Reset();
                
                //assign the data to the thread 
                this.processorThreadObjs[iThread].SetData(i);

                //set the thread into motion
                StartEvents[iThread].Set();
            }

            //(2) wait until all the threads are done
            WaitHandle.WaitAll(DoneEvents);

            //(3) final clean up
            for (int i = 0; i < this.cThreads; i++)
            {
                this.processorThreads[i].Abort();
            }
        }
        
        private int MAX_THREADS
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

        Processor processor;

        //data related to parallel processing of features
        [NonSerialized] int cThreads;
        [NonSerialized] int max_Threads;
        [NonSerialized] ProcessorThreadObj[] processorThreadObjs;
        [NonSerialized] Thread[] processorThreads;
        [NonSerialized] ManualResetEvent[] StartEvents;
        [NonSerialized] ManualResetEvent[] DoneEvents;
    }

    class ProcessorThreadObj
    {
        //events used to communicate with main thread
        ManualResetEvent startEvent;
        ManualResetEvent doneEvent;
        ProcessorThread processorThread;

        public ProcessorThreadObj(ManualResetEvent startEvent, ManualResetEvent doneEvent, ProcessorThread processorThread)
        {
            this.startEvent = startEvent;
            this.doneEvent = doneEvent;
            this.processorThread = processorThread;
        }

        public void SetData(int iJob)
        {
            this.processorThread.SetData(iJob);
        }
        
        public void Process()
        {
            while (true)
            {
                startEvent.WaitOne();
                startEvent.Reset();
                this.processorThread.process();
                doneEvent.Set();
            }
        }
    }
}