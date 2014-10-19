using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Diagnostics;
using Microsoft.TMSN.IO;
using System.Text.RegularExpressions;

namespace StochasticGradientBoost
{
    class DataSection
    {
        public string Name
        {
            get
            {
                return name;
            }
        }
        public string[] Data
        {
            get
            {
                return dataList.ToArray();
            }
        }        

        public DataSection(string name)
        {
            this.name = name;
            dataList = new List<string>();
        }
        public void AddData(string data)
        {
            dataList.Add(data);
        }

        private string name;
        private List<string> dataList;
    }

    class IniFile
    {       
        static public DataSection[] Parse(string fileName)
        {
            List<DataSection> dataSecList = new List<DataSection>();
            using (StreamReader srIni = ZStreamReader.Open(fileName))
            {               
                string line;
                DataSection dataSec = null;
                while ((line = srIni.ReadLine()) != null)
                {
                    line.Trim();

                    if (line.StartsWith("["))
                    {
                        if (dataSec != null)
                        {
                            dataSecList.Add(dataSec);
                        }

                        string name = Regex.Replace(line, @"\[(.*?)\]", "$1");
                        dataSec = new DataSection(name);
                    }
                    else
                    {
                        if (line.Length > 0 && 
                            //igore comments
                            !line.StartsWith(";") && 
                            !line.StartsWith("C:", StringComparison.CurrentCultureIgnoreCase))
                        {
                            dataSec.AddData(line);
                        }
                    }
                }

                if (dataSec != null)
                {
                    dataSecList.Add(dataSec);
                }
            }
            return dataSecList.ToArray();
        }
    }

    interface Layer
    {
        float[] FProp(float[] input);
        void WriteMSNStyle(StreamWriter wStream);
        int cOutputs
        {
            get;
        }
    }

    [Serializable]
    class InputLayer : Layer
    {
        public InputLayer(List<InputTransform> inputTransforms)
        {
            this.cTransforms = inputTransforms.Count;
            this.inputTransforms = new InputTransform[this.cTransforms];
            foreach (InputTransform inputTrans in inputTransforms)
            {
                this.inputTransforms[inputTrans.InputID-1] = inputTrans;
            }
            this.outputs = new float[this.cTransforms];
        }

        public InputLayer(InputLayer inLayer, DTNode[,][] boostedDTs, int cIter)
        {
            this.cTransforms = inLayer.cTransforms + cIter;
            this.inputTransforms = new InputTransform[this.cTransforms];
            this.outputs = new float[this.cTransforms];

            for (int i = 0; i < this.cTransforms; i++)
            {
                if (i < inLayer.cTransforms)
                {
                    this.inputTransforms[i] = new InputTransform(inLayer.inputTransforms[i]);
                }
                else
                {
                    int iNew = i - inLayer.cTransforms;
                    this.inputTransforms[i] = new InputTransform(boostedDTs[iNew, 0], i + 1);
                }
            }

        }

        public InputLayer(DTNode[,][] boostedDTs, int cIter) // REVIEW: CJCB
        {
            int cClass = boostedDTs.GetLength(1);
            this.cTransforms = cIter * cClass;
            this.inputTransforms = new InputTransform[this.cTransforms];

            this.outputs = new float[this.cTransforms];

            for (int i = 0; i < cIter; i++)
            {
                for (int j = 0; j < cClass; j++)
                {
                    this.inputTransforms[i*cClass + j] = new InputTransform(boostedDTs[i, j], i + 1);
                }
            }
        }

        public bool SetFeatureNames(string[] FeatureNames)
        {
            for (int i = 0; i < this.cTransforms; i++)
            {
                if (!this.inputTransforms[i].SetFeatureNames(FeatureNames))
                {
                    return false;
                }
            }
            return true; ;
        }

        public float[] FProp(float[] input)
        {
            for (int i = 0; i < this.cTransforms; i++)
            {
                this.outputs[i] = this.inputTransforms[i].Apply(input);
            }
            return this.outputs;
        }

        public void WriteMSNStyle(StreamWriter wStream)
        {
            for (int i = 0; i < inputTransforms.Length; i++)
            {
                inputTransforms[i].WriteMSNStyle(wStream);
            }
        }

        public InputTransform[] InTransform
        {
            get
            {
                return inputTransforms;
            }
        }
        public int cOutputs
        {
            get
            {            
                return this.outputs.Length;
            }
        }

        InputTransform[] inputTransforms;
        int cTransforms;
        float[] outputs;
    }

    [Serializable]
    abstract class TransformFunction : ICloneable
    {
        static public TransformFunction Create(string funcName, string[] parameters, int idxStart)
        {
            TransformFunction transFunc;
            switch (funcName)
            {
                case "linear":
                    transFunc = new LinearTransform(parameters, idxStart);
                    break;
                case "bucket":
                    transFunc = new BucketTransform(parameters, idxStart);
                    break;
                case "rational":
                    transFunc = new RationalTransform(parameters, idxStart);
                    break;
                case "loglinear":
                    transFunc = new LogLinearTransform(parameters, idxStart);
                    break;
                case "DecisionTree":
                    transFunc = new DecisionTreeTransform(parameters, idxStart);
                    break;
                case "BM25F2":
                    transFunc = new BM25F2Transform(parameters, idxStart);
                    break;
                case "LogBM25F2":
                    transFunc = new LogBM25F2Transform(parameters, idxStart);
                    break;
                default:
                    throw new Exception("un-handled input transformation " + funcName);
            }

            return transFunc;
        }

        abstract public void WriteMSNStyle(StreamWriter wStream, string ftrName, string funcName);

        abstract public object Clone();
        virtual public bool SetFeatureNames(string[] FeatureNames, string featName)
        {
            for (int i = 0; i < FeatureNames.Length; i++)
            {
                if (string.Compare(FeatureNames[i], featName, true) == 0)
                {
                    this.idxInput = i;
                    return true;
                }
            }

            Console.WriteLine("input feature" + featName + " does not exist, using 0 instead");

            this.idxInput = -1;
            return true;
        }

        virtual public float Apply(float[] inputs)
        {
            float v = (this.idxInput >= 0)? inputs[this.idxInput] : 0.0F;            
            return Apply(v);
        }

        virtual protected float Apply(float x)
        {
            return x;
        }

        private int idxInput = -1;
    }

    [Serializable]           
    class NoneTransform : TransformFunction
    {
        protected override float Apply(float x)
        {
            return x;
        }

        public override void WriteMSNStyle(StreamWriter wStream, string ftrName, string funcName)       
        {
            return;
        }
        
        public override object Clone()
        {
            return this;
        }
    }

    [Serializable]
    public class DTNode
    {
        public enum NodeType { BRANCH, VALUE, };

        public DTNode(int nodeId)
        {
            this.nodeId = nodeId;
            this.fIsThresholdInt = true;
        }

        public DTNode(DTNode node)
        {
            this.nodeGT = node.nodeGT;
            this.nodeId = node.nodeId;
            this.nodeLTE = node.nodeLTE;
            this.nodeType = node.nodeType;
            this.splitFeature = node.splitFeature;
            this.splitFeatureIdx = node.splitFeatureIdx;
            this.threshold = node.threshold;
            this.fIsThresholdInt = node.fIsThresholdInt;
            this.value = node.value;            
        }

        public void Add(string name, string value)
        {
            if (string.Compare(name, "NodeType", true) == 0)
            {
                if (string.Compare(value, "Branch", true) == 0)
                {
                    this.nodeType = NodeType.BRANCH;
                }
                else if (string.Compare(value, "Value", true) == 0)
                {
                    this.nodeType = NodeType.VALUE;
                }
            }
            else if (string.Compare(name, "NodeValue", true) == 0)
            {
                this.value = float.Parse(value);
            }
            else if (string.Compare(name, "NodeDecision", true) == 0)
            {
                this.splitFeature = value;
            }
            else if (string.Compare(name, "NodeThreshold", true) == 0)
            {
                this.threshold = float.Parse(value);
            }
            else if (string.Compare(name, "NodeLTE", true) == 0)
            {
                this.nodeLTE = int.Parse(value);
            }
            else if (string.Compare(name, "NodeGT", true) == 0)
            {
                this.nodeGT = int.Parse(value);
            }
        }

        public bool IsThresholdInt
        {
            set
            {
                this.fIsThresholdInt = value;
            }
        }

        public void WriteMSNStyle(StreamWriter wStream)
        {
            if (this.nodeType == NodeType.BRANCH)
            {
                wStream.WriteLine("NodeType:{0}=Branch", this.nodeId);
                wStream.WriteLine("NodeDecision:{0}={1}", this.nodeId, this.splitFeature);
                if (this.fIsThresholdInt)
                {
                    //the MSN .Ini implementation requires the threshold to be an integer
                    //==> It is important that the input training features are integers
                    //==> which is true for raw .tsv data, but not true for transformed data
                    if (this.threshold < 0 || this.threshold > UInt32.MaxValue)
                    {
                        throw new Exception("splitValue has to be UInt32");
                    }
                    else
                    {
                        wStream.WriteLine("NodeThreshold:{0}={1}", this.nodeId, (UInt32)this.threshold);
                    }
                }
                else
                {
                    wStream.WriteLine("NodeThreshold:{0}={1}", this.nodeId, this.threshold);
                }

                wStream.WriteLine("NodeLTE:{0}={1}", this.nodeId, this.nodeLTE);
                wStream.WriteLine("NodeGT:{0}={1}", this.nodeId, this.nodeGT);
            }
            else
            {
                wStream.WriteLine("NodeType:{0}=Value", this.nodeId);
                wStream.WriteLine("NodeValue:{0}={1}", this.nodeId, this.value);
            }
        }

        public bool SetFeatureNames(string[] FeatureNames)
        {
            this.splitFeatureIdx = -1;

            if (this.nodeType == NodeType.VALUE)
            {
                return true;
            }

            for (int i = 0; i < FeatureNames.Length; i++)
            {
                if (string.Compare(FeatureNames[i], this.splitFeature, true) == 0)
                {
                    this.splitFeatureIdx = i;
                    return true;
                }
            }
            Console.WriteLine("input feature" + this.splitFeature + " does not exist, using 0 instead");

            return true;
        }

        public int NodeID
        {
            get
            {
                return nodeId;
            }
        }

        public NodeType NodeTYPE
        {
            get
            {
                return nodeType;
            }
        }

        public int FeatureIdx
        {
            get
            {
                return splitFeatureIdx;
            }
        }

        public float Threshold
        {
            get
            {
                return threshold;
            }
        }

        public float Value
        {
            get
            {
                return value;
            }
        }

        public int NodeLTE
        {
            get
            {
                return nodeLTE;
            }
        }

        public int NodeGT
        {
            get
            {
                return nodeGT;
            }
        }

        private NodeType nodeType;
        private int nodeId;
        private string splitFeature;
        private int splitFeatureIdx;
        private float threshold;
        private bool fIsThresholdInt;
        private int nodeLTE;
        private int nodeGT;
        private float value;
    }

    [Serializable]
    class DecisionTreeTransform : TransformFunction
    {
        private DTNode GetNode(int nodeId, List<DTNode> listDTNode)
        {
            foreach (DTNode node in listDTNode)
            {
                if (node.NodeID == nodeId)
                {
                    return node;
                }
            }
            DTNode nodeNew = new DTNode(nodeId);
            listDTNode.Add(nodeNew);
            return nodeNew;
        }

        public DecisionTreeTransform(string[] parameters, int idxStart)
        {
            this.listDTNode = new List<DTNode>();
            for (int i = idxStart; i < parameters.Length; i++)
            {
                string[] fields = parameters[i].Split(':');
                string[] ps = fields[1].Split('=');
                int nodeId = int.Parse(ps[0]);
                DTNode dtNode = GetNode(nodeId, this.listDTNode);
                dtNode.Add(fields[0], ps[1]);                
            }            
        }

        public DecisionTreeTransform(DecisionTreeTransform oriTransform)
        {
            this.listDTNode = new List<DTNode>(oriTransform.listDTNode.Count);
            foreach (DTNode node in oriTransform.listDTNode)
            {
                this.listDTNode.Add(new DTNode(node));
            }                       
        }

        public DecisionTreeTransform(DTNode[] dtNodes)
        {
            this.listDTNode = new List<DTNode>(dtNodes.Length);
            for(int i=0; i<dtNodes.Length; i++)
            {
                this.listDTNode.Add(dtNodes[i]);
            }   
        }

        public override object Clone()
        {
            return new DecisionTreeTransform(this);
        }

        public override void WriteMSNStyle(StreamWriter wStream, string ftrName, string funcName)
        {           
            wStream.WriteLine("Name=AnchorMostFrequent");
            wStream.WriteLine("Transform=DecisionTree");

            //writing tree nodes
            foreach(DTNode node in this.listDTNode)
            {
                if (node != null)
                {
                    node.WriteMSNStyle(wStream);
                    wStream.WriteLine();
                }
            }                    
        }

        public override float Apply(float[] feat)
        {
            DTNode curNode = this.listDTNode[0];
            while (true)
            {
                if (curNode.NodeTYPE == DTNode.NodeType.BRANCH)
                {
                    float featVal = (curNode.FeatureIdx >= 0) ? feat[curNode.FeatureIdx] : 0.0F;
                    if (featVal <= curNode.Threshold)
                    {
                        curNode = this.listDTNode[curNode.NodeLTE];
                    }
                    else
                    {
                        curNode = this.listDTNode[curNode.NodeGT];
                    }
                }
                else
                {
                    return curNode.Value;
                }
            }
        }        

        override public bool SetFeatureNames(string[] FeatureNames, string featName)
        {
            foreach (DTNode node in listDTNode)
            {
                if (! node.SetFeatureNames(FeatureNames))
                {
                    return false;
                }                
            }
            return true;            
        }  

        private List<DTNode> listDTNode;
    }

    [Serializable]
    class LinearTransform : TransformFunction
    {            
        public LinearTransform(string[] parameters, int idxStart)
        {
            for (int i = idxStart; i < parameters.Length; i++)
            {
                string[] fields = parameters[i].Split('=');
                if (string.Compare(fields[0], "Slope", true) == 0)
                {
                    this.slope = float.Parse(fields[1]);
                }
                else if (string.Compare(fields[0], "Intercept", true) == 0)
                {
                    this.intercept = float.Parse(fields[1]);
                }
            }               
        }

        public LinearTransform(LinearTransform oriTransform)
        {
            this.intercept = oriTransform.intercept;            
            this.slope = oriTransform.slope;            
        }

        public override object Clone()
        {
            return new LinearTransform(this);
        }

        public override void WriteMSNStyle(StreamWriter wStream, string ftrName, string funcName)
        {
            wStream.WriteLine("Name={0}", ftrName);
            wStream.WriteLine("Transform={0}", funcName);                 
            wStream.WriteLine("Intercept={0}", this.intercept);
            wStream.WriteLine("Slope={0}", this.slope);                     
        }

        protected override float Apply(float x)
        {
            return slope*x + intercept;
        }
        
        float slope;
        float intercept;
    }

    [Serializable]
    class LogLinearTransform : TransformFunction
    {
        public LogLinearTransform(string[] parameters, int idxStart)
        {
            for (int i = idxStart; i < parameters.Length; i++)
            {
                string[] fields = parameters[i].Split('=');
                if (string.Compare(fields[0], "Slope", true) == 0)
                {
                    this.slope = float.Parse(fields[1]);
                }
                else if (string.Compare(fields[0], "Intercept", true) == 0)
                {
                    this.intercept = float.Parse(fields[1]);
                }
            }               
        }

        public LogLinearTransform(LogLinearTransform oriTransform)
        {
            this.intercept = oriTransform.intercept;            
            this.slope = oriTransform.slope;            
        }

        public override object Clone()
        {
            return new LogLinearTransform(this);
        }

        public override void WriteMSNStyle(StreamWriter wStream, string ftrName, string funcName)
        {
            wStream.WriteLine("Name={0}", ftrName);
            wStream.WriteLine("Transform={0}", funcName); 
            wStream.WriteLine("Intercept={0}", this.intercept);
            wStream.WriteLine("Slope={0}", this.slope);            
        }

        protected override float Apply(float x)
        {
            const float minX = -0.99999F;
            if (x < minX)
            {
                Console.WriteLine("Warning, LogLinear transform of "+x+" being clipped");
                x = minX;
            }				
            return slope*(float)Math.Log(x+1) + intercept;
        }            
        float slope;
        float intercept;
    }

    [Serializable]
    class RationalTransform : TransformFunction
    {
        public RationalTransform(string[] parameters, int idxStart)
        {
            this.slope = 1.0F;
            this.intercept = 0.0F;
            for (int i = idxStart; i < parameters.Length; i++)
            {
                string[] fields = parameters[i].Split('=');
                if (string.Compare(fields[0], "DampingFactor", true) == 0)
                {
                    this.dampingFactor = float.Parse(fields[1]);
                }
                else if (string.Compare(fields[0], "Slope", true) == 0)
                {
                    this.slope = float.Parse(fields[1]);
                }
                else if (string.Compare(fields[0], "Intercept", true) == 0)
                {
                    this.intercept = float.Parse(fields[1]);
                }
            }               
        }

        public RationalTransform(RationalTransform oriTransform)
        {
            this.intercept = oriTransform.intercept;
            this.dampingFactor = oriTransform.dampingFactor;
            this.slope = oriTransform.slope;            
        }

        public override object Clone()
        {
            return new RationalTransform(this);
        }

        public override void WriteMSNStyle(StreamWriter wStream, string ftrName, string funcName)
        {
            wStream.WriteLine("Name={0}", ftrName);
            wStream.WriteLine("Transform={0}", funcName); 
            wStream.WriteLine("DampingFactor={0}", this.dampingFactor);
            //wStream.WriteLine("Slope={0}", this.slope);
            //wStream.WriteLine("Intercept={0}", this.intercept);                       
        }

        protected override float Apply(float x)
        {
            return ((slope*x)/(x+dampingFactor)) + intercept;
        }
       
        float dampingFactor;
        float slope;
        float intercept;
    }

    [Serializable]
    class BucketTransform : TransformFunction
    {        
        public BucketTransform(string[] parameters, int idxStart)
        {
            for (int i = idxStart; i < parameters.Length; i++)
            {
                string[] fields = parameters[i].Split('=');
                if (string.Compare(fields[0], "MinValue", true) == 0)
                {
                    this.minValue = float.Parse(fields[1]);
                }
                else if (string.Compare(fields[0], "MinInclusive", true) == 0)
                {
                    this.minInclusive = bool.Parse(fields[1]);
                }
                else if (string.Compare(fields[0], "MaxValue", true) == 0)
                {
                    this.maxValue = float.Parse(fields[1]);
                }
                else if (string.Compare(fields[0], "MaxInclusive", true) == 0)
                {
                    this.maxInclusive = bool.Parse(fields[1]);
                }
            }               
        }
        
        public override void WriteMSNStyle(StreamWriter wStream, string ftrName, string funcName)
        {
            wStream.WriteLine("Name={0}", ftrName);
            wStream.WriteLine("Transform={0}", funcName);
            wStream.WriteLine("MinValue={0}", this.minValue);
            wStream.WriteLine("MaxValue={0}", this.maxValue);
            wStream.WriteLine("MinInclusive={0}", this.minInclusive);  
            wStream.WriteLine("MaxInclusive={0}", this.maxInclusive);          
        }

        protected override float Apply(float x)
        {
            if ((x > minValue || (minInclusive && x==minValue)) &&
                (x < maxValue || (maxInclusive && x==maxValue)))
                return 1.0F;
            else
                return 0.0F;
        }

        public BucketTransform(BucketTransform oriTrans)
        {
            this.minValue = oriTrans.minValue;
            this.minInclusive = oriTrans.minInclusive;
            this.maxValue = oriTrans.maxValue;
            this.maxInclusive = oriTrans.maxInclusive;
        }

        public override object Clone()
        {
            return new BucketTransform(this);
        }

        float minValue;
        bool minInclusive;
        float maxValue;
        bool maxInclusive;
    }

    // Note that the RelTypes[] and float[] arrays are kept local to this class.  Everything else
    // sees them as tab separated strings, for consistency with other Transforms.
    [Serializable]
    class BinTransform : TransformFunction
    {
        public BinTransform(string[] parameters, int idxStart)
        {
            for (int i = idxStart; i < parameters.Length; i++)
            {
                string[] fields = parameters[i].Split('=');
                if (string.Compare(fields[0], "Thresholds", true) == 0)
                {
                    this.thresholdsStr = fields[1];
                }
                else if (string.Compare(fields[0], "Relations", true) == 0)
                {
                    this.relationsStr = fields[1];
                }                
            }

            throw new Exception("Not yet fully implemented Binary transformation ");           

        }


        public BinTransform(BinTransform oriTransform)
        {
            this.thresholdsStr = oriTransform.thresholdsStr;
            this.relationsStr = oriTransform.relationsStr;                     
        }

        public override object Clone()
        {
            return new BinTransform(this);
        }

        public override void WriteMSNStyle(StreamWriter wStream, string ftrName, string funcName)
        {
            throw new Exception("Not yet fully implemented Binary transformation ");  
#if BINTRANSFORM
            wStream.WriteLine("Name={0}", ftrName);
            wStream.WriteLine("Transform={0}", funcName);             
            
            for (int i = 0; i < thresholdsStr.Length; i++)
            {
                wStream.WriteLine("Thresholds={0}", thresholdsStr[i]);
            }

            for (int i = 0; i < relationsStr.Length; i++)
            {
                wStream.WriteLine("Relations={0}", relationsStr[i]);
            }   
#endif //0        
        }

        // Actually returns an integer - the index of the bin that contains the datum
        protected override float Apply(float x)
        {
            throw new Exception("BinTransform not yet fully implemented yet");
            //return 0.0F;
            //return (double)BinData.MapNumToBin((float)x, thresholds, relations);
        }
        
        //float[] thresholds;
        //RelType[] relations;
        string thresholdsStr;
        string relationsStr;
    }

    [Serializable]
    class InputTransform
    {
        public static bool IsType(DataSection dataSec)
        {
            if (dataSec.Name.StartsWith("Input:"))
            {
                return true;
            }
            return false;
        }

        public InputTransform(DataSection dataSec)
        {
            string[] fields = dataSec.Name.Split(':');
            this.inputID = int.Parse(fields[1]);            
            for (int i = 0; i < dataSec.Data.Length; i++)
            {
                if (dataSec.Data[i].StartsWith("Name="))
                {
                    fields = dataSec.Data[i].Split('=');
                    this.ftrName = fields[1];
                }
                else if (dataSec.Data[i].StartsWith("Transform="))
                {
                    fields = dataSec.Data[i].Split('=');
                    this.funcName = fields[1];
                    this.transformFunc = TransformFunction.Create(this.funcName, dataSec.Data, i + 1); 
                }                                                                       
            }
        }

        public InputTransform(InputTransform inTransform)
        {
            this.ftrName = inTransform.ftrName;
            this.funcName = inTransform.funcName;
            this.inputID = inTransform.inputID;
            this.transformFunc = (TransformFunction)inTransform.transformFunc.Clone();
        }

        public InputTransform(DTNode[] dtNodes, int inID)
        {
            this.ftrName = "AnchorMostFrequent";
            this.funcName = "DecisionTree";
            this.inputID = inID;
            this.transformFunc = new DecisionTreeTransform(dtNodes);
        }

        public void WriteMSNStyle(StreamWriter wStream)
        {
            wStream.WriteLine("[Input:{0}]", this.inputID);            
            this.transformFunc.WriteMSNStyle(wStream, this.ftrName, this.funcName);
            wStream.WriteLine();            
        }

        public bool SetFeatureNames(string[] FeatureNames)
        {
            return this.transformFunc.SetFeatureNames(FeatureNames, this.ftrName);
        }

        public float Apply(float[] input)
        {
            return this.transformFunc.Apply(input);
        }

        public string FtrName
        {
            get
            {
                return this.ftrName;
            }
        }

        public string FuncName
        {
            get
            {
                return this.funcName;
            }
        }

        public int InputID
        {
            get
            {
                return this.inputID;
            }
        }

        private string ftrName;
        private string funcName;
        private int inputID;        
        private TransformFunction transformFunc;
    }

    [Serializable]
    class NodeLayer : Layer
    {
        public static bool IsType(DataSection dataSec)
        {
            if (dataSec.Name.StartsWith("Layer:"))
            {
                return true;
            }
            return false;
        }

        //construct a layer from data
        public NodeLayer(DataSection dataSec)
        {
            string[] fields = dataSec.Name.Split(':');
            this.layerID = int.Parse(fields[1]);
            for (int i = 0; i < dataSec.Data.Length; i++)
            {                
                if (dataSec.Data[i].StartsWith("nodes=", StringComparison.CurrentCultureIgnoreCase))
                {
                    fields = dataSec.Data[i].Split('=');
                    this.cNodes = int.Parse(fields[1]);
                }
            }
            nodes = new Node[this.cNodes];
            outputs = new float[this.cNodes];
        }
            
        //construct an one node layer which is the output of the multiclass classification
        //it compute the probability of each class and dot product it which their class ID as real numbers
        public NodeLayer(int layerID, int cClass)
        {
            this.cNodes = 1;
            this.layerID = layerID;
            this.nodes = new Node[1];
            this.nodes[0] = new Node(layerID, 1, cClass);            
            this.outputs = new float[this.cNodes];
        }

        //construct the hidden layer for multiclass classification
        //it has cClass nodes where each node sums the regression trees for the corresponding class
        public NodeLayer(int layerID, int cInputs, int cClass)
        {
            this.cNodes = cClass;            
            this.layerID = layerID;
            this.nodes = new Node[this.cNodes];
            for (int iClass = 0; iClass < cClass; iClass++)
            {
                this.nodes[iClass] = new Node(layerID, iClass+1, cInputs, cClass, iClass);
            }
            this.outputs = new float[this.cNodes];
        }

        //extending the layer of the submodel (curLayerOri) with the additional boosted regression trees
        public NodeLayer(NodeLayer curLayerOri, Layer prevLayer)
        {
            this.layerID = curLayerOri.LayerID;
            if (curLayerOri.cNodes == 1 && curLayerOri.nodes[0].IsLinear())
            {
                this.cNodes = 1;                
                this.nodes = new Node[this.cNodes];
                this.nodes[0] = new Node(curLayerOri.layerID, 1, prevLayer.cOutputs, curLayerOri.nodes[0], true, true);                
            }
            else
            {
                this.cNodes = curLayerOri.cNodes+1;                
                this.nodes = new Node[this.cNodes];
                for (int i = 0; i < curLayerOri.cNodes; i++)
                {
                    this.nodes[i] = new Node(curLayerOri.layerID, i+1, prevLayer.cOutputs, curLayerOri.nodes[i], true, false);
                }
                this.nodes[this.cNodes - 1] = new Node(curLayerOri.layerID, this.cNodes, prevLayer.cOutputs, curLayerOri.nodes[0], false, true);                
            }
            this.outputs = new float[this.cNodes];
        }

        public float[] FProp(float[] input)
        {
            for (int i = 0; i < this.cNodes; i++)
            {
                this.outputs[i] = this.nodes[i].FProp(input);
            }
            return this.outputs;
        }

        public int LayerID
        {
            get
            {
                return this.layerID;
            }
        }

        public void WriteMSNStyle(StreamWriter wStream)
        {
            wStream.WriteLine("[Layer:{0}]", this.layerID);
            wStream.WriteLine("nodes={0}", this.cNodes);
            wStream.WriteLine();

            //writing nodes
            for (int i = 0; i < this.nodes.Length; i++)
            {
                this.nodes[i].WriteMSNStyle(wStream);
            }
        }

        public int cOutputs
        {
            get
            {
                return this.outputs.Length;
            }
        }

        public void AddNodes(List<Node> nodesList)
        {
            foreach (Node node in nodesList)
            {
                if (node.LayerID == this.LayerID)
                {
                    nodes[node.NodeID - 1] = node;
                }
            }
        }

        private int layerID;
        private int cNodes;
        private Node[] nodes;
        private float[] outputs;
    }

    [Serializable]
    class Node
    {
        public static bool IsType(DataSection dataSec)
        {
            if (dataSec.Name.StartsWith("Node:"))
            {
                return true;
            }
            return false;
        }
      
        //construct node from .ini configuration/description
        public Node(DataSection dataSec)
        {
            string[] fields = dataSec.Name.Split(':');
            this.layerID = int.Parse(fields[1]);
            this.nodeID = int.Parse(fields[2]);
            List<float> weightList = new List<float>();
            List<int> idxList = new List<int>();
            for (int i = 0; i < dataSec.Data.Length; i++)
            {               
                if (dataSec.Data[i].StartsWith("Weight:"))
                {
                    fields = dataSec.Data[i].Split(':');
                    fields = fields[1].Split('=');
                    idxList.Add(int.Parse(fields[0]));
                    weightList.Add(float.Parse(fields[1]));
                }
                else if (dataSec.Data[i].StartsWith("Type="))
                {
                    fields = dataSec.Data[i].Split('=');  
                    if (string.Compare(fields[1], "Tanh", StringComparison.CurrentCultureIgnoreCase) == 0)
                    {
                        this.transFunc = TransferFunction.Tanh;
                    }
                    else if (string.Compare(fields[1], "Sigmoid", StringComparison.CurrentCultureIgnoreCase) == 0)
                    {
                        this.transFunc = TransferFunction.Sigmoid;
                    }
                    else if (string.Compare(fields[1], "Linear", StringComparison.CurrentCultureIgnoreCase) == 0)
                    {
                        this.transFunc = TransferFunction.Linear;
                    }
                    else if (string.Compare(fields[1], "Logistic", StringComparison.CurrentCultureIgnoreCase) == 0)
                    {
                        this.IsLogistic = true;
                    }
                }
            }
            this.Weights = new float[weightList.Count];
            for (int i = 0; i < this.Weights.Length; i++)
            {
                this.Weights[idxList[i]] = weightList[i];
            }
        }

        //construct a node for multiclass (> 2) classification - there are cClass nodes for cClass classification model
        //where each node corresponding to each class; i.e., the (iClass) node sums all and only the regression trees for that (iClass) class
        public Node(int layerID, int nodeID, int cInputs, int cClass, int iClass)
        {
            this.layerID = layerID;
            this.nodeID = nodeID;
            this.Weights = new float[cInputs + 1];
            this.Weights[0] = 0.0F;
            for (int i = 1; i < cInputs + 1; i++)
            {
                this.Weights[i] = 0.0F;
                if ((i-1) % cClass == iClass) //iClass is zero based but i is one based
                {
                    this.Weights[i] = 1.0F;
                }
            }
            this.transFunc = TransferFunction.Linear;
        }

        //construct the output node for multiclass classification model
        //the node computes the probability of each class (p0, p1, .. ,p4) and dot it
        //the the corresponding class ID as real value (0, 1, ,..., 4) to produce a single score
        public Node(int layerID, int nodeID, int cClass)
        {
            this.layerID = layerID;
            this.nodeID = nodeID;           
            this.IsLogistic = true;

            this.Weights = new float[cClass + 1];
            this.Weights[0] = 0;
            for (int i = 1; i < cClass + 1; i++)
            {
                this.Weights[i] = i - 1;
            }
        }              

        //construct a new node based on corresponding node in the sub-model to handle the additional boosted regression trees
        public Node(int layerID, int nodeID, int cInputs, Node curNode, bool fKeepCur, bool fKeepNew)
        {
            this.layerID = layerID;
            this.nodeID = nodeID;
            this.Weights = new float[cInputs+1];
            for (int i = 0; i < cInputs+1; i++)
            {
                this.Weights[i] = 0.0F;
                if (i < curNode.Weights.Length)
                {
                    if (fKeepCur)
                    {
                        this.Weights[i] = curNode.Weights[i];
                    }                    
                }
                else
                {
                    if (fKeepNew)
                    {
                        this.Weights[i] = 1.0F;
                    }                    
                }
            }

            if (fKeepCur)
            {
                this.transFunc = curNode.transFunc;
            }
            else
            {
                this.transFunc = TransferFunction.Linear;
            }
        }

        public bool IsLinear()
        {
            if (this.transFunc != null && string.Compare(this.transFunc.NameString, TransferFunction.Linear.NameString, true) == 0)
            {
                return true;
            }
            return false;
        }

        public void WriteMSNStyle(StreamWriter wStream)
        {
            wStream.WriteLine("[Node:{0}:{1}]", this.layerID, this.nodeID);
           
            //writing weights
            for (int i = 0; i < this.Weights.Length; i++)
            {
                wStream.WriteLine("Weight:{0}={1}", i, (double)this.Weights[i]);                
            }

            //transfer function type
            if (this.IsLogistic)
            {
                wStream.WriteLine("Type={0}", "Logistic");
            }
            else
            {
                wStream.WriteLine("Type={0}", this.transFunc.NameString);
            }
            wStream.WriteLine();            
        }

        public float FProp(float[] input)
        {
            Debug.Assert(input.Length == (this.Weights.Length - 1), "wrong input to NN node");
            float result = this.Weights[0];

            if (this.IsLogistic)
            {                
                double dblNumerator = 0.0;
                double dblDenominator = 0.0;

                for (int i = 0; i < input.Length; i++)
                {
                    double dblExp = Math.Exp(input[i]);
                    dblNumerator += dblExp * this.Weights[i+1];
                    dblDenominator += dblExp;
                }

                if (dblDenominator == 0)
                {
                    dblNumerator = 1;
                    dblDenominator = (double)input.Length;
                }

                double dblResult = dblNumerator / dblDenominator;

                result = (float)dblResult;
            }
            else
            {
                for (int i = 0; i < input.Length; i++)
                {
                    result += this.Weights[i + 1] * input[i];
                }
                result = (float)transFunc.Eval(result);
            }
            return result;
        }

        public int LayerID
        {
            get
            {
                return layerID;
            }
        }

        public int NodeID
        {
            get
            {
                return nodeID;
            }
        }

        private int layerID;
        private int nodeID;
        private float[] Weights; //Weights[0] is the bias
        private TransferFunction transFunc;

        //qiangwu (TODO): can implement "Logisitc" node better
        private bool IsLogistic = false;
    }

    [Serializable]
    public class NNModelMSN : Model
    {       
        public static NNModelMSN Create(string fileName)
        {                        
            List<InputTransform> inputList = new List<InputTransform>();
            List<NodeLayer> layerList = new List<NodeLayer>();
            List<Node> nodeList = new List<Node>();
            NNModelMSN nnMSN = null;

            DataSection[] dataSec = IniFile.Parse(fileName);
            for (int i = 0; i < dataSec.Length; i++)
            {
                if (InputTransform.IsType(dataSec[i]))
                {
                    inputList.Add(new InputTransform(dataSec[i]));
                }
                else if (NodeLayer.IsType(dataSec[i]))
                {
                    layerList.Add(new NodeLayer(dataSec[i]));
                }
                else if(Node.IsType(dataSec[i]))
                {
                    nodeList.Add(new Node(dataSec[i]));
                }
                else if (NNModelMSN.IsType(dataSec[i]))
                {
                    nnMSN = new NNModelMSN(dataSec[i]);
                }
            }

            if (nnMSN != null)
            {
                nnMSN.Init(inputList, layerList, nodeList);
            }

            return nnMSN;
        }
        
        private NNModelMSN(DataSection dataSec)
        {
            string[] fields;
            for (int i = 0; i < dataSec.Data.Length; i++)
            {                
                if (dataSec.Data[i].StartsWith("Layers="))
                {
                    fields = dataSec.Data[i].Split('=');
                    this.cNodeLayer = int.Parse(fields[1]);
                }
                else if (dataSec.Data[i].StartsWith("Inputs="))
                {
                    fields = dataSec.Data[i].Split('=');
                    this.cInputs = int.Parse(fields[1]);
                }
            }           
        }        

        public NNModelMSN(NNModelMSN subModel, DTNode[,][] boostedDTs)
        {                        
            int cIter = boostedDTs.GetLength(0);
            int cClass = boostedDTs.GetLength(1);
            if (cClass == 1)
            {
                if (subModel != null) 
                {
                    this.cNodeLayer = subModel.cNodeLayer;
                    this.cInputs = subModel.cInputs + cIter;

                    this.layers = new Layer[this.cNodeLayer + 1];

                    //create the input layer
                    this.layers[0] = new InputLayer((InputLayer)subModel.layers[0], boostedDTs, cIter);

                    //create the extended node layers                
                    for (int l = 1; l <= this.cNodeLayer; l++)
                    {
                        this.layers[l] = new NodeLayer((NodeLayer)subModel.layers[l], this.layers[l - 1]);
                    }
                }
                else
                {
                    this.cNodeLayer = 1;
                    this.cInputs = cIter;
                    this.layers = new Layer[this.cNodeLayer + 1];

                    this.layers[0] = new InputLayer(boostedDTs, cIter);
                    this.layers[1] = new NodeLayer(1, this.cInputs, 1);

                }

                //add a new layer if necessary
                if (this.layers[this.cNodeLayer].cOutputs > 1)
                {
                    this.cNodeLayer++;
                    Layer[] layers = new Layer[this.cNodeLayer];
                    for (int i = 0; i < this.layers.Length; i++)
                    {
                        layers[i] = this.layers[i];
                    }
                    this.layers = layers;
                    this.layers[this.cNodeLayer-1] = new NodeLayer(this.cNodeLayer, this.layers[this.cNodeLayer-2].cOutputs, 1);
                }
            }
            else
            {
                if (subModel == null)
                {
                    this.cNodeLayer = 2;
                    this.cInputs = cIter * cClass;
                    this.layers = new Layer[this.cNodeLayer + 1];

                    this.layers[0] = new InputLayer(boostedDTs, cIter);
                    this.layers[1] = new NodeLayer(1, this.cInputs, cClass);//multiplex layer
                    this.layers[2] = new NodeLayer(2, cClass); //dotproduct node layer 
                }
                else
                {
                    throw new Exception("Multiclass sub-mart has not been implemented yet");
                }
            }
        }

        private static bool IsType(DataSection dataSec)
        {
            if (dataSec.Name.StartsWith("NeuralNet"))
            {
                return true;
            }
            return false;
        }

        private void Init(List<InputTransform> inputList, List<NodeLayer> layerList, List<Node> nodeList)
        {
            Debug.Assert(layerList.Count == this.cNodeLayer, "NN Layers mismatch");
            
            this.layers = new Layer[this.cNodeLayer + 1]; //nodeLayers + inputLayer
            this.layers[0] = new InputLayer(inputList);

            foreach (NodeLayer nodeLayer in layerList)
            {
                nodeLayer.AddNodes(nodeList);
                layers[nodeLayer.LayerID] = nodeLayer; 
            }            
        }        

        public void WriteMSNStyle(StreamWriter wStream)
        {
            wStream.WriteLine("[NeuralNet]");
            wStream.WriteLine("Layers={0}", this.cNodeLayer);
            wStream.WriteLine("Inputs={0}", this.cInputs);
            wStream.WriteLine();

            //writing layers
            for (int i = 0; i < this.layers.Length; i++)
            {
                this.layers[i].WriteMSNStyle(wStream);
            }                        
        }

        public void WriteMSNStyle(string outFileName)
        {
            FileStream file = new FileStream(outFileName, FileMode.Create);
            StreamWriter wStream = new StreamWriter(file);
            this.WriteMSNStyle(wStream);
            wStream.Close();
        }       

        override public bool SetFeatureNames(string[] FeatureNames)
        {
            InputLayer inputLayer = (InputLayer)layers[0];
            return inputLayer.SetFeatureNames(FeatureNames);            
        }

        override public float Evaluate(float[] features)
        {            
            float[] results = new float[1];
            if (Evaluate(features, results))
            {
                return results[0];
            }
            return 0;
        }

        /// <summary>
        /// compute the rank score for each input feature vector
        /// </summary>
        /// <param name="features">input feature of a data point</param>        
        /// <param name="results">the output score of the data</param>
        /// <returns></returns>
        override public bool Evaluate(float[] features, float[] results)
        {
            float[] inputs = features;
            for (int i = 0; i < this.layers.Length; i++)
            {
                float[] outputs = this.layers[i].FProp(inputs);
                inputs = outputs;
            }
            for (int i = 0; i < results.Length; i++)
            {
                results[i] = inputs[i];
            }
            return true;
        }

        private Layer[] layers;
        private int cNodeLayer;
        private int cInputs;
    }

}
