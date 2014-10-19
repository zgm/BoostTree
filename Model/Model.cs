using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace StochasticGradientBoost
{
    [Serializable]
    public class Model
    {
        /// <summary>
        /// Informa the model the names of the inpute features.
        /// The actual input feature vector passed in during model evaluation time
        /// has to confirm the same order and dimension of the features specified here.
        /// The function returns true if the model is able to handle the input features;
        /// otherwise, it returns false.
        /// </summary>
        /// <param name="FeatureNames">input feature paramemters</param>
        virtual public bool SetFeatureNames(string[] FeatureNames)
        {
            return false;
        }

        /// <summary>
        /// The model computes a score give a input feature vector
        /// </summary>
        /// <param name="features">input feature vector to the model</param>
        /// <returns></returns>
        virtual public float Evaluate(float[] features)
        {
            return (float)0.0;
        }

        /// <summary>
        /// The model computes a score corresponding to each label:
        /// for example, the probabilities of each class in a multi-class
        /// classification problem
        /// </summary>
        /// <param name="features">input feature to the model</param>
        /// <param name="labels">the label of the classes that the model has to compute a score for each</param>
        /// <param name="results">the score computed by the model corresponding to each label</param>
        /// <returns></returns> true if successful false otherwise
        virtual public bool Evaluate(float[] features, float[] results)
        {
            return false;
        }       
    }

    [Serializable]
    public class DummyClassifyModel : Model
    {
        public DummyClassifyModel(string[] parameters)
        {
        }

        override public bool SetFeatureNames(string[] FeatureNames)
        {
            return true;
        }

        override public float Evaluate(float[] features)
        {
            return 0;
        }

        /// <summary>
        /// produces equal probabilities corresponding to every class
        /// </summary>
        /// <param name="features"></param>
        /// <param name="labels"></param>
        /// <param name="results"></param>
        /// <returns></returns>
        override public bool Evaluate(float[] features, float[] results)
        {
            int cClasses = results.Length;
            float prob = (float)1.0 / (float)cClasses;
            for (int i = 0; i < cClasses; i++)
            {
                results[i] = prob;
            }
            return true;
        }
    }

    [Serializable]
    public class NNLayer
    {
        enum StateParse { Begin, StartNodes, EndNodes,NWeights, Weights, Thresholds, End };

        public NNLayer()
        {                    
        }
       
        public bool FProp(float[] inputs)
        {
            if (inputs.Length != this.nInputs)
            {
                return false;
            }
            for (int i = 0; i < this.nOutputs; i++)
            {
                this.outputs[i] = this.thresholds[i];
                for (int j = 0; j < this.nInputs; j++)
                {
                    this.outputs[i] += this.weights[i][j] * inputs[j];
                }
                this.outputs[i] = (float)this.transFunc.Eval(this.outputs[i]);                 
            }
            return true;
        }

        public float[] Outputs
        {
            get
            {
                return this.outputs;
            }
        }

        public void Load(string fileName, TransferFunction transFunc)
        {
            int iWeight = 0;
            int iThresh = 0;
            StateParse curState = StateParse.Begin;
            StreamReader modelFile = new StreamReader(fileName);            
            while (!modelFile.EndOfStream)
            {
                string line = modelFile.ReadLine();
                if (StateParse.Begin == curState)
                {
                    if (line.Equals("nStartNodes", StringComparison.OrdinalIgnoreCase))
                    {
                        curState = StateParse.StartNodes;
                    }
                }
                else if (StateParse.StartNodes == curState)
                {
                    if (line.Equals("nEndNodes", StringComparison.OrdinalIgnoreCase))
                    {
                        curState = StateParse.EndNodes;
                    }
                    else
                    {
                        nInputs = int.Parse(line);
                    }
                }
                else if (StateParse.EndNodes == curState)
                {
                    if (line.Equals("nWeights", StringComparison.OrdinalIgnoreCase))
                    {
                        curState = StateParse.NWeights;
                    }
                    else
                    {
                        this.nOutputs = int.Parse(line);
                        this.outputs = new float[nOutputs];
                        this.weights = new float[nOutputs][];
                        this.thresholds = new float[nOutputs];
                        for (int i = 0; i < nOutputs; i++)
                        {
                            weights[i] = new float[nInputs];
                        }
                    }
                }
                else if (StateParse.NWeights == curState)
                {                                            
                    int.Parse(line); // == nOutputs * nInputs
                    curState = StateParse.Weights;
                        
                }
                else if (StateParse.Weights == curState)
                {
                    if (line.Equals("thresholds", StringComparison.OrdinalIgnoreCase))
                    {
                        curState = StateParse.Thresholds;
                    }
                    else
                    {
                        float w = float.Parse(line);
                        int iOut = iWeight / nInputs;
                        int iIn = iWeight % nInputs;
                        weights[iOut][iIn] = w;
                        iWeight++;
                    }
                }
                else if (StateParse.Thresholds == curState)
                {
                    float t = float.Parse(line);
                    this.thresholds[iThresh++] = t;
                }
            }
            
            modelFile.Close();

            this.transFunc = transFunc;
        }

        private TransferFunction transFunc = null;
        private int nInputs = 0;
        private int nOutputs = 0;        
        private float[][] weights = null;
        private float[] outputs = null;
        private float[] thresholds = null;
    }

    [Serializable]
    public class NNModel : Model
    {
        private int nLayer = 0; 
        private NNLayer[] nnLayers = null;
       
        public NNModel(string[] parameters)
        {
            nLayer = parameters.Length;
            nnLayers = new NNLayer[nLayer];
            for (int i=0; i< nLayer; i++)
            {
                nnLayers[i] = new NNLayer();
                TransferFunction transFunc = (i == nLayer - 1) ? TransferFunction.Linear : TransferFunction.Tanh;
                nnLayers[i].Load(parameters[i], transFunc);
            }            
        }        

        override public bool SetFeatureNames(string[] FeatureNames)
        {            
            return true;
        }

        override public float Evaluate(float[] features)
        {
            float[] results = null;
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
            for (int i = 0; i < this.nLayer; i++)
            {
                this.nnLayers[i].FProp(inputs);
                inputs = this.nnLayers[i].Outputs;
            }
            results[0] = this.nnLayers[this.nLayer - 1].Outputs[0];
            return true;
        }
    }

    [Serializable]
    public abstract class TransferFunction 
    {
        public static readonly TransferFunction Tanh = new Tanh();
        public static readonly TransferFunction Linear = new Linear();
        public static readonly TransferFunction Sigmoid = new Sigmoid();
        
        abstract public string NameString
        {
            get;
        }

#if !IMPRECISE
        public abstract double Eval(double x);
        public abstract double Grad(double x);
        // Can be cheaper with one call.
        public abstract double EvalNGrad(double x, out double grad);
        public abstract decimal EvalNGrad(decimal x, out decimal grad);
#else
		public abstract float Eval(float x);
		public abstract float Grad(float x);
		// Can be cheaper with one call.
		public abstract float EvalNGrad(float x, out float grad);
#endif
    }

    [Serializable]
    public sealed class Tanh : TransferFunction
    {
        override public string NameString
        {
            get
            {
                return "Tanh";
            }
        }        

#if !IMPRECISE
        public override double Eval(double x)
        {
            return Math.Tanh(x);
        }
        public override double Grad(double x)
        {
            double tmp = Math.Tanh(x);
            return 1.0 - tmp * tmp;
        }

        public override double EvalNGrad(double x, out double grad)
        {
            double act = Math.Tanh(x);
            grad = 1.0 - act * act;
            return act;
        }
        public override decimal EvalNGrad(decimal x, out decimal grad)
        {
            decimal act = (decimal)Math.Tanh((double)x);
            grad = (decimal)1.0 - act * act;
            return act;
        }
#else
		public override float Eval(float x)
		{
			return TanhFunction(x);
		}
		public override float Grad(float x)
		{
			float tmp = TanhFunction(x);
			return 1.0F - tmp*tmp;
		}

		private override static float TanhFunction(float x)
		{
			return (float)Math.Tanh(x);
		}

		public override float EvalNGrad(float x, out float grad)
		{
			float act = TanhFunction(x);
			grad = 1.0F - act*act;
			return act;
		}
#endif

    }

    [Serializable]
    public sealed class Sigmoid : TransferFunction
    {
        override public string NameString
        {
            get
            {
                return "Sigmoid";
            }
        }

        public override double Eval(double x)
        {
            return 1/(Math.Exp(-x)+1.0);
        }
        public override double Grad(double x)
        {
            double tmp = Math.Exp(-x) + 1.0;
            return -Math.Exp(-x)/(tmp * tmp);
        }

        public override double EvalNGrad(double x, out double grad)
        {
            double tmp = Math.Exp(-x) + 1.0;
     
            double act = 1/tmp;
            grad = -Math.Exp(-x) / (tmp * tmp);
            return act;
        }
        public override decimal EvalNGrad(decimal x, out decimal grad)
        {       
            double tmp = Math.Exp((double)-x) + 1.0;

            decimal act = (decimal)(1 / tmp);
            grad = (decimal)(-Math.Exp((double)-x) / (tmp * tmp));
            return act;
        }

    }

    [Serializable]
    public sealed class Linear : TransferFunction
    {
        override public string NameString
        {
            get
            {
                return "Linear";
            }
        }
#if !IMPRECISE
        public override double Eval(double x)
        {
            return x;
        }
        public override double Grad(double x)
        {
            return 1.0;
        }
        public override double EvalNGrad(double x, out double grad)
        {
            grad = 1.0;
            return x;
        }
        public override decimal EvalNGrad(decimal x, out decimal grad)
        {
            grad = (decimal)1.0;
            return x;
        }
#else
		public override float Eval(float x)
		{
			return x;
		}
		public override float Grad(float x)
		{
			return 1.0F;
		}
		public override float EvalNGrad(float x, out float grad)
		{
			grad = 1.0F;
			return x;
		}
#endif
       
    }
}
