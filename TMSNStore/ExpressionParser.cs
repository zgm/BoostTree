using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace Microsoft.TMSN.Data {
	/// <summary>
	/// Parser class for evaluating a txp expression.
	/// </summary>
	public class ExpressionParser {
		private List<ExecutionOperator> _executionOperators = null;
		private TStoreProcessor _processor = null;
        
		internal class SourceStack {
            private ArrayList _stack = new ArrayList();

            public void Push(RecordSource source) {
                _stack.Add(source);
            }

            public RecordSource Pop() {
                int numEls = _stack.Count;
                if (numEls == 0) return null;
                RecordSource source = (RecordSource)_stack[numEls - 1];
                _stack.RemoveAt(numEls - 1);

                return source;
            }

            public int Size {
                get {
                    return _stack.Count;
                }
            }
        }

        internal interface ExecutionOperator {
            bool TestToken(string token);
            void Execute(SourceStack stack, TStoreProcessor processor, string token);
            string Description { get;}
		}

		#region OPERATOR_CLASSES
		internal class InputOp : ExecutionOperator {
			private bool _stdinUsed = false;

            public bool TestToken(string token) {
				if (token.StartsWith("store:")) { // if there's a protocol attached
					token = token.Substring(6);
				}

                bool exists = (File.Exists(token) || Directory.Exists(token)
                               || token.Equals("null") || token.Equals("null0")
                               || token.Equals("$"));

				// filter files end in .cs.
				if (token.EndsWith(".cs") || !exists) return false;

				// we only allow one stdin in the expression because it's impossible
				// to exhaust two stdin's.
				if (token.Equals("$")) {
					if (_stdinUsed) {
						Console.WriteLine("Only one stdin allowed per execution graph");
						Environment.Exit(1);
					}
					_stdinUsed = true;
				}

				return true;
            }

            public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
                // case: uri's (flatfile, directory, tstore, recordFile)

                RecordSource source = processor.Input(token);
                stack.Push(source);
            }

            public string Description {
                get {
                    return "<filePath>\t0\tfile, dir, TStore or $ (stdin)";
                }
            }
        }
		internal class FilterFileOp : ExecutionOperator {
			public bool TestToken(string token) {
				return token.EndsWith(".cs");
			}

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				// case: cs sharp filter file
				RecordSource source = stack.Pop();
				source = processor.FilterByCSharpSnippet(source, token);
				stack.Push(source);
			}

			public string Description {
				get {
					return "<filename>.cs\t1\tIRecordFilter provided in a file.";
				}
			}
		}
		internal class AboutOp : ExecutionOperator {
			public bool TestToken(string token) {
				return token.Equals("About");
			}

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				// case: cs sharp filter file
				RecordSource source = stack.Pop();
				source = processor.GetStatistics(source);
				stack.Push(source);
			}

			public string Description {
				get {
					return "About\t\t1\tInput source ignored. Output source is information about input source.";
				}
			}
		}
		internal class SortOp : ExecutionOperator {
			private Regex sortReduceRegex = new Regex(@"^Sort\((?<d>\+|\-)\)$", RegexOptions.Compiled);

			public bool TestToken(string token) {
				return token.StartsWith("Sort(");
			}

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				Match m = sortReduceRegex.Match(token);
				if (!m.Success) {
					Console.WriteLine("Syntax error in Sort()");
					Environment.Exit(1);
				}

				string direction = m.Groups["d"].ToString().Trim();
				bool ascending = true;
				if (direction.Equals("-")) {
					ascending = false;
				}

				RecordSource source = stack.Pop();
				source.SortReduce(ascending, false); // default to no reduction.  If reduction comes next, this is updated.
				stack.Push(source);
			}

			public string Description {
				get {
					return "Sort(+|-)\t1\tSorts record source. (+ = ascending, - = descending)";
				}
			}
		}
		internal class ReduceOp : ExecutionOperator {
			private Regex sortReduceRegex = new Regex(@"^Reduce$", RegexOptions.Compiled);

			public bool TestToken(string token) {
				return token.StartsWith("Reduce");
			}

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				Match m = sortReduceRegex.Match(token);
				if (!m.Success) {
					Console.WriteLine("Syntax error in Reduce");
					Environment.Exit(1);
				}
				RecordSource source = stack.Pop();
				source.Reduce();
				stack.Push(source);
			}

			public string Description {
				get {
					return "Reduce\t\t1\tReduces record source";
				}
			}
		}
        internal class SortedMergeOp : ExecutionOperator {
            private Regex regex = new Regex(@"Merge\((?<n>[^,]+),(?<d>\+|\-)\)", RegexOptions.Compiled);

            public bool TestToken(string token) {
                return token.StartsWith("Merge(");
            }

            public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
                Match m = regex.Match(token);
                if (!m.Success) {
                    Console.WriteLine("syntax error in Merge()");
                    Environment.Exit(0);
                }

				string direction = m.Groups["d"].ToString().Trim();
				bool ascending = true;
				if (direction.Equals("-")) {
					ascending = false;
				}

                int nary = int.Parse(m.Groups["n"].ToString());

				// set the direction of the empty source so that a sorter doesn't get inserted
				// to sort nothing.

				RecordSource source = processor.EmptySource(true, ascending);
                for (int i = 0; i < nary; i++) {
                    RecordSource next = stack.Pop();
                    if (ascending) source = processor.Pair(next, source, PairOperation.MergeAscend);
                    else source = processor.Pair(next, source, PairOperation.MergeDescend);
                }

				stack.Push(source);
            }

            public string Description {
                get { return "Merge(n,+|-)\tn\tMerges sorted record sources. (int n == arity) (+ = ascending, - = descending)"; }
            }
        }
        internal class ConcatOp : ExecutionOperator {
			private Regex regex = new Regex(@"Concat\((?<a>[^,]+)\)", RegexOptions.Compiled);

            public bool TestToken(string token) {
                return token.StartsWith("Concat(");
            }

            public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
                Match m = regex.Match(token);
                if (!m.Success) {
                    Console.WriteLine("syntax error in Concat()");
                    Environment.Exit(0);
                }

                int arity = int.Parse(m.Groups["a"].ToString());
				if (arity > stack.Size) {
					Console.WriteLine("Concat arity greater than stack size.");
					Environment.Exit(0);					
				}

                RecordSource right = stack.Pop();
				for (int i = 1; i < arity; i++) {
                    RecordSource left = stack.Pop();
                    right = processor.Pair(left, right, PairOperation.CatLeftThenRight);
                }

				stack.Push(right);
            }

            public string Description {
                get {
                    return "Concat(n)\tn\tConcatenates record sources. (int n == arity)";
                }
            }
        }
        internal class WhereClauseOp : ExecutionOperator {
			// lazy evaluation
			private Regex regex = new Regex(@"Where\((?<c>.+)\)$", RegexOptions.Compiled);

            public bool TestToken(string token) {
				return token.StartsWith("Where(");
            }

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				Match m = regex.Match(token);
				if (!m.Success) {
					Console.WriteLine("syntax error in Where()");
					Environment.Exit(0);
				}

				string clause = m.Groups["c"].ToString().Trim();
				RecordSource source = stack.Pop();
				source = processor.SelectFilter(source, clause);
				stack.Push(source);
			}

            public string Description {
                get { return "Where(r)\t1\tFilters source based on 'record' expression";}
            }
        }
        internal class InnerJoinOp : ExecutionOperator {
            public bool TestToken(string token) {
                return token.StartsWith("InnerJoin");
            }

            public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
                RecordSource right = stack.Pop();
                RecordSource left = stack.Pop();
                RecordSource source = processor.InnerJoin(left, right);
                stack.Push(source);
            }

            public string Description {
                get { return "InnerJoin\t2\tPerforms Inner join"; }
            }
        }
        internal class LeftOuterJoinOp : ExecutionOperator {
            public bool TestToken(string token) {
                return token.StartsWith("LeftOuterJoin");
            }

            public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
                RecordSource right = stack.Pop();
                RecordSource left = stack.Pop();
                RecordSource source = processor.LeftOuterJoin(left, right);
                stack.Push(source);
            }

            public string Description {
                get { return "LeftOuterJoin\t2\tPerforms Left outer join"; }
            }
        }
        internal class TableOp : ExecutionOperator {
            private Regex regex = new Regex(@"^ToTable\((?<k>.+?)\)$", RegexOptions.Compiled);
            
            public bool TestToken(string token) {
                return token.StartsWith("ToTable(");
            }

            public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
                Match m = regex.Match(token);
                if (!m.Success) {
                    Console.WriteLine("syntax error in ToTable()");
                    Environment.Exit(0);
                }

                string keyExpression = m.Groups["k"].ToString().Trim();
				string[] headers = null;
				if (keyExpression.IndexOf(',') >= 0) {
					string[] pieces = keyExpression.Split(',');
					keyExpression = pieces[0].Trim();
					headers = pieces[1].Trim().Split('+');
				}

				RecordSource source = stack.Pop();
				string sourceName = Path.GetFileNameWithoutExtension(source.CurrentSourceName);
				ToTableFilter filter = new ToTableFilter(keyExpression, processor.TableColumnSeparator, sourceName);
				filter.ColumnNames = headers;
                source = processor.Filter(source, filter);
                stack.Push(source);
            }

            public string Description {
                get { return "ToTable(k)\t1\tConverts to table Records. (k == <key col exp>)"; }
            }
        }
		internal class DataOp : ExecutionOperator {
			private Regex regex = new Regex(@"^ToLine\((?<k>[^\)]+)\)$", RegexOptions.Compiled);

			public bool TestToken(string token) {
				return token.StartsWith("ToLine(");
			}

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				Match m = regex.Match(token);
				if (!m.Success) {
					Console.WriteLine("syntax error in ToLine()");
					Environment.Exit(0);
				}

				string keyExpression = m.Groups["k"].ToString().Trim();

				RecordSource source = stack.Pop();
				string sourceName = Path.GetFileNameWithoutExtension(source.CurrentSourceName);
				ToDataFilter filter = new ToDataFilter(keyExpression, processor.TableColumnSeparator, sourceName);
				source = processor.Filter(source, filter);
				stack.Push(source);
			}

			public string Description {
				get { return "ToLine(k)\t1\tConverts to line (data) records. Basically a select. (k == <key col exp>)"; }
			}
		}
		internal class CountOp : ExecutionOperator {
			private Regex regex = new Regex(@"^ToCount\((?<k>[^\)]+)\)$", RegexOptions.Compiled);

			public bool TestToken(string token) {
				return token.StartsWith("ToCount(");
			}

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				Match m = regex.Match(token);
				if (!m.Success) {
					Console.WriteLine("syntax error in ToCount()");
					Environment.Exit(0);
				}

				string keyExpression = m.Groups["k"].ToString().Trim();
				string countExpression = null;
				if (keyExpression.IndexOf(',') >= 0) {
					string[] pieces = keyExpression.Split(',');
					keyExpression = pieces[0].Trim();
					countExpression = pieces[1].Trim();
				}

				RecordSource source = stack.Pop();
				string sourceName = Path.GetFileNameWithoutExtension(source.CurrentSourceName);
				ToCountFilter filter = new ToCountFilter(keyExpression, countExpression, processor.TableColumnSeparator, sourceName);
				source = processor.Filter(source, filter);
				stack.Push(source);
			}

			public string Description {
				get { return "ToCount(k[,c])\t1\tConverts to count records. (k == <key col exp>) (c == <count col exp>)"; }
			}
		}
		internal class HeadOp : ExecutionOperator {
			private Regex regex = new Regex(@"Head\((?<l>[^\)]+)\)", RegexOptions.Compiled);

			public bool TestToken(string token) {
				return token.StartsWith("Head(");
			}

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				Match m = regex.Match(token);
				if (!m.Success) {
					Console.WriteLine("syntax error in Head()");
					Environment.Exit(0);
				}

				long limit = long.Parse(m.Groups["l"].ToString().Trim());

				RecordSource source = stack.Pop();
				source.Limit(limit);
				stack.Push(source);
			}

			public string Description {
				get { return "Head(n)\t\t1\tLimits record source to first 'n' records.  (int n == numRecords)"; }
			}
		}
		internal class RandomOp : ExecutionOperator {
			private Regex regex = new Regex(@"Random\((?<l>[^)]+)\)", RegexOptions.Compiled);

			public bool TestToken(string token) {
				return token.StartsWith("Random(");
			}

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				Match m = regex.Match(token);
				if (!m.Success) {
					Console.WriteLine("syntax error in Random()");
					Environment.Exit(0);
				}

				string number = m.Groups["l"].ToString().Trim();
				int seed = 31415927;

				if (number.IndexOf(',') > 0) {
					string[] pieces = number.Split(',');
					number = pieces[0].Trim();
					seed = int.Parse(pieces[1].Trim());
				}

				int numToKeep = int.Parse(number);

				RecordSource source = stack.Pop();
				source.Random(numToKeep, seed);
				stack.Push(source);
			}

			public string Description {
				get { return "Random(n[,s])\t1\tChooses random records from input source.  (int n == numRecords) (int s == seed)"; }
			}
		}
		internal class FilterLeftInRightOp : ExecutionOperator {

			public bool TestToken(string token) {
				return token.StartsWith("FilterByRight");
			}

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				RecordSource right = stack.Pop();
				RecordSource left = stack.Pop();
				RecordSource source = processor.Pair(left, right, PairOperation.FilterLeftInRight);
				stack.Push(source);
			}

			public string Description {
				get {
					return "FilterByRight\t2\tPasses through records in left whose keys are in the right source.";
				}
			}
		}
		internal class FilterLeftNotInRightOp : ExecutionOperator {

			public bool TestToken(string token) {
				return token.StartsWith("FilterNotRight");
			}

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				RecordSource right = stack.Pop();
				RecordSource left = stack.Pop();
				RecordSource source = processor.Pair(left, right, PairOperation.FilterLeftNotInRight);
				stack.Push(source);
			}

			public string Description {
				get {
					return "FilterNotRight\t2\tPasses through records in left whose keys are NOT in the right source.";
				}
			}
		}
		internal class SearchOp : ExecutionOperator {
			public bool TestToken(string token) {
				return (token.StartsWith("'") && token.EndsWith("'"));
			}

			public void Execute(SourceStack stack, TStoreProcessor processor, string token) {
				RecordSource source = stack.Pop();
				token = token.Trim("'".ToCharArray());
				source = processor.QueryKey(source, token);
				stack.Push(source);
			}

			public string Description {
				get { return "'<query>'\t1\tFilters by key (searches TStores)"; }
			}
		}
		#endregion

		/// <summary>
		/// Parser for evaluating a txp expression.
		/// </summary>
		internal ExpressionParser(TStoreProcessor processor) {
			_executionOperators = new List<ExecutionOperator>();
			_processor = processor;

			#region OPERATORS
            _executionOperators.Add(new InputOp());
            _executionOperators.Add(new FilterFileOp());
			_executionOperators.Add(new SortOp());
			_executionOperators.Add(new ReduceOp());
            _executionOperators.Add(new WhereClauseOp());
			_executionOperators.Add(new SearchOp());
			_executionOperators.Add(new TableOp());
			_executionOperators.Add(new CountOp());
			_executionOperators.Add(new DataOp());
			_executionOperators.Add(new HeadOp());
			_executionOperators.Add(new RandomOp());
			_executionOperators.Add(new AboutOp());
			_executionOperators.Add(new InnerJoinOp());
            _executionOperators.Add(new LeftOuterJoinOp());
			_executionOperators.Add(new FilterLeftInRightOp());
			_executionOperators.Add(new FilterLeftNotInRightOp());
			_executionOperators.Add(new SortedMergeOp());
			_executionOperators.Add(new ConcatOp());
			#endregion
		}

        private static Thread _mainThread = null;

        private static void ControlCHandler(object sender, ConsoleCancelEventArgs args) {
            args.Cancel = true;  // So main thread doesn't die after returning
            _mainThread.Abort();
        }

		/// <summary>
		/// This method creates an internal execution graph for the expression and evaluates it.
		/// </summary>
		/// <param name="txpExpression">The txp expression as described in the PrintSyntax method.</param>
		public void Evaluate(string txpExpression) {
            _mainThread = Thread.CurrentThread;
            Console.CancelKeyPress += new ConsoleCancelEventHandler(ControlCHandler);
            
            #region COMMANDFILE
            // if the expression is in a file: @<filename>
            if (txpExpression.StartsWith("@")) {
                StringBuilder sb = new StringBuilder();
                StreamReader sr = new StreamReader(txpExpression.Substring(1));
                while (true) {
                    string line = sr.ReadLine();
                    if (line == null) break;

                    sb.Append(line);
                }

                sr.Close();
                txpExpression = sb.ToString();
            }
            #endregion

            char[] delims = "^".ToCharArray();
            int outSymb = txpExpression.LastIndexOf('>');

			// if there's no output symbol assume stdout e.g. > $
			string outputUri = "$";
			string inputExpression = txpExpression; 

            if (outSymb >= 0) {
				outputUri = txpExpression.Substring(outSymb + 1).Trim();
				inputExpression = txpExpression.Substring(0, outSymb).Trim();
            }

            string[] pieces = inputExpression.Split(delims);
			for (int i = 0; i < pieces.Length; i++) {
				pieces[i] = pieces[i].Trim();
			}

            SourceStack sourceStack = new SourceStack();

			for (int i = 0; i < pieces.Length; i++) {
				string token = pieces[i];

				bool recognized = false;
				foreach (ExecutionOperator op in _executionOperators) {
					if (op.TestToken(token)) {
						op.Execute(sourceStack, _processor, token);
						recognized = true;
						break;
					}
				}

				// unknown token
				if (!recognized) {
					Console.WriteLine("OPERATORS");
					for (int j = 0; j < pieces.Length; j++) {
						Console.Write("<" + pieces[j] + ">");
						if (j == i) Console.WriteLine(" <-- illegal operator");
						else Console.WriteLine();
					}

					Environment.Exit(-1);
				}
			}

            if (sourceStack.Size != 1) {
                Console.WriteLine("invalid txpExpression (stack not empty)");
                Environment.Exit(-1);
            }

            RecordSource outputSource = sourceStack.Pop();

			// evaluate it 
            try {
                outputSource.Write(outputUri);
            }

            // caused by control-c
            catch (ThreadAbortException) {
                outputSource.InternalSource.Close();
                Environment.Exit(0);
            }
		}

		/// <summary>
		/// This method prints the txp expression syntax to the Console.
		/// </summary>
		public void PrintSyntax() {
			Console.WriteLine("Expression Syntax (RPN)");
			Console.WriteLine();
			Console.WriteLine("<operator> [^ <operator> ] [> <outputUri>]");
			Console.WriteLine();
			Console.WriteLine("Operator\tArity\tDescription");
			Console.WriteLine("=========\t=====\t===========");
			foreach (ExecutionOperator op in _executionOperators) {
				Console.WriteLine(op.Description);
			}

			Console.WriteLine();
			Console.WriteLine("<outputUri> = [<filename> | store:<directory> | $ (stdout)]");
			Console.WriteLine("<column expression> = <col> [+ <col>]");
			Console.WriteLine(" <col> = [<columnName> | $<columnNo> for a TableRecord or column delimited flat file");
			Console.WriteLine(" <col> = $<columnNo> for any other type of DataRecord.  Columns across Key and DataAsString.");			
		}
	}
}
