using System;
using System.Collections;
using System.IO;
using System.Reflection;
using System.Text;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace Microsoft.TMSN.Data {
	/// <summary>
	/// Summary description for DataProcessNebula.
	/// </summary>
	internal class NebulaLauncher {
		private ArrayList _transferFiles = new ArrayList();
		private string _binaryName;
		private ArrayList _finalSubTaskOutputFiles = new ArrayList();
		private string _failureProcessDir = null;

		public NebulaLauncher() { 
			_GetAssemblyPaths(out _binaryName, _transferFiles);

		}

		public string BinaryName {
			get {
				return _binaryName;
			}
		}

		public string FailureProcessDir {
			get {
				return _failureProcessDir;
			}
		}

		public string[] FinalSubTaskOutputFiles {
			get {
				return (string[])_finalSubTaskOutputFiles.ToArray(typeof(string));
				}
		}

		public int Run(string algebra, string finalSubTaskName) {

			string currentDir = Directory.GetCurrentDirectory();
			string algebraFileName = Path.Combine(currentDir, _binaryName + ".Abr");
			
			StreamWriter sw = new StreamWriter(algebraFileName);
			sw.Write(algebra);
			sw.Close();

			string nebulaArguments = _MakeNebulaArguments(_transferFiles, algebraFileName);

			System.Diagnostics.ProcessStartInfo psi = new ProcessStartInfo();
			string workingDir = Environment.GetEnvironmentVariable("SDROOT");
			workingDir = Path.Combine(workingDir, "private");
			workingDir = Path.Combine(workingDir, "bin");
			workingDir = Path.Combine(workingDir, "cosmos");
			workingDir = Path.Combine(workingDir, "bin");

			psi.WorkingDirectory = workingDir;

			string binPath = Path.Combine(workingDir, "Nebula.exe");
			psi.FileName = binPath;
			psi.Arguments = nebulaArguments;
			psi.UseShellExecute = false;
			psi.RedirectStandardOutput = true;

			System.Diagnostics.Process process = System.Diagnostics.Process.Start(psi);

			StreamWriter nebulaOut = new StreamWriter("nebula.out");
			// read standard out to get the aggregate file names
			ArrayList nebulaOutputFiles = new ArrayList();
			while (true) {
				string line = process.StandardOutput.ReadLine(); // read it in
				if (line == null) break;

				nebulaOut.WriteLine(line);

				if (line.IndexOf("CREATE") != -1) {
					string myline = Regex.Replace(line, @"\s+", " ");
					string [] columns = myline.Split(' ');
					// 0        1         2         3             4           5         6    7  8
					// <taskId> <someNum> <someNum> <machineName> CREATE|COMP SUCC|FAIL pdir = <processDir>
					
#if false
					string machine = columns[3];
					int dash = machine.IndexOf('-');
					if (dash == -1) {
						throw new Exception("bad machine name");
					}
					machine = machine.Substring(dash+1);
#endif

					// my output line:
					// <taskId> <someNum> <someNum> CREATE|COMP SUCC|FAIL <myProcessId>
					string[] pathParts = columns[8].Split('\\');
					string myProcessId = pathParts[pathParts.Length-2];

					string output = columns[0] + " " +
						columns[1] + " " +
						columns[2] + " " +
						columns[4] + " " +
						columns[5] + " " + 
						columns[3] + "-" + myProcessId;
					Console.WriteLine(output);
				}
				
				else if (line.IndexOf("SUCC") != -1) {
					// 0        1     2     3    4    5    6 7  8   9 10  11      12 13   14       15   16 17
					// <taskid> <num> <num> COMP SUCC exit = 0, err = OK, RunTime = x.xxx seconds, pdir = \\...
					string myline = Regex.Replace(line, @"\s+", " ");
					string [] columns = myline.Split(' ');

					string[] pathParts = columns[17].Split('\\');
					string myProcessId = pathParts[2] + "-" + pathParts[pathParts.Length-2];
					string output = columns[0] + " " +
						columns[1] + " " +
						columns[2] + " " +
						columns[3] + " " +
						columns[4] + " " + 
						myProcessId + " " +	columns[13];
					Console.WriteLine(output);
				}

				else {
					Console.WriteLine(line); // write it back out
				}

				// collect file names of final subtask
				if (line.StartsWith(finalSubTaskName) && line.IndexOf("COMP     SUCC exit = 0, err = OK") != -1) {
					Match m = Regex.Match(line, @"pdir = (?<d>.*)");
					if (m.Success) {
						string file = Path.Combine(m.Groups["d"].ToString(), "wd");
						file = Path.Combine(file, "0");
						_finalSubTaskOutputFiles.Add(file);
					}
				}
			
				// collect the first FAIL

				// COMP     FAIL exit = 1, err = The operation failed, pdir = \\tmsncos-02\data\pn\Processes\5EE66DA5-4C06-4D3D-9B60-FA574F7D64A1\
				if (line.IndexOf("COMP     FAIL exit = 1") != -1) {
					Match n = Regex.Match(line, @"pdir = (?<d>.*)");
					if (n.Success) {
						_failureProcessDir = n.Groups["d"].ToString();
					}
				}
			}

			process.WaitForExit();
			nebulaOut.Close();

			return process.ExitCode;

#if false
			// don't continue if exit code isn't zero
			if (process.ExitCode == 0) {
				_ProcessClusterOutput(nebulaOutputFiles);
			} else {
				_PrintNebulaError(failureProcessDir);
				Environment.Exit(process.ExitCode);
			}
#endif
		}


		private static void _PrintNebulaError(string processDir) {
			if (processDir == null) return;

			string processInfo = Path.Combine(processDir, "ProcessInfo.txt");
			StreamReader sr = new StreamReader(processInfo);

			Console.WriteLine("-----------------------------------------------------");
			Console.WriteLine("Nebula Error Processing Command: ");

			while (true) {
				string line = sr.ReadLine();
				if (line == null) break;
				
				Match m = Regex.Match(line, @"\[(?<d>[^\]]+)");
				if (m.Success) {
					Console.WriteLine(m.Groups["d"].ToString());
				}
			}
			sr.Close();
			
			string stdout = Path.Combine(processDir, "wd");
			stdout = Path.Combine(stdout, "stdout.txt");
			Console.WriteLine("");
			Console.Write(stdout);
			Console.WriteLine(":");
			sr = new StreamReader(stdout);
			while (true) {
				string line = sr.ReadLine();
				if (line == null) break;
				Console.WriteLine(line);
			}
			sr.Close();
		}

		private static void _GetAssemblyPaths(out string binaryName, ArrayList paths) {
			// figure out the dlls that Nebula needs to know about

			Hashtable assemblies = new Hashtable();
			Assembly asm = Assembly.GetEntryAssembly();
			assemblies.Add(asm, null); // add it to the ones we know about
			//paths.Add(asm.Location); // the binary itself

			Module[] mods = asm.GetModules();
			string name = mods[0].Name;
			binaryName = Path.GetFileNameWithoutExtension(name);

			string dir = Path.GetDirectoryName(asm.Location);
			// if the pdb file exists in this directory add it too for debugging info
			string pdbFile = Path.Combine(dir, Path.GetFileNameWithoutExtension(asm.Location) + ".pdb");
			if (File.Exists(pdbFile)) {
				paths.Add(pdbFile);
			}

			// recurse
			_CollectAssemblies(asm, assemblies);
			foreach (Assembly a in assemblies.Keys) {
				paths.Add(a.Location);
			}
		}

		private static void _CollectAssemblies(Assembly asm, Hashtable assemblies) {
			AssemblyName[] names = asm.GetReferencedAssemblies();

			for (int i = 0; i < names.Length; i++) {
				Assembly a = Assembly.Load(names[i]);
				if (a.Location.IndexOf(@"windows") != -1) continue;

				if (!assemblies.Contains(a)) {
					assemblies.Add(a, null);
					_CollectAssemblies(a, assemblies);
				}
			}
		}

		public void AddTransferFile(string filePath) {
			_transferFiles.Add(filePath);
		}

		private static string _MakeNebulaArguments(ArrayList filePaths, string algebraFile) {
			StringBuilder sb = new StringBuilder();
			sb.Append(" RunAlgebra");
			foreach (string path in filePaths) {
				sb.Append(" -r ");
				sb.Append(path);
			}
			sb.Append(" ");
			sb.Append(algebraFile);
			return sb.ToString();
		}

#if false
		private void _ProcessClusterOutput(ArrayList nebulaOutputFiles) {
			if (_outputUri.FilePath.StartsWith("cosmos://")) {
				return; // we do nothing because it's all processed and taken
				// care of on the cluster.
			}

			// else we're pulling the output from the cluster to the client

			// nebula's filtered output is a list of our input files to aggregate
			string[] inFiles = (string[]) nebulaOutputFiles.ToArray(typeof(string));

			RecordAggregator aggregator = new RecordAggregator(_recordInstance, inFiles);
			aggregator.ReductionEnabled = _reductionEnabled;

			// outputDir is local
			RecordOutputter outputter = new RecordOutputter(_outputUri, _recordInstance);
			outputter.Input = aggregator;
			outputter.Write();
		}
#endif
	}
}
