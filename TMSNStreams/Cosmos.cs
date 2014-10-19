// owner: rragno

#define LZMA_PLAIN
#define UNBUFFERED

using System;
using System.IO;
using System.Collections;
using System.Collections.Specialized;
using System.Text;
using System.Threading;
using System.Runtime.InteropServices;
//using System.Xml;
//using System.Data;
//using System.Data.OleDb;


namespace Microsoft.TMSN.IO
{

	//// TODO:
	////
	////   - Use central configuration path for all settings
	////   - Make case-insensitive?

	/// <summary>
	/// General functionality supporting Cosmos usage.
	/// </summary>
	/// <remarks>
	/// <p>
	/// This class provides functionality that supports the <see cref="CosmosReadStream"/>
	/// and <see cref="CosmosWriteStream"/> classes. It is also helpful for general Cosmos
	/// usage and inspection.
	/// </p>
	/// <p>
	/// A tmsncosmos.exe, cosmos.cmd, or cosmos.exe command is needed in the path in order for this functionality
	/// to operate correctly.
	/// </p>
	/// <p>
	/// A shared set of tools will be used if no version is found in the path, although this
	/// increases the startup time. The environment variable COSMOS_TOOLS can be used to
	/// specify a directory or share for this purpose, instead of the default.
	/// </p>
	/// </remarks>
	public class Cosmos
	{
		private static bool checkedCosmosToolShare = false;
		private static string cosmosToolShare = @"\\tmsn\cosmos";

		private static string cosmosCmdTmsn = "tmsncosmos.exe";
		private static string cosmosCmd = "cosmos.exe";
		private static string cosmosCmdBackup = "cosmos.cmd";
		private static string cosmosCmdConfigDirectory = null;
		internal static string cosmosCmdFlags = null;
		private static bool cosmosCmdTmsnFailed = false;
		private static bool cosmosCmdTmsnChecked = false;
		private static bool cosmosCmdFailed = false;
		private static bool cosmosCmdChecked = false;
		private static bool cosmosCmdBackupFailed = false;
		private static bool cosmosCmdBackupChecked = false;
		internal static string[] cosmosCmds = { cosmosCmdTmsn, cosmosCmd, cosmosCmdBackup, null };
		private static string cosmosCmdShared = null;
		private static bool cosmosCmdSharedFailed = false;

		internal static readonly string cosmosDirCmdArgs = "dir \"{0}\"";
		internal static readonly string cosmosInCmdArgs = "type \"{0}\"";
		internal static readonly string cosmosExtentsCmdArgs = "streaminfo \"{0}\"";

		internal static readonly string cosmosOutCmdArgs = "copy {1} stdin \"{0}\"";
		internal static readonly string cosmosDeleteCmdArgs = "delete \"{0}\"";
		internal static readonly string cosmosCmdAppendArg = "-a ";
		internal static readonly string cosmosCmdLineBoundariesArg = "-L ";
		internal static readonly string cosmosAppendCmdArgs = "appendrange \"{1}\" \"{0}\" 0 -1";
		//internal static readonly string cosmosBatchCmdArgs = "batchbreak";
		internal static readonly string cosmosBatchCmdArgs = "batch";

		private string CosmosToolShare
		{
			get
			{
				if (!checkedCosmosToolShare)
				{
					// app.config?
					string env = Environment.GetEnvironmentVariable("COSMOS_TOOLS");
					if (env != null && (env = env.Trim()).Length != 0)
					{
						cosmosToolShare = env;
					}
					checkedCosmosToolShare = true;
				}
				return cosmosToolShare;
			}
		}

		/// <summary>
		/// Set the environment and directory as needed.
		/// </summary>
		/// <param name="psi">the process information to modify</param>
		internal static void ConfigureExec(System.Diagnostics.ProcessStartInfo psi)
		{
			// hack to support current cosmos.exe limitations:
			//psi.WorkingDirectory = Path.GetDirectoryName(cosmosCmd);
			// that is still needed, if operating in a root directory:
			string cwd = Path.GetFullPath(Environment.CurrentDirectory);
			if (Path.GetPathRoot(cwd) == cwd)
			{
				string cmd = null;
				for (int i = 0; i < cosmosCmds.Length; i++)
				{
					if (cosmosCmds[i] != null)
					{
						cmd = cosmosCmds[i];
						break;
					}
				}
				if (cmd != null)
				{
					psi.WorkingDirectory = Path.GetDirectoryName(cmd);
				}
			}
			if (cosmosCmdConfigDirectory != null)
			{
				// just do it for the subprocess, for now:
				//#if DOTNET2
				//                // set it for our process!
				//                // (this enables the stream creation to also work)
				//                string val = Environment.GetEnvironmentVariable("APINIFILE");
				//                if (val == null || val.Length == 0)
				//                {
				//                    Environment.SetEnvironmentVariable("APINIFILE",
				//                        Path.Combine(cosmosCmdConfigDirectory, "autopilot.ini"));
				//                }
				//                val = Environment.GetEnvironmentVariable("APENVIRONMENTDIR");
				//                if (val == null || val.Length == 0)
				//                {
				//                    Environment.SetEnvironmentVariable("APENVIRONMENTDIR",
				//                        cosmosCmdConfigDirectory);
				//                }
				//#else
				//                                // no Set in .NET 1.1!!! ***
				//                                // NOTE: Reading and writing streams still will not work!!
				//                                psi.EnvironmentVariables["APINIFILE"] = Path.Combine(cosmosCmdConfigDirectory, "autopilot.ini");
				//                                psi.EnvironmentVariables["APENVIRONMENTDIR"] = cosmosCmdConfigDirectory;
				//#endif
				string val = psi.EnvironmentVariables["APINIFILE"];
				if (val == null || val.Length == 0)
				{
					psi.EnvironmentVariables["APINIFILE"] = Path.Combine(cosmosCmdConfigDirectory, "autopilot.ini");
				}
				val = psi.EnvironmentVariables["APENVIRONMENTDIR"];
				if (val == null || val.Length == 0)
				{
					psi.EnvironmentVariables["APENVIRONMENTDIR"] = cosmosCmdConfigDirectory;
				}
			}
		}

		/// <summary>
		/// Helper function to reliably execute a comsos.exe command.
		/// </summary>
		/// <param name="args">the arguments to provide to cosmos.exe</param>
		/// <returns>A Process for the executing command</returns>
		internal static System.Diagnostics.Process CosmosExec(string args)
		{
			System.Diagnostics.Process proc = null;
			// tmsncosmos.exe is preferred, currently. It removes the limit
			// on line length and automatically finds the proper ini files.
			// It also supports batch modes, and has some tweaks.
			if (!cosmosCmdTmsnFailed)
			{
				try
				{
					if (!cosmosCmdTmsnChecked)
					{
						try
						{
							if (File.Exists(Path.Combine(DllPath, cosmosCmdTmsn)))
							{
								cosmosCmdTmsn = Path.Combine(DllPath, cosmosCmdTmsn);
							}
							else if (File.Exists(Path.Combine(Path.Combine(DllPath, "bin"), cosmosCmdTmsn)))
							{
								cosmosCmdTmsn = Path.Combine(DllPath, cosmosCmdTmsn);
							}
							else
							{
								cosmosCmdTmsn = IOUtil.FindInPath(cosmosCmdTmsn, true);
							}
							cosmosCmds[0] = cosmosCmdTmsn;
						}
						catch
						{
							// ignore...
						}
						cosmosCmdTmsnChecked = true;
					}
					if (cosmosCmdTmsn != null)
					{
						System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
							cosmosCmdTmsn, args);
						ConfigureExec(psi);
						psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
						psi.CreateNoWindow = true;
						psi.RedirectStandardInput = false;
						psi.RedirectStandardOutput = true;
						psi.RedirectStandardError = true;
#if DOTNET2
						psi.StandardOutputEncoding = Encoding.UTF8;
#endif
						psi.UseShellExecute = false;
						proc = System.Diagnostics.Process.Start(psi);
						// should not do this if we ever want the batch output:
						IOUtil.ConsumeBackground(proc.StandardError);
					}
					else
					{
						cosmosCmdTmsnFailed = true;
						cosmosCmds[0] = null;
					}
				}
				catch
				{
					cosmosCmdTmsnFailed = true;
					cosmosCmds[0] = null;
				}
			}

			if (proc == null)
			{
				if (!cosmosCmdFailed)
				{
					try
					{
						if (!cosmosCmdChecked)
						{
							try
							{
								if (File.Exists(Path.Combine(DllPath, cosmosCmd)))
								{
									cosmosCmd = Path.Combine(DllPath, cosmosCmd);
								}
								else if (File.Exists(Path.Combine(Path.Combine(DllPath, "bin"), cosmosCmd)))
								{
									cosmosCmd = Path.Combine(DllPath, cosmosCmd);
								}
								else
								{
									cosmosCmd = IOUtil.FindInPath(cosmosCmd, true);
								}
								cosmosCmds[1] = cosmosCmd;
								// maybe should check if environment variables already exist?
								if (cosmosCmd != null)
								{
									cosmosCmdConfigDirectory = Path.GetDirectoryName(cosmosCmd);
									if (File.Exists(Path.Combine(cosmosCmdConfigDirectory, "tmsncosmos.ini")))
									{
										cosmosCmdFlags = "-c\"" + Path.Combine(cosmosCmdConfigDirectory, "tmsncosmos.ini") + "\" ";
									}
									else if (File.Exists(Path.Combine(cosmosCmdConfigDirectory, "..\\tmsncosmos.ini")))
									{
										cosmosCmdFlags = "-c\"" + IOUtil.PathCombine(cosmosCmdConfigDirectory, "..\\tmsncosmos.ini") + "\" ";
									}
									else if (File.Exists(Path.Combine(cosmosCmdConfigDirectory, "cosmos.ini")))
									{
										cosmosCmdFlags = "-c\"" + Path.Combine(cosmosCmdConfigDirectory, "cosmos.ini") + "\" ";
									}
									else if (File.Exists(Path.Combine(cosmosCmdConfigDirectory, "..\\cosmos.ini")))
									{
										cosmosCmdFlags = "-c\"" + IOUtil.PathCombine(cosmosCmdConfigDirectory, "..\\cosmos.ini") + "\" ";
									}
									//Console.WriteLine("cosmosCmdFlags : " + cosmosCmdFlags);
									if (!File.Exists(Path.Combine(cosmosCmdConfigDirectory, "autopilot.ini")))
									{
										cosmosCmdConfigDirectory = IOUtil.PathCombine(cosmosCmdConfigDirectory, "..");
										//if (File.Exists(Path.Combine(cosmosCmdConfigDirectory, "..\\autopilot.ini")))
										//{
										//}
									}
									//Console.WriteLine("cosmosCmdConfigDirectory : " + cosmosCmdConfigDirectory);
								}
							}
							catch
							{
								// ignore...
							}
							cosmosCmdChecked = true;
						}
						if (cosmosCmd != null)
						{
							System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
								cosmosCmd, cosmosCmdFlags == null ? args : cosmosCmdFlags + args);
							ConfigureExec(psi);
							psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
							psi.CreateNoWindow = true;
							psi.RedirectStandardInput = false;
							psi.RedirectStandardOutput = true;
							psi.RedirectStandardError = true;
#if DOTNET2
							psi.StandardOutputEncoding = Encoding.UTF8;
#endif
							psi.UseShellExecute = false;
							proc = System.Diagnostics.Process.Start(psi);
							IOUtil.ConsumeBackground(proc.StandardError);
						}
						else
						{
							cosmosCmdFailed = true;
							cosmosCmd = null;
							cosmosCmds[1] = null;
							cosmosCmdFlags = null;
							cosmosCmdConfigDirectory = null;
						}
					}
					catch
					{
						cosmosCmdFailed = true;
						cosmosCmd = null;
						cosmosCmds[1] = null;
						cosmosCmdFlags = null;
						cosmosCmdConfigDirectory = null;
					}
				}
			}

			if (proc == null)
			{
				if (!cosmosCmdBackupFailed)
				{
					try
					{
						if (!cosmosCmdBackupChecked)
						{
							try
							{
								if (File.Exists(Path.Combine(DllPath, cosmosCmdBackup)))
								{
									cosmosCmdBackup = Path.Combine(DllPath, cosmosCmdBackup);
								}
								else if (File.Exists(Path.Combine(Path.Combine(DllPath, "bin"), cosmosCmdBackup)))
								{
									cosmosCmdBackup = Path.Combine(DllPath, cosmosCmdBackup);
								}
								else
								{
									cosmosCmdBackup = IOUtil.FindInPath(cosmosCmdBackup, true);
								}
								cosmosCmds[2] = cosmosCmdBackup;
							}
							catch
							{
								// ignore...
							}
							cosmosCmdBackupChecked = true;
						}
						if (cosmosCmdBackup != null)
						{
							System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
								cosmosCmdBackup, args);
							ConfigureExec(psi);
							psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
							psi.CreateNoWindow = true;
							psi.RedirectStandardInput = false;
							psi.RedirectStandardOutput = true;
							psi.RedirectStandardError = true;
#if DOTNET2
							psi.StandardOutputEncoding = Encoding.UTF8;
#endif
							psi.UseShellExecute = false;
							proc = System.Diagnostics.Process.Start(psi);
							IOUtil.ConsumeBackground(proc.StandardError);
						}
						else
						{
							cosmosCmdBackupFailed = true;
							cosmosCmds[2] = null;
						}
					}
					catch
					{
						cosmosCmdBackupFailed = true;
						cosmosCmds[2] = null;
					}
				}
			}


			if (proc == null)
			{
				if (!cosmosCmdSharedFailed)
				{
					try
					{
						if (cosmosCmdShared == null)
						{
							//string temp = Environment.GetEnvironmentVariable("TEMP");
							//if (temp == null || (temp = temp.Trim()).Length == 0)
							//{
							//    temp = Environment.GetEnvironmentVariable("TMP");
							//}
							string temp = Path.GetTempPath();
							//tempSubdir = Path.GetFileName(cosmosToolShare);
							cosmosCmdShared = Path.Combine(
								Path.Combine(temp, Path.GetFileName(cosmosToolShare)),
								"tmsncosmos.exe");

							// this avoids copying, but will fail if corrupted!!
							if (!File.Exists(cosmosCmdShared))
							{
								IOUtil.Copy(cosmosToolShare, temp);
							}
						}
						if (cosmosCmdShared != null)
						{
							System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
								cosmosCmdShared, args);
							ConfigureExec(psi);
							psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
							psi.CreateNoWindow = true;
							psi.RedirectStandardInput = false;
							psi.RedirectStandardOutput = true;
							psi.RedirectStandardError = true;
#if DOTNET2
							psi.StandardOutputEncoding = Encoding.UTF8;
#endif
							psi.UseShellExecute = false;
							proc = System.Diagnostics.Process.Start(psi);
							IOUtil.ConsumeBackground(proc.StandardError);
							cosmosCmds[3] = cosmosCmdShared;
						}
						else
						{
							cosmosCmdShared = null;
							cosmosCmds[3] = null;
							cosmosCmdSharedFailed = true;
						}
					}
					catch
					{
						cosmosCmdSharedFailed = true;
					}
				}
			}

			if (proc == null)
			{
				throw new IOException("No Cosmos client executable available");
			}
			return proc;
		}


		/// <summary>
		/// Helper function to reliably execute a batch tmsncomsos.exe command.
		/// </summary>
		/// <returns>A Process for the executing batch command</returns>
		internal static System.Diagnostics.Process CosmosExecBatchStart()
		{
			System.Diagnostics.Process proc = null;
			if (!cosmosCmdTmsnFailed)
			{
				try
				{
					if (!cosmosCmdTmsnChecked)
					{
						try
						{
							if (File.Exists(Path.Combine(DllPath, cosmosCmdTmsn)))
							{
								cosmosCmdTmsn = Path.Combine(DllPath, cosmosCmdTmsn);
							}
							else if (File.Exists(Path.Combine(Path.Combine(DllPath, "bin"), cosmosCmdTmsn)))
							{
								cosmosCmdTmsn = Path.Combine(DllPath, cosmosCmdTmsn);
							}
							else
							{
								cosmosCmdTmsn = IOUtil.FindInPath(cosmosCmdTmsn, true);
							}
							cosmosCmds[0] = cosmosCmdTmsn;
						}
						catch
						{
							// ignore...
						}
						cosmosCmdTmsnChecked = true;
					}
					if (cosmosCmdTmsn != null)
					{
						System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
							cosmosCmdTmsn, cosmosBatchCmdArgs);
						ConfigureExec(psi);
						psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
						psi.CreateNoWindow = true;
						psi.RedirectStandardInput = false;
						psi.RedirectStandardOutput = true;
						psi.RedirectStandardError = true;
#if DOTNET2
						psi.StandardOutputEncoding = Encoding.UTF8;
#endif
						psi.UseShellExecute = false;
						proc = System.Diagnostics.Process.Start(psi);
						// should not do this if we ever want the batch output:
						IOUtil.ConsumeBackground(proc.StandardError);
					}
					else
					{
						cosmosCmdTmsnFailed = true;
						cosmosCmds[0] = null;
					}
				}
				catch
				{
					cosmosCmdTmsnFailed = true;
					cosmosCmds[0] = null;
				}
			}
			
			if (proc == null)
			{
				if (!cosmosCmdSharedFailed)
				{
					try
					{
						if (cosmosCmdShared == null)
						{
							//string temp = Environment.GetEnvironmentVariable("TEMP");
							//if (temp == null || (temp = temp.Trim()).Length == 0)
							//{
							//    temp = Environment.GetEnvironmentVariable("TMP");
							//}
							string temp = Path.GetTempPath();
							//tempSubdir = Path.GetFileName(cosmosToolShare);
							cosmosCmdShared = Path.Combine(
								Path.Combine(temp, Path.GetFileName(cosmosToolShare)),
								"tmsncosmos.exe");

							// this avoids copying, but will fail if corrupted!!
							if (!File.Exists(cosmosCmdShared))
							{
								IOUtil.Copy(cosmosToolShare, temp);
							}
						}
						if (cosmosCmdShared != null)
						{
							System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(
								cosmosCmdShared, cosmosBatchCmdArgs);
							ConfigureExec(psi);
							psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
							psi.CreateNoWindow = true;
							psi.RedirectStandardInput = false;
							psi.RedirectStandardOutput = true;
							psi.RedirectStandardError = true;
#if DOTNET2
							psi.StandardOutputEncoding = Encoding.UTF8;
#endif
							psi.UseShellExecute = false;
							proc = System.Diagnostics.Process.Start(psi);
							IOUtil.ConsumeBackground(proc.StandardError);
							cosmosCmds[3] = cosmosCmdShared;
						}
						else
						{
							cosmosCmdShared = null;
							cosmosCmds[3] = null;
							cosmosCmdSharedFailed = true;
						}
					}
					catch
					{
						cosmosCmdSharedFailed = true;
					}
				}
			}

			if (proc == null)
			{
				throw new IOException("No batch-capable Cosmos client executable available");
			}
			return proc;
		}


		private static string dllPath = null;
		private static string DllPath
		{
			get
			{
				if (dllPath == null)
				{
					lock (typeof(Cosmos))
					{
						dllPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
						dllPath = Path.GetDirectoryName(dllPath);
					}
				}
				return dllPath;
			}
		}

		#region Hiding Members
		private Cosmos()
		{
		}

		/// <exclude/>
		/// <summary></summary>
		/// <returns></returns>
		protected new object MemberwiseClone()
		{
			return base.MemberwiseClone();
		}

		/// <exclude/>
		/// <summary></summary>
		/// <param name="obj"></param>
		/// <returns></returns>
		public override bool Equals(object obj)
		{
			return base.Equals(obj);
		}

		/// <exclude/>
		/// <summary></summary>
		/// <returns></returns>
		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		/// <exclude/>
		/// <summary></summary>
		/// <returns></returns>
		public override string ToString()
		{
			return base.ToString();
		}
		#endregion


		/// <summary>
		/// Retrieve the set of extents that make up the given stream, in sorted order.
		/// </summary>
		/// <remarks>
		/// For basic stream usage, the extents do not need to be considered.
		/// This method is intended for advanced manipulation of the underlying
		/// Cosmos data.
		/// </remarks>
		/// <param name="fileName">the name of the stream</param>
		/// <returns>the list of extents, or null if it cannot be retrieved.</returns>
		public static CosmosExtent[] GetExtents(string fileName)
		{
			try
			{
				// extract volume name:
				string volume = fileName;
				int v = volume.IndexOf("//");
				if (v < 0) return null;
				v += 2;
				v = volume.IndexOf('/', v);
				if (v < 0) return null;
				v++;
				v = volume.IndexOf('/', v);
				if (v < 0) return null;
				v++;
				volume = volume.Substring(0, v);

				System.Diagnostics.Process proc = CosmosExec(string.Format(cosmosExtentsCmdArgs, fileName));

				string[] chunks;
				using (StreamReader outReader = proc.StandardOutput)
				{
					chunks = outReader.ReadToEnd().Replace("\r\n", "\n").Replace("\n\n", "\0").Split('\0');
				}
				proc.WaitForExit();
				int exit = proc.ExitCode;
				proc.Close();
				// what are the error conditions? What about an empty stream? ***
				if (exit != 0 || chunks.Length < 2)
				{
					if (exit != 0 || chunks.Length == 0 || chunks[0].ToLower().Trim().StartsWith("command failed"))
					{
						return null;
					}
					return new CosmosExtent[0];
				}

				CosmosExtent[] res = new CosmosExtent[chunks.Length - 1];
				for (int i = 1; i < chunks.Length; i++)
				{
					res[i - 1] = new CosmosExtent(volume, chunks[i]);
				}
				return res;
			}
			catch
			{
				return null;
			}
		}


		/// <summary>
		/// Get the length of a Cosmos stream, in bytes.
		/// </summary>
		/// <param name="fileName">name of the stream</param>
		/// <returns>the length of the stream in bytes, or -1 if it cannot be determined</returns>
		public static long GetLength(string fileName)
		{
			try
			{
				return GetLengthInner(fileName);
			}
			catch
			{
				return -1;
			}
		}

		internal static readonly char[] nonnumber = new char[] { ':', '-', 'T' };

		internal static long GetLengthInner(string fileName)
		{
			if (fileName == null)  throw new ArgumentNullException("fileName cannot be null", "fileName");
			fileName = fileName.Replace('\\', '/');
			int slashIndex = fileName.LastIndexOf('/');
			if (slashIndex <= 0) throw new ArgumentException("fileName is not a valid Cosmos path", "fileName");
			if (fileName.ToLower().IndexOf("/.extentid/") >= 0)
			{
				return -1;
			}
			// try to limit the directory:
			//string dir = fileName.Substring(0, slashIndex);
			string dir = fileName + "*";
			// could use fileName.Substring(0, fileName.Length - 1) + "?", instead...
			string file = fileName.Substring(slashIndex + 1);

			long length = -1;
			System.Diagnostics.Process proc = null;
			try
			{
				proc = CosmosExec(string.Format(cosmosDirCmdArgs, dir));
				using (StreamReader sr = proc.StandardOutput)
				{
					//////// Old:
					// Contents of directory 'cosmos://msrcosmos/vol1/logdata/slogs':
					// D StreamLength Last update (local)  Name
					//    7733886955 2006-05-03T15:04:00.670L+7h sr20060317.txt.gz
					//    4776338052 2006-05-04T09:23:40.326L+7h sr20060317.txt.7z
					//   23331633766 2006-05-03T16:31:29.930L+7h sr20060317.txt
					//////// New:
					//
					//  Directory of cosmos://msrcosmos/vol1/test
					//
					// 2006-06-20 16:27:21.963 PST            34,703 CLP.doc
					// ...........................    <DIR>          log
					// 2006-07-14 16:24:10.619 PST                20 test.txt
					//                      2 Stream(s)       34,723 bytes
					//                      1 Dir(s)
					//// case-sensitive:
					//string match = " " + file.ToLower();
					string match = " " + file;
					bool started = false;
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						if (!started)
						{
							line = line.Trim();
							if (//line.Length == 0 ||
								line.StartsWith("Directory of") ||
								(line.StartsWith("D ") && line.EndsWith(" Name")))
							{
								started = true;
							}
							continue;
						}
						if ((line.StartsWith("   ") || line.StartsWith("\t")) && line.EndsWith(" bytes"))
						{
							break;
						}
						line = line.Trim();
						if (line.Length == 0) continue;

						if (line.EndsWith(match))
						{
							//string[] cols = regexWhitespace.Split(line);
							line = line.Replace('\t', ' ');
							int oldLen = -1;
							while (line.Length != oldLen)
							{
								oldLen = line.Length;
								line = line.Replace("  ", " ");
							}
							line = line.Trim();
							string[] cols = line.Split(' ');

							if (cols.Length > 1)
							{
								if (cols[0].ToLower() != "d" && cols[cols.Length - 2].ToLower() != "<dir>")
								{
									for (int i = 0; i < cols.Length - 1; i++)
									{
										try
										{
											string size = cols[i];
											if (size.IndexOfAny(nonnumber) < 0)
											{
												size = size.Replace(",", "");
												length = long.Parse(size);
												break;
											}
										}
										catch
										{
											// keep going...
										}
									}
									if (length >= 0) break;
								}
							}
						}
					}
				}
			}
			catch (System.ComponentModel.Win32Exception)
			{
				throw new InvalidOperationException("CosmosReadStream requires tmsncosmos.exe, cosmos.cmd, or cosmos.exe to be in the " +
					"path for reading.");
			}
			catch (Exception ex)
			{
				System.Diagnostics.Debug.WriteLine("Exception when getting cosmos length: " +
					ex.ToString());
				//throw new InvalidOperationException("CosmosReadStream needs cosmos.cmd to be in the path", ex);
			}
			finally
			{
				try
				{
					if (proc != null)  proc.Close();
				}
				catch
				{
					// ignore
				}
			}
			if (length < 0)
			{
				throw new FileNotFoundException("File cannot be found or opened: " + fileName);
			}
			return length;
		}



		#region Exists Checks

		//internal static readonly System.Text.RegularExpressions.Regex regexWhitespace =
		//    new System.Text.RegularExpressions.Regex(@"\s+",
		//    System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.CultureInvariant);

		/// <summary>
		/// Determine if a Cosmos stream exists.
		/// </summary>
		/// <param name="fileName">the name of the stream</param>
		/// <returns>true if the stream exists, false otherwise</returns>
		public static bool FileExists(string fileName)
		{
			return Exists(fileName, true, false);
		}

		/// <summary>
		/// Determine if a Cosmos directory exists.
		/// </summary>
		/// <param name="directoryName">the name of the directory</param>
		/// <returns>true if the directory exists, false otherwise</returns>
		public static bool DirectoryExists(string directoryName)
		{
			return Exists(directoryName, false, true);
		}

		/// <summary>
		/// Determine if a Cosmos stream or directory exists.
		/// </summary>
		/// <param name="path">the name of the stream or directory</param>
		/// <returns>true if the entry exists, false otherwise</returns>
		public static bool Exists(string path)
		{
			return Exists(path, true, true);
		}

		/// <summary>
		/// Determine if a Cosmos stream or directory exists.
		/// </summary>
		/// <param name="fileName">the name of the stream or directory</param>
		/// <param name="allowFile">allow a file match</param>
		/// <param name="allowDirectory">allow a directory match</param>
		/// <returns>true if the entry exists, false otherwise</returns>
		private static bool Exists(string fileName, bool allowFile, bool allowDirectory)
		{
			if (fileName == null || fileName.Length == 0 || !fileName.ToLower().StartsWith("cosmos:")) return false;
			fileName = fileName.Replace('\\', '/');
			//fileName = fileName.Replace("//", "/");
			// cosmos://clustername/volume/path1/path2/filename
			if (fileName[fileName.Length - 1] == '/')
			{
				if (!allowDirectory)  return false;
				fileName = fileName.TrimEnd('/');
			}
			int slashIndex = fileName.LastIndexOf('/');
			if (slashIndex <= "cosmos://".Length) return false;
			string dir = fileName.Substring(0, slashIndex);
			string file = fileName.Substring(slashIndex + 1);
			if (dir.IndexOf('/', "cosmos://".Length) < 0)
			{
				// this is a volume root directory!
				if (!allowDirectory) return false;
				allowFile = true;
				dir = dir + "/" + file;
				file = "";
			}
			else
			{
				// use wildcards to limit search:
				dir = dir + "/" + file + "*";
			}

			System.Diagnostics.Process proc = null;
			try
			{
				proc = CosmosExec(string.Format(cosmosDirCmdArgs, dir));
				using (StreamReader sr = proc.StandardOutput)
				{
					//////// Old:
					// Contents of directory 'cosmos://msrcosmos/vol1/logdata/slogs':
					// D StreamLength Last update (local)  Name
					//    7733886955 2006-05-03T15:04:00.670L+7h sr20060317.txt.gz
					//    4776338052 2006-05-04T09:23:40.326L+7h sr20060317.txt.7z
					//   23331633766 2006-05-03T16:31:29.930L+7h sr20060317.txt
					//////// New:
					//  Directory of cosmos://msrcosmos/vol1/test
					//
					// 2006-06-20 16:27:21.963 PST            34,703 CLP.doc
					// ...........................    <DIR>          log
					// 2006-07-14 16:24:10.619 PST                20 test.txt
					//                      2 Stream(s)       34,723 bytes
					//                      1 Dir(s)
					// Cosmos is case-sensitive!
					//string match = " " + file.ToLower();
					string match = " " + file;
					bool started = false;
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						if (!started)
						{
							line = line.Trim();
							if (//line.Length == 0 ||
								line.StartsWith("Directory of") ||
								(line.StartsWith("D ") && line.EndsWith(" Name")))
							{
								started = true;
							}
							continue;
						}
						if ((line.StartsWith("   ") || line.StartsWith("\t")) && line.EndsWith(" bytes"))
						{
							break;
						}
						line = line.Trim();
						if (line.Length == 0) continue;

						if (file.Length == 0 || line.EndsWith(match))
						{
							if (allowFile && allowDirectory)  return true;
							if (allowFile)
							{
								// make sure it is not a directory:
								//string[] cols = regexWhitespace.Split(line);
								line = line.Replace('\t', ' ');
								int oldLen = -1;
								while (line.Length != oldLen)
								{
									oldLen = line.Length;
									line = line.Replace("  ", " ");
								}
								line = line.Trim();
								string[] cols = line.Split(' ');

								if (cols.Length > 1)
								{
									if (cols[cols.Length - 2].ToLower() == "<dir>" ||
										cols[0].ToLower() == "d")
									{
										return false;
									}
									return true;
								}
							}
							else
							{
								// make sure it is a directory:
								//string[] cols = regexWhitespace.Split(line);
								line = line.Replace('\t', ' ');
								int oldLen = -1;
								while (line.Length != oldLen)
								{
									oldLen = line.Length;
									line = line.Replace("  ", " ");
								}
								line = line.Trim();
								string[] cols = line.Split(' ');

								if (cols.Length > 1)
								{
									if (cols[cols.Length - 2].ToLower() != "<dir>" &&
										cols[0].ToLower() != "d")
									{
										return false;
									}
									return true;
								}
							}
						}
					}
					try
					{
						sr.ReadToEnd();
						proc.WaitForExit();
					}
					catch
					{
						// ignore...
					}
				}
				if (proc.ExitCode != 0)
				{
					throw new IOException("Cosmos execution failed");
				}
				return false;
			}
			catch (Exception ex)
			{
				System.Diagnostics.Debug.WriteLine("Exception when getting cosmos existance: " +
					ex.ToString());
				return false;
			}
			finally
			{
				try
				{
					if (proc != null) proc.Close();
				}
				catch
				{
					// ignore
				}
			}
		}

		/// <summary>
		/// Get the paths to files and directories within a directory.
		/// </summary>
		/// <param name="path">the directory to look in</param>
		/// <returns>the set of file paths for the files and directories in that directory</returns>
		/// <remarks>
		/// <p>
		/// Directories will be distinguished by ending with "/".
		/// </p>
		/// <p>
		/// This will silently return the empty list if there are any problems.
		/// </p>
		/// </remarks>
		public static string[] DirectoryEntries(string path)
		{
			return DirectoryEntries(path, true, true, false);
		}

		/// <summary>
		/// Get the paths to files within a directory.
		/// </summary>
		/// <param name="path">the directory to look in</param>
		/// <returns>the set of file paths for the files in that directory</returns>
		/// <remarks>
		/// This will silently return the empty list if there are any problems.
		/// </remarks>
		public static string[] DirectoryFiles(string path)
		{
			return DirectoryEntries(path, true, false, false);
		}

		internal static string[] DirectoryEntries(string path,
			bool allowFile, bool allowDirectory, bool unfiltered)
		{
			if (path == null || path.Length == 0 || !path.ToLower().StartsWith("cosmos:")) return new string[0];
			// cosmos://clustername/volume/path1/path2/filename
			path = path.Replace('\\', '/');
			path = path.TrimEnd('/');
			// hack for limited wildcards:
			string basePath = path + "/";
			//Console.WriteLine("searching: " + basePath);
			System.Diagnostics.Process proc = null;
			try
			{
				proc = CosmosExec(string.Format(cosmosDirCmdArgs, path));
				ArrayList res = new ArrayList();
				ArrayList resDirs = new ArrayList();
				using (StreamReader sr = proc.StandardOutput)
				{
					//////// Old:
					// Contents of directory 'cosmos://msrcosmos/vol1/logdata/slogs':
					// D StreamLength Last update (local)  Name
					//    7733886955 2006-05-03T15:04:00.670L+7h sr20060317.txt.gz
					//    4776338052 2006-05-04T09:23:40.326L+7h sr20060317.txt.7z
					//   23331633766 2006-05-03T16:31:29.930L+7h sr20060317.txt
					//////// New:
					//
					//  Directory of cosmos://msrcosmos/vol1/test
					//
					// 2006-06-20 16:27:21.963 PST            34,703 CLP.doc
					// ...........................    <DIR>          log
					// 2006-07-14 16:24:10.619 PST                20 test.txt
					//                      2 Stream(s)       34,723 bytes
					//                      1 Dir(s)
					bool started = false;
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						//Console.WriteLine(">> " + line);
						if (!started)
						{
							if (line.Length == 0) continue;
							line = line.Trim();
							if (line.StartsWith("Directory of "))
							{
								basePath = line.Substring("Directory of ".Length).Trim();
								if (basePath.Length != 0 && basePath[basePath.Length - 1] != '/')
								{
									basePath = basePath + "/";
								}
								started = true;
							}
							else if (line.StartsWith("Contents of directory "))
							{
								basePath = line.Substring("Contents of directory ".Length).Trim();
								if (basePath.Length != 0 && basePath[basePath.Length - 1] != '/')
								{
									basePath = basePath + "/";
								}
							}
							else if (line.StartsWith("D ") && line.EndsWith(" Name"))
							{
								started = true;
							}
							continue;
						}
						//if ((line.StartsWith("   ") || line.StartsWith("\t")) && line.EndsWith(" bytes"))
						//{
						//	break;
						//}
						if (line.Length == 0) continue;
						// this will break the old Comsos exe:
						if (line[0] == ' ' || line[0] == '\t') continue;
						line = line.Trim();
						if (line.Length == 0) continue;
						if (line.StartsWith("Directory of "))
						{
							basePath = line.Substring("Directory of ".Length).Trim();
							if (basePath.Length != 0 && basePath[basePath.Length - 1] != '/')
							{
								basePath = basePath + "/";
							}
							continue;
						}
						else if (line.StartsWith("Contents of directory "))
						{
							basePath = line.Substring("Contents of directory ".Length).Trim();
							if (basePath.Length != 0 && basePath[basePath.Length - 1] != '/')
							{
								basePath = basePath + "/";
							}
							continue;
						}

						// make sure it is not a directory:
						//string[] cols = regexWhitespace.Split(line);
						line = line.Replace('\t', ' ');
						int oldLen = -1;
						while (line.Length != oldLen)
						{
							oldLen = line.Length;
							line = line.Replace("  ", " ");
						}
						line = line.Trim();
						string[] cols = line.Split(' ');

						if (cols.Length > 1)
						{
							if (cols[cols.Length - 2].ToLower() != "<dir>" &&
								cols[0].ToLower() != "d")
							{
								if (allowFile)
								{
									string fileName = cols[cols.Length - 1];
									if (fileName == "." || fileName == "..") continue;
									if (fileName.Length != 0)
									{
										if (unfiltered ||
											(string.Compare(fileName, ".dir", true) != 0 &&
											string.Compare(fileName, "CosmosShell", true) != 0))
										{
											res.Add(basePath + fileName);
										}
									}
								}
							}
							else
							{
								if (allowDirectory)
								{
									string fileName = cols[cols.Length - 1];
									if (fileName == "." || fileName == "..") continue;
									if (fileName.Length != 0)
									{
										if (unfiltered ||
											(string.Compare(fileName, ".streamid", true) != 0))
										{
											resDirs.Add(basePath + fileName + "/");
										}
									}
								}
							}
						}
					}
					//					try
					//					{
					//						sr.ReadToEnd();
					//						proc.WaitForExit();
					//					}
					//					catch
					//					{
					//						// ignore...
					//					}
				}
				//				if (proc.ExitCode != 0)
				//				{
				//					// ignore
				//				}
				res.Sort();
				if (resDirs.Count != 0)
				{
					resDirs.Sort();
					resDirs.AddRange(res);
					res = resDirs;
				}
				return (string[])res.ToArray(typeof(string));
			}
			catch (Exception ex)
			{
				System.Diagnostics.Debug.WriteLine("Exception when getting cosmos existance: " +
					ex.ToString());
				return new string[0];
			}
			finally
			{
				try
				{
					if (proc != null) proc.Close();
				}
				catch
				{
					// ignore
				}
			}
		}


		internal static StreamInfo[] DirectoryEntriesInfo(string path,
			bool allowFile, bool allowDirectory, bool unfiltered)
		{
			if (path == null || path.Length == 0 || !path.ToLower().StartsWith("cosmos:")) return new StreamInfo[0];
			// cosmos://clustername/volume/path1/path2/filename
			path = path.Replace('\\', '/');
			path = path.TrimEnd('/');
			// hack for limited wildcards:
			string basePath = path + "/";
			System.Diagnostics.Process proc = null;
			try
			{
				proc = CosmosExec(string.Format(cosmosDirCmdArgs, path));
				ArrayList res = new ArrayList();
				ArrayList resDirs = new ArrayList();
				using (StreamReader sr = proc.StandardOutput)
				{
					//////// Old:
					// Contents of directory 'cosmos://msrcosmos/vol1/logdata/slogs':
					// D StreamLength Last update (local)  Name
					//    7733886955 2006-05-03T15:04:00.670L+7h sr20060317.txt.gz
					//    4776338052 2006-05-04T09:23:40.326L+7h sr20060317.txt.7z
					//   23331633766 2006-05-03T16:31:29.930L+7h sr20060317.txt
					//////// New:
					//
					//  Directory of cosmos://msrcosmos/vol1/test
					//
					// 2006-06-20 16:27:21.963 PST            34,703 CLP.doc
					// ...........................    <DIR>          log
					// 2006-07-14 16:24:10.619 PST                20 test.txt
					//                      2 Stream(s)       34,723 bytes
					//                      1 Dir(s)
					bool started = false;
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						if (!started)
						{
							if (line.Length == 0) continue;
							line = line.Trim();
							if (line.StartsWith("Directory of "))
							{
								basePath = line.Substring("Directory of ".Length).Trim();
								if (basePath.Length != 0 && basePath[basePath.Length - 1] != '/')
								{
									basePath = basePath + "/";
								}
								started = true;
							}
							else if (line.StartsWith("Contents of directory "))
							{
								basePath = line.Substring("Contents of directory ".Length).Trim();
								if (basePath.Length != 0 && basePath[basePath.Length - 1] != '/')
								{
									basePath = basePath + "/";
								}
							}
							else if (line.StartsWith("D ") && line.EndsWith(" Name"))
							{
								started = true;
							}
							continue;
						}
						//if ((line.StartsWith("   ") || line.StartsWith("\t")) && line.EndsWith(" bytes"))
						//{
						//	break;
						//}
						if (line.Length == 0) continue;
						// this will break the old Comsos exe:
						if (line[0] == ' ' || line[0] == '\t') continue;
						line = line.Trim();
						if (line.Length == 0) continue;
						if (line.StartsWith("Directory of "))
						{
							basePath = line.Substring("Directory of ".Length).Trim();
							if (basePath.Length != 0 && basePath[basePath.Length - 1] != '/')
							{
								basePath = basePath + "/";
							}
							continue;
						}
						else if (line.StartsWith("Contents of directory "))
						{
							basePath = line.Substring("Contents of directory ".Length).Trim();
							if (basePath.Length != 0 && basePath[basePath.Length - 1] != '/')
							{
								basePath = basePath + "/";
							}
							continue;
						}

						// make sure it is not a directory:
						//string[] cols = regexWhitespace.Split(line);
						line = line.Replace('\t', ' ');
						int oldLen = -1;
						while (line.Length != oldLen)
						{
							oldLen = line.Length;
							line = line.Replace("  ", " ");
						}
						line = line.Trim();
						string[] cols = line.Split(' ');

						if (cols.Length > 1)
						{
							if (cols[cols.Length - 2].ToLower() != "<dir>" &&
								cols[0].ToLower() != "d")
							{
								if (allowFile)
								{
									string fileName = cols[cols.Length - 1];
									if (fileName == "." || fileName == "..") continue;
									if (fileName.Length != 0)
									{
										if (unfiltered ||
											(string.Compare(fileName, ".dir", true) != 0 &&
											string.Compare(fileName, "CosmosShell", true) != 0))
										{
											fileName = basePath + fileName;

											long length = -1;
											for (int i = 0; i < cols.Length - 1; i++)
											{
												try
												{
													string size = cols[i];
													if (size.IndexOfAny(nonnumber) < 0)
													{
														size = size.Replace(",", "");
														length = long.Parse(size);
														break;
													}
												}
												catch
												{
													// keep going...
												}
											}
											DateTime lastMod = DateTime.MinValue;
											try
											{
												lastMod = DateTime.Parse(cols[0] + " " + cols[1]);
											}
											catch
											{
												// ignore
											}

											res.Add(new StreamInfo(fileName, length, lastMod));
										}
									}
								}
							}
							else
							{
								if (allowDirectory)
								{
									string fileName = cols[cols.Length - 1];
									if (fileName == "." || fileName == "..") continue;
									if (fileName.Length != 0)
									{
										if (unfiltered ||
											(string.Compare(fileName, ".streamid", true) != 0))
										{
											fileName = basePath + fileName + "/";
											resDirs.Add(new StreamInfo(fileName, 0, DateTime.MinValue));
										}
									}
								}
							}
						}
					}
					//					try
					//					{
					//						sr.ReadToEnd();
					//						proc.WaitForExit();
					//					}
					//					catch
					//					{
					//						// ignore...
					//					}
				}
				//				if (proc.ExitCode != 0)
				//				{
				//					// ignore
				//				}
				res.Sort();
				if (resDirs.Count != 0)
				{
					resDirs.Sort();
					resDirs.AddRange(res);
					res = resDirs;
				}
				return (StreamInfo[])res.ToArray(typeof(StreamInfo));
			}
			catch (Exception ex)
			{
				System.Diagnostics.Debug.WriteLine("Exception when getting cosmos existance: " +
					ex.ToString());
				return new StreamInfo[0];
			}
			finally
			{
				try
				{
					if (proc != null) proc.Close();
				}
				catch
				{
					// ignore
				}
			}
		}

		#endregion


		#region Delete, Copy, Append

		/// <summary>
		/// Create a Cosmos directory.
		/// </summary>
		/// <param name="path">the directory name to create</param>
		/// <exception cref="IOException">The directory cannot be created.</exception>
		public static void CreateDirectory(string path)
		{
			if (path == null || path.Length == 0)  throw new IOException("Cannot create directory for empty path");
			path = path.Replace('\\', '/');
			try
			{
				if (path[path.Length - 1] != '/')  path = path + "/";
				using (CosmosWriteStream cw = new CosmosWriteStream(path + ".dir"))
				{
					cw.Flush();
					cw.Close();
				}
			}
			catch
			{
			}
		}


		/// <summary>
		/// Delete a Cosmos stream or directory.
		/// </summary>
		/// <param name="fileName">the name of the stream</param>
		/// <p>
		/// This will silently do nothing if the file already does not exist.
		/// </p>
		/// <p>
		/// If used on a directory, the directory must already be empty.
		/// </p>
		/// <exception cref="ArgumentException">The stream name is invalid.</exception>
		/// <exception cref="InvalidOperationException">tmsncosmos.exe, cosmos.cmd, or cosmos.exe is not in the path, or Cosmos cannot be contacted.</exception>
		/// <exception cref="IOException">The delete cannot complete because of lack of permission or other issues.</exception>
		public static void Delete(string fileName)
		{
			Delete(fileName, false);
		}
		/// <summary>
		/// Delete a Cosmos stream or directory.
		/// </summary>
		/// <param name="fileName">the name of the stream</param>
		/// <param name="recursive">if true, delete all files and subdirectories if
		/// fileName is a directory; otherwise, fileName must be empty if it is a directory</param>
		/// <p>
		/// This will silently do nothing if the file already does not exist.
		/// </p>
		/// <exception cref="ArgumentException">The stream name is invalid.</exception>
		/// <exception cref="InvalidOperationException">tmsncosmos.exe, cosmos.cmd, or cosmos.exe is not in the path, or Cosmos cannot be contacted.</exception>
		/// <exception cref="IOException">The delete cannot complete because of lack of permission or other issues.</exception>
		public static void Delete(string fileName, bool recursive)
		{
			if (fileName == null || fileName.Length == 0 || !fileName.ToLower().StartsWith("cosmos:"))
			{
				throw new ArgumentException("Invalid Cosmos stream name: " + fileName, "fileName");
			}
			fileName = fileName.Replace('\\', '/');
			int slashIndex = fileName.LastIndexOf('/');
			if (slashIndex <= 0)
			{
				throw new ArgumentException("Invalid Cosmos stream name: " + fileName, "fileName");
			}

			string[] dirFiles;
			if (fileName[fileName.Length - 1] != '/' && fileName[fileName.Length - 1] != '\\')
			{
				try
				{
					if (DeleteFile(fileName)) return;
					// just accept as a "not exists" failure?? ***

					// check for a directory, unfortunately:
					dirFiles = Cosmos.DirectoryEntries(fileName, true, true, true);
					if (dirFiles.Length == 0)
					{
						return;
					}
					//throw new IOException("Cosmos stream cannot be deleted: " + fileName);
				}
				catch (IOException)
				{
					dirFiles = Cosmos.DirectoryEntries(fileName, true, true, true);
					if (dirFiles.Length == 0) throw;
				}
			}
			else
			{
				dirFiles = Cosmos.DirectoryEntries(fileName, true, true, true);
				if (dirFiles.Length == 0) return;
			}

			ArrayList realFiles = new ArrayList(dirFiles.Length);
			ArrayList markerFiles = new ArrayList(2);
			for (int i = 0; i < dirFiles.Length; i++)
			{
				if (!dirFiles[i].EndsWith("/.dir") && !dirFiles[i].EndsWith("/CosmosShell"))
				{
					realFiles.Add(dirFiles[i]);
				}
				else
				{
					markerFiles.Add(dirFiles[i]);
				}
			}
			// delete real files and directories, if needed:
			if (realFiles.Count != 0)
			{
				if (!recursive)
				{
					throw new IOException("Directory not empty");
				}
				for (int i = 0; i < realFiles.Count; i++)
				{
					Delete((string)realFiles[i], true);
				}
			}
			// delete the marker files to flush the directory:
			for (int i = 0; i < markerFiles.Count; i++)
			{
				if (!DeleteFile((string)markerFiles[i]))
				{
					throw new IOException("Cosmos stream cannot be deleted: " + (string)markerFiles[i]);
				}
			}
		}
		private static bool DeleteFile(string fileName)
		{
			if (fileName == null || fileName.Length == 0 || !fileName.ToLower().StartsWith("cosmos:"))
			{
				throw new ArgumentException("Invalid Cosmos stream name: " + fileName, "fileName");
			}
			fileName = fileName.Replace('\\', '/');
			int slashIndex = fileName.LastIndexOf('/');
			if (slashIndex <= 0)
			{
				throw new ArgumentException("Invalid Cosmos stream name: " + fileName, "fileName");
			}
			//string dir = fileName.Substring(0, slashIndex);
			//string file = fileName.Substring(slashIndex + 1);

			System.Diagnostics.Process proc = null;
			try
			{
				proc = Cosmos.CosmosExec(string.Format(Cosmos.cosmosDeleteCmdArgs, fileName));
				using (StreamReader sr = proc.StandardOutput)
				{
					//					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					//					{
					//						if (line.ToLower().Trim().StartsWith("command failed"))
					//						{
					//							throw new IOException("Cosmos stream cannot be deleted: " + fileName);
					//						}
					//					}

					try
					{
						sr.ReadToEnd();
						proc.WaitForExit();
					}
					catch
					{
						// ignore...
					}
					if (proc.ExitCode != 0)
					{
						//throw new IOException("Cosmos stream cannot be deleted: " + fileName);
						return false;
					}
				}
			}
			catch (Exception ex)
			{
				System.Diagnostics.Debug.WriteLine("Exception when performing Cosmos delete: " +
					ex.ToString());
				throw new IOException("Cosmos stream cannot be deleted: " + fileName);
			}
			finally
			{
				try
				{
					if (proc != null)  proc.Close();
				}
				catch
				{
					// ignore
				}
			}
			return true;
		}

	
		/// <summary>
		/// Append one Cosmos stream to another.
		/// </summary>
		/// <remarks>
		/// This should be an efficient operation, but it will leave the trailing extents incomplete.
		/// </remarks>
		/// <param name="source">the name of the stream to append</param>
		/// <param name="destination">the name of the stream to append to</param>
		/// <exception cref="IOException">The append could not be completed.</exception>
		public static void Append(string source, string destination)
		{
			Append(source, destination, true);
		}
		private static void Append(string source, string destination, bool retryNoExist)
		{
			if (source == destination)
			{
				throw new IOException("Could not append '" + source + "' to itself");
			}
			//if (!Cosmos.FileExists(destination))
			//{
			//    Console.WriteLine("Copy(" + source + ", " + destination + ")...");
			//    Copy(source, destination);
			//    Console.WriteLine("    (" + source + ", " + destination + ").");
			//    return;
			//}
			System.Diagnostics.Process proc = null;
			try
			{
				//Console.WriteLine("Executing cosmos " + string.Format(cosmosAppendCmdArgs, source, destination));
				proc = Cosmos.CosmosExec(string.Format(Cosmos.cosmosAppendCmdArgs, source, destination));

				using (StreamReader sr = proc.StandardOutput)
				{
					try
					{
						sr.ReadToEnd();
						proc.WaitForExit();
					}
					catch
					{
						// ignore...
					}
				}
				if (proc.ExitCode != 0)
				{
					//Console.WriteLine("bad exit code: " + proc.ExitCode);
					throw new Exception();
				}
			}
			catch
			{
				if (proc != null)
				{
					proc.Close();
					proc = null;
				}
				if (!Cosmos.FileExists(destination))
				{
					//Console.WriteLine("Copy(" + source + ", " + destination + ")...");
					//Copy(source, destination);
					//Console.WriteLine("    (" + source + ", " + destination + ").");

					if (retryNoExist)
					{
						// make sure the file is written...
						using (Stream os = new CosmosWriteStream(destination))
						{
						}
						// should be 0 bytes.

						// hopefully, no infinite loop here:
						Append(source, destination, false);
						return;
					}
					else
					{
						try
						{
							DeleteFile(destination);
						}
						catch
						{
						}
						throw new IOException("Could not append '" + source + "' to '" + destination + "'");
					}
				}
				else if (Cosmos.GetLength(source) == 0)
				{
					// nothing to do, then...
					return;
				}
				throw new IOException("Could not append '" + source + "' to '" + destination + "'");
			}
			finally
			{
				if (proc != null)
				{
					proc.Close();
				}
			}
		}


		/// <summary>
		/// Create a Cosmos stream that is the concatenation of other Cosmos streams.
		/// </summary>
		/// <remarks>
		/// <p>
		/// This should be an efficient operation, but it will leave the trailing extents incomplete.
		/// </p>
		/// <p>
		/// Wildcard patterns are allowed.
		/// </p>
		/// </remarks>
		/// <param name="sources">the name of the streams to concatenate</param>
		/// <param name="destination">the name of the stream to create</param>
		/// <exception cref="IOException">The concatenation could not be completed.</exception>
		/// <exception cref="ArgumentException">Some sources are not cosmos stream names.</exception>
		public static void Concatenate(string destination, params string[] sources)
		{
			//Console.WriteLine("Join:");
			if (sources == null)  sources = new string[0];
			ArrayList expanded = null;
			for (int i = 0; i < sources.Length; i++)
			{
				if (sources[i] == null || string.Compare(sources[i], 0, "cosmos://", 0, "cosmos://".Length, true) != 0)
				{
					throw new ArgumentException("Source is not a Cosmos stream name: " + sources[i], "sources");
				}
				if (sources[i].IndexOf('*') >= 0 || sources[i].IndexOf('?') >= 0 ||
					sources[i].IndexOf("...") >= 0)
				{
					if (expanded == null)
					{
						expanded = new ArrayList();
						for (int j = 0; j < i; j++)
						{
							expanded.Add(sources[j]);
						}
					}
					// should share batch process: ***
					expanded.AddRange(IOUtil.ExpandWildcards(sources[i]));
				}
				else
				{
					if (expanded != null) expanded.Add(sources[i]);
				}
			}
			if (expanded != null)
			{
				sources = (string[])expanded.ToArray(typeof(string));
			}
			//for (int i = 0; i < sources.Length; i++)
			//{
			//    if (!Cosmos.FileExists(sources[i]))
			//    {
			//        throw new IOException("Cosmos stream to join not found: " + sources[i]);
			//    }
			//}
			//Console.WriteLine("Join:Exists");

			//Overwrite(destination, false);
			try
			{
				// should share batch process: ***
				DeleteFile(destination);
			}
			catch
			{
				// ignore
			}

			// make sure the file is written...
			using (Stream os = new CosmosWriteStream(destination))
			{
			}
			// should be 0 bytes.

			//Console.WriteLine("Join:Init[" + sources.Length + "]");
			for (int i = 0; i < sources.Length; i++)
			{
				string source = sources[i];
				try
				{
					//Console.Write(".");
					// should share batch process: ***
					Append(source, destination);
					//Console.Write(",");
				}
				catch
				{
					try
					{
						// should share batch process: ***
						DeleteFile(destination);
					}
					catch
					{
						// ignore
					}
					throw new IOException("Could not concatenate '" + source + "' to '" + destination + "'");
				}
			}
			//Console.WriteLine("Join:Done.");
		}


		/// <summary>
		/// Create a Cosmos stream that is a copy of another Cosmos stream on the same cluster.
		/// </summary>
		/// <remarks>
		/// This should be an efficient operation. It does not actually copy any underlying
		/// data - it just creates a stream that contains the same extents.
		/// </remarks>
		/// <param name="source">the name of the stream to copy</param>
		/// <param name="destination">the name of the stream to create</param>
		/// <exception cref="IOException">The copy could not be completed.</exception>
		public static void Copy(string source, string destination)
		{
			// silently accept if already equal?
			if (source == destination)  return;
			Concatenate(destination, source);
		}

		#endregion


		#region Store

		private static int extentSize = 100 * 1024 * 1024;

		/// <summary>
		/// Get or Set the expected extent size.
		/// 
		/// </summary>
		/// <remarks>
		/// <p>
		/// This primarily affects the <see cref="Store(string,string)"/> operation. If it is
		/// inaccurate, extents might be less full than is optimal.
		/// </p>
		/// <p>
		/// This could possibly be gather from the Cosmos INI file, but the setting is
		/// not currently exposed in the configuration information.
		/// </p>
		/// </remarks>
		public static int ExtentSize
		{
			get { return extentSize; }
			set
			{
				extentSize = value;
				if (extentSize <= 0)  extentSize = 100 * 1024 * 1024;
			}
		}

		private static int storeParallelLevel = 6;

		/// <summary>
		/// Get or Set the number of parallel writes to be made with <see cref="Store(string, string)"/>.
		/// 6 by default.
		/// </summary>
		/// <remarks>
		/// The optimal setting for this can vary, depending on the client machine
		/// capabilities and the cluster load.
		/// </remarks>
		public static int StoreParallelLevel
		{
			get { return storeParallelLevel; }
			set
			{
				storeParallelLevel = value;
				if (storeParallelLevel <= 0)  storeParallelLevel = 6;
			}
		}

		private static bool storeBreakFiles = true;

		/// <summary>
		/// Get or Set whether to break up files when using <see cref="Store(string,string)"/>.
		/// true by default.
		/// </summary>
		/// <remarks>
		/// If this is set to false, multiple files will be stored in parallel,
		/// but single files will not. While this can hurt the performance and
		/// layout, it avoids seeking in the source files (which is very expensive
		/// for compressed files).
		/// </remarks>
		public static bool StoreBreakFiles
		{
			get { return storeBreakFiles; }
			set
			{
				storeBreakFiles = value;
			}
		}

		/// <summary>
		/// Store data to Cosmos with enhanced speed.
		/// </summary>
		/// <param name="source">the source file or wildcard pattern (normally local)</param>
		/// <param name="destination">the Cosmos stream name to store the data to</param>
		/// <remarks>
		/// <p>
		/// This will blast data onto Cosmos at higher speed than a normal copy.
		/// It is helpful to set the <see cref="ExtentSize"/> and <see cref="StoreParallelLevel"/>
		/// parameters to match the system being used.
		/// </p>
		/// <p>
		/// This overload will not avoid breaking extents in the middle of lines.
		/// </p>
		/// </remarks>
		/// <exception cref="ArgumentException">The destination is not a valid Cosmos streamname.</exception>
		/// <exception cref="IOException">The stream cannot be written or the source cannot be read.</exception>
		public static void Store(string source, string destination)
		{
			Store(source, destination, false);
		}

		/// <summary>
		/// Store data to Cosmos with enhanced speed.
		/// </summary>
		/// <param name="source">the source file or wildcard pattern (normally local)</param>
		/// <param name="destination">the Cosmos stream name to store the data to</param>
		/// <param name="breakAtLines">if true, break extents only at the end of lines;
		/// otherwise, break at exact byte limits</param>
		/// <remarks>
		/// <p>
		/// This will blast data onto Cosmos at higher speed than a normal copy.
		/// It is helpful to set the <see cref="ExtentSize"/> and <see cref="StoreParallelLevel"/>
		/// parameters to match the system being used.
		/// </p>
		/// </remarks>
		/// <exception cref="ArgumentException">The destination is not a valid Cosmos streamname.</exception>
		/// <exception cref="IOException">The stream cannot be written or the source cannot be read.</exception>
		public static void Store(string source, string destination, bool breakAtLines)
		{
			Store(new string[] { source }, destination, breakAtLines);
		}

		/// <summary>
		/// Store data to Cosmos with enhanced speed.
		/// </summary>
		/// <param name="source">the source files or wildcard patterns (normally local)</param>
		/// <param name="destination">the Cosmos stream name to store the data to</param>
		/// <remarks>
		/// <p>
		/// This will blast data onto Cosmos at higher speed than a normal copy.
		/// It is helpful to set the <see cref="ExtentSize"/> and <see cref="StoreParallelLevel"/>
		/// parameters to match the system being used.
		/// </p>
		/// <p>
		/// This overload will not break extents at line boundaries.
		/// </p>
		/// </remarks>
		/// <exception cref="ArgumentException">The destination is not a valid Cosmos streamname.</exception>
		/// <exception cref="IOException">The stream cannot be written or the source cannot be read.</exception>
		public static void Store(string[] source, string destination)
		{
			Store(source, destination, false);
		}


		/// <summary>
		/// Store concatenated data to Cosmos with enhanced speed.
		/// </summary>
		/// <param name="source">the source files or wildcard patterns (normally local)</param>
		/// <param name="destination">the Cosmos stream name to store the data to</param>
		/// <param name="breakAtLines">if true, break extents only at the end of lines;
		/// otherwise, break at exact byte limits</param>
		/// <remarks>
		/// <p>
		/// This will blast data onto Cosmos at higher speed than a normal copy.
		/// It is helpful to set the <see cref="ExtentSize"/> and <see cref="StoreParallelLevel"/>
		/// parameters to match the system being used.
		/// </p>
		/// <p>
		/// The order of the input files will be preserved, and the order within each file is
		/// also preserved.
		/// </p>
		/// </remarks>
		/// <exception cref="ArgumentException">The destination is not a valid Cosmos streamname,
		/// or no source files are specified.</exception>
		/// <exception cref="IOException">The stream cannot be written or the source cannot be read.</exception>
		public static void Store(string[] source, string destination, bool breakAtLines)
		{
			if (source == null) source = new string[0];
			if (destination == null || destination.Length == 0 || !destination.ToLower().StartsWith("cosmos://"))
			{
				throw new ArgumentException("destination is not a valid Cosmos streamname: " + destination, "destination");
			}
			int parallelLevel = StoreParallelLevel;
			int extentSize = ExtentSize;
			bool breakFiles = StoreBreakFiles;
			bool textMode = breakAtLines;
			// hack to account for boundaries:
			if (breakAtLines)
			{
				extentSize = extentSize - 8*1024;
			}
			if (!breakFiles)
			{
				// might as well, for efficiency:
				// (this actually affects the encoding transformation and such, since
				// it turns it into a binary transfer...) ***
				breakAtLines = false;
			}

			// expand sources:
			ArrayList fullSource = new ArrayList();
			for (int i = 0; i < source.Length; i++)
			{
				fullSource.AddRange(IOUtil.ExpandWildcards(source[i]));
			}
			source = (string[])fullSource.ToArray(typeof(string));
			// handle empty case:
			if (source.Length == 0)
			{
				//				using (Stream empty = ZStreamOut.Open(destination))
				//				{
				//				}
				//				return;
				throw new ArgumentException("No source files specified.", "source");
			}

			// get lengths:
			// this is a problem - gzip streams do not give the correct length!! ***
			long[] lengths = new long[source.Length];
			long totalLength = 0;
			for (int i = 0; i < source.Length; i++)
			{
				lengths[i] = IOUtil.GetLength(source[i]);
				if (lengths[i] < 0)
				{
					//					throw new NotSupportedException("Cannot Store files whose length is unknown: " +
					//						source[i]);
					// hack this in??
					lengths[1] = ExtentSize;
				}
				totalLength += lengths[i];
			}

			// handle empty case:
			if (source.Length == 1 && lengths[0] == 0)
			{
				using (Stream empty = ZStreamOut.Open(destination))
				{
				}
				return;
			}

			// handle simple case of small input:
			// we could make this fast, also, at the cost of unfull extents... ***
			if (totalLength <= ExtentSize)
			{
				if (textMode)
				{
					using (StreamWriter d = ZStreamWriter.Open(destination))
					{
						string line;
						for (int i = 0; i < source.Length; i++)
						{
							using (StreamReader s = ZStreamReader.Open(source[i]))
							{
								while ((line = s.ReadLine()) != null)
								{
									d.WriteLine(line);
								}
							}
						}
					}
				}
				else
				{
					using (Stream d = ZStreamOut.Open(destination))
					{
						byte[] buf = new byte[256*1024];
						int count;
						for (int i = 0; i < source.Length; i++)
						{
							using (Stream s = ZStreamIn.Open(source[i]))
							{
								while ((count = s.Read(buf, 0, buf.Length)) > 0)
								{
									d.Write(buf, 0, count);
								}
							}
						}
					}
				}
				return;
			}

			// we will split this up if possible...
			if (parallelLevel > Math.Ceiling(totalLength / (double)extentSize))
			{
				parallelLevel = (int)Math.Ceiling(totalLength / (double)extentSize);
			}
			int[] startFile = new int[parallelLevel];
			long[] startPos = new long[parallelLevel];
			long sum = 0;
			long sumWithinCur = 0;
			int curSource = 0;
			//			Console.WriteLine("totalLength: " + totalLength);
			for (int i = 0; i < parallelLevel; i++)
			{
				long target = (long)(i * (totalLength / (double)parallelLevel));
				while (sum < target)
				{
					//					Console.WriteLine("parallelLevel: " + i +
					//						"  target: " + target +
					//						"  sum: " + sum +
					//						"  curSource: " + curSource);
					if (sum + lengths[curSource] - sumWithinCur < target)
					{
						sum += lengths[curSource] - sumWithinCur;
						curSource++;
						sumWithinCur = 0;
						if (curSource >= source.Length)  break;
					}
					else
					{
						break;
					}
				}
				if (curSource >= source.Length)
				{
					// we made a mistake. remove a level...
					// (should not happen)
					parallelLevel = i;
					int[] oldStartFile = startFile;
					long[] oldStartPos = startPos;
					startFile = new int[parallelLevel];
					startPos = new long[parallelLevel];
					Array.Copy(oldStartFile, startFile, startFile.Length);
					Array.Copy(oldStartPos, startPos, startPos.Length);
					break;
				}
				startFile[i] = curSource;
				startPos[i] = breakFiles ? target - sum + sumWithinCur : 0;
				if (breakFiles)
				{
					sumWithinCur += (target - sum);
					sum = target;
				}
				else
				{
					// should really pull back, not advance!!!
					sum += lengths[curSource];
					curSource++;
					sumWithinCur = 0;
				}
			}

			// Spin off the copies
			// threadpool or explicit threads? Or async delegate calls?
			StoreThread[] copyThreads = new StoreThread[parallelLevel];
			StoreThread next = null;
			for (int i = copyThreads.Length - 1; i >= 0; i--)
			{
				int endFile;
				long endPos;
				if (i == startFile.Length - 1)
				{
					endFile = source.Length - 1;
					endPos = long.MaxValue;
				}
				else
				{
					if (startPos[i+1] == 0)
					{
						endFile = startFile[i+1] - 1;
						endPos = long.MaxValue;
					}
					else
					{
						endFile = startFile[i+1];
						endPos = startPos[i+1];
					}
				}
				//Console.WriteLine("Store: start = " + startFile[i] + ":" + startPos[i] + ", end = " + endFile + ":" + endPos +
				//	(textMode ? " (txt)" : ""));
				copyThreads[i] = new StoreThread(source, startFile[i], startPos[i],
					endFile, endPos, destination + ".store_" + i, breakAtLines, textMode, next);
				next = copyThreads[i];
			}

			for (int i = 0; i < copyThreads.Length; i++)
			{
				copyThreads[i].Start();
			}
			for (int i = 0; i < copyThreads.Length; i++)
			{
				copyThreads[i].End();
			}
			for (int i = 0; i < copyThreads.Length; i++)
			{
				if (copyThreads[i].HasError)
				{
					for (int j = 0; j < copyThreads.Length; j++)
					{
						try
						{
							Delete(copyThreads[j].FileName);
						}
						catch
						{
						}
					}
					throw new IOException("Could not copy source data.");
				}
			}

			try
			{
				string[] threadSources = new string[copyThreads.Length];
				for (int i = 0; i < copyThreads.Length; i++)
				{
					try
					{
						threadSources[i] = copyThreads[i].FileName;
					}
					catch
					{
					}
				}
				Concatenate(destination, threadSources);
			}
			finally
			{
				for (int i = 0; i < copyThreads.Length; i++)
				{
					try
					{
						Delete(copyThreads[i].FileName);
					}
					catch
					{
					}
				}
			}
		}

		private class StoreThread
		{
			private readonly string[] source;
			private readonly int firstSource;
			private readonly int lastSource;
			private long startPosition;
			private long endPosition;
			private readonly string dest;
			private readonly bool breakAtLines;
			private readonly bool textMode;
			private readonly StoreThread next;

			private Thread thread;
			private bool hasError;

			public StoreThread(string[] source, int firstSource, long startPosition, int lastSource, long endPosition,
				string dest, bool breakAtLines, bool textMode, StoreThread next)
			{
				//Console.WriteLine("> " + breakAtLines + ", " + textMode);
				this.source = source;
				this.dest = dest;
				this.firstSource = firstSource;
				this.lastSource = lastSource;
				this.startPosition = startPosition;
				this.endPosition = endPosition;
				this.breakAtLines = breakAtLines;
				this.textMode = textMode;
				this.next = next;
			}

			public void Start()
			{
				ThreadStart ts = new ThreadStart(CopySection);
				thread = new Thread(ts);
				thread.Start();
			}

			public void End()
			{
				thread.Join();
			}

			public string FileName
			{
				get { return dest; }
			}

			//// really, we should have an encoding that merely removes the preamble from a provided encoding!
			//private class UTF16Raw : System.Text.UnicodeEncoding
			//{
			//    public static readonly UTF16Raw UTF16 = new UTF16Raw();
			//    private UTF16Raw()
			//    {
			//    }
			//    public override object Clone()
			//    {
			//        return this;
			//    }
			//    private static readonly byte[] preamble = new byte[0];
			//    public override byte[] GetPreamble()
			//    {
			//        return preamble;
			//    }
			//}


			AutoResetEvent startMutex = new AutoResetEvent(false);

			public long TrueStart
			{
				get
				{
					lock (this)
					{
						if (startMutex != null)
						{
							startMutex.WaitOne();
							startMutex = null;
						}
					}
					return startPosition;
				}
			}

			private void CopySection()
			{
				//				Console.WriteLine("first = " + firstSource + ":" + startPosition + "  last = " + lastSource + ":" + endPosition);
				try
				{
					if (textMode)
					{
						//Console.WriteLine(">> first = " + firstSource + ":" + startPosition + "  last = " + lastSource + ":" + endPosition);
						char[] buf = new char[256 * 1024];
						int c;
						using (StreamWriter d = ZStreamWriter.Open(dest))
						{
							// First file:
							Encoding enc = null;
							if (startPosition == 0)
							{
								startMutex.Set();
							}
							else
							{
								using (StreamReader s = ZStreamReader.Open(source[firstSource]))
								{
									s.Read();
									enc = s.CurrentEncoding;
								}
								//if (enc is UnicodeEncoding)
								//{
								//    // what about endianness? ***
								//    enc = UTF16Raw.UTF16;
								//}
								byte[] preamble = enc.GetPreamble();
								if (preamble != null && preamble.Length != 0)
								{
									enc = new NoPreambleEncoding(enc);
								}
							}
							//using (PositionStreamReader s = new PositionStreamReader(
							//	ZStreamIn.Open(source[firstSource])))
							//{
							//    while ((line = s.ReadLine()) != null)
							//    {
							//        if (s.Position >= startPosition)  break;
							//        d.WriteLine(line);
							//    }
							//    while ((line = s.ReadLine()) != null)
							//    {
							//        d.WriteLine(line);
							//        if (firstSource == lastSource && s.Position > endPosition)  break;
							//    }
							//}
							using (Stream ss = ZStreamIn.Open(source[firstSource]))
							{
								bool done = false;
								// find the start (next newline after specified):
								if (startPosition != 0)
								{
									ss.Seek(startPosition, SeekOrigin.Begin);
									int b = ss.ReadByte();
									for (; b >= 0 && b != '\n'; b = ss.ReadByte())
									{
									}
									if (b < 0)
									{
										done = true;
										startPosition = long.MaxValue;
									}
									else
									{
										startPosition = ss.Position;
									}
									startMutex.Set();
								}
								//Console.WriteLine("start: " + firstSource + ":" + ss.Position);
								if (!done)
								{
									long len = ss.Length;
									//bool newlineTerminate = false;
									Stream sss = ss;
									if (firstSource < lastSource || endPosition == long.MaxValue ||
										endPosition < 0 ||
										(len >= 0 && endPosition >= len))
									{
										// write the whole file until the end:
										//newlineTerminate = true;
									}
									else
									{
										// need to get the end correctly!! From the next reader?? ******
										if (next != null)
										{
											endPosition = next.TrueStart;
										}
										sss = new BoundedStream(ss, ss.Position, endPosition);
										//Console.WriteLine("end: " + firstSource + ":" + endPosition);
									}
									using (StreamReader s = startPosition == 0 ?
										new StreamReader(sss) : new StreamReader(sss, enc, false))
									{
										c = s.Read(buf, 0, buf.Length);
										if (c > 0)
										{
											int lastC = c;
											d.Write(buf, 0, c);
											while ((c = s.Read(buf, 0, buf.Length)) > 0)
											{
												d.Write(buf, 0, c);
												lastC = c;
											}
											if (buf[lastC - 1] != '\n')
											{
												// file didn't end in a newline - add one in!
												// but newlines are no longer converted without ReadLine/WriteLine! ***
												//Console.Error.WriteLine("no newline!!! 1");
												d.WriteLine();
											}
										}
									}
								}
							}

						// Remaining files:
							if (firstSource < lastSource)
							{
								int lastFullSource = lastSource;
								if (endPosition != long.MaxValue && endPosition >= 0)
								{
									lastFullSource--;
								}
								// Full files:
								for (int i = firstSource + 1; i <= lastFullSource; i++)
								{
									using (StreamReader s = ZStreamReader.Open(source[i]))
									{
										//while ((line = s.ReadLine()) != null)
										//{
										//    d.WriteLine(line);
										//}
										c = s.Read(buf, 0, buf.Length);
										if (c > 0)
										{
											int lastC = c;
											d.Write(buf, 0, c);
											while ((c = s.Read(buf, 0, buf.Length)) > 0)
											{
												d.Write(buf, 0, c);
												lastC = c;
											}
											if (buf[lastC - 1] != '\n')
											{
												// file didn't end in a newline - add one in!
												// but newlines are no longer converted without ReadLine/WriteLine! ***
												//Console.Error.WriteLine("no newline!!! 2");
												d.WriteLine();
											}
										}
									}
								}
								// Last file:
								//if (last != lastSource)
								//{
								//using (PositionStreamReader s = new PositionStreamReader(
								//    ZStreamIn.Open(source[lastSource])))
								//{
								//    while ((line = s.ReadLine()) != null)
								//    {
								//        d.WriteLine(line);
								//        if (s.Position > endPosition) break;
								//    }
								//}
								if (lastFullSource != lastSource)
								{
									using (Stream ss = ZStreamIn.Open(source[lastSource]))
									{
										// need to get the end correctly!! From the next reader?? ******
										long len = ss.Length;
										Stream sss = ss;
										if (len >= 0 && endPosition >= len)
										{
											// write the whole file until the end:
											//newlineTerminate = true;
										}
										else
										{
											// need to get the end correctly!! From the next reader?? ******
											if (next != null)
											{
												endPosition = next.TrueStart;
											}
											sss = new BoundedStream(ss, 0, endPosition);
											//Console.WriteLine("end: " + firstSource + ":" + endPosition);
										}
										using (StreamReader s = new StreamReader(sss))
										{
											c = s.Read(buf, 0, buf.Length);
											if (c > 0)
											{
												int lastC = c;
												d.Write(buf, 0, c);
												while ((c = s.Read(buf, 0, buf.Length)) > 0)
												{
													d.Write(buf, 0, c);
													lastC = c;
												}
												if (buf[lastC - 1] != '\n')
												{
													// file didn't end in a newline - add one in!
													// but newlines are no longer converted without ReadLine/WriteLine! ***
													// this one should never happen...
													//Console.Error.WriteLine("no newline!!! 3");
													d.WriteLine();
												}
											}
										}
									}
								}
							}
						}
					}
					else
					{
						//using (Stream d = ZStreamOut.Open(dest))
						using (Stream d = new CosmosWriteStream(dest, false, breakAtLines))
						{
							byte[] buf = new byte[256 * 1024];
							int count;
							for (int i = firstSource; i <= lastSource; i++)
							{
								using (Stream s = ZStreamIn.Open(source[i]))
								//using (Stream s = new BufferedStream(ZStreamIn.Open(source[i]), 8*1024*1024))
								{
									if (i == firstSource && startPosition != 0)
									{
										s.Seek(startPosition, SeekOrigin.Begin);
									}
									if (i == lastSource && endPosition >= 0 && endPosition < long.MaxValue)
									{
										long total = startPosition;
										int limit = buf.Length;
										if (total + limit > endPosition)
										{
											limit = (int)(endPosition - total);
										}
										while (total < endPosition && (count = s.Read(buf, 0, limit)) > 0)
										{
											d.Write(buf, 0, count);
											total += count;
											// > ? or >=?
											if (total + limit > endPosition)
											{
												limit = (int)(endPosition - total);
											}
										}
									}
									else
									{
										while ((count = s.Read(buf, 0, buf.Length)) > 0)
										{
											d.Write(buf, 0, count);
										}
									}
								}
							}
						}
					}
					Console.WriteLine("<< first = " + firstSource + ":" + startPosition + "  last = " + lastSource + ":" + endPosition);
				}
				catch
				{
					try
					{
						Delete(dest);
					}
					catch
					{
					}
					hasError = true;
				}
			}

			public bool HasError
			{
				get { return hasError; }
			}
		}

		#endregion
	}




	/// <summary>
	/// Information about a Cosmos extent (the storage chunks used
	/// to store a Cosmos stream).
	/// </summary>
	/// <remarks>
	/// For basic stream usage, the extents do not need to be considered.
	/// This class is intended for advanced manipulation of the underlying
	/// Cosmos data.
	/// </remarks>
	public class CosmosExtent : ICloneable
	{
		private Guid extentId;
		private long length;
		//private bool sealedFlag;
		private string volume;
		private string[] nodes;
		private int sequence;

		/// <summary>
		/// Get the ID for this extent.
		/// </summary>
		public Guid ExtentId
		{
			get { return extentId; }
		}

		/// <summary>
		/// Get the uncompressed length of this extent.
		/// </summary>
		public long Length
		{
			get { return length; }
		}

		/// <summary>
		/// Get or Set a sequence number for this extent.
		/// </summary>
		/// <remarks>
		/// Note that a sequence number is not unique for a given extent,
		/// since it may belong to multiple Cosmos streams.
		/// </remarks>
		public int Sequence
		{
			get { return sequence; }
			set { sequence = value; }
		}

		/// <summary>
		/// Get a Cosmos stream path for this individual extent.
		/// </summary>
		public string ExtentPath
		{
			get
			{
				if (volume == null || volume.Length == 0) return null;
				return volume + (volume[volume.Length-1] == '/' ? "" : "/") + ".extentid/{" + ExtentId.ToString() + "}";
			}
		}

		/// <summary>
		/// Get the list of node names that store this extent.
		/// </summary>
		/// <remarks>
		/// Note that this array is not read-only, but it should probably be treated
		/// as such.
		/// </remarks>
		public string[] Nodes
		{
			get { return nodes; }
		}

		/// <summary>
		/// Create a Cosmos extent description with the given ID and length.
		/// </summary>
		/// <param name="extentId">the ID of the extent</param>
		/// <param name="length">the uncompressed length of the extent</param>
		public CosmosExtent(Guid extentId, long length)
			: this(extentId, length, null)
		{
		}
		/// <summary>
		/// Create a Cosmos extent description with the given ID and length,
		/// and a given sequence number.
		/// </summary>
		/// <param name="extentId">the ID of the extent</param>
		/// <param name="length">the uncompressed length of the extent</param>
		/// <param name="volume">the full Cosmos volume on which the extent resides</param>
		public CosmosExtent(Guid extentId, long length, string volume)
		{
			this.extentId = extentId;
			this.length = length;
			this.volume = volume;
			this.sequence = -1;
		}

		internal CosmosExtent(string volume, string description)
		{
			//        ======= Extent[219] ======
			//                 Extent ID: {A528DB41-6315-452D-A985-8B85D8C3233F}
			//                     Flags: 00001000 ( Sealed )
			//               Update Time: 2006-05-03T16:10:53.068L+7h
			//                    Length: 67108864
			//Number of extent instances: 3
			//               Instance  0: Node msr-pool45, Flags=00000004 ( Sealed )
			//               Instance  1: Node msr-pool49, Flags=00000004 ( Sealed )
			//               Instance  2: Node msr-pool48, Flags=00000008 ( Incomplete )
			//
			this.volume = volume;
			sequence = -1;
			extentId = Guid.Empty;
			length = -1;
			ArrayList nodeList = new ArrayList();
			using (StringReader sr = new StringReader(description))
			{
				for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
				{
					line = line.Trim().ToLower();
					if (line.Length == 0) continue;
					int e = line.IndexOf("extent[");
					if (e >= 0)
					{
						e += "extent[".Length;
						int eEnd = line.IndexOf(']', e );
						if (eEnd > e)
						{
							try
							{
								sequence = int.Parse(line.Substring(e, eEnd - e));
							}
							catch
							{
								// ignore
							}
							continue;
						}
					}
					if (line.StartsWith("extent id:"))
					{
						extentId = new Guid(line.Substring("extent id:".Length).Trim());
					}
					else if (line.StartsWith("length:"))
					{
						length = long.Parse(line.Substring("length:".Length).Trim());
					}
					else if (line.StartsWith("instance"))
					{
						if (line.IndexOf("( sealed") > 0 || line.IndexOf("(sealed") > 0)
						{
							int i = line.IndexOf("node ");
							if (i > 0)
							{
								i += "node ".Length;
								int iEnd = line.IndexOf(',', i);
								if (iEnd > i)
								{
									string node = line.Substring(i, iEnd - i).Trim();
									if (node.Length != 0)
									{
										nodeList.Add(node);
									}
								}
							}
						}
					}
				}
				nodes = (string[])nodeList.ToArray(typeof(string));
			}
		}

		#region ICloneable Members
		/// <summary>
		/// Create a copy of this CosmosExtent.
		/// </summary>
		/// <returns>a copy of this CosmosExtent</returns>
		/// <remarks>
		/// Note that the Nodes array is shared with any new copies.
		/// </remarks>
		public CosmosExtent Clone()
		{
			CosmosExtent res = new CosmosExtent(extentId, length);
			res.sequence = sequence;
			res.volume = volume;
			res.nodes = nodes;
			return res;
		}
		/// <summary>
		/// Create a copy of this CosmosExtent.
		/// </summary>
		/// <returns>a copy of this CosmosExtent</returns>
		/// <remarks>
		/// Note that the Nodes array is shared with any new copies.
		/// </remarks>
		object ICloneable.Clone()
		{
			return Clone();
		}
		#endregion

		/// <summary>
		/// Return the string representation of this CosmosExtent.
		/// </summary>
		/// <returns>the string representation of this CosmosExtent</returns>
		public override string ToString()
		{
			StringBuilder res = new StringBuilder();
			if (sequence >= 0)
			{
				res.Append(sequence);
				res.Append(": ");
			}
			res.Append(extentId.ToString());
			res.Append(" [");
			res.Append(length);
			res.Append("]");
			res.Append(" (");
			for (int i = 0; i < nodes.Length; i++)
			{
				if (i != 0) res.Append(", ");
				res.Append(nodes[i]);
			}
			res.Append(")");
			return res.ToString();
		}

		/// <summary>
		/// Determine equality, based only on extent IDs.
		/// </summary>
		/// <param name="obj">the other object to compare</param>
		/// <returns>true if obj is a CosmosExtent with an equal ExtentId; false otherwise</returns>
		public override bool Equals(object obj)
		{
			return (obj is CosmosExtent) ? Equals((CosmosExtent)obj) : false;
		}
		/// <summary>
		/// Determine equality, based only on extent IDs.
		/// </summary>
		/// <param name="other">the other CosmosExtent to compare</param>
		/// <returns>true if other is a CosmosExtent with an equal ExtentId; false otherwise</returns>
		public bool Equals(CosmosExtent other)
		{
			return extentId == other.extentId;
			//	&& length == other.length && sequence == other.sequence;
		}

		/// <summary>
		/// Get a hashcode, based only on extent ID.
		/// </summary>
		/// <returns>the hashcode, based only on the extent ID.</returns>
		public override int GetHashCode()
		{
			return extentId.GetHashCode();
		}
	}





	/// <summary>
	/// Wrapper Stream to perform Cosmos reading.
	/// </summary>
	/// <remarks>
	/// <p>
	/// A tmsncosmos.exe, cosmos.cmd, or cosmos.exe command is needed in the path in order for this functionality
	/// to operate correctly.
	/// </p>
	/// <p>
	/// A shared set of tools will be used if no version is found in the path, although this
	/// increases the startup time. The environment variable COSMOS_TOOLS can be used to
	/// specify a directory or share for this purpose, instead of the default.
	/// </p>
	/// </remarks>
	public class CosmosReadStream : CmdStream
	{
		//private static bool backgroundGetLength = false;
		private readonly string fileName;

		/// <summary>
		/// Create a stream to read from Cosmos.
		/// </summary>
		/// <param name="fileName">Name of the Cosmos stream to be wrapped for reading</param>
		/// <exception cref="ArgumentNullException">The stream name is null.</exception>
		/// <exception cref="ArgumentException">The stream name is invalid.</exception>
		/// <exception cref="InvalidOperationException">tmsncosmos.exe, cosmos.cmd, or cosmos.exe is not in the path, or Cosmos cannot be contacted.</exception>
		/// <exception cref="FileNotFoundException">The Cosmos stream cannot be found.</exception>
		public CosmosReadStream(string fileName)
			//: this(fileName, -1)
			: this(fileName, new LengthGetter(fileName))
			//: this(fileName, Cosmos.GetLengthInner(fileName))
		{
			// if we always get the length in the background, we send an extra cosmos request that
			// will not be needed. However, we can't really predict if .Length will be called,
			// until it is too late...
		}
		private CosmosReadStream(string fileName, long length)
			: base(
			Cosmos.cosmosCmds,
			new ProcessStartCustomizer(Cosmos.ConfigureExec),
			(Cosmos.cosmosCmdFlags == null ? "" : Cosmos.cosmosCmdFlags) +
			string.Format(Cosmos.cosmosInCmdArgs, (fileName = fileName.Replace('\\', '/'))),
			false,
			true,
			null)
		{
			this.fileName = fileName;
			this.length = length;
			//if (length < 0 && backgroundGetLength)
			//{
			//    ThreadStart ts = new ThreadStart(FillLength);
			//    Thread t = new Thread(ts);
			//    t.IsBackground = true;
			//    gotLength = new ManualResetEvent(false);
			//    gettingLength = true;
			//    t.Start();
			//}
		}
		private CosmosReadStream(string fileName, LengthGetter lengthget)
			: base(
			Cosmos.cosmosCmds,
			new ProcessStartCustomizer(Cosmos.ConfigureExec),
			(Cosmos.cosmosCmdFlags == null ? "" : Cosmos.cosmosCmdFlags) +
			string.Format(Cosmos.cosmosInCmdArgs, (fileName = fileName.Replace('\\', '/'))),
			false,
			true,
			null)
		{
			this.fileName = fileName;
			this.length = lengthget.GetLength();
		}

		private long length = -1;
		//private ManualResetEvent gotLength;
		//private volatile bool gettingLength = false;
		//private void FillLength()
		//{
		//    try
		//    {
		//        length = Cosmos.GetLengthInner(fileName);
		//    }
		//    finally
		//    {
		//        gotLength.Set();
		//        gettingLength = false;
		//        gotLength = null;
		//    }
		//}
		//private long GetLengthBackground()
		//{
		//    ThreadStart ts = new ThreadStart(FillLength);
		//    Thread t = new Thread(ts);
		//    t.IsBackground = true;
		//    gotLength = new ManualResetEvent(false);
		//    gettingLength = true;
		//    t.Start();
		//    return -1;
		//}
		private class LengthGetter
		{
			string fileName;
			long length;
			Thread thread;

			public LengthGetter(string fileName)
			{
				this.fileName = fileName;
				ThreadStart ts = new ThreadStart(Go);
				thread = new Thread(ts);
				thread.IsBackground = true;
				thread.Start();
			}
			public void Go()
			{
				//length = IOUtil.GetLength(fileName);
				length = Cosmos.GetLengthInner(fileName);
			}

			public long GetLength()
			{
				if (thread != null)
				{
					thread.Join();
					thread = null;
				}
				return length;
			}

			public void Abort()
			{
				thread.Abort();
			}
		}

		/// <summary>
		/// Get the length of the stream, in bytes.
		/// </summary>
		/// <exception cref="NotSupportedException">The length cannot be retrieved, sometimes.</exception>
		/// <exception cref="ArgumentNullException">The stream name is null.</exception>
		/// <exception cref="ArgumentException">The stream name is invalid.</exception>
		/// <exception cref="InvalidOperationException">tmsncosmos.exe, cosmos.cmd, or cosmos.exe is not in the path, or Cosmos cannot be contacted.</exception>
		public override long Length
		{
			get
			{
				if (length < 0)
				{
					//ManualResetEvent m = gotLength;
					//if (gettingLength)
					//{
					//    m.WaitOne();
					//}
					//if (length < 0)
					//{
					length = Cosmos.GetLengthInner(fileName);
					if (length < 0)
					{
						throw new NotSupportedException("Cannot always get length of Cosmos file.");
					}
					//}
				}
				return length;
			}
		}

		/// <summary>
		/// Get the message to include in exceptions thrown when the commands cannot be found.
		/// </summary>
		protected override string CommandMissingMessage
		{
			get
			{
				return "CosmosReadStream requires tmsncosmos.exe, cosmos.cmd, or cosmos.exe to be in the " +
					"path for reading.";
			}
		}


		/// <summary>
		/// Get whether the stream can seek. Currently returns true, although Seek()
		/// will actually work with low performance.
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				return true;
			}
		}


		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for Cosmos streams, and seeking backwards reopens the stream!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset)
		{
			return base.Seek(offset);
		}
		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for Cosmos streams, and seeking backwards reopens the stream!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			return base.Seek(offset, origin);
		}
		/// <summary>
		/// Get or Set the position in the file, in bytes. Note that setting the position can
		/// be very slow for Cosmos streams!
		/// </summary>
		public override long Position
		{
			get
			{
				return base.Position;
			}
			set
			{
				base.Position = value;
			}
		}
	
	}




	/// <summary>
	/// Wrapper Stream to perform Cosmos writing.
	/// </summary>
	/// <remarks>
	/// <p>
	/// A tmsncosmos.exe, cosmos.cmd, or cosmos.exe command is needed in the path in order for this functionality
	/// to operate correctly.
	/// </p>
	/// <p>
	/// A shared set of tools will be used if no version is found in the path, although this
	/// increases the startup time. The environment variable COSMOS_TOOLS can be used to
	/// specify a directory or share for this purpose, instead of the default.
	/// </p>
	/// </remarks>
	public class CosmosWriteStream : CmdStream
	{
		private readonly string fileName;

		/// <summary>
		/// Create a stream to write to Cosmos.
		/// </summary>
		/// <param name="fileName">Name of the Cosmos stream to be wrapped for writing</param>
		/// <exception cref="ArgumentNullException">The stream name is null.</exception>
		/// <exception cref="ArgumentException">The stream name is invalid.</exception>
		/// <exception cref="InvalidOperationException">tmsncosmos.exe, cosmos.cmd, or cosmos.exe is not in the path, or Cosmos cannot be contacted.</exception>
		public CosmosWriteStream(string fileName)
			: this(fileName, false)
		{
		}
		/// <summary>
		/// Create a stream to write to Cosmos.
		/// The "cosmos" program must be in the path for this to work.
		/// </summary>
		/// <remarks>
		/// This will not avoid breaking between line boundaries.
		/// </remarks>
		/// <param name="fileName">Name of the Cosmos stream to be wrapped for writing</param>
		/// <param name="append">if true, append to an existing stream; otherwise, overwrite or create a new one</param>
		/// <exception cref="ArgumentNullException">The stream name is null.</exception>
		/// <exception cref="ArgumentException">The stream name is invalid.</exception>
		/// <exception cref="InvalidOperationException">tmsncosmos.exe, cosmos.cmd, or cosmos.exe is not in the path, or Cosmos cannot be contacted.</exception>
		/// <exception cref="FileNotFoundException">Append is specified, and the Cosmos stream cannot be found.</exception>
		/// <exception cref="IOException">Append is specified, and the delete cannot complete because of lack of permission or other issues.</exception>
		public CosmosWriteStream(string fileName, bool append)
			: this(fileName, append, false)
		{
		}
		/// <summary>
		/// Create a stream to write to Cosmos.
		/// The "cosmos" program must be in the path for this to work.
		/// </summary>
		/// <param name="fileName">Name of the Cosmos stream to be wrapped for writing</param>
		/// <param name="append">if true, append to an existing stream; otherwise, overwrite or create a new one</param>
		/// <param name="breakAtLines">if true, break extents internally only on line boundaries; otherwise,
		/// break at any byte</param>
		/// <exception cref="ArgumentNullException">The stream name is null.</exception>
		/// <exception cref="ArgumentException">The stream name is invalid.</exception>
		/// <exception cref="InvalidOperationException">tmsncosmos.exe, cosmos.cmd, or cosmos.exe is not in the path, or Cosmos cannot be contacted.</exception>
		/// <exception cref="FileNotFoundException">Append is specified, and the Cosmos stream cannot be found.</exception>
		/// <exception cref="IOException">Append is specified, and the delete cannot complete because of lack of permission or other issues.</exception>
		public CosmosWriteStream(string fileName, bool append, bool breakAtLines)
			: this(fileName, Overwrite(fileName, append), append, breakAtLines)
		{
		}
		private CosmosWriteStream(string fileName, long initialPosition, bool append, bool breakAtLines)
			: base(
			Cosmos.cosmosCmds,
			new ProcessStartCustomizer(Cosmos.ConfigureExec),
			(Cosmos.cosmosCmdFlags == null ? "" : Cosmos.cosmosCmdFlags) +
			string.Format(Cosmos.cosmosOutCmdArgs, (fileName = fileName.Replace('\\', '/')),
			((initialPosition != 0 && append) ? Cosmos.cosmosCmdAppendArg : "") +
			(breakAtLines ? Cosmos.cosmosCmdLineBoundariesArg : "")),
			true,
			false, null)
		{
			this.fileName = fileName;
			if (initialPosition > 0)
			{
				SetPosition(initialPosition);
			}
		}


		/// <summary>
		/// Get the length of the file, in bytes.
		/// </summary>
		/// <remarks>
		/// This will be equal to the current <see cref="Position"/>.
		/// </remarks>
		public override long Length
		{
			get
			{
				return Position;
			}
		}

		private static long Overwrite(string fileName, bool forAppend)
		{
			if (fileName == null) throw new ArgumentNullException("fileName cannot be null", "fileName");
			int slashIndex = fileName.LastIndexOf('/');
			if (slashIndex <= 0) throw new ArgumentException("fileName is not a valid Cosmos path", "fileName");
			// try to limit the directory:
			//string dir = fileName.Substring(0, slashIndex);
			string dir = fileName + "*";
			// could use fileName.Substring(0, fileName.Length - 1) + "?", instead...
			string file = fileName.Substring(slashIndex + 1);

			System.Diagnostics.Process proc = null;
			try
			{
				proc = Cosmos.CosmosExec(string.Format(Cosmos.cosmosDirCmdArgs, dir));
				using (StreamReader sr = proc.StandardOutput)
				{
					//////// Old:
					// Contents of directory 'cosmos://msrcosmos/vol1/logdata/slogs':
					// D StreamLength Last update (local)  Name
					//    7733886955 2006-05-03T15:04:00.670L+7h sr20060317.txt.gz
					//    4776338052 2006-05-04T09:23:40.326L+7h sr20060317.txt.7z
					//   23331633766 2006-05-03T16:31:29.930L+7h sr20060317.txt
					//////// New:
					//
					//  Directory of cosmos://msrcosmos/vol1/test
					//
					// 2006-06-20 16:27:21.963 PST            34,703 CLP.doc
					// ...........................    <DIR>          log
					// 2006-07-14 16:24:10.619 PST                20 test.txt
					//                      2 Stream(s)       34,723 bytes
					//                      1 Dir(s)
					// case-sensitive!:
					string match = " " + file;  //.ToLower();
					bool started = false;
					for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
					{
						if (!started)
						{
							line = line.Trim();
							if (//line.Length == 0 ||
								line.StartsWith("Directory of") ||
								(line.StartsWith("D ") && line.EndsWith(" Name")))
							{
								started = true;
							}
							continue;
						}
						if ((line.StartsWith("   ") || line.StartsWith("\t")) && line.EndsWith(" bytes"))
						{
							break;
						}
						line = line.Trim();
						if (line.Length == 0) continue;

						if (line.EndsWith(match))
						{
							if (forAppend)
							{
								// we really should offset the Position:
								try
								{
									//string[] cols = Cosmos.regexWhitespace.Split(line);
									line = line.Replace('\t', ' ');
									int oldLen = -1;
									while (line.Length != oldLen)
									{
										oldLen = line.Length;
										line = line.Replace("  ", " ");
									}
									line = line.Trim();
									string[] cols = line.Split(' ');

									if (cols[0].ToLower() == "d" ||
										(cols.Length > 1 && cols[cols.Length - 2].ToLower() == "<dir>"))
									{
										throw new IOException("Can not overwrite Cosmos directory as file: " + fileName);
									}
									long length = 0;
									for (int i = 0; i < cols.Length; i++)
									{
										try
										{
											string size = cols[i];
											if (size.IndexOfAny(Cosmos.nonnumber) < 0)
											{
												size = size.Replace(",", "");
												length = long.Parse(size);
												break;
											}
										}
										catch
										{
											// keep going...
										}
									}

									if (length == 0)
									{
										// we must delete it, since the caller will be trying to open without append!
										try
										{
											try
											{
												//sr.ReadToEnd();
												//proc.WaitForExit();
												proc.Close();
											}
											catch
											{
												// ignore...
											}
											Cosmos.Delete(fileName);
										}
										catch
										{
											// ignore here?
										}
									}
									return length;
								}
								catch
								{
									// When file is not found, we must create it!
									//throw new FileNotFoundException("Cosmos stream cannot be found for append: " + fileName);
									return 0;
								}
							}
							else
							{
								// need to delete the file!
								try
								{
									try
									{
										//sr.ReadToEnd();
										//proc.WaitForExit();
										proc.Close();
									}
									catch
									{
										// ignore...
									}
									Cosmos.Delete(fileName);
								}
								catch
								{
									throw new IOException("Cosmos stream cannot be deleted: " + fileName);
								}
								return 0;
							}
						}
					}
				}
			}
			catch (System.ComponentModel.Win32Exception)
			{
				throw new InvalidOperationException("CosmosWriteStream requires tmsncosmos.exe, cosmos.cmd, or cosmos.exe to be in the " +
					"path for reading.");
			}
			catch (IOException)
			{
				throw;
			}
			catch (Exception ex)
			{
				System.Diagnostics.Debug.WriteLine("Exception when opening for append: " +
					ex.ToString());
				//throw new InvalidOperationException("CosmosWriteStream needs cosmos.cmd to be in the path", ex);
			}
			finally
			{
				try
				{
					if (proc != null)  proc.Close();
				}
				catch
				{
					// ignore
				}
			}
			if (forAppend)
			{
				throw new FileNotFoundException("Cosmos stream cannot be found for append: " + fileName);
			}
			return 0;
		}


		/// <summary>
		/// Get whether the stream can seek (false).
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				return false;
			}
		}


		/// <summary>
		/// Seek to a new position in the file, in bytes (always fails).
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override long Seek(long offset)
		{
			throw new NotSupportedException("Seeking is not supported");
		}
		/// <summary>
		/// Seek to a new position in the file, in bytes (always fails).
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException("Seeking is not supported");
		}
		/// <summary>
		/// Get or Set the position in the file, in bytes. Set always fails.
		/// </summary>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override long Position
		{
			get
			{
				return base.Position;
			}
			set
			{
				throw new NotSupportedException("Seeking is not supported");
			}
		}
	
	}

}

