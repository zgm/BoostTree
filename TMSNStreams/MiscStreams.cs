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
	#region Command Stream

	/// <summary>
	/// This is a Stream that processes another Stream through a command-line program.
	/// </summary>
	public class CmdStream : Stream, IDisposable
	{
		private Stream inStream;
		private Stream outStream;
		private Stream origStream;
		private byte[] origBytes;
		private bool forWriting;
		private bool outToOrig = false;
		private bool reopenable;
		private Thread errorThread;

		private string commandName;
		private string commandArgs;
		private bool skipStdin;

#if !ENABLE_BARTOK
		private System.Diagnostics.Process proc;
#endif
		private bool ignoreClose = false;
		private const int BUFFER_SIZE = 32768;

		private long position = 0;

		/// <summary>
		/// Get or set whether to ignore a call to Close().
		/// </summary>
		public bool IgnoreClose
		{
			get { return ignoreClose; }
			set { ignoreClose = value; }
		}

		/// <summary>
		/// Get the message to include in exceptions thrown when the commands cannot be found.
		/// </summary>
		protected virtual string CommandMissingMessage
		{
			get
			{
				return "No commands found to execute.";
			}
		}

		//			protected abstract string CommandName
		//			{
		//				get;
		//			}
		//
		//			protected virtual string CommandArguments
		//			{
		//				get { return ""; }
		//			}

		/// <summary>
		/// Create a new Stream based on a command-line application, for reading.
		/// </summary>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string commandName, string commandArgs)
			: this(new string[] { commandName }, commandArgs)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application, for reading.
		/// </summary>
		/// <param name="commandNames">the set of commands to execute - the first one that is successful is used</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string[] commandNames, string commandArgs)
			: this(commandNames, commandArgs, false)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application, for reading.
		/// </summary>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="checkFileName">if non-null, fail if checkFileName does not exist</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string commandName, string commandArgs, string checkFileName)
			: this(new string[] { commandName }, commandArgs, checkFileName)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application, for reading.
		/// </summary>
		/// <param name="commandNames">the set of commands to execute - the first one that is successful is used</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="checkFileName">if non-null, fail if checkFileName does not exist</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string[] commandNames, string commandArgs, string checkFileName)
			: this(commandNames, commandArgs, false, checkFileName)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application.
		/// </summary>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="forWriting">if true, the Stream is for writing; if false, it is for reading</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string commandName, string commandArgs, bool forWriting)
			: this(new string[] { commandName }, commandArgs, forWriting)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application.
		/// </summary>
		/// <param name="commandNames">the set of commands to execute - the first one that is successful is used</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="forWriting">if true, the Stream is for writing; if false, it is for reading</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string[] commandNames, string commandArgs, bool forWriting)
			: this(commandNames, commandArgs, forWriting, false)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application.
		/// </summary>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="forWriting">if true, the Stream is for writing; if false, it is for reading</param>
		/// <param name="checkFileName">if non-null, fail if checkFileName does not exist</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string commandName, string commandArgs, bool forWriting, string checkFileName)
			: this(new string[] { commandName }, commandArgs, forWriting, checkFileName)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application.
		/// </summary>
		/// <param name="commandNames">the set of commands to execute - the first one that is successful is used</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="forWriting">if true, the Stream is for writing; if false, it is for reading</param>
		/// <param name="checkFileName">if non-null, fail if checkFileName does not exist</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string[] commandNames, string commandArgs, bool forWriting, string checkFileName)
			: this(commandNames, commandArgs, forWriting, false, checkFileName)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application.
		/// </summary>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="forWriting">if true, the Stream is for writing; if false, it is for reading</param>
		/// <param name="skipStdin">if true, do not redirect stdin</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string commandName, string commandArgs, bool forWriting, bool skipStdin)
			: this(new string[] { commandName }, commandArgs, forWriting, skipStdin)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application.
		/// </summary>
		/// <param name="commandNames">the set of commands to execute - the first one that is successful is used</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="forWriting">if true, the Stream is for writing; if false, it is for reading</param>
		/// <param name="skipStdin">if true, do not redirect stdin</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string[] commandNames, string commandArgs, bool forWriting, bool skipStdin)
			: this(commandNames, commandArgs, forWriting, skipStdin, null)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application.
		/// </summary>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="forWriting">if true, the Stream is for writing; if false, it is for reading</param>
		/// <param name="skipStdin">if true, do not redirect stdin</param>
		/// <param name="checkFileName">if non-null, fail if checkFileName does not exist</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string commandName, string commandArgs, bool forWriting, bool skipStdin, string checkFileName)
			: this(new string[] { commandName }, commandArgs, forWriting, skipStdin, checkFileName)
		{
		}

		/// <summary>
		/// Create a new Stream based on a command-line application.
		/// </summary>
		/// <param name="commandNames">the set of commands to execute - the first one that is successful is used</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="forWriting">if true, the Stream is for writing; if false, it is for reading</param>
		/// <param name="skipStdin">if true, do not redirect stdin</param>
		/// <param name="checkFileName">if non-null, fail if checkFileName does not exist</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string[] commandNames, string commandArgs, bool forWriting, bool skipStdin, string checkFileName)
			: this(commandNames, null, commandArgs, forWriting, skipStdin, checkFileName)
		{
		}

		/// <summary>
		/// Create a new Stream based on a command-line application.
		/// </summary>
		/// <param name="commandNames">the set of commands to execute - the first one that is successful is used</param>
		/// <param name="psiCustomizer">the delegate used to customize the ProcessStartInfo used, or null if not needed</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="forWriting">if true, the Stream is for writing; if false, it is for reading</param>
		/// <param name="skipStdin">if true, do not redirect stdin</param>
		/// <param name="checkFileName">if non-null, fail if checkFileName does not exist</param>
		/// <exception cref="FileNotFoundException">checkFileName cannot be found.</exception>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The command given cannot be found or executed.</exception>
		protected CmdStream(string[] commandNames, ProcessStartCustomizer psiCustomizer, string commandArgs, bool forWriting, bool skipStdin, string checkFileName)
		{
			// should we just return the bare stream here?? ****

			//Console.Out.Flush();
			//Console.Error.Flush();
			//Console.Error.WriteLine();
			//Console.Error.WriteLine(commandName + " " + commandArgs);
			//Console.Error.Flush();
			//if (checkFileName != null && !File.Exists(checkFileName)) throw new FileNotFoundException("File '" + checkFileName + "' not found.");
			if (checkFileName != null && !IOUtil.FileExists(checkFileName)) throw new FileNotFoundException("File '" + checkFileName + "' not found.");
			this.forWriting = forWriting;
			this.commandArgs = commandArgs;
			this.forWriting = forWriting;
			this.skipStdin = skipStdin;

			this.psiCustomizer = psiCustomizer;

			if (commandNames.Length == 0)
			{
				throw new InvalidOperationException("Stream cannot be created because no commands were supplied to wrap.");
			}
			for (int i = 0; i < commandNames.Length; i++)
			{
				commandName = commandNames[i];
				if (commandName == null)
				{
					if (i == commandNames.Length - 1)
					{
						throw new IOException(CommandMissingMessage);
					}
					continue;
				}
				// force to use the version next to the library, if available:
				try
				{
					if (File.Exists(Path.Combine(DllPath, commandName)) ||
						File.Exists(Path.Combine(DllPath, commandName + ".exe")))
					{
						commandName = Path.Combine(DllPath, commandName);
					}
					else if (File.Exists(Path.Combine(Path.Combine(DllPath, "bin"), commandName)) ||
						File.Exists(Path.Combine(Path.Combine(DllPath, "bin"), commandName + ".exe")))
					{
						commandName = Path.Combine(DllPath, commandName);
					}
				}
				catch
				{
					// ignore...
				}
				try
				{
					reopenable = true;
					Reopen();
					reopenable = !forWriting;
					// leave, if successful
					break;
				}
				catch
				{
					if (i == commandNames.Length - 1) throw;
					// try the next one, otherwise...
				}
			}
		}


		private int exitCode = 0;

		/// <summary>
		/// Get the last exit code produced by the underlying process.
		/// </summary>
		public int ExitCode
		{
			get
			{
				return exitCode;
			}
		}


		/// <summary>
		/// Method used to modify the ProcessStartInfo before executing the process.
		/// </summary>
		/// <param name="psi">the ProcessStartInfo to be modified as needed</param>
		protected delegate void ProcessStartCustomizer(System.Diagnostics.ProcessStartInfo psi);


		private ProcessStartCustomizer psiCustomizer = null;


		private static readonly object searchPathLock = new object();
		private static string[] searchPath = null;
		private static string FindInPath(string file)
		{
			if (searchPath == null)
			{
				lock (searchPathLock)
				{
					if (searchPath == null)
					{
						string[] pathVar = Environment.GetEnvironmentVariable("PATH").Split(';');
						ArrayList dirs = new ArrayList(pathVar.Length + 2);
						//if ((string)dirs[0] != DllPath)
						//{
						dirs.Add(DllPath);
						//}
						// ignore current directory? ***
						////dirs.Add(Environment.CurrentDirectory);
						for (int i = 0; i < pathVar.Length; i++)
						{
							string p = pathVar[i].Trim();
							if (p.Length == 0 || p == ".") continue;
							if (!dirs.Contains(p))
							{
								dirs.Add(p);
							}
						}
						searchPath = (string[])dirs.ToArray(typeof(string));
					}
				}
			}

			if (file == null || file.Length == 0) return null;
			// allow absolute path:
			// but avoid current directory? ***
			if (File.Exists(file)) return file;
			for (int i = 0; i < searchPath.Length; i++)
			{
				string p = Path.Combine(searchPath[i], file);
				if (File.Exists(p)) return p;
			}
			return null;
		}


		/// <summary>
		/// Restart the file reading at the beginning.
		/// </summary>
		/// <exception cref="IOException">The command cannot be executed.</exception>
		/// <exception cref="InvalidOperationException">The stream is not reopenable</exception>
		protected virtual void Reopen()
		{
			if (!reopenable) throw new InvalidOperationException("The stream is not reopenable.");
			Finish();
			if (proc != null)
			{
				try
				{
					proc.Kill();
					proc.WaitForExit();
				}
				catch
				{
				}
				//proc.WaitForExit();
				proc.Close();
				proc = null;
			}

			exitCode = -10;
			try
			{
				// **** threads must be restarted!!

				if (commandName.IndexOf('\\') < 0 && commandName.IndexOf('/') < 0) // && !File.Exists(commandName))
				{
					// should this be done? ***
					string foundName = FindInPath(commandName);
					if (foundName == null) foundName = FindInPath(commandName + ".exe");
					if (foundName == null) foundName = FindInPath(commandName + ".cmd");
					if (foundName == null) foundName = FindInPath(commandName + ".bat");
					if (foundName == null)
					{
						throw new InvalidOperationException("Stream cannot be created to wrap '" + commandName +
							"' because it cannot be found.");
					}
					commandName = foundName;
				}
				System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(commandName, commandArgs);
				//psi.WorkingDirectory = workingDir;
				psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
				psi.CreateNoWindow = true;
				psi.RedirectStandardInput = !skipStdin;  // is this always OK?
				psi.RedirectStandardOutput = true;
				psi.RedirectStandardError = true;
#if DOTNET2
				psi.StandardOutputEncoding = Encoding.UTF8;
#endif
				psi.UseShellExecute = false;

				if (psiCustomizer != null)
				{
					psiCustomizer(psi);
				}

				proc = System.Diagnostics.Process.Start(psi);

				StreamReader outReader = proc.StandardOutput;
				if (outReader == null)
				{
					throw new IOException("Cannot obtain output reader for command");
				}
				outStream = outReader.BaseStream;
				if (outStream == null)
				{
					throw new IOException("Cannot obtain output stream for command");
				}
#if !NO_BUFFER
				// this is a small buffer to make byte operations not be horribly expensive:
				//outStream = new BufferedStream(outStream, 4096 << 1);
				outStream = new BufferedStream(outStream, BUFFER_SIZE);
#endif
				errorThread = IOUtil.ConsumeBackground(proc.StandardError);

				// is this always OK?
				if (skipStdin)
				{
					inStream = null;
				}
				else
				{
					StreamWriter inWriter = proc.StandardInput;
					if (inWriter == null)
					{
						throw new IOException("Cannot obtain input writer for command");
					}
					inStream = inWriter.BaseStream;
					if (inStream == null)
					{
						throw new IOException("Cannot obtain input stream for command");
					}
#if !NO_BUFFER
					// this is a small buffer to make byte operations not be horribly expensive:
					//inStream = new BufferedStream(inStream, 4096 << 1);
					inStream = new BufferedStream(inStream, BUFFER_SIZE);
#endif
				}
				//Console.WriteLine("CmsStream: " + commandName + " " + commandArgs);
			}
			catch (System.ComponentModel.Win32Exception)
			{
				try
				{
					try
					{
						if (proc != null)
						{
							proc.Kill();
							proc.WaitForExit();
						}
						exitCode = proc.ExitCode;
					}
					catch
					{
					}
					proc.Close();
					proc = null;
				}
				catch
				{
				}
				//throw new InvalidOperationException("Stream cannot be created to wrap '" + commandName +
				//    "' because it cannot be found.");
				throw new InvalidOperationException(CommandMissingMessage);
			}
			catch
			{
				if (proc != null)
				{
					try
					{
						try
						{
							proc.Kill();
							proc.WaitForExit();
							exitCode = proc.ExitCode;
						}
						catch
						{
						}
						proc.Close();
						proc = null;
					}
					catch
					{
					}
				}
				throw;
			}
			exitCode = 0;
			position = 0;
		}

		private static readonly object dllPathLock = new object();
		private static string dllPath = null;
		private static string DllPath
		{
			get
			{
				if (dllPath == null)
				{
					lock (dllPathLock)
					{
						if (dllPath == null)
						{
							dllPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
							dllPath = Path.GetDirectoryName(dllPath);
						}
					}
				}
				return dllPath;
			}
		}

		/// <summary>
		/// Create a new readable stream based on a command-line application that
		/// passes the provided data through the application.
		/// </summary>
		/// <param name="orig">the buffer of data to use</param>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		protected CmdStream(byte[] orig, string commandName, string commandArgs)
			: this(commandName, commandArgs, false)
		{
			// this needs to be fixed - Reopen should handle this... ***
			reopenable = false;
			writeThread = new Thread(new ThreadStart(WriteOrigBytesToIn));
			writeThreadCancelable = true;
			writeThread.IsBackground = true;
			writeThread.Start();
		}

		/// <summary>
		/// Create a new stream based on a command-line application, for reading,
		/// that passes the data read from the original stream to the command-line
		/// application and provides the results output by the application.
		/// </summary>
		/// <param name="orig">the original data to use</param>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		protected CmdStream(Stream orig, string commandName, string commandArgs)
			: this(orig, commandName, commandArgs, false)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application, for reading.
		/// </summary>
		/// <param name="origStreamName">the stream of data to use</param>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="streamFlag">ignored parameter that signals that a stream name is provided</param>
		protected CmdStream(string origStreamName, bool streamFlag, string commandName, string commandArgs)
			: this(origStreamName, streamFlag, commandName, commandArgs, false)
		{
		}

		/// <summary>
		/// Create a new Stream based on a command-line application, for writing,
		/// that passes all data written to the original stream to the command-line
		/// application.
		/// </summary>
		/// <param name="dest">the stream to send the output to</param>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		protected CmdStream(string commandName, string commandArgs, Stream dest)
			: this(dest, commandName, commandArgs, true)
		{
		}
		/// <summary>
		/// Create a new Stream based on a command-line application, for writing.
		/// </summary>
		/// <param name="destStreamName">the Stream to send the output to</param>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="streamFlag">ignored parameter that signals that a stream name is provided</param>
		protected CmdStream(string commandName, string commandArgs, string destStreamName, bool streamFlag)
			: this(destStreamName, streamFlag, commandName, commandArgs, true)
		{
		}

		/// <summary>
		/// Create a new Stream based on a command-line application.
		/// </summary>
		/// <param name="orig">the buffer of data to use</param>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="forWriting">if true, the Stream is for writing; if false, it is for reading</param>
		protected CmdStream(Stream orig, string commandName, string commandArgs, bool forWriting)
			: this(commandName, commandArgs, forWriting)
		{
			// should we create and support an IReopenable ? ***
			// or perhaps use CanSeek and Seek? ***
			reopenable = false;
			origStream = orig;
			//if (origStream != null)
			//{
			if (forWriting)
			{
				outToOrig = true;
				writeThread = new Thread(new ThreadStart(WriteOutToOrig));
				// should this be false, and the other true?? ***
				writeThreadCancelable = false;
				writeThread.IsBackground = false;
				writeThread.Start();
			}
			else
			{
				writeThread = new Thread(new ThreadStart(WriteOrigToIn));
				writeThreadCancelable = true;
				writeThread.IsBackground = true;
				writeThread.Start();
			}
			//}
		}


		/// <summary>
		/// Create a new Stream based on a command-line application.
		/// </summary>
		/// <param name="origStreamName">the buffer of data to use</param>
		/// <param name="commandName">the command to execute</param>
		/// <param name="commandArgs">the arguments to pass to the command</param>
		/// <param name="forWriting">if true, the Stream is for writing; if false, it is for reading</param>
		/// <param name="streamFlag">ignored parameter that signals that a stream name is provided</param>
		protected CmdStream(string origStreamName, bool streamFlag, string commandName, string commandArgs, bool forWriting)
			: this(commandName, commandArgs, forWriting)
		{
			// should we create and support an IReopenable ? ***
			// or perhaps use CanSeek and Seek? ***
			try
			{
				if (forWriting)
				{
					origStream = ZStreamOut.Open(origStreamName);
					outToOrig = true;
					writeThread = new Thread(new ThreadStart(WriteOutToOrig));
					// should this be false, and the other true?? ***
					writeThreadCancelable = false;
					writeThread.IsBackground = false;
					writeThread.Start();
				}
				else
				{
					origStream = ZStreamIn.Open(origStreamName);
					writeThread = new Thread(new ThreadStart(WriteOrigToIn));
					writeThreadCancelable = true;
					writeThread.IsBackground = true;
					writeThread.Start();
				}
			}
			catch
			{
				if (origStream != null)
				{
					try
					{
						origStream.Close();
					}
					catch
					{
					}
				}
				if (proc != null)
				{
					try
					{
						proc.Close();
					}
					catch
					{
					}
				}
				throw;
			}
		}


		private bool writeThreadCancelable = false;
		private Thread writeThread;


		private void WriteOutToOrig()
		{
			//Console.WriteLine("WriteOutToOrig");
			try
			{
				byte[] buffer = new byte[BUFFER_SIZE];  //BUFFER_SIZE > 0 ? BUFFER_SIZE : 4096];
				int bytesRead;
				while ((bytesRead = outStream.Read(buffer, 0, buffer.Length)) > 0)
				{
					//Console.WriteLine("WriteOutToOrig: read " + bytesRead);
					//try
					//{
					origStream.Write(buffer, 0, bytesRead);
					//}
					//catch (Exception ex2)
					//{
					//	throw new InvalidOperationException("origStream broke", ex2);
					//}
				}
				// don't close the orig stream?
				origStream.Flush();
				outStream.Close();
				outStream = null;
			}
			catch //(Exception ex)
			{
				//Console.WriteLine("WriteOutToOrig broke: " + ex.ToString());
				// should we say anything here? ***
			}
		}

		private void WriteOrigToIn()
		{
			try
			{
				byte[] buffer = new byte[BUFFER_SIZE];  //BUFFER_SIZE > 0 ? BUFFER_SIZE : 4096];
				int bytesRead;
				while ((bytesRead = origStream.Read(buffer, 0, buffer.Length)) > 0)
				{
					inStream.Write(buffer, 0, bytesRead);
				}
				// don't close the orig stream?
				inStream.Flush();
				inStream.Close();
				//Console.WriteLine("WriteOrigToIn()");
				inStream = null;
			}
			catch
			{
				// should we say anything here? ***
			}
			finally
			{
			}
		}

		private void WriteOrigBytesToIn()
		{
			try
			{
				if (origBytes != null)
				{
					int pos = 0;
					while (pos < origBytes.Length)
					{
						int end = pos + BUFFER_SIZE;
						if (end > origBytes.Length) end = origBytes.Length;
						inStream.Write(origBytes, pos, end - pos);
						pos = end;
					}
					origBytes = null;
				}
				inStream.Flush();
				inStream.Close();
				//Console.WriteLine("WriteOrigBytesToIn()");
				inStream = null;
			}
			catch
			{
				// should we say anything here? ***
			}
		}



		#region Cleanup

		/// <summary>
		/// <para>Ensures that resources are freed and other cleanup operations are performed when the garbage collector
		/// reclaims the <see langword="CmdStream" /> .</para>
		/// </summary>
		~CmdStream()
		{
			//Console.WriteLine("Finalize: " + commandName + " " + commandArgs);
			//this.Dispose(false);
			this.Dispose(true);
		}

		void IDisposable.Dispose()
		{
			//Console.WriteLine("IDisposable.Dispose: " + commandName + " " + commandArgs);
			Close();
		}

		/// <summary>
		/// <para>Closes the file and releases any resources associated with
		/// the current file stream.</para>
		/// </summary>
		/// <exception cref="T:System.IO.IOException">An error occurred while trying to close the stream.</exception>
		public override void Close()
		{
			//Console.WriteLine("Close: " + commandName + " " + commandArgs);
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// <para>Releases the unmanaged resources used by the <see cref="CmdStream" /> and optionally
		/// releases the managed resources.</para>
		/// </summary>
		/// <remarks>
		/// Actually, the value of disposing is ignored, just as in <see cref="FileStream"/>.
		/// </remarks>
		/// <param name="disposing">
		/// <see langword="true" /> to release both managed and unmanaged resources; <see langword="false" /> to release only unmanaged resources.</param>
#if DOTNET2
		protected override void Dispose(bool disposing)
#else
		protected virtual void Dispose(bool disposing)
#endif
		{
			//Console.WriteLine("Dispose(" + disposing + "): " + commandName + " " + commandArgs);
			if (!IgnoreClose)
			{
				Finish();
				if (forWriting)
				{
					// close the orig stream?
					if (origStream != null && !outToOrig)
					{
						//Console.WriteLine("Closing: origStream (Dispose forWriting)");
						try
						{
							origStream.Flush();
							origStream.Close();
						}
						catch { }
						origStream = null;
					}
				}
				else
				{
					if (outStream != null)
					{
						//Console.WriteLine("Closing: outStream (Dispose !forWriting)");
						try
						{
							outStream.Flush();
							outStream.Close();
						}
						catch { }
						outStream = null;
					}
				}
				// kill the process? ***
			}
			if (writeThread != null)
			{
				//try
				//{
				//    //writeThread.Join();
				//    writeThread.Abort();
				//}
				//catch
				//{
				//}
				if (writeThreadCancelable)
				{
					if (origStream != null)
					{
						try
						{
							origStream.Close();
							origStream = null;
						}
						catch { }
						origStream = null;
					}
					try
					{
						writeThread.Abort();
					}
					catch
					{
					}
					writeThread = null;
				}
				else
				{
					//if (outToOrig)
					//{
					writeThread.Join();
					writeThread = null;
					//Console.WriteLine("Closing: origStream (Dispose forWriting)");
					if (origStream != null)
					{
						try
						{
							origStream.Flush();
							origStream.Close();
						}
						catch { }
						origStream = null;
					}
					//}
				}
			}
			// is this OK is we are writing? ***
			if (errorThread != null)
			{
				// kill the error consumer?
				try
				{
					errorThread.Abort();
				}
				catch
				{
					// ignore
				}
			}

			// kill the process? ***
			if (proc != null)
			{
				if (!forWriting)
				{
					proc.WaitForExit(4);
					if (!proc.HasExited)
					{
						try
						{
							proc.Kill();
						}
						catch
						{
						}
						exitCode = -20;
					}
					else
					{
						exitCode = proc.ExitCode;
					}
				}
				else
				{
					// This is not good enough! If it fails, we have not written successfully.
					// Should we throw an exception, then? ***
					// Note that we should also consider throwing an exception for both reading
					// *and* writing, based on the exit code (and possibly the error stream)... ***
					proc.WaitForExit(32000);
					
                    try {
   					    exitCode = proc.HasExited ? proc.ExitCode : -20;

                        if (!proc.HasExited)
                        {
                            // kill? alert? ***
                            try
                            {
                                proc.Kill();
                            }
                            catch
                            {
                            }
                        }
                    }
   					catch (Exception)
   					{
   					    exitCode = -20;
   					}
				}
				
				try {
				    proc.Close();
				} catch {}

				proc = null;
			}
#if DOTNET2
			base.Dispose(disposing);
#else
			//base.Dispose(disposing);
#endif
		}

		/// <summary>
		/// Flush all data and close the streams.
		/// </summary>
		private void Finish()
		{
			//Console.WriteLine("Finish: " + commandName + " " + commandArgs);
			Flush();
			if (forWriting)
			{
				if (inStream != null)
				{
					//Console.WriteLine("Closing: inStream (Finish forWriting)");
					try
					{
						inStream.Flush();
						inStream.Close();
					}
					catch { }
					inStream = null;
				}
				//if (writeThread != null)
				//{
				//    try
				//    {
				//        writeThread.Join();
				//        writeThread = null;
				//    }
				//    catch { }
				//}
			}
			else
			{
				// nothing to do, here...
			}
			if (outStream != null && !outToOrig)
			{
				//Console.WriteLine("Closing: outStream (Finish)");
				try
				{
					outStream.Flush();
					outStream.Close();
				}
				catch { }
				outStream = null;
			}
			if (inStream != null)
			{
				//Console.WriteLine("Closing: inStream (Finish forWriting)");
				try
				{
					inStream.Flush();
					inStream.Close();
				}
				catch { }
				inStream = null;
			}
		}

		#endregion


		/// <summary>
		/// Flush all data.
		/// </summary>
		public override void Flush()
		{
			if (inStream != null)
			{
				if (inStream.CanWrite)
				{
					try
					{
						inStream.Flush();
					}
					catch { }
				}
			}
			if (outStream != null)
			{
				if (outStream.CanWrite)
				{
					try
					{
						outStream.Flush();
					}
					catch { }
				}
			}
			if (origStream != null)
			{
				if (origStream.CanWrite)
				{
					try
					{
						origStream.Flush();
					}
					catch { }
				}
			}
		}


		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <returns>the number of bytes read</returns>
		/// <exception cref="NotSupportedException">The stream was opened for writing.</exception>
		public override int Read(byte[] buffer, int offset, int count)
		{
			if (forWriting) throw new NotSupportedException();
			//return outStream.Read(buffer, offset, count);
			int countRead = outStream.Read(buffer, offset, count);
			//			if (countRead <= 0)
			//			{
			//				Console.WriteLine("countRead : " + countRead);
			//				Console.WriteLine("outStream.ReadByte() : " + outStream.ReadByte());
			//			}
			if (countRead < 0)
			{
				return countRead;
			}
			if (countRead != count)
			{
				while (true)
				{
					int next = outStream.Read(buffer, countRead + offset, count - countRead);
					if (next <= 0)
					{
						//Console.WriteLine("!! failed to complete read - " + last + " / " + Backing.Length);
						break;
					}
					countRead += next;
					if (countRead == count) break;
				}
			}
			position += countRead;
			return countRead;
		}

		/// <summary>
		/// Read a single byte.
		/// </summary>
		/// <returns>the byte read, or a negative number if end of file</returns>
		/// <exception cref="NotSupportedException">The stream was opened for writing.</exception>
		public override int ReadByte()
		{
			if (forWriting) throw new NotSupportedException();
			// this can be very slow if called too often!! ***
			int res = outStream.ReadByte();
			if (res >= 0) position++;
			return res;
		}

		/// <summary>
		/// Begin an asynchronous read.
		/// </summary>
		/// <param name="buffer">the buffer to fill</param>
		/// <param name="offset">the position to start filling</param>
		/// <param name="count">the number of bytes to read</param>
		/// <param name="callback">the method to call when completed</param>
		/// <param name="state">the state to pass to the callback</param>
		/// <returns>an IAsyncResult for managing this request</returns>
		public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			// position??
			position += count;
			return outStream.BeginRead(buffer, offset, count, callback, state);
		}

		/// <summary>
		/// End an asynchronous read.
		/// </summary>
		/// <param name="asyncResult">the IAsyncResult of the request to end</param>
		/// <returns>the number of bytes read</returns>
		public override int EndRead(IAsyncResult asyncResult)
		{
			return outStream.EndRead(asyncResult);
		}

		/// <summary>
		/// Move forward by reading and discarding bytes.
		/// </summary>
		/// <param name="count">the number of bytes to skip</param>
		protected void Skip(long count)
		{
			if (count == 0) return;
			//if (forWriting)  throw new NotSupportedException();
			byte[] dump = new byte[Math.Min(count, 256 * 1024)];
			if (dump.Length != count)
			{
				int rem = (int)(count % dump.Length);
				if (Read(dump, 0, rem) < rem) return;
				count -= rem;
			}
			while (count > 0)
			{
				int read = Read(dump, 0, dump.Length);
				if (read != dump.Length)
				{
					System.Diagnostics.Debug.WriteLine("Skip failed! Read " + read + " / " + dump.Length + " for chunk.");
					return;
				}
				count -= dump.Length;
			}
		}


		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for CmdStreams, and seeking backwards only works when Reopen is
		/// supported!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public virtual long Seek(long offset)
		{
			return Seek(offset, SeekOrigin.Begin);
		}
		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for CmdStreams, and seeking backwards only works when Reopen is
		/// supported!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		/// <exception cref="NotSupportedException">The stream cannot seek.</exception>
		public override long Seek(long offset, SeekOrigin origin)
		{
			if (!CanSeek) throw new NotSupportedException("The stream cannot seek.");
			long cur = Position;
			switch (origin)
			{
				case SeekOrigin.Begin:
					break;
				case SeekOrigin.Current:
					offset += cur;
					break;
				case SeekOrigin.End:
					offset = Length - offset;
					break;
			}
			if (offset < 0) offset = 0;
			if (offset == cur) return cur;

			if (offset > cur)
			{
				Skip(offset - cur);
				return Position;
			}
			else
			{
				Reopen();
				Skip(offset);
				return Position;
			}
		}

		/// <summary>
		/// Get or Set the position in the file, in bytes. Note that setting the position can
		/// be very slow for CmdStreams!
		/// </summary>
		public override long Position
		{
			get
			{
				return position;
			}
			set
			{
				Seek(value, SeekOrigin.Begin);
			}
		}

		/// <summary>
		/// Force the current known position, without seeking.
		/// </summary>
		/// <param name="position">the new position to use</param>
		protected void SetPosition(long position)
		{
			this.position = position;
		}

		/// <summary>
		/// Set the file length - not supported.
		/// </summary>
		/// <param name="value">the length to ignore</param>
		public override void SetLength(long value)
		{
			// should we ignore this? It is supposed to be for modifying Streams while *writing*...
			//throw new NotSupportedException();
		}


		/// <summary>
		/// Write data from the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to read the data from</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to write</param>
		/// <exception cref="InvalidOperationException">The stream was opened for reading.</exception>
		public override void Write(byte[] buffer, int offset, int count)
		{
			if (!forWriting)  throw new InvalidOperationException("Cannot write to a CmdStream opened for reading.");
			//Console.WriteLine("Write: " + position + " + " + count +
			//	"  : " + commandName + " " + commandArgs);

			// avoid choking:
			int fullEnd = offset + count;
			while (offset < fullEnd)
			{
				int end = offset + BUFFER_SIZE;
				if (end > fullEnd) end = fullEnd;
				//if (inStream == null) throw new NullReferenceException("inStream null at " + position);
				inStream.Write(buffer, offset, end - offset);
				offset = end;
			}
			//inStream.Write(buffer, offset, count);
			
			//Console.WriteLine("Write completed");
			position += count;
		}

		/// <summary>
		/// Write a single byte.
		/// </summary>
		/// <param name="value">the byte to write</param>
		/// <exception cref="InvalidOperationException">The stream was opened for reading.</exception>
		public override void WriteByte(byte value)
		{
			if (!forWriting) throw new InvalidOperationException("Cannot write to a CmdStream opened for reading.");
			// this can be very slow if called too often! ***
			inStream.WriteByte(value);
			position++;
		}

		/// <summary>
		/// Begin an aynchronous write.
		/// </summary>
		/// <param name="buffer">the buffer to write from</param>
		/// <param name="offset">the position at which to use the bytes</param>
		/// <param name="count">the number of bytes to write</param>
		/// <param name="callback">the method to call when complete</param>
		/// <param name="state">tje state to pass the callback</param>
		/// <returns>an IAsyncResult for controlling the operation</returns>
		/// <exception cref="InvalidOperationException">The stream was opened for reading.</exception>
		public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			if (!forWriting) throw new InvalidOperationException("Cannot write to a CmdStream opened for reading.");
			return inStream.BeginWrite(buffer, offset, count, callback, state);
		}

		/// <summary>
		/// End an asynchronous write.
		/// </summary>
		/// <param name="asyncResult">the IAsyncResult for the write to end</param>
		/// <exception cref="InvalidOperationException">The stream was opened for reading.</exception>
		public override void EndWrite(IAsyncResult asyncResult)
		{
			if (!forWriting) throw new InvalidOperationException("Cannot write to a CmdStream opened for reading.");
			inStream.EndWrite(asyncResult);
		}


		/// <summary>
		/// Whether the stream can read,
		/// </summary>
		public override bool CanRead
		{
			get
			{
				return !forWriting;
			}
		}

		/// <summary>
		/// Whether the stream can seek,
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				//return false;
				return reopenable;
			}
		}

		/// <summary>
		/// Whether the stream can write,
		/// </summary>
		public override bool CanWrite
		{
			get
			{
				return forWriting;
			}
		}

		/// <summary>
		/// Get the length of the file - not supported by default.
		/// </summary>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override long Length
		{
			get
			{
				throw new NotSupportedException();
			}
		}
	}

	#endregion


	#region Bounded Stream

	/// <summary>
	/// A stream that exposes a given section of another stream.
	/// It follows the model of BufferedStream in wrapping an existing stream, rather
	/// than being a subclass, for flexibility.
	/// It is read-only, and the Stream used to construct it must be readable.
	/// </summary>
	public class BoundedStream : Stream
	{
		private long offset;
		private long limit;
		private Stream baseStream;
		private bool ignoreClose = false;
		private bool checkedLength = false;
		private long position;

		/// <summary>
		/// Get or set whether to ignore a call to Close(). False by default.
		/// </summary>
		public bool IgnoreClose
		{
			get { return ignoreClose; }
			set { ignoreClose = value; }
		}

		/// <summary>
		/// Construct a read-only Stream wrapping the given Stream which starts at the specified
		/// offset position and ends at the specified limit position, not inclusive of the byte
		/// at the limit position.
		/// </summary>
		/// <param name="orig">The original Stream, which must be readable and seekable</param>
		/// <param name="offset">The position at which to start (to be considered 0)</param>
		/// <param name="limit">The position at which to cut off</param>
		/// <exception cref="ArgumentNullException">The base stream is null.</exception>
		/// <exception cref="ArgumentException">The base stream does not support reading.</exception>
		/// <exception cref="IOException">The base stream cannot seek to the beginning of the bounds.</exception>
		/// <remarks>
		/// Seeking is only required if the offset is negative, but many stream types will
		/// irritatingly not support the Position property if they cannot seek. In that case, consider
		/// the constructor that assumes the current position as that starting point.
		/// </remarks>
		public BoundedStream(Stream orig, long offset, long limit)
		{
			baseStream = orig;
			if (baseStream == null) throw new ArgumentNullException("Base Stream cannot be null.", "orig");
			if (!baseStream.CanRead) throw new ArgumentException("Base Stream must support reading.", "orig");
			this.offset = offset;
			this.limit = limit;
			////if (!baseStream.CanSeek)  throw new ArgumentException("Base Stream must support seeking.", "orig");
			position = baseStream.Position;
			if (position != offset)
			{
				if (position < offset)
				{
					long left = offset - position;
					if (left < 40000 || !baseStream.CanSeek)
					{
						byte[] dummy = new byte[(int)Math.Min(left, 40000)];
						while (left > 0)
						{
							int c = baseStream.Read(dummy, 0, (int)Math.Min(left, dummy.Length));
							if (c <= 0) throw new IOException("Cannot seek to beginning of bounds");
							left -= c;
						}
					}
					else
					{
						long resOffset;
						try
						{
							resOffset = baseStream.Seek(offset, SeekOrigin.Begin);
							//baseStream.Seek(b, SeekOrigin.Current);
						}
						catch (Exception ex)
						{
							throw new IOException("Cannot seek to beginning of bounds", ex);
						}
						if (resOffset != offset)
						{
							throw new IOException("Cannot seek to beginning of bounds");
						}
					}
				}
				else
				{
					long resOffset;
					try
					{
						resOffset = baseStream.Seek(offset, SeekOrigin.Begin);
					}
					catch (Exception ex)
					{
						throw new IOException("Cannot seek to beginning of bounds", ex);
					}
					if (resOffset != offset)
					{
						throw new IOException("Cannot seek to beginning of bounds");
					}
				}
			}
			position = 0;
		}

		/// <summary>
		/// Construct a read-only Stream wrapping the given Stream which starts at the current
		/// position and ends at the current position plus the specified length, not inclusive of the byte
		/// at the current position plus length.
		/// </summary>
		/// <param name="orig">The original Stream, which must be readable</param>
		/// <param name="length">The position past the current at which to cut off</param>
		/// <exception cref="ArgumentNullException">The base stream is null.</exception>
		/// <exception cref="ArgumentException">The base stream does not support reading.</exception>
		public BoundedStream(Stream orig, long length)
		{
			baseStream = orig;
			if (baseStream == null) throw new ArgumentNullException("Base Stream cannot be null.", "orig");
			if (!baseStream.CanRead) throw new ArgumentException("Base Stream must support reading.", "orig");
			try
			{
				this.offset = baseStream.Position;
				this.limit = offset + length;
				position = 0;
			}
			catch
			{
				// assume that baseStream.Position is not supported!
				// this is signified by the negative offset:
				this.offset = -1;
				this.limit = length;
				position = 0;
			}
		}

		/// <summary>
		/// Close the stream.
		/// </summary>
		public override void Close()
		{
			if (!IgnoreClose)
			{
				baseStream.Close();
			}
		}

		/// <summary>
		/// Flush all data.
		/// </summary>
		public override void Flush()
		{
			baseStream.Flush();
		}

		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <returns>the number of bytes read</returns>
		public override int Read(byte[] buffer, int offset, int count)
		{
			long truncCount = limit - Position;
			if (truncCount < 0) truncCount = 0;
			if (truncCount < count) count = (int)truncCount;
			if (count <= 0) return 0;
			int b = baseStream.Read(buffer, offset, count);
			position += b;
			return b;
		}

		/// <summary>
		/// Read a single byte.
		/// </summary>
		/// <returns>the byte read, or a negative number if end of file</returns>
		public override int ReadByte()
		{
			if (Position >= limit) return -1;
			int b = baseStream.ReadByte();
			if (b >= 0) position++;
			return b;
		}

		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <param name="callback">the callback to use</param>
		/// <param name="state">the state to use for the callback</param>
		/// <returns>the number of bytes read</returns>
		/// <exception cref="IOException">Read is positioned out of bounds.</exception>
		public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			// we could just let this map back to the sync version, with the performance hit for
			// some base streams...
			long truncCount = limit - Position;
			if (truncCount < 0) truncCount = 0;
			if (truncCount < count) count = (int)truncCount;
			IAsyncResult res = baseStream.BeginRead(buffer, offset, count, callback, state);
			// Assume it will all get read? **
			position += count;
			return res;
		}

		/// <summary>
		/// End an asynchronous read.
		/// </summary>
		/// <param name="asyncResult">the result</param>
		/// <returns>number of bytes read</returns>
		public override int EndRead(IAsyncResult asyncResult)
		{
			return baseStream.EndRead(asyncResult);
		}

		/// <summary>
		/// Seek to a given position.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public long Seek(long offset)
		{
			return Seek(offset, SeekOrigin.Begin);
		}
		/// <summary>
		/// Seek to a given position.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			if (this.offset < 0)
			{
				throw new NotSupportedException("Stream does not support seeking.");
			}
			switch (origin)
			{
				case SeekOrigin.Begin:
					// leave it as it is
					break;
				case SeekOrigin.Current:
					offset = Position;
					break;
				case SeekOrigin.End:
					offset = Length + offset;
					break;
			}
			// what should we do with out-of-bounds seeks? **
			if (offset < 0)
			{
				offset = 0;
			}
			else if (offset > Length)
			{
				offset = Length;
			}
			long res = baseStream.Seek(this.offset + offset, SeekOrigin.Begin);
			position = res - this.offset;
			return position;
		}

		/// <summary>
		/// Set the file length - not a good idea.
		/// </summary>
		/// <param name="value">the length to set it to</param>
		public override void SetLength(long value)
		{
			// should we ignore this? It is supposed to be for modifying Streams while *writing*...
			if (value < 0)
			{
				value = 0;
			}
			if (offset < 0)
			{
				// non-seekable...
				limit = value;
			}
			else
			{
				if (offset + value > limit)
				{
					checkedLength = false;
				}
				limit = offset + value;
			}
		}

		/// <summary>
		/// Write data from the buffer - always throws.
		/// </summary>
		/// <param name="buffer">the buffer to read the data from</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to write</param>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException("BoundedStream cannot write.");
			//// just pass this call on... Should we throw an exception?
			//if (offset < 0)
			//{
			//    // non-seekable... what is best?
			//    // hack in an update if no exception is thrown... this can also overrun the limit!
			//    baseStream.Write(buffer, offset, count);
			//    position += count;
			//}
			//else
			//{
			//    position = -1;
			//    baseStream.Write(buffer, offset, count);
			//}
		}

		/// <summary>
		/// Write a single byte - always throws.
		/// </summary>
		/// <param name="value">the byte to write</param>
		/// <exception cref="NotSupportedException">Always thrown.</exception>
		public override void WriteByte(byte value)
		{
			throw new NotSupportedException("BoundedStream cannot write.");
			//// just pass this call on... Should we throw an exception?
			//if (offset < 0)
			//{
			//    // non-seekable... what is best?
			//    // hack in an update if no exception is thrown... this can also overrun the limit!
			//    baseStream.Write(buffer, offset, count);
			//    position++;
			//}
			//else
			//{
			//    position = -1;
			//    baseStream.WriteByte(value);
			//}
		}

		/// <summary>
		/// Get whether the stream can read - true.
		/// </summary>
		public override bool CanRead
		{
			get
			{
				return true;
			}
		}

		/// <summary>
		/// Get whether the stream can seek.
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				// should always be false if (offset < 0):
				return baseStream.CanSeek;
			}
		}

		/// <summary>
		/// Get whether the stream can write - false.
		/// </summary>
		public override bool CanWrite
		{
			get
			{
				return false;
			}
		}

		/// <summary>
		/// Get the length of the stream window.
		/// </summary>
		public override long Length
		{
			get
			{
				if (offset < 0) return limit;
				try
				{
					// this might not be desirable:
					if (!checkedLength)
					{
						checkedLength = true;
						limit = Math.Min(limit, baseStream.Length);
						// should we keep checking?
						//checkedLength = true;
					}
					return limit - offset;
				}
				catch
				{
					// maybe it can't seek?
					return limit - offset;
				}
			}
		}

		/// <summary>
		/// Get or set the position in the stream window.
		/// </summary>
		public override long Position
		{
			get
			{
				// Actually, all of this mess is *almost* not needed.
				// Calling the FileStream Position is not very expensive unless the handle is exposed -
				// it is the Length that is expensive, and that is somewhat understandable, since it
				// could be changing. However, Position is for some strange reason not supported if
				// the FileStream is not seekable, and assumedly others are similar.
				if (offset >= 0 && position < 0) position = baseStream.Position - offset;
				return position;
			}
			set
			{
				if (position < 0 || position != value)
				{
					if (offset < 0) throw new NotSupportedException("Cannot set position on a non-seekable stream.");
					long newPosition = value;
					// these are questionable: **
					if (newPosition < 0) newPosition = 0;
					if (newPosition > limit - offset) newPosition = limit - offset;
					if (newPosition != Position)
					{
						baseStream.Position = newPosition + offset;
						position = newPosition;
					}
				}
			}
		}
	}

	#endregion


	#region Ignore Close Stream

	/// <summary>
	/// A stream that wraps another stream to ignore any call to close it.
	/// This allows the stream to be used in cases where the wrapping class thinks
	/// that it 'owns' the stream.
	/// </summary>
	/// <remarks>
	/// Note that a <see cref="BoundedStream"/> can be set to ignore close using a
	/// property.
	/// </remarks>
	public sealed class IgnoreCloseStream : Stream
	{
		private readonly Stream baseStream;
		private readonly bool ignoreFlush;

		///// <summary>
		///// Get or set whether to ignore a call to Close().
		///// </summary>
		//public bool IgnoreClose
		//{
		//    get { return ignoreClose; }
		//    set { ignoreClose = value; }
		//}

		/// <summary>
		/// Construct a non-closing stream wrapping the given stream.
		/// </summary>
		/// <param name="orig">The original stream</param>
		public IgnoreCloseStream(Stream orig)
			: this(orig, false)
		{
		}
		/// <summary>
		/// Construct a non-closing stream wrapping the given stream,
		/// optionally also ignoring flushing.
		/// </summary>
		/// <param name="orig">The original stream</param>
		/// <param name="ignoreFlush">If true, ignore calls to <see cref="Flush"/> also; if false, pass them through</param>
		public IgnoreCloseStream(Stream orig, bool ignoreFlush)
		{
			baseStream = orig;
			this.ignoreFlush = ignoreFlush;
		}

		/// <summary>
		/// Close the stream.
		/// </summary>
		public override void Close()
		{
			// ignore
			try
			{
				baseStream.Flush();
			}
			catch
			{
			}
		}

		/// <summary>
		/// Flush all data.
		/// </summary>
		public override void Flush()
		{
			//Console.WriteLine("Flush");
			if (!ignoreFlush)
			{
				baseStream.Flush();
			}
		}

		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <returns>the number of bytes read</returns>
		public override int Read(byte[] buffer, int offset, int count)
		{
			//Console.Write("Read count = " + count);
			int res =  baseStream.Read(buffer, offset, count);
			//Console.WriteLine(" completed");
			return res;
		}

		/// <summary>
		/// Read a single byte.
		/// </summary>
		/// <returns>the byte read, or a negative number if end of file</returns>
		public override int ReadByte()
		{
			//Console.WriteLine("ReadByte");
			return baseStream.ReadByte();
		}

		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <param name="callback">the callback to use</param>
		/// <param name="state">the state to use for the callback</param>
		/// <returns>the number of bytes read</returns>
		/// <exception cref="IOException">Read is positioned out of bounds.</exception>
		public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			//Console.WriteLine("BeginRead count = " + count);
			return baseStream.BeginRead(buffer, offset, count, callback, state);
		}

		/// <summary>
		/// End an asynchronous read.
		/// </summary>
		/// <param name="asyncResult">the result</param>
		/// <returns>number of bytes read</returns>
		public override int EndRead(IAsyncResult asyncResult)
		{
			return baseStream.EndRead(asyncResult);
		}

		/// <summary>
		/// Seek to a given position.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public long Seek(long offset)
		{
			return Seek(offset, SeekOrigin.Begin);
		}
		/// <summary>
		/// Seek to a given position.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			//Console.WriteLine("Seek " + offset);
			return baseStream.Seek(offset, origin);
		}

		/// <summary>
		/// Set the file length.
		/// </summary>
		/// <param name="value">the length to set it to</param>
		public override void SetLength(long value)
		{
			baseStream.SetLength(value);
		}

		/// <summary>
		/// Write data from the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to read the data from</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to write</param>
		public override void Write(byte[] buffer, int offset, int count)
		{
			baseStream.Write(buffer, offset, count);
		}

		/// <summary>
		/// Write a single byte.
		/// </summary>
		/// <param name="value">the byte to write</param>
		public override void WriteByte(byte value)
		{
			baseStream.WriteByte(value);
		}

		/// <summary>
		/// Get whether the stream can read.
		/// </summary>
		public override bool CanRead
		{
			get
			{
				//Console.WriteLine("CanRead");
				return baseStream.CanRead;
			}
		}

		/// <summary>
		/// Get whether the stream can seek.
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				//Console.WriteLine("CanSeek");
				return baseStream.CanSeek;
			}
		}

		/// <summary>
		/// Get whether the stream can write.
		/// </summary>
		public override bool CanWrite
		{
			get
			{
				return baseStream.CanWrite;
			}
		}

		/// <summary>
		/// Get the length of the stream.
		/// </summary>
		public override long Length
		{
			get
			{
				//Console.WriteLine("Length");
				return baseStream.Length;
			}
		}

		/// <summary>
		/// Get or set the position in the stream.
		/// </summary>
		public override long Position
		{
			get
			{
				//Console.WriteLine("Position");
				return baseStream.Position;
			}
			set
			{
				baseStream.Position = value;
			}
		}
	}

	#endregion


	#region Null Stream

	/// <summary>
	/// This is a Stream that ignores reads and writes.
	/// </summary>
	public class NullStream : Stream
	{
		bool zeroFillReads = false;

		/// <summary>
		/// Get or Set whether to return sequences of 0 for all reads, if true,
		/// or to act as a zero-length stream for reads, if false.
		/// </summary>
		public bool ZeroFillReads
		{
			get { return zeroFillReads; }
			set { zeroFillReads = value; }
		}

		/// <summary>
		/// Construct a Stream that ignores all input.
		/// </summary>
		/// <remarks>
		/// This defaults ZeroFillReads to false.
		/// </remarks>
		public NullStream()
			: this(false)
		{
		}

		/// <summary>
		/// Construct a Stream that ignores all input.
		/// </summary>
		/// <param name="zeroFillReads">if true, return sequences of 0 for all reads;
		/// act as a zero-length stream for reads, otherwise</param>
		public NullStream(bool zeroFillReads)
		{
			ZeroFillReads = zeroFillReads;
		}

		/// <summary>
		/// Close the stream.
		/// </summary>
		public override void Close()
		{
		}

		/// <summary>
		/// Flush all data.
		/// </summary>
		public override void Flush()
		{
		}

		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <returns>the number of bytes read</returns>
		public override int Read(byte[] buffer, int offset, int count)
		{
			if (ZeroFillReads)
			{
				// should handle bad input here...
				for (int i = offset; i < offset + count; i++)
				{
					buffer[i] = (byte)0;
				}
				return count;
			}
			else
			{
				return 0;
			}
		}

		/// <summary>
		/// Read a single byte.
		/// </summary>
		/// <returns>the byte read, or a negative number if end of file</returns>
		public override int ReadByte()
		{
			if (ZeroFillReads)
			{
				return 0;
			}
			else
			{
				return -1;
			}
		}


		/// <summary>
		/// Seek to a given position.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public long Seek(long offset)
		{
			return offset;
		}
		/// <summary>
		/// Seek to a given position.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			return offset;
		}

		/// <summary>
		/// Set the file length.
		/// </summary>
		/// <param name="value">the length to set it to</param>
		public override void SetLength(long value)
		{
		}

		/// <summary>
		/// Write data from the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to read the data from</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to write</param>
		public override void Write(byte[] buffer, int offset, int count)
		{
		}

		/// <summary>
		/// Write a single byte.
		/// </summary>
		/// <param name="value">the byte to write</param>
		public override void WriteByte(byte value)
		{
		}

		/// <summary>
		/// Whether the stream can read,
		/// </summary>
		public override bool CanRead
		{
			get
			{
				return true;
			}
		}

		/// <summary>
		/// Whether the stream can seek,
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				return true;
			}
		}

		/// <summary>
		/// Whether the stream can write,
		/// </summary>
		public override bool CanWrite
		{
			get
			{
				return true;
			}
		}

		/// <summary>
		/// Get the length of the file.
		/// </summary>
		public override long Length
		{
			get
			{
				if (ZeroFillReads)
				{
					return long.MaxValue;
				}
				else
				{
					return 0;
				}
			}
		}

		/// <summary>
		/// Get or set the position in the file - not supported.
		/// </summary>
		public override long Position
		{
			get
			{
				return 0;
			}
			set
			{
			}
		}
	}

	#endregion


	#region HTTP Stream

	/// <summary>
	/// Wrapper Stream to perform HTTP reading.
	/// </summary>
	/// <remarks>
	/// <p>
	/// NT authentication will work, if needed.
	/// </p>
	/// <p>
	/// A URL can specify a POST request if the request message is added after the
	/// URL, surrounded by "$$" pairs. ("$" should be escaped as "\$" within the POST message).
	/// The POST message should then be URL-encoded. This would be of the form:
	/// </p>
	/// <p>
	/// <code>  "http://www.microsoft.com$$var1=2&amp;var3=hello$$"</code>
	/// </p>
	/// </remarks>
	public class HttpStream : Stream
	{
		private readonly string url;
		private long length = -1;
		private Stream response = null;

		private static string referer = null;
		/// <summary>
		/// Get or Set the referer to use when sending HTTP requests.
		/// </summary>
		/// <remarks>
		/// By default, no referer is sent.
		/// </remarks>
		public static string Referer
		{
			get { return referer; }
			set { referer = value; }
		}

		private static string userAgent = null;
		/// <summary>
		/// Get or Set the user-agent to use when sending HTTP requests.
		/// </summary>
		public static string UserAgent
		{
			get { return userAgent; }
			set { userAgent = value; }
		}

		private static string proxy = null;
		/// <summary>
		/// Get or Set the proxy to use when sending HTTP requests.
		/// </summary>
		/// <remarks>
		/// A value of null means to use the default proxy, and a value of the
		/// empty string means to use no proxy. The default proxy will obey any
		/// static settings in IE, but it will not understand automatic proxy
		/// detection.
		/// </remarks>
		public static string Proxy
		{
			get { return proxy; }
			set { proxy = value; }
		}

		/// <summary>
		/// Get the URL for this HTTP stream.
		/// </summary>
		public string Url
		{
			get
			{
				return url;
			}
		}

		private static int timeout = -1;
		/// <summary>
		/// Get or Set the timeout, in seconds, to use when sending HTTP requests.
		/// </summary>
		/// <remarks>
		/// <p>
		/// If less than zero, requests will not time out.
		/// </p>
		/// <p>
		/// By default, requests will wait indefinitely.
		/// </p>
		/// </remarks>
		public static int Timeout
		{
			get { return timeout; }
			set { timeout = value; }
		}


		/// <summary>
		/// Create a stream to read from an HTTP URL.
		/// </summary>
		/// <param name="url">URL of the HTTP stream to be read</param>
		/// <exception cref="FileNotFoundException">The URL cannot be found.</exception>
		/// <remarks>
		/// <p>
		/// NT authentication will work, if needed.
		/// </p>
		/// <p>
		/// A URL can specify a POST request if the request message is added after the
		/// URL, surrounded by "$$" pairs. ("$" should be escaped as "\$" within the POST message).
		/// The POST message should then be URL-encoded. This would be of the form:
		/// </p>
		/// <p>
		/// <code>  "http://www.microsoft.com$$var1=2&amp;var3=hello$$"</code>
		/// </p>
		/// <p>
		/// A URL can force a proxy server to be used if the proxy server is added to the host after
		/// an '@' character, as in:
		/// </p>
		/// <p>
		/// <code>  "http://www.microsoft.com@netproxy/default.aspx"</code>
		/// </p>
		/// </remarks>
		public HttpStream(string url)
		{
			int cIndex = url.IndexOf(':');
			int sIndex = url.IndexOf('/');
			if (cIndex < 0 || (sIndex >= 0 && sIndex < cIndex))
			{
				url = url.TrimStart('/');
				url = "http://" + url;
			}
			this.url = url;

			Reopen();
		}

		private void Reopen()
		{
			if (response != null)
			{
				response.Close();
				response = null;
			}
			System.Net.HttpWebRequest webRequest = null;
			System.Net.HttpWebResponse webResponse = null;
			try
			{
				// check for POST hack:
				string post = null;
				string u = url;
				string p = proxy;
				if (url.EndsWith("$$"))
				{
					int start = url.LastIndexOf("$$", url.Length - 3);
					if (start > 0)
					{
						u = url.Substring(0, start);
						post = url.Substring(start + 2, url.Length - start - 4);
						post = post.Replace("\\$", "$");
						// convert to newlines?
						//post = post.Replace("\\&", "\x1");
						//post = post.Replace("&", "\r\n");
						//post = post.Replace("\x1", "&");
					}
				}

				// check for forced proxy server:
				// skip past protocol
				int hostStart = u.IndexOf(':');
				if (hostStart > 0)
				{
					hostStart++;
					while (hostStart < u.Length && u[hostStart] == '/') hostStart++;
					if (hostStart < u.Length - 2)
					{
						int hostEnd = hostStart + 1;
						while (hostEnd < u.Length && u[hostEnd] != '/') hostEnd++;
						int div = u.IndexOf('@', hostStart + 1, hostEnd - hostStart - 1);
						if (div > 0)
						{
							// extract proxy!
							p = u.Substring(div + 1, hostEnd - div - 1);
							u = u.Substring(0, div) + u.Substring(hostEnd);
						}
					}
				}

				// initialize the request:
				webRequest = (System.Net.HttpWebRequest)System.Net.WebRequest.Create(u);

				// configure for authentication:
				webRequest.UnsafeAuthenticatedConnectionSharing = true;
				webRequest.Credentials = System.Net.CredentialCache.DefaultCredentials;

				// configure proxy:
				if (p == null)
				{
					// should we use the .NET current setting, or force to the default?
					//webRequest.Proxy = System.Net.WebProxy.GetDefaultProxy();
				}
				else
				{
					if (p.Length == 0)
					{
#if DOTNET2
						webRequest.Proxy = null;
#else
						webRequest.Proxy = System.Net.GlobalProxySelection.GetEmptyWebProxy();
#endif
					}
					else
					{
						if (p.Length <= "http://".Length ||
							string.Compare("http://", 0, p, 0, "http://".Length, true) != 0)
						{
							p = "http://" + p;
						}
						// could cache, but it is mutable...
						// on the other hand, we never expose the proxy...
						// (this could throw an exception of UrlFormatException.)
						webRequest.Proxy = new System.Net.WebProxy(p);
						webRequest.Proxy.Credentials = System.Net.CredentialCache.DefaultCredentials;
					}
				}

				// configure for POST:
				if (post != null)
				{
					webRequest.Method = "POST";
					webRequest.ContentType = "application/x-www-form-urlencoded";
					webRequest.KeepAlive = false;
					try
					{
						//post = HttpUtility.UrlEncode(post);
						byte[] postBytes = UTF8Encoding.UTF8.GetBytes(post);
						webRequest.ContentLength = postBytes.Length;
						using (Stream postWriter = webRequest.GetRequestStream())
						{
							postWriter.Write(postBytes, 0, postBytes.Length);
						}
					}
					catch
					{
						throw new FileNotFoundException("The url is malformed.");
					}
					//Console.WriteLine("Sending post: " + post);
				}

				// set various properties:
				string userAgent = UserAgent;
				if (userAgent != null)
				{
					webRequest.UserAgent = userAgent;
				}
				string referer = Referer;
				if (referer != null)
				{
					webRequest.Referer = referer;
				}
				int timeout = Timeout;
				if (timeout > 0)
				{
					webRequest.Timeout = timeout;
				}

				// increase connection limit:
				webRequest.ServicePoint.ConnectionLimit = 40;

				// send the request:
				webResponse = (System.Net.HttpWebResponse)webRequest.GetResponse();
				webRequest = null;
				if (webResponse == null) throw new FileNotFoundException("The URL cannot be found: " + url);
				length = webResponse.ContentLength;

				response = webResponse.GetResponseStream();
				// will this close the WebResponse when the Stream is closed?? ***
				return;
			}
			catch
			{
				//if (webRequest != null)  webRequest.Abort();
				if (webResponse != null)
				{
					try
					{
						webResponse.Close();
					}
					catch
					{
						// ignore
					}
				}
				throw;
			}
		}

		/// <summary>
		/// Move forward by reading and discarding bytes.
		/// </summary>
		/// <param name="count">the number of bytes to skip</param>
		private void Skip(long count)
		{
			if (count == 0) return;
			byte[] dump = new byte[Math.Min(count, 256 * 1024)];
			if (dump.Length != count)
			{
				int rem = (int)(count % dump.Length);
				if (Read(dump, 0, rem) < rem) return;
				count -= rem;
			}
			while (count > 0)
			{
				int read = Read(dump, 0, dump.Length);
				if (read != dump.Length)
				{
					System.Diagnostics.Debug.WriteLine("Skip failed! Read " + read + " / " + dump.Length + " for chunk.");
					return;
				}
				count -= dump.Length;
			}
		}

		/// <summary>
		/// Get the length of the Stream, in bytes.
		/// </summary>
		/// <exception cref="NotSupportedException">The length cannot be found sometimes.</exception>
		public override long Length
		{
			get
			{
				if (length < 0)
				{
					throw new NotSupportedException("Cannot always get length of HTTP Stream.");
				}
				return length;
			}
		}


		/// <summary>
		/// Determine if an HTTP URL exists.
		/// </summary>
		/// <param name="url">the URL to look for</param>
		/// <returns>true if the URL exists, false otherwise</returns>
		public static bool Exists(string url)
		{
			if (url == null || url.Length == 0) return false;
			try
			{
				using (Stream s = new HttpStream(url))
				{
				}
			}
			catch
			{
				return false;
			}
			return true;
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
		/// be very slow for HTTP streams, and seeking backwards reopens the stream!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <returns>the new position</returns>
		public virtual long Seek(long offset)
		{
			return Seek(offset, SeekOrigin.Begin);
		}
		/// <summary>
		/// Seek to a new position in the file, in bytes. Note that setting the position can
		/// be very slow for HTTP streams, and seeking backwards reopens the stream!
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			long cur = response.Position;
			switch (origin)
			{
				case SeekOrigin.Begin:
					break;
				case SeekOrigin.Current:
					offset += cur;
					break;
				case SeekOrigin.End:
					offset = Length - offset;
					break;
			}
			if (offset < 0) offset = 0;
			if (offset == cur) return cur;

			if (offset > cur)
			{
				Skip(offset - cur);
				return Position;
			}
			else
			{
				Reopen();
				Skip(offset);
				return Position;
			}
		}

		/// <summary>
		/// Get or Set the position in the file, in bytes. Note that setting the position can
		/// be very slow for HTTP streams!
		/// </summary>
		public override long Position
		{
			get
			{
				return response.Position;
			}
			set
			{
				Seek(value, SeekOrigin.Begin);
			}
		}


		/// <summary>
		/// Close the stream.
		/// </summary>
		public override void Close()
		{
			response.Close();
		}

		/// <summary>
		/// Flush all data.
		/// </summary>
		public override void Flush()
		{
		}

		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <returns>the number of bytes read</returns>
		public override int Read(byte[] buffer, int offset, int count)
		{
			return response.Read(buffer, offset, count);
		}

		/// <summary>
		/// Read a single byte.
		/// </summary>
		/// <returns>the byte read, or a negative number if end of file</returns>
		public override int ReadByte()
		{
			return response.ReadByte();
		}

		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <param name="callback">the callback to use</param>
		/// <param name="state">the state to use for the callback</param>
		/// <returns>the number of bytes read</returns>
		public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			return response.BeginRead(buffer, offset, count, callback, state);
		}

		/// <summary>
		/// End an asynchronous read.
		/// </summary>
		/// <param name="asyncResult">the result</param>
		/// <returns>number of bytes read</returns>
		public override int EndRead(IAsyncResult asyncResult)
		{
			return response.EndRead(asyncResult);
		}

		/// <summary>
		/// Set the file length (not supported)
		/// </summary>
		/// <param name="value">the length to not set it to</param>
		/// <exception cref="NotSupportedException">This is always thrown, since the length cannot be set.</exception>
		public override void SetLength(long value)
		{
			throw new NotSupportedException("Length cannot be set for HttpStream");
		}

		/// <summary>
		/// Write data from the buffer (not supported)
		/// </summary>
		/// <param name="buffer">the buffer to read the data from</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to write</param>
		/// <exception cref="NotSupportedException">This is always thrown, since writing is not allowed.</exception>
		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException("Writing not supported for HttpStream");
		}

		/// <summary>
		/// Write a single byte (not supported)
		/// </summary>
		/// <param name="value">the byte to write</param>
		/// <exception cref="NotSupportedException">This is always thrown, since writing is not allowed.</exception>
		public override void WriteByte(byte value)
		{
			throw new NotSupportedException("Writing not supported for HttpStream");
		}

		/// <summary>
		/// Whether the stream can read,
		/// </summary>
		public override bool CanRead
		{
			get
			{
				return true;
			}
		}

		/// <summary>
		/// Whether the stream can write,
		/// </summary>
		public override bool CanWrite
		{
			get
			{
				return false;
			}
		}

	}

	#endregion


	#region MultiStream

	// This could be extended to create the catalogs, and also to work directly
	// on Streams (effectively splitting or concatenating them) ***

	/// <summary>
	/// Stream made up of multiple files.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The specified filename for a MultiStream actually refers to a list of files.
	/// This list can itself be stored in any form accessible to TMSNStreams for
	/// reading - a simple file, a compressed archive, an HTTP URL, a Cosmos
	/// stream, the clipboard, the console, and so on.
	/// </para>
	/// <para>
	/// Within that list, lines are trimmed, and blank lines are ignored. Lines
	/// beginning with "#" are also ignored. Lines beginning with "@" are treated as
	/// recursive inclusions of additional lists of files.
	/// </para>
	/// <para>
	/// When opening for writing, all listed files except the last must have a tab-seperated
	/// size listed, in bytes, which will be the maximum size for that file.
	/// (For include lines, the size is not needed if all included lines themselves have
	/// sizes specified).
	/// </para>
	/// </remarks>
	public class MultiStream : Stream, IDisposable
	{
		Chunk[] chunks;
		int currentChunkIndex;
		bool forWriting;

		#region Constructors


		/// <summary>
		/// Open the specified catalog for reading as a Stream.
		/// The catalog should have one filename per line; an optional tab-seperated size will
		/// be ignored if present.
		/// </summary>
		/// <param name="fileCatalogName">a list of chunk locations, one filename per line,
		/// with an optional tab-seperated size.</param>
		/// <exception cref="ArgumentException">The file catalog is invalid.</exception>
		/// <remarks>
		/// <para>
		/// The specified filename for a MultiStream actually refers to a list of files.
		/// This list can itself be stored in any form accessible to TMSNStreams for
		/// reading - a simple file, a compressed archive, an HTTP URL, a Cosmos
		/// stream, the clipboard, the console, and so on.
		/// </para>
		/// <para>
		/// Within that list, lines are trimmed, and blank lines are ignored. Lines
		/// beginning with "#" are also ignored. Lines beginning with "@" are treated as
		/// recursive inclusions of additional lists of files.
		/// </para>
		/// </remarks>
		public MultiStream(string fileCatalogName)
			: this(fileCatalogName, false)
		{
		}

		/// <summary>
		/// Open the specified catalog for reading or writing as a Stream.
		/// The catalog should have one filename per line, with an optional tab-seperated size.
		/// The size is ignored for reading, and it is required for writing (except for the
		/// last file, which may be unbounded).
		/// </summary>
		/// <param name="fileCatalogName">a list of chunk locations, one filename per line,
		/// with an optional tab-seperated size.</param>
		/// <param name="forWriting">if true, open for writing; otherwise, open for reading</param>
		/// <exception cref="ArgumentException">The file catalog is invalid.</exception>
		/// <remarks>
		/// <para>
		/// The specified filename for a MultiStream actually refers to a list of files.
		/// This list can itself be stored in any form accessible to TMSNStreams for
		/// reading - a simple file, a compressed archive, an HTTP URL, a Cosmos
		/// stream, the clipboard, the console, and so on.
		/// </para>
		/// <para>
		/// Within that list, lines are trimmed, and blank lines are ignored. Lines
		/// beginning with "#" are also ignored. Lines beginning with "@" are treated as
		/// recursive inclusions of additional lists of files.
		/// </para>
		/// <para>
		/// When opening for writing, all listed files except the last must have a tab-seperated
		/// size listed, in bytes, which will be the maximum size for that file.
		/// (For include lines, the size is not needed if all included lines themselves have
		/// sizes specified).
		/// </para>
		/// </remarks>
		public MultiStream(string fileCatalogName, bool forWriting)
		{
			this.forWriting = forWriting;
			ArrayList chunkList = FetchChunkList(fileCatalogName, forWriting);
			//Console.WriteLine("Catalog: " + fileCatalogName);
			//foreach (Chunk chunk in chunkList)
			//{
			//    Console.WriteLine(chunk.FileName);
			//}

			chunks = (Chunk[])chunkList.ToArray(typeof(Chunk));
			if (forWriting)
			{
				if (chunks.Length == 0)
				{
					throw new ArgumentException("Empty catalog provided for MultiStream when writing", "fileCatalogName");
				}
				for (int i = 0; i < chunks.Length - 1; i++)
				{
					if (chunks[i].Size <= 0)
					{
						throw new ArgumentException("Only the last segment size can be unspecified", "fileCatalogName");
					}
				}
			}
			currentChunkIndex = 0;
			chunks[currentChunkIndex].Open();
		}


		private static ArrayList FetchChunkList(string fileCatalogName, bool forWriting)
		{
			// bubble up all exceptions

			ArrayList chunkList = new ArrayList();
			// should we hold this open to keep it locked?
			using (StreamReader sr = ZStreamReader.Open(fileCatalogName))
			{
				for (string line = sr.ReadLine(); line != null; line = sr.ReadLine())
				{
					line = line.Trim();
					if (line.Length == 0) continue;
					// allow comments:
					if (line[0] == '#') continue;

					long maxLength = -1;
					int tab = line.IndexOf('\t');
					if (tab >= 0)
					{
						// (tab cannot be 0)
						string maxLengthString = line.Substring(tab + 1).Trim();
						line = line.Substring(0, tab).Trim();
						//if (line.Length == 0) continue;
						if (forWriting)
						{
							try
							{
								maxLength = long.Parse(maxLengthString);
							}
							catch
							{
								maxLength = -1;
							}
							if (maxLength <= 0) maxLength = -1;
						}
					}

					// allow some wildcards:
					// (should all be allowed?)
					if (line.IndexOf('*') >= 0 || line.IndexOf('?') >= 0)
					{
						if (forWriting)
						{
							throw new ArgumentException("Wildcards are not allowed when writing.", "fileCatalogName");
						}
						string[] matches = IOUtil.ExpandWildcards(line);
						for (int i = 0; i < matches.Length; i++)
						{
							chunkList.Add(new Chunk(matches[i]));
						}
						continue;
					}

					// allow includes:
					if (line[0] == '@')
					{
						string includedList = line.Substring(1).Trim();
						if (includedList.Length == 0) continue;
						ArrayList included = FetchChunkList(includedList, forWriting);
						if (forWriting && maxLength > 0)
						{
							for (int i = 0; i < included.Count; i++)
							{
								if (maxLength <= 0)
								{
								}
								Chunk chunk = (Chunk)included[i];
								if (chunk.Size <= 0)
								{
									if (i < included.Count - 1)
									{
										throw new ArgumentException("Invalid size specified for writing", "fileCatalogName");
									}
									chunk.Size = maxLength;
								}
								else
								{
									if (maxLength <= chunk.Size)
									{
										chunk.Size = maxLength;
										if (i < included.Count - 1)
										{
											included.RemoveRange(i + 1, included.Count - (i + 1));
										}
										break;
									}
									maxLength -= chunk.Size;
								}
							}
						}
						chunkList.AddRange(included);
						continue;
					}

					if (forWriting)
					{
						chunkList.Add(new Chunk(line, maxLength));
					}
					else
					{
						chunkList.Add(new Chunk(line));
					}
				}
			}
			return chunkList;
		}


		/// <summary>
		/// Release and resources held by this instance.
		/// </summary>
		~MultiStream()
		{
			Close();
		}
		#endregion


		#region Status
		/// <summary>
		/// Get wether reading is allowed.
		/// </summary>
		public override bool CanRead
		{
			get
			{
				return !forWriting;
			}
		}

		/// <summary>
		/// Get whether seeking is allowed.
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				// what should this be?
				return !forWriting;
			}
		}

		/// <summary>
		/// Get whether writing is allowed.
		/// </summary>
		public override bool CanWrite
		{
			get
			{
				return forWriting;
			}
		}

		/// <summary>
		/// Close the stream,
		/// </summary>
		public override void Close()
		{
			for (int i = 0; i < chunks.Length; i++)
			{
				if (chunks[i] != null)
				{
					chunks[i].Close();
				}
			}
			base.Close();
			GC.SuppressFinalize(this);
		}

		void IDisposable.Dispose()
		{
			//((IDisposable)base).Dispose();
			Close();
			GC.SuppressFinalize(this);
		}

		private Chunk Current
		{
			get
			{
				if (currentChunkIndex < 0 || currentChunkIndex >= chunks.Length) return null;
				return chunks[currentChunkIndex];
			}
		}

		/// <summary>
		/// Flush all data.
		/// </summary>
		public override void Flush()
		{
			Chunk c = Current;
			if (c != null)
			{
				Stream s = c.Stream;
				if (s != null)
				{
					s.Flush();
				}
			}
		}
		#endregion


		#region Reading

		/// <summary>
		/// Read a single byte.
		/// </summary>
		/// <returns>the byte read, or a negative number if end of file</returns>
		public override int ReadByte()
		{
			while (currentChunkIndex < chunks.Length)
			{
				//Console.WriteLine("Reading byte of chunk: " + Current.FileName + " @ " + Current.Position);
				Chunk s = Current;
				//if (s == null) return -1;
				int res = s.Stream.ReadByte();
				if (res < 0)
				{
					NextChunk();
				}
				else
				{
					s.Position++;
					return res;
				}
			}
			return -1;
		}

		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <returns>the number of bytes read</returns>
		public override int Read(byte[] buffer, int offset, int count)
		{
			if (count == 0) return 0;
			if (buffer == null) throw new ArgumentNullException("buffer cannot be null.", "buffer");
			if (offset < 0 || offset >= buffer.Length) throw new ArgumentException("Invalid offset: " + offset + ", buffer.Length: " + buffer.Length, "offset");
			if (count < 0 || offset + count > buffer.Length) throw new ArgumentException("Invalid count: " + count + ", offset: " + offset + ", buffer.Length: " + buffer.Length, "count");
			int numReadTotal = 0;
			while (count > 0 && !Eof())
			{
				//Console.WriteLine("Reading chunk: " + Current.FileName + " @ " + Current.Position);
				int numRead = chunks[currentChunkIndex].Stream.Read(buffer, offset, count);
				if (numRead <= 0)
				{
					NextChunk();
					continue;
				}
				numReadTotal += numRead;
				count -= numRead;
				offset += numRead;
				chunks[currentChunkIndex].Position += numRead;
			}
			return numReadTotal;
		}

		private void NextChunk()
		{
			if (currentChunkIndex >= chunks.Length) return;
			if (currentChunkIndex >= 0)
			{
				chunks[currentChunkIndex].Close(true);
			}
			currentChunkIndex++;
			if (currentChunkIndex < chunks.Length)
			{
				chunks[currentChunkIndex].Open();
			}
		}

		private bool Eof()
		{
			return currentChunkIndex >= chunks.Length;
		}
		#endregion


		#region Writing

		/// <summary>
		/// Write a single byte.
		/// </summary>
		/// <param name="value">the byte to write</param>
		/// <exception cref="IOException">The segments are full.</exception>
		public override void WriteByte(byte value)
		{
			if (Eof())
			{
				throw new IOException("MultiStream segments are full.");
			}
			if (chunks[currentChunkIndex].Size > 0)
			{
				if (chunks[currentChunkIndex].Position >= chunks[currentChunkIndex].Size)
				{
					NextChunk();
					if (Eof())
					{
						throw new IOException("MultiStream segments are full.");
					}
				}
				chunks[currentChunkIndex].Stream.WriteByte(value);
				chunks[currentChunkIndex].Position++;
			}
			else
			{
				chunks[currentChunkIndex].Stream.WriteByte(value);
				chunks[currentChunkIndex].Position++;
			}
		}

		/// <summary>
		/// Write data from the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to read the data from</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to write</param>
		/// <exception cref="IOException">The segments are full.</exception>
		public override void Write(byte[] buffer, int offset, int count)
		{
			if (Eof())
			{
				throw new IOException("MultiStream segments are full.");
			}
			if (chunks[currentChunkIndex].Size > 0)
			{
				if (chunks[currentChunkIndex].Position >= chunks[currentChunkIndex].Size)
				{
					NextChunk();
					if (Eof())
					{
						throw new IOException("MultiStream segments are full.");
					}
				}
				while (count > 0 && !Eof())
				{
					long segmentLeft = chunks[currentChunkIndex].Size - chunks[currentChunkIndex].Position;
					if (segmentLeft < count)
					{
						chunks[currentChunkIndex].Stream.Write(buffer, offset, (int)segmentLeft);
						chunks[currentChunkIndex].Position += segmentLeft;
						NextChunk();
						count -= (int)segmentLeft;
						offset += (int)segmentLeft;
					}
					else
					{
						chunks[currentChunkIndex].Stream.Write(buffer, offset, count);
						chunks[currentChunkIndex].Position += count;
						count = 0;
					}
				}
			}
			else
			{
				chunks[currentChunkIndex].Stream.Write(buffer, offset, count);
				chunks[currentChunkIndex].Position += count;
			}
		}
		#endregion


		#region Position
		/// <summary>
		/// Set the file length - not supported.
		/// </summary>
		/// <param name="value">the size to not set the file to</param>
		public override void SetLength(long value)
		{
			// what do we do here??
			// should we write back to the catalog?
			// for that matter, should the whole catalog be constructed through interacting with this class? ***

			// ignore, for now...
		}

		/// <summary>
		/// Seek to the given position.
		/// </summary>
		/// <param name="offset">the offset to seek to</param>
		/// <returns>the new position</returns>
		public long Seek(long offset)
		{
			return Seek(offset, SeekOrigin.Begin);
		}
		/// <summary>
		/// Seek to the given position.
		/// </summary>
		/// <param name="offset">the offset to seek to</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			long position = offset;
			if (origin == SeekOrigin.Current)
			{
				position += Position;
			}
			else if (origin == SeekOrigin.End)
			{
				position = Length - position - 1;
			}
			long totalPosition = position;

			int chunk = 0;
			for (; chunk < chunks.Length; chunk++)
			{
				long size = chunks[chunk].Size;
				if (size <= 0)
				{
					if (forWriting)
					{
						chunk++;
						break;
					}
					size = IOUtil.GetLength(chunks[chunk].FileName);
					chunks[chunk].Size = size;
				}
				if (size > position)
				{
					break;
				}
				position -= size;
			}
			if (chunk >= chunks.Length)
			{
				if (forWriting && chunks[chunks.Length].Size <= 0)
				{
					chunk = chunks.Length - 1;
					if (chunk != currentChunkIndex)
					{
						if (currentChunkIndex < chunks.Length)
						{
							chunks[currentChunkIndex].Close();
						}
						currentChunkIndex = chunk;
						chunks[currentChunkIndex].Open();
					}
					chunks[currentChunkIndex].Stream.Seek(position, SeekOrigin.Begin);
				}
				else
				{
					if (!Eof()) Current.Close();
					currentChunkIndex = chunks.Length;
					totalPosition = Length;
				}
			}
			else
			{
				if (chunk == currentChunkIndex)
				{
					chunks[currentChunkIndex].Stream.Seek(position, SeekOrigin.Begin);
				}
				else
				{
					if (!Eof()) Current.Close();
					currentChunkIndex = chunk;
					chunks[currentChunkIndex].Open();
					chunks[currentChunkIndex].Stream.Seek(position, SeekOrigin.Begin);
				}
			}
			return totalPosition;
		}

		/// <summary>
		/// Get the length of the file.
		/// </summary>
		public override long Length
		{
			get
			{
				long total = 0;
				for (int i = 0; i < chunks.Length; i++)
				{
					long size = chunks[i].Size;
					if (size <= 0)
					{
						try
						{
							size = IOUtil.GetLength(chunks[i].FileName);
						}
						catch
						{

							// ignore, for case where at last chunk in writing... this is not clear.
						}
						if (size < 0) continue;
						chunks[i].Size = size;
					}
					total += size;
				}
				return total;
			}
		}

		/// <summary>
		/// Get or set the position in the file.
		/// </summary>
		public override long Position
		{
			get
			{
				long total = 0;
				for (int i = 0; i < currentChunkIndex; i++)
				{
					long size = chunks[i].Size;
					if (size <= 0)
					{
						size = IOUtil.GetLength(chunks[i].FileName);
						chunks[i].Size = size;
					}
					total += size;
				}
				if (!Eof())
				{
					total += chunks[currentChunkIndex].Position;
				}
				return total;
			}
			set
			{
				Seek(value);
			}
		}
		#endregion


		private class Chunk : IDisposable
		{
			private readonly string fileName;
			private long size;
			private Stream stream;
			private bool read;
			private long position;

			public Chunk(string fileName)
			{
				this.fileName = fileName;
				this.stream = null;
				this.size = -1;
				position = 0;
				read = true;
			}

			public Chunk(string fileName, long size)
			{
				this.fileName = fileName;
				this.stream = null;
				this.size = size;
				position = 0;
				read = false;
			}

			~Chunk()
			{
				Dispose();
			}


			public string FileName
			{
				get { return fileName; }
			}

			public long Size
			{
				get { return size; }
				set { size = value; }
			}

			public long Position
			{
				get { return position; }
				set { position = value; }
			}

			//public static Chunk[] Chunks(string[] fileNames)
			//{
			//    if (fileNames == null)  return null;
			//    Chunk[] res = new Chunk[fileNames.Length];
			//    for (int i = 0; i < fileNames.Length; i++)
			//    {
			//        if (fileNames[i] != null)
			//        {
			//            res[i] = new Chunk(fileNames[i]);
			//        }
			//    }
			//    return res;
			//}

			//public static Chunk[] Chunks(string[] fileNames, long[] sizes)
			//{
			//    if (fileNames == null)  return null;
			//    if (sizes == null)  return Chunks(fileNames);
			//    if (fileNames.Length != sizes.Length)  return null;
			//    Chunk[] res = new Chunk[fileNames.Length];
			//    for (int i = 0; i < fileNames.Length; i++)
			//    {
			//        if (fileNames[i] != null)
			//        {
			//            res[i] = new Chunk(fileNames[i], sizes[i]);
			//        }
			//    }
			//    return res;
			//}

			public void Open()
			{
				//Console.WriteLine("Opening chunk: " + FileName);
				if (stream != null)
				{
					Close();
				}

				if (read)
				{
					stream = ZStreamIn.Open(fileName);
				}
				else
				{
					stream = ZStreamOut.Open(fileName);
				}
				position = 0;
			}

			public void Close()
			{
				Close(false);
			}
			public void Close(bool storeSize)
			{
				if (stream != null)
				{
					if (storeSize)
					{
						size = stream.Position;
					}
					stream.Close();
					stream = null;
				}
			}

			public Stream Stream
			{
				get
				{
					// open automatically?
					return stream;
				}
			}

			#region IDisposable Members

			public void Dispose()
			{
				if (stream != null)
				{
					try
					{
						stream.Close();
						stream = null;
					}
					catch
					{
					}
				}
				GC.SuppressFinalize(this);
			}

			#endregion
		}

	}

	#endregion



	#region Fixed Stream

	/// <summary>
	/// This Stream is needed because the Console functionality in .NET 1.1 is broken.
	/// Console streams always Flush() in the Dispose(), even for stdin. However, in
	/// the Flush(), the class checks to see if the Stream CanWrite, and it throws an
	/// exception if not. This is particularly ridiculous, given that the Flush()
	/// method also does not flush.
	/// </summary>
	internal class FixedStream : Stream, IDisposable
	{
		private Stream baseStream;

		/// <summary>
		/// Construct a Stream wrapping the given Stream which does not throw exceptions
		/// when used.
		/// </summary>
		/// <param name="baseStream">The original Stream</param>
		public FixedStream(Stream baseStream)
		{
			this.baseStream = baseStream;
		}

		/// <summary>
		/// Close the stream.
		/// </summary>
		public override void Close()
		{
			try
			{
				baseStream.Close();
			}
			catch
			{
				// ignore
			}
		}

		/// <summary>
		/// Flush all data.
		/// </summary>
		public override void Flush()
		{
			//			try
			//			{
			//				baseStream.Flush();
			//			}
			//			catch
			//			{
			//				// ignore
			//			}
		}

		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <returns>the number of bytes read</returns>
		public override int Read(byte[] buffer, int offset, int count)
		{
			try
			{
				return baseStream.Read(buffer, offset, count);
			}
			catch (IOException)
			{
				// assume we are just broken?
				// and assume nothing was read? ***
				for (int c = 0; c < count; c++)
				{
					int b = baseStream.ReadByte();
					//Console.Write(" " + c + "/" + count);
					if (b < 0) return c;
					buffer[offset++] = (byte)b;
				}
				return count;
			}
		}

		private readonly byte[] byteBuffer = new byte[1];

		/// <summary>
		/// Read a single byte.
		/// </summary>
		/// <returns>the byte read, or a negative number if end of file</returns>
		public override int ReadByte()
		{
			// the base stream in the Framework commonly does a horrible job here and
			// actually allocates a new array on every call. This is 200x slower.
			return baseStream.ReadByte();
			//if (baseStream.Read(byteBuffer, 0, 1) < 1) return -1;
			//return byteBuffer[0];
		}

		/// <summary>
		/// Read data into the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to place the data in</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to read</param>
		/// <param name="callback">the callback to use</param>
		/// <param name="state">the state to use for the callback</param>
		/// <returns>the number of bytes read</returns>
		/// <exception cref="IOException">Read is positioned out of bounds.</exception>
		public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			return baseStream.BeginRead(buffer, offset, count, callback, state);
		}

		/// <summary>
		/// End an asynchronous read.
		/// </summary>
		/// <param name="asyncResult">the result</param>
		/// <returns>number of bytes read</returns>
		public override int EndRead(IAsyncResult asyncResult)
		{
			return baseStream.EndRead(asyncResult);
		}

		/// <summary>
		/// Seek to a given position.
		/// </summary>
		/// <param name="offset">the offset in bytes</param>
		/// <param name="origin">the SeekOrigin to take the offset from</param>
		/// <returns>the new position</returns>
		public override long Seek(long offset, SeekOrigin origin)
		{
			return baseStream.Seek(offset, origin);
		}

		/// <summary>
		/// Set the file length - not a good idea.
		/// </summary>
		/// <param name="value">the length to set it to</param>
		public override void SetLength(long value)
		{
			baseStream.SetLength(value);
		}

		/// <summary>
		/// Write data from the buffer.
		/// </summary>
		/// <param name="buffer">the buffer to read the data from</param>
		/// <param name="offset">the starting index in buffer</param>
		/// <param name="count">the maximum number of bytes to write</param>
		public override void Write(byte[] buffer, int offset, int count)
		{
			baseStream.Write(buffer, offset, count);
		}

		/// <summary>
		/// Write a single byte.
		/// </summary>
		/// <param name="value">the byte to write</param>
		public override void WriteByte(byte value)
		{
			// the base stream in the Framework commonly does a horrible job here and
			// actually allocates a new array on every call. This is 200x slower.
			baseStream.WriteByte(value);
			//byteBuffer[0] = value;
			//baseStream.Write(byteBuffer, 0, 1);
		}

		/// <summary>
		/// Whether the stream can read,
		/// </summary>
		public override bool CanRead
		{
			get
			{
				return baseStream.CanRead;
			}
		}

		/// <summary>
		/// Whether the stream can seek,
		/// </summary>
		public override bool CanSeek
		{
			get
			{
				return baseStream.CanSeek;
			}
		}

		/// <summary>
		/// Whether the stream can write,
		/// </summary>
		public override bool CanWrite
		{
			get
			{
				return baseStream.CanWrite;
			}
		}

		/// <summary>
		/// Get the length of the file - not supported.
		/// </summary>
		public override long Length
		{
			get
			{
				return baseStream.Length;
			}
		}

		/// <summary>
		/// Get or set the position in the file - not supported.
		/// </summary>
		public override long Position
		{
			get
			{
				// This could be supported, if maintained... ***
				return baseStream.Position;
			}
			set
			{
				baseStream.Position = value;
			}
		}

		public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			return baseStream.BeginWrite(buffer, offset, count, callback, state);
		}

#if DOTNET2
		public override bool CanTimeout
		{
			get
			{
				return baseStream.CanTimeout;
			}
		}
#endif

		public override void EndWrite(IAsyncResult asyncResult)
		{
			baseStream.EndWrite(asyncResult);
		}

#if DOTNET2
		public override int ReadTimeout
		{
			get
			{
				return baseStream.ReadTimeout;
			}
			set
			{
				baseStream.ReadTimeout = value;
			}
		}

		public override int WriteTimeout
		{
			get
			{
				return baseStream.WriteTimeout;
			}
			set
			{
				baseStream.WriteTimeout = value;
			}
		}
#endif

		#region IDisposable Members

#if DOTNET2
		protected override void Dispose(bool disposing)
		{
			// ugly!!!
			//baseStream.Dispose(disposing);
			if (disposing)
			{
				baseStream.Close();
			}
			else
			{
				Dispose();
			}
		}

		void IDisposable.Dispose()
		{
			baseStream.Dispose();
		}
#else
		void IDisposable.Dispose()
		{
			// ugly!!!
			//baseStream.Dispose(disposing);
			baseStream.Close();
		}
#endif

		#endregion
	}

	#endregion



	#region LineBufferedStream

	/// <summary>
	/// Adds a buffering layer that flushes at newlines to write operations on another stream.
	/// </summary>
	/// <remarks>
	/// This class can actually be switched between line buffering, full buffering, and no buffering,
	/// after creation.
	/// </remarks>
	public /*sealed*/ class LineBufferedStream : Stream
#if !DOTNET2
		, IDisposable
#endif
	{
		private byte[] _buffer;
		private int _bufferSize;
		private const int _DefaultBufferSize = 0x100000;
		private Stream _s;
		private int _writePos;
		private bool enableBuffer = true;
		private bool lineBuffer = true;

		private LineBufferedStream()
		{
		}

		/// <summary>Initializes a new instance of the <see cref="LineBufferedStream"></see> class with a default buffer size of 4096 bytes.</summary>
		/// <param name="stream">The current stream. </param>
		/// <exception cref="T:System.ArgumentNullException">stream is null. </exception>
		public LineBufferedStream(Stream stream)
			: this(stream, _DefaultBufferSize)
		{
		}

		/// <summary>Initializes a new instance of the <see cref="LineBufferedStream"></see> class with the specified buffer size.</summary>
		/// <param name="bufferSize">The buffer size in bytes. </param>
		/// <param name="stream">The current stream. </param>
		/// <exception cref="T:System.ArgumentNullException">stream is null. </exception>
		/// <exception cref="T:System.ArgumentOutOfRangeException">bufferSize is non-positive. </exception>
		public LineBufferedStream(Stream stream, int bufferSize)
		{
			if (stream == null)
			{
				throw new ArgumentNullException("stream");
			}
			if (bufferSize <= 0)
			{
				throw new ArgumentOutOfRangeException("bufferSize", "bufferSize must be non-negative: bufferSize = " + bufferSize);
			}
			this._s = stream;
			this._bufferSize = bufferSize;
			if (!_s.CanWrite)
			{
				throw new ObjectDisposedException("The stream is closed.");
			}
		}

		/// <summary>
		/// Close the stream and release its resources, if needed.
		/// </summary>
		~LineBufferedStream()
		{
			if (_s != null)
			{
				try
				{
					Flush();
					_s.Flush();
					_s.Close();
				}
				catch
				{
				}
				_s = null;
			}
		}

		/// <summary>
		/// Get or Set whether to enable any buffering on this stream.
		/// </summary>
		public bool EnableBuffer
		{
			get { return enableBuffer; }
			set
			{
				if (enableBuffer == value) return;
				enableBuffer = value;
				if (!enableBuffer)
				{
					Flush();
					_buffer = null;
				}
			}
		}

		/// <summary>
		/// Get or Set whether to use line buffering on this stream, or just full buffering.
		/// </summary>
		/// <remarks>
		/// Buffering will still be disabled if <see cref="EnableBuffer"/> is false.
		/// </remarks>
		public bool LineBuffer
		{
			get { return lineBuffer; }
			set { lineBuffer = value; }
		}

#if DOTNET2
		/// <summary>
		/// Release the unmanaged resources and, optionally, the managed resources.
		/// </summary>
		/// <param name="disposing">if true, also release the managed resources</param>
		protected override void Dispose(bool disposing)
		{
			try
			{
				if (disposing && (this._s != null))
				{
					try
					{
						this.Flush();
					}
					finally
					{
						this._s.Close();
					}
				}
			}
			finally
			{
				this._s = null;
				this._buffer = null;
				GC.SuppressFinalize(this);
				base.Dispose(disposing);
			}
		}
#else
		/// <summary>
		/// Release the unmanaged resources and, optionally, the managed resources.
		/// </summary>
		void IDisposable.Dispose()
		{
			try
			{
				if (this._s != null)
				{
					try
					{
						this.Flush();
					}
					finally
					{
						this._s.Close();
					}
				}
			}
			finally
			{
				this._s = null;
				this._buffer = null;
				GC.SuppressFinalize(this);
			}
		}
#endif

		/// <summary>Clears all buffers for this stream and causes any buffered data to be written to the underlying device.</summary>
		/// <exception cref="T:System.IO.IOException">The stream is not open. </exception>
		public override void Flush()
		{
			//if (this._s == null)
			//{
			//    __Error.StreamIsClosed();
			//}
			if (this._writePos > 0)
			{
				this.FlushWrite();
			}
		}

		private void FlushWrite()
		{
			this._s.Write(this._buffer, 0, this._writePos);
			this._writePos = 0;
			this._s.Flush();
		}

		/// <summary>Always throws an exception, since reads are not support.</summary>
		/// <returns>The total number of bytes read into array. This can be less than the number of bytes requested if that many bytes aren't currently available, or 0 if the end of the stream has been reached before any data can be read.</returns>
		/// <param name="offset">The byte offset in the buffer at which to begin reading bytes. </param>
		/// <param name="array">The buffer to which bytes are to be copied. </param>
		/// <param name="count">The number of bytes to be read. </param>
		/// <exception cref="T:System.NotSupportedException">The stream does not support reading - always thrown.</exception>
		public override int Read(byte[] array, int offset, int count)
		{
			throw new NotSupportedException("Reading is not supported.");
		}
		/// <summary>Always throws an exception, since reads are not support.</summary>
		/// <returns>The byte cast to an int, or -1 if reading from the end of the stream.</returns>
		/// <exception cref="T:System.NotSupportedException">The stream does not support reading - always thrown.</exception>
		public override int ReadByte()
		{
			throw new NotSupportedException("Reading is not supported.");
		}

		/// <summary>Sets the position within the current buffered stream.</summary>
		/// <returns>The new position within the current buffered stream.</returns>
		/// <param name="offset">A byte offset relative to origin. </param>
		/// <param name="origin">A value of type <see cref="T:System.IO.SeekOrigin"></see> indicating the reference point from which to obtain the new position. </param>
		/// <exception cref="T:System.NotSupportedException">The stream does not support seeking. </exception>
		/// <exception cref="T:System.IO.IOException">The stream is not open or is null. </exception>
		/// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed. </exception>
		public override long Seek(long offset, SeekOrigin origin)
		{
			if (this._s == null)
			{
				throw new ObjectDisposedException("The stream is closed.");
			}
			if (!this._s.CanSeek)
			{
				throw new NotSupportedException("The underlying stream of type " + _s.GetType().FullName + " cannot seek.");
			}
			if (offset == 0 && origin == SeekOrigin.Current) return Position;
			Flush();
			return _s.Seek(offset, origin);
		}

		/// <summary>Sets the length of the buffered stream.</summary>
		/// <param name="value">An integer indicating the desired length of the current buffered stream in bytes. </param>
		/// <exception cref="T:System.NotSupportedException">The stream does not support both writing and seeking. </exception>
		/// <exception cref="T:System.ArgumentOutOfRangeException">value is negative. </exception>
		/// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed. </exception>
		/// <exception cref="T:System.IO.IOException">The stream is not open or is null. </exception>
		public override void SetLength(long value)
		{
			if (value < 0)
			{
				throw new ArgumentOutOfRangeException("value", "value cannot be negative: value = " + value);
			}
			if (this._s == null)
			{
				throw new ObjectDisposedException("The stream is closed.");
			}
			if (!this._s.CanSeek)
			{
				throw new NotSupportedException("The underlying stream of type " + _s.GetType().FullName + " cannot seek.");
			}
			Flush();
			_s.SetLength(value);
		}

		/// <summary>Copies bytes to the buffered stream and advances the current position within the buffered stream by the number of bytes written.</summary>
		/// <param name="offset">The offset in the buffer at which to begin copying bytes to the current buffered stream. </param>
		/// <param name="array">The byte array from which to copy count bytes to the current buffered stream. </param>
		/// <param name="count">The number of bytes to be written to the current buffered stream. </param>
		/// <exception cref="T:System.ArgumentNullException">array is null. </exception>
		/// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed. </exception>
		/// <exception cref="T:System.IO.IOException">The stream is closed or null. </exception>
		/// <exception cref="T:System.ArgumentException">Length of array minus offset is less than count. </exception>
		/// <exception cref="T:System.ArgumentOutOfRangeException">offset or count is negative. </exception>
		public override void Write(byte[] array, int offset, int count)
		{
			if (this._s == null)
			{
				throw new ObjectDisposedException("The stream is closed.");
			}
			if (!EnableBuffer)
			{
				_s.Write(array, offset, count);
				return;
			}
			if (array == null)
			{
				throw new ArgumentNullException("array", "array cannot be null.");
			}
			if (offset < 0)
			{
				throw new ArgumentOutOfRangeException("offset", "offset cannot be negative: offset = " + offset);
			}
			if (count < 0)
			{
				throw new ArgumentOutOfRangeException("count", "count cannot be negative: count = " + count);
			}
			if ((array.Length - offset) < count)
			{
				throw new ArgumentException("Invalid count or offset: count = " + count + ", offset = " + offset + ", array.Length = " + array.Length);
			}

			if (count == 0) return;

			bool hasNewline = false;
			if (LineBuffer)
			{
				for (int i = offset; i < offset + count; i++)
				{
					if (array[i] == (byte)'\n')
					{
						hasNewline = true;
						break;
					}
				}
			}
			//bool hasNewline = Array.IndexOf<byte>(array, (byte)'\n', offset, count) >= 0;
			//bool hasNewline = false;
			//unsafe
			//{
			//    fixed (byte* a = array)
			//    {
			//        byte* b = a + offset;
			//        byte* e = b + count;
			//        while (b != e)
			//        {
			//            if (*b == 10)
			//            {
			//                hasNewline = true;
			//                break;
			//            }
			//            b++;
			//        }
			//    }
			//}

			if (_writePos > 0)
			{
				int num = _bufferSize - _writePos;
				if (num > 0)
				{
					if (num > count)
					{
						num = count;
					}
					Buffer.BlockCopy(array, offset, _buffer, _writePos, num);
					this._writePos += num;
					if (count == num)
					{
						if (hasNewline) FlushWrite();
						return;
					}
					offset += num;
					count -= num;
				}
				_s.Write(_buffer, 0, _writePos);
				_writePos = 0;
			}
			// only write in multiples of bufferSize...
			while (count >= _bufferSize)
			{
				_s.Write(array, offset, _bufferSize);
				offset += _bufferSize;
				count -= _bufferSize;
			}
			if (count != 0)
			{
				if (hasNewline)
				{
					_s.Write(array, offset, count);
				}
				else
				{
					if (_buffer == null)
					{
						_buffer = new byte[_bufferSize];
					}
					Buffer.BlockCopy(array, offset, _buffer, 0, count);
					_writePos = count;
				}
			}
			if (hasNewline) Flush();
		}

		/// <summary>Writes a byte to the current position in the buffered stream.</summary>
		/// <param name="value">A byte to write to the stream. </param>
		/// <exception cref="T:System.NotSupportedException">The stream does not support writing. </exception>
		/// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed. </exception>
		/// <exception cref="T:System.ArgumentNullException">value is null. </exception>
		public override void WriteByte(byte value)
		{
			if (this._s == null)
			{
				throw new ObjectDisposedException("The stream is closed.");
			}
			if (!EnableBuffer)
			{
				_s.WriteByte(value);
				return;
			}
			if (_writePos == 0)
			{
				if (LineBuffer)
				{
					if (value == (byte)'\n')
					{
						_s.WriteByte(value);
						return;
					}
				}
				if (_buffer == null)
				{
					_buffer = new byte[_bufferSize];
				}
			}
			if (_writePos == _bufferSize)
			{
				FlushWrite();
			}

			_buffer[_writePos] = value;
			_writePos++;

			if (LineBuffer)
			{
				if (value == (byte)'\n')
				{
					FlushWrite();
				}
			}
		}

		/// <summary>Gets a value indicating whether the current stream supports reading - always false.</summary>
		/// <returns>true if the stream supports reading; false if the stream is closed or was opened with write-only access.</returns>
		public override bool CanRead
		{
			get
			{
				return false;
			}
		}

		/// <summary>Gets a value indicating whether the current stream supports seeking.</summary>
		/// <returns>true if the stream supports seeking; false if the stream is closed or if the stream was constructed from an operating-system handle such as a pipe or output to the console.</returns>
		/// <filterpriority>2</filterpriority>
		public override bool CanSeek
		{
			get
			{
				if (this._s != null)
				{
					return this._s.CanSeek;
				}
				return false;
			}
		}

		/// <summary>Gets a value indicating whether the current stream supports writing - always true, when the stream is not closed.</summary>
		/// <returns>true if the stream supports writing; false if the stream is closed or was opened with read-only access.</returns>
		/// <filterpriority>2</filterpriority>
		public override bool CanWrite
		{
			get
			{
				if (this._s != null)
				{
					return true;
				}
				return false;
			}
		}

		/// <summary>Gets the stream length in bytes.</summary>
		/// <remarks>
		/// This also flushes any pending written data. It will fail if the stream is closed.
		/// </remarks>
		/// <returns>The stream length in bytes.</returns>
		/// <exception cref="T:System.NotSupportedException">The stream does not support seeking. </exception>
		/// <exception cref="T:System.IO.IOException">The underlying stream is null or closed. </exception>
		/// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed. </exception>
		/// <filterpriority>2</filterpriority>
		public override long Length
		{
			get
			{
				// should be fixed to not fail after Close... ***
				if (this._s == null)
				{
					throw new ObjectDisposedException("The stream is closed.");
				}
				if (_writePos > 0)
				{
					FlushWrite();
				}
				return _s.Length;
			}
		}

		/// <summary>Gets the position within the current stream.</summary>
		/// <returns>The position within the current stream.</returns>
		/// <exception cref="T:System.NotSupportedException">The stream does not support seeking. </exception>
		/// <exception cref="T:System.IO.IOException">An I/O error occurs, such as the stream being closed. </exception>
		/// <exception cref="T:System.ArgumentOutOfRangeException">The value passed to <see cref="M:System.IO.BufferedStream.Seek(System.Int64,System.IO.SeekOrigin)"></see> is negative. </exception>
		/// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed. </exception>
		/// <filterpriority>2</filterpriority>
		public override long Position
		{
			get
			{
				// should be fixed to work without Seek support... ***
				if (_s == null)
				{
					throw new ObjectDisposedException("The stream is closed.");
				}
				if (!_s.CanSeek)
				{
					throw new NotSupportedException("The underlying stream of type " + _s.GetType().FullName + " cannot seek.");
				}
				return (_s.Position + _writePos);
			}
			set
			{
				if (value < 0)
				{
					throw new ArgumentOutOfRangeException("value", "value cannot be negative: value = " + value);
				}
				if (_s == null)
				{
					throw new ObjectDisposedException("The stream is closed.");
				}
				if (!_s.CanSeek)
				{
					throw new NotSupportedException("The underlying stream of type " + _s.GetType().FullName + " cannot seek.");
				}
				if (_writePos > 0)
				{
					FlushWrite();
				}
				_s.Seek(value, SeekOrigin.Begin);
			}
		}
	}

	#endregion

}

