using System;
using System.IO;
using System.Collections;
using Microsoft.TMSN.IO;

namespace Microsoft.TMSN {

	/// <summary>
	/// RankingTSVFile which joins the columns across two physical files.
	/// </summary>
	public class JoinRankingTSVFile : RankingTSVFile {
		/// <summary>
		/// Line reader which joins the lines across two physical files
		/// </summary>
		private class JoinLineReader : LineReader {
			private string baseFileName = null;
			private string secondaryFileName = null;
			private StreamReader baseReader = null;
			private StreamReader secondaryReader = null;
			private string headers = null;

			/// <summary>
			/// Constructor for join Line Reader
			/// </summary>
			/// <param name="baseFileName">first file to join whose columns end up leftmost</param>
			/// <param name="secondaryFileName">second file to join whose columns end up rightmost</param>
			public JoinLineReader(string baseFileName, string secondaryFileName) {
				this.baseFileName = baseFileName;
				this.secondaryFileName = secondaryFileName;
			}

			/// <summary>
			/// opens the rankingTSVFile
			/// </summary>
			public override void Open() {
				baseReader = new StreamReader(baseFileName);
				secondaryReader = new StreamReader(secondaryFileName);
				headers = baseReader.ReadLine() + '\t' + secondaryReader.ReadLine();
			}

			/// <summary>
			/// Closes the rankingTSVFile
			/// </summary>
			public override void Close() {
				if (IsOpened) {
					baseReader.Close();
					secondaryReader.Close();
				}

				baseReader = null;
				secondaryReader = null;
				headers = null;
			}

			/// <summary>
			/// Returns the headers of the joined ranking TSV File object
			/// </summary>
			public override string Headers {
				get {
					if (headers == null) Open();
					return headers;
				}
			}

			/// <summary>
			/// The readline method for reading a joined line
			/// </summary>
			/// <returns>returns the line joined across the base and secondary files</returns>
			public override string ReadLine() {
				string baseLine = baseReader.ReadLine();
				string secondaryLine = secondaryReader.ReadLine();

				if (baseLine == null || secondaryLine == null) {
					if (baseLine != secondaryLine) {
						throw new Exception("JoinLineReader exception: inequal number of lines in files");
					}

					// otherwise they are both null so return null
					return null;
				}

				return baseLine + '\t' + secondaryLine;
			}

			/// <summary>
			/// returns true if the ranking TSV file is open
			/// </summary>
			public override bool IsOpened {
				get {
					return (baseReader != null);
				}
			}

			/// <summary>
			/// returns a clone of the ranking TSV file object
			/// </summary>
			/// <returns></returns>
			public override object Clone() {
				JoinLineReader jlr = new JoinLineReader(baseFileName, secondaryFileName);
				return jlr;
			}

		}

		/// <summary>
		/// JoinRankingTSVFile constructor
		/// </summary>
		/// <param name="baseFileName">base features to be joined.  Includes mandatory features</param>
		/// <param name="secondaryFileName">additional features to be joined.</param>
		public JoinRankingTSVFile(string baseFileName, string secondaryFileName) :
			base(new JoinLineReader(baseFileName, secondaryFileName)) {
		}
	
		/// <summary>
		/// JoinRankingTSVFile constructor 
		/// </summary>
		/// <param name="baseFileName">base features to be joined.  Includes mandatory features.</param>
		/// <param name="secondaryFileName">addtional features to be joined.</param>
		/// <param name="keepMeta">boolean which when true causes metadata to be retained</param>
		public JoinRankingTSVFile(string baseFileName, string secondaryFileName, bool keepMeta) :
			base(new JoinLineReader(baseFileName, secondaryFileName), keepMeta) {
		}

	}

}
