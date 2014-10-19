using System;
using System.Collections;
using System.IO;
using System.Reflection;

namespace Microsoft.TMSN.Data {

	internal class ReduceFilter : IRecordFilter {
		private bool _reductionOccured = false;
		private DataRecord _accumulatorRecord = null;
		private DataRecord _prevInputRecord = null;
		private DataRecord _currentInputRecord = null;

		public bool KeyOrderIsPreserved {
			get {
				return true;
			}
		}

		// just call the Process routine with a null
		public void Finish(RecordAccepter accepter) {
			ProcessRecord(null, accepter);
		}

		public void ProcessRecord(DataRecord record, RecordAccepter accepter) {
			DataRecord returnRecord = null;

			// eat the first record
			if (_prevInputRecord == null) {
				_prevInputRecord = record;
				return; // give nothing to the accepter
			}

			_currentInputRecord = record;

			// we get called a final time with null so we can output any state we have
			int diff;
			if (record != null) {
                //diff = _prevInputRecord.Key.CompareTo(_currentInputRecord.Key);
				diff = TMSNStoreUtils.Utf8BytesCompare(_prevInputRecord.KeyBytes, _currentInputRecord.KeyBytes);
			}

			else diff = -1; // force a non-match

			// the keys match
			if (diff == 0) {
				// if reduction hasn't occured yet then this is the first time thru here.
				// use the prevInputRecord as an accumulator
				if (!_reductionOccured) {
					_accumulatorRecord = _prevInputRecord;
					_reductionOccured = true;
				}

				((IReducableRecord)_accumulatorRecord).ReduceData((IReducableRecord)_currentInputRecord);
				returnRecord = null; // no record to return yet
			}

				// the keys don't match
			else {
				// if no reduction occured, the prev record needs to get out
				if (!_reductionOccured) {
					returnRecord = (DataRecord)_prevInputRecord;
				}

					// reduction occured in the _accumulatorRecord output it
				else {
					// set up for next time around
					_reductionOccured = false; // 
					returnRecord = (DataRecord)_accumulatorRecord;
				}
			}

			// advance 
			_prevInputRecord = _currentInputRecord;
			
			if (returnRecord != null) accepter.AddRecord(returnRecord);
		}
	}
}