using System;
using System.Text;
using System.Reflection;
using System.Collections;
using System.IO;

namespace Microsoft.TMSN.Data {
	/// <summary>
	/// Summary description for PairFilter.
	/// </summary>
	public enum PairOperation {
		/// <summary>
		/// Causes input sources to merged in ascending order.
		/// </summary>
		MergeAscend,
		/// <summary>
		/// Causes input sources to merged in descending order.
		/// </summary>
		MergeDescend,
		/// <summary>
		/// Records in the left source which are in the right source are passed through the filter.
		/// </summary>
		FilterLeftInRight,
		/// <summary>
		/// Records in the left source which are not in the right source are passed through the filter.
		/// </summary>
		FilterLeftNotInRight,
		/// <summary>
		/// The right source is concatenated to the left source.
		/// </summary>
		CatLeftThenRight,
	}

	internal abstract class PairFilter : InternalRecordSource {
		public abstract bool KeyOrderIsPreserved { get;}
	}

	internal class CatLeftThenRight : PairFilter {
		private InternalRecordSource _source = null;
		private int _currentSourceNo = 0;
		private bool _notDone = true;

		public override bool KeyOrderIsPreserved {
			get {
				return false;
			}
		}

		public override bool MoveNext() {
			if (_source == null) {
				_source = (InternalRecordSource)_inputList[_currentSourceNo++];
			}

			_notDone = _source.MoveNext();

			if (!_notDone && _currentSourceNo < 2) {
				_source = (InternalRecordSource)_inputList[_currentSourceNo++];
				_notDone = _source.MoveNext();
			}

			CurrentRecord = _source.CurrentRecord;
			return _notDone;
		}

		public override void Close() {
		}
	}

	internal class FilterLeftByRightKey : PairFilter {
		private InternalRecordSource _leftSource = null;
		private InternalRecordSource _rightSource = null;
		private DataRecord _currentLeftRecord = null;
		private DataRecord _currentRightRecord = null;

		private bool _leftNotDone = true;
		private bool _rightNotDone = true;

		private bool _passThruOnMatch = true;

		public FilterLeftByRightKey(bool passthruOnMatch) {
			_passThruOnMatch = passthruOnMatch;
		}

		public override bool KeyOrderIsPreserved {
			get {
				return true;
			}
		}

		public override bool MoveNext() {
			if (_leftSource == null) {
				_leftSource = (InternalRecordSource)_inputList[0];
				_rightSource = (InternalRecordSource)_inputList[1];
			}

			while (true) {
				_leftNotDone = _leftSource.MoveNext();
				if (!_leftNotDone) return false;
				_currentLeftRecord = _leftSource.CurrentRecord;

				// advance the right side
				int diff = -1;
				bool firstTime = true;
				while (diff < 0) {

					// the first time we test we check against the currentRightSource since
					// we allow dups on the left side.
					if (!firstTime || _currentRightRecord == null) {
						if (_rightNotDone) _rightNotDone = _rightSource.MoveNext();

						// passThruOnMatch == left & right
						// !passThruOnMatch == left &! right

						// if left & right then when right is done we're done.
						// if left &! right when right is done keep going so we
						// can emit all the lefts that come after the last right.
						if (!_rightNotDone) {
							if (_passThruOnMatch) return false;
							else {
								CurrentRecord = _currentLeftRecord;
								return true;
							}
						}

						_currentRightRecord = _rightSource.CurrentRecord;
					}


					diff = TMSNStoreUtils.Utf8BytesCompare(_currentLeftRecord.KeyBytes, _currentRightRecord.KeyBytes);
					firstTime = false;
				}

				// if there's a match
				if (diff == 0) {
					if (_passThruOnMatch) {
						CurrentRecord = _currentLeftRecord;
						return true;
					}
				}

				else if (!_passThruOnMatch) {
					CurrentRecord = _currentLeftRecord;
					return true;
				}
			}
		}

		public override void Close() {
		}
	}

	internal class SortedMerge : PairFilter {
		private InternalRecordSource _leftSource = null;
		private InternalRecordSource _rightSource = null;
		private InternalRecordSource _currentOutputSource = null;

		private bool _leftNotDone = true;
		private bool _rightNotDone = true;

		private bool _advanceLeft = true;
		private bool _advanceRight = true;

		private int _ascendingFactor = 1;

		public override bool KeyOrderIsPreserved {
			get {
				return true;
			}
		}

		public SortedMerge(bool ascending) {
			if (!ascending) _ascendingFactor = -1;
		}

		public override bool MoveNext() {
			if (_leftSource == null) {
				_leftSource = (InternalRecordSource)_inputList[0];
				_rightSource = (InternalRecordSource)_inputList[1];
			}

			if (_advanceLeft) {
				_leftNotDone = _leftSource.MoveNext();
				_advanceLeft = false;
			}

			if (_advanceRight) {
				_rightNotDone = _rightSource.MoveNext();
				_advanceRight = false;
			}

			int diff = 0;

			// if left is done
			if (!_leftNotDone) {
				if (!_rightNotDone) return false; // if right is done too
				diff = -1; // take right since right not done
			}

			// else left is NOT done, if right is done...
			else if (!_rightNotDone) diff = 1; // take left

			// else both NOT done compare
			else diff = _ascendingFactor * TMSNStoreUtils.Utf8BytesCompare(_leftSource.CurrentRecord.KeyBytes, _rightSource.CurrentRecord.KeyBytes);

			if (diff > 0) { // take left
				_currentOutputSource = _leftSource;
				_advanceLeft = true;
			}

			else { // take right
				_currentOutputSource = _rightSource;
				_advanceRight = true;
			}

			CurrentRecord = _currentOutputSource.CurrentRecord;
			return true;
		}

		public override void Close() {
		}

		//public override void PrintProcessTree(int level, string appendToCurrent) {
		//base.PrintProcessTree(level, "[ascending:" + (_ascendingFactor > 0) + "]");
		//}
	}
}
