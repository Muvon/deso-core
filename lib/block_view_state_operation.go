package lib

import "github.com/golang/glog"

func (bav *UtxoView) GetStateOperation(txID *BlockHash) *StateOperation {
	// Put this check in place, since sometimes people accidentally
	// pass a pointer that shouldn't be copied.
	txid := &BlockHash{}
	if txID != nil {
		*txid = *txID
	}
	// If an entry exists in the in-memory map, return the value of that mapping.
	mapValue, existsMapValue := bav.StateOperationEntry[*txid]
	if existsMapValue {
		return mapValue
	}

	stateOp := DBGetTxIdToStateOperationMapping(bav.Handle, bav.Snapshot, txid)
	if stateOp != nil {
		bav.SetStateOperationMappings(stateOp)
	}
	return stateOp
}

func (bav *UtxoView) SetStateOperationMappings(stateOp *StateOperation) {
	// This function shouldn't be called with nil.
	if stateOp == nil {
		glog.Errorf("SetStateOperationMapping: Called with nil stateOp; " +
			"this should never happen.")
		return
	}

	// Add a mapping for the profile.
	bav.StateOperationEntry[*stateOp.TxID] = stateOp
}

func (bav *UtxoView) DeleteStateOperationMappings(stateOp *StateOperation) {
	// Create a tombstone entry.
	tombstoneStateOp := *stateOp
	tombstoneStateOp.isDeleted = true

	// Set the mappings to point to the tombstone entry.
	bav.SetStateOperationMappings(&tombstoneStateOp)
}
