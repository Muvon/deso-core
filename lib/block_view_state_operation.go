package lib

import (
	"bytes"

	"github.com/golang/glog"
	"github.com/pkg/errors"
)

// -- Types --
type StateOperation struct {
	// This is ID of transaction that state belongs to
	TxID *BlockHash

	// This flag is used only for flushing to disk in normal way
	isDeleted bool

	// Changes that affected any existed profiles to be updated
	Profiles []*ProfileEntry

	Follows []*FollowEntry

	// All virtually created utxos are go here
	Utxos []*UtxoEntry

	// All posts to be updated
	Posts []*PostEntry

	Likes []*LikeEntry

	// All balance entries to be updated
	Balances []*BalanceEntry

	Coins []*CoinEntry

	// All DAO Coin limit orders to update/create
	LimitOrders []*DAOCoinLimitOrderEntry

	// All nfts entries that should be updated
	NFTs []*NFTEntry

	// NFT bids
	Bids []*NFTBidEntry

	// It's all about diamonds
	Diamonds []*DiamondEntry

	Messages []*MessageEntry

	Groups []*MessagingGroupEntry

	// This is for legacy transaction type with updated Bitcoin USD Rate
	BitcoinUSDRate uint64
}

func (stateOp *StateOperation) RawEncodeWithoutMetadata(blockHeight uint64, skipMetadata ...bool) []byte {
	var data []byte

	data = append(data, EncodeToBytes(blockHeight, stateOp.TxID, skipMetadata...)...)

	if stateOp.Profiles == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.Profiles)))...)
		for _, v := range stateOp.Profiles {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.Follows == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.Follows)))...)
		for _, v := range stateOp.Follows {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.Utxos == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.Utxos)))...)
		for _, v := range stateOp.Utxos {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.Posts == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.Posts)))...)
		for _, v := range stateOp.Posts {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.Likes == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.Likes)))...)
		for _, v := range stateOp.Likes {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.Balances == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.Balances)))...)
		for _, v := range stateOp.Balances {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.Coins == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.Coins)))...)
		for _, v := range stateOp.Coins {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.LimitOrders == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.LimitOrders)))...)
		for _, v := range stateOp.LimitOrders {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.NFTs == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.NFTs)))...)
		for _, v := range stateOp.NFTs {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.Bids == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.Bids)))...)
		for _, v := range stateOp.Bids {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.Diamonds == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.Diamonds)))...)
		for _, v := range stateOp.Diamonds {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.Messages == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.Messages)))...)
		for _, v := range stateOp.Messages {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	if stateOp.Groups == nil {
		data = append(data, UintToBuf(0)...)
	} else {
		data = append(data, UintToBuf(uint64(len(stateOp.Groups)))...)
		for _, v := range stateOp.Groups {
			data = append(data, EncodeToBytes(blockHeight, v, skipMetadata...)...)
		}
	}

	data = append(data, UintToBuf(stateOp.BitcoinUSDRate)...)

	return data
}

func (stateOp *StateOperation) RawDecodeWithoutMetadata(blockHeight uint64, rr *bytes.Reader) error {
	var err error

	entry := &BlockHash{}
	if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
		stateOp.TxID = entry
	} else if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: Problem reading txID")
	}

	var cnt uint64

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of profiles")
	}
	for cnt > 0 {
		entry := &ProfileEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.Profiles = append(stateOp.Profiles, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading profiles")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of follows")
	}
	for cnt > 0 {
		entry := &FollowEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.Follows = append(stateOp.Follows, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading follows")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of utxos")
	}
	for cnt > 0 {
		entry := &UtxoEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.Utxos = append(stateOp.Utxos, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading utxos")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of posts")
	}
	for cnt > 0 {
		entry := &PostEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.Posts = append(stateOp.Posts, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading posts")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of likes")
	}
	for cnt > 0 {
		entry := &LikeEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.Likes = append(stateOp.Likes, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading likes")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of balance entries")
	}
	for cnt > 0 {
		entry := &BalanceEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.Balances = append(stateOp.Balances, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading balance entries")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of coin entries")
	}
	for cnt > 0 {
		entry := &CoinEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.Coins = append(stateOp.Coins, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading coin entries")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of limit orders")
	}
	for cnt > 0 {
		entry := &DAOCoinLimitOrderEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.LimitOrders = append(stateOp.LimitOrders, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading limit orders")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of nfts")
	}
	for cnt > 0 {
		entry := &NFTEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.NFTs = append(stateOp.NFTs, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading nfts")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of nft bids")
	}
	for cnt > 0 {
		entry := &NFTBidEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.Bids = append(stateOp.Bids, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading nft bids")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of diamonds")
	}
	for cnt > 0 {
		entry := &DiamondEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.Diamonds = append(stateOp.Diamonds, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading diamonds")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of messages")
	}
	for cnt > 0 {
		entry := &MessageEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.Messages = append(stateOp.Messages, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading messages")
		}
		cnt--
	}

	cnt, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding count of groups")
	}
	for cnt > 0 {
		entry := &MessagingGroupEntry{}
		if exist, err := DecodeFromBytes(entry, rr); exist && err == nil {
			stateOp.Groups = append(stateOp.Groups, entry)
		} else if err != nil {
			return errors.Wrapf(err, "StateOperation.Decode: Problem reading groups")
		}
		cnt--
	}

	stateOp.BitcoinUSDRate, err = ReadUvarint(rr)
	if err != nil {
		return errors.Wrapf(err, "StateOperation.Decode: problem decoding Bitcoin USD rate")
	}

	return nil
}

func (message *StateOperation) GetVersionByte(blockHeight uint64) byte {
	return 0
}

func (message *StateOperation) GetEncoderType() EncoderType {
	return EncoderTypeStateOperation
}

// -- Functions
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
