package recon

import (
	"github.com/MichaelMure/go-iblite"
)

// Config configures the IBLT parameters.
const (
	BucketCount = 20
	HashCount   = 3
)

// BuildTable creates a new KTable from a slice of hashes.
func BuildTable(hashes []uint64) *iblt.KTable {
	table := iblt.NewKTable(BucketCount, HashCount)
	for _, h := range hashes {
		table.Insert(h)
	}
	return table
}

// DiffResult holds the missing and extra hashes after reconciliation.
type DiffResult struct {
	Missing []uint64 // Hashes missing locally (we need to fetch)
	Extra   []uint64 // Hashes we have but remote doesn't
}

// Compare computes the difference between local table and remote table bytes.
// remoteBytes is the serialized KTable from the remote node.
// Returns hashes that the remote has but we don't (Missing) and hashes we have that remote doesn't (Extra).
func Compare(localHashes []uint64, remoteBytes []byte) (*DiffResult, error) {
	localTable := BuildTable(localHashes)
	remoteTable, err := iblt.KTableFromBytes(remoteBytes)
	if err != nil {
		return nil, err
	}

	// We subtract remote from local.
	// Positive count -> item in local, not in remote (Extra)
	// Negative count -> item in remote, not in local (Missing)
	localTable.Subtract(remoteTable)

	res := &DiffResult{}
	for key, count := range localTable.Peel() {
		if count > 0 {
			res.Extra = append(res.Extra, key)
		} else if count < 0 {
			res.Missing = append(res.Missing, key)
		}
	}
	return res, nil
}
