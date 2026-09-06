package recon

import (
	"encoding/binary"
	"fmt"

	"github.com/MichaelMure/go-iblite"
)

// Config configures the default IBLT parameters.
const (
	DefaultBucketCount = 30
	HashCount          = 3
)

// BuildTable creates a new KTable from a slice of hashes with default bucket count.
func BuildTable(hashes []uint64) *iblt.KTable {
	return BuildTableWithCapacity(hashes, 0)
}

// BuildTableWithCapacity creates a new KTable sized appropriately for expected difference.
// diffCount is the estimated number of changed items.
func BuildTableWithCapacity(hashes []uint64, diffCount int) *iblt.KTable {
	buckets := DefaultBucketCount
	if diffCount > 0 {
		// Each changed item creates 1 extra in local and 1 extra in remote (2 * diffCount items in subtracted IBLT)
		// 4.0x safety factor ensures near-100% peeling success
		buckets = int(float64(2*diffCount) * 4.0)
		if buckets < DefaultBucketCount {
			buckets = DefaultBucketCount
		}
	}
	return BuildTableWithBuckets(hashes, buckets)
}

// BuildTableWithBuckets creates a new KTable with an explicit bucket count.
func BuildTableWithBuckets(hashes []uint64, buckets int) *iblt.KTable {
	table := iblt.NewKTable(buckets, HashCount)
	for _, h := range hashes {
		table.Insert(h)
	}
	return table
}

// DiffResult holds the missing and extra hashes after reconciliation.
type DiffResult struct {
	Missing []uint64 // Hashes missing locally (we need to fetch)
	Extra   []uint64 // Hashes we have but remote doesn't
	Success bool     // True if all differences were fully recovered
}

// Compare computes the difference between local table and remote table bytes.
// remoteBytes is the serialized KTable from the remote node.
// Returns hashes that the remote has but we don't (Missing) and hashes we have that remote doesn't (Extra).
func Compare(localHashes []uint64, remoteBytes []byte) (*DiffResult, error) {
	remoteTable, err := iblt.KTableFromBytes(remoteBytes)
	if err != nil {
		return nil, err
	}

	// Extract bucketCount and hashCount from serialized header to build a compatible local table
	if len(remoteBytes) < 4 {
		return nil, fmt.Errorf("invalid table bytes: header too short")
	}
	hashCount := int(binary.BigEndian.Uint16(remoteBytes[0:2]))
	bucketCount := int(binary.BigEndian.Uint16(remoteBytes[2:4]))

	localTable := iblt.NewKTable(bucketCount, hashCount)
	for _, h := range localHashes {
		localTable.Insert(h)
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
	res.Success = localTable.Empty()
	return res, nil
}
