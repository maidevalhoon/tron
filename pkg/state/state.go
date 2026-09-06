package state

import (
	"sync"
)

// FileRecord holds the state of a synchronized file.
type FileRecord struct {
	Path    string   `json:"path"`
	Version uint64   `json:"version"`
	Chunks  []uint64 `json:"chunks"`
}

// Store holds the file state in memory.
type Store struct {
	sync.RWMutex
	records map[string]FileRecord
}

func NewStore() *Store {
	return &Store{
		records: make(map[string]FileRecord),
	}
}

func (s *Store) GetHashes(filepath string) []uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.records[filepath].Chunks
}

func (s *Store) GetRecord(filepath string) (FileRecord, bool) {
	s.RLock()
	defer s.RUnlock()
	r, ok := s.records[filepath]
	return r, ok
}

func (s *Store) SetRecord(r FileRecord) {
	s.Lock()
	defer s.Unlock()
	s.records[r.Path] = r
}

func (s *Store) ShouldAccept(filepath string, version uint64) bool {
	s.RLock()
	defer s.RUnlock()
	cur, ok := s.records[filepath]
	if !ok {
		return true
	}
	return version > cur.Version
}

func (s *Store) SetHashes(filepath string, hashes []uint64) {
	s.Lock()
	defer s.Unlock()
	rec := s.records[filepath]
	rec.Path = filepath
	rec.Chunks = hashes
	s.records[filepath] = rec
}

// Bump updates hashes, increments version, and returns the newly introduced chunk count and record.
func (s *Store) Bump(filepath string, newHashes []uint64) (FileRecord, int) {
	s.Lock()
	defer s.Unlock()

	cur := s.records[filepath]
	oldSet := make(map[uint64]bool, len(cur.Chunks))
	for _, h := range cur.Chunks {
		oldSet[h] = true
	}

	diffCount := 0
	for _, h := range newHashes {
		if !oldSet[h] {
			diffCount++
		}
	}

	cur.Path = filepath
	cur.Version++
	cur.Chunks = newHashes
	s.records[filepath] = cur
	return cur, diffCount
}

// UpdateHashes updates the stored hashes and returns the number of newly introduced chunk hashes.
func (s *Store) UpdateHashes(filepath string, newHashes []uint64) int {
	_, diff := s.Bump(filepath, newHashes)
	return diff
}
