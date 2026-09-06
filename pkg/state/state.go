package state

import (
	"sync"
)

// Store holds the file state in memory.
type Store struct {
	sync.RWMutex
	// map[string][]uint64
	files map[string][]uint64
}

func NewStore() *Store {
	return &Store{
		files: make(map[string][]uint64),
	}
}

func (s *Store) GetHashes(filepath string) []uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.files[filepath]
}

func (s *Store) SetHashes(filepath string, hashes []uint64) {
	s.Lock()
	defer s.Unlock()
	s.files[filepath] = hashes
}

// UpdateHashes updates the stored hashes and returns the number of newly introduced chunk hashes.
func (s *Store) UpdateHashes(filepath string, newHashes []uint64) int {
	s.Lock()
	defer s.Unlock()

	oldHashes := s.files[filepath]
	oldSet := make(map[uint64]bool, len(oldHashes))
	for _, h := range oldHashes {
		oldSet[h] = true
	}

	diffCount := 0
	for _, h := range newHashes {
		if !oldSet[h] {
			diffCount++
		}
	}

	s.files[filepath] = newHashes
	return diffCount
}
