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
