package chunker

import (
	"bytes"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"

	"github.com/askeladdk/fastcdc"
)

const CacheDir = ".sync_cache"

// ChunkFile chunks the given file and saves the chunks into the cache directory.
// It returns an ordered slice of uint64 chunk hashes.
func ChunkFile(filePath string) ([]uint64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if err := os.MkdirAll(CacheDir, 0755); err != nil {
		return nil, err
	}

	cw := &chunkWriter{
		hashes: []uint64{},
	}

	chunker := fastcdc.DefaultChunker()
	// Using Copy will write each chunk separately to cw
	_, err = chunker.Copy(cw, file)
	if err != nil {
		return nil, err
	}

	return cw.hashes, nil
}

type chunkWriter struct {
	hashes []uint64
}

func (cw *chunkWriter) Write(p []byte) (n int, err error) {
	// Hash the chunk
	h := fnv.New64a()
	h.Write(p)
	hash := h.Sum64()

	cw.hashes = append(cw.hashes, hash)

	// Save chunk to disk only if not already cached
	chunkPath := filepath.Join(CacheDir, strconv.FormatUint(hash, 10))
	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		// Write to temp file then rename for atomic write
		tmpPath := chunkPath + ".tmp"
		if err := os.WriteFile(tmpPath, p, 0644); err == nil {
			_ = os.Rename(tmpPath, chunkPath)
		}
	}

	return len(p), nil
}

// SaveChunk saves a chunk data to cache with given hash.
func SaveChunk(hash uint64, data []byte) error {
	if err := os.MkdirAll(CacheDir, 0755); err != nil {
		return err
	}
	chunkPath := filepath.Join(CacheDir, strconv.FormatUint(hash, 10))
	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		tmpPath := chunkPath + ".tmp"
		if err := os.WriteFile(tmpPath, data, 0644); err == nil {
			return os.Rename(tmpPath, chunkPath)
		}
	}
	return nil
}

// GetChunk returns the contents of a chunk given its hash.
func GetChunk(hash uint64) ([]byte, error) {
	chunkPath := filepath.Join(CacheDir, strconv.FormatUint(hash, 10))
	return os.ReadFile(chunkPath)
}

// ReassembleFile reconstructs a file from a list of chunk hashes.
func ReassembleFile(hashes []uint64, destPath string) error {
	var buf bytes.Buffer
	for _, hash := range hashes {
		chunkData, err := GetChunk(hash)
		if err != nil {
			return err
		}
		buf.Write(chunkData)
	}
	return os.WriteFile(destPath, buf.Bytes(), 0644)
}
