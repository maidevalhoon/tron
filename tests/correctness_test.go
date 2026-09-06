package tests

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hackathon/sync-engine/pkg/chunker"
	"github.com/hackathon/sync-engine/pkg/recon"
	"github.com/hackathon/sync-engine/pkg/state"
)

// hashFile computes sha256 checksum of a file.
func hashFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file %s: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// makeData creates deterministic slice of length n.
func makeData(n int) []byte {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte((i * 37) ^ 0x5A)
	}
	return buf
}

// syncDirect simulates complete one-way sync from Node A to Node B.
func syncDirect(t *testing.T, fileA, fileB string, storeA, storeB *state.Store) (bool, string, string) {
	t.Helper()

	// 1. Node A chunks fileA
	hashesA, err := chunker.ChunkFile(fileA)
	if err != nil {
		t.Fatalf("chunk error on A: %v", err)
	}
	recA, diffCount := storeA.Bump(fileA, hashesA)

	// Build IBLT
	tableA := recon.BuildTableWithCapacity(hashesA, diffCount)
	ibltBytes := tableA.ToBytes()

	// 2. Node A stands up HTTP server
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hashStr := strings.TrimPrefix(r.URL.Path, "/chunk/")
		hash, err := strconv.ParseUint(hashStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid hash", http.StatusBadRequest)
			return
		}
		data, err := chunker.GetChunk(hash)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Write(data)
	}))
	defer serverA.Close()

	// 3. Node B receives gossip
	if !storeB.ShouldAccept(fileA, recA.Version) {
		t.Logf("Node B rejected stale version %d", recA.Version)
		return false, hashFile(t, fileA), hashFile(t, fileB)
	}

	hashesB := storeB.GetHashes(fileA)
	diffRes, err := recon.Compare(hashesB, ibltBytes)
	if err != nil {
		t.Fatalf("recon Compare error: %v", err)
	}

	missing := diffRes.Missing
	if !diffRes.Success {
		// Fallback to manifest comparison
		setB := make(map[uint64]bool, len(hashesB))
		for _, h := range hashesB {
			setB[h] = true
		}
		missing = nil
		for _, h := range hashesA {
			if !setB[h] {
				missing = append(missing, h)
			}
		}
	}

	// 4. Fetch missing chunks
	client := serverA.Client()
	for _, h := range missing {
		resp, err := client.Get(fmt.Sprintf("%s/chunk/%d", serverA.URL, h))
		if err != nil {
			t.Fatalf("failed to fetch chunk %d: %v", h, err)
		}
		cdata, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("failed to read chunk body %d: %v", h, err)
		}
		_ = chunker.SaveChunk(h, cdata)
	}

	// 5. Reassemble on B
	if err := chunker.ReassembleFile(hashesA, fileB); err != nil {
		t.Fatalf("failed to reassemble file on B: %v", err)
	}
	storeB.SetRecord(state.FileRecord{
		Path:    fileA,
		Version: recA.Version,
		Chunks:  hashesA,
	})

	shaA := hashFile(t, fileA)
	shaB := hashFile(t, fileB)
	return shaA == shaB, shaA, shaB
}

func TestCorrectnessSuite(t *testing.T) {
	testCases := []struct {
		name string
		mut  func(base []byte) []byte
	}{
		{
			name: "Identical files / zero delta",
			mut:  func(b []byte) []byte { return b },
		},
		{
			name: "1-byte modification",
			mut: func(b []byte) []byte {
				c := bytes.Clone(b)
				c[len(c)/2] ^= 0xFF
				return c
			},
		},
		{
			name: "Small modification (10 bytes)",
			mut: func(b []byte) []byte {
				c := bytes.Clone(b)
				for i := 0; i < 10; i++ {
					c[len(c)/2+i] ^= 0xAA
				}
				return c
			},
		},
		{
			name: "Large modification (100 KB)",
			mut: func(b []byte) []byte {
				c := bytes.Clone(b)
				for i := 0; i < 100*1024 && len(c)/2+i < len(c); i++ {
					c[len(c)/2+i] ^= 0x55
				}
				return c
			},
		},
		{
			name: "Insertion near beginning (shift boundary test)",
			mut: func(b []byte) []byte {
				prefix := []byte("PREPENDED_ARBITRARY_BYTES_FOR_BOUNDARY_SHIFT_VERIFICATION_")
				return append(prefix, b...)
			},
		},
		{
			name: "Insertion in middle",
			mut: func(b []byte) []byte {
				mid := len(b) / 2
				res := make([]byte, 0, len(b)+64)
				res = append(res, b[:mid]...)
				res = append(res, []byte("INSERTED_MIDDLE_PAYLOAD_CHUNK_CDC_BOUNDARY_CHECK")...)
				res = append(res, b[mid:]...)
				return res
			},
		},
		{
			name: "Insertion near end",
			mut: func(b []byte) []byte {
				return append(bytes.Clone(b), []byte("_APPENDED_TAIL_CHUNKS_CHECK")...)
			},
		},
		{
			name: "Deletion (delete 50 KB from middle)",
			mut: func(b []byte) []byte {
				mid := len(b) / 2
				delLen := 50 * 1024
				res := make([]byte, 0, len(b)-delLen)
				res = append(res, b[:mid]...)
				res = append(res, b[mid+delLen:]...)
				return res
			},
		},
		{
			name: "Multiple separated modifications",
			mut: func(b []byte) []byte {
				c := bytes.Clone(b)
				c[100] ^= 0xFF
				c[len(c)/4] ^= 0xCC
				c[len(c)/2] ^= 0xEE
				c[len(c)-100] ^= 0x33
				return c
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			fileA := filepath.Join(tmpDir, "fileA.bin")
			fileB := filepath.Join(tmpDir, "fileB.bin")

			baseData := makeData(1024 * 1024) // 1 MB base file
			modData := tc.mut(baseData)

			if err := os.WriteFile(fileA, modData, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(fileB, baseData, 0644); err != nil {
				t.Fatal(err)
			}

			storeA := state.NewStore()
			storeB := state.NewStore()

			// Pre-populate B with initial base data
			baseHashes, _ := chunker.ChunkFile(fileB)
			storeB.SetRecord(state.FileRecord{
				Path:    fileA,
				Version: 0,
				Chunks:  baseHashes,
			})

			match, shaA, shaB := syncDirect(t, fileA, fileB, storeA, storeB)
			if !match {
				t.Fatalf("SHA-256 mismatch!\nA: %s\nB: %s", shaA, shaB)
			}
			t.Logf("PASS: %s -> SHA-256: %s", tc.name, shaA)
		})
	}
}

func TestMultipleFilesSync(t *testing.T) {
	tmpDir := t.TempDir()
	storeA := state.NewStore()
	storeB := state.NewStore()

	for i := 1; i <= 5; i++ {
		fileA := filepath.Join(tmpDir, fmt.Sprintf("fileA_%d.bin", i))
		fileB := filepath.Join(tmpDir, fmt.Sprintf("fileB_%d.bin", i))

		dataA := makeData(200 * 1024 * i)
		dataB := makeData(100 * 1024 * i)

		os.WriteFile(fileA, dataA, 0644)
		os.WriteFile(fileB, dataB, 0644)

		bHashes, _ := chunker.ChunkFile(fileB)
		storeB.SetRecord(state.FileRecord{Path: fileA, Version: 0, Chunks: bHashes})

		match, shaA, shaB := syncDirect(t, fileA, fileB, storeA, storeB)
		if !match {
			t.Fatalf("file %d mismatch: %s != %s", i, shaA, shaB)
		}
	}
}

func TestDuplicateGossipRejection(t *testing.T) {
	tmpDir := t.TempDir()
	store := state.NewStore()
	filePath := filepath.Join(tmpDir, "test.txt")

	store.SetRecord(state.FileRecord{
		Path:    filePath,
		Version: 3,
		Chunks:  []uint64{101, 102},
	})

	if store.ShouldAccept(filePath, 2) {
		t.Fatal("Accepted older version 2 when current is 3")
	}
	if store.ShouldAccept(filePath, 3) {
		t.Fatal("Accepted duplicate version 3 when current is 3")
	}
	if !store.ShouldAccept(filePath, 4) {
		t.Fatal("Failed to accept newer version 4")
	}
}

func TestOutOfOrderUpdates(t *testing.T) {
	tmpDir := t.TempDir()
	store := state.NewStore()
	filePath := filepath.Join(tmpDir, "order.txt")

	// Version 5 arrives
	store.SetRecord(state.FileRecord{
		Path:    filePath,
		Version: 5,
		Chunks:  []uint64{501},
	})

	// Delayed Version 4 arrives out of order
	if store.ShouldAccept(filePath, 4) {
		t.Fatal("Out of order older update (v4) was incorrectly accepted")
	}

	// Newer Version 6 arrives
	if !store.ShouldAccept(filePath, 6) {
		t.Fatal("Newer update (v6) was rejected")
	}
}

func TestRepeatedSyncStability(t *testing.T) {
	tmpDir := t.TempDir()
	storeA := state.NewStore()
	storeB := state.NewStore()

	fileA := filepath.Join(tmpDir, "repeatA.bin")
	fileB := filepath.Join(tmpDir, "repeatB.bin")

	data := makeData(300 * 1024)
	os.WriteFile(fileA, data, 0644)
	os.WriteFile(fileB, data, 0644)

	// Run 10 successive edits and sync cycles
	for step := 1; step <= 10; step++ {
		data[step*1000] ^= byte(step)
		os.WriteFile(fileA, data, 0644)

		match, shaA, shaB := syncDirect(t, fileA, fileB, storeA, storeB)
		if !match {
			t.Fatalf("repeated sync failed at step %d: %s != %s", step, shaA, shaB)
		}
	}
}

func TestConcurrentSync(t *testing.T) {
	tmpDir := t.TempDir()
	storeA := state.NewStore()
	storeB := state.NewStore()

	var wg sync.WaitGroup
	errCh := make(chan error, 10)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fA := filepath.Join(tmpDir, fmt.Sprintf("concA_%d.bin", id))
			fB := filepath.Join(tmpDir, fmt.Sprintf("concB_%d.bin", id))
			os.WriteFile(fA, makeData(150*1024+id*100), 0644)
			os.WriteFile(fB, makeData(100*1024+id*100), 0644)

			hashesB, _ := chunker.ChunkFile(fB)
			storeB.SetRecord(state.FileRecord{Path: fA, Version: 0, Chunks: hashesB})

			hashesA, _ := chunker.ChunkFile(fA)
			recA, diffCount := storeA.Bump(fA, hashesA)

			iblt := recon.BuildTableWithCapacity(hashesA, diffCount)
			diff, err := recon.Compare(hashesB, iblt.ToBytes())
			if err != nil {
				errCh <- fmt.Errorf("task %d recon failed: %w", id, err)
				return
			}
			_ = diff
			_ = recA
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}
