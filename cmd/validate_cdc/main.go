package main

import (
	"bytes"
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"

	"github.com/hackathon/sync-engine/pkg/chunker"
)

// fixedChunk splits data into fixed-size chunks (16 KB) and returns their 64-bit hashes.
func fixedChunk(data []byte, chunkSize int) []uint64 {
	var hashes []uint64
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[i:end]
		h := fnv.New64a()
		h.Write(chunk)
		hashes = append(hashes, h.Sum64())
	}
	return hashes
}

func countReusable(oldHashes, newHashes []uint64) (int, float64) {
	oldSet := make(map[uint64]bool, len(oldHashes))
	for _, h := range oldHashes {
		oldSet[h] = true
	}
	reusable := 0
	for _, h := range newHashes {
		if oldSet[h] {
			reusable++
		}
	}
	ratio := float64(reusable) / float64(len(newHashes)) * 100.0
	return reusable, ratio
}

func main() {
	const fileSize = 10 * 1024 * 1024 // 10 MB base file
	const fixedChunkSize = 16 * 1024

	tmpDir, err := os.MkdirTemp("", "cdc_bench_*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create baseline file with realistic entropy so content boundaries trigger properly
	baseFile := filepath.Join(tmpDir, "base.bin")
	baseData := make([]byte, fileSize)
	rand.Read(baseData)
	os.WriteFile(baseFile, baseData, 0644)

	baseFastHashes, _ := chunker.ChunkFile(baseFile)
	baseFixedHashes := fixedChunk(baseData, fixedChunkSize)

	fmt.Println("======================================================================================================================")
	fmt.Println("                                FASTCDC vs. FIXED-SIZE CHUNKING BOUNDARY SHIFT TEST                                   ")
	fmt.Println("======================================================================================================================")
	fmt.Printf("Baseline 10 MB File: FastCDC chunks = %d, Fixed-Size (16KB) chunks = %d\n\n", len(baseFastHashes), len(baseFixedHashes))

	scenarios := []struct {
		name string
		mut  func(b []byte) []byte
	}{
		{
			name: "1. In-place modify (100 bytes at 5MB)",
			mut: func(b []byte) []byte {
				c := bytes.Clone(b)
				for i := 0; i < 100; i++ {
					c[len(c)/2+i] ^= 0xFF
				}
				return c
			},
		},
		{
			name: "2. Insert 100 bytes near BEGINNING (byte 1000)",
			mut: func(b []byte) []byte {
				ins := make([]byte, 100)
				for i := range ins {
					ins[i] = 0xAA
				}
				res := make([]byte, 0, len(b)+len(ins))
				res = append(res, b[:1000]...)
				res = append(res, ins...)
				res = append(res, b[1000:]...)
				return res
			},
		},
		{
			name: "3. Insert 100 bytes in MIDDLE (at 5MB)",
			mut: func(b []byte) []byte {
				ins := make([]byte, 100)
				for i := range ins {
					ins[i] = 0xBB
				}
				mid := len(b) / 2
				res := make([]byte, 0, len(b)+len(ins))
				res = append(res, b[:mid]...)
				res = append(res, ins...)
				res = append(res, b[mid:]...)
				return res
			},
		},
		{
			name: "4. Delete 100 bytes near BEGINNING (byte 1000)",
			mut: func(b []byte) []byte {
				res := make([]byte, 0, len(b)-100)
				res = append(res, b[:1000]...)
				res = append(res, b[1100:]...)
				return res
			},
		},
		{
			name: "5. Delete 100 bytes in MIDDLE (at 5MB)",
			mut: func(b []byte) []byte {
				mid := len(b) / 2
				res := make([]byte, 0, len(b)-100)
				res = append(res, b[:mid]...)
				res = append(res, b[mid+100:]...)
				return res
			},
		},
	}

	_ = os.MkdirAll("bench/results", 0755)
	csvFile, _ := os.Create("bench/results/fastcdc_validation.csv")
	defer csvFile.Close()
	w := csv.NewWriter(csvFile)
	defer w.Flush()

	w.Write([]string{
		"MutationScenario",
		"FastCDCTotalChunks",
		"FastCDCReusableChunks",
		"FastCDCReuseRatioPct",
		"FixedTotalChunks",
		"FixedReusableChunks",
		"FixedReuseRatioPct",
		"Advantage",
	})

	fmt.Printf("%-44s | %-16s | %-16s | %-16s | %-16s\n",
		"Mutation Scenario", "FastCDC Reuse", "FastCDC %", "Fixed Reuse", "Fixed %")
	fmt.Println("----------------------------------------------------------------------------------------------------------------------")

	for _, sc := range scenarios {
		modData := sc.mut(baseData)
		modFile := filepath.Join(tmpDir, "mod.bin")
		os.WriteFile(modFile, modData, 0644)

		fastHashes, _ := chunker.ChunkFile(modFile)
		fastReusable, fastRatio := countReusable(baseFastHashes, fastHashes)

		fixedHashes := fixedChunk(modData, fixedChunkSize)
		fixedReusable, fixedRatio := countReusable(baseFixedHashes, fixedHashes)

		adv := fmt.Sprintf("%.1fx better", fastRatio/fixedRatio)
		if fixedRatio == 0 {
			adv = "Infinity (100% vs 0%)"
		}

		fmt.Printf("%-44s | %-16s | %-15.2f%% | %-16s | %-15.2f%%\n",
			sc.name,
			fmt.Sprintf("%d/%d", fastReusable, len(fastHashes)),
			fastRatio,
			fmt.Sprintf("%d/%d", fixedReusable, len(fixedHashes)),
			fixedRatio,
		)

		w.Write([]string{
			sc.name,
			fmt.Sprintf("%d", len(fastHashes)),
			fmt.Sprintf("%d", fastReusable),
			fmt.Sprintf("%.2f", fastRatio),
			fmt.Sprintf("%d", len(fixedHashes)),
			fmt.Sprintf("%d", fixedReusable),
			fmt.Sprintf("%.2f", fixedRatio),
			adv,
		})
	}
	fmt.Println("======================================================================================================================\n")
}
