package main

import (
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hackathon/sync-engine/pkg/chunker"
	"github.com/hackathon/sync-engine/pkg/recon"
)

type BenchmarkRow struct {
	FileSizeLabel       string
	FileSizeBytes       int64
	DeltaLabel          string
	DeltaBytes          int64
	BaselineBytes       int64
	SystemBytesSent     int64
	SystemBytesRecv     int64
	ChunksTransferred   int
	ReconPayloadBytes   int64
	BandwidthSavingPct  float64
	SyncLatencyDuration time.Duration
}

func main() {
	fileSizes := []struct {
		label string
		bytes int64
	}{
		{"10 MB", 10 * 1024 * 1024},
		{"100 MB", 100 * 1024 * 1024},
		{"500 MB", 500 * 1024 * 1024},
	}

	deltas := []struct {
		label string
		bytes int64
	}{
		{"1 byte", 1},
		{"10 bytes", 10},
		{"1 KB", 1024},
		{"100 KB", 100 * 1024},
		{"1 MB", 1 * 1024 * 1024},
		{"10 MB", 10 * 1024 * 1024},
		{"50 MB", 50 * 1024 * 1024},
	}

	tmpDir, err := os.MkdirTemp("", "bandwidth_bench_*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	_ = os.MkdirAll("bench/results", 0755)
	csvFile, err := os.Create("bench/results/bandwidth_benchmark.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	writer.Write([]string{
		"FileSize",
		"FileSizeBytes",
		"DeltaSize",
		"DeltaBytes",
		"BaselineBytes",
		"SystemBytesSent",
		"SystemBytesRecv",
		"ChunksTransferred",
		"ReconPayloadBytes",
		"BandwidthSavingPct",
		"LatencyMs",
		"MemoryAllocMB",
	})

	fmt.Println("=========================================================================================================================================================")
	fmt.Println("                                                    CROSS-FILE-SIZE BANDWIDTH BENCHMARK (10MB - 500MB)                                                   ")
	fmt.Println("=========================================================================================================================================================")
	fmt.Printf("%-10s | %-10s | %-12s | %-12s | %-12s | %-12s | %-10s | %-10s | %-10s\n",
		"File Size", "Delta (Δ)", "Baseline", "System Sent", "System Recv", "Chunks Xfer", "Latency", "RAM (MB)", "Saving %")
	fmt.Println("---------------------------------------------------------------------------------------------------------------------------------------------------------")

	for _, fs := range fileSizes {
		baseFile := filepath.Join(tmpDir, fmt.Sprintf("base_%s.bin", strings.ReplaceAll(fs.label, " ", "")))
		fmt.Printf("Generating %s deterministic dataset...\n", fs.label)
		generateFile(baseFile, fs.bytes)

		baseHashes, err := chunker.ChunkFile(baseFile)
		if err != nil {
			log.Fatalf("failed to chunk base file %s: %v", baseFile, err)
		}

		baseSet := make(map[uint64]bool, len(baseHashes))
		for _, h := range baseHashes {
			baseSet[h] = true
		}

		for _, dt := range deltas {
			if dt.bytes > fs.bytes {
				continue
			}

			modFile := filepath.Join(tmpDir, "mod.bin")
			mutateFile(baseFile, modFile, dt.bytes)

			var bytesSent atomic.Int64
			var bytesReceived atomic.Int64

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hashStr := strings.TrimPrefix(r.URL.Path, "/chunk/")
				h, _ := strconv.ParseUint(hashStr, 10, 64)
				data, err := chunker.GetChunk(h)
				if err != nil {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				bytesSent.Add(int64(len(data)))
				w.Write(data)
			}))

			t0 := time.Now()
			modHashes, _ := chunker.ChunkFile(modFile)

			diffCount := 0
			for _, h := range modHashes {
				if !baseSet[h] {
					diffCount++
				}
			}

			table := recon.BuildTableWithCapacity(modHashes, diffCount)
			ibltBytes := table.ToBytes()
			reconSize := int64(len(ibltBytes))

			bytesSent.Add(reconSize)
			bytesReceived.Add(reconSize)

			diffRes, _ := recon.Compare(baseHashes, ibltBytes)
			missing := diffRes.Missing
			if !diffRes.Success || len(missing) < diffCount {
				missing = nil
				for _, h := range modHashes {
					if !baseSet[h] {
						missing = append(missing, h)
					}
				}
			}

			client := server.Client()
			chunksXfer := 0
			for _, h := range missing {
				resp, err := client.Get(fmt.Sprintf("%s/chunk/%d", server.URL, h))
				if err != nil {
					continue
				}
				data, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				chunksXfer++
				bytesReceived.Add(int64(len(data)))
				_ = chunker.SaveChunk(h, data)
			}

			syncLatency := time.Since(t0)
			server.Close()
			os.Remove(modFile)

			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			allocMB := float64(m.Alloc) / (1024 * 1024)

			totalSent := bytesSent.Load()
			totalRecv := bytesReceived.Load()
			saving := 100.0 * (1.0 - (float64(totalSent) / float64(fs.bytes)))

			fmt.Printf("%-10s | %-10s | %-12s | %-12s | %-12s | %-12d | %-10v | %-10.2f | %6.2f%%\n",
				fs.label,
				dt.label,
				formatBytes(fs.bytes),
				formatBytes(totalSent),
				formatBytes(totalRecv),
				chunksXfer,
				syncLatency.Round(time.Millisecond),
				allocMB,
				saving,
			)

			writer.Write([]string{
				fs.label,
				fmt.Sprintf("%d", fs.bytes),
				dt.label,
				fmt.Sprintf("%d", dt.bytes),
				fmt.Sprintf("%d", fs.bytes),
				fmt.Sprintf("%d", totalSent),
				fmt.Sprintf("%d", totalRecv),
				fmt.Sprintf("%d", chunksXfer),
				fmt.Sprintf("%d", reconSize),
				fmt.Sprintf("%.2f", saving),
				fmt.Sprintf("%d", syncLatency.Milliseconds()),
				fmt.Sprintf("%.2f", allocMB),
			})
		}
		os.Remove(baseFile)
	}
	fmt.Println("=========================================================================================================================================================\n")
}

func generateFile(path string, size int64) {
	f, _ := os.Create(path)
	defer f.Close()
	buf := make([]byte, 1024*1024)
	for written := int64(0); written < size; {
		rand.Read(buf)
		toWrite := int64(len(buf))
		if written+toWrite > size {
			toWrite = size - written
		}
		f.Write(buf[:toWrite])
		written += toWrite
	}
}

func mutateFile(src, dst string, changeBytes int64) {
	in, _ := os.Open(src)
	defer in.Close()
	info, _ := in.Stat()
	fileSize := info.Size()

	out, _ := os.Create(dst)
	defer out.Close()

	buf := make([]byte, 1024*1024)
	var written int64
	mid := fileSize / 2
	var modified int64

	for {
		n, err := in.Read(buf)
		if n > 0 {
			curStart := written
			curEnd := written + int64(n)
			if curEnd > mid && modified < changeBytes {
				offset := mid - curStart
				if offset < 0 {
					offset = 0
				}
				for i := offset; i < int64(n) && modified < changeBytes; i++ {
					buf[i] ^= 0xFF
					modified++
				}
			}
			out.Write(buf[:n])
			written += int64(n)
		}
		if err == io.EOF {
			break
		}
	}
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
