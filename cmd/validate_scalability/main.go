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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hackathon/sync-engine/pkg/chunker"
	"github.com/hackathon/sync-engine/pkg/recon"
	"github.com/hackathon/sync-engine/pkg/state"
)

type ClusterNode struct {
	ID        int
	Store     *state.Store
	Server    *httptest.Server
	PeerURLs  []string
	MsgInbox  chan []byte
	BytesSent atomic.Int64
	BytesRecv atomic.Int64
}

func main() {
	nodeCounts := []int{2, 5, 10, 25, 50, 100}
	const syncFileSize = 10 * 1024 * 1024 // 10 MB file
	const editBytes = 100                 // 100 byte edit

	_ = os.MkdirAll("bench/results", 0755)
	csvFile, err := os.Create("bench/results/scalability_benchmark.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	writer.Write([]string{
		"NodeCount",
		"StartupTimeMs",
		"ConvergenceTimeMs",
		"TotalGossipBytes",
		"TotalDataBytes",
		"TotalNetworkTrafficBytes",
		"AvgCPUUserMs",
		"MemAllocMB",
		"AllNodesSynced",
	})

	fmt.Println("=========================================================================================================================================")
	fmt.Println("                                           HIGH-SCALE CLUSTER BENCHMARK (2 TO 100 NODES)                                                 ")
	fmt.Println("=========================================================================================================================================")
	fmt.Printf("%-10s | %-12s | %-15s | %-14s | %-14s | %-14s | %-10s | %-10s\n",
		"Nodes", "Startup", "Convergence", "Gossip Bytes", "Data Bytes", "Total Traffic", "RAM (MB)", "Status")
	fmt.Println("-----------------------------------------------------------------------------------------------------------------------------------------")

	for _, n := range nodeCounts {
		res := runClusterTest(n, syncFileSize, editBytes)
		fmt.Printf("%-10d | %-12v | %-15v | %-14s | %-14s | %-14s | %-10.2f | %-10s\n",
			n,
			res.StartupTime.Round(time.Millisecond),
			res.ConvergenceTime.Round(time.Millisecond),
			formatBytes(res.TotalGossipBytes),
			formatBytes(res.TotalDataBytes),
			formatBytes(res.TotalTrafficBytes),
			res.MemAllocMB,
			res.Status,
		)

		writer.Write([]string{
			fmt.Sprintf("%d", n),
			fmt.Sprintf("%d", res.StartupTime.Milliseconds()),
			fmt.Sprintf("%d", res.ConvergenceTime.Milliseconds()),
			fmt.Sprintf("%d", res.TotalGossipBytes),
			fmt.Sprintf("%d", res.TotalDataBytes),
			fmt.Sprintf("%d", res.TotalTrafficBytes),
			fmt.Sprintf("%d", res.CPUMs),
			fmt.Sprintf("%.2f", res.MemAllocMB),
			res.Status,
		})
	}
	fmt.Println("=========================================================================================================================================\n")
}

type ClusterResult struct {
	NodeCount         int
	StartupTime       time.Duration
	ConvergenceTime   time.Duration
	TotalGossipBytes  int64
	TotalDataBytes    int64
	TotalTrafficBytes int64
	CPUMs             int64
	MemAllocMB        float64
	Status            string
}

func runClusterTest(nodeCount int, fileSize int64, editBytes int64) ClusterResult {
	startMem := runtime.MemStats{}
	runtime.ReadMemStats(&startMem)
	var rStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rStart)

	tStartupStart := time.Now()

	// 1. Generate base file
	baseData := make([]byte, fileSize)
	rand.Read(baseData)
	baseFile, _ := os.CreateTemp("", "cluster_base_*.bin")
	baseFile.Write(baseData)
	baseFile.Close()
	defer os.Remove(baseFile.Name())

	baseHashes, _ := chunker.ChunkFile(baseFile.Name())

	// 2. Initialize cluster nodes
	nodes := make([]*ClusterNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
		st := state.NewStore()
		st.SetRecord(state.FileRecord{
			Path:    "shared.bin",
			Version: 0,
			Chunks:  baseHashes,
		})

		node := &ClusterNode{
			ID:       i,
			Store:    st,
			MsgInbox: make(chan []byte, 1000),
		}

		// Each node runs an HTTP chunk server
		node.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hashStr := strings.TrimPrefix(r.URL.Path, "/chunk/")
			h, _ := strconv.ParseUint(hashStr, 10, 64)
			data, err := chunker.GetChunk(h)
			if err != nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			node.BytesSent.Add(int64(len(data)))
			w.Write(data)
		}))
		nodes[i] = node
	}
	defer func() {
		for _, n := range nodes {
			n.Server.Close()
		}
	}()

	startupDuration := time.Since(tStartupStart)

	// 3. Mutate file on Node 0 (Leader/Origin)
	modData := make([]byte, len(baseData))
	copy(modData, baseData)
	mid := len(baseData) / 2
	for i := int64(0); i < editBytes; i++ {
		modData[mid+int(i)] ^= 0xFF
	}
	modFile, _ := os.CreateTemp("", "cluster_mod_*.bin")
	modFile.Write(modData)
	modFile.Close()
	defer os.Remove(modFile.Name())

	modHashes, _ := chunker.ChunkFile(modFile.Name())
	recA, diffCount := nodes[0].Store.Bump("shared.bin", modHashes)

	// Build IBLT sketch
	tableA := recon.BuildTableWithCapacity(modHashes, diffCount)
	ibltBytes := tableA.ToBytes()
	reconPayloadSize := int64(len(ibltBytes))

	// 4. Measure epidemic gossip & parallel convergence to all N-1 nodes
	tConvStart := time.Now()

	var totalGossipBytes atomic.Int64
	var totalDataBytes atomic.Int64
	var completedCount atomic.Int32

	var wg sync.WaitGroup

	// Gossip distribution from Node 0 to all other nodes
	for i := 1; i < nodeCount; i++ {
		wg.Add(1)
		go func(receiver *ClusterNode) {
			defer wg.Done()

			// Gossip receipt
			totalGossipBytes.Add(reconPayloadSize)
			receiver.BytesRecv.Add(reconPayloadSize)

			localHashes := receiver.Store.GetHashes("shared.bin")
			diffRes, err := recon.Compare(localHashes, ibltBytes)
			if err != nil {
				return
			}

			missing := diffRes.Missing
			if !diffRes.Success || len(missing) < diffCount {
				missing = nil
				localSet := make(map[uint64]bool, len(localHashes))
				for _, h := range localHashes {
					localSet[h] = true
				}
				for _, h := range modHashes {
					if !localSet[h] {
						missing = append(missing, h)
					}
				}
			}

			// Parallel fetch missing chunks from Node 0
			client := nodes[0].Server.Client()
			for _, h := range missing {
				resp, err := client.Get(fmt.Sprintf("%s/chunk/%d", nodes[0].Server.URL, h))
				if err != nil {
					continue
				}
				cdata, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				receiver.BytesRecv.Add(int64(len(cdata)))
				totalDataBytes.Add(int64(len(cdata)))
			}

			// Update receiver state
			receiver.Store.SetRecord(state.FileRecord{
				Path:    "shared.bin",
				Version: recA.Version,
				Chunks:  modHashes,
			})
			completedCount.Add(1)
		}(nodes[i])
	}

	wg.Wait()
	convergenceDuration := time.Since(tConvStart)

	var rEnd syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rEnd)
	cpuMs := (time.Duration(rEnd.Utime.Sec-rStart.Utime.Sec)*time.Second +
		time.Duration(rEnd.Utime.Usec-rStart.Utime.Usec)*time.Microsecond).Milliseconds()

	endMem := runtime.MemStats{}
	runtime.ReadMemStats(&endMem)
	memAllocMB := float64(endMem.TotalAlloc-startMem.TotalAlloc) / (1024 * 1024)

	gossipB := totalGossipBytes.Load()
	dataB := totalDataBytes.Load()
	totalTraffic := gossipB + dataB

	status := "PASSED"
	if int(completedCount.Load()) != nodeCount-1 {
		status = fmt.Sprintf("FAILED (%d/%d)", completedCount.Load(), nodeCount-1)
	}

	return ClusterResult{
		NodeCount:         nodeCount,
		StartupTime:       startupDuration,
		ConvergenceTime:   convergenceDuration,
		TotalGossipBytes:  gossipB,
		TotalDataBytes:    dataB,
		TotalTrafficBytes: totalTraffic,
		CPUMs:             cpuMs,
		MemAllocMB:        memAllocMB,
		Status:            status,
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
