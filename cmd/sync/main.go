package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hackathon/sync-engine/pkg/chunker"
	"github.com/hackathon/sync-engine/pkg/network"
	"github.com/hackathon/sync-engine/pkg/recon"
	"github.com/hackathon/sync-engine/pkg/state"
	"github.com/hackathon/sync-engine/pkg/watcher"
)

func main() {
	bindPort := flag.Int("port", 7946, "memberlist bind port")
	httpPort := flag.Int("http", 8080, "http port for chunks")
	peersFlag := flag.String("peers", "", "comma separated list of peers")
	watchDir := flag.String("watch", ".", "directory to watch")
	flag.Parse()

	peers := []string{}
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	absWatchDir, err := filepath.Abs(*watchDir)
	if err != nil {
		log.Fatal(err)
	}

	st := state.NewStore()

	// Initial chunking of existing files
	err = filepath.Walk(absWatchDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && !strings.Contains(path, chunker.CacheDir) {
			hashes, err := chunker.ChunkFile(path)
			if err == nil {
				st.SetHashes(path, hashes)
				log.Printf("Initial chunked %s (chunks: %d)", path, len(hashes))
			}
		}
		return nil
	})
	if err != nil {
		log.Println("Walk error:", err)
	}

	msgCh := make(chan network.GossipPayload, 100)

	net, err := network.NewNetwork(*bindPort, *httpPort, msgCh)
	if err != nil {
		log.Fatal(err)
	}

	if err := net.Join(peers); err != nil {
		log.Println("Join error:", err)
	}

	network.StartHTTPServer(*httpPort, chunker.GetChunk)

	w, err := watcher.NewWatcher(500 * time.Millisecond)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	if err := w.Add(absWatchDir); err != nil {
		log.Fatal(err)
	}

	w.OnChange(func(path string) {
		// Ignore cache dir
		if strings.Contains(path, chunker.CacheDir) {
			return
		}
		
		log.Printf("[Event] File changed: %s\n", path)
		hashes, err := chunker.ChunkFile(path)
		if err != nil {
			log.Println("Chunk error:", err)
			return
		}

		diffCount := st.UpdateHashes(path, hashes)

		ibltTable := recon.BuildTableWithCapacity(hashes, diffCount)
		ibltBytes := ibltTable.ToBytes()

		log.Printf("[Gossip] Broadcasting IBLT for %s (chunks: %d, diffCount: %d, size: %d bytes)\n", path, len(hashes), diffCount, len(ibltBytes))
		if err := net.Broadcast(path, ibltBytes); err != nil {
			log.Println("Broadcast error:", err)
		}
	})

	w.Start()

	go func() {
		for payload := range msgCh {
			log.Printf("[Gossip Recv] Received IBLT for %s from %s:%d\n", payload.Filename, payload.OriginIP, payload.OriginHTTPPort)
			
			localHashes := st.GetHashes(payload.Filename)
			
			diff, err := recon.Compare(localHashes, payload.IBLTBytes)
			if err != nil {
				log.Println("Compare error:", err)
				continue
			}

			if len(diff.Missing) == 0 {
				log.Println("[Sync] No missing chunks.")
				continue
			}

			for _, missingHash := range diff.Missing {
				log.Printf("[Fetch] Need chunk %d. Downloading from %s:%d...\n", missingHash, payload.OriginIP, payload.OriginHTTPPort)
				data, err := network.FetchChunk(payload.OriginIP, payload.OriginHTTPPort, missingHash)
				if err != nil {
					log.Println("Fetch error:", err)
					continue
				}
				
				// Just save it to cache to simulate receiving it
				chunkPath := filepath.Join(chunker.CacheDir, fmt.Sprintf("%d", missingHash))
				os.WriteFile(chunkPath, data, 0644)
				log.Printf("[Fetch] Successfully downloaded chunk %d (%d bytes).\n", missingHash, len(data))
			}
			
			// Note: We don't reconstruct the actual file here without the manifest (hash sequence)
			// But for the hackathon MVP, fetching the specific 1-byte changed chunk proves O(|Delta|) efficiency!
		}
	}()

	fmt.Printf("Node running on Memberlist:%d, HTTP:%d\n", *bindPort, *httpPort)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	fmt.Println("Shutting down...")
}
