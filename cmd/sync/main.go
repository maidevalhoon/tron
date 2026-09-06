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

		rec, diffCount := st.Bump(path, hashes)

		ibltTable := recon.BuildTableWithCapacity(hashes, diffCount)
		ibltBytes := ibltTable.ToBytes()

		log.Printf("[Gossip] Broadcasting update for %s (version: %d, chunks: %d, diffCount: %d, iblt: %d bytes)\n",
			path, rec.Version, len(hashes), diffCount, len(ibltBytes))
		if err := net.BroadcastUpdate(path, rec.Version, ibltBytes, hashes); err != nil {
			log.Println("Broadcast error:", err)
		}
	})

	w.Start()

	go func() {
		for payload := range msgCh {
			log.Printf("[Gossip Recv] Received update for %s (version %d) from %s:%d\n",
				payload.Filename, payload.Version, payload.OriginIP, payload.OriginHTTPPort)

			if !st.ShouldAccept(payload.Filename, payload.Version) {
				log.Printf("[Sync] Ignoring stale/duplicate version %d for %s\n", payload.Version, payload.Filename)
				continue
			}

			localHashes := st.GetHashes(payload.Filename)

			diff, err := recon.Compare(localHashes, payload.IBLTBytes)
			if err != nil {
				log.Println("Compare error:", err)
				continue
			}

			// Determine missing chunks (with manifest fallback if IBLT decoding fails)
			missingChunks := diff.Missing
			if !diff.Success && len(payload.Manifest) > 0 {
				log.Println("[Sync] IBLT decode incomplete, falling back to manifest comparison")
				localSet := make(map[uint64]bool, len(localHashes))
				for _, h := range localHashes {
					localSet[h] = true
				}
				missingChunks = nil
				for _, h := range payload.Manifest {
					if !localSet[h] {
						missingChunks = append(missingChunks, h)
					}
				}
			}

			for _, missingHash := range missingChunks {
				log.Printf("[Fetch] Need chunk %d. Downloading from %s:%d...\n", missingHash, payload.OriginIP, payload.OriginHTTPPort)
				data, err := network.FetchChunk(payload.OriginIP, payload.OriginHTTPPort, missingHash)
				if err != nil {
					log.Println("Fetch error:", err)
					continue
				}

				if err := chunker.SaveChunk(missingHash, data); err != nil {
					log.Println("SaveChunk error:", err)
				}
				log.Printf("[Fetch] Successfully downloaded chunk %d (%d bytes).\n", missingHash, len(data))
			}

			// If manifest is provided, reassemble the file into place
			if len(payload.Manifest) > 0 {
				destFile := filepath.Join(absWatchDir, filepath.Base(payload.Filename))
				if err := chunker.ReassembleFile(payload.Manifest, destFile); err != nil {
					log.Printf("[Reassemble Error] %v\n", err)
				} else {
					log.Printf("[Reassemble] File %s synchronized successfully (version %d).\n", destFile, payload.Version)
					st.SetRecord(state.FileRecord{
						Path:    payload.Filename,
						Version: payload.Version,
						Chunks:  payload.Manifest,
					})
				}
			}
		}
	}()

	fmt.Printf("Node running on Memberlist:%d, HTTP:%d\n", *bindPort, *httpPort)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	fmt.Println("Shutting down...")
}
