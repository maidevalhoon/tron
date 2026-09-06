package network

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/hashicorp/memberlist"
)

// GossipPayload is the data gossiped via memberlist
type GossipPayload struct {
	Filename  string
	IBLTBytes []byte
	OriginIP  string
	OriginHTTPPort int
}

type delegate struct {
	msgCh chan<- GossipPayload
	queue *memberlist.TransmitLimitedQueue
}

func (d *delegate) NodeMeta(limit int) []byte { return nil }
func (d *delegate) NotifyMsg(b []byte) {
	var payload GossipPayload
	if err := json.Unmarshal(b, &payload); err == nil {
		d.msgCh <- payload
	}
}
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	if d.queue != nil {
		return d.queue.GetBroadcasts(overhead, limit)
	}
	return nil
}
func (d *delegate) LocalState(join bool) []byte                { return nil }
func (d *delegate) MergeRemoteState(buf []byte, join bool)     {}

type Network struct {
	list     *memberlist.Memberlist
	msgCh    chan GossipPayload
	bcast    *memberlist.TransmitLimitedQueue
	httpPort int
}

func NewNetwork(bindPort int, httpPort int, msgCh chan GossipPayload) (*Network, error) {
	config := memberlist.DefaultLocalConfig()
	config.BindPort = bindPort
	config.Name = fmt.Sprintf("node-%d", bindPort)
	
	del := &delegate{msgCh: msgCh}
	config.Delegate = del

	list, err := memberlist.Create(config)
	if err != nil {
		return nil, err
	}

	bcast := &memberlist.TransmitLimitedQueue{
		NumNodes: func() int { return list.NumMembers() },
		RetransmitMult: 3,
	}
	del.queue = bcast

	return &Network{
		list:     list,
		msgCh:    msgCh,
		bcast:    bcast,
		httpPort: httpPort,
	}, nil
}

func (n *Network) Join(peers []string) error {
	if len(peers) > 0 {
		_, err := n.list.Join(peers)
		return err
	}
	return nil
}

type broadcastMsg struct {
	data []byte
}

func (b *broadcastMsg) Invalidates(other memberlist.Broadcast) bool { return false }
func (b *broadcastMsg) Message() []byte                             { return b.data }
func (b *broadcastMsg) Finished()                                   {}

func (n *Network) Broadcast(filename string, ibltBytes []byte) error {
	ip := n.list.LocalNode().Addr.String()
	payload := GossipPayload{
		Filename: filename,
		IBLTBytes: ibltBytes,
		OriginIP: ip,
		OriginHTTPPort: n.httpPort,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	
	n.bcast.QueueBroadcast(&broadcastMsg{data: data})
	
	// Memberlist queue broadcasting needs an explicit send or is sent on ping?
	// We can directly send to nodes for hackathon reliability.
	for _, node := range n.list.Members() {
		if node.Name != n.list.LocalNode().Name {
			_ = n.list.SendReliable(node, data)
		}
	}

	return nil
}

// FetchChunk makes an HTTP GET to retrieve a chunk from another node.
func FetchChunk(ip string, port int, hash uint64) ([]byte, error) {
	url := fmt.Sprintf("http://%s:%d/chunk/%d", ip, port, hash)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch chunk: %s", resp.Status)
	}

	return io.ReadAll(resp.Body)
}

// StartHTTPServer starts the server for serving chunks.
func StartHTTPServer(port int, getChunk func(uint64) ([]byte, error)) {
	http.HandleFunc("/chunk/", func(w http.ResponseWriter, r *http.Request) {
		hashStr := strings.TrimPrefix(r.URL.Path, "/chunk/")
		hash, err := strconv.ParseUint(hashStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid hash", http.StatusBadRequest)
			return
		}

		data, err := getChunk(hash)
		if err != nil {
			http.Error(w, "chunk not found", http.StatusNotFound)
			return
		}

		w.Write(data)
	})

	go func() {
		log.Printf("Starting HTTP data server on :%d\n", port)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
}
