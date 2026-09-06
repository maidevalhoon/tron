# Architecture

This document describes the design for the hackathon MVP of the synchronization engine.

## Goal
Build a flawless, end-to-end working system that definitively proves the O(|Δ|) network efficiency (the "1-byte edit" demo).

## Simplified Technology Stack
*   **Core Engine**: Go (Golang)
*   **File Chunking**: `fastcdc`
*   **Set Reconciliation**: `go-iblite` (The mathematical core)
*   **File Watcher**: `fsnotify` (With a mandatory 500ms debounce buffer)
*   **State Storage**: In-Memory Go Maps + JSON backup on disk
*   **Networking**: Hardcoded IP peer list + standard Go HTTP Server (Replaces SWIM/memberlist for data plane, uses memberlist for gossip)
*   **Observability**: `bubbletea` Terminal UI (optional, or just console logs for simplicity initially)

## Architecture

1.  **The Watcher & Debouncer**: `fsnotify` detects a file save. A Go channel acts as a 500ms "debounce" buffer to ensure the file is only processed once.
2.  **The Chunker**: The file is passed to `fastcdc`. Only modified chunks generate new SHA-256 (or uint64) hashes.
3.  **In-Memory State**: The file's metadata is updated in a standard Go map. Key: The file path, Value: An ordered array of 64-bit integer hashes.
4.  **Epidemic Gossip**: The node gossips a tiny JSON payload containing the filename and the serialized IBLT byte array via `memberlist`.
5.  **IBLT Reconciliation & Direct Fetch**: Peer nodes receive the gossip, build their own IBLT, subtract, peel to reveal the exact missing chunk hash, and fetch it directly via a simple HTTP GET `/chunk/{hash}`.

## Execution Flow (The 1-Byte Edit Demo)
1.  **Content-Defined Chunking**: Node A `fsnotify` detects a file save. `fastcdc` slices the file and generates uint64 hashes for each chunk. The single changed chunk gets a new hash.
2.  **IBLT Generation**: Node A inserts all chunk hashes into a new `go-iblite` table (e.g., capacity 20). It serializes this table to a small byte array.
3.  **Epidemic Gossip**: Node A gossips the payload (filename + IBLT bytes) via `memberlist`.
4.  **Set Subtraction**: Node B builds its own IBLT for the file, subtracts Node A's IBLT.
5.  **IBLT Peeling & Fetch**: Node B peels the table, revealing the exact missing chunk hash. It fetches the chunk via HTTP from Node A.
