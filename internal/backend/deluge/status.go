// Status parsing: converting Deluge status dicts into cached tState.
package deluge

import (
	"context"
	"fmt"
	"github.com/jonny-gm/torrentseek/internal/backend"
	"github.com/jonny-gm/torrentseek/internal/pieces"
)

// --- status fetch and parsing ---

var statusFields = []any{
	"hash", "name", "total_size", "num_pieces", "piece_length", "progress",
	"state", "download_location", "files", "pieces",
	"download_payload_rate", "num_peers", "num_seeds",
}

func (b *Backend) fetchAll(ctx context.Context, c *conn, filter map[any]any) (map[any]any, error) {
	if filter == nil {
		filter = map[any]any{}
	}
	v, err := c.callTimeout(ctx, "core.get_torrents_status", []any{filter, statusFields}, nil)
	if err != nil {
		return nil, err
	}
	m, ok := v.(map[any]any)
	if !ok {
		return nil, fmt.Errorf("deluge: get_torrents_status returned %T", v)
	}
	return m, nil
}

func asInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	}
	return 0
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	}
	return 0
}

// parseStatus turns one torrent's status dict into cached state.
func parseStatus(id backend.ID, st map[any]any) *tState {
	info := backend.TorrentInfo{
		ID:           id,
		Name:         asString(st["name"]),
		TotalSize:    asInt64(st["total_size"]),
		PieceSize:    asInt64(st["piece_length"]),
		PieceCount:   int(asInt64(st["num_pieces"])),
		Progress:     asFloat(st["progress"]) / 100, // Deluge reports 0-100
		RateDownload: asInt64(st["download_payload_rate"]),
		// Deluge's num_peers counts only non-seed peers; a torrent fed
		// entirely by seeds reports num_peers=0 while downloading at full
		// speed. Peers means "connected peers" here, so both counts sum.
		Peers: int(asInt64(st["num_peers"]) + asInt64(st["num_seeds"])),
	}

	files, _ := st["files"].([]any)
	pending := len(files) == 0
	fis := make([]backend.FileInfo, 0, len(files))
	raw := make([]string, 0, len(files))
	for _, fv := range files {
		fd, ok := fv.(map[any]any)
		if !ok {
			continue
		}
		fis = append(fis, backend.FileInfo{
			Index:  int(asInt64(fd["index"])),
			Path:   asString(fd["path"]),
			Length: asInt64(fd["size"]),
			Offset: asInt64(fd["offset"]),
		})
		raw = append(raw, asString(fd["path"]))
	}

	switch state := asString(st["state"]); {
	case pending:
		info.State = backend.StateMetadataPending
	case state == "Seeding":
		info.State = backend.StateSeeding
	case state == "Paused" || state == "Error":
		info.State = backend.StateStopped
	default: // Downloading, Checking, Allocating, Queued, Moving
		info.State = backend.StateDownloading
	}

	// Deluge's pieces field carries swarm availability, not just
	// completion: 0 = no connected peer has the piece, 1 = a peer has it,
	// 2 = being downloaded, 3 = have (deluge/core/torrent.py,
	// _get_pieces_info). The bitfield keeps only 3; the raw states back
	// the optional SwarmObserver capability.
	have := pieces.NewBitfield(info.PieceCount)
	var swarm []backend.PieceSwarmState
	if ps, ok := st["pieces"].([]any); ok {
		swarm = make([]backend.PieceSwarmState, 0, info.PieceCount)
		for i, pv := range ps {
			if i >= info.PieceCount {
				break
			}
			state := backend.PieceSwarmState(asInt64(pv))
			if state > backend.PieceSwarmHave {
				state = backend.PieceSwarmUnavailable
			}
			swarm = append(swarm, state)
			if state == backend.PieceSwarmHave {
				have.Set(i)
			}
		}
	} else if info.PieceCount > 0 && info.Progress >= 1 {
		// A seeding torrent's pieces field is nil (Deluge stops tracking
		// per-piece state once nothing is left to download): all complete.
		swarm = make([]backend.PieceSwarmState, info.PieceCount)
		for i := 0; i < info.PieceCount; i++ {
			have.Set(i)
			swarm[i] = backend.PieceSwarmHave
		}
	}

	return &tState{
		info:             info,
		files:            fis,
		rawPaths:         raw,
		downloadLocation: asString(st["download_location"]),
		have:             have,
		swarm:            swarm,
		pending:          pending,
	}
}
