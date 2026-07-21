package ipfscluster

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	peer "github.com/libp2p/go-libp2p/core/peer"
)

type NodeInfo struct {
	Name   string
	IP     string
	PeerID peer.ID

	GlobalOut uint64 // Mbit/s
	GlobalIn  uint64 // Mbit/s

	DiskRead  uint64 // MB/s
	DiskWrite uint64 // MB/s
}

type LinkInfo struct {
	Src       peer.ID
	Dst       peer.ID
	PairOut   uint64 // Mbit/s
	Effective uint64 // Mbit/s
}

type NetworkTopology struct {
	NodesByPeer map[peer.ID]*NodeInfo
	NodesByName map[string]*NodeInfo
	Links       map[peer.ID]map[peer.ID]*LinkInfo
}

func parseBandwidthToMbit(v string) uint64 {
	v = strings.TrimSpace(v)

	if v == "" || v == "NOT_FOUND" {
		return 0
	}

	if strings.HasSuffix(v, "Gbit") {
		n := strings.TrimSuffix(v, "Gbit")
		x, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0
		}
		return uint64(x * 1000)
	}

	if strings.HasSuffix(v, "Mbit") {
		n := strings.TrimSuffix(v, "Mbit")
		x, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0
		}
		return uint64(x)
	}

	return 0
}

func parseDiskSpeedToMBps(v string) uint64 {
	v = strings.TrimSpace(v)

	if v == "" || v == "NOT_FOUND" {
		return 0
	}

	if strings.HasSuffix(v, "GB/s") {
		n := strings.TrimSuffix(v, "GB/s")
		x, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0
		}
		return uint64(x * 1000)
	}

	if strings.HasSuffix(v, "MB/s") {
		n := strings.TrimSuffix(v, "MB/s")
		x, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0
		}
		return uint64(x)
	}

	// Allow plain numeric values and interpret them as MB/s.
	x, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}

	return uint64(x)
}

func min3(a, b, c uint64) uint64 {
	m := a

	if b < m {
		m = b
	}

	if c < m {
		m = c
	}

	return m
}

func LoadNetworkTopology(path string) (*NetworkTopology, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.TrimLeadingSpace = true

	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	topo := &NetworkTopology{
		NodesByPeer: make(map[peer.ID]*NodeInfo),
		NodesByName: make(map[string]*NodeInfo),
		Links:       make(map[peer.ID]map[peer.ID]*LinkInfo),
	}

	// Expected header:
	//
	// src,
	// src_ip,
	// src_ipfs_id,
	// src_global_out,
	// src_global_in,
	// src_disk_read,
	// src_disk_write,
	// dst,
	// dst_ip,
	// dst_ipfs_id,
	// dst_global_out,
	// dst_global_in,
	// dst_disk_read,
	// dst_disk_write,
	// pairwise_out_src_to_dst,
	// pairwise_out_dst_to_src

	for i, row := range rows {
		if i == 0 {
			continue
		}

		if len(row) < 16 {
			return nil, fmt.Errorf(
				"invalid row %d: expected 16 columns, got %d",
				i+1,
				len(row),
			)
		}

		srcName := strings.TrimSpace(row[0])
		srcIP := strings.TrimSpace(row[1])
		srcPeerStr := strings.TrimSpace(row[2])
		srcGlobalOut := parseBandwidthToMbit(row[3])
		srcGlobalIn := parseBandwidthToMbit(row[4])
		srcDiskRead := parseDiskSpeedToMBps(row[5])
		srcDiskWrite := parseDiskSpeedToMBps(row[6])

		dstName := strings.TrimSpace(row[7])
		dstIP := strings.TrimSpace(row[8])
		dstPeerStr := strings.TrimSpace(row[9])
		dstGlobalOut := parseBandwidthToMbit(row[10])
		dstGlobalIn := parseBandwidthToMbit(row[11])
		dstDiskRead := parseDiskSpeedToMBps(row[12])
		dstDiskWrite := parseDiskSpeedToMBps(row[13])

		pairSrcToDst := parseBandwidthToMbit(row[14])
		pairDstToSrc := parseBandwidthToMbit(row[15])

		srcPeer, err := peer.Decode(srcPeerStr)
		if err != nil {
			return nil, fmt.Errorf(
				"cannot decode src peer %s: %w",
				srcPeerStr,
				err,
			)
		}

		dstPeer, err := peer.Decode(dstPeerStr)
		if err != nil {
			return nil, fmt.Errorf(
				"cannot decode dst peer %s: %w",
				dstPeerStr,
				err,
			)
		}

		if _, ok := topo.NodesByPeer[srcPeer]; !ok {
			n := &NodeInfo{
				Name:      srcName,
				IP:        srcIP,
				PeerID:    srcPeer,
				GlobalOut: srcGlobalOut,
				GlobalIn:  srcGlobalIn,
				DiskRead:  srcDiskRead,
				DiskWrite: srcDiskWrite,
			}

			topo.NodesByPeer[srcPeer] = n
			topo.NodesByName[srcName] = n
		}

		if _, ok := topo.NodesByPeer[dstPeer]; !ok {
			n := &NodeInfo{
				Name:      dstName,
				IP:        dstIP,
				PeerID:    dstPeer,
				GlobalOut: dstGlobalOut,
				GlobalIn:  dstGlobalIn,
				DiskRead:  dstDiskRead,
				DiskWrite: dstDiskWrite,
			}

			topo.NodesByPeer[dstPeer] = n
			topo.NodesByName[dstName] = n
		}

		addLink := func(src, dst peer.ID, pair uint64) {
			srcNode := topo.NodesByPeer[src]
			dstNode := topo.NodesByPeer[dst]

			if topo.Links[src] == nil {
				topo.Links[src] = make(map[peer.ID]*LinkInfo)
			}

			topo.Links[src][dst] = &LinkInfo{
				Src:     src,
				Dst:     dst,
				PairOut: pair,
				Effective: min3(
					srcNode.GlobalOut,
					dstNode.GlobalIn,
					pair,
				),
			}
		}

		addLink(srcPeer, dstPeer, pairSrcToDst)
		addLink(dstPeer, srcPeer, pairDstToSrc)
	}

	return topo, nil
}

func (t *NetworkTopology) EffectiveBandwidth(
	src peer.ID,
	dst peer.ID,
) uint64 {
	if t == nil {
		return 0
	}

	if t.Links[src] == nil {
		return 0
	}

	link := t.Links[src][dst]
	if link == nil {
		return 0
	}

	return link.Effective
}

func (t *NetworkTopology) PairwiseBandwidth(
	src peer.ID,
	dst peer.ID,
) uint64 {
	if t == nil {
		return 0
	}

	if t.Links[src] == nil {
		return 0
	}

	link := t.Links[src][dst]
	if link == nil {
		return 0
	}

	return link.PairOut
}

func (t *NetworkTopology) DiskReadSpeed(p peer.ID) uint64 {
	if t == nil {
		return 0
	}

	node := t.NodesByPeer[p]
	if node == nil {
		return 0
	}

	return node.DiskRead
}

func (t *NetworkTopology) DiskWriteSpeed(p peer.ID) uint64 {
	if t == nil {
		return 0
	}

	node := t.NodesByPeer[p]
	if node == nil {
		return 0
	}

	return node.DiskWrite
}

func (t *NetworkTopology) PrintFull() {
	fmt.Println("========== FULL NETWORK TOPOLOGY ==========")

	// ---- Nodes ----
	fmt.Println("\n--- NODES (Capacity) ---")

	for _, n := range t.NodesByPeer {
		fmt.Printf(
			"Node: %-10s | PeerID: %s | "+
				"OUT: %4d Mbit/s | IN: %4d Mbit/s | "+
				"DISK READ: %4d MB/s | DISK WRITE: %4d MB/s\n",
			n.Name,
			short(n.PeerID),
			n.GlobalOut,
			n.GlobalIn,
			n.DiskRead,
			n.DiskWrite,
		)
	}

	// ---- Links ----
	fmt.Println("\n--- LINKS (Pairwise + Effective) ---")

	for src, dsts := range t.Links {
		for dst, link := range dsts {
			srcNode := t.NodesByPeer[src]
			dstNode := t.NodesByPeer[dst]

			fmt.Printf(
				"%-10s -> %-10s | "+
					"Pair: %4d | OUT(src): %4d | "+
					"IN(dst): %4d | Effective: %4d\n",
				srcNode.Name,
				dstNode.Name,
				link.PairOut,
				srcNode.GlobalOut,
				dstNode.GlobalIn,
				link.Effective,
			)
		}
	}

	fmt.Println("\n===========================================")
}

func short(p peer.ID) string {
	s := p.String()

	if len(s) > 8 {
		return s[:8]
	}

	return s
}

func (t *NetworkTopology) TopPeersByGlobalIn(limit int) []peer.ID {
	if t == nil || limit <= 0 {
		return nil
	}

	nodes := make([]*NodeInfo, 0, len(t.NodesByPeer))

	for _, n := range t.NodesByPeer {
		nodes = append(nodes, n)
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GlobalIn > nodes[j].GlobalIn
	})

	if limit > len(nodes) {
		limit = len(nodes)
	}

	result := make([]peer.ID, limit)

	for i := 0; i < limit; i++ {
		result[i] = nodes[i].PeerID
	}

	return result
}
