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
func hasValidValue(v string) bool {
	v = strings.TrimSpace(v)

	return v != "" &&
		!strings.EqualFold(v, "NOT_FOUND")
}

func validNodeValues(
	peerID string,
	globalOut string,
	globalIn string,
	diskRead string,
	diskWrite string,
) bool {
	peerID = strings.TrimSpace(peerID)

	if peerID == "" || !hasValidValue(peerID) {
		return false
	}

	if _, err := peer.Decode(peerID); err != nil {
		return false
	}

	if !hasValidValue(globalOut) ||
		!hasValidValue(globalIn) ||
		!hasValidValue(diskRead) ||
		!hasValidValue(diskWrite) {
		return false
	}

	if parseBandwidthToMbit(globalOut) == 0 ||
		parseBandwidthToMbit(globalIn) == 0 ||
		parseDiskSpeedToMBps(diskRead) == 0 ||
		parseDiskSpeedToMBps(diskWrite) == 0 {
		return false
	}

	return true
}

func validPairwiseValue(v string) bool {
	return hasValidValue(v) && parseBandwidthToMbit(v) > 0
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

	/*
		First pass: identify peers with invalid node information.

		A peer is rejected when:
		  - its peer ID is missing or invalid;
		  - global input/output is missing or zero;
		  - disk read/write is missing or zero.
	*/
	invalidPeersByName := make(map[string]string)

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
		srcPeerStr := strings.TrimSpace(row[2])

		if !validNodeValues(
			srcPeerStr,
			row[3],
			row[4],
			row[5],
			row[6],
		) {
			invalidPeersByName[srcName] = fmt.Sprintf(
				"invalid values: peer_id=%q, global_out=%q, global_in=%q, disk_read=%q, disk_write=%q",
				srcPeerStr,
				strings.TrimSpace(row[3]),
				strings.TrimSpace(row[4]),
				strings.TrimSpace(row[5]),
				strings.TrimSpace(row[6]),
			)
		}

		dstName := strings.TrimSpace(row[7])
		dstPeerStr := strings.TrimSpace(row[9])

		if !validNodeValues(
			dstPeerStr,
			row[10],
			row[11],
			row[12],
			row[13],
		) {
			invalidPeersByName[dstName] = fmt.Sprintf(
				"invalid values: peer_id=%q, global_out=%q, global_in=%q, disk_read=%q, disk_write=%q",
				dstPeerStr,
				strings.TrimSpace(row[10]),
				strings.TrimSpace(row[11]),
				strings.TrimSpace(row[12]),
				strings.TrimSpace(row[13]),
			)
		}
	}

	for name, reason := range invalidPeersByName {
		fmt.Printf(
			"Removing peer %s from topology: %s\n",
			name,
			reason,
		)
	}

	topo := &NetworkTopology{
		NodesByPeer: make(map[peer.ID]*NodeInfo),
		NodesByName: make(map[string]*NodeInfo),
		Links:       make(map[peer.ID]map[peer.ID]*LinkInfo),
	}

	/*
		Second pass: load only rows whose source and destination peers
		have complete and valid node information.
	*/
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
		dstName := strings.TrimSpace(row[7])

		// Completely remove every row involving an invalid peer.
		if _, invalid := invalidPeersByName[srcName]; invalid {
			continue
		}

		if _, invalid := invalidPeersByName[dstName]; invalid {
			continue
		}

		srcIP := strings.TrimSpace(row[1])
		srcPeerStr := strings.TrimSpace(row[2])
		srcGlobalOut := parseBandwidthToMbit(row[3])
		srcGlobalIn := parseBandwidthToMbit(row[4])
		srcDiskRead := parseDiskSpeedToMBps(row[5])
		srcDiskWrite := parseDiskSpeedToMBps(row[6])

		dstIP := strings.TrimSpace(row[8])
		dstPeerStr := strings.TrimSpace(row[9])
		dstGlobalOut := parseBandwidthToMbit(row[10])
		dstGlobalIn := parseBandwidthToMbit(row[11])
		dstDiskRead := parseDiskSpeedToMBps(row[12])
		dstDiskWrite := parseDiskSpeedToMBps(row[13])

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

		addLink := func(
			src peer.ID,
			dst peer.ID,
			pairValue string,
		) {
			// Do not create links with missing or invalid measurements.
			if !validPairwiseValue(pairValue) {
				return
			}

			pair := parseBandwidthToMbit(pairValue)

			srcNode := topo.NodesByPeer[src]
			dstNode := topo.NodesByPeer[dst]

			if srcNode == nil || dstNode == nil {
				return
			}

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

		addLink(srcPeer, dstPeer, row[14])
		addLink(dstPeer, srcPeer, row[15])
	}

	/*
		Final cleanup: remove nodes that ended up with no valid outgoing
		links after pairwise filtering.
	*/
	for p, node := range topo.NodesByPeer {
		if len(topo.Links[p]) > 0 {
			continue
		}

		fmt.Printf(
			"Removing peer %s: no valid outgoing pairwise links\n",
			node.Name,
		)

		delete(topo.NodesByPeer, p)
		delete(topo.NodesByName, node.Name)
		delete(topo.Links, p)
	}

	/*
		Remove incoming links pointing to any peer removed during the
		final cleanup.
	*/
	for src, links := range topo.Links {
		for dst := range links {
			if _, exists := topo.NodesByPeer[dst]; !exists {
				delete(links, dst)
			}
		}

		if len(links) == 0 {
			delete(topo.Links, src)
		}
	}

	fmt.Printf(
		"Loaded topology with %d valid peers and %d invalid peers removed\n",
		len(topo.NodesByPeer),
		len(invalidPeersByName),
	)

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
