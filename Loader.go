package ipfscluster

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	peer "github.com/libp2p/go-libp2p/core/peer"
)

type NodeInfo struct {
	Name      string
	IP        string
	PeerID    peer.ID
	GlobalOut uint64 // Mbit
	GlobalIn  uint64 // Mbit
}

type LinkInfo struct {
	Src       peer.ID
	Dst       peer.ID
	PairOut   uint64 // Mbit
	Effective uint64 // Mbit
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
		x, _ := strconv.ParseFloat(n, 64)
		return uint64(x * 1000)
	}

	if strings.HasSuffix(v, "Mbit") {
		n := strings.TrimSuffix(v, "Mbit")
		x, _ := strconv.ParseFloat(n, 64)
		return uint64(x)
	}

	return 0
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
	// src,src_ip,src_ipfs_id,src_global_out,src_global_in,
	// dst,dst_ip,dst_ipfs_id,dst_global_out,dst_global_in,
	// pairwise_out_src_to_dst,pairwise_out_dst_to_src

	for i, row := range rows {
		if i == 0 {
			continue
		}

		if len(row) < 12 {
			return nil, fmt.Errorf("invalid row %d: expected 12 columns, got %d", i+1, len(row))
		}

		srcName := row[0]
		srcIP := row[1]
		srcPeerStr := row[2]
		srcGlobalOut := parseBandwidthToMbit(row[3])
		srcGlobalIn := parseBandwidthToMbit(row[4])

		dstName := row[5]
		dstIP := row[6]
		dstPeerStr := row[7]
		dstGlobalOut := parseBandwidthToMbit(row[8])
		dstGlobalIn := parseBandwidthToMbit(row[9])

		pairSrcToDst := parseBandwidthToMbit(row[10])
		pairDstToSrc := parseBandwidthToMbit(row[11])

		srcPeer, err := peer.Decode(srcPeerStr)
		if err != nil {
			return nil, fmt.Errorf("cannot decode src peer %s: %w", srcPeerStr, err)
		}

		dstPeer, err := peer.Decode(dstPeerStr)
		if err != nil {
			return nil, fmt.Errorf("cannot decode dst peer %s: %w", dstPeerStr, err)
		}

		if _, ok := topo.NodesByPeer[srcPeer]; !ok {
			n := &NodeInfo{
				Name:      srcName,
				IP:        srcIP,
				PeerID:    srcPeer,
				GlobalOut: srcGlobalOut,
				GlobalIn:  srcGlobalIn,
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
				Src:       src,
				Dst:       dst,
				PairOut:   pair,
				Effective: min3(srcNode.GlobalOut, dstNode.GlobalIn, pair),
			}
		}

		addLink(srcPeer, dstPeer, pairSrcToDst)
		addLink(dstPeer, srcPeer, pairDstToSrc)
	}

	return topo, nil
}

func (t *NetworkTopology) EffectiveBandwidth(src, dst peer.ID) uint64 {
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

func (t *NetworkTopology) PairwiseBandwidth(src, dst peer.ID) uint64 {
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

func (t *NetworkTopology) PrintFull() {
	fmt.Println("========== FULL NETWORK TOPOLOGY ==========")

	// ---- Nodes ----
	fmt.Println("\n--- NODES (Capacity) ---")
	for _, n := range t.NodesByPeer {
		fmt.Printf("Node: %-10s | PeerID: %s | OUT: %4d Mbit | IN: %4d Mbit\n",
			n.Name,
			short(n.PeerID),
			n.GlobalOut,
			n.GlobalIn,
		)
	}

	// ---- Links ----
	fmt.Println("\n--- LINKS (Pairwise + Effective) ---")
	for src, dsts := range t.Links {
		for dst, link := range dsts {
			srcNode := t.NodesByPeer[src]
			dstNode := t.NodesByPeer[dst]

			fmt.Printf("%-10s -> %-10s | Pair: %4d | OUT(src): %4d | IN(dst): %4d | Effective: %4d\n",
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
