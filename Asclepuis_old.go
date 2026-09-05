package ipfscluster

// ASCLEPIUS paper-era / HotStorage scheduler.
//
// This version intentionally models ONLY the incoming/download side of the
// selected repair peer. It does not model relocation, final placement,
// destination bandwidth, upload bandwidth, disk bandwidth, or free space.
//
// For each failed shard and candidate repair peer:
//   - a chunk already present on the repair peer costs 0 incoming chunks;
//   - a reusable duplicate available on another peer costs 1 incoming chunk;
//   - a missing chunk that must be reconstructed costs n incoming chunks.
//
// Global Max-Min then schedules failed shards using only the accumulated
// incoming load and incoming bandwidth of candidate repair peers.
//
// The scheduler returns the selected RepairPeer and the common/reusable chunk
// CIDs as []string so the existing repair executor can populate
// Metadata["common"].

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/libp2p/go-libp2p/core/peer"
)

func ascOldCleanCIDString(c string) string {
	c = strings.TrimSpace(c)
	c = strings.Trim(c, "<>")
	return c
}

func ascOldCIDListFromPin(pin api.Pin) []string {
	cidString := pin.Metadata["Cids"]
	parts := strings.Split(cidString, ",")

	out := make([]string, 0, len(parts))
	for _, c := range parts {
		c = ascOldCleanCIDString(c)
		if c != "" {
			out = append(out, c)
		}
	}
	return out
}

func ascOldValidNode(topology *NetworkTopology, p peer.ID) bool {
	return topology != nil &&
		topology.NodesByPeer != nil &&
		topology.NodesByPeer[p] != nil
}

// NetworkTopology stores bandwidth in Mbit/s. The scheduler uses MB/s.
// This is intentionally the same peer-to-topology lookup used by the newer
// ASCLEPIUS scheduler.
func ascOldInMBps(topology *NetworkTopology, p peer.ID) float64 {
	if !ascOldValidNode(topology, p) {
		return 0
	}

	return float64(topology.NodesByPeer[p].GlobalIn) / 8.0
}

func ascOldSortedUniquePeers(peers []peer.ID) []peer.ID {
	seen := make(map[peer.ID]bool)
	out := make([]peer.ID, 0, len(peers))

	for _, p := range peers {
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].String() < out[j].String()
	})

	return out
}

// ASCOldIncomingOnlyShardIndex is a compact per-shard index used to test
// whether a CID is already available locally on a candidate repair peer or is
// reusable from another valid peer.
type ASCOldIncomingOnlyShardIndex struct {
	PeerCIDSet map[peer.ID]map[string]bool
	CIDSources map[string][]peer.ID
}

func ascOldBuildIncomingOnlyShardIndex(
	peerMatchedCIDs map[peer.ID][]string,
) ASCOldIncomingOnlyShardIndex {
	peerCIDSet := make(map[peer.ID]map[string]bool)
	cidSources := make(map[string][]peer.ID)

	for p, cids := range peerMatchedCIDs {
		if peerCIDSet[p] == nil {
			peerCIDSet[p] = make(map[string]bool)
		}

		for _, c := range cids {
			c = ascOldCleanCIDString(c)
			if c == "" {
				continue
			}

			if !peerCIDSet[p][c] {
				peerCIDSet[p][c] = true
				cidSources[c] = append(cidSources[c], p)
			}
		}
	}

	for c := range cidSources {
		cidSources[c] = ascOldSortedUniquePeers(cidSources[c])
	}

	return ASCOldIncomingOnlyShardIndex{
		PeerCIDSet: peerCIDSet,
		CIDSources: cidSources,
	}
}

func ascOldIncomingOnlyPeerHasCIDFast(
	index ASCOldIncomingOnlyShardIndex,
	p peer.ID,
	cidStr string,
) bool {
	if index.PeerCIDSet[p] == nil {
		return false
	}

	return index.PeerCIDSet[p][cidStr]
}

func ascOldIncomingOnlyHasValidSourceFast(
	index ASCOldIncomingOnlyShardIndex,
	cidStr string,
	repairPeer peer.ID,
	failedPeer peer.ID,
) bool {
	for _, src := range index.CIDSources[cidStr] {
		if src == failedPeer || src == repairPeer {
			continue
		}
		return true
	}

	return false
}

// ascOldBuildIncomingOnlyCountsFast classifies every chunk of a failed shard
// relative to one candidate repair peer.
//
// localCount:
//
//	chunk already exists on the repair peer -> 0 incoming chunks.
//
// directCount:
//
//	chunk is duplicated on another valid peer -> 1 incoming chunk.
//
// missingCount:
//
//	chunk is not reusable and must be reconstructed -> n incoming chunks.
//
// commonChunks contains all reusable CIDs (both local and remote duplicates)
// as plain strings for the repair executor.
func ascOldBuildIncomingOnlyCountsFast(
	repairPeer peer.ID,
	failedPeer peer.ID,
	shardCIDs []string,
	index ASCOldIncomingOnlyShardIndex,
	n int,
) (int, int, int, int, []string) {
	localCount := 0
	directCount := 0
	missingCount := 0
	commonChunks := make([]string, 0)

	for _, c := range shardCIDs {
		c = ascOldCleanCIDString(c)
		if c == "" {
			continue
		}

		// The repair peer already has this chunk locally.
		if ascOldIncomingOnlyPeerHasCIDFast(index, repairPeer, c) {
			localCount++
			commonChunks = append(commonChunks, c)
			continue
		}

		// The same chunk is available from another valid peer.
		if ascOldIncomingOnlyHasValidSourceFast(
			index,
			c,
			repairPeer,
			failedPeer,
		) {
			directCount++
			commonChunks = append(commonChunks, c)
			continue
		}

		// Otherwise this chunk must be reconstructed using n helper chunks.
		missingCount++
	}

	incomingChunkCount := directCount + (missingCount * n)

	return localCount,
		directCount,
		missingCount,
		incomingChunkCount,
		commonChunks
}

// ASCOldIncomingOnlyEstimate is the output produced for one scheduled failed
// shard. There is intentionally no FinalPeer/Relocated field: placement and
// relocation are outside the old HotStorage scheduling model.
type ASCOldIncomingOnlyEstimate struct {
	Shard      api.Pin
	RepairPeer peer.ID

	CommonChunks []string

	LocalChunkCount   int
	DirectChunkCount  int
	MissingChunkCount int

	IncomingChunkCount int

	// ProcessingTime is the repair time without any previously accumulated
	// load on the repair peer.
	ProcessingTime float64

	// FinishTime is the completion estimate used by Global Max-Min after
	// including the repair peer's currently accumulated incoming load.
	FinishTime float64
}

// ascOldIncomingFinishTime returns the completion time on one candidate repair
// peer using only its accumulated incoming MB load and GlobalIn bandwidth.
func ascOldIncomingFinishTime(
	repairPeer peer.ID,
	incomingChunks int,
	currentIncomingLoadMB float64,
	topology *NetworkTopology,
	chunkMB float64,
) float64 {
	inMBps := ascOldInMBps(topology, repairPeer)
	if inMBps <= 0 {
		return math.Inf(1)
	}

	incomingMB := float64(incomingChunks) * chunkMB
	return (currentIncomingLoadMB + incomingMB) / inMBps
}

func ascOldIncomingProcessingTime(
	repairPeer peer.ID,
	incomingChunks int,
	topology *NetworkTopology,
	chunkMB float64,
) float64 {
	return ascOldIncomingFinishTime(
		repairPeer,
		incomingChunks,
		0.0,
		topology,
		chunkMB,
	)
}

// ScheduleASCLEPIUSOldIncomingOnly implements the paper-era ASCLEPIUS Global
// Max-Min scheduler using only incoming/download bandwidth of repair peers.
//
// assignments is keyed by RepairPeer, not by a final placement destination.
// estimates contains the same selected RepairPeer together with the common CID
// strings needed by the repair executor.
func ScheduleASCLEPIUSOldIncomingOnly(
	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,

	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (peer.ID, []string, map[peer.ID]int, map[peer.ID][]string),
) (map[peer.ID][]api.Pin, []ASCOldIncomingOnlyEstimate) {
	fmt.Println("[ASC-OLD] incoming-only Global Max-Min repair strategy")

	totalStart := time.Now()

	assignments := make(map[peer.ID][]api.Pin)
	estimates := make([]ASCOldIncomingOnlyEstimate, 0)

	if len(failedShards) == 0 || len(candidatePeers) == 0 {
		fmt.Printf("[ASC-OLD][TOTAL] exited early in %v\n", time.Since(totalStart))
		return assignments, estimates
	}

	candidatePeers = ascOldSortedUniquePeers(candidatePeers)

	if topology == nil {
		fmt.Printf("[ASC-OLD][TOTAL] nil network topology\n")
		return assignments, estimates
	}

	if chunkMB <= 0 {
		fmt.Printf("[ASC-OLD][TOTAL] invalid chunkMB=%f\n", chunkMB)
		return assignments, estimates
	}

	// Use exactly the same peer-to-topology validity convention as the newer
	// ASCLEPIUS scheduler: a candidate must exist in topology.NodesByPeer.
	filteredPeers := make([]peer.ID, 0, len(candidatePeers))
	for _, p := range candidatePeers {
		if p == failedPeer ||
			!ascOldValidNode(topology, p) ||
			ascOldInMBps(topology, p) <= 0 {
			continue
		}
		filteredPeers = append(filteredPeers, p)
	}
	candidatePeers = filteredPeers

	if len(candidatePeers) == 0 {
		fmt.Printf("[ASC-OLD][TOTAL] no valid candidate repair peers\n")
		return assignments, estimates
	}

	type ShardPrecompute struct {
		Shard     api.Pin
		ShardCIDs []string
		N         int
		Index     ASCOldIncomingOnlyShardIndex
	}

	type CandidateCost struct {
		LocalChunkCount    int
		DirectChunkCount   int
		MissingChunkCount  int
		IncomingChunkCount int
		CommonChunks       []string
		ProcessingTime     float64
	}

	precomputed := make(map[string]ShardPrecompute)

	start := time.Now()

	for _, shard := range failedShards {
		shardKey := shard.Cid.String()

		shardCIDs := ascOldCIDListFromPin(shard)
		if len(shardCIDs) == 0 {
			continue
		}

		// The old scheduler only needs n from the stripe metadata.
		_, _, n, _ := getSameStripe(shard)
		if n <= 0 {
			continue
		}

		_, _, _, peerMatchedCIDs := getSimilarity(shard)
		index := ascOldBuildIncomingOnlyShardIndex(peerMatchedCIDs)

		precomputed[shardKey] = ShardPrecompute{
			Shard:     shard,
			ShardCIDs: shardCIDs,
			N:         n,
			Index:     index,
		}
	}

	fmt.Printf(
		"[ASC-OLD][PHASE] precompute similarities + indexes took: %v\n",
		time.Since(start),
	)

	unscheduled := make([]api.Pin, 0, len(failedShards))
	for _, shard := range failedShards {
		if _, ok := precomputed[shard.Cid.String()]; ok {
			unscheduled = append(unscheduled, shard)
		}
	}

	// Accumulated incoming traffic assigned to every repair peer, in MB.
	// This mirrors the newer ASCLEPIUS DownloadMB load representation while
	// still modeling ONLY the repair peer's incoming side.
	peerIncomingLoadMB := make(map[peer.ID]float64)
	for _, p := range candidatePeers {
		peerIncomingLoadMB[p] = 0
	}

	start = time.Now()

	// Counts are independent of accumulated load, so compute them once.
	candidateCosts := make(map[string]map[peer.ID]CandidateCost)

	for _, shard := range unscheduled {
		shardKey := shard.Cid.String()
		pc := precomputed[shardKey]

		candidateCosts[shardKey] = make(map[peer.ID]CandidateCost)

		for _, repairPeer := range candidatePeers {
			if repairPeer == failedPeer {
				continue
			}

			localCount,
				directCount,
				missingCount,
				incomingCount,
				commonChunks := ascOldBuildIncomingOnlyCountsFast(
				repairPeer,
				failedPeer,
				pc.ShardCIDs,
				pc.Index,
				pc.N,
			)

			processing := ascOldIncomingProcessingTime(
				repairPeer,
				incomingCount,
				topology,
				chunkMB,
			)

			if math.IsInf(processing, 1) {
				continue
			}

			candidateCosts[shardKey][repairPeer] = CandidateCost{
				LocalChunkCount:    localCount,
				DirectChunkCount:   directCount,
				MissingChunkCount:  missingCount,
				IncomingChunkCount: incomingCount,
				CommonChunks:       append([]string(nil), commonChunks...),
				ProcessingTime:     processing,
			}
		}
	}

	fmt.Printf(
		"[ASC-OLD][PHASE] precompute candidate costs took: %v\n",
		time.Since(start),
	)

	start = time.Now()

	// Global Max-Min:
	// 1. For each unscheduled shard, find the repair peer with minimum current
	//    completion time.
	// 2. Among those shard minima, select the shard with the maximum minimum
	//    completion time.
	// 3. Commit it and increase only that repair peer's incoming load.
	for len(unscheduled) > 0 {
		type CandidateBest struct {
			Shard      api.Pin
			RepairPeer peer.ID

			LocalChunkCount    int
			DirectChunkCount   int
			MissingChunkCount  int
			IncomingChunkCount int
			CommonChunks       []string

			ProcessingTime float64
			CompletionTime float64
		}

		bestForShard := make(map[string]CandidateBest)

		for _, shard := range unscheduled {
			shardKey := shard.Cid.String()

			bestRepairPeer := peer.ID("")
			bestCompletion := math.Inf(1)
			bestCost := CandidateCost{}

			for _, repairPeer := range candidatePeers {
				cost, ok := candidateCosts[shardKey][repairPeer]
				if !ok {
					continue
				}

				completion := ascOldIncomingFinishTime(
					repairPeer,
					cost.IncomingChunkCount,
					peerIncomingLoadMB[repairPeer],
					topology,
					chunkMB,
				)

				if math.IsInf(completion, 1) {
					continue
				}

				if completion < bestCompletion ||
					(completion == bestCompletion &&
						(bestRepairPeer == "" || repairPeer.String() < bestRepairPeer.String())) {
					bestRepairPeer = repairPeer
					bestCompletion = completion
					bestCost = cost
				}
			}

			if bestRepairPeer != "" && !math.IsInf(bestCompletion, 1) {
				bestForShard[shardKey] = CandidateBest{
					Shard:      shard,
					RepairPeer: bestRepairPeer,

					LocalChunkCount:    bestCost.LocalChunkCount,
					DirectChunkCount:   bestCost.DirectChunkCount,
					MissingChunkCount:  bestCost.MissingChunkCount,
					IncomingChunkCount: bestCost.IncomingChunkCount,
					CommonChunks:       append([]string(nil), bestCost.CommonChunks...),

					ProcessingTime: bestCost.ProcessingTime,
					CompletionTime: bestCompletion,
				}
			}
		}

		if len(bestForShard) == 0 {
			break
		}

		// Max-Min step: choose the shard whose best achievable completion time
		// is the largest.
		chosenIndex := -1
		chosenCompletion := -1.0
		chosenKey := ""

		for idx, shard := range unscheduled {
			key := shard.Cid.String()
			cand, ok := bestForShard[key]
			if !ok {
				continue
			}

			if chosenIndex == -1 ||
				cand.CompletionTime > chosenCompletion ||
				(cand.CompletionTime == chosenCompletion && key < chosenKey) {
				chosenIndex = idx
				chosenCompletion = cand.CompletionTime
				chosenKey = key
			}
		}

		if chosenIndex == -1 {
			break
		}

		chosenShard := unscheduled[chosenIndex]
		chosen := bestForShard[chosenShard.Cid.String()]

		// Commit ONLY incoming work on the selected repair peer.
		peerIncomingLoadMB[chosen.RepairPeer] +=
			float64(chosen.IncomingChunkCount) * chunkMB

		// The assignment is keyed by the repairing peer. There is no final
		// placement/destination decision in this scheduler.
		assignments[chosen.RepairPeer] = append(
			assignments[chosen.RepairPeer],
			chosenShard,
		)

		estimates = append(estimates, ASCOldIncomingOnlyEstimate{
			Shard:      chosenShard,
			RepairPeer: chosen.RepairPeer,

			CommonChunks: append([]string(nil), chosen.CommonChunks...),

			LocalChunkCount:    chosen.LocalChunkCount,
			DirectChunkCount:   chosen.DirectChunkCount,
			MissingChunkCount:  chosen.MissingChunkCount,
			IncomingChunkCount: chosen.IncomingChunkCount,

			ProcessingTime: chosen.ProcessingTime,
			FinishTime:     chosen.CompletionTime,
		})

		fmt.Printf(
			"[ASC-OLD] assigned shard=%s repairPeer=%s processing=%f finish=%f local=%d direct=%d missing=%d incoming=%d common=%s\n",
			chosenShard.Name,
			chosen.RepairPeer.String(),
			chosen.ProcessingTime,
			chosen.CompletionTime,
			chosen.LocalChunkCount,
			chosen.DirectChunkCount,
			chosen.MissingChunkCount,
			chosen.IncomingChunkCount,
			strings.Join(chosen.CommonChunks, ","),
		)

		unscheduled = append(
			unscheduled[:chosenIndex],
			unscheduled[chosenIndex+1:]...,
		)
	}

	fmt.Printf(
		"[ASC-OLD][PHASE] scheduling loop took: %v\n",
		time.Since(start),
	)
	fmt.Printf(
		"[ASC-OLD][TOTAL] ScheduleASCLEPIUSOldIncomingOnly took: %v\n",
		time.Since(totalStart),
	)

	return assignments, estimates
}
