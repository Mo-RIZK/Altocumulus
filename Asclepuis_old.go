package ipfscluster

// ASCLEPIUS paper-era scheduler.
// This file intentionally models only incoming/download bandwidth.
// All private identifiers are prefixed with ascOld to avoid collisions with
// the current multi-resource ASCLEPIUS implementation in the same package.

import (
	"fmt"
	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/libp2p/go-libp2p/core/peer"
	"math"
	"sort"
	"strings"
	"time"
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

func ascOldTopologyNodeIn(t *NetworkTopology, p peer.ID) uint64 {
	if t == nil || t.NodesByPeer == nil {
		return 0
	}
	n := t.NodesByPeer[p]
	if n == nil {
		return 0
	}
	return n.GlobalIn
}

type ASCOldIndexedChunkKind string

type ASCOldIndexedChunkRepair struct {
	Index int
	CID   string
	Kind  ASCOldIndexedChunkKind
	Cost  int // local=0, direct=1, missing=n
}

type ASCOldIndexedRepairEstimate struct {
	Shard      api.Pin
	RepairPeer peer.ID

	Timeline    []ASCOldIndexedChunkRepair
	LoadByIndex map[int]int

	LocalChunkCount   int
	DirectChunkCount  int
	MissingChunkCount int

	ProcessingTime float64
	FinishTime     float64
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

func ascOldEstimateIncomingOnlyProcessingTime(

	incomingChunkCount int,
	repairPeer peer.ID,
	topology *NetworkTopology,
	chunkMB float64,

) float64 {
	in := ascOldTopologyNodeIn(topology, repairPeer)
	if in == 0 {
		return math.Inf(1)
	}

	return float64(incomingChunkCount) * chunkMB / float64(in)
}

////////////////////////////////////////////////////////////////////////

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

		// Local duplicate on the repair peer.
		if ascOldIncomingOnlyPeerHasCIDFast(index, repairPeer, c) {
			localCount++
			commonChunks = append(commonChunks, c)
			continue
		}

		// Remote duplicate. The old scheduler only cares that a valid
		// duplicate exists; we return only its CID as a string.
		if ascOldIncomingOnlyHasValidSourceFast(index, c, repairPeer, failedPeer) {
			directCount++
			commonChunks = append(commonChunks, c)
			continue
		}

		missingCount++
	}

	incomingChunkCount := directCount + (missingCount * n)

	return localCount, directCount, missingCount, incomingChunkCount, commonChunks
}

type ASCOldIncomingOnlyRelocationEstimate struct {
	Shard api.Pin

	RepairPeer peer.ID
	FinalPeer  peer.ID
	Relocated  bool

	// Same shape as the new ASCLEPIUS result.
	// The caller only needs CommonChunks[i].CID for Metadata["common"].
	CommonChunks []string

	LocalChunkCount   int
	DirectChunkCount  int
	MissingChunkCount int

	IncomingChunkCount int

	RepairIncomingChunkCount     int
	RelocationIncomingChunkCount int

	ProcessingTime float64
	FinishTime     float64
}

func ascOldPeerSetRelocationFast(peers []peer.ID) map[peer.ID]bool {
	out := make(map[peer.ID]bool)
	for _, p := range peers {
		if p != "" {
			out[p] = true
		}
	}
	return out
}

func ascOldPeerIncomingTimeRelocationFast(

	p peer.ID,
	chunks int,
	topology *NetworkTopology,
	chunkMB float64,

) float64 {
	if chunks == 0 {
		return 0
	}

	in := ascOldTopologyNodeIn(topology, p)
	if in == 0 {
		return math.Inf(1)
	}

	return float64(chunks) * chunkMB / float64(in)
}

func ascOldMax2RelocationFast(a, b float64) float64 {
	if b > a {
		return b
	}
	return a
}

func ScheduleASCLEPIUSOldIncomingOnly(

	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,

	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (peer.ID, []string, map[peer.ID]int, map[peer.ID][]string),

) (map[peer.ID][]api.Pin, []ASCOldIncomingOnlyRelocationEstimate) {
	fmt.Println("[ASC-OLD] incoming-only Global Max-Min repair strategy with relocation")

	totalStart := time.Now()

	assignments := make(map[peer.ID][]api.Pin)
	estimates := make([]ASCOldIncomingOnlyRelocationEstimate, 0)

	if len(failedShards) == 0 || len(candidatePeers) == 0 {
		fmt.Printf("[ASC-OLD][TOTAL] exited early in %v\n", time.Since(totalStart))
		return assignments, estimates
	}

	candidatePeers = ascOldSortedUniquePeers(candidatePeers)

	type ShardPrecompute struct {
		Shard           api.Pin
		ShardCIDs       []string
		ShardSize       int
		N               int
		SameStripePeers map[peer.ID]bool
		PeerMatchedCIDs map[peer.ID][]string
		Index           ASCOldIncomingOnlyShardIndex
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
		shardSize := len(shardCIDs)
		if shardSize == 0 {
			continue
		}

		_, sameStripePeers, n, shardLength := getSameStripe(shard)
		if shardLength > 0 {
			shardSize = shardLength
		}

		_, _, _, peerMatchedCIDs := getSimilarity(shard)
		index := ascOldBuildIncomingOnlyShardIndex(peerMatchedCIDs)

		precomputed[shardKey] = ShardPrecompute{
			Shard:           shard,
			ShardCIDs:       shardCIDs,
			ShardSize:       shardSize,
			N:               n,
			SameStripePeers: ascOldPeerSetRelocationFast(sameStripePeers),
			PeerMatchedCIDs: peerMatchedCIDs,
			Index:           index,
		}
	}

	fmt.Printf("[ASC-OLD][PHASE] precompute similarities + indexes took: %v\n", time.Since(start))

	unscheduled := make([]api.Pin, 0)
	for _, shard := range failedShards {
		if _, ok := precomputed[shard.Cid.String()]; ok {
			unscheduled = append(unscheduled, shard)
		}
	}

	peerIncomingLoad := make(map[peer.ID]int)
	for _, p := range candidatePeers {
		peerIncomingLoad[p] = 0
	}

	start = time.Now()

	candidateCosts := make(map[string]map[peer.ID]CandidateCost)

	for _, shard := range unscheduled {
		shardKey := shard.Cid.String()
		pc := precomputed[shardKey]

		candidateCosts[shardKey] = make(map[peer.ID]CandidateCost)

		for _, repairPeer := range candidatePeers {
			if repairPeer == failedPeer {
				continue
			}

			if ascOldTopologyNodeIn(topology, repairPeer) == 0 {
				continue
			}

			localCount, directCount, missingCount, incomingCount, commonChunks :=
				ascOldBuildIncomingOnlyCountsFast(
					repairPeer,
					failedPeer,
					pc.ShardCIDs,
					pc.Index,
					pc.N,
				)

			processing := ascOldEstimateIncomingOnlyProcessingTime(
				incomingCount,
				repairPeer,
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

	fmt.Printf("[ASC-OLD][PHASE] precompute candidate costs took: %v\n", time.Since(start))

	start = time.Now()

	for len(unscheduled) > 0 {
		type CandidateBest struct {
			Shard api.Pin

			RepairPeer peer.ID
			FinalPeer  peer.ID
			Relocated  bool

			LocalChunkCount   int
			DirectChunkCount  int
			MissingChunkCount int
			CommonChunks      []string

			RepairIncomingChunkCount     int
			RelocationIncomingChunkCount int

			ProcessingTime float64
			CompletionTime float64
		}

		bestForShard := make(map[string]CandidateBest)

		for _, shard := range unscheduled {
			shardKey := shard.Cid.String()
			pc := precomputed[shardKey]

			bestRepairPeer := peer.ID("")
			bestFinalPeer := peer.ID("")
			bestRelocated := false

			bestProcessing := math.Inf(1)
			bestCompletion := math.Inf(1)

			bestLocal := 0
			bestDirect := 0
			bestMissing := 0
			var bestCommonChunks []string
			bestRepairIncoming := 0
			bestRelocationIncoming := 0

			bestDestPeer := peer.ID("")
			bestDestTime := math.Inf(1)

			for _, finalPeer := range candidatePeers {
				if finalPeer == failedPeer {
					continue
				}

				if ascOldTopologyNodeIn(topology, finalPeer) == 0 {
					continue
				}

				if pc.SameStripePeers[finalPeer] {
					continue
				}

				destTime := ascOldPeerIncomingTimeRelocationFast(
					finalPeer,
					peerIncomingLoad[finalPeer]+pc.ShardSize,
					topology,
					chunkMB,
				)

				if math.IsInf(destTime, 1) {
					continue
				}

				if destTime < bestDestTime ||
					(destTime == bestDestTime &&
						(bestDestPeer == "" || finalPeer.String() < bestDestPeer.String())) {
					bestDestPeer = finalPeer
					bestDestTime = destTime
				}
			}

			for _, repairPeer := range candidatePeers {
				cost, ok := candidateCosts[shardKey][repairPeer]
				if !ok {
					continue
				}

				repairTime := ascOldPeerIncomingTimeRelocationFast(
					repairPeer,
					peerIncomingLoad[repairPeer]+cost.IncomingChunkCount,
					topology,
					chunkMB,
				)

				if math.IsInf(repairTime, 1) {
					continue
				}

				repairPeerHasSameStripeShard := pc.SameStripePeers[repairPeer]

				var finalPeer peer.ID
				relocated := false
				relocationIncoming := 0
				completion := math.Inf(1)

				if !repairPeerHasSameStripeShard {
					finalPeer = repairPeer
					relocated = false
					relocationIncoming = 0

					// No CurrentGlobalMax here.
					completion = repairTime
				} else {
					if bestDestPeer == "" {
						continue
					}

					finalPeer = bestDestPeer
					relocated = true
					relocationIncoming = pc.ShardSize

					// No CurrentGlobalMax here.
					// Candidate time is the bottleneck between repair and relocation.
					completion = ascOldMax2RelocationFast(
						repairTime,
						bestDestTime,
					)
				}

				if completion < bestCompletion ||
					(completion == bestCompletion &&
						(bestRepairPeer == "" ||
							repairPeer.String() < bestRepairPeer.String() ||
							(repairPeer.String() == bestRepairPeer.String() &&
								finalPeer.String() < bestFinalPeer.String()))) {
					bestRepairPeer = repairPeer
					bestFinalPeer = finalPeer
					bestRelocated = relocated

					bestProcessing = cost.ProcessingTime
					bestCompletion = completion

					bestLocal = cost.LocalChunkCount
					bestDirect = cost.DirectChunkCount
					bestMissing = cost.MissingChunkCount
					bestCommonChunks = append([]string(nil), cost.CommonChunks...)
					bestRepairIncoming = cost.IncomingChunkCount
					bestRelocationIncoming = relocationIncoming
				}
			}

			if bestRepairPeer != "" && bestFinalPeer != "" && !math.IsInf(bestCompletion, 1) {
				bestForShard[shardKey] = CandidateBest{
					Shard: shard,

					RepairPeer: bestRepairPeer,
					FinalPeer:  bestFinalPeer,
					Relocated:  bestRelocated,

					LocalChunkCount:   bestLocal,
					DirectChunkCount:  bestDirect,
					MissingChunkCount: bestMissing,
					CommonChunks:      append([]string(nil), bestCommonChunks...),

					RepairIncomingChunkCount:     bestRepairIncoming,
					RelocationIncomingChunkCount: bestRelocationIncoming,

					ProcessingTime: bestProcessing,
					CompletionTime: bestCompletion,
				}
			}
		}

		if len(bestForShard) == 0 {
			break
		}

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

		peerIncomingLoad[chosen.RepairPeer] += chosen.RepairIncomingChunkCount

		if chosen.Relocated {
			peerIncomingLoad[chosen.FinalPeer] += chosen.RelocationIncomingChunkCount
		}

		assignments[chosen.FinalPeer] = append(assignments[chosen.FinalPeer], chosenShard)

		estimates = append(estimates, ASCOldIncomingOnlyRelocationEstimate{
			Shard:      chosenShard,
			RepairPeer: chosen.RepairPeer,

			FinalPeer: chosen.FinalPeer,
			Relocated: chosen.Relocated,

			CommonChunks: append([]string(nil), chosen.CommonChunks...),

			LocalChunkCount:   chosen.LocalChunkCount,
			DirectChunkCount:  chosen.DirectChunkCount,
			MissingChunkCount: chosen.MissingChunkCount,

			IncomingChunkCount: chosen.RepairIncomingChunkCount,

			RepairIncomingChunkCount:     chosen.RepairIncomingChunkCount,
			RelocationIncomingChunkCount: chosen.RelocationIncomingChunkCount,

			ProcessingTime: chosen.ProcessingTime,
			FinishTime:     chosen.CompletionTime,
		})

		commonCIDs := append([]string(nil), chosen.CommonChunks...)

		fmt.Printf(
			"[ASC-OLD] assigned shard=%s repairPeer=%s finalPeer=%s relocated=%v processing=%f finish=%f local=%d direct=%d missing=%d repairIncoming=%d relocationIncoming=%d common=%s\n",
			chosenShard.Name,
			chosen.RepairPeer.String(),
			chosen.FinalPeer.String(),
			chosen.Relocated,
			chosen.ProcessingTime,
			chosen.CompletionTime,
			chosen.LocalChunkCount,
			chosen.DirectChunkCount,
			chosen.MissingChunkCount,
			chosen.RepairIncomingChunkCount,
			chosen.RelocationIncomingChunkCount,
			strings.Join(commonCIDs, ","),
		)

		unscheduled = append(
			unscheduled[:chosenIndex],
			unscheduled[chosenIndex+1:]...,
		)
	}

	fmt.Printf("[ASC-OLD][PHASE] scheduling loop took: %v\n", time.Since(start))
	fmt.Printf("[ASC-OLD][TOTAL] ScheduleASCLEPIUSOldIncomingOnly WITHOUT CURRENT GLOBAL MAX took: %v\n", time.Since(totalStart))

	return assignments, estimates
}
