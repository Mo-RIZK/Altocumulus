/*
package ipfscluster

import (

	"fmt"
	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/libp2p/go-libp2p/core/peer"
	"math"
	"sort"
	"strings"
	"time"

)

// Erasure coded Heterogeneity and Duplication aware Scheduler //

import (

	"fmt"
	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/libp2p/go-libp2p/core/peer"
	"math"
	"sort"
	"strings"
	"time"

)

	type Transfer struct {
		Src    peer.ID
		Dst    peer.ID
		SizeMB float64
		Kind   string
		CID    string
	}

	type RepairStep struct {
		Transfers []Transfer
		Kind      string
	}

	type RepairJob struct {
		Shard      api.Pin
		RepairPeer peer.ID
		Steps      []RepairStep

		LocalChunkCount   int
		DirectChunkCount  int
		MissingChunkCount int

		OtherElementSources []peer.ID
		SelectedHelpers     []peer.ID

		RepairPeerLocalHelper bool
		NeededRemoteHelpers   int

		FinishTime float64
	}

	type MaxMinEstimate struct {
		Shard      api.Pin
		RepairPeer peer.ID

		ProcessingTime float64
		FinishTime     float64

		ShardSize int

		LocalChunkCount   int
		DirectChunkCount  int
		MissingChunkCount int

		OtherElementSources []peer.ID
		SelectedHelpers     []peer.ID

		RepairPeerLocalHelper bool
		NeededRemoteHelpers   int

		Job RepairJob
	}

	type MaxMinAssignment struct {
		Shard    api.Pin
		Estimate MaxMinEstimate
	}

	type SimTransfer struct {
		Src peer.ID
		Dst peer.ID

		SizeMB      float64
		RemainingMB float64

		JobIndex int
		Step     int

		Kind string
		CID  string
	}

	type SimJobState struct {
		Job        RepairJob
		StepIndex  int
		Finished   bool
		FinishTime float64
	}

	type SimulationResult struct {
		TotalFinishTime float64
		JobFinishTimes  map[string]float64
	}

	func cleanCIDString(c string) string {
		c = strings.TrimSpace(c)
		c = strings.Trim(c, "<>")
		return c
	}

	func cidListFromPin(pin api.Pin) []string {
		cidString := pin.Metadata["Cids"]
		parts := strings.Split(cidString, ",")

		out := make([]string, 0, len(parts))
		for _, c := range parts {
			c = cleanCIDString(c)
			if c != "" {
				out = append(out, c)
			}
		}
		return out
	}

	func topologyNodeIn(t *NetworkTopology, p peer.ID) uint64 {
		if t == nil || t.NodesByPeer == nil {
			return 0
		}
		n := t.NodesByPeer[p]
		if n == nil {
			return 0
		}
		return n.GlobalIn
	}

type IndexedChunkKind string

	type IndexedChunkRepair struct {
		Index int
		CID   string
		Kind  IndexedChunkKind
		Cost  int // local=0, direct=1, missing=n
	}

	type IndexedRepairEstimate struct {
		Shard      api.Pin
		RepairPeer peer.ID

		Timeline    []IndexedChunkRepair
		LoadByIndex map[int]int

		LocalChunkCount   int
		DirectChunkCount  int
		MissingChunkCount int

		ProcessingTime float64
		FinishTime     float64
	}

	func sortedUniquePeers(peers []peer.ID) []peer.ID {
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

func estimateIncomingOnlyProcessingTime(

	incomingChunkCount int,
	repairPeer peer.ID,
	topology *NetworkTopology,
	chunkMB float64,

	) float64 {
		in := topologyNodeIn(topology, repairPeer)
		if in == 0 {
			return math.Inf(1)
		}

		return float64(incomingChunkCount) * chunkMB / float64(in)
	}

////////////////////////////////////////////////////////////////////////

	type IncomingOnlyShardIndex struct {
		PeerCIDSet map[peer.ID]map[string]bool
		CIDSources map[string][]peer.ID
	}

func buildIncomingOnlyShardIndex(

	peerMatchedCIDs map[peer.ID][]string,

	) IncomingOnlyShardIndex {
		peerCIDSet := make(map[peer.ID]map[string]bool)
		cidSources := make(map[string][]peer.ID)

		for p, cids := range peerMatchedCIDs {
			if peerCIDSet[p] == nil {
				peerCIDSet[p] = make(map[string]bool)
			}

			for _, c := range cids {
				c = cleanCIDString(c)
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
			cidSources[c] = sortedUniquePeers(cidSources[c])
		}

		return IncomingOnlyShardIndex{
			PeerCIDSet: peerCIDSet,
			CIDSources: cidSources,
		}
	}

func incomingOnlyPeerHasCIDFast(

	index IncomingOnlyShardIndex,
	p peer.ID,
	cidStr string,

	) bool {
		if index.PeerCIDSet[p] == nil {
			return false
		}

		return index.PeerCIDSet[p][cidStr]
	}

func incomingOnlyHasValidSourceFast(

	index IncomingOnlyShardIndex,
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

func buildIncomingOnlyCountsFast(

	repairPeer peer.ID,
	failedPeer peer.ID,
	shardCIDs []string,
	index IncomingOnlyShardIndex,
	n int,

	) (int, int, int, int) {
		localCount := 0
		directCount := 0
		missingCount := 0

		for _, c := range shardCIDs {
			c = cleanCIDString(c)
			if c == "" {
				continue
			}

			if incomingOnlyPeerHasCIDFast(index, repairPeer, c) {
				localCount++
				continue
			}

			if incomingOnlyHasValidSourceFast(index, c, repairPeer, failedPeer) {
				directCount++
				continue
			}

			missingCount++
		}

		incomingChunkCount := directCount + (missingCount * n)

		return localCount, directCount, missingCount, incomingChunkCount
	}

	type IncomingOnlyRelocationEstimate struct {
		Shard api.Pin

		RepairPeer peer.ID
		FinalPeer  peer.ID
		Relocated  bool

		LocalChunkCount   int
		DirectChunkCount  int
		MissingChunkCount int

		IncomingChunkCount int

		RepairIncomingChunkCount     int
		RelocationIncomingChunkCount int

		ProcessingTime float64
		FinishTime     float64
	}

	func peerSetRelocationFast(peers []peer.ID) map[peer.ID]bool {
		out := make(map[peer.ID]bool)
		for _, p := range peers {
			if p != "" {
				out[p] = true
			}
		}
		return out
	}

func peerIncomingTimeRelocationFast(

	p peer.ID,
	chunks int,
	topology *NetworkTopology,
	chunkMB float64,

	) float64 {
		if chunks == 0 {
			return 0
		}

		in := topologyNodeIn(topology, p)
		if in == 0 {
			return math.Inf(1)
		}

		return float64(chunks) * chunkMB / float64(in)
	}

	func max2RelocationFast(a, b float64) float64 {
		if b > a {
			return b
		}
		return a
	}

func ScheduleGlobalMaxMinIncomingOnly_PrecomputedRelocationFast(

	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,

	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (peer.ID, []string, map[peer.ID]int, map[peer.ID][]string),

	) (map[peer.ID][]api.Pin, []IncomingOnlyRelocationEstimate) {
		fmt.Println("In FAST PRECOMPUTED INCOMING-ONLY MAX-MIN Repair Strategy with Relocation WITHOUT CurrentGlobalMax !!!")

		totalStart := time.Now()

		assignments := make(map[peer.ID][]api.Pin)
		estimates := make([]IncomingOnlyRelocationEstimate, 0)

		if len(failedShards) == 0 || len(candidatePeers) == 0 {
			fmt.Printf("[TOTAL] exited early in %v\n", time.Since(totalStart))
			return assignments, estimates
		}

		candidatePeers = sortedUniquePeers(candidatePeers)

		type ShardPrecompute struct {
			Shard           api.Pin
			ShardCIDs       []string
			ShardSize       int
			N               int
			SameStripePeers map[peer.ID]bool
			PeerMatchedCIDs map[peer.ID][]string
			Index           IncomingOnlyShardIndex
		}

		type CandidateCost struct {
			LocalChunkCount    int
			DirectChunkCount   int
			MissingChunkCount  int
			IncomingChunkCount int
			ProcessingTime     float64
		}

		precomputed := make(map[string]ShardPrecompute)

		start := time.Now()

		for _, shard := range failedShards {
			shardKey := shard.Cid.String()

			shardCIDs := cidListFromPin(shard)
			shardSize := len(shardCIDs)
			if shardSize == 0 {
				continue
			}

			_, sameStripePeers, n, shardLength := getSameStripe(shard)
			if shardLength > 0 {
				shardSize = shardLength
			}

			_, _, _, peerMatchedCIDs := getSimilarity(shard)
			index := buildIncomingOnlyShardIndex(peerMatchedCIDs)

			precomputed[shardKey] = ShardPrecompute{
				Shard:           shard,
				ShardCIDs:       shardCIDs,
				ShardSize:       shardSize,
				N:               n,
				SameStripePeers: peerSetRelocationFast(sameStripePeers),
				PeerMatchedCIDs: peerMatchedCIDs,
				Index:           index,
			}
		}

		fmt.Printf("[PHASE] precompute similarities + indexes took: %v\n", time.Since(start))

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

				if topologyNodeIn(topology, repairPeer) == 0 {
					continue
				}

				localCount, directCount, missingCount, incomingCount :=
					buildIncomingOnlyCountsFast(
						repairPeer,
						failedPeer,
						pc.ShardCIDs,
						pc.Index,
						pc.N,
					)

				processing := estimateIncomingOnlyProcessingTime(
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
					ProcessingTime:     processing,
				}
			}
		}

		fmt.Printf("[PHASE] precompute candidate costs took: %v\n", time.Since(start))

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
				bestRepairIncoming := 0
				bestRelocationIncoming := 0

				bestDestPeer := peer.ID("")
				bestDestTime := math.Inf(1)

				for _, finalPeer := range candidatePeers {
					if finalPeer == failedPeer {
						continue
					}

					if topologyNodeIn(topology, finalPeer) == 0 {
						continue
					}

					if pc.SameStripePeers[finalPeer] {
						continue
					}

					destTime := peerIncomingTimeRelocationFast(
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

					repairTime := peerIncomingTimeRelocationFast(
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
						completion = max2RelocationFast(
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

			estimates = append(estimates, IncomingOnlyRelocationEstimate{
				Shard:      chosenShard,
				RepairPeer: chosen.RepairPeer,

				FinalPeer: chosen.FinalPeer,
				Relocated: chosen.Relocated,

				LocalChunkCount:   chosen.LocalChunkCount,
				DirectChunkCount:  chosen.DirectChunkCount,
				MissingChunkCount: chosen.MissingChunkCount,

				IncomingChunkCount: chosen.RepairIncomingChunkCount,

				RepairIncomingChunkCount:     chosen.RepairIncomingChunkCount,
				RelocationIncomingChunkCount: chosen.RelocationIncomingChunkCount,

				ProcessingTime: chosen.ProcessingTime,
				FinishTime:     chosen.CompletionTime,
			})

			fmt.Printf(
				"FAST PRECOMPUTED INCOMING-ONLY MAX-MIN RELOCATION WITHOUT CURRENT GLOBAL MAX assigned shard=%s repairPeer=%s finalPeer=%s relocated=%v processing=%f finish=%f local=%d direct=%d missing=%d repairIncoming=%d relocationIncoming=%d\n",
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
			)

			unscheduled = append(
				unscheduled[:chosenIndex],
				unscheduled[chosenIndex+1:]...,
			)
		}

		fmt.Printf("[PHASE] scheduling loop took: %v\n", time.Since(start))
		fmt.Printf("[TOTAL] ScheduleGlobalMaxMinIncomingOnly_PrecomputedRelocationFast WITHOUT CURRENT GLOBAL MAX took: %v\n", time.Since(totalStart))

		return assignments, estimates
	}
*/
package ipfscluster

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

type ASCNetworkLoad struct {
	UploadMB    map[peer.ID]float64
	DownloadMB  map[peer.ID]float64
	DiskReadMB  map[peer.ID]float64
	DiskWriteMB map[peer.ID]float64
}

// Source == RepairPeer means local; otherwise it is a remote direct source.
type ASCCommonChunk struct {
	ChunkIndex int
	CID        string
	Source     peer.ID
}

// FinalPeer == RepairPeer means local placement; otherwise relocation.
type ASCRepairDecision struct {
	Shard        api.Pin
	RepairPeer   peer.ID
	Helpers      []peer.ID
	CommonChunks []ASCCommonChunk
	FinalPeer    peer.ID
}

type ascChunk struct {
	Index      int
	CID        string
	Sources    []peer.ID
	SourcePeer peer.ID
}

type ascTask struct {
	Shard api.Pin
	Key   string

	N       int
	ShardMB float64

	SameStripePeers  map[peer.ID]bool
	HelperCandidates []peer.ID

	Chunks         []ascChunk
	CommonIndexes  []int
	MissingIndexes []int
	Helpers        []peer.ID
}

type ascRepairCandidate struct {
	TaskIndex int

	RepairPeer peer.ID
	FinalPeer  peer.ID

	RepairIncomingMB     float64
	LocalUploadReduction float64
	CompletionTime       float64
}

func ascCleanCID(c string) string {
	c = strings.TrimSpace(c)
	c = strings.Trim(c, "<>")
	return c
}

func ascCIDList(pin api.Pin) []string {
	parts := strings.Split(pin.Metadata["Cids"], ",")
	out := make([]string, 0, len(parts))
	for _, raw := range parts {
		cid := ascCleanCID(raw)
		if cid != "" {
			out = append(out, cid)
		}
	}
	return out
}

func ascSortedUniquePeers(peers []peer.ID) []peer.ID {
	seen := make(map[peer.ID]bool, len(peers))
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

func ascPeerSet(peers []peer.ID) map[peer.ID]bool {
	out := make(map[peer.ID]bool, len(peers))
	for _, p := range peers {
		if p != "" {
			out[p] = true
		}
	}
	return out
}

func ascContainsPeer(peers []peer.ID, target peer.ID) bool {
	for _, p := range peers {
		if p == target {
			return true
		}
	}
	return false
}

func ascValidNode(topology *NetworkTopology, p peer.ID) bool {
	return topology != nil && topology.NodesByPeer != nil && topology.NodesByPeer[p] != nil
}

func ascNewLoadState(topology *NetworkTopology) *ASCNetworkLoad {
	loads := &ASCNetworkLoad{
		UploadMB:    make(map[peer.ID]float64),
		DownloadMB:  make(map[peer.ID]float64),
		DiskReadMB:  make(map[peer.ID]float64),
		DiskWriteMB: make(map[peer.ID]float64),
	}
	if topology != nil {
		for p := range topology.NodesByPeer {
			loads.UploadMB[p] = 0
			loads.DownloadMB[p] = 0
			loads.DiskReadMB[p] = 0
			loads.DiskWriteMB[p] = 0
		}
	}
	return loads
}

func ascOutMBps(topology *NetworkTopology, p peer.ID) float64 {
	if !ascValidNode(topology, p) {
		return 0
	}
	return float64(topology.NodesByPeer[p].GlobalOut) / 8.0
}

func ascInMBps(topology *NetworkTopology, p peer.ID) float64 {
	if !ascValidNode(topology, p) {
		return 0
	}
	return float64(topology.NodesByPeer[p].GlobalIn) / 8.0
}

func ascDiskReadMBps(topology *NetworkTopology, p peer.ID) float64 {
	if !ascValidNode(topology, p) {
		return 0
	}
	return float64(topology.NodesByPeer[p].DiskRead)
}

func ascDiskWriteMBps(topology *NetworkTopology, p peer.ID) float64 {
	if !ascValidNode(topology, p) {
		return 0
	}
	return float64(topology.NodesByPeer[p].DiskWrite)
}

func ascCompletion(loadMB, additionalMB, capacityMBps float64) float64 {
	if loadMB == 0 && additionalMB == 0 {
		return 0
	}
	if capacityMBps <= 0 {
		return math.Inf(1)
	}
	return (loadMB + additionalMB) / capacityMBps
}

func ascMax(values ...float64) float64 {
	m := 0.0
	for _, value := range values {
		if value > m {
			m = value
		}
	}
	return m
}

// Phase 1: classify chunks as common or missing.
func ascBuildTasks(
	failedPeer peer.ID,
	failedShards []api.Pin,
	topology *NetworkTopology,
	chunkMB float64,
	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (peer.ID, []string, map[peer.ID]int, map[peer.ID][]string),
) ([]*ascTask, error) {
	tasks := make([]*ascTask, 0, len(failedShards))

	for _, shard := range failedShards {
		cids := ascCIDList(shard)
		if len(cids) == 0 {
			continue
		}

		_, sameStripePeers, n, shardLength := getSameStripe(shard)
		if n <= 0 {
			return nil, fmt.Errorf("shard %s has invalid EC helper count n=%d", shard.Name, n)
		}

		shardChunkCount := len(cids)
		if shardLength > 0 {
			shardChunkCount = shardLength
		}

		_, _, _, peerMatchedCIDs := getSimilarity(shard)
		cidSources := make(map[string][]peer.ID)

		for p, matchedCIDs := range peerMatchedCIDs {
			if p == "" || p == failedPeer || !ascValidNode(topology, p) {
				continue
			}
			seenOnPeer := make(map[string]bool)
			for _, rawCID := range matchedCIDs {
				cid := ascCleanCID(rawCID)
				if cid == "" || seenOnPeer[cid] {
					continue
				}
				seenOnPeer[cid] = true
				cidSources[cid] = append(cidSources[cid], p)
			}
		}

		for cid := range cidSources {
			cidSources[cid] = ascSortedUniquePeers(cidSources[cid])
		}

		helperCandidates := make([]peer.ID, 0, len(sameStripePeers))
		for _, p := range ascSortedUniquePeers(sameStripePeers) {
			if p == failedPeer || !ascValidNode(topology, p) {
				continue
			}
			if ascOutMBps(topology, p) <= 0 || ascDiskReadMBps(topology, p) <= 0 {
				continue
			}
			helperCandidates = append(helperCandidates, p)
		}

		task := &ascTask{
			Shard:            shard,
			Key:              shard.Cid.String(),
			N:                n,
			ShardMB:          float64(shardChunkCount) * chunkMB,
			SameStripePeers:  ascPeerSet(sameStripePeers),
			HelperCandidates: helperCandidates,
			Chunks:           make([]ascChunk, 0, len(cids)),
			CommonIndexes:    make([]int, 0),
			MissingIndexes:   make([]int, 0),
			Helpers:          make([]peer.ID, 0, n),
		}

		for index, cid := range cids {
			chunk := ascChunk{Index: index, CID: cid, Sources: cidSources[cid]}
			if len(chunk.Sources) > 0 {
				task.CommonIndexes = append(task.CommonIndexes, index)
			} else {
				task.MissingIndexes = append(task.MissingIndexes, index)
			}
			task.Chunks = append(task.Chunks, chunk)
		}

		if len(task.MissingIndexes) > 0 && len(task.HelperCandidates) < task.N {
			return nil, fmt.Errorf(
				"shard %s needs %d helpers but only %d valid same-stripe helpers exist",
				shard.Name, task.N, len(task.HelperCandidates),
			)
		}

		tasks = append(tasks, task)
	}

	return tasks, nil
}

// Helper selection cost = disk-read completion time + upload completion time.
func ascHelperProjectedCost(
	helper peer.ID,
	additionalMB float64,
	topology *NetworkTopology,
	loads *ASCNetworkLoad,
) float64 {
	uploadTime := ascCompletion(loads.UploadMB[helper], additionalMB, ascOutMBps(topology, helper))
	readTime := ascCompletion(loads.DiskReadMB[helper], additionalMB, ascDiskReadMBps(topology, helper))
	return readTime + uploadTime
}

type ascHelperChoice struct {
	TaskIndex    int
	Helper       peer.ID
	BestCost     float64
	Sufferage    float64
	MissingCount int
}

// Phase 2: assign N balanced helpers to every task with missing chunks.
func ascAssignMissingHelpers(
	tasks []*ascTask,
	topology *NetworkTopology,
	loads *ASCNetworkLoad,
	chunkMB float64,
) error {
	for {
		maxMissing := -1
		active := false
		for _, task := range tasks {
			if len(task.MissingIndexes) == 0 || len(task.Helpers) >= task.N {
				continue
			}
			active = true
			if len(task.MissingIndexes) > maxMissing {
				maxMissing = len(task.MissingIndexes)
			}
		}
		if !active {
			return nil
		}

		var chosen *ascHelperChoice
		for taskIndex, task := range tasks {
			if len(task.MissingIndexes) != maxMissing || len(task.Helpers) >= task.N {
				continue
			}

			additionalMB := float64(len(task.MissingIndexes)) * chunkMB
			type helperCost struct {
				Peer peer.ID
				Cost float64
			}
			costs := make([]helperCost, 0, len(task.HelperCandidates))

			for _, helper := range task.HelperCandidates {
				if ascContainsPeer(task.Helpers, helper) {
					continue
				}
				cost := ascHelperProjectedCost(helper, additionalMB, topology, loads)
				if !math.IsInf(cost, 1) {
					costs = append(costs, helperCost{Peer: helper, Cost: cost})
				}
			}

			if len(costs) == 0 {
				return fmt.Errorf("cannot assign another helper for shard %s", task.Shard.Name)
			}

			sort.Slice(costs, func(i, j int) bool {
				if costs[i].Cost != costs[j].Cost {
					return costs[i].Cost < costs[j].Cost
				}
				return costs[i].Peer.String() < costs[j].Peer.String()
			})

			sufferage := math.Inf(1)
			if len(costs) > 1 {
				sufferage = costs[1].Cost - costs[0].Cost
			}

			candidate := &ascHelperChoice{
				TaskIndex:    taskIndex,
				Helper:       costs[0].Peer,
				BestCost:     costs[0].Cost,
				Sufferage:    sufferage,
				MissingCount: len(task.MissingIndexes),
			}

			if chosen == nil ||
				candidate.Sufferage > chosen.Sufferage ||
				(candidate.Sufferage == chosen.Sufferage && candidate.BestCost > chosen.BestCost) ||
				(candidate.Sufferage == chosen.Sufferage && candidate.BestCost == chosen.BestCost && task.Key < tasks[chosen.TaskIndex].Key) {
				chosen = candidate
			}
		}

		if chosen == nil {
			return fmt.Errorf("helper assignment reached an inconsistent state")
		}

		task := tasks[chosen.TaskIndex]
		additionalMB := float64(len(task.MissingIndexes)) * chunkMB
		task.Helpers = append(task.Helpers, chosen.Helper)
		loads.UploadMB[chosen.Helper] += additionalMB
		loads.DiskReadMB[chosen.Helper] += additionalMB
	}
}

func ascCommonSourceProjectedCost(
	source peer.ID,
	chunkMB float64,
	topology *NetworkTopology,
	loads *ASCNetworkLoad,
) float64 {
	uploadTime := ascCompletion(loads.UploadMB[source], chunkMB, ascOutMBps(topology, source))
	readTime := ascCompletion(loads.DiskReadMB[source], chunkMB, ascDiskReadMBps(topology, source))
	return readTime + uploadTime
}

func ascUnassignedCommonCount(task *ascTask) int {
	count := 0
	for _, chunkIndex := range task.CommonIndexes {
		if task.Chunks[chunkIndex].SourcePeer == "" {
			count++
		}
	}
	return count
}

type ascCommonSourceChoice struct {
	TaskIndex       int
	ChunkIndex      int
	Source          peer.ID
	BestCost        float64
	Sufferage       float64
	RemainingCommon int
}

// Phase 3: select one source for every common chunk without deciding locality.
func ascAssignCommonSources(
	tasks []*ascTask,
	topology *NetworkTopology,
	loads *ASCNetworkLoad,
	chunkMB float64,
) error {
	for {
		maxRemaining := 0
		for _, task := range tasks {
			if remaining := ascUnassignedCommonCount(task); remaining > maxRemaining {
				maxRemaining = remaining
			}
		}
		if maxRemaining == 0 {
			return nil
		}

		var chosen *ascCommonSourceChoice
		for taskIndex, task := range tasks {
			remaining := ascUnassignedCommonCount(task)
			if remaining != maxRemaining {
				continue
			}

			var taskChoice *ascCommonSourceChoice
			for _, chunkIndex := range task.CommonIndexes {
				chunk := &task.Chunks[chunkIndex]
				if chunk.SourcePeer != "" {
					continue
				}

				type sourceCost struct {
					Peer peer.ID
					Cost float64
				}
				costs := make([]sourceCost, 0, len(chunk.Sources))
				for _, source := range chunk.Sources {
					if ascOutMBps(topology, source) <= 0 || ascDiskReadMBps(topology, source) <= 0 {
						continue
					}
					cost := ascCommonSourceProjectedCost(source, chunkMB, topology, loads)
					if !math.IsInf(cost, 1) {
						costs = append(costs, sourceCost{Peer: source, Cost: cost})
					}
				}

				if len(costs) == 0 {
					return fmt.Errorf("common chunk %s of shard %s has no valid source", chunk.CID, task.Shard.Name)
				}

				sort.Slice(costs, func(i, j int) bool {
					if costs[i].Cost != costs[j].Cost {
						return costs[i].Cost < costs[j].Cost
					}
					return costs[i].Peer.String() < costs[j].Peer.String()
				})

				sufferage := math.Inf(1)
				if len(costs) > 1 {
					sufferage = costs[1].Cost - costs[0].Cost
				}

				candidate := &ascCommonSourceChoice{
					TaskIndex:       taskIndex,
					ChunkIndex:      chunkIndex,
					Source:          costs[0].Peer,
					BestCost:        costs[0].Cost,
					Sufferage:       sufferage,
					RemainingCommon: remaining,
				}

				if taskChoice == nil ||
					candidate.Sufferage > taskChoice.Sufferage ||
					(candidate.Sufferage == taskChoice.Sufferage && candidate.BestCost > taskChoice.BestCost) ||
					(candidate.Sufferage == taskChoice.Sufferage && candidate.BestCost == taskChoice.BestCost && candidate.ChunkIndex < taskChoice.ChunkIndex) {
					taskChoice = candidate
				}
			}

			if taskChoice == nil {
				continue
			}

			if chosen == nil ||
				taskChoice.Sufferage > chosen.Sufferage ||
				(taskChoice.Sufferage == chosen.Sufferage && taskChoice.BestCost > chosen.BestCost) ||
				(taskChoice.Sufferage == chosen.Sufferage && taskChoice.BestCost == chosen.BestCost && task.Key < tasks[chosen.TaskIndex].Key) {
				chosen = taskChoice
			}
		}

		if chosen == nil {
			return fmt.Errorf("common-source assignment reached an inconsistent state")
		}

		task := tasks[chosen.TaskIndex]
		task.Chunks[chosen.ChunkIndex].SourcePeer = chosen.Source
		loads.UploadMB[chosen.Source] += chunkMB
		loads.DiskReadMB[chosen.Source] += chunkMB
	}
}

func ascTaskNominalIncomingMB(task *ascTask, chunkMB float64) float64 {
	helperMB := float64(len(task.Helpers)*len(task.MissingIndexes)) * chunkMB
	commonMB := float64(len(task.CommonIndexes)) * chunkMB
	return helperMB + commonMB
}

// Traffic charged as upload that becomes local for candidate.
// Disk-read load remains because local data still has to be read.
func ascLocalUploadReduction(task *ascTask, candidate peer.ID, chunkMB float64) float64 {
	reduction := 0.0
	if ascContainsPeer(task.Helpers, candidate) {
		reduction += float64(len(task.MissingIndexes)) * chunkMB
	}
	for _, chunkIndex := range task.CommonIndexes {
		if task.Chunks[chunkIndex].SourcePeer == candidate {
			reduction += chunkMB
		}
	}
	return reduction
}

// Destination score is max(incoming completion, disk-write completion).
func ascBestRelocationDestination(
	task *ascTask,
	repairPeer peer.ID,
	failedPeer peer.ID,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	loads *ASCNetworkLoad,
) (peer.ID, float64) {
	bestPeer := peer.ID("")
	bestTime := math.Inf(1)

	for _, destination := range candidatePeers {
		if destination == "" || destination == failedPeer || destination == repairPeer {
			continue
		}
		if task.SameStripePeers[destination] {
			continue
		}
		if ascInMBps(topology, destination) <= 0 || ascDiskWriteMBps(topology, destination) <= 0 {
			continue
		}

		inTime := ascCompletion(loads.DownloadMB[destination], task.ShardMB, ascInMBps(topology, destination))
		writeTime := ascCompletion(loads.DiskWriteMB[destination], task.ShardMB, ascDiskWriteMBps(topology, destination))
		destinationTime := ascMax(inTime, writeTime)

		if destinationTime < bestTime ||
			(destinationTime == bestTime && (bestPeer == "" || destination.String() < bestPeer.String())) {
			bestPeer = destination
			bestTime = destinationTime
		}
	}

	return bestPeer, bestTime
}

func ascEvaluateRepairCandidate(
	taskIndex int,
	task *ascTask,
	repairPeer peer.ID,
	failedPeer peer.ID,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	loads *ASCNetworkLoad,
	chunkMB float64,
) (ascRepairCandidate, bool) {
	if repairPeer == "" || repairPeer == failedPeer || !ascValidNode(topology, repairPeer) {
		return ascRepairCandidate{}, false
	}
	if ascInMBps(topology, repairPeer) <= 0 || ascDiskWriteMBps(topology, repairPeer) <= 0 {
		return ascRepairCandidate{}, false
	}

	nominalIncomingMB := ascTaskNominalIncomingMB(task, chunkMB)
	localReduction := ascLocalUploadReduction(task, repairPeer, chunkMB)
	repairIncomingMB := nominalIncomingMB - localReduction
	if repairIncomingMB < 0 {
		repairIncomingMB = 0
	}

	repairDownloadTime := ascCompletion(
		loads.DownloadMB[repairPeer],
		repairIncomingMB,
		ascInMBps(topology, repairPeer),
	)
	if math.IsInf(repairDownloadTime, 1) {
		return ascRepairCandidate{}, false
	}

	localWriteTime := ascCompletion(
		loads.DiskWriteMB[repairPeer],
		task.ShardMB,
		ascDiskWriteMBps(topology, repairPeer),
	)
	localCompletion := ascMax(repairDownloadTime, localWriteTime)

	localPlacementEligible := !task.SameStripePeers[repairPeer]

	adjustedUploadLoad := loads.UploadMB[repairPeer] - localReduction
	if adjustedUploadLoad < 0 {
		adjustedUploadLoad = 0
	}
	relocationUploadTime := ascCompletion(
		adjustedUploadLoad,
		task.ShardMB,
		ascOutMBps(topology, repairPeer),
	)

	bestDestination, bestDestinationTime := ascBestRelocationDestination(
		task, repairPeer, failedPeer, candidatePeers, topology, loads,
	)

	if !localPlacementEligible {
		if bestDestination == "" || math.IsInf(relocationUploadTime, 1) {
			return ascRepairCandidate{}, false
		}
		return ascRepairCandidate{
			TaskIndex:            taskIndex,
			RepairPeer:           repairPeer,
			FinalPeer:            bestDestination,
			RepairIncomingMB:     repairIncomingMB,
			LocalUploadReduction: localReduction,
			CompletionTime:       ascMax(repairDownloadTime, relocationUploadTime, bestDestinationTime),
		}, true
	}

	// Optional relocation:
	// first compare repair-peer upload against local disk write;
	// then require the best destination's incoming/write side to also be faster.
	chooseRelocation := bestDestination != "" &&
		!math.IsInf(relocationUploadTime, 1) &&
		relocationUploadTime < localWriteTime &&
		bestDestinationTime < localWriteTime

	if chooseRelocation {
		return ascRepairCandidate{
			TaskIndex:            taskIndex,
			RepairPeer:           repairPeer,
			FinalPeer:            bestDestination,
			RepairIncomingMB:     repairIncomingMB,
			LocalUploadReduction: localReduction,
			CompletionTime:       ascMax(repairDownloadTime, relocationUploadTime, bestDestinationTime),
		}, true
	}

	return ascRepairCandidate{
		TaskIndex:            taskIndex,
		RepairPeer:           repairPeer,
		FinalPeer:            repairPeer,
		RepairIncomingMB:     repairIncomingMB,
		LocalUploadReduction: localReduction,
		CompletionTime:       localCompletion,
	}, true
}

func ascBestCandidateForTask(
	taskIndex int,
	task *ascTask,
	failedPeer peer.ID,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	loads *ASCNetworkLoad,
	chunkMB float64,
) (ascRepairCandidate, bool) {
	best := ascRepairCandidate{}
	found := false

	for _, repairPeer := range candidatePeers {
		candidate, ok := ascEvaluateRepairCandidate(
			taskIndex, task, repairPeer, failedPeer, candidatePeers, topology, loads, chunkMB,
		)
		if !ok {
			continue
		}
		if !found ||
			candidate.CompletionTime < best.CompletionTime ||
			(candidate.CompletionTime == best.CompletionTime && candidate.RepairPeer.String() < best.RepairPeer.String()) ||
			(candidate.CompletionTime == best.CompletionTime && candidate.RepairPeer == best.RepairPeer && candidate.FinalPeer.String() < best.FinalPeer.String()) {
			best = candidate
			found = true
		}
	}

	return best, found
}

func ascCommitRepairCandidate(task *ascTask, candidate ascRepairCandidate, loads *ASCNetworkLoad) {
	loads.UploadMB[candidate.RepairPeer] -= candidate.LocalUploadReduction
	if loads.UploadMB[candidate.RepairPeer] < 0 {
		loads.UploadMB[candidate.RepairPeer] = 0
	}

	loads.DownloadMB[candidate.RepairPeer] += candidate.RepairIncomingMB

	if candidate.FinalPeer == candidate.RepairPeer {
		loads.DiskWriteMB[candidate.RepairPeer] += task.ShardMB
		return
	}

	loads.UploadMB[candidate.RepairPeer] += task.ShardMB
	loads.DownloadMB[candidate.FinalPeer] += task.ShardMB
	loads.DiskWriteMB[candidate.FinalPeer] += task.ShardMB
}

func ascBuildDecision(task *ascTask, candidate ascRepairCandidate) ASCRepairDecision {
	commonChunks := make([]ASCCommonChunk, 0, len(task.CommonIndexes))
	for _, chunkIndex := range task.CommonIndexes {
		chunk := task.Chunks[chunkIndex]
		commonChunks = append(commonChunks, ASCCommonChunk{
			ChunkIndex: chunk.Index,
			CID:        chunk.CID,
			Source:     chunk.SourcePeer,
		})
	}
	sort.Slice(commonChunks, func(i, j int) bool {
		return commonChunks[i].ChunkIndex < commonChunks[j].ChunkIndex
	})

	return ASCRepairDecision{
		Shard:        task.Shard,
		RepairPeer:   candidate.RepairPeer,
		Helpers:      append([]peer.ID(nil), task.Helpers...),
		CommonChunks: commonChunks,
		FinalPeer:    candidate.FinalPeer,
	}
}

// ScheduleASCLEPIUSMultiResource returns only the selected repair decisions.
// For CommonChunks, Source == RepairPeer means local; otherwise remote direct.
// FinalPeer == RepairPeer means local final storage; otherwise relocation.
func ScheduleASCLEPIUSMultiResource(
	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,
	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (peer.ID, []string, map[peer.ID]int, map[peer.ID][]string),
) ([]ASCRepairDecision, error) {
	started := time.Now()
	decisions := make([]ASCRepairDecision, 0, len(failedShards))
	loads := ascNewLoadState(topology)

	if topology == nil {
		return decisions, fmt.Errorf("nil network topology")
	}
	if chunkMB <= 0 {
		return decisions, fmt.Errorf("chunkMB must be positive")
	}
	if len(failedShards) == 0 {
		return decisions, nil
	}

	candidatePeers = ascSortedUniquePeers(candidatePeers)
	filtered := make([]peer.ID, 0, len(candidatePeers))
	for _, p := range candidatePeers {
		if p != failedPeer && ascValidNode(topology, p) {
			filtered = append(filtered, p)
		}
	}
	candidatePeers = filtered
	if len(candidatePeers) == 0 {
		return decisions, fmt.Errorf("no valid candidate peers")
	}

	phase := time.Now()
	tasks, err := ascBuildTasks(
		failedPeer, failedShards, topology, chunkMB, getSameStripe, getSimilarity,
	)
	if err != nil {
		return decisions, err
	}
	fmt.Printf("[ASC-MR] common/missing classification took %v\n", time.Since(phase))

	phase = time.Now()
	if err := ascAssignMissingHelpers(tasks, topology, loads, chunkMB); err != nil {
		return decisions, err
	}
	fmt.Printf("[ASC-MR] missing-helper assignment took %v\n", time.Since(phase))

	phase = time.Now()
	if err := ascAssignCommonSources(tasks, topology, loads, chunkMB); err != nil {
		return decisions, err
	}
	fmt.Printf("[ASC-MR] common-source assignment took %v\n", time.Since(phase))

	unscheduled := make(map[int]bool, len(tasks))
	for taskIndex := range tasks {
		unscheduled[taskIndex] = true
	}

	phase = time.Now()
	for len(unscheduled) > 0 {
		bestByTask := make(map[int]ascRepairCandidate, len(unscheduled))
		for taskIndex := range unscheduled {
			candidate, ok := ascBestCandidateForTask(
				taskIndex, tasks[taskIndex], failedPeer, candidatePeers, topology, loads, chunkMB,
			)
			if ok {
				bestByTask[taskIndex] = candidate
			}
		}

		if len(bestByTask) == 0 {
			return decisions, fmt.Errorf(
				"no feasible repair/relocation assignment for %d remaining tasks",
				len(unscheduled),
			)
		}

		// Global Max-Min: schedule the task whose best option is currently worst.
		chosenTaskIndex := -1
		chosen := ascRepairCandidate{}
		for taskIndex, candidate := range bestByTask {
			if chosenTaskIndex == -1 ||
				candidate.CompletionTime > chosen.CompletionTime ||
				(candidate.CompletionTime == chosen.CompletionTime && tasks[taskIndex].Key < tasks[chosenTaskIndex].Key) {
				chosenTaskIndex = taskIndex
				chosen = candidate
			}
		}

		task := tasks[chosenTaskIndex]
		ascCommitRepairCandidate(task, chosen, loads)
		decision := ascBuildDecision(task, chosen)
		decisions = append(decisions, decision)
		delete(unscheduled, chosenTaskIndex)

		localCommon := 0
		for _, common := range decision.CommonChunks {
			if common.Source == decision.RepairPeer {
				localCommon++
			}
		}

		fmt.Printf(
			"[ASC-MR] shard=%s missing=%d helpers=%d common=%d localCommon=%d repair=%s final=%s relocated=%v incomingMB=%.3f finish=%.6f\n",
			task.Shard.Name,
			len(task.MissingIndexes),
			len(task.Helpers),
			len(task.CommonIndexes),
			localCommon,
			decision.RepairPeer.String(),
			decision.FinalPeer.String(),
			decision.FinalPeer != decision.RepairPeer,
			chosen.RepairIncomingMB,
			chosen.CompletionTime,
		)
	}

	fmt.Printf("[ASC-MR] Global Max-Min repair/relocation phase took %v\n", time.Since(phase))
	fmt.Printf("[ASC-MR] total scheduling time %v\n", time.Since(started))

	return decisions, nil
}
