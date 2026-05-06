package ipfscluster

import (
	"fmt"
	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/libp2p/go-libp2p/core/peer"
	"math"
	"sort"
	"strings"
)

type Transfer struct {
	Src    peer.ID
	Dst    peer.ID
	SizeMB float64
	Kind   string
	CID    string
}

type RepairPlan struct {
	Shard api.Pin

	RepairPeer peer.ID

	Transfers  []Transfer
	FinishTime float64

	LocalChunkCount   int
	DirectChunkCount  int
	MissingChunkCount int

	OtherElementSources []peer.ID
	SelectedHelpers     []peer.ID

	RepairPeerLocalHelper bool
	NeededRemoteHelpers   int
}

type ResourceState struct {
	NodeOutEnd map[peer.ID]float64
	NodeInEnd  map[peer.ID]float64
	LinkEnd    map[peer.ID]map[peer.ID]float64
}

type MaxMinEstimate struct {
	Shard api.Pin

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

	Plan RepairPlan
}

type MaxMinAssignment struct {
	Shard    api.Pin
	Estimate MaxMinEstimate
}

func NewResourceState() *ResourceState {
	return &ResourceState{
		NodeOutEnd: make(map[peer.ID]float64),
		NodeInEnd:  make(map[peer.ID]float64),
		LinkEnd:    make(map[peer.ID]map[peer.ID]float64),
	}
}

func cloneResourceState(rs *ResourceState) *ResourceState {
	cp := NewResourceState()

	for p, v := range rs.NodeOutEnd {
		cp.NodeOutEnd[p] = v
	}

	for p, v := range rs.NodeInEnd {
		cp.NodeInEnd[p] = v
	}

	for src, dsts := range rs.LinkEnd {
		cp.LinkEnd[src] = make(map[peer.ID]float64)
		for dst, v := range dsts {
			cp.LinkEnd[src][dst] = v
		}
	}

	return cp
}

func getLinkEnd(rs *ResourceState, src, dst peer.ID) float64 {
	if rs.LinkEnd[src] == nil {
		return 0.0
	}
	return rs.LinkEnd[src][dst]
}

func setLinkEnd(rs *ResourceState, src, dst peer.ID, t float64) {
	if rs.LinkEnd[src] == nil {
		rs.LinkEnd[src] = make(map[peer.ID]float64)
	}
	rs.LinkEnd[src][dst] = t
}

func max3Float(a, b, c float64) float64 {
	m := a
	if b > m {
		m = b
	}
	if c > m {
		m = c
	}
	return m
}

func scheduleTransfer(
	rs *ResourceState,
	topology *NetworkTopology,
	tr Transfer,
) float64 {
	bw := topology.EffectiveBandwidth(tr.Src, tr.Dst)
	if bw == 0 {
		return math.Inf(1)
	}

	start := max3Float(
		rs.NodeOutEnd[tr.Src],
		rs.NodeInEnd[tr.Dst],
		getLinkEnd(rs, tr.Src, tr.Dst),
	)

	duration := tr.SizeMB / float64(bw)
	finish := start + duration

	rs.NodeOutEnd[tr.Src] = finish
	rs.NodeInEnd[tr.Dst] = finish
	setLinkEnd(rs, tr.Src, tr.Dst, finish)

	return finish
}

func simulatePlanFinish(
	rs *ResourceState,
	topology *NetworkTopology,
	plan RepairPlan,
) float64 {
	tmp := cloneResourceState(rs)

	finish := 0.0

	for _, tr := range plan.Transfers {
		f := scheduleTransfer(tmp, topology, tr)
		if math.IsInf(f, 1) {
			return math.Inf(1)
		}
		if f > finish {
			finish = f
		}
	}

	return finish
}

func commitPlan(
	rs *ResourceState,
	topology *NetworkTopology,
	plan RepairPlan,
) float64 {
	finish := 0.0

	for _, tr := range plan.Transfers {
		f := scheduleTransfer(rs, topology, tr)
		if math.IsInf(f, 1) {
			return math.Inf(1)
		}
		if f > finish {
			finish = f
		}
	}

	return finish
}

func buildUniqueMatches(peerMatchedCIDs map[peer.ID][]string) map[string]bool {
	unique := make(map[string]bool)

	for _, cids := range peerMatchedCIDs {
		for _, c := range cids {
			c = strings.TrimSpace(c)
			if c != "" {
				unique[c] = true
			}
		}
	}

	return unique
}

func cidListFromPin(pin api.Pin) []string {
	cidString := pin.Metadata["Cids"]
	parts := strings.Split(cidString, ",")

	out := make([]string, 0, len(parts))

	for _, c := range parts {
		c = strings.TrimSpace(c)
		if c != "" {
			out = append(out, c)
		}
	}

	return out
}

func peerHasCID(peerMatchedCIDs map[peer.ID][]string, p peer.ID, cidStr string) bool {
	for _, c := range peerMatchedCIDs[p] {
		if c == cidStr {
			return true
		}
	}
	return false
}

func selectBestHelpersMaxMin(
	helpers []peer.ID,
	repairPeer peer.ID,
	n int,
	topology *NetworkTopology,
	rs *ResourceState,
) []peer.ID {
	filtered := make([]peer.ID, 0)
	seen := make(map[peer.ID]bool)

	for _, h := range helpers {
		if h == repairPeer {
			continue
		}
		if seen[h] {
			continue
		}
		seen[h] = true
		filtered = append(filtered, h)
	}

	sort.Slice(filtered, func(i, j int) bool {
		ti := simulatePlanFinish(rs, topology, RepairPlan{
			Transfers: []Transfer{
				{
					Src:    filtered[i],
					Dst:    repairPeer,
					SizeMB: 1.0,
					Kind:   "helper-test",
				},
			},
		})

		tj := simulatePlanFinish(rs, topology, RepairPlan{
			Transfers: []Transfer{
				{
					Src:    filtered[j],
					Dst:    repairPeer,
					SizeMB: 1.0,
					Kind:   "helper-test",
				},
			},
		})

		if ti == tj {
			return filtered[i].String() < filtered[j].String()
		}

		return ti < tj
	})

	if len(filtered) > n {
		filtered = filtered[:n]
	}

	return filtered
}

func chooseBestSourceForCIDResourceAware(
	cidStr string,
	repairPeer peer.ID,
	failedPeer peer.ID,
	peerMatchedCIDs map[peer.ID][]string,
	topology *NetworkTopology,
	rs *ResourceState,
	chunkMB float64,
) peer.ID {
	bestPeer := peer.ID("")
	bestFinish := math.Inf(1)

	for p, cids := range peerMatchedCIDs {
		if p == failedPeer || p == repairPeer {
			continue
		}

		hasCID := false
		for _, c := range cids {
			if c == cidStr {
				hasCID = true
				break
			}
		}

		if !hasCID {
			continue
		}

		plan := RepairPlan{
			Transfers: []Transfer{
				{
					Src:    p,
					Dst:    repairPeer,
					SizeMB: chunkMB,
					Kind:   "direct-test",
					CID:    cidStr,
				},
			},
		}

		finish := simulatePlanFinish(rs, topology, plan)

		if bestPeer == "" ||
			finish < bestFinish ||
			(finish == bestFinish && p.String() < bestPeer.String()) {
			bestPeer = p
			bestFinish = finish
		}
	}

	return bestPeer
}

func buildBestRepairPlanForPeer(
	shard api.Pin,
	repairPeer peer.ID,
	failedPeer peer.ID,
	helperCandidates []peer.ID,
	n int,
	shardSize int,
	shardCIDs []string,
	peerMatches map[peer.ID]int,
	peerMatchedCIDs map[peer.ID][]string,
	topology *NetworkTopology,
	rs *ResourceState,
	chunkMB float64,
) (RepairPlan, bool) {
	localChunkCount := 0
	if peerMatches != nil {
		localChunkCount = peerMatches[repairPeer]
	}

	repairPeerLocalHelper := false
	remoteHelpers := make([]peer.ID, 0)
	seenHelpers := make(map[peer.ID]bool)

	for _, h := range helperCandidates {
		if h == failedPeer {
			continue
		}

		if seenHelpers[h] {
			continue
		}
		seenHelpers[h] = true

		if h == repairPeer {
			repairPeerLocalHelper = true
			continue
		}

		remoteHelpers = append(remoteHelpers, h)
	}

	neededRemoteHelpers := n
	if repairPeerLocalHelper {
		neededRemoteHelpers = n - 1
	}
	if neededRemoteHelpers < 0 {
		neededRemoteHelpers = 0
	}

	uniqueMatches := buildUniqueMatches(peerMatchedCIDs)

	// Variant 1: direct available chunks + reconstruct missing chunks.
	hybridTransfers := make([]Transfer, 0)
	hybridSources := make([]peer.ID, 0)
	hybridDirect := 0

	for _, c := range shardCIDs {
		if peerHasCID(peerMatchedCIDs, repairPeer, c) {
			continue
		}

		if !uniqueMatches[c] {
			continue
		}

		src := chooseBestSourceForCIDResourceAware(
			c,
			repairPeer,
			failedPeer,
			peerMatchedCIDs,
			topology,
			rs,
			chunkMB,
		)

		if src != "" {
			hybridDirect++
			hybridSources = append(hybridSources, src)

			hybridTransfers = append(hybridTransfers, Transfer{
				Src:    src,
				Dst:    repairPeer,
				SizeMB: chunkMB,
				Kind:   "direct",
				CID:    c,
			})
		}
	}

	hybridMissing := shardSize - localChunkCount - hybridDirect
	if hybridMissing < 0 {
		hybridMissing = 0
	}

	hybridHelpers := make([]peer.ID, 0)

	if hybridMissing > 0 {
		if len(remoteHelpers) >= neededRemoteHelpers {
			hybridHelpers = selectBestHelpersMaxMin(
				remoteHelpers,
				repairPeer,
				neededRemoteHelpers,
				topology,
				rs,
			)
		}

		if len(hybridHelpers) >= neededRemoteHelpers {
			for _, h := range hybridHelpers {
				hybridTransfers = append(hybridTransfers, Transfer{
					Src:    h,
					Dst:    repairPeer,
					SizeMB: float64(hybridMissing) * chunkMB,
					Kind:   "reconstruct",
				})
			}
		}
	}

	hybridPlan := RepairPlan{
		Shard:      shard,
		RepairPeer: repairPeer,

		Transfers: hybridTransfers,

		LocalChunkCount:   localChunkCount,
		DirectChunkCount:  hybridDirect,
		MissingChunkCount: hybridMissing,

		OtherElementSources: hybridSources,
		SelectedHelpers:     hybridHelpers,

		RepairPeerLocalHelper: repairPeerLocalHelper,
		NeededRemoteHelpers:   neededRemoteHelpers,
	}

	if hybridMissing > 0 && len(hybridHelpers) < neededRemoteHelpers {
		hybridPlan.FinishTime = math.Inf(1)
	} else {
		hybridPlan.FinishTime = simulatePlanFinish(rs, topology, hybridPlan)
	}

	// Variant 2: ignore direct chunks and reconstruct all non-local chunks.
	reconstructCount := shardSize - localChunkCount
	if reconstructCount < 0 {
		reconstructCount = 0
	}

	reconstructTransfers := make([]Transfer, 0)
	reconstructHelpers := make([]peer.ID, 0)

	if reconstructCount > 0 {
		if len(remoteHelpers) >= neededRemoteHelpers {
			reconstructHelpers = selectBestHelpersMaxMin(
				remoteHelpers,
				repairPeer,
				neededRemoteHelpers,
				topology,
				rs,
			)
		}

		if len(reconstructHelpers) >= neededRemoteHelpers {
			for _, h := range reconstructHelpers {
				reconstructTransfers = append(reconstructTransfers, Transfer{
					Src:    h,
					Dst:    repairPeer,
					SizeMB: float64(reconstructCount) * chunkMB,
					Kind:   "reconstruct-all",
				})
			}
		}
	}

	reconstructOnlyPlan := RepairPlan{
		Shard:      shard,
		RepairPeer: repairPeer,

		Transfers: reconstructTransfers,

		LocalChunkCount:   localChunkCount,
		DirectChunkCount:  0,
		MissingChunkCount: reconstructCount,

		OtherElementSources: nil,
		SelectedHelpers:     reconstructHelpers,

		RepairPeerLocalHelper: repairPeerLocalHelper,
		NeededRemoteHelpers:   neededRemoteHelpers,
	}

	if reconstructCount == 0 {
		reconstructOnlyPlan.FinishTime = 0.0
	} else if len(reconstructHelpers) >= neededRemoteHelpers {
		reconstructOnlyPlan.FinishTime = simulatePlanFinish(rs, topology, reconstructOnlyPlan)
	} else {
		reconstructOnlyPlan.FinishTime = math.Inf(1)
	}

	if hybridPlan.FinishTime <= reconstructOnlyPlan.FinishTime {
		return hybridPlan, !math.IsInf(hybridPlan.FinishTime, 1)
	}

	return reconstructOnlyPlan, !math.IsInf(reconstructOnlyPlan.FinishTime, 1)
}

func ScheduleGlobalMaxMin(
	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,

	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (peer.ID, []string, map[peer.ID]int, map[peer.ID][]string),
) (map[peer.ID][]api.Pin, []MaxMinAssignment) {
	fmt.Println("In RESOURCE-AWARE Go Global Max-Min Repair Strategy !!!")

	assignments := make(map[peer.ID][]api.Pin)
	assignedPairs := make([]MaxMinAssignment, 0)

	if len(failedShards) == 0 || len(candidatePeers) == 0 {
		return assignments, assignedPairs
	}

	type ShardPrecompute struct {
		Shard api.Pin

		ShardCIDs []string
		ShardSize int

		HelperCandidates []peer.ID
		N                int

		PeerMatches     map[peer.ID]int
		PeerMatchedCIDs map[peer.ID][]string
	}

	precomputed := make(map[string]ShardPrecompute)

	// PHASE 0: precompute similarities once. No RPC during scheduling.
	for _, shard := range failedShards {
		shardKey := shard.Cid.String()

		shardCIDs := cidListFromPin(shard)
		shardSize := len(shardCIDs)

		if shardSize == 0 {
			continue
		}

		_, helperCandidates, n, shardLength := getSameStripe(shard)
		if shardLength > 0 {
			shardSize = shardLength
		}
		fmt.Printf(
			"DEBUG SAME STRIPE shard=%s helperCandidates=%d n=%d shardLength=%d helpers=%v\n",
			shard.Name,
			len(helperCandidates),
			n,
			shardLength,
			helperCandidates,
		)

		_, _, peerMatches, peerMatchedCIDs := getSimilarity(shard)

		precomputed[shardKey] = ShardPrecompute{
			Shard: shard,

			ShardCIDs: shardCIDs,
			ShardSize: shardSize,

			HelperCandidates: helperCandidates,
			N:                n,

			PeerMatches:     peerMatches,
			PeerMatchedCIDs: peerMatchedCIDs,
		}
	}

	unscheduled := make([]api.Pin, 0)

	for _, shard := range failedShards {
		if _, ok := precomputed[shard.Cid.String()]; ok {
			unscheduled = append(unscheduled, shard)
		}
	}

	if len(unscheduled) == 0 {
		return assignments, assignedPairs
	}

	resourceState := NewResourceState()

	for len(unscheduled) > 0 {
		type taskBestInfo struct {
			Completion float64
			Peer       peer.ID
			Plan       RepairPlan
			Estimate   MaxMinEstimate
		}

		taskBest := make(map[string]taskBestInfo)

		for _, shard := range unscheduled {
			shardKey := shard.Cid.String()
			pc := precomputed[shardKey]

			bestPeer := peer.ID("")
			bestCompletion := math.Inf(1)
			var bestPlan RepairPlan

			for _, repairPeer := range candidatePeers {
				if repairPeer == failedPeer {
					continue
				}

				plan, ok := buildBestRepairPlanForPeer(
					pc.Shard,
					repairPeer,
					failedPeer,
					pc.HelperCandidates,
					pc.N,
					pc.ShardSize,
					pc.ShardCIDs,
					pc.PeerMatches,
					pc.PeerMatchedCIDs,
					topology,
					resourceState,
					chunkMB,
				)

				if !ok {
					continue
				}

				completionTime := plan.FinishTime

				if bestPeer == "" ||
					completionTime < bestCompletion ||
					(completionTime == bestCompletion && repairPeer.String() < bestPeer.String()) {
					bestPeer = repairPeer
					bestCompletion = completionTime
					bestPlan = plan
				}
			}

			if bestPeer != "" {
				estimate := MaxMinEstimate{
					Shard: pc.Shard,

					RepairPeer: bestPeer,

					ProcessingTime: bestPlan.FinishTime,
					FinishTime:     bestPlan.FinishTime,

					ShardSize: pc.ShardSize,

					LocalChunkCount:   bestPlan.LocalChunkCount,
					DirectChunkCount:  bestPlan.DirectChunkCount,
					MissingChunkCount: bestPlan.MissingChunkCount,

					OtherElementSources: bestPlan.OtherElementSources,
					SelectedHelpers:     bestPlan.SelectedHelpers,

					RepairPeerLocalHelper: bestPlan.RepairPeerLocalHelper,
					NeededRemoteHelpers:   bestPlan.NeededRemoteHelpers,

					Plan: bestPlan,
				}

				taskBest[shardKey] = taskBestInfo{
					Completion: bestCompletion,
					Peer:       bestPeer,
					Plan:       bestPlan,
					Estimate:   estimate,
				}
			}
		}

		if len(taskBest) == 0 {
			break
		}

		chosenIndex := -1
		chosenCompletion := -1.0
		chosenShardSize := 0
		chosenShardKey := ""

		for idx, shard := range unscheduled {
			shardKey := shard.Cid.String()

			info, ok := taskBest[shardKey]
			if !ok {
				continue
			}

			shardSize := info.Estimate.ShardSize

			if chosenIndex == -1 ||
				info.Completion > chosenCompletion ||
				(info.Completion == chosenCompletion && shardSize < chosenShardSize) ||
				(info.Completion == chosenCompletion && shardSize == chosenShardSize && shardKey < chosenShardKey) {
				chosenIndex = idx
				chosenCompletion = info.Completion
				chosenShardSize = shardSize
				chosenShardKey = shardKey
			}
		}

		if chosenIndex == -1 {
			break
		}

		chosenShard := unscheduled[chosenIndex]
		chosenInfo := taskBest[chosenShard.Cid.String()]
		chosenPlan := chosenInfo.Plan
		chosenEstimate := chosenInfo.Estimate
		repairPeer := chosenInfo.Peer

		actualFinish := commitPlan(resourceState, topology, chosenPlan)
		chosenEstimate.FinishTime = actualFinish
		chosenEstimate.ProcessingTime = actualFinish

		assignments[repairPeer] = append(assignments[repairPeer], chosenShard)

		assignedPairs = append(assignedPairs, MaxMinAssignment{
			Shard:    chosenShard,
			Estimate: chosenEstimate,
		})

		fmt.Printf(
			"RESOURCE-AWARE MAX-MIN assigned shard %s -> peer %s | finish=%f | local=%d direct=%d missing=%d helpers=%d localHelper=%v neededRemote=%d transfers=%d\n",
			chosenShard.Name,
			repairPeer.String(),
			actualFinish,
			chosenEstimate.LocalChunkCount,
			chosenEstimate.DirectChunkCount,
			chosenEstimate.MissingChunkCount,
			len(chosenEstimate.SelectedHelpers),
			chosenEstimate.RepairPeerLocalHelper,
			chosenEstimate.NeededRemoteHelpers,
			len(chosenPlan.Transfers),
		)

		unscheduled = append(
			unscheduled[:chosenIndex],
			unscheduled[chosenIndex+1:]...,
		)
	}

	return assignments, assignedPairs
}
