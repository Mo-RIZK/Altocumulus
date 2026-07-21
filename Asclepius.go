package ipfscluster

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Erasure-coded heterogeneity- and duplication-aware Global Max-Min scheduler.
// The scheduler balances:
//   1. helper outgoing network load,
//   2. repair-peer incoming network load,
//   3. repair-peer outgoing load when relocation is selected,
//   4. final-peer incoming load when relocation is selected, and
//   5. final-peer disk-write load.
//
// CPU reconstruction time is intentionally ignored.

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

// IncomingOnlyRelocationEstimate records the resource-aware assignment selected by Global Max-Min.

// SelectedHelperShard identifies a selected same-stripe helper and the real
// global shard number stored by that helper.
//
// GlobalShardNumber is intentionally NOT converted to a zero-based
// Reed-Solomon position here. The repair model performs:
//
//	rsIndex := (GlobalShardNumber - 1) % (n + k)
//
// only when it builds the reconstruction array.
type SelectedHelperShard struct {
	Peer              peer.ID
	GlobalShardNumber int
}

type resourceAwareCandidateBest struct {
	Shard api.Pin

	RepairPeer peer.ID
	FinalPeer  peer.ID
	Relocated  bool

	LocalChunkCount   int
	DirectChunkCount  int
	MissingChunkCount int

	HelperTrafficMB map[peer.ID]float64

	// SelectedHelpers contains every peer contributing network traffic,
	// including direct-similarity sources.
	SelectedHelpers []peer.ID

	// SelectedHelperShards contains only selected helpers that store a
	// surviving shard from the failed shard's stripe. The global shard
	// number is returned unchanged; no modulo is applied by the scheduler.
	SelectedHelperShards []SelectedHelperShard

	RepairIncomingMB     float64
	RelocationOutgoingMB float64
	RelocationIncomingMB float64
	DiskWriteMB          float64

	DownloadTime   float64
	RelocationTime float64
	DiskWriteTime  float64
	ProcessingTime float64
	CompletionTime float64
}

type IncomingOnlyRelocationEstimate struct {
	Shard api.Pin

	RepairPeer peer.ID
	FinalPeer  peer.ID
	Relocated  bool

	LocalChunkCount   int
	DirectChunkCount  int
	MissingChunkCount int

	RepairIncomingMB     float64
	RelocationOutgoingMB float64
	RelocationIncomingMB float64
	DiskWriteMB          float64

	// HelperTrafficMB maps every selected network source to the amount it uploads.
	HelperTrafficMB map[peer.ID]float64

	// SelectedHelpers contains all peers that contribute network traffic,
	// including direct-similarity sources.
	SelectedHelpers []peer.ID

	// SelectedHelperShards contains selected same-stripe helpers together
	// with their real global shard numbers. The repair model applies modulo.
	SelectedHelperShards []SelectedHelperShard

	DownloadTime   float64
	RelocationTime float64
	DiskWriteTime  float64
	ProcessingTime float64
	FinishTime     float64
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

func topologyNodeIn(t *NetworkTopology, p peer.ID) uint64 {
	if t == nil || t.NodesByPeer == nil || t.NodesByPeer[p] == nil {
		return 0
	}
	return t.NodesByPeer[p].GlobalIn
}

func topologyNodeOut(t *NetworkTopology, p peer.ID) uint64 {
	if t == nil || t.NodesByPeer == nil || t.NodesByPeer[p] == nil {
		return 0
	}
	return t.NodesByPeer[p].GlobalOut
}

func topologyNodeDiskWrite(t *NetworkTopology, p peer.ID) uint64 {
	if t == nil || t.NodesByPeer == nil || t.NodesByPeer[p] == nil {
		return 0
	}
	return t.NodesByPeer[p].DiskWrite
}

// Network capacities are stored in Mbit/s. Loads are stored in MB, so divide
// the network capacity by 8 to obtain MB/s before calculating time.
func networkMbitToMBps(capacityMbit uint64) float64 {
	return float64(capacityMbit) / 8.0
}

func projectedNetworkTime(loadMB, addedMB float64, capacityMbit uint64) float64 {
	capacityMBps := networkMbitToMBps(capacityMbit)
	if capacityMBps <= 0 {
		return math.Inf(1)
	}
	return (loadMB + addedMB) / capacityMBps
}

func projectedDiskWriteTime(loadMB, addedMB float64, capacityMBps uint64) float64 {
	if capacityMBps == 0 {
		return math.Inf(1)
	}
	return (loadMB + addedMB) / float64(capacityMBps)
}

func maxFloat(values ...float64) float64 {
	m := 0.0
	for _, v := range values {
		if v > m {
			m = v
		}
	}
	return m
}

type ResourceAwareShardIndex struct {
	PeerCIDSet map[peer.ID]map[string]bool
	CIDSources map[string][]peer.ID
}

func buildResourceAwareShardIndex(
	peerMatchedCIDs map[peer.ID][]string,
) ResourceAwareShardIndex {
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

	return ResourceAwareShardIndex{
		PeerCIDSet: peerCIDSet,
		CIDSources: cidSources,
	}
}

func resourceAwarePeerHasCIDFast(
	index ResourceAwareShardIndex,
	p peer.ID,
	cidStr string,
) bool {
	return index.PeerCIDSet[p] != nil && index.PeerCIDSet[p][cidStr]
}

func peerSet(peers []peer.ID) map[peer.ID]bool {
	out := make(map[peer.ID]bool)
	for _, p := range peers {
		if p != "" {
			out[p] = true
		}
	}
	return out
}

func copyPeerFloatMap(src map[peer.ID]float64) map[peer.ID]float64 {
	dst := make(map[peer.ID]float64, len(src))
	for p, v := range src {
		dst[p] = v
	}
	return dst
}

func copySelectedHelperShards(src []SelectedHelperShard) []SelectedHelperShard {
	return append([]SelectedHelperShard(nil), src...)
}

// buildHelperGlobalShardNumbers maps each surviving same-stripe allocation to
// the real global shard number parsed from the shard name.
//
// No modulo is applied here. A peer is expected to store at most one shard
// from a given stripe.
func buildHelperGlobalShardNumbers(
	sameStripeShards []api.Pin,
	failedPeer peer.ID,
) (map[peer.ID]int, error) {
	result := make(map[peer.ID]int)

	for _, stripeShard := range sameStripeShards {
		globalShardNumber, _, err := getShardNumber(stripeShard.Name)
		if err != nil {
			return nil, fmt.Errorf(
				"cannot extract global shard number from %q: %w",
				stripeShard.Name,
				err,
			)
		}
		if globalShardNumber <= 0 {
			return nil, fmt.Errorf(
				"invalid global shard number %d parsed from %q",
				globalShardNumber,
				stripeShard.Name,
			)
		}

		for _, allocation := range stripeShard.Allocations {
			if allocation == "" || allocation == failedPeer {
				continue
			}

			oldNumber, exists := result[allocation]
			if exists && oldNumber != globalShardNumber {
				return nil, fmt.Errorf(
					"peer %s maps to multiple global shard numbers in one stripe: %d and %d",
					allocation.String(),
					oldNumber,
					globalShardNumber,
				)
			}

			result[allocation] = globalShardNumber
		}
	}

	return result, nil
}

// selectedSameStripeHelperShards keeps only selected helpers that have a
// concrete surviving shard in the failed shard's stripe.
func selectedSameStripeHelperShards(
	selectedHelpers []peer.ID,
	helperGlobalShardNumbers map[peer.ID]int,
) []SelectedHelperShard {
	out := make([]SelectedHelperShard, 0, len(selectedHelpers))

	for _, helper := range selectedHelpers {
		globalShardNumber, exists := helperGlobalShardNumbers[helper]
		if !exists {
			// This may be a direct-similarity source rather than a conventional
			// same-stripe reconstruction helper.
			continue
		}

		out = append(out, SelectedHelperShard{
			Peer:              helper,
			GlobalShardNumber: globalShardNumber,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Peer == out[j].Peer {
			return out[i].GlobalShardNumber < out[j].GlobalShardNumber
		}
		return out[i].Peer.String() < out[j].Peer.String()
	})

	return out
}

// chooseBestUploadSource selects the source whose projected outgoing completion
// time is smallest after adding one chunk. temporaryTrafficMB includes traffic
// already tentatively selected for the candidate currently being evaluated.
func chooseBestUploadSource(
	sources []peer.ID,
	repairPeer peer.ID,
	failedPeer peer.ID,
	chunkMB float64,
	peerOutgoingLoadMB map[peer.ID]float64,
	temporaryTrafficMB map[peer.ID]float64,
	topology *NetworkTopology,
) (peer.ID, bool) {
	best := peer.ID("")
	bestTime := math.Inf(1)

	for _, src := range sortedUniquePeers(sources) {
		if src == "" || src == failedPeer || src == repairPeer {
			continue
		}

		out := topologyNodeOut(topology, src)
		if out == 0 {
			continue
		}

		projected := projectedNetworkTime(
			peerOutgoingLoadMB[src],
			temporaryTrafficMB[src]+chunkMB,
			out,
		)

		if projected < bestTime ||
			(projected == bestTime && (best == "" || src.String() < best.String())) {
			best = src
			bestTime = projected
		}
	}

	return best, best != ""
}

// chooseFixedECHelpers selects one fixed set of n surviving same-stripe
// helpers for the entire failed shard.
//
// Every selected helper uploads one chunk for every failed-shard chunk that
// requires conventional EC reconstruction. Consequently, the added outgoing
// traffic for each selected helper is:
//
//	missingChunkCount * chunkMB
//
// temporaryTrafficMB contains direct-similarity traffic already selected for
// the candidate. It is included when evaluating the projected outgoing load.
func chooseFixedECHelpers(
	repairPeer peer.ID,
	failedPeer peer.ID,
	sameStripePeers []peer.ID,
	n int,
	missingChunkCount int,
	chunkMB float64,
	peerOutgoingLoadMB map[peer.ID]float64,
	temporaryTrafficMB map[peer.ID]float64,
	topology *NetworkTopology,
) ([]peer.ID, bool) {
	if n <= 0 {
		return nil, false
	}

	// No conventional EC reconstruction is required.
	if missingChunkCount == 0 {
		return nil, true
	}

	trafficPerHelperMB := float64(missingChunkCount) * chunkMB

	type helperCandidate struct {
		Peer          peer.ID
		ProjectedTime float64
	}

	candidates := make([]helperCandidate, 0, len(sameStripePeers))

	for _, helper := range sortedUniquePeers(sameStripePeers) {
		if helper == "" || helper == failedPeer || helper == repairPeer {
			continue
		}

		outCapacity := topologyNodeOut(topology, helper)
		if outCapacity == 0 {
			continue
		}

		projectedTime := projectedNetworkTime(
			peerOutgoingLoadMB[helper],
			temporaryTrafficMB[helper]+trafficPerHelperMB,
			outCapacity,
		)

		if math.IsInf(projectedTime, 1) {
			continue
		}

		candidates = append(candidates, helperCandidate{
			Peer:          helper,
			ProjectedTime: projectedTime,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ProjectedTime == candidates[j].ProjectedTime {
			return candidates[i].Peer.String() < candidates[j].Peer.String()
		}
		return candidates[i].ProjectedTime < candidates[j].ProjectedTime
	})

	if len(candidates) < n {
		return nil, false
	}

	selected := make([]peer.ID, n)
	for i := 0; i < n; i++ {
		selected[i] = candidates[i].Peer
	}

	return selected, true
}

// buildHelperTrafficForCandidate performs load-aware source selection for one
// (failed shard, repair peer) candidate.
//
// The source-selection model is:
//
//  1. Local chunk:
//     The repair peer already contains the failed chunk CID. No network
//     transfer is added.
//
//  2. Direct-similarity chunk:
//     One peer containing the exact CID uploads one chunk.
//
//  3. Missing chunk:
//     One fixed set of n surviving same-stripe helpers is selected once for
//     the entire failed shard. Every helper in that fixed set uploads one
//     chunk for every missing chunk position.
//
// selectedHelpers contains every peer that contributes network traffic.
// fixedECHelpers contains only the conventional same-stripe helpers that must
// be written to the repair metadata.
func buildHelperTrafficForCandidate(
	repairPeer peer.ID,
	failedPeer peer.ID,
	shardCIDs []string,
	n int,
	index ResourceAwareShardIndex,
	sameStripePeers []peer.ID,
	peerOutgoingLoadMB map[peer.ID]float64,
	topology *NetworkTopology,
	chunkMB float64,
) (
	localCount int,
	directCount int,
	missingCount int,
	helperTrafficMB map[peer.ID]float64,
	selectedHelpers []peer.ID,
	fixedECHelpers []peer.ID,
	ok bool,
) {
	helperTrafficMB = make(map[peer.ID]float64)

	// First classify the failed shard's chunks. Direct-similarity sources are
	// selected immediately and their temporary traffic is recorded.
	for _, rawCID := range shardCIDs {
		cidStr := cleanCIDString(rawCID)
		if cidStr == "" {
			continue
		}

		if resourceAwarePeerHasCIDFast(index, repairPeer, cidStr) {
			localCount++
			continue
		}

		if src, found := chooseBestUploadSource(
			index.CIDSources[cidStr],
			repairPeer,
			failedPeer,
			chunkMB,
			peerOutgoingLoadMB,
			helperTrafficMB,
			topology,
		); found {
			directCount++
			helperTrafficMB[src] += chunkMB
			continue
		}

		missingCount++
	}

	// Select exactly one conventional EC helper set for all missing chunks.
	var helpersOK bool
	fixedECHelpers, helpersOK = chooseFixedECHelpers(
		repairPeer,
		failedPeer,
		sameStripePeers,
		n,
		missingCount,
		chunkMB,
		peerOutgoingLoadMB,
		helperTrafficMB,
		topology,
	)
	if !helpersOK {
		return 0, 0, 0, nil, nil, nil, false
	}

	// Each fixed helper uploads one chunk for each missing chunk position.
	ecTrafficPerHelperMB := float64(missingCount) * chunkMB
	for _, helper := range fixedECHelpers {
		helperTrafficMB[helper] += ecTrafficPerHelperMB
	}

	for helper, trafficMB := range helperTrafficMB {
		if helper == "" || trafficMB <= 0 {
			continue
		}
		selectedHelpers = append(selectedHelpers, helper)
	}

	selectedHelpers = sortedUniquePeers(selectedHelpers)
	fixedECHelpers = sortedUniquePeers(fixedECHelpers)

	return localCount,
		directCount,
		missingCount,
		helperTrafficMB,
		selectedHelpers,
		fixedECHelpers,
		true
}

// estimateDownloadPhase returns the projected bottleneck between:
//   - the repair peer's incoming completion time, and
//   - the slowest selected helper's outgoing completion time.
func estimateDownloadPhase(
	repairPeer peer.ID,
	helperTrafficMB map[peer.ID]float64,
	peerIncomingLoadMB map[peer.ID]float64,
	peerOutgoingLoadMB map[peer.ID]float64,
	topology *NetworkTopology,
) (downloadTime float64, incomingMB float64, ok bool) {
	incomingMB = 0
	for _, mb := range helperTrafficMB {
		incomingMB += mb
	}

	if incomingMB == 0 {
		return 0, 0, true
	}

	repairIncomingTime := projectedNetworkTime(
		peerIncomingLoadMB[repairPeer],
		incomingMB,
		topologyNodeIn(topology, repairPeer),
	)
	if math.IsInf(repairIncomingTime, 1) {
		return math.Inf(1), incomingMB, false
	}

	slowestHelperTime := 0.0
	for helper, addedMB := range helperTrafficMB {
		helperTime := projectedNetworkTime(
			peerOutgoingLoadMB[helper],
			addedMB,
			topologyNodeOut(topology, helper),
		)
		if math.IsInf(helperTime, 1) {
			return math.Inf(1), incomingMB, false
		}
		if helperTime > slowestHelperTime {
			slowestHelperTime = helperTime
		}
	}

	return maxFloat(repairIncomingTime, slowestHelperTime), incomingMB, true
}

// ScheduleGlobalMaxMinIncomingOnly_PrecomputedRelocationFast keeps the old
// function name so existing call sites do not need to change. Its behavior is
// now fully resource-aware rather than incoming-only.
func ScheduleGlobalMaxMinIncomingOnly_PrecomputedRelocationFast(
	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,

	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (peer.ID, []string, map[peer.ID]int, map[peer.ID][]string),
) (map[peer.ID][]api.Pin, []IncomingOnlyRelocationEstimate) {
	fmt.Println("In RESOURCE-AWARE PRECOMPUTED GLOBAL MAX-MIN repair strategy")

	totalStart := time.Now()
	assignments := make(map[peer.ID][]api.Pin)
	estimates := make([]IncomingOnlyRelocationEstimate, 0)

	if len(failedShards) == 0 || len(candidatePeers) == 0 || topology == nil {
		fmt.Printf("[TOTAL] exited early in %v\n", time.Since(totalStart))
		return assignments, estimates
	}

	candidatePeers = sortedUniquePeers(candidatePeers)

	type ShardPrecompute struct {
		Shard api.Pin

		ShardCIDs []string
		ShardSize int
		N         int

		SameStripePeers         []peer.ID
		SameStripePeerSet       map[peer.ID]bool
		HelperGlobalShardNumber map[peer.ID]int
		Index                   ResourceAwareShardIndex
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

		sameStripeShards, sameStripePeers, n, shardLength := getSameStripe(shard)
		if shardLength > 0 {
			shardSize = shardLength
		}
		if n <= 0 {
			continue
		}

		helperGlobalShardNumbers, err :=
			buildHelperGlobalShardNumbers(sameStripeShards, failedPeer)
		if err != nil {
			fmt.Printf(
				"[WARN] skipping shard=%s because helper shard numbers could not be built: %v\n",
				shard.Name,
				err,
			)
			continue
		}

		_, _, _, peerMatchedCIDs := getSimilarity(shard)

		precomputed[shardKey] = ShardPrecompute{
			Shard:                   shard,
			ShardCIDs:               shardCIDs,
			ShardSize:               shardSize,
			N:                       n,
			SameStripePeers:         sortedUniquePeers(sameStripePeers),
			SameStripePeerSet:       peerSet(sameStripePeers),
			HelperGlobalShardNumber: helperGlobalShardNumbers,
			Index:                   buildResourceAwareShardIndex(peerMatchedCIDs),
		}
	}

	fmt.Printf("[PHASE] precompute similarities + indexes took: %v\n", time.Since(start))

	unscheduled := make([]api.Pin, 0, len(failedShards))
	for _, shard := range failedShards {
		if _, ok := precomputed[shard.Cid.String()]; ok {
			unscheduled = append(unscheduled, shard)
		}
	}

	// All loads are cumulative assigned MB.
	peerIncomingLoadMB := make(map[peer.ID]float64)
	peerOutgoingLoadMB := make(map[peer.ID]float64)
	peerDiskWriteLoadMB := make(map[peer.ID]float64)

	for _, p := range candidatePeers {
		peerIncomingLoadMB[p] = 0
		peerOutgoingLoadMB[p] = 0
		peerDiskWriteLoadMB[p] = 0
	}

	start = time.Now()

	for len(unscheduled) > 0 {
		bestForShard := make(map[string]resourceAwareCandidateBest)

		for _, shard := range unscheduled {
			shardKey := shard.Cid.String()
			pc := precomputed[shardKey]
			shardSizeMB := float64(pc.ShardSize) * chunkMB

			best := resourceAwareCandidateBest{
				Shard:          shard,
				CompletionTime: math.Inf(1),
			}

			for _, repairPeer := range candidatePeers {
				if repairPeer == failedPeer || topologyNodeIn(topology, repairPeer) == 0 {
					continue
				}

				localCount, directCount, missingCount,
					helperTrafficMB, selectedHelpers, fixedECHelpers, helpersOK :=
					buildHelperTrafficForCandidate(
						repairPeer,
						failedPeer,
						pc.ShardCIDs,
						pc.N,
						pc.Index,
						pc.SameStripePeers,
						peerOutgoingLoadMB,
						topology,
						chunkMB,
					)
				if !helpersOK {
					continue
				}

				// Build repair metadata only from the fixed conventional EC helper set.
				// Direct-similarity sources may contribute traffic, but they are not
				// complete helper shards and must not be inserted into helpers metadata.
				selectedHelperShards := selectedSameStripeHelperShards(
					fixedECHelpers,
					pc.HelperGlobalShardNumber,
				)

				if missingCount > 0 && len(selectedHelperShards) != pc.N {
					fmt.Printf(
						"[WARN] rejecting candidate shard=%s repairPeer=%s: "+
							"expected %d fixed EC helpers, got %d\n",
						shard.Name,
						repairPeer.String(),
						pc.N,
						len(selectedHelperShards),
					)
					continue
				}

				downloadTime, repairIncomingMB, downloadOK := estimateDownloadPhase(
					repairPeer,
					helperTrafficMB,
					peerIncomingLoadMB,
					peerOutgoingLoadMB,
					topology,
				)
				if !downloadOK {
					continue
				}

				// ------------------------------------------------------------
				// Option 1: store locally on the repair peer.
				// Allowed only if doing so does not violate the stripe rule.
				// ------------------------------------------------------------
				if !pc.SameStripePeerSet[repairPeer] {
					localWriteTime := projectedDiskWriteTime(
						peerDiskWriteLoadMB[repairPeer],
						shardSizeMB,
						topologyNodeDiskWrite(topology, repairPeer),
					)

					if !math.IsInf(localWriteTime, 1) {
						completion := maxFloat(downloadTime, localWriteTime)

						if betterResourceAwareCandidate(
							completion,
							repairPeer,
							repairPeer,
							best,
						) {
							best = resourceAwareCandidateBest{
								Shard: shard,

								RepairPeer: repairPeer,
								FinalPeer:  repairPeer,
								Relocated:  false,

								LocalChunkCount:   localCount,
								DirectChunkCount:  directCount,
								MissingChunkCount: missingCount,

								HelperTrafficMB:      copyPeerFloatMap(helperTrafficMB),
								SelectedHelpers:      append([]peer.ID(nil), selectedHelpers...),
								SelectedHelperShards: copySelectedHelperShards(selectedHelperShards),

								RepairIncomingMB:     repairIncomingMB,
								RelocationOutgoingMB: 0,
								RelocationIncomingMB: 0,
								DiskWriteMB:          shardSizeMB,

								DownloadTime:   downloadTime,
								RelocationTime: 0,
								DiskWriteTime:  localWriteTime,
								ProcessingTime: downloadTime,
								CompletionTime: completion,
							}
						}
					}
				}

				// ------------------------------------------------------------
				// Option 2: repair on repairPeer, then relocate to finalPeer.
				// Relocation contributes:
				//   - shardSizeMB to repairPeer outgoing load,
				//   - shardSizeMB to finalPeer incoming load, and
				//   - shardSizeMB to finalPeer disk-write load.
				// ------------------------------------------------------------
				for _, finalPeer := range candidatePeers {
					if finalPeer == failedPeer || finalPeer == repairPeer {
						continue
					}
					if pc.SameStripePeerSet[finalPeer] {
						continue
					}
					if topologyNodeOut(topology, repairPeer) == 0 ||
						topologyNodeIn(topology, finalPeer) == 0 ||
						topologyNodeDiskWrite(topology, finalPeer) == 0 {
						continue
					}

					relocationOutTime := projectedNetworkTime(
						peerOutgoingLoadMB[repairPeer],
						shardSizeMB,
						topologyNodeOut(topology, repairPeer),
					)
					relocationInTime := projectedNetworkTime(
						peerIncomingLoadMB[finalPeer],
						shardSizeMB,
						topologyNodeIn(topology, finalPeer),
					)
					finalWriteTime := projectedDiskWriteTime(
						peerDiskWriteLoadMB[finalPeer],
						shardSizeMB,
						topologyNodeDiskWrite(topology, finalPeer),
					)

					if math.IsInf(relocationOutTime, 1) ||
						math.IsInf(relocationInTime, 1) ||
						math.IsInf(finalWriteTime, 1) {
						continue
					}

					relocationTime := maxFloat(
						relocationOutTime,
						relocationInTime,
						finalWriteTime,
					)
					completion := maxFloat(downloadTime, relocationTime)

					if betterResourceAwareCandidate(
						completion,
						repairPeer,
						finalPeer,
						best,
					) {
						best = resourceAwareCandidateBest{
							Shard: shard,

							RepairPeer: repairPeer,
							FinalPeer:  finalPeer,
							Relocated:  true,

							LocalChunkCount:   localCount,
							DirectChunkCount:  directCount,
							MissingChunkCount: missingCount,

							HelperTrafficMB:      copyPeerFloatMap(helperTrafficMB),
							SelectedHelpers:      append([]peer.ID(nil), selectedHelpers...),
							SelectedHelperShards: copySelectedHelperShards(selectedHelperShards),

							RepairIncomingMB:     repairIncomingMB,
							RelocationOutgoingMB: shardSizeMB,
							RelocationIncomingMB: shardSizeMB,
							DiskWriteMB:          shardSizeMB,

							DownloadTime:   downloadTime,
							RelocationTime: relocationTime,
							DiskWriteTime:  finalWriteTime,
							ProcessingTime: downloadTime,
							CompletionTime: completion,
						}
					}
				}
			}

			if best.RepairPeer != "" && best.FinalPeer != "" &&
				!math.IsInf(best.CompletionTime, 1) {
				bestForShard[shardKey] = best
			}
		}

		if len(bestForShard) == 0 {
			fmt.Println("[WARN] no valid assignment remains for unscheduled shards")
			break
		}

		// Global Max-Min: choose the shard whose best available assignment has
		// the largest completion time.
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

		// Commit helper upload and repair download loads.
		for helper, mb := range chosen.HelperTrafficMB {
			peerOutgoingLoadMB[helper] += mb
		}
		peerIncomingLoadMB[chosen.RepairPeer] += chosen.RepairIncomingMB

		// Commit storage-related loads.
		if chosen.Relocated {
			peerOutgoingLoadMB[chosen.RepairPeer] += chosen.RelocationOutgoingMB
			peerIncomingLoadMB[chosen.FinalPeer] += chosen.RelocationIncomingMB
			peerDiskWriteLoadMB[chosen.FinalPeer] += chosen.DiskWriteMB
		} else {
			peerDiskWriteLoadMB[chosen.RepairPeer] += chosen.DiskWriteMB
		}

		assignments[chosen.FinalPeer] = append(assignments[chosen.FinalPeer], chosenShard)

		estimates = append(estimates, IncomingOnlyRelocationEstimate{
			Shard: chosenShard,

			RepairPeer: chosen.RepairPeer,
			FinalPeer:  chosen.FinalPeer,
			Relocated:  chosen.Relocated,

			LocalChunkCount:   chosen.LocalChunkCount,
			DirectChunkCount:  chosen.DirectChunkCount,
			MissingChunkCount: chosen.MissingChunkCount,

			RepairIncomingMB:     chosen.RepairIncomingMB,
			RelocationOutgoingMB: chosen.RelocationOutgoingMB,
			RelocationIncomingMB: chosen.RelocationIncomingMB,
			DiskWriteMB:          chosen.DiskWriteMB,

			HelperTrafficMB:      copyPeerFloatMap(chosen.HelperTrafficMB),
			SelectedHelpers:      append([]peer.ID(nil), chosen.SelectedHelpers...),
			SelectedHelperShards: copySelectedHelperShards(chosen.SelectedHelperShards),

			DownloadTime:   chosen.DownloadTime,
			RelocationTime: chosen.RelocationTime,
			DiskWriteTime:  chosen.DiskWriteTime,
			ProcessingTime: chosen.ProcessingTime,
			FinishTime:     chosen.CompletionTime,
		})

		fmt.Printf(
			"RESOURCE-AWARE GLOBAL MAX-MIN assigned shard=%s repairPeer=%s finalPeer=%s relocated=%v finish=%f download=%f relocation=%f diskWrite=%f local=%d direct=%d missing=%d repairInMB=%.3f relocationOutMB=%.3f relocationInMB=%.3f helpers=%d indexedStripeHelpers=%d\n",
			chosenShard.Name,
			chosen.RepairPeer.String(),
			chosen.FinalPeer.String(),
			chosen.Relocated,
			chosen.CompletionTime,
			chosen.DownloadTime,
			chosen.RelocationTime,
			chosen.DiskWriteTime,
			chosen.LocalChunkCount,
			chosen.DirectChunkCount,
			chosen.MissingChunkCount,
			chosen.RepairIncomingMB,
			chosen.RelocationOutgoingMB,
			chosen.RelocationIncomingMB,
			len(chosen.SelectedHelpers),
			len(chosen.SelectedHelperShards),
		)

		unscheduled = append(
			unscheduled[:chosenIndex],
			unscheduled[chosenIndex+1:]...,
		)
	}

	fmt.Printf("[PHASE] scheduling loop took: %v\n", time.Since(start))
	fmt.Printf("[TOTAL] resource-aware Global Max-Min took: %v\n", time.Since(totalStart))

	return assignments, estimates
}

// betterResourceAwareCandidate applies deterministic tie-breaking.
// Lower completion time is preferred, then repair-peer ID, then final-peer ID.
func betterResourceAwareCandidate(
	completion float64,
	repairPeer peer.ID,
	finalPeer peer.ID,
	current resourceAwareCandidateBest) bool {
	if completion < current.CompletionTime {
		return true
	}
	if completion > current.CompletionTime {
		return false
	}
	if current.RepairPeer == "" || repairPeer.String() < current.RepairPeer.String() {
		return true
	}
	if repairPeer.String() > current.RepairPeer.String() {
		return false
	}
	return current.FinalPeer == "" || finalPeer.String() < current.FinalPeer.String()
}

/*
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

//////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
