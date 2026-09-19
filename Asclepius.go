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

// ASCNetworkLoad tracks committed network traffic.
// Common/direct duplicate traffic is intentionally NOT charged to source uploads.
// UploadMB therefore models:
//  1. EC helper uploads for missing chunks;
//  2. relocation upload from RepairPeer to FinalPeer.
//
// DownloadMB models:
//  1. repair incoming traffic;
//  2. relocation incoming traffic at FinalPeer.
type ASCNetworkLoad struct {
	UploadMB   map[peer.ID]float64
	DownloadMB map[peer.ID]float64
}

// ASCCommonChunk is kept for compatibility with the existing repair decision.
//
// Source == RepairPeer means the common chunk is already local on RepairPeer.
// Source == "" means the chunk is a remote reusable duplicate.
//
// The scheduler intentionally does NOT select or load-balance the remote source
// of a common chunk.
type ASCCommonChunk struct {
	ChunkIndex int
	CID        string
	Source     peer.ID
}

// FinalPeer == RepairPeer means the repaired shard remains on RepairPeer.
// Otherwise, the repaired shard is relocated from RepairPeer to FinalPeer.
type ASCRepairDecision struct {
	Shard        api.Pin
	RepairPeer   peer.ID
	Helpers      []peer.ID
	CommonChunks []ASCCommonChunk
	FinalPeer    peer.ID
}

type ascChunk struct {
	Index   int
	CID     string
	Sources []peer.ID
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

	// StaticCandidate is computed ONCE before Global Max-Min starts.
	// It contains all information that depends only on immutable chunk
	// placement / stripe placement for a given candidate RepairPeer.
	StaticCandidate map[peer.ID]ascStaticRepairCandidate
}

// ascStaticRepairCandidate contains immutable information for one
// (shard, RepairPeer) pair.  Nothing in this structure depends on the
// accumulated UploadMB/DownloadMB loads, so it must never be rebuilt in the
// Global Max-Min loop.
type ascStaticRepairCandidate struct {
	CommonChunks      []ASCCommonChunk
	LocalCommonCount  int
	RemoteCommonCount int
	RemoteCommonMB    float64

	NeedsRelocation   bool
	ValidDestinations []peer.ID
}

// ascShardHelpers is selected ONCE for one shard in one Global Max-Min
// iteration. The same helper set is then used while evaluating every possible
// RepairPeer for that shard.
type ascShardHelpers struct {
	Helpers  []peer.ID
	HelperMB float64

	// HelperUploadTime is computed ONCE after helper selection for the current
	// shard/Global-Max-Min iteration.  Candidate RepairPeers reuse it instead
	// of recalculating the same helper completion times again and again.
	//
	// MaxUploadTime is the bottleneck when RepairPeer is not a helper.
	// MaxUploadTimeWithout[h] is the bottleneck when helper h is the
	// RepairPeer and therefore its reconstruction contribution is local.
	MaxUploadTime        float64
	MaxUploadTimeWithout map[peer.ID]float64
	HelperSet            map[peer.ID]bool
}

// ascRepairCandidate is the complete plan for one fixed-helper shard and one
// candidate repair peer.
type ascRepairCandidate struct {
	TaskIndex int

	RepairPeer peer.ID
	FinalPeer  peer.ID

	Helpers      []peer.ID
	CommonChunks []ASCCommonChunk

	// Helpers is the fixed helper set selected once for this shard in the
	// current Global Max-Min iteration.  It is shared by all RepairPeer
	// candidates; we do not allocate/copy it per candidate.

	HelperMB         float64
	RepairIncomingMB float64
	CompletionTime   float64
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
	return topology != nil &&
		topology.NodesByPeer != nil &&
		topology.NodesByPeer[p] != nil
}

func ascNewLoadState(topology *NetworkTopology) *ASCNetworkLoad {
	loads := &ASCNetworkLoad{
		UploadMB:   make(map[peer.ID]float64),
		DownloadMB: make(map[peer.ID]float64),
	}

	if topology != nil {
		for p := range topology.NodesByPeer {
			loads.UploadMB[p] = 0
			loads.DownloadMB[p] = 0
		}
	}

	return loads
}

// NetworkTopology stores bandwidth in Mbit/s. The scheduler uses MB/s.
func ascOutMBps(topology *NetworkTopology, p peer.ID) float64 {
	if !ascValidNode(topology, p) {
		return 0
	}

	return float64(topology.NodesByPeer[p].GlobalOut) / 8.0
}

// NetworkTopology stores bandwidth in Mbit/s. The scheduler uses MB/s.
func ascInMBps(topology *NetworkTopology, p peer.ID) float64 {
	if !ascValidNode(topology, p) {
		return 0
	}

	return float64(topology.NodesByPeer[p].GlobalIn) / 8.0
}

// ascCompletion computes:
//
//	(current assigned traffic + new traffic) / bandwidth.
func ascCompletion(
	loadMB float64,
	additionalMB float64,
	capacityMBps float64,
) float64 {
	if loadMB == 0 && additionalMB == 0 {
		return 0
	}

	if capacityMBps <= 0 {
		return math.Inf(1)
	}

	return (loadMB + additionalMB) / capacityMBps
}

func ascMax(values ...float64) float64 {
	maximum := 0.0

	for _, value := range values {
		if value > maximum {
			maximum = value
		}
	}

	return maximum
}

// ascIncomingOnlyShardIndex is the same indexed representation used by the
// old incoming-only ASCLEPIUS preprocessing.  It is built once per failed
// shard from getSimilarity() and then reused for all CID classification.
type ascIncomingOnlyShardIndex struct {
	PeerCIDSet map[peer.ID]map[string]bool
	CIDSources map[string][]peer.ID
}

func ascBuildIncomingOnlyShardIndex(
	failedPeer peer.ID,
	topology *NetworkTopology,
	peerMatchedCIDs map[peer.ID][]string,
) ascIncomingOnlyShardIndex {
	peerCIDSet := make(map[peer.ID]map[string]bool, len(peerMatchedCIDs))
	cidSources := make(map[string][]peer.ID)

	for p, matchedCIDs := range peerMatchedCIDs {
		if p == "" || p == failedPeer || !ascValidNode(topology, p) {
			continue
		}

		set := make(map[string]bool, len(matchedCIDs))
		peerCIDSet[p] = set

		for _, rawCID := range matchedCIDs {
			cid := ascCleanCID(rawCID)
			if cid == "" || set[cid] {
				continue
			}

			set[cid] = true
			cidSources[cid] = append(cidSources[cid], p)
		}
	}

	for cid := range cidSources {
		cidSources[cid] = ascSortedUniquePeers(cidSources[cid])
	}

	return ascIncomingOnlyShardIndex{
		PeerCIDSet: peerCIDSet,
		CIDSources: cidSources,
	}
}

// Phase 1 uses the same preprocessing pattern as the old incoming-only code:
//  1. get stripe metadata once per failed shard;
//  2. get similarity once per failed shard;
//  3. immediately build a CID -> surviving-source index;
//  4. classify the shard's CIDs by O(1) index lookup.
//
// No similarity/CID-source scan is repeated in Global Max-Min.
func ascBuildTasks(
	failedPeer peer.ID,
	failedShards []api.Pin,
	topology *NetworkTopology,
	chunkMB float64,
	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (
		peer.ID,
		[]string,
		map[peer.ID]int,
		map[peer.ID][]string,
	),
) ([]*ascTask, error) {
	tasks := make([]*ascTask, 0, len(failedShards))

	for _, shard := range failedShards {
		cids := ascCIDList(shard)
		if len(cids) == 0 {
			continue
		}

		// Same as the old preprocessing: stripe metadata is fetched once.
		_, sameStripePeers, n, shardLength := getSameStripe(shard)
		if n <= 0 {
			return nil, fmt.Errorf(
				"shard %s has invalid EC helper count n=%d",
				shard.Name,
				n,
			)
		}

		shardChunkCount := len(cids)
		if shardLength > 0 {
			shardChunkCount = shardLength
		}

		// Same as the old preprocessing: similarity is fetched exactly once for
		// this failed shard, then converted immediately into an indexed form.
		_, _, _, peerMatchedCIDs := getSimilarity(shard)
		index := ascBuildIncomingOnlyShardIndex(
			failedPeer,
			topology,
			peerMatchedCIDs,
		)

		helperCandidates := make([]peer.ID, 0, len(sameStripePeers))
		for _, p := range ascSortedUniquePeers(sameStripePeers) {
			if p == failedPeer ||
				!ascValidNode(topology, p) ||
				ascOutMBps(topology, p) <= 0 {
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
			StaticCandidate:  make(map[peer.ID]ascStaticRepairCandidate),
		}

		// Classification is now only indexed lookups.  CIDSources already contains
		// only valid surviving peers, so a non-empty entry means the chunk is common.
		for chunkIndex, rawCID := range cids {
			cid := ascCleanCID(rawCID)
			sources := index.CIDSources[cid]

			chunk := ascChunk{
				Index:   chunkIndex,
				CID:     cid,
				Sources: sources,
			}

			if len(sources) > 0 {
				task.CommonIndexes = append(task.CommonIndexes, chunkIndex)
			} else {
				task.MissingIndexes = append(task.MissingIndexes, chunkIndex)
			}

			task.Chunks = append(task.Chunks, chunk)
		}

		if len(task.MissingIndexes) > 0 &&
			len(task.HelperCandidates) < task.N {
			return nil, fmt.Errorf(
				"shard %s needs %d helpers but only %d valid helpers exist",
				shard.Name,
				task.N,
				len(task.HelperCandidates),
			)
		}

		tasks = append(tasks, task)
	}

	return tasks, nil
}

// Select the N EC helpers ONCE for this shard in the CURRENT Global Max-Min
// iteration.
//
// Helper choice depends only on the currently committed upload loads and each
// helper's outgoing bandwidth. It does NOT depend on the candidate RepairPeer.
//
// Every selected helper is provisionally charged:
//
//	|MissingIndexes| * chunkMB
//
// while selecting the next helper, so the N selected helpers are themselves
// load-aware.
//
// The helper set is then FIXED while all RepairPeer candidates for this shard
// are evaluated.
func ascSelectHelpersForShard(
	task *ascTask,
	topology *NetworkTopology,
	loads *ASCNetworkLoad,
	chunkMB float64,
) (ascShardHelpers, error) {
	if len(task.MissingIndexes) == 0 {
		return ascShardHelpers{
			Helpers:              nil,
			HelperMB:             0,
			MaxUploadTime:        0,
			MaxUploadTimeWithout: nil,
			HelperSet:            nil,
		}, nil
	}

	helperMB := float64(len(task.MissingIndexes)) * chunkMB

	// The provisional charge previously stored in temporaryUpload does not
	// change the cost of any still-unselected helper: once a helper is chosen,
	// it is excluded from the following choices.  Therefore helper selection
	// is exactly equivalent to computing every candidate's projected cost ONCE,
	// sorting by (cost, peer ID), and taking the first N.
	type helperScore struct {
		Peer peer.ID
		Time float64
	}

	scores := make([]helperScore, 0, len(task.HelperCandidates))
	for _, helper := range task.HelperCandidates {
		projected := ascCompletion(
			loads.UploadMB[helper],
			helperMB,
			ascOutMBps(topology, helper),
		)
		if math.IsInf(projected, 1) {
			continue
		}
		scores = append(scores, helperScore{Peer: helper, Time: projected})
	}

	sort.Slice(scores, func(i, j int) bool {
		if scores[i].Time != scores[j].Time {
			return scores[i].Time < scores[j].Time
		}
		return scores[i].Peer.String() < scores[j].Peer.String()
	})

	if len(scores) < task.N {
		return ascShardHelpers{}, fmt.Errorf(
			"cannot assign %d helpers for shard %s; only %d valid helpers exist",
			task.N,
			task.Shard.Name,
			len(scores),
		)
	}

	helpers := make([]peer.ID, task.N)
	helperTimes := make([]float64, task.N)
	helperSet := make(map[peer.ID]bool, task.N)
	maxUploadTime := 0.0

	for i := 0; i < task.N; i++ {
		helpers[i] = scores[i].Peer
		helperTimes[i] = scores[i].Time
		helperSet[scores[i].Peer] = true
		if scores[i].Time > maxUploadTime {
			maxUploadTime = scores[i].Time
		}
	}

	// Precompute the helper-upload bottleneck for the only special case that
	// varies with RepairPeer: RepairPeer itself is one of the fixed helpers,
	// so that helper's transfer is local and must be excluded.
	// N is small, so this O(N^2) work is done once per shard evaluation and
	// replaces an O(N) helper scan for every RepairPeer candidate.
	maxWithout := make(map[peer.ID]float64, task.N)
	for excludedIndex, excludedPeer := range helpers {
		maximum := 0.0
		for i, value := range helperTimes {
			if i == excludedIndex {
				continue
			}
			if value > maximum {
				maximum = value
			}
		}
		maxWithout[excludedPeer] = maximum
	}

	return ascShardHelpers{
		Helpers:              helpers,
		HelperMB:             helperMB,
		MaxUploadTime:        maxUploadTime,
		MaxUploadTimeWithout: maxWithout,
		HelperSet:            helperSet,
	}, nil
}

// Phase 2: precompute immutable information for every (shard, RepairPeer).
//
// This is the same optimization used by the old incoming-only ASCLEPIUS code:
// common/local/remote classification is performed ONCE before Global Max-Min.
//
// For every candidate RepairPeer we precompute:
//   - the CommonChunks metadata required by the executor;
//   - how many common chunks are local;
//   - how many common chunks are remote;
//   - the corresponding remote-common incoming MB;
//   - whether relocation is required;
//   - all statically valid relocation destinations.
//
// The dynamic Global Max-Min loop therefore never scans common CIDs or their
// source lists again.
func ascPrecomputeStaticCandidates(
	tasks []*ascTask,
	failedPeer peer.ID,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,
) {
	for _, task := range tasks {
		task.StaticCandidate = make(map[peer.ID]ascStaticRepairCandidate, len(candidatePeers))

		for _, repairPeer := range candidatePeers {
			if repairPeer == "" ||
				repairPeer == failedPeer ||
				!ascValidNode(topology, repairPeer) ||
				ascInMBps(topology, repairPeer) <= 0 {
				continue
			}

			commonChunks := make([]ASCCommonChunk, 0, len(task.CommonIndexes))
			localCommonCount := 0

			for _, chunkIndex := range task.CommonIndexes {
				chunk := task.Chunks[chunkIndex]

				source := peer.ID("")
				if ascContainsPeer(chunk.Sources, repairPeer) {
					source = repairPeer
					localCommonCount++
				}

				commonChunks = append(commonChunks, ASCCommonChunk{
					ChunkIndex: chunk.Index,
					CID:        chunk.CID,
					Source:     source,
				})
			}

			remoteCommonCount := len(task.CommonIndexes) - localCommonCount
			if remoteCommonCount < 0 {
				remoteCommonCount = 0
			}

			needsRelocation := task.SameStripePeers[repairPeer]

			validDestinations := make([]peer.ID, 0)
			if needsRelocation {
				for _, destination := range candidatePeers {
					if destination == "" ||
						destination == failedPeer ||
						destination == repairPeer ||
						task.SameStripePeers[destination] ||
						!ascValidNode(topology, destination) ||
						ascInMBps(topology, destination) <= 0 {
						continue
					}

					validDestinations = append(validDestinations, destination)
				}
			}

			task.StaticCandidate[repairPeer] = ascStaticRepairCandidate{
				CommonChunks:      commonChunks,
				LocalCommonCount:  localCommonCount,
				RemoteCommonCount: remoteCommonCount,
				RemoteCommonMB:    float64(remoteCommonCount) * chunkMB,
				NeedsRelocation:   needsRelocation,
				ValidDestinations: validDestinations,
			}
		}
	}
}

// Select the currently best relocation destination from the statically
// precomputed valid-destination list.
//
// Destination validity does not change during scheduling, so it is precomputed.
// Only the projected destination completion time remains dynamic because
// DownloadMB changes after every committed repair.
func ascBestRelocationDestination(
	task *ascTask,
	static ascStaticRepairCandidate,
	topology *NetworkTopology,
	loads *ASCNetworkLoad,
) (peer.ID, float64) {
	bestPeer := peer.ID("")
	bestTime := math.Inf(1)

	for _, destination := range static.ValidDestinations {
		destinationTime := ascCompletion(
			loads.DownloadMB[destination],
			task.ShardMB,
			ascInMBps(topology, destination),
		)
		if math.IsInf(destinationTime, 1) {
			continue
		}

		if destinationTime < bestTime ||
			(destinationTime == bestTime &&
				(bestPeer == "" || destination.String() < bestPeer.String())) {
			bestPeer = destination
			bestTime = destinationTime
		}
	}

	return bestPeer, bestTime
}

// Evaluate one RepairPeer candidate using a helper set that was already
// selected and FIXED for this shard in the current Global Max-Min iteration.
//
// Common/direct duplicate chunks:
//   - local common chunk: 0 incoming;
//   - remote common chunk: 1 * chunkMB incoming;
//   - no remote source is selected;
//   - no common-source upload load is modeled.
//
// Missing chunks:
//   - normally require N * |M| * chunkMB incoming;
//   - if RepairPeer is one of the fixed helpers, that helper contribution is
//     local, so only (N-1) helper streams are incoming;
//   - only remote fixed helpers contribute UploadMB.
//
// Candidate completion time is the maximum of:
//   - projected EC-helper upload completion;
//   - projected RepairPeer download completion;
//   - relocation upload completion, when relocation is needed;
//   - relocation destination download completion, when relocation is needed.
func ascEvaluateRepairCandidateWithFixedHelpers(
	taskIndex int,
	task *ascTask,
	fixed ascShardHelpers,
	repairPeer peer.ID,
	topology *NetworkTopology,
	loads *ASCNetworkLoad,
	relocationPeer peer.ID,
	relocationDownloadTime float64,
) (ascRepairCandidate, bool) {
	// All common/local/remote and placement-validity work was done once before
	// Global Max-Min. The hot loop performs only a map lookup here.
	static, ok := task.StaticCandidate[repairPeer]
	if !ok {
		return ascRepairCandidate{}, false
	}

	// Helper membership and helper-upload bottleneck were already computed once
	// for this fixed helper set. Do not rescan/recalculate the same helpers for
	// every RepairPeer candidate.
	repairPeerIsHelper := fixed.HelperSet != nil && fixed.HelperSet[repairPeer]
	remoteHelperCount := len(fixed.Helpers)
	helperUploadTime := fixed.MaxUploadTime

	if repairPeerIsHelper {
		remoteHelperCount--
		helperUploadTime = fixed.MaxUploadTimeWithout[repairPeer]
	}
	if remoteHelperCount < 0 {
		remoteHelperCount = 0
	}

	// RepairPeer incoming traffic:
	//   remote common chunks * q
	// + remote EC helpers * missing chunks * q.
	repairIncomingMB :=
		static.RemoteCommonMB +
			float64(remoteHelperCount)*fixed.HelperMB

	repairDownloadTime := ascCompletion(
		loads.DownloadMB[repairPeer],
		repairIncomingMB,
		ascInMBps(topology, repairPeer),
	)
	if math.IsInf(repairDownloadTime, 1) {
		return ascRepairCandidate{}, false
	}

	// Reuse the fixed helper slice and static common-chunk slice.  They are
	// immutable during this shard evaluation, so copying them for every
	// RepairPeer candidate is unnecessary.
	candidate := ascRepairCandidate{
		TaskIndex:        taskIndex,
		RepairPeer:       repairPeer,
		FinalPeer:        repairPeer,
		Helpers:          fixed.Helpers,
		CommonChunks:     static.CommonChunks,
		HelperMB:         fixed.HelperMB,
		RepairIncomingMB: repairIncomingMB,
		CompletionTime: ascMax(
			helperUploadTime,
			repairDownloadTime,
		),
	}

	if !static.NeedsRelocation {
		return candidate, true
	}

	// For a given shard in one Global Max-Min iteration, every RepairPeer that
	// needs relocation belongs to SameStripePeers.  The valid final destinations
	// therefore have the same static constraint: not failed and not same-stripe.
	// Their DownloadMB loads also stay unchanged while candidates of this shard
	// are being evaluated.  Hence the best relocation destination/time is
	// computed ONCE in ascBestCandidateForTask and reused here.
	if relocationPeer == "" || math.IsInf(relocationDownloadTime, 1) {
		return ascRepairCandidate{}, false
	}

	relocationUploadTime := ascCompletion(
		loads.UploadMB[repairPeer],
		task.ShardMB,
		ascOutMBps(topology, repairPeer),
	)
	if math.IsInf(relocationUploadTime, 1) {
		return ascRepairCandidate{}, false
	}

	candidate.FinalPeer = relocationPeer
	candidate.CompletionTime = ascMax(
		helperUploadTime,
		repairDownloadTime,
		relocationUploadTime,
		relocationDownloadTime,
	)

	return candidate, true
}

// For task i in the CURRENT Global Max-Min iteration:
//
//  1. select the N EC helpers ONCE using current UploadMB/BWout;
//  2. FIX that helper set;
//  3. evaluate every candidate RepairPeer j using the same helpers;
//  4. include relocation/final placement when needed;
//  5. return min_j P_ij.
func ascBestCandidateForTask(
	taskIndex int,
	task *ascTask,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	loads *ASCNetworkLoad,
	chunkMB float64,
) (ascRepairCandidate, bool) {
	fixedHelpers, err := ascSelectHelpersForShard(
		task,
		topology,
		loads,
		chunkMB,
	)
	if err != nil {
		return ascRepairCandidate{}, false
	}

	// Relocation destination quality depends on current committed DownloadMB,
	// but those loads do not change while we evaluate the RepairPeers of this
	// one shard.  Compute the best destination ONCE and reuse it for every
	// RepairPeer that needs relocation.
	relocationPeer := peer.ID("")
	relocationDownloadTime := math.Inf(1)

	for _, repairPeer := range candidatePeers {
		static, ok := task.StaticCandidate[repairPeer]
		if !ok || !static.NeedsRelocation {
			continue
		}

		relocationPeer, relocationDownloadTime = ascBestRelocationDestination(
			task,
			static,
			topology,
			loads,
		)
		break
	}

	best := ascRepairCandidate{}
	found := false

	for _, repairPeer := range candidatePeers {
		candidate, ok := ascEvaluateRepairCandidateWithFixedHelpers(
			taskIndex,
			task,
			fixedHelpers,
			repairPeer,
			topology,
			loads,
			relocationPeer,
			relocationDownloadTime,
		)
		if !ok {
			continue
		}

		if !found ||
			candidate.CompletionTime < best.CompletionTime ||
			(candidate.CompletionTime == best.CompletionTime &&
				candidate.RepairPeer.String() < best.RepairPeer.String()) ||
			(candidate.CompletionTime == best.CompletionTime &&
				candidate.RepairPeer == best.RepairPeer &&
				candidate.FinalPeer.String() < best.FinalPeer.String()) {
			best = candidate
			found = true
		}
	}

	return best, found
}

// Commit only the plan selected by Global Max-Min.
func ascCommitRepairCandidate(
	task *ascTask,
	candidate ascRepairCandidate,
	loads *ASCNetworkLoad,
) {
	// Commit ONLY remote EC-helper uploads.  The fixed helper set already
	// tells us exactly which peers contribute; no per-candidate UploadAddMB map
	// is needed.  If RepairPeer is a helper, its contribution is local.
	for _, helper := range candidate.Helpers {
		if helper == candidate.RepairPeer {
			continue
		}
		loads.UploadMB[helper] += candidate.HelperMB
	}

	// Commit repair incoming traffic.
	loads.DownloadMB[candidate.RepairPeer] += candidate.RepairIncomingMB

	if candidate.FinalPeer == candidate.RepairPeer {
		return
	}

	// Relocation: RepairPeer uploads the repaired shard.
	loads.UploadMB[candidate.RepairPeer] += task.ShardMB

	// Relocation: FinalPeer downloads the repaired shard.
	loads.DownloadMB[candidate.FinalPeer] += task.ShardMB
}

func ascBuildDecision(
	task *ascTask,
	candidate ascRepairCandidate,
) ASCRepairDecision {
	return ASCRepairDecision{
		Shard:        task.Shard,
		RepairPeer:   candidate.RepairPeer,
		Helpers:      append([]peer.ID(nil), candidate.Helpers...),
		CommonChunks: append([]ASCCommonChunk(nil), candidate.CommonChunks...),
		FinalPeer:    candidate.FinalPeer,
	}
}

// ScheduleASCLEPIUSMultiResource applies the intended joint bandwidth-aware
// ASCLEPIUS Global Max-Min scheduler.
//
// In EACH Global Max-Min iteration, for EACH remaining shard:
//
//  1. Select N EC helpers ONCE using current committed helper upload load and
//     each helper's outgoing bandwidth.
//  2. FIX those helpers for that shard for the whole current iteration.
//  3. Evaluate every candidate RepairPeer using the SAME fixed helpers.
//  4. Common chunks follow the old incoming-only abstraction:
//     - local common = 0 incoming;
//     - remote common = 1 incoming chunk;
//     - no remote common source is selected;
//     - no common-source upload load is modeled.
//  5. Missing chunks use the fixed EC helpers. Only those EC helpers contribute
//     helper upload load.
//  6. If a RepairPeer already stores another shard of the same stripe, select
//     the valid relocation destination with minimum projected incoming time.
//  7. Candidate completion is the bottleneck across helper upload, repair
//     download, and relocation upload/download when relocation exists.
//  8. Keep the minimum candidate for each shard.
//  9. Global Max-Min commits max_i(min_j P_ij).
//
// After one shard is committed, UploadMB/DownloadMB change. In the NEXT
// Global Max-Min iteration, each still-unscheduled shard selects its helpers
// again once using the new committed loads.
func ScheduleASCLEPIUSMultiResource(
	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,
	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (
		peer.ID,
		[]string,
		map[peer.ID]int,
		map[peer.ID][]string,
	),
) ([]ASCRepairDecision, error) {
	started := time.Now()
	decisions := make([]ASCRepairDecision, 0, len(failedShards))

	if topology == nil {
		return decisions, fmt.Errorf("nil network topology")
	}
	if chunkMB <= 0 {
		return decisions, fmt.Errorf("chunkMB must be positive")
	}
	if getSameStripe == nil {
		return decisions, fmt.Errorf("nil getSameStripe callback")
	}
	if getSimilarity == nil {
		return decisions, fmt.Errorf("nil getSimilarity callback")
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

	loads := ascNewLoadState(topology)

	phase := time.Now()
	tasks, err := ascBuildTasks(
		failedPeer,
		failedShards,
		topology,
		chunkMB,
		getSameStripe,
		getSimilarity,
	)
	if err != nil {
		return decisions, err
	}

	fmt.Printf(
		"[ASC-BW] common/missing shard classification took %v\n",
		time.Since(phase),
	)

	// Precompute all immutable (shard, RepairPeer) information ONCE, exactly
	// like the old incoming-only scheduler precomputed candidate costs.
	phase = time.Now()
	ascPrecomputeStaticCandidates(
		tasks,
		failedPeer,
		candidatePeers,
		topology,
		chunkMB,
	)
	fmt.Printf(
		"[ASC-BW] static per-(shard,repairPeer) common/relocation precompute took %v\n",
		time.Since(phase),
	)

	unscheduled := make(map[int]bool, len(tasks))
	for taskIndex := range tasks {
		unscheduled[taskIndex] = true
	}

	phase = time.Now()

	for len(unscheduled) > 0 {
		bestByTask := make(map[int]ascRepairCandidate, len(unscheduled))

		// For each remaining shard:
		//   - select helpers ONCE;
		//   - fix them;
		//   - evaluate all RepairPeers + relocation with those same helpers.
		for taskIndex := range unscheduled {
			candidate, ok := ascBestCandidateForTask(
				taskIndex,
				tasks[taskIndex],
				candidatePeers,
				topology,
				loads,
				chunkMB,
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

		// Global Max-Min: choose max_i(min_j P_ij).
		chosenTaskIndex := -1
		chosen := ascRepairCandidate{}

		for taskIndex, candidate := range bestByTask {
			if chosenTaskIndex == -1 ||
				candidate.CompletionTime > chosen.CompletionTime ||
				(candidate.CompletionTime == chosen.CompletionTime &&
					tasks[taskIndex].Key < tasks[chosenTaskIndex].Key) {
				chosenTaskIndex = taskIndex
				chosen = candidate
			}
		}

		task := tasks[chosenTaskIndex]

		// Only the Global Max-Min winner becomes committed load.
		ascCommitRepairCandidate(task, chosen, loads)

		decision := ascBuildDecision(task, chosen)
		decisions = append(decisions, decision)
		delete(unscheduled, chosenTaskIndex)

		static := task.StaticCandidate[decision.RepairPeer]
		localCommon := static.LocalCommonCount
		remoteCommon := static.RemoteCommonCount

		fmt.Printf(
			"[ASC-BW] shard=%s missing=%d helpers=%d common=%d "+
				"localCommon=%d remoteCommon=%d repair=%s final=%s relocated=%v "+
				"incomingMB=%.3f finish=%.6f\n",
			task.Shard.Name,
			len(task.MissingIndexes),
			len(decision.Helpers),
			len(task.CommonIndexes),
			localCommon,
			remoteCommon,
			decision.RepairPeer.String(),
			decision.FinalPeer.String(),
			decision.FinalPeer != decision.RepairPeer,
			chosen.RepairIncomingMB,
			chosen.CompletionTime,
		)
	}

	fmt.Printf(
		"[ASC-BW] joint Global Max-Min scheduling phase took %v\n",
		time.Since(phase),
	)
	fmt.Printf(
		"[ASC-BW] total scheduling time %v\n",
		time.Since(started),
	)

	return decisions, nil
}

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
/* package ipfscluster

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

// Phase 2: greedily assign the least-loaded valid helpers.
// Heterogeneity is considered through each peer's upload and disk-read speeds.
func ascAssignMissingHelpers(
    tasks []*ascTask,
    topology *NetworkTopology,
    loads *ASCNetworkLoad,
    chunkMB float64,
) error {
    for _, task := range tasks {
       if len(task.MissingIndexes) == 0 {
          continue
       }

       additionalMB := float64(len(task.MissingIndexes)) * chunkMB

       for len(task.Helpers) < task.N {
          bestHelper := peer.ID("")
          bestCost := math.Inf(1)

          for _, helper := range task.HelperCandidates {
             if ascContainsPeer(task.Helpers, helper) {
                continue
             }
             if ascOutMBps(topology, helper) <= 0 ||
                ascDiskReadMBps(topology, helper) <= 0 {
                continue
             }

             cost := ascHelperProjectedCost(
                helper,
                additionalMB,
                topology,
                loads,
             )
             if math.IsInf(cost, 1) {
                continue
             }

             if cost < bestCost ||
                (cost == bestCost &&
                   (bestHelper == "" ||
                      helper.String() < bestHelper.String())) {
                bestHelper = helper
                bestCost = cost
             }
          }

          if bestHelper == "" {
             return fmt.Errorf(
                "cannot assign helper %d/%d for shard %s",
                len(task.Helpers)+1,
                task.N,
                task.Shard.Name,
             )
          }

          task.Helpers = append(task.Helpers, bestHelper)
          loads.UploadMB[bestHelper] += additionalMB
          loads.DiskReadMB[bestHelper] += additionalMB
       }
    }

    return nil
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

// Phase 3: greedily select the least-loaded valid source for every common chunk.
func ascAssignCommonSources(
    tasks []*ascTask,
    topology *NetworkTopology,
    loads *ASCNetworkLoad,
    chunkMB float64,
) error {
    for _, task := range tasks {
       for _, chunkIndex := range task.CommonIndexes {
          chunk := &task.Chunks[chunkIndex]

          if chunk.SourcePeer != "" {
             continue
          }

          bestSource := peer.ID("")
          bestCost := math.Inf(1)

          for _, source := range chunk.Sources {
             if ascOutMBps(topology, source) <= 0 ||
                ascDiskReadMBps(topology, source) <= 0 {
                continue
             }

             cost := ascCommonSourceProjectedCost(
                source,
                chunkMB,
                topology,
                loads,
             )
             if math.IsInf(cost, 1) {
                continue
             }

             if cost < bestCost ||
                (cost == bestCost &&
                   (bestSource == "" ||
                      source.String() < bestSource.String())) {
                bestSource = source
                bestCost = cost
             }
          }

          if bestSource == "" {
             return fmt.Errorf(
                "common chunk %s of shard %s has no valid source",
                chunk.CID,
                task.Shard.Name,
             )
          }

          chunk.SourcePeer = bestSource
          loads.UploadMB[bestSource] += chunkMB
          loads.DiskReadMB[bestSource] += chunkMB
       }
    }

    return nil
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

// Select the least-loaded valid relocation destination.
// Destination time is projected download completion plus disk-write completion.
// Effective destination speed is shard size divided by this total time.
func ascBestRelocationDestination(
    task *ascTask,
    repairPeer peer.ID,
    failedPeer peer.ID,
    candidatePeers []peer.ID,
    topology *NetworkTopology,
    loads *ASCNetworkLoad,
) (peer.ID, float64, float64) {
    bestPeer := peer.ID("")
    bestTime := math.Inf(1)
    bestEffectiveSpeed := 0.0

    for _, destination := range candidatePeers {
       if destination == "" ||
          destination == failedPeer ||
          destination == repairPeer {
          continue
       }
       if task.SameStripePeers[destination] {
          continue
       }

       inSpeed := ascInMBps(topology, destination)
       writeSpeed := ascDiskWriteMBps(topology, destination)
       if inSpeed <= 0 || writeSpeed <= 0 {
          continue
       }

       downloadTime := ascCompletion(
          loads.DownloadMB[destination],
          task.ShardMB,
          inSpeed,
       )
       writeTime := ascCompletion(
          loads.DiskWriteMB[destination],
          task.ShardMB,
          writeSpeed,
       )
       if math.IsInf(downloadTime, 1) || math.IsInf(writeTime, 1) {
          continue
       }

       destinationTime := downloadTime + writeTime
       effectiveSpeed := 0.0
       if destinationTime > 0 {
          effectiveSpeed = task.ShardMB / destinationTime
       }

       if destinationTime < bestTime ||
          (destinationTime == bestTime &&
             (bestPeer == "" ||
                destination.String() < bestPeer.String())) {
          bestPeer = destination
          bestTime = destinationTime
          bestEffectiveSpeed = effectiveSpeed
       }
    }

    return bestPeer, bestTime, bestEffectiveSpeed
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
    if repairPeer == "" ||
       repairPeer == failedPeer ||
       !ascValidNode(topology, repairPeer) {
       return ascRepairCandidate{}, false
    }

    repairInSpeed := ascInMBps(topology, repairPeer)
    repairOutSpeed := ascOutMBps(topology, repairPeer)
    localWriteSpeed := ascDiskWriteMBps(topology, repairPeer)

    if repairInSpeed <= 0 || localWriteSpeed <= 0 {
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
       repairInSpeed,
    )
    if math.IsInf(repairDownloadTime, 1) {
       return ascRepairCandidate{}, false
    }

    localWriteTime := ascCompletion(
       loads.DiskWriteMB[repairPeer],
       task.ShardMB,
       localWriteSpeed,
    )
    if math.IsInf(localWriteTime, 1) {
       return ascRepairCandidate{}, false
    }

    localCompletion := repairDownloadTime + localWriteTime
    localPlacementEligible := !task.SameStripePeers[repairPeer]

    adjustedUploadLoad := loads.UploadMB[repairPeer] - localReduction
    if adjustedUploadLoad < 0 {
       adjustedUploadLoad = 0
    }

    bestDestination, bestDestinationTime, bestDestinationEffectiveSpeed :=
       ascBestRelocationDestination(
          task,
          repairPeer,
          failedPeer,
          candidatePeers,
          topology,
          loads,
       )

    // Local placement is forbidden because the repair peer already stores
    // another shard from the same stripe. Relocation is mandatory.
    if !localPlacementEligible {
       if bestDestination == "" || repairOutSpeed <= 0 {
          return ascRepairCandidate{}, false
       }

       relocationUploadTime := ascCompletion(
          adjustedUploadLoad,
          task.ShardMB,
          repairOutSpeed,
       )
       if math.IsInf(relocationUploadTime, 1) ||
          math.IsInf(bestDestinationTime, 1) {
          return ascRepairCandidate{}, false
       }

       return ascRepairCandidate{
          TaskIndex:            taskIndex,
          RepairPeer:           repairPeer,
          FinalPeer:            bestDestination,
          RepairIncomingMB:     repairIncomingMB,
          LocalUploadReduction: localReduction,
          CompletionTime: repairDownloadTime +
             relocationUploadTime +
             bestDestinationTime,
       }, true
    }

    // Local disk writing is at least as fast as uploading.
    if repairOutSpeed <= 0 || localWriteSpeed >= repairOutSpeed {
       return ascRepairCandidate{
          TaskIndex:            taskIndex,
          RepairPeer:           repairPeer,
          FinalPeer:            repairPeer,
          RepairIncomingMB:     repairIncomingMB,
          LocalUploadReduction: localReduction,
          CompletionTime:       localCompletion,
       }, true
    }

    // Upload is faster than local writing, but relocation is selected only
    // when the least-loaded destination is also effectively faster.
    if bestDestination == "" ||
       bestDestinationEffectiveSpeed <= localWriteSpeed {
       return ascRepairCandidate{
          TaskIndex:            taskIndex,
          RepairPeer:           repairPeer,
          FinalPeer:            repairPeer,
          RepairIncomingMB:     repairIncomingMB,
          LocalUploadReduction: localReduction,
          CompletionTime:       localCompletion,
       }, true
    }

    relocationUploadTime := ascCompletion(
       adjustedUploadLoad,
       task.ShardMB,
       repairOutSpeed,
    )
    if math.IsInf(relocationUploadTime, 1) ||
       math.IsInf(bestDestinationTime, 1) {
       return ascRepairCandidate{
          TaskIndex:            taskIndex,
          RepairPeer:           repairPeer,
          FinalPeer:            repairPeer,
          RepairIncomingMB:     repairIncomingMB,
          LocalUploadReduction: localReduction,
          CompletionTime:       localCompletion,
       }, true
    }

    return ascRepairCandidate{
       TaskIndex:            taskIndex,
       RepairPeer:           repairPeer,
       FinalPeer:            bestDestination,
       RepairIncomingMB:     repairIncomingMB,
       LocalUploadReduction: localReduction,
       CompletionTime: repairDownloadTime +
          relocationUploadTime +
          bestDestinationTime,
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
}*/
