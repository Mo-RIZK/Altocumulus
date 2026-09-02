package ipfscluster

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

/*
===============================================================================
CMRepair scheduler for ALTOCUMULUS
===============================================================================

This file implements the repair-solution generation part of CMRepair under the
controlled comparison used in ALTOCUMULUS:

    one rack == one peer/node

Therefore:
  - every inter-peer helper transfer is a cross-rack transfer;
  - there is no intra-rack aggregation layer;
  - CMRepair rack upload/download times map directly to peer upload/download
    times;
  - the requestor rack maps directly to RepairPeer.

The scheduler DOES NOT exploit duplication/similarity. A failed shard is
reconstructed conventionally from exactly N surviving same-stripe shards.

Two algorithms are provided:

    ScheduleCMRepairCTP(...)
    ScheduleCMRepairRTP(...)

Both return CMRepairSchedule. Every decision contains:
  - the failed shard;
  - the requestor / repair peer;
  - the exact N helper shard CIDs;
  - the zero-based RS indexes expected by the existing repair executor.

This intentionally mirrors SelectiveECDecision so cluster.go can populate:

    shard.Metadata["helper_cids"]
    shard.Metadata["helper_indexes"]

and enqueue the repair exactly as it already does for SelectiveEC.

IMPORTANT ABOUT CMRepair ALGORITHM 3
------------------------------------
CMRepair also schedules INDIVIDUAL cross-rack repair links after CTP/RTP.
That execution ordering cannot be enforced merely by returning one repair
decision per shard, because ALTOCUMULUS's current Enqueue path starts the
helper downloads inside each repair.

This implementation therefore computes and returns Algorithm-3 ordering in:

    schedule.OrderedLinks

but ScheduleCMRepairCTP / ScheduleCMRepairRTP do not themselves execute those
links. If cluster.go simply enqueues every returned decision concurrently, the
experiment is CMRepair's repair-solution-generation component (GRS). To claim
full CMRepair (GRS + SRL), the repair executor must additionally expose
per-helper transfer launch control and consume OrderedLinks.

Network model
-------------
NetworkTopology bandwidth values are in Mbit/s. For helper h and requestor d:

    bandwidth(h,d) = topology.EffectiveBandwidth(h,d)

which already accounts for:
    min(GlobalOut[h], GlobalIn[d], PairwiseBandwidth[h,d])

For a shard of size Q MB:

    Cost(h,d) = Q * 8 / bandwidth(h,d) seconds

This is CMRepair's per-block cross-rack Cost_ij specialized to one node/rack.

For a complete multi-stripe solution:

    Tu[p] = sum Cost(p,d) over all helper transfers sent by p
    Td[p] = sum Cost(h,p) over all helper transfers received by p
    T[p]  = max(Tu[p], Td[p])
    MT    = max_p T[p]

===============================================================================
*/

// =============================================================================
// Public structures
// =============================================================================

// CMRepairAlgorithm identifies the repair-solution generator.
type CMRepairAlgorithm string

const (
	CMRepairCTP CMRepairAlgorithm = "CMREPAIR_CTP"
	CMRepairRTP CMRepairAlgorithm = "CMREPAIR_RTP"
)

// CMRepairHelper is deliberately compatible with the SelectiveEC executor
// metadata format: exact helper shard CID plus zero-based RS index.
type CMRepairHelper struct {
	CID     cid.Cid
	RSIndex int
}

// CMRepairDecision is one final per-shard repair assignment.
type CMRepairDecision struct {
	Shard      api.Pin
	RepairPeer peer.ID
	Helpers    []CMRepairHelper
}

// CMRepairLink represents one selected helper -> requestor transfer.
//
// OrderedLinks in CMRepairSchedule contains these links in the order generated
// by CMRepair Algorithm 3.
type CMRepairLink struct {
	TaskIndex       int
	Shard           api.Pin
	Source          peer.ID
	Destination     peer.ID
	Helper          CMRepairHelper
	CostSeconds     float64
	CongestionLevel float64
}

// CMRepairSchedule is the complete result of CTP or RTP.
type CMRepairSchedule struct {
	Algorithm CMRepairAlgorithm

	Decisions []CMRepairDecision

	// Theoretical maximum transmission time of the selected solution.
	MT float64

	// Per-peer transmission-time loads of the selected solution.
	UploadTime   map[peer.ID]float64
	DownloadTime map[peer.ID]float64

	// Algorithm-3 link order. See file-level comment: the current repair
	// executor must be extended if this order is to be enforced physically.
	OrderedLinks []CMRepairLink

	// Time spent inside the scheduler.
	ComputationTime time.Duration
}

// CMRepairRTPOptions are the two stopping controls described by CMRepair plus a
// deterministic random seed for reproducible experiments.
type CMRepairRTPOptions struct {
	// Maximum number of consecutively tolerated non-improving/worse moves.
	W int

	// Maximum scheduler computation time.
	MaxComputationTime time.Duration

	// Random seed used when RTP deliberately leaves a local optimum.
	Seed int64
}

// DefaultCMRepairRTPOptions matches the paper's reported default EC2 setting
// w=4 and t=30 s. Seed is fixed for reproducible experimental runs.
func DefaultCMRepairRTPOptions() CMRepairRTPOptions {
	return CMRepairRTPOptions{
		W:                  4,
		MaxComputationTime: 30 * time.Second,
		Seed:               1,
	}
}

// CMRepairStripeInfoFunc adapts c.get_shards_same_stripe(pin).
//
// Expected return:
//
//	sameStripeShards
//	allocations
//	reconstructionN
//	chunkCount
type CMRepairStripeInfoFunc func(
	api.Pin,
) ([]api.Pin, []peer.ID, int, int)

// =============================================================================
// Internal structures
// =============================================================================

type cmRepairTask struct {
	Shard api.Pin
	Key   string

	// N surviving shards are required by the RS decoder.
	N int

	// Size of one encoded shard/block in MB.
	ShardMB float64

	// Every valid surviving same-stripe source peer.
	SourcePeers []peer.ID
	SourceSet   map[peer.ID]bool

	// One exact helper shard associated with every source peer.
	HelperByPeer map[peer.ID]CMRepairHelper

	// A destination must not already store a surviving shard of this stripe.
	DestinationPeers []peer.ID
}

type cmRepairSolution struct {
	Helpers     []peer.ID
	Destination peer.ID
}

// Used when evaluating MT.
type cmRepairTimes struct {
	Upload   map[peer.ID]float64
	Download map[peer.ID]float64
	MT       float64

	BottleneckPeer peer.ID
	BottleneckKind string // "upload" or "download"
}

// =============================================================================
// General utilities
// =============================================================================

const cmRepairEpsilon = 1e-12

func cmRepairLess(a, b float64) bool {
	return a < b-cmRepairEpsilon
}

func cmRepairEqual(a, b float64) bool {
	return math.Abs(a-b) <= cmRepairEpsilon
}

func cmRepairSortedUniquePeers(in []peer.ID) []peer.ID {
	seen := make(map[peer.ID]bool, len(in))
	out := make([]peer.ID, 0, len(in))

	for _, p := range in {
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

func cmRepairContainsPeer(in []peer.ID, target peer.ID) bool {
	for _, p := range in {
		if p == target {
			return true
		}
	}
	return false
}

func cmRepairCopySolution(in cmRepairSolution) cmRepairSolution {
	return cmRepairSolution{
		Helpers:     append([]peer.ID(nil), in.Helpers...),
		Destination: in.Destination,
	}
}

func cmRepairCopyMultiSolution(in []cmRepairSolution) []cmRepairSolution {
	out := make([]cmRepairSolution, len(in))
	for i := range in {
		out[i] = cmRepairCopySolution(in[i])
	}
	return out
}

func cmRepairPeerSliceKey(peers []peer.ID) string {
	values := make([]string, len(peers))
	for i, p := range peers {
		values[i] = p.String()
	}
	return strings.Join(values, "|")
}

func cmRepairSolutionKey(s cmRepairSolution) string {
	return cmRepairPeerSliceKey(s.Helpers) + "->" + s.Destination.String()
}

func cmRepairSameHelpers(a, b []peer.ID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func cmRepairSameSolution(a, b cmRepairSolution) bool {
	return a.Destination == b.Destination &&
		cmRepairSameHelpers(a.Helpers, b.Helpers)
}

func cmRepairValidNode(topology *NetworkTopology, p peer.ID) bool {
	return topology != nil &&
		topology.NodesByPeer != nil &&
		topology.NodesByPeer[p] != nil
}

// =============================================================================
// RS-index parsing
// =============================================================================

// cmRepairTotalStripeShards extracts k,m from "RS(k,m)" in the shard name and
// returns k+m. This mirrors the convention already used by SelectiveEC.
func cmRepairTotalStripeShards(shardName string) (int, error) {
	open := strings.Index(shardName, "(")
	close := strings.Index(shardName, ")")

	if open < 0 || close < 0 || close <= open+1 {
		return 0, fmt.Errorf(
			"CMRepair: cannot extract RS parameters from shard name %q",
			shardName,
		)
	}

	parts := strings.Split(shardName[open+1:close], ",")
	if len(parts) != 2 {
		return 0, fmt.Errorf(
			"CMRepair: invalid RS parameter string %q in shard %q",
			shardName[open+1:close],
			shardName,
		)
	}

	k, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || k <= 0 {
		return 0, fmt.Errorf(
			"CMRepair: invalid data-shard count %q in shard %q",
			parts[0],
			shardName,
		)
	}

	m, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || m < 0 {
		return 0, fmt.Errorf(
			"CMRepair: invalid parity-shard count %q in shard %q",
			parts[1],
			shardName,
		)
	}

	if k+m <= 0 {
		return 0, fmt.Errorf(
			"CMRepair: invalid stripe width %d in shard %q",
			k+m,
			shardName,
		)
	}

	return k + m, nil
}

func cmRepairCIDCount(pin api.Pin) int {
	if pin.Metadata == nil {
		return 0
	}

	raw := strings.TrimSpace(pin.Metadata["Cids"])
	if raw == "" {
		return 0
	}

	count := 0
	for _, value := range strings.Split(raw, ",") {
		if strings.TrimSpace(strings.Trim(value, "<>")) != "" {
			count++
		}
	}
	return count
}

// =============================================================================
// Task construction
// =============================================================================

// cmRepairBuildTaskReal performs actual topology-aware task construction.
func cmRepairBuildTaskReal(
	shard api.Pin,
	failedPeer peer.ID,
	livePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,
	getStripe CMRepairStripeInfoFunc,
) (*cmRepairTask, error) {

	sameStripeShards, _, reconstructionN, chunkCount := getStripe(shard)

	if reconstructionN <= 0 {
		return nil, fmt.Errorf(
			"CMRepair: invalid reconstruction N=%d for shard %s",
			reconstructionN,
			shard.Name,
		)
	}
	if chunkMB <= 0 {
		return nil, fmt.Errorf("CMRepair: chunkMB must be positive")
	}

	if chunkCount <= 0 {
		chunkCount = cmRepairCIDCount(shard)
	}
	if chunkCount <= 0 {
		return nil, fmt.Errorf(
			"CMRepair: cannot determine shard size/chunk count for %s",
			shard.Name,
		)
	}

	totalStripeShards, err := cmRepairTotalStripeShards(shard.Name)
	if err != nil {
		return nil, err
	}

	failedShardNumber, _, err := getShardNumber(shard.Name)
	if err != nil {
		return nil, fmt.Errorf(
			"CMRepair: cannot parse failed shard number from %q: %w",
			shard.Name,
			err,
		)
	}
	if failedShardNumber <= 0 {
		return nil, fmt.Errorf(
			"CMRepair: invalid failed shard number %d for shard %s",
			failedShardNumber,
			shard.Name,
		)
	}
	failedRSIndex := (failedShardNumber - 1) % totalStripeShards

	cleanLive := make([]peer.ID, 0, len(livePeers))
	liveSet := make(map[peer.ID]bool, len(livePeers))

	for _, p := range cmRepairSortedUniquePeers(livePeers) {
		if p == "" || p == failedPeer || !cmRepairValidNode(topology, p) {
			continue
		}
		liveSet[p] = true
		cleanLive = append(cleanLive, p)
	}

	helperByPeer := make(map[peer.ID]CMRepairHelper)

	for _, survivingShard := range sameStripeShards {
		globalShardNumber, _, parseErr := getShardNumber(survivingShard.Name)
		if parseErr != nil {
			return nil, fmt.Errorf(
				"CMRepair: cannot parse surviving shard number from %q: %w",
				survivingShard.Name,
				parseErr,
			)
		}
		if globalShardNumber <= 0 {
			return nil, fmt.Errorf(
				"CMRepair: invalid surviving shard number %d for %q",
				globalShardNumber,
				survivingShard.Name,
			)
		}

		rsIndex := (globalShardNumber - 1) % totalStripeShards
		if rsIndex == failedRSIndex {
			continue
		}

		helper := CMRepairHelper{
			CID:     survivingShard.Cid.Cid,
			RSIndex: rsIndex,
		}
		if !helper.CID.Defined() {
			return nil, fmt.Errorf(
				"CMRepair: surviving shard %q has undefined CID",
				survivingShard.Name,
			)
		}

		for _, allocation := range survivingShard.Allocations {
			if allocation == "" ||
				allocation == failedPeer ||
				!liveSet[allocation] {
				continue
			}

			existing, exists := helperByPeer[allocation]
			if !exists ||
				helper.RSIndex < existing.RSIndex ||
				(helper.RSIndex == existing.RSIndex &&
					helper.CID.String() < existing.CID.String()) {
				helperByPeer[allocation] = helper
			}
		}
	}

	sourcePeers := make([]peer.ID, 0, len(helperByPeer))
	sourceSet := make(map[peer.ID]bool, len(helperByPeer))

	for p := range helperByPeer {
		sourcePeers = append(sourcePeers, p)
		sourceSet[p] = true
	}
	sourcePeers = cmRepairSortedUniquePeers(sourcePeers)

	if len(sourcePeers) < reconstructionN {
		return nil, fmt.Errorf(
			"CMRepair: shard %s requires %d distinct source peers, only %d are available",
			shard.Name,
			reconstructionN,
			len(sourcePeers),
		)
	}

	destinations := make([]peer.ID, 0, len(cleanLive))
	for _, p := range cleanLive {
		// CMRepair requestor must not already contain a surviving block from the
		// same stripe. Under one-node-per-rack this means p is not a source peer.
		if sourceSet[p] {
			continue
		}
		if topology.NodesByPeer[p].GlobalIn == 0 {
			continue
		}
		destinations = append(destinations, p)
	}

	if len(destinations) == 0 {
		return nil, fmt.Errorf(
			"CMRepair: shard %s has no valid requestor/destination peer",
			shard.Name,
		)
	}

	return &cmRepairTask{
		Shard:            shard,
		Key:              shard.Cid.String(),
		N:                reconstructionN,
		ShardMB:          float64(chunkCount) * chunkMB,
		SourcePeers:      sourcePeers,
		SourceSet:        sourceSet,
		HelperByPeer:     helperByPeer,
		DestinationPeers: destinations,
	}, nil
}

func cmRepairBuildTasks(
	failedPeer peer.ID,
	failedShards []api.Pin,
	livePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,
	getStripe CMRepairStripeInfoFunc,
) ([]*cmRepairTask, error) {

	tasks := make([]*cmRepairTask, 0, len(failedShards))

	for _, shard := range failedShards {
		task, err := cmRepairBuildTaskReal(
			shard,
			failedPeer,
			livePeers,
			topology,
			chunkMB,
			getStripe,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// =============================================================================
// Link cost and MT
// =============================================================================

// cmRepairTransferCostSeconds implements Cost_ij for one encoded shard/block.
func cmRepairTransferCostSeconds(
	topology *NetworkTopology,
	src peer.ID,
	dst peer.ID,
	shardMB float64,
) float64 {

	if src == "" || dst == "" || src == dst || shardMB <= 0 {
		return math.Inf(1)
	}

	bwMbit := topology.EffectiveBandwidth(src, dst)
	if bwMbit == 0 {
		return math.Inf(1)
	}

	return shardMB * 8.0 / float64(bwMbit)
}

func cmRepairEvaluateMultiSolution(
	tasks []*cmRepairTask,
	solutions []cmRepairSolution,
	topology *NetworkTopology,
) cmRepairTimes {

	times := cmRepairTimes{
		Upload:   make(map[peer.ID]float64),
		Download: make(map[peer.ID]float64),
		MT:       0,
	}

	for p := range topology.NodesByPeer {
		times.Upload[p] = 0
		times.Download[p] = 0
	}

	for taskIndex, solution := range solutions {
		if taskIndex >= len(tasks) {
			break
		}
		task := tasks[taskIndex]

		for _, helper := range solution.Helpers {
			cost := cmRepairTransferCostSeconds(
				topology,
				helper,
				solution.Destination,
				task.ShardMB,
			)

			if math.IsInf(cost, 1) {
				times.MT = math.Inf(1)
				return times
			}

			times.Upload[helper] += cost
			times.Download[solution.Destination] += cost
		}
	}

	// Deterministic bottleneck selection:
	// sorted peer ID; upload wins an exact upload/download tie.
	peers := make([]peer.ID, 0, len(topology.NodesByPeer))
	for p := range topology.NodesByPeer {
		peers = append(peers, p)
	}
	peers = cmRepairSortedUniquePeers(peers)

	for _, p := range peers {
		u := times.Upload[p]
		d := times.Download[p]

		local := u
		kind := "upload"
		if cmRepairLess(u, d) {
			local = d
			kind = "download"
		}

		if times.BottleneckPeer == "" ||
			cmRepairLess(times.MT, local) {
			times.MT = local
			times.BottleneckPeer = p
			times.BottleneckKind = kind
		}
	}

	return times
}

// =============================================================================
// Combination enumeration
// =============================================================================

func cmRepairEnumerateHelperCombinations(
	sourcePeers []peer.ID,
	n int,
	excluded peer.ID,
) [][]peer.ID {

	filtered := make([]peer.ID, 0, len(sourcePeers))
	for _, p := range sourcePeers {
		if p == "" || p == excluded {
			continue
		}
		filtered = append(filtered, p)
	}
	filtered = cmRepairSortedUniquePeers(filtered)

	if n <= 0 || len(filtered) < n {
		return nil
	}

	out := make([][]peer.ID, 0)
	current := make([]peer.ID, 0, n)

	var visit func(start int)
	visit = func(start int) {
		if len(current) == n {
			combo := append([]peer.ID(nil), current...)
			out = append(out, combo)
			return
		}

		need := n - len(current)
		for i := start; i <= len(filtered)-need; i++ {
			current = append(current, filtered[i])
			visit(i + 1)
			current = current[:len(current)-1]
		}
	}

	visit(0)
	return out
}

func cmRepairCandidateFeasible(
	task *cmRepairTask,
	candidate cmRepairSolution,
	topology *NetworkTopology,
) bool {

	if candidate.Destination == "" ||
		task.SourceSet[candidate.Destination] ||
		len(candidate.Helpers) != task.N {
		return false
	}

	seen := make(map[peer.ID]bool, len(candidate.Helpers))

	for _, helper := range candidate.Helpers {
		if helper == "" ||
			seen[helper] ||
			!task.SourceSet[helper] {
			return false
		}
		seen[helper] = true

		cost := cmRepairTransferCostSeconds(
			topology,
			helper,
			candidate.Destination,
			task.ShardMB,
		)
		if math.IsInf(cost, 1) {
			return false
		}
	}

	return true
}

func cmRepairAllCandidatesForTask(
	task *cmRepairTask,
	topology *NetworkTopology,
) []cmRepairSolution {

	helperSets := cmRepairEnumerateHelperCombinations(
		task.SourcePeers,
		task.N,
		"",
	)

	candidates := make([]cmRepairSolution, 0)

	for _, destination := range task.DestinationPeers {
		for _, helpers := range helperSets {
			candidate := cmRepairSolution{
				Helpers:     append([]peer.ID(nil), helpers...),
				Destination: destination,
			}

			if cmRepairCandidateFeasible(task, candidate, topology) {
				candidates = append(candidates, candidate)
			}
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		return cmRepairSolutionKey(candidates[i]) <
			cmRepairSolutionKey(candidates[j])
	})

	return candidates
}

// =============================================================================
// AZ-Recovery initialization
// =============================================================================

// Under one-node-per-rack, AZ initialization exhaustively enumerates every
// valid helper-combination/requestor pair for one stripe and selects the
// candidate with the smallest single-stripe MT.
func cmRepairAZForTask(
	task *cmRepairTask,
	topology *NetworkTopology,
) (cmRepairSolution, error) {

	candidates := cmRepairAllCandidatesForTask(task, topology)
	if len(candidates) == 0 {
		return cmRepairSolution{}, fmt.Errorf(
			"CMRepair AZ: no valid solution for shard %s",
			task.Shard.Name,
		)
	}

	best := cmRepairSolution{}
	bestMT := math.Inf(1)
	found := false

	singleTask := []*cmRepairTask{task}

	for _, candidate := range candidates {
		t := cmRepairEvaluateMultiSolution(
			singleTask,
			[]cmRepairSolution{candidate},
			topology,
		)

		if !found ||
			cmRepairLess(t.MT, bestMT) ||
			(cmRepairEqual(t.MT, bestMT) &&
				cmRepairSolutionKey(candidate) < cmRepairSolutionKey(best)) {
			best = cmRepairCopySolution(candidate)
			bestMT = t.MT
			found = true
		}
	}

	if !found {
		return cmRepairSolution{}, fmt.Errorf(
			"CMRepair AZ: no finite solution for shard %s",
			task.Shard.Name,
		)
	}

	return best, nil
}

func cmRepairAZInitialize(
	tasks []*cmRepairTask,
	topology *NetworkTopology,
) ([]cmRepairSolution, error) {

	solutions := make([]cmRepairSolution, len(tasks))

	for i, task := range tasks {
		solution, err := cmRepairAZForTask(task, topology)
		if err != nil {
			return nil, err
		}
		solutions[i] = solution
	}

	return solutions, nil
}

// =============================================================================
// CTP
// =============================================================================

// cmRepairCTPUploadAlternatives implements CMRepair Algorithm 1's upload
// bottleneck move:
//   - requestor unchanged;
//   - choose another helper set;
//   - retrieve no helper block from bottleneck peer.
func cmRepairCTPUploadAlternatives(
	task *cmRepairTask,
	current cmRepairSolution,
	bottleneck peer.ID,
	topology *NetworkTopology,
) []cmRepairSolution {

	helperSets := cmRepairEnumerateHelperCombinations(
		task.SourcePeers,
		task.N,
		bottleneck,
	)

	out := make([]cmRepairSolution, 0, len(helperSets))

	for _, helpers := range helperSets {
		candidate := cmRepairSolution{
			Helpers:     append([]peer.ID(nil), helpers...),
			Destination: current.Destination,
		}

		if cmRepairSameSolution(candidate, current) {
			continue
		}
		if cmRepairCandidateFeasible(task, candidate, topology) {
			out = append(out, candidate)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return cmRepairSolutionKey(out[i]) < cmRepairSolutionKey(out[j])
	})

	return out
}

// cmRepairCTPDownloadAlternatives implements CMRepair Algorithm 1's download
// bottleneck move:
//   - helper set unchanged;
//   - choose another requestor;
//   - requestor cannot be the bottleneck peer.
func cmRepairCTPDownloadAlternatives(
	task *cmRepairTask,
	current cmRepairSolution,
	bottleneck peer.ID,
	topology *NetworkTopology,
) []cmRepairSolution {

	out := make([]cmRepairSolution, 0, len(task.DestinationPeers))

	for _, destination := range task.DestinationPeers {
		if destination == bottleneck ||
			destination == current.Destination {
			continue
		}

		candidate := cmRepairSolution{
			Helpers:     append([]peer.ID(nil), current.Helpers...),
			Destination: destination,
		}

		if cmRepairCandidateFeasible(task, candidate, topology) {
			out = append(out, candidate)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return cmRepairSolutionKey(out[i]) < cmRepairSolutionKey(out[j])
	})

	return out
}

func cmRepairRunCTP(
	tasks []*cmRepairTask,
	initial []cmRepairSolution,
	topology *NetworkTopology,
) []cmRepairSolution {

	solutions := cmRepairCopyMultiSolution(initial)

	for {
		changedInPass := false

		for taskIndex, task := range tasks {
			currentTimes := cmRepairEvaluateMultiSolution(
				tasks,
				solutions,
				topology,
			)

			bottleneck := currentTimes.BottleneckPeer
			if bottleneck == "" || math.IsInf(currentTimes.MT, 1) {
				continue
			}

			current := solutions[taskIndex]
			var alternatives []cmRepairSolution

			if currentTimes.BottleneckKind == "upload" {
				// This stripe is relevant only if it currently obtains a helper
				// from the upload-bottleneck peer.
				if !cmRepairContainsPeer(current.Helpers, bottleneck) {
					continue
				}

				alternatives = cmRepairCTPUploadAlternatives(
					task,
					current,
					bottleneck,
					topology,
				)
			} else {
				// This stripe is relevant only if its current requestor is the
				// download-bottleneck peer.
				if current.Destination != bottleneck {
					continue
				}

				alternatives = cmRepairCTPDownloadAlternatives(
					task,
					current,
					bottleneck,
					topology,
				)
			}

			// Paper behavior: traverse eligible alternatives and accept the
			// FIRST one producing nMT < MT, then jump to the next stripe.
			for _, candidate := range alternatives {
				trial := cmRepairCopyMultiSolution(solutions)
				trial[taskIndex] = cmRepairCopySolution(candidate)

				newTimes := cmRepairEvaluateMultiSolution(
					tasks,
					trial,
					topology,
				)

				if cmRepairLess(newTimes.MT, currentTimes.MT) {
					solutions = trial
					changedInPass = true
					break
				}
			}
		}

		// Algorithm 1 stops when a complete pass performs no substitution.
		if !changedInPass {
			break
		}
	}

	return solutions
}

// =============================================================================
// RTP
// =============================================================================

func cmRepairRunRTP(
	tasks []*cmRepairTask,
	initial []cmRepairSolution,
	topology *NetworkTopology,
	options CMRepairRTPOptions,
) []cmRepairSolution {

	if options.W <= 0 {
		options.W = 4
	}
	if options.MaxComputationTime <= 0 {
		options.MaxComputationTime = 30 * time.Second
	}

	rng := rand.New(rand.NewSource(options.Seed))
	started := time.Now()

	current := cmRepairCopyMultiSolution(initial)
	currentTimes := cmRepairEvaluateMultiSolution(tasks, current, topology)

	best := cmRepairCopyMultiSolution(current)
	bestMT := currentTimes.MT

	consecutiveTolerated := 0

	for consecutiveTolerated < options.W &&
		time.Since(started) < options.MaxComputationTime {

		improvedInPass := false

		// Traverse all stripes and all valid single-stripe solutions.
		for taskIndex, task := range tasks {
			if time.Since(started) >= options.MaxComputationTime {
				break
			}

			candidates := cmRepairAllCandidatesForTask(task, topology)

			for _, candidate := range candidates {
				if time.Since(started) >= options.MaxComputationTime {
					break
				}
				if cmRepairSameSolution(candidate, current[taskIndex]) {
					continue
				}

				trial := cmRepairCopyMultiSolution(current)
				trial[taskIndex] = cmRepairCopySolution(candidate)

				newTimes := cmRepairEvaluateMultiSolution(
					tasks,
					trial,
					topology,
				)

				// RTP greedily accepts any strict improvement encountered while
				// traversing valid single-stripe alternatives.
				if cmRepairLess(newTimes.MT, currentTimes.MT) {
					current = trial
					currentTimes = newTimes
					improvedInPass = true
					consecutiveTolerated = 0

					if cmRepairLess(currentTimes.MT, bestMT) {
						best = cmRepairCopyMultiSolution(current)
						bestMT = currentTimes.MT
					}
				}
			}
		}

		if time.Since(started) >= options.MaxComputationTime {
			break
		}

		if improvedInPass {
			continue
		}

		// Local optimum: tolerate one random non-improving/worse single-stripe
		// solution and continue searching from the new state.
		type toleratedMove struct {
			TaskIndex int
			Solution  cmRepairSolution
			MT        float64
		}

		moves := make([]toleratedMove, 0)

		for taskIndex, task := range tasks {
			candidates := cmRepairAllCandidatesForTask(task, topology)

			for _, candidate := range candidates {
				if cmRepairSameSolution(candidate, current[taskIndex]) {
					continue
				}

				trial := cmRepairCopyMultiSolution(current)
				trial[taskIndex] = cmRepairCopySolution(candidate)

				newTimes := cmRepairEvaluateMultiSolution(
					tasks,
					trial,
					topology,
				)

				// The paper's tolerated move is not better than the old Solu.
				if !cmRepairLess(newTimes.MT, currentTimes.MT) {
					moves = append(moves, toleratedMove{
						TaskIndex: taskIndex,
						Solution:  cmRepairCopySolution(candidate),
						MT:        newTimes.MT,
					})
				}
			}
		}

		if len(moves) == 0 {
			break
		}

		move := moves[rng.Intn(len(moves))]
		current[move.TaskIndex] = cmRepairCopySolution(move.Solution)
		currentTimes = cmRepairEvaluateMultiSolution(tasks, current, topology)
		consecutiveTolerated++

		// Normally a tolerated move cannot improve bestMT, but keeping this
		// guard makes the invariant explicit and safe against floating error.
		if cmRepairLess(currentTimes.MT, bestMT) {
			best = cmRepairCopyMultiSolution(current)
			bestMT = currentTimes.MT
		}
	}

	return best
}

// =============================================================================
// Build public decisions
// =============================================================================

func cmRepairBuildDecisions(
	tasks []*cmRepairTask,
	solutions []cmRepairSolution,
) ([]CMRepairDecision, error) {

	if len(tasks) != len(solutions) {
		return nil, fmt.Errorf(
			"CMRepair: task/solution length mismatch: %d vs %d",
			len(tasks),
			len(solutions),
		)
	}

	decisions := make([]CMRepairDecision, 0, len(tasks))

	for i, task := range tasks {
		solution := solutions[i]
		helpers := make([]CMRepairHelper, 0, len(solution.Helpers))

		for _, helperPeer := range solution.Helpers {
			helper, exists := task.HelperByPeer[helperPeer]
			if !exists {
				return nil, fmt.Errorf(
					"CMRepair: selected source peer %s for shard %s has no exact helper shard",
					helperPeer.String(),
					task.Shard.Name,
				)
			}
			helpers = append(helpers, helper)
		}

		// Keep helper CID/index pairing intact while returning deterministic
		// RS-index order to the repair executor.
		sort.Slice(helpers, func(a, b int) bool {
			if helpers[a].RSIndex != helpers[b].RSIndex {
				return helpers[a].RSIndex < helpers[b].RSIndex
			}
			return helpers[a].CID.String() < helpers[b].CID.String()
		})

		decisions = append(decisions, CMRepairDecision{
			Shard:      task.Shard,
			RepairPeer: solution.Destination,
			Helpers:    helpers,
		})
	}

	return decisions, nil
}

// EncodeCMRepairHelpers returns:
//
//	CID:RSIndex,CID:RSIndex,...
//
// It is provided for the same reason as EncodeSelectiveECHelpers.
func EncodeCMRepairHelpers(helpers []CMRepairHelper) string {
	ordered := append([]CMRepairHelper(nil), helpers...)

	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].RSIndex != ordered[j].RSIndex {
			return ordered[i].RSIndex < ordered[j].RSIndex
		}
		return ordered[i].CID.String() < ordered[j].CID.String()
	})

	values := make([]string, 0, len(ordered))
	for _, helper := range ordered {
		if !helper.CID.Defined() || helper.RSIndex < 0 {
			continue
		}
		values = append(
			values,
			fmt.Sprintf("%s:%d", helper.CID.String(), helper.RSIndex),
		)
	}

	return strings.Join(values, ",")
}

// =============================================================================
// CMRepair Algorithm 3: repair-link ordering
// =============================================================================

// cmRepairOrderLinks computes Algorithm 3's congestion-priority order.
//
// For every selected helper transfer:
//
//	Cu[src]++
//	Cd[dst]++
//
// Then:
//
//	CL = (Cu[src] + Cd[dst]) * TC
//
// where TC is that link's transmission time.
//
// The returned order is descending CL. Deterministic tie breaks are used only
// for reproducibility.
func cmRepairOrderLinks(
	tasks []*cmRepairTask,
	solutions []cmRepairSolution,
	topology *NetworkTopology,
) []CMRepairLink {

	uploadCount := make(map[peer.ID]int)
	downloadCount := make(map[peer.ID]int)

	links := make([]CMRepairLink, 0)

	for taskIndex, solution := range solutions {
		task := tasks[taskIndex]

		for _, helperPeer := range solution.Helpers {
			helper, ok := task.HelperByPeer[helperPeer]
			if !ok {
				continue
			}

			cost := cmRepairTransferCostSeconds(
				topology,
				helperPeer,
				solution.Destination,
				task.ShardMB,
			)
			if math.IsInf(cost, 1) {
				continue
			}

			uploadCount[helperPeer]++
			downloadCount[solution.Destination]++

			links = append(links, CMRepairLink{
				TaskIndex:   taskIndex,
				Shard:       task.Shard,
				Source:      helperPeer,
				Destination: solution.Destination,
				Helper:      helper,
				CostSeconds: cost,
			})
		}
	}

	for i := range links {
		link := &links[i]
		link.CongestionLevel =
			float64(uploadCount[link.Source]+downloadCount[link.Destination]) *
				link.CostSeconds
	}

	sort.SliceStable(links, func(i, j int) bool {
		if !cmRepairEqual(
			links[i].CongestionLevel,
			links[j].CongestionLevel,
		) {
			return links[i].CongestionLevel >
				links[j].CongestionLevel
		}

		if !cmRepairEqual(links[i].CostSeconds, links[j].CostSeconds) {
			return links[i].CostSeconds > links[j].CostSeconds
		}
		if links[i].Source != links[j].Source {
			return links[i].Source.String() < links[j].Source.String()
		}
		if links[i].Destination != links[j].Destination {
			return links[i].Destination.String() <
				links[j].Destination.String()
		}
		return links[i].TaskIndex < links[j].TaskIndex
	})

	return links
}

// =============================================================================
// Common validation / finalization
// =============================================================================

func cmRepairValidateInputs(
	failedPeer peer.ID,
	failedShards []api.Pin,
	livePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,
	getStripe CMRepairStripeInfoFunc,
) error {

	if failedPeer == "" {
		return fmt.Errorf("CMRepair: failed peer is empty")
	}
	if topology == nil {
		return fmt.Errorf("CMRepair: nil network topology")
	}
	if getStripe == nil {
		return fmt.Errorf("CMRepair: nil stripe-information function")
	}
	if chunkMB <= 0 {
		return fmt.Errorf("CMRepair: chunkMB must be positive")
	}
	if len(failedShards) == 0 {
		return nil
	}

	validLive := 0
	for _, p := range cmRepairSortedUniquePeers(livePeers) {
		if p != "" &&
			p != failedPeer &&
			cmRepairValidNode(topology, p) {
			validLive++
		}
	}
	if validLive == 0 {
		return fmt.Errorf("CMRepair: no valid live peers")
	}

	return nil
}

func cmRepairFinalizeSchedule(
	algorithm CMRepairAlgorithm,
	started time.Time,
	tasks []*cmRepairTask,
	solutions []cmRepairSolution,
	topology *NetworkTopology,
) (CMRepairSchedule, error) {

	decisions, err := cmRepairBuildDecisions(tasks, solutions)
	if err != nil {
		return CMRepairSchedule{}, err
	}

	times := cmRepairEvaluateMultiSolution(tasks, solutions, topology)

	return CMRepairSchedule{
		Algorithm:       algorithm,
		Decisions:       decisions,
		MT:              times.MT,
		UploadTime:      times.Upload,
		DownloadTime:    times.Download,
		OrderedLinks:    cmRepairOrderLinks(tasks, solutions, topology),
		ComputationTime: time.Since(started),
	}, nil
}

// =============================================================================
// PUBLIC CTP SCHEDULER
// =============================================================================

// ScheduleCMRepairCTP:
//  1. builds all failed-stripe tasks;
//  2. performs per-stripe AZ exhaustive initialization;
//  3. applies CMRepair Algorithm 1 (CTP);
//  4. returns exact helper CIDs/RS indexes + requestor peer;
//  5. also returns Algorithm-3 link priority order.
func ScheduleCMRepairCTP(
	failedPeer peer.ID,
	failedShards []api.Pin,
	livePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,
	getStripe CMRepairStripeInfoFunc,
) (CMRepairSchedule, error) {

	started := time.Now()

	if err := cmRepairValidateInputs(
		failedPeer,
		failedShards,
		livePeers,
		topology,
		chunkMB,
		getStripe,
	); err != nil {
		return CMRepairSchedule{}, err
	}

	if len(failedShards) == 0 {
		return CMRepairSchedule{
			Algorithm:       CMRepairCTP,
			Decisions:       []CMRepairDecision{},
			UploadTime:      map[peer.ID]float64{},
			DownloadTime:    map[peer.ID]float64{},
			OrderedLinks:    []CMRepairLink{},
			ComputationTime: time.Since(started),
		}, nil
	}

	tasks, err := cmRepairBuildTasks(
		failedPeer,
		failedShards,
		livePeers,
		topology,
		chunkMB,
		getStripe,
	)
	if err != nil {
		return CMRepairSchedule{}, err
	}

	initial, err := cmRepairAZInitialize(tasks, topology)
	if err != nil {
		return CMRepairSchedule{}, err
	}

	finalSolutions := cmRepairRunCTP(
		tasks,
		initial,
		topology,
	)

	return cmRepairFinalizeSchedule(
		CMRepairCTP,
		started,
		tasks,
		finalSolutions,
		topology,
	)
}

// =============================================================================
// PUBLIC RTP SCHEDULER
// =============================================================================

// ScheduleCMRepairRTP:
//  1. builds all failed-stripe tasks;
//  2. performs the same AZ initialization as CTP;
//  3. applies CMRepair Algorithm 2 (RTP);
//  4. returns the best multi-stripe solution seen before w/t stopping;
//  5. returns exact helper CIDs/RS indexes + requestor peer;
//  6. also returns Algorithm-3 link priority order.
func ScheduleCMRepairRTP(
	failedPeer peer.ID,
	failedShards []api.Pin,
	livePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,
	getStripe CMRepairStripeInfoFunc,
	options CMRepairRTPOptions,
) (CMRepairSchedule, error) {

	started := time.Now()

	if err := cmRepairValidateInputs(
		failedPeer,
		failedShards,
		livePeers,
		topology,
		chunkMB,
		getStripe,
	); err != nil {
		return CMRepairSchedule{}, err
	}

	if len(failedShards) == 0 {
		return CMRepairSchedule{
			Algorithm:       CMRepairRTP,
			Decisions:       []CMRepairDecision{},
			UploadTime:      map[peer.ID]float64{},
			DownloadTime:    map[peer.ID]float64{},
			OrderedLinks:    []CMRepairLink{},
			ComputationTime: time.Since(started),
		}, nil
	}

	tasks, err := cmRepairBuildTasks(
		failedPeer,
		failedShards,
		livePeers,
		topology,
		chunkMB,
		getStripe,
	)
	if err != nil {
		return CMRepairSchedule{}, err
	}

	initial, err := cmRepairAZInitialize(tasks, topology)
	if err != nil {
		return CMRepairSchedule{}, err
	}

	finalSolutions := cmRepairRunRTP(
		tasks,
		initial,
		topology,
		options,
	)

	return cmRepairFinalizeSchedule(
		CMRepairRTP,
		started,
		tasks,
		finalSolutions,
		topology,
	)
}
