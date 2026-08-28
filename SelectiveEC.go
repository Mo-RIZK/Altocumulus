package ipfscluster

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

/*
===============================================================================
SelectiveEC scheduler for ALTOCUMULUS
===============================================================================

This file ONLY performs scheduling.

It does NOT execute repairs.

The public function:

    ScheduleSelectiveECBatches(...)

returns:

    SelectiveECSchedule{
        Batches:  []SelectiveECBatch,
        Leftover: []api.Pin,
    }

Each batch contains:

    - the lost shard
    - the selected helper shard CIDs and zero-based RS indexes
    - the selected repair/target peer

The caller must execute:

    batch 0
    wait / separate
    batch 1
    wait / separate
    batch 2
    ...

-------------------------------------------------------------------------------
SelectiveEC Graph 1: source/helper selection
-------------------------------------------------------------------------------

        S -> repair task -> source/helper peer -> T

Capacities:

        S -> task       = N
        task -> peer    = 1
        peer -> T       = N

An edge:

        task -> peer

exists iff that peer stores a surviving shard belonging to the same EC stripe.

For a full batch containing B tasks:

        required max-flow = B * N

The normal SelectiveEC batch size is:

        B = number of live peers

-------------------------------------------------------------------------------
SelectiveEC task replacement
-------------------------------------------------------------------------------

If Graph 1 cannot achieve B*N flow:

1. Find the task having the smallest S->task flow.

2. Let:

       replaceFlow = flow(S -> that task)

3. Construct unsaturatedNodes from:

       a) source peers currently carrying flow for that task

          UNION

       b) source peers whose peer->T edge is not saturated

4. Scan UNSCHEDULED recovery tasks in original order.

5. Select the FIRST candidate satisfying:

       |candidate.SourceSet INTERSECTION unsaturatedNodes|
           > replaceFlow

6. Replace the selected task and rebuild Graph 1.

This follows SelectiveEC's findMostUnsaturate /
updateGraphWithNewReconwork behavior.

-------------------------------------------------------------------------------
SelectiveEC Graph 2: target/replacement-node selection
-------------------------------------------------------------------------------

Graph 2 is the COMPLEMENT of source eligibility:

        S -> repair task -> target peer -> T

Capacities:

        S -> task       = 1
        task -> peer    = 1
        peer -> T       = 1

A task->peer target edge exists only when that peer was NOT a valid source
peer for that task.

Therefore the repair target does not already store a surviving shard from
the same EC stripe.

-------------------------------------------------------------------------------
Important ALTOCUMULUS mapping
-------------------------------------------------------------------------------

get_shards_same_stripe(pin) returns:

    []api.Pin
    []peer.ID
    int
    int

The third returned value ("or" in cluster.go) is used here as N:

    N = number of surviving EC shards required to reconstruct
        one missing shard.

The fourth value is NOT used by SelectiveEC scheduling.

-------------------------------------------------------------------------------
Heterogeneity (TPDS Section 4.3)
-------------------------------------------------------------------------------

SelectiveEC extends the homogeneous flow graphs by weighting node capacities
with the AVAILABLE bandwidth measured for each live peer.

For source/helper selection, let B_out[p] be peer p's available upstream
bandwidth and avgOut the average available upstream bandwidth of all live
peers. The source-peer -> sink capacity is:

        ceil(N * B_out[p] / avgOut)

This is the formula explicitly given in SelectiveEC Section 4.3.

For replacement-node selection, the paper states that it is handled similarly.
The homogeneous replacement-node capacity is 1, so the analogous weighted
capacity used here is:

        ceil(B_in[p] / avgIn)

where B_in[p] is peer p's available downstream bandwidth and avgIn is the
average available downstream bandwidth of all live peers.

No pairwise bandwidth, disk-speed weight, or extra scheduling optimization is
introduced here.

===============================================================================
*/

// =============================================================================
// Public structures
// =============================================================================

// SelectiveECBandwidth contains the real-time AVAILABLE bandwidth used by
// SelectiveEC Section 4.3.
//
// Out: available upstream/upload bandwidth.
// In:  available downstream/download bandwidth.
//
// Any common unit may be used (Mb/s, MB/s, etc.), provided all peers use the
// same unit.
type SelectiveECBandwidth struct {
	Out float64
	In  float64
}

// SelectiveECHelper identifies one exact surviving shard selected by
// SelectiveEC for reconstruction.
//
// CID is the CID of the selected surviving shard.
//
// RSIndex is the zero-based position of that shard inside the RS stripe:
//
//	0 ... (N+K-1)
//
// The scheduler returns exactly N helpers for each repair decision, where N is
// the number of shards required by the Reed-Solomon decoder.
type SelectiveECHelper struct {
	CID     cid.Cid
	RSIndex int
}

// SelectiveECDecision is one repair assignment inside one SelectiveEC batch.
type SelectiveECDecision struct {
	Shard      api.Pin
	RepairPeer peer.ID
	Helpers    []SelectiveECHelper
}

// SelectiveECBatch contains all repairs belonging to one SelectiveEC batch.
//
// All decisions inside one batch may execute concurrently.
//
// Batches themselves must be executed in order.
type SelectiveECBatch struct {
	Index     int
	Decisions []SelectiveECDecision
}

// SelectiveECSchedule is the complete SelectiveEC scheduling result.
type SelectiveECSchedule struct {
	Batches  []SelectiveECBatch
	Leftover []api.Pin
}

// SelectiveECStripeInfoFunc adapts:
//
//	c.get_shards_same_stripe(pin)
//
// Expected return:
//
//	sameStripeShards
//	allocations
//	reconstructionN
//	chunkCount
type SelectiveECStripeInfoFunc func(
	api.Pin,
) ([]api.Pin, []peer.ID, int, int)

// =============================================================================
// Internal task representation
// =============================================================================

type selectiveECTask struct {
	Shard api.Pin
	Key   string

	// Number of distinct surviving shards required for reconstruction.
	N int

	// Every live peer which stores at least one surviving same-stripe shard.
	SourcePeers []peer.ID

	// Fast eligibility lookup.
	SourceSet map[peer.ID]bool

	// One exact surviving helper shard associated with every source peer.
	//
	// The public helper information contains only what the repair executor
	// needs: the selected shard CID and its zero-based RS index.
	//
	// The peer itself remains internal to the scheduling graph.
	HelperByPeer map[peer.ID]SelectiveECHelper
}

// =============================================================================
// Utility functions
// =============================================================================

func selectiveECSortedUniquePeers(in []peer.ID) []peer.ID {
	seen := make(map[peer.ID]bool)

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

// EncodeSelectiveECHelpers returns:
//
//	CID:RSIndex,CID:RSIndex,...
//
// This helper is optional for the scheduler itself. It is provided so
// cluster.go can serialize the selected helpers into pin metadata before
// dispatching the repair.
func EncodeSelectiveECHelpers(
	helpers []SelectiveECHelper,
) string {

	ordered := append(
		[]SelectiveECHelper(nil),
		helpers...,
	)

	sort.Slice(
		ordered,
		func(i, j int) bool {
			if ordered[i].RSIndex != ordered[j].RSIndex {
				return ordered[i].RSIndex < ordered[j].RSIndex
			}

			return ordered[i].CID.String() <
				ordered[j].CID.String()
		},
	)

	values := make(
		[]string,
		0,
		len(ordered),
	)

	for _, helper := range ordered {
		if !helper.CID.Defined() ||
			helper.RSIndex < 0 {

			continue
		}

		values = append(
			values,
			fmt.Sprintf(
				"%s:%d",
				helper.CID.String(),
				helper.RSIndex,
			),
		)
	}

	return strings.Join(values, ",")
}

// selectiveECTotalStripeShards extracts N and K from the RS(...) portion of
// the shard name and returns N+K.
//
// The repair executor uses the same zero-based RS-index convention:
//
//	rsIndex = (globalShardNumber - 1) % (N + K)
func selectiveECTotalStripeShards(
	shardName string,
) (int, error) {

	openParenthesis :=
		strings.Index(
			shardName,
			"(",
		)

	closeParenthesis :=
		strings.Index(
			shardName,
			")",
		)

	if openParenthesis == -1 ||
		closeParenthesis == -1 ||
		closeParenthesis <= openParenthesis+1 {

		return 0,
			fmt.Errorf(
				"SelectiveEC: cannot extract RS parameters from shard name %q",
				shardName,
			)
	}

	parametersString :=
		shardName[openParenthesis+1 : closeParenthesis]

	parameters :=
		strings.Split(
			parametersString,
			",",
		)

	if len(parameters) != 2 {
		return 0,
			fmt.Errorf(
				"SelectiveEC: invalid RS parameter string %q in shard %q",
				parametersString,
				shardName,
			)
	}

	dataShards, err :=
		strconv.Atoi(
			strings.TrimSpace(
				parameters[0],
			),
		)

	if err != nil ||
		dataShards <= 0 {

		return 0,
			fmt.Errorf(
				"SelectiveEC: invalid data-shard count %q in shard %q",
				parameters[0],
				shardName,
			)
	}

	parityShards, err :=
		strconv.Atoi(
			strings.TrimSpace(
				parameters[1],
			),
		)

	if err != nil ||
		parityShards < 0 {

		return 0,
			fmt.Errorf(
				"SelectiveEC: invalid parity-shard count %q in shard %q",
				parameters[1],
				shardName,
			)
	}

	total :=
		dataShards +
			parityShards

	if total <= 0 {
		return 0,
			fmt.Errorf(
				"SelectiveEC: invalid total stripe width %d for shard %q",
				total,
				shardName,
			)
	}

	return total, nil
}

// =============================================================================
// Residual max-flow graph
// =============================================================================

type selectiveECEdge struct {
	To int

	// Reverse-edge index inside Adj[To].
	Rev int

	// Current residual capacity.
	Cap int

	// Initial forward capacity.
	Initial int
}

type selectiveECEdgeRef struct {
	From  int
	Index int
}

type selectiveECFlowGraph struct {
	Adj [][]selectiveECEdge
}

func newSelectiveECFlowGraph(
	nodeCount int,
) *selectiveECFlowGraph {

	return &selectiveECFlowGraph{
		Adj: make([][]selectiveECEdge, nodeCount),
	}
}

func (g *selectiveECFlowGraph) AddEdge(
	from int,
	to int,
	capacity int,
) selectiveECEdgeRef {

	forwardIndex := len(g.Adj[from])
	reverseIndex := len(g.Adj[to])

	forward := selectiveECEdge{
		To:      to,
		Rev:     reverseIndex,
		Cap:     capacity,
		Initial: capacity,
	}

	reverse := selectiveECEdge{
		To:      from,
		Rev:     forwardIndex,
		Cap:     0,
		Initial: 0,
	}

	g.Adj[from] = append(
		g.Adj[from],
		forward,
	)

	g.Adj[to] = append(
		g.Adj[to],
		reverse,
	)

	return selectiveECEdgeRef{
		From:  from,
		Index: forwardIndex,
	}
}

func (g *selectiveECFlowGraph) Flow(
	ref selectiveECEdgeRef,
) int {

	edge := g.Adj[ref.From][ref.Index]

	return edge.Initial - edge.Cap
}

func (g *selectiveECFlowGraph) Capacity(
	ref selectiveECEdgeRef,
) int {

	return g.Adj[ref.From][ref.Index].Initial
}

// MaxFlow implements integer residual augmenting-path max-flow.
//
// BFS is used to find augmenting paths.
//
// This reproduces the same max-flow constraints as SelectiveEC.
// Exact tie-breaking among multiple equivalent maximum flows may differ
// from the Java implementation.
func (g *selectiveECFlowGraph) MaxFlow(
	source int,
	sink int,
) int {

	totalFlow := 0
	nodeCount := len(g.Adj)

	for {
		parentNode := make(
			[]int,
			nodeCount,
		)

		parentEdge := make(
			[]int,
			nodeCount,
		)

		for i := 0; i < nodeCount; i++ {
			parentNode[i] = -1
			parentEdge[i] = -1
		}

		queue := make(
			[]int,
			0,
			nodeCount,
		)

		queue = append(
			queue,
			source,
		)

		parentNode[source] = source

		// -------------------------------------------------------------
		// Find one augmenting path.
		// -------------------------------------------------------------

		for len(queue) > 0 &&
			parentNode[sink] == -1 {

			u := queue[0]
			queue = queue[1:]

			for edgeIndex, edge := range g.Adj[u] {

				if edge.Cap <= 0 {
					continue
				}

				if parentNode[edge.To] != -1 {
					continue
				}

				parentNode[edge.To] = u
				parentEdge[edge.To] = edgeIndex

				queue = append(
					queue,
					edge.To,
				)

				if edge.To == sink {
					break
				}
			}
		}

		if parentNode[sink] == -1 {
			break
		}

		// -------------------------------------------------------------
		// Determine bottleneck capacity.
		// -------------------------------------------------------------

		augment := int(^uint(0) >> 1)

		for v := sink; v != source; {
			u := parentNode[v]
			edgeIndex := parentEdge[v]

			if g.Adj[u][edgeIndex].Cap <
				augment {

				augment =
					g.Adj[u][edgeIndex].Cap
			}

			v = u
		}

		// -------------------------------------------------------------
		// Update residual graph.
		// -------------------------------------------------------------

		for v := sink; v != source; {
			u := parentNode[v]
			edgeIndex := parentEdge[v]

			reverseIndex :=
				g.Adj[u][edgeIndex].Rev

			g.Adj[u][edgeIndex].Cap -=
				augment

			g.Adj[v][reverseIndex].Cap +=
				augment

			v = u
		}

		totalFlow += augment
	}

	return totalFlow
}

// =============================================================================
// Build SelectiveEC repair tasks from ALTOCUMULUS state
// =============================================================================

func selectiveECBuildTask(
	shard api.Pin,
	failedPeer peer.ID,
	livePeers []peer.ID,
	getStripe SelectiveECStripeInfoFunc,
) (*selectiveECTask, error) {

	sameStripeShards,
		_,
		reconstructionN,
		_ := getStripe(shard)

	if reconstructionN <= 0 {
		return nil,
			fmt.Errorf(
				"SelectiveEC: invalid reconstruction N=%d for shard %s",
				reconstructionN,
				shard.Name,
			)
	}

	totalStripeShards, err :=
		selectiveECTotalStripeShards(
			shard.Name,
		)

	if err != nil {
		return nil, err
	}

	failedShardNumber,
		_,
		err :=
		getShardNumber(
			shard.Name,
		)

	if err != nil {
		return nil,
			fmt.Errorf(
				"SelectiveEC: cannot parse failed shard number from %q: %w",
				shard.Name,
				err,
			)
	}

	if failedShardNumber <= 0 {
		return nil,
			fmt.Errorf(
				"SelectiveEC: invalid failed shard number %d for shard %s",
				failedShardNumber,
				shard.Name,
			)
	}

	failedRSIndex :=
		(failedShardNumber - 1) %
			totalStripeShards

	liveSet := make(
		map[peer.ID]bool,
		len(livePeers),
	)

	for _, p := range livePeers {
		if p == "" ||
			p == failedPeer {

			continue
		}

		liveSet[p] = true
	}

	helperByPeer :=
		make(
			map[peer.ID]SelectiveECHelper,
		)

	// -----------------------------------------------------------------
	// Determine every source-eligible live peer.
	//
	// For each surviving shard in the same stripe, compute the exact
	// zero-based RS position that the repair executor will use.
	//
	// The failed RS index is explicitly excluded.
	// -----------------------------------------------------------------

	for _, survivingShard := range sameStripeShards {

		globalShardNumber,
			_,
			parseErr :=
			getShardNumber(
				survivingShard.Name,
			)

		if parseErr != nil {
			return nil,
				fmt.Errorf(
					"SelectiveEC: cannot parse surviving shard number from %q: %w",
					survivingShard.Name,
					parseErr,
				)
		}

		if globalShardNumber <= 0 {
			return nil,
				fmt.Errorf(
					"SelectiveEC: invalid surviving shard number %d for %q",
					globalShardNumber,
					survivingShard.Name,
				)
		}

		rsIndex :=
			(globalShardNumber - 1) %
				totalStripeShards

		// The missing shard can never be used as a helper.
		if rsIndex ==
			failedRSIndex {

			continue
		}

		helper :=
			SelectiveECHelper{
				CID:     survivingShard.Cid.Cid,
				RSIndex: rsIndex,
			}

		if !helper.CID.Defined() {
			return nil,
				fmt.Errorf(
					"SelectiveEC: surviving shard %q has an undefined CID",
					survivingShard.Name,
				)
		}

		for _, allocation := range survivingShard.Allocations {

			if allocation == "" ||
				allocation == failedPeer ||
				!liveSet[allocation] {

				continue
			}

			existing, exists :=
				helperByPeer[allocation]

			// If one peer stores more than one surviving shard from the same
			// stripe, keep one deterministic candidate: lowest RS index, then
			// lowest CID on an RS-index tie.
			if !exists ||
				helper.RSIndex <
					existing.RSIndex ||
				(helper.RSIndex ==
					existing.RSIndex &&
					helper.CID.String() <
						existing.CID.String()) {

				helperByPeer[allocation] =
					helper
			}
		}
	}

	sourcePeers := make(
		[]peer.ID,
		0,
		len(helperByPeer),
	)

	sourceSet := make(
		map[peer.ID]bool,
		len(helperByPeer),
	)

	for p := range helperByPeer {
		sourcePeers =
			append(
				sourcePeers,
				p,
			)

		sourceSet[p] = true
	}

	sourcePeers =
		selectiveECSortedUniquePeers(
			sourcePeers,
		)

	if len(sourcePeers) <
		reconstructionN {

		return nil,
			fmt.Errorf(
				"SelectiveEC: shard %s requires %d distinct source peers, only %d are available",
				shard.Name,
				reconstructionN,
				len(sourcePeers),
			)
	}

	return &selectiveECTask{
		Shard: shard,
		Key:   shard.Cid.String(),

		N: reconstructionN,

		SourcePeers: sourcePeers,

		SourceSet: sourceSet,

		HelperByPeer: helperByPeer,
	}, nil
}

func selectiveECBuildTasks(
	shards []api.Pin,
	failedPeer peer.ID,
	livePeers []peer.ID,
	getStripe SelectiveECStripeInfoFunc,
) ([]*selectiveECTask, error) {

	tasks := make(
		[]*selectiveECTask,
		0,
		len(shards),
	)

	for _, shard := range shards {
		task, err :=
			selectiveECBuildTask(
				shard,
				failedPeer,
				livePeers,
				getStripe,
			)

		if err != nil {
			return nil, err
		}

		tasks =
			append(
				tasks,
				task,
			)
	}

	return tasks, nil
}

// =============================================================================
// TPDS Section 4.3 bandwidth helpers
// =============================================================================

func selectiveECAverageBandwidth(
	livePeers []peer.ID,
	bandwidth map[peer.ID]SelectiveECBandwidth,
	upstream bool,
) (float64, error) {

	if len(livePeers) == 0 {
		return 0,
			fmt.Errorf(
				"SelectiveEC: cannot compute average bandwidth with no live peers",
			)
	}

	total := 0.0

	for _, p := range livePeers {
		bw, exists := bandwidth[p]

		if !exists {
			return 0,
				fmt.Errorf(
					"SelectiveEC: missing bandwidth information for peer %s",
					p.String(),
				)
		}

		value := bw.In

		if upstream {
			value = bw.Out
		}

		if value < 0 {
			return 0,
				fmt.Errorf(
					"SelectiveEC: negative available bandwidth %.6f for peer %s",
					value,
					p.String(),
				)
		}

		total += value
	}

	average :=
		total /
			float64(len(livePeers))

	if average <= 0 {
		direction := "downstream"

		if upstream {
			direction = "upstream"
		}

		return 0,
			fmt.Errorf(
				"SelectiveEC: average available %s bandwidth must be > 0",
				direction,
			)
	}

	return average, nil
}

func selectiveECWeightedCapacity(
	baseCapacity int,
	available float64,
	average float64,
) int {

	if baseCapacity <= 0 ||
		available <= 0 ||
		average <= 0 {

		return 0
	}

	return int(
		math.Ceil(
			float64(baseCapacity) *
				available /
				average,
		),
	)
}

// =============================================================================
// Graph 1: source/helper selection
// =============================================================================

type selectiveECSourceGraph struct {
	Graph *selectiveECFlowGraph

	Source int
	Sink   int

	// S -> task edges.
	TaskSourceEdges []selectiveECEdgeRef

	// taskIndex -> sourcePeer -> task->peer edge.
	TaskPeerEdges []map[peer.ID]selectiveECEdgeRef

	// sourcePeer -> peer->T edge.
	PeerSinkEdges map[peer.ID]selectiveECEdgeRef
}

func selectiveECBuildSourceGraph(
	batch []*selectiveECTask,
	livePeers []peer.ID,
	n int,
	bandwidth map[peer.ID]SelectiveECBandwidth,
) (*selectiveECSourceGraph, error) {

	batchSize := len(batch)
	peerCount := len(livePeers)

	avgOut, err :=
		selectiveECAverageBandwidth(
			livePeers,
			bandwidth,
			true,
		)

	if err != nil {
		return nil, err
	}

	/*
	   Vertex layout:

	       0
	           source

	       1 ... batchSize
	           repair tasks

	       batchSize+1 ...
	           source peers

	       last
	           sink
	*/

	source := 0
	taskStart := 1

	peerStart :=
		taskStart +
			batchSize

	sink :=
		peerStart +
			peerCount

	graph :=
		newSelectiveECFlowGraph(
			sink + 1,
		)

	peerNode := make(
		map[peer.ID]int,
		peerCount,
	)

	for i, p := range livePeers {
		peerNode[p] =
			peerStart + i
	}

	taskSourceEdges :=
		make(
			[]selectiveECEdgeRef,
			batchSize,
		)

	taskPeerEdges :=
		make(
			[]map[peer.ID]selectiveECEdgeRef,
			batchSize,
		)

	// -----------------------------------------------------------------
	// S -> task capacity N.
	// -----------------------------------------------------------------

	for taskIndex, task := range batch {

		taskNode :=
			taskStart +
				taskIndex

		taskSourceEdges[taskIndex] =
			graph.AddEdge(
				source,
				taskNode,
				n,
			)

		taskPeerEdges[taskIndex] =
			make(
				map[peer.ID]selectiveECEdgeRef,
			)

		// -------------------------------------------------------------
		// task -> eligible source peer capacity 1.
		// -------------------------------------------------------------

		for _, p := range task.SourcePeers {

			node, exists :=
				peerNode[p]

			if !exists {
				continue
			}

			ref :=
				graph.AddEdge(
					taskNode,
					node,
					1,
				)

			taskPeerEdges[taskIndex][p] =
				ref
		}
	}

	peerSinkEdges :=
		make(
			map[peer.ID]selectiveECEdgeRef,
			peerCount,
		)

	// -----------------------------------------------------------------
	// TPDS Section 4.3:
	//
	// source peer p -> T capacity:
	//
	//     ceil(N * B_out[p] / avgOut)
	//
	// B_out[p] is the peer's AVAILABLE upstream bandwidth.
	// -----------------------------------------------------------------

	for _, p := range livePeers {
		capacity :=
			selectiveECWeightedCapacity(
				n,
				bandwidth[p].Out,
				avgOut,
			)

		ref :=
			graph.AddEdge(
				peerNode[p],
				sink,
				capacity,
			)

		peerSinkEdges[p] =
			ref
	}

	return &selectiveECSourceGraph{
		Graph: graph,

		Source: source,

		Sink: sink,

		TaskSourceEdges: taskSourceEdges,

		TaskPeerEdges: taskPeerEdges,

		PeerSinkEdges: peerSinkEdges,
	}, nil
}

// =============================================================================
// SelectiveEC unsaturated-task replacement
// =============================================================================

// Return the task position in the CURRENT candidate batch having the smallest
// achieved S->task flow.
//
// Using '<' instead of '<=' means the first task wins ties.
func selectiveECMostUnsaturatedTask(
	sourceGraph *selectiveECSourceGraph,
) int {

	worstTask := -1
	worstFlow :=
		int(^uint(0) >> 1)

	for taskIndex, edge := range sourceGraph.TaskSourceEdges {

		flow :=
			sourceGraph.Graph.Flow(
				edge,
			)

		if flow < worstFlow {
			worstFlow = flow
			worstTask = taskIndex
		}
	}

	return worstTask
}

// Build the SelectiveEC unsaturatedNodes set for the selected task.
//
// This contains:
//
//  1. source nodes CURRENTLY carrying flow for the selected task
//
//     UNION
//
//  2. every source node whose peer->sink flow is below its weighted capacity
func selectiveECReplacementNodeSet(
	sourceGraph *selectiveECSourceGraph,
	taskPosition int,
) map[peer.ID]bool {

	nodes := make(
		map[peer.ID]bool,
	)

	// -----------------------------------------------------------------
	// Nodes already carrying flow for this task.
	// -----------------------------------------------------------------

	if taskPosition >= 0 &&
		taskPosition <
			len(sourceGraph.TaskPeerEdges) {

		for p, edge := range sourceGraph.
			TaskPeerEdges[taskPosition] {

			if sourceGraph.Graph.Flow(edge) >
				0 {

				nodes[p] = true
			}
		}
	}

	// -----------------------------------------------------------------
	// Globally under-saturated source nodes.
	// -----------------------------------------------------------------

	for p, edge := range sourceGraph.PeerSinkEdges {

		if sourceGraph.Graph.Flow(edge) !=
			sourceGraph.Graph.Capacity(edge) {

			nodes[p] = true
		}
	}

	return nodes
}

func selectiveECIntersectionCount(
	task *selectiveECTask,
	nodes map[peer.ID]bool,
) int {

	count := 0

	for _, p := range task.SourcePeers {

		if nodes[p] {
			count++
		}
	}

	return count
}

// selectiveECReplaceUnsaturatedTask reproduces the SelectiveEC replacement
// rule:
//
//  1. Find most unsaturated current task.
//
//  2. replaceFlow = flow(S -> current task).
//
//  3. Build unsaturatedNodes.
//
//  4. Scan all UNSCHEDULED tasks in ORIGINAL ORDER.
//
//  5. Pick the FIRST candidate where:
//
//     intersection(candidateSources, unsaturatedNodes) > replaceFlow
//
//  6. Return old task to unscheduled pool.
func selectiveECReplaceUnsaturatedTask(
	allTasks []*selectiveECTask,
	batchIndexes []int,
	used []bool,
	sourceGraph *selectiveECSourceGraph,
) bool {

	position :=
		selectiveECMostUnsaturatedTask(
			sourceGraph,
		)

	if position < 0 ||
		position >=
			len(batchIndexes) {

		return false
	}

	oldTaskIndex :=
		batchIndexes[position]

	replaceFlow :=
		sourceGraph.Graph.Flow(
			sourceGraph.
				TaskSourceEdges[position],
		)

	unsaturatedNodes :=
		selectiveECReplacementNodeSet(
			sourceGraph,
			position,
		)

	// -----------------------------------------------------------------
	// IMPORTANT:
	//
	// Scan remaining tasks in ORIGINAL task order and accept FIRST one
	// satisfying the SelectiveEC condition.
	// -----------------------------------------------------------------

	replacementIndex := -1

	for candidateIndex, candidate := range allTasks {

		// Already selected in this or an earlier batch.
		if used[candidateIndex] {
			continue
		}

		// SelectiveEC assumes the same EC policy.
		if candidate.N != allTasks[oldTaskIndex].N {
			continue
		}

		intersection :=
			selectiveECIntersectionCount(
				candidate,
				unsaturatedNodes,
			)

		if intersection >
			replaceFlow {

			replacementIndex =
				candidateIndex

			break
		}
	}

	if replacementIndex < 0 {
		return false
	}

	// -----------------------------------------------------------------
	// Displaced task returns to unscheduled pool.
	// -----------------------------------------------------------------

	used[oldTaskIndex] =
		false

	// -----------------------------------------------------------------
	// Replacement enters the current batch.
	// -----------------------------------------------------------------

	used[replacementIndex] =
		true

	batchIndexes[position] =
		replacementIndex

	return true
}

// =============================================================================
// Extract Graph-1 helper assignments
// =============================================================================

func selectiveECExtractHelpers(
	batch []*selectiveECTask,
	sourceGraph *selectiveECSourceGraph,
	n int,
) ([][]SelectiveECHelper, error) {

	allHelpers :=
		make(
			[][]SelectiveECHelper,
			len(batch),
		)

	for taskIndex, task := range batch {

		helpers :=
			make(
				[]SelectiveECHelper,
				0,
				n,
			)

		for _, p := range task.SourcePeers {

			edge, exists :=
				sourceGraph.
					TaskPeerEdges[taskIndex][p]

			if !exists {
				continue
			}

			if sourceGraph.Graph.Flow(edge) <=
				0 {

				continue
			}

			helper, exists :=
				task.HelperByPeer[p]

			if !exists {
				return nil,
					fmt.Errorf(
						"SelectiveEC: source peer %s selected for shard %s but no helper shard exists",
						p.String(),
						task.Shard.Name,
					)
			}

			helpers =
				append(
					helpers,
					helper,
				)
		}

		sort.Slice(
			helpers,
			func(i, j int) bool {
				if helpers[i].RSIndex !=
					helpers[j].RSIndex {

					return helpers[i].
						RSIndex <
						helpers[j].
							RSIndex
				}

				return helpers[i].
					CID.String() <
					helpers[j].
						CID.String()
			},
		)

		if len(helpers) != n {
			return nil,
				fmt.Errorf(
					"SelectiveEC: task %s received %d helpers, expected %d",
					task.Shard.Name,
					len(helpers),
					n,
				)
		}

		// Reed-Solomon reconstruction requires distinct RS positions.
		selectedRSIndexes :=
			make(
				map[int]bool,
				len(helpers),
			)

		for _, helper := range helpers {
			if selectedRSIndexes[helper.RSIndex] {
				return nil,
					fmt.Errorf(
						"SelectiveEC: task %s selected duplicate RS index %d",
						task.Shard.Name,
						helper.RSIndex,
					)
			}

			selectedRSIndexes[helper.RSIndex] =
				true
		}

		allHelpers[taskIndex] =
			helpers
	}

	return allHelpers, nil
}

// =============================================================================
// Graph 2: repair/target-node selection
// =============================================================================

type selectiveECTargetGraph struct {
	Graph *selectiveECFlowGraph

	Source int
	Sink   int

	TaskPeerEdges []map[peer.ID]selectiveECEdgeRef

	// replacementPeer -> peer->T edge.
	PeerSinkEdges map[peer.ID]selectiveECEdgeRef
}

func selectiveECBuildTargetGraph(
	batch []*selectiveECTask,
	livePeers []peer.ID,
	bandwidth map[peer.ID]SelectiveECBandwidth,
) (*selectiveECTargetGraph, error) {

	batchSize := len(batch)
	peerCount := len(livePeers)

	avgIn, err :=
		selectiveECAverageBandwidth(
			livePeers,
			bandwidth,
			false,
		)

	if err != nil {
		return nil, err
	}

	source := 0
	taskStart := 1

	peerStart :=
		taskStart +
			batchSize

	sink :=
		peerStart +
			peerCount

	graph :=
		newSelectiveECFlowGraph(
			sink + 1,
		)

	peerNode := make(
		map[peer.ID]int,
		peerCount,
	)

	for i, p := range livePeers {
		peerNode[p] =
			peerStart + i
	}

	taskPeerEdges :=
		make(
			[]map[peer.ID]selectiveECEdgeRef,
			batchSize,
		)

	for taskIndex, task := range batch {

		taskNode :=
			taskStart +
				taskIndex

		// -------------------------------------------------------------
		// S -> task = 1.
		// -------------------------------------------------------------

		graph.AddEdge(
			source,
			taskNode,
			1,
		)

		taskPeerEdges[taskIndex] =
			make(
				map[peer.ID]selectiveECEdgeRef,
			)

		// -------------------------------------------------------------
		// Complement of ALL Graph-1 source eligibility.
		//
		// IMPORTANT:
		//
		// We complement task.SourceSet, not merely the N helper edges
		// selected by max-flow.
		// -------------------------------------------------------------

		for _, p := range livePeers {

			if task.SourceSet[p] {
				continue
			}

			ref :=
				graph.AddEdge(
					taskNode,
					peerNode[p],
					1,
				)

			taskPeerEdges[taskIndex][p] =
				ref
		}
	}

	peerSinkEdges :=
		make(
			map[peer.ID]selectiveECEdgeRef,
			peerCount,
		)

	// -----------------------------------------------------------------
	// TPDS Section 4.3:
	//
	// Replacement-node selection is handled similarly with AVAILABLE
	// downstream bandwidth. The homogeneous replacement capacity is 1:
	//
	//     ceil(B_in[p] / avgIn)
	// -----------------------------------------------------------------

	for _, p := range livePeers {
		capacity :=
			selectiveECWeightedCapacity(
				1,
				bandwidth[p].In,
				avgIn,
			)

		ref :=
			graph.AddEdge(
				peerNode[p],
				sink,
				capacity,
			)

		peerSinkEdges[p] =
			ref
	}

	return &selectiveECTargetGraph{
		Graph: graph,

		Source: source,

		Sink: sink,

		TaskPeerEdges: taskPeerEdges,

		PeerSinkEdges: peerSinkEdges,
	}, nil
}

// Extract matched repair nodes.
//
// If Graph 2 does not provide a target for a task, use the supplementary
// heuristic described in the paper: choose the lightest-loaded valid
// replacement node.
func selectiveECExtractTargets(
	batch []*selectiveECTask,
	livePeers []peer.ID,
	targetGraph *selectiveECTargetGraph,
) ([]peer.ID, error) {

	targets :=
		make(
			[]peer.ID,
			len(batch),
		)

	// -----------------------------------------------------------------
	// First extract target assignments produced by max-flow.
	// -----------------------------------------------------------------

	for taskIndex := range batch {

		for _, p := range livePeers {

			edge, exists :=
				targetGraph.
					TaskPeerEdges[taskIndex][p]

			if !exists {
				continue
			}

			if targetGraph.Graph.Flow(edge) <=
				0 {

				continue
			}

			targets[taskIndex] =
				p

			break
		}
	}

	// -----------------------------------------------------------------
	// Supplementary heuristic described in the paper:
	//
	// choose the lightest-loaded valid replacement node for each
	// unmatched task.
	// -----------------------------------------------------------------

	for taskIndex, task := range batch {

		if targets[taskIndex] != "" {
			continue
		}

		bestPeer := peer.ID("")
		bestLoad := int(^uint(0) >> 1)

		for _, p := range livePeers {

			if task.SourceSet[p] {
				continue
			}

			edge, exists :=
				targetGraph.PeerSinkEdges[p]

			if !exists {
				continue
			}

			load :=
				targetGraph.Graph.Flow(
					edge,
				)

			if bestPeer == "" ||
				load < bestLoad ||
				(load == bestLoad &&
					p.String() < bestPeer.String()) {

				bestPeer = p
				bestLoad = load
			}
		}

		if bestPeer == "" {
			return nil,
				fmt.Errorf(
					"SelectiveEC: task %s has no valid target peer",
					task.Shard.Name,
				)
		}

		targets[taskIndex] =
			bestPeer
	}

	return targets, nil
}

// =============================================================================
// Schedule one complete SelectiveEC batch
// =============================================================================

func selectiveECScheduleOneBatch(
	allTasks []*selectiveECTask,
	batchIndexes []int,
	used []bool,
	livePeers []peer.ID,
	bandwidth map[peer.ID]SelectiveECBandwidth,
) (SelectiveECBatch, error) {

	if len(batchIndexes) == 0 {
		return SelectiveECBatch{},
			fmt.Errorf(
				"SelectiveEC: empty batch",
			)
	}

	n :=
		allTasks[batchIndexes[0]].N

	// -----------------------------------------------------------------
	// SelectiveEC normally uses one configured EC policy.
	// -----------------------------------------------------------------

	for _, index := range batchIndexes {

		if allTasks[index].N != n {
			return SelectiveECBatch{},
				fmt.Errorf(
					"SelectiveEC: mixed N values in one batch: %d and %d",
					n,
					allTasks[index].N,
				)
		}
	}

	expectedFlow :=
		len(batchIndexes) *
			n

	var finalBatch []*selectiveECTask

	var finalSourceGraph *selectiveECSourceGraph

	// -----------------------------------------------------------------
	// Repeatedly construct Graph 1 and apply SelectiveEC replacement
	// until the batch reaches complete flow.
	// -----------------------------------------------------------------

	maxAttempts :=
		len(allTasks) *
			len(batchIndexes)

	if maxAttempts < 1 {
		maxAttempts = 1
	}

	for attempt := 0; attempt <= maxAttempts; attempt++ {

		currentBatch :=
			make(
				[]*selectiveECTask,
				len(batchIndexes),
			)

		for position, taskIndex := range batchIndexes {

			currentBatch[position] =
				allTasks[taskIndex]
		}

		sourceGraph, err :=
			selectiveECBuildSourceGraph(
				currentBatch,
				livePeers,
				n,
				bandwidth,
			)

		if err != nil {
			return SelectiveECBatch{}, err
		}

		maxFlow :=
			sourceGraph.Graph.MaxFlow(
				sourceGraph.Source,
				sourceGraph.Sink,
			)

		// -------------------------------------------------------------
		// Full n-regular source assignment found.
		// -------------------------------------------------------------

		if maxFlow ==
			expectedFlow {

			finalBatch =
				currentBatch

			finalSourceGraph =
				sourceGraph

			break
		}

		// -------------------------------------------------------------
		// Otherwise use remaining unscheduled tasks to improve batch.
		// -------------------------------------------------------------

		replaced :=
			selectiveECReplaceUnsaturatedTask(
				allTasks,
				batchIndexes,
				used,
				sourceGraph,
			)

		if !replaced {
			return SelectiveECBatch{},
				fmt.Errorf(
					"SelectiveEC: source graph cannot reach full flow: got %d expected %d",
					maxFlow,
					expectedFlow,
				)
		}
	}

	if finalSourceGraph == nil {
		return SelectiveECBatch{},
			fmt.Errorf(
				"SelectiveEC: source scheduling failed to converge",
			)
	}

	// -----------------------------------------------------------------
	// Retrieve exact N helper shards from Graph 1.
	// -----------------------------------------------------------------

	helpers, err :=
		selectiveECExtractHelpers(
			finalBatch,
			finalSourceGraph,
			n,
		)

	if err != nil {
		return SelectiveECBatch{},
			err
	}

	// -----------------------------------------------------------------
	// Construct complement Graph 2.
	// -----------------------------------------------------------------

	targetGraph, err :=
		selectiveECBuildTargetGraph(
			finalBatch,
			livePeers,
			bandwidth,
		)

	if err != nil {
		return SelectiveECBatch{},
			err
	}

	targetGraph.Graph.MaxFlow(
		targetGraph.Source,
		targetGraph.Sink,
	)

	targets, err :=
		selectiveECExtractTargets(
			finalBatch,
			livePeers,
			targetGraph,
		)

	if err != nil {
		return SelectiveECBatch{},
			err
	}

	// -----------------------------------------------------------------
	// Construct final decisions.
	// -----------------------------------------------------------------

	decisions :=
		make(
			[]SelectiveECDecision,
			0,
			len(finalBatch),
		)

	for taskIndex, task := range finalBatch {

		if targets[taskIndex] == "" {
			return SelectiveECBatch{},
				fmt.Errorf(
					"SelectiveEC: no repair peer selected for shard %s",
					task.Shard.Name,
				)
		}

		decisions =
			append(
				decisions,
				SelectiveECDecision{
					Shard: task.Shard,

					RepairPeer: targets[taskIndex],

					Helpers: helpers[taskIndex],
				},
			)
	}

	return SelectiveECBatch{
		Decisions: decisions,
	}, nil
}

// =============================================================================
// PUBLIC SELECTIVEEC SCHEDULER
// =============================================================================

// ScheduleSelectiveECBatches computes the complete ordered SelectiveEC schedule.
//
// Example:
//
//	schedule, err := ScheduleSelectiveECBatches(
//	    failedPeer,
//	    lostShards,
//	    livePeers,
//	    bandwidth,
//	    func(pin api.Pin) ([]api.Pin, []peer.ID, int, int) {
//	        return c.get_shards_same_stripe(pin)
//	    },
//	)
//
// Returned:
//
//	schedule.Batches[0]
//	schedule.Batches[1]
//	schedule.Batches[2]
//	...
//
// Each batch contains:
//   - lost shard
//   - selected reconstruction helpers (CID + zero-based RS index)
//   - selected repair peer
//
// IMPORTANT:
//
// SelectiveEC's normal transformer only forms COMPLETE batches where:
//
//	batchSize = number of live peers
//
// Therefore a final number of repairs smaller than batchSize is returned in:
//
//	schedule.Leftover
//
// This function DOES NOT execute the batches.
func ScheduleSelectiveECBatches(
	failedPeer peer.ID,
	lostShards []api.Pin,
	livePeers []peer.ID,
	bandwidth map[peer.ID]SelectiveECBandwidth,
	getStripe SelectiveECStripeInfoFunc,
) (SelectiveECSchedule, error) {

	result :=
		SelectiveECSchedule{
			Batches: make(
				[]SelectiveECBatch,
				0,
			),

			Leftover: make(
				[]api.Pin,
				0,
			),
		}

	// -----------------------------------------------------------------
	// Build clean list of currently live peers.
	// -----------------------------------------------------------------

	cleanPeers :=
		make(
			[]peer.ID,
			0,
			len(livePeers),
		)

	for _, p := range selectiveECSortedUniquePeers(
		livePeers,
	) {

		if p == "" ||
			p == failedPeer {

			continue
		}

		cleanPeers =
			append(
				cleanPeers,
				p,
			)
	}

	if len(cleanPeers) == 0 {
		return result,
			fmt.Errorf(
				"SelectiveEC: no live peers available",
			)
	}

	// Section 4.3 requires real-time available upstream/downstream bandwidth
	// for every live peer. Do not invent default values if a measurement is
	// missing.
	if _, err :=
		selectiveECAverageBandwidth(
			cleanPeers,
			bandwidth,
			true,
		); err != nil {

		return result, err
	}

	if _, err :=
		selectiveECAverageBandwidth(
			cleanPeers,
			bandwidth,
			false,
		); err != nil {

		return result, err
	}

	if len(lostShards) == 0 {
		return result, nil
	}

	// -----------------------------------------------------------------
	// Convert every lost shard into a SelectiveEC task.
	//
	// All lost tasks remain available to the batch-selection process.
	// -----------------------------------------------------------------

	tasks, err :=
		selectiveECBuildTasks(
			lostShards,
			failedPeer,
			cleanPeers,
			getStripe,
		)

	if err != nil {
		return result, err
	}

	batchSize :=
		len(cleanPeers)

	// used[i]:
	//
	//     false -> task currently unscheduled
	//     true  -> task currently/finally selected in a batch
	//
	// When a task is displaced during SelectiveEC replacement, it is changed
	// back to false and becomes available for a later batch.
	used :=
		make(
			[]bool,
			len(tasks),
		)

	batchNumber := 0

	for {
		remaining := 0

		for taskIndex := range tasks {

			if !used[taskIndex] {
				remaining++
			}
		}

		// -------------------------------------------------------------
		// Faithful normal-transformer behavior:
		//
		// do not create a partial node-count batch.
		// -------------------------------------------------------------

		if remaining <
			batchSize {

			break
		}

		// -------------------------------------------------------------
		// Initial candidate batch:
		//
		// first batchSize currently-unscheduled tasks.
		// -------------------------------------------------------------

		batchIndexes :=
			make(
				[]int,
				0,
				batchSize,
			)

		for taskIndex := range tasks {

			if used[taskIndex] {
				continue
			}

			batchIndexes =
				append(
					batchIndexes,
					taskIndex,
				)

			used[taskIndex] =
				true

			if len(batchIndexes) ==
				batchSize {

				break
			}
		}

		if len(batchIndexes) !=
			batchSize {

			return result,
				fmt.Errorf(
					"SelectiveEC: internal batch construction error: got %d tasks expected %d",
					len(batchIndexes),
					batchSize,
				)
		}

		// -------------------------------------------------------------
		// Let SelectiveEC replace tasks if needed and produce the final
		// balanced batch.
		// -------------------------------------------------------------

		batch, err :=
			selectiveECScheduleOneBatch(
				tasks,
				batchIndexes,
				used,
				cleanPeers,
				bandwidth,
			)

		if err != nil {
			return result,
				fmt.Errorf(
					"SelectiveEC batch %d failed: %w",
					batchNumber,
					err,
				)
		}

		batch.Index =
			batchNumber

		result.Batches =
			append(
				result.Batches,
				batch,
			)

		batchNumber++
	}

	// -----------------------------------------------------------------
	// Any tasks which were never placed in a full batch are returned
	// explicitly as leftovers.
	// -----------------------------------------------------------------

	for taskIndex, task := range tasks {

		if used[taskIndex] {
			continue
		}

		result.Leftover =
			append(
				result.Leftover,
				task.Shard,
			)
	}

	return result, nil
}
