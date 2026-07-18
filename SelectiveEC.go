package ipfscluster

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ============================================================================
// SelectiveEC: balanced helpers + balanced repair peers
//
// Assumptions:
//   1. All failed shards form one batch.
//   2. Every repair task has equal weight.
//   3. Each task requires N distinct surviving helpers.
//   4. A repair peer must not:
//        - be the failed peer,
//        - already store a shard from the same stripe.
//   5. No bandwidth or similarity information is used.
//   6. A peer stores at most one shard from a given stripe.
// ============================================================================

type SelectiveECAssignment struct {
	Shard      api.Pin
	RepairPeer peer.ID

	// Helper peers selected internally by the max-flow scheduler.
	Helpers []peer.ID

	// Zero-based Reed-Solomon positions held by the selected helpers.
	//
	// Example for RS(6,3):
	//
	//     []int{0, 2, 3, 5, 7, 8}
	//
	// The repair function uses these indexes directly in:
	//
	//     reconstructShards[index] = downloadedData
	HelperIndexes []int

	// Number of tasks assigned to this repair peer.
	RepairPeerLoad int

	// Final helper loads after scheduling the complete batch.
	HelperLoads map[peer.ID]int
}

type selectiveECTask struct {
	Shard api.Pin
	N     int
	K     int

	// Peers that hold surviving shards from the same stripe.
	HelperCandidates []peer.ID

	// Maps every helper peer to the zero-based Reed-Solomon index
	// of the shard stored by that peer.
	HelperIndexes map[peer.ID]int

	// All peers currently containing a surviving shard from this stripe.
	StripePeers []peer.ID

	// Peers eligible to reconstruct/store the repaired shard.
	RepairCandidates []peer.ID
}

// ----------------------------------------------------------------------------
// Dinic maximum-flow implementation
// ----------------------------------------------------------------------------

type selectiveECEdge struct {
	to       int
	reverse  int
	capacity int
	original int
}

type selectiveECFlow struct {
	graph [][]selectiveECEdge
	level []int
	next  []int
}

func newSelectiveECFlow(nodeCount int) *selectiveECFlow {
	return &selectiveECFlow{
		graph: make([][]selectiveECEdge, nodeCount),
		level: make([]int, nodeCount),
		next:  make([]int, nodeCount),
	}
}

func (f *selectiveECFlow) addEdge(
	from int,
	to int,
	capacity int,
) {
	forward := selectiveECEdge{
		to:       to,
		reverse:  len(f.graph[to]),
		capacity: capacity,
		original: capacity,
	}

	backward := selectiveECEdge{
		to:       from,
		reverse:  len(f.graph[from]),
		capacity: 0,
		original: 0,
	}

	f.graph[from] = append(
		f.graph[from],
		forward,
	)

	f.graph[to] = append(
		f.graph[to],
		backward,
	)
}

func (f *selectiveECFlow) buildLevels(
	source int,
	sink int,
) bool {
	for i := range f.level {
		f.level[i] = -1
	}

	queue := make(
		[]int,
		0,
		len(f.graph),
	)

	queue = append(
		queue,
		source,
	)

	f.level[source] = 0

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		for _, edge := range f.graph[node] {
			if edge.capacity <= 0 ||
				f.level[edge.to] != -1 {

				continue
			}

			f.level[edge.to] =
				f.level[node] + 1

			queue = append(
				queue,
				edge.to,
			)
		}
	}

	return f.level[sink] != -1
}

func (f *selectiveECFlow) sendFlow(
	node int,
	sink int,
	pushed int,
) int {
	if node == sink {
		return pushed
	}

	for f.next[node] < len(f.graph[node]) {
		edgeIndex := f.next[node]
		edge := &f.graph[node][edgeIndex]

		if edge.capacity > 0 &&
			f.level[edge.to] ==
				f.level[node]+1 {

			amount := pushed

			if edge.capacity < amount {
				amount = edge.capacity
			}

			flow :=
				f.sendFlow(
					edge.to,
					sink,
					amount,
				)

			if flow > 0 {
				edge.capacity -= flow

				reverseIndex :=
					edge.reverse

				f.graph[edge.to][reverseIndex].
					capacity += flow

				return flow
			}
		}

		f.next[node]++
	}

	return 0
}

func (f *selectiveECFlow) maxFlow(
	source int,
	sink int,
) int {
	total := 0

	for f.buildLevels(source, sink) {
		for i := range f.next {
			f.next[i] = 0
		}

		for {
			pushed :=
				f.sendFlow(
					source,
					sink,
					int(^uint(0)>>1),
				)

			if pushed == 0 {
				break
			}

			total += pushed
		}
	}

	return total
}

// ----------------------------------------------------------------------------
// Small utilities
// ----------------------------------------------------------------------------

func selectiveECCopyPin(
	pin api.Pin,
) api.Pin {
	out := pin

	out.Metadata =
		make(
			map[string]string,
			len(pin.Metadata)+4,
		)

	for key, value := range pin.Metadata {
		out.Metadata[key] = value
	}

	return out
}

func selectiveECContainsPeer(
	peers []peer.ID,
	target peer.ID,
) bool {
	for _, currentPeer := range peers {
		if currentPeer == target {
			return true
		}
	}

	return false
}

func selectiveECCeilDivide(
	numerator int,
	denominator int,
) int {
	if denominator <= 0 {
		return 0
	}

	return (numerator + denominator - 1) /
		denominator
}

func selectiveECUnionHelpers(
	tasks []selectiveECTask,
) []peer.ID {
	allHelpers :=
		make([]peer.ID, 0)

	for _, task := range tasks {
		allHelpers = append(
			allHelpers,
			task.HelperCandidates...,
		)
	}

	return sortedUniquePeers(allHelpers)
}

func selectiveECUnionRepairPeers(
	tasks []selectiveECTask,
) []peer.ID {
	allRepairPeers :=
		make([]peer.ID, 0)

	for _, task := range tasks {
		allRepairPeers = append(
			allRepairPeers,
			task.RepairCandidates...,
		)
	}

	return sortedUniquePeers(
		allRepairPeers,
	)
}

func selectiveECContainsInt(
	values []int,
	target int,
) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}

	return false
}

// ----------------------------------------------------------------------------
// Balanced helper-selection flow
//
// source -> task          capacity N(task)
// task   -> helper        capacity 1
// helper -> sink          capacity maxHelperLoad
// ----------------------------------------------------------------------------

func selectiveECSelectHelpersWithLimit(
	tasks []selectiveECTask,
	allHelpers []peer.ID,
	maxHelperLoad int,
) (
	[][]peer.ID,
	map[peer.ID]int,
	bool,
) {
	if len(tasks) == 0 {
		return [][]peer.ID{},
			map[peer.ID]int{},
			true
	}

	helperIndex :=
		make(
			map[peer.ID]int,
			len(allHelpers),
		)

	for index, helper := range allHelpers {

		helperIndex[helper] = index
	}

	taskCount := len(tasks)
	helperCount := len(allHelpers)

	source := 0
	taskStart := 1
	helperStart := taskStart + taskCount
	sink := helperStart + helperCount
	nodeCount := sink + 1

	flow :=
		newSelectiveECFlow(nodeCount)

	totalDemand := 0

	for taskIndex, task := range tasks {
		taskNode :=
			taskStart + taskIndex

		flow.addEdge(
			source,
			taskNode,
			task.N,
		)

		totalDemand += task.N

		for _, helper := range task.HelperCandidates {

			index, exists :=
				helperIndex[helper]

			if !exists {
				continue
			}

			helperNode :=
				helperStart + index

			// One helper contributes at most once to a task.
			flow.addEdge(
				taskNode,
				helperNode,
				1,
			)
		}
	}

	for helperIndex := range allHelpers {

		helperNode :=
			helperStart + helperIndex

		flow.addEdge(
			helperNode,
			sink,
			maxHelperLoad,
		)
	}

	obtainedFlow :=
		flow.maxFlow(
			source,
			sink,
		)

	if obtainedFlow != totalDemand {
		return nil, nil, false
	}

	selectedByTask :=
		make(
			[][]peer.ID,
			taskCount,
		)

	helperLoads :=
		make(
			map[peer.ID]int,
			helperCount,
		)

	for taskIndex := range tasks {
		taskNode :=
			taskStart + taskIndex

		for _, edge := range flow.graph[taskNode] {

			if edge.to < helperStart ||
				edge.to >= sink {

				continue
			}

			// Flow was used when the original capacity was 1
			// and the residual capacity became zero.
			if edge.original == 1 &&
				edge.capacity == 0 {

				index :=
					edge.to - helperStart

				helper :=
					allHelpers[index]

				selectedByTask[taskIndex] =
					append(
						selectedByTask[taskIndex],
						helper,
					)

				helperLoads[helper]++
			}
		}

		selectedByTask[taskIndex] =
			sortedUniquePeers(
				selectedByTask[taskIndex],
			)

		if len(selectedByTask[taskIndex]) !=
			tasks[taskIndex].N {

			return nil, nil, false
		}
	}

	return selectedByTask,
		helperLoads,
		true
}

func selectiveECSelectBalancedHelpers(
	tasks []selectiveECTask,
) (
	[][]peer.ID,
	map[peer.ID]int,
	int,
	error,
) {
	allHelpers :=
		selectiveECUnionHelpers(tasks)

	if len(allHelpers) == 0 {
		return nil, nil, 0,
			fmt.Errorf(
				"SelectiveEC: no helper peers are available",
			)
	}

	totalDemand := 0
	maxTaskDemand := 0

	for _, task := range tasks {
		if task.N <= 0 {
			return nil, nil, 0,
				fmt.Errorf(
					"SelectiveEC: shard %s has invalid "+
						"helper requirement n=%d",
					task.Shard.Cid.String(),
					task.N,
				)
		}

		if len(task.HelperCandidates) <
			task.N {

			return nil, nil, 0,
				fmt.Errorf(
					"SelectiveEC: shard %s requires %d "+
						"helpers but only %d are available",
					task.Shard.Cid.String(),
					task.N,
					len(task.HelperCandidates),
				)
		}

		totalDemand += task.N

		if task.N > maxTaskDemand {
			maxTaskDemand = task.N
		}
	}

	// The theoretical lower bound is the average helper load.
	lowerBound :=
		selectiveECCeilDivide(
			totalDemand,
			len(allHelpers),
		)

	if lowerBound < 1 {
		lowerBound = 1
	}

	// A helper can participate at most once in each task.
	upperBound := len(tasks)

	for limit := lowerBound; limit <= upperBound; limit++ {

		selected,
			loads,
			feasible :=
			selectiveECSelectHelpersWithLimit(
				tasks,
				allHelpers,
				limit,
			)

		if feasible {
			return selected,
				loads,
				limit,
				nil
		}
	}

	return nil, nil, 0,
		fmt.Errorf(
			"SelectiveEC: no feasible helper assignment "+
				"for %d tasks; total demand=%d, "+
				"helpers=%d, maximum task demand=%d",
			len(tasks),
			totalDemand,
			len(allHelpers),
			maxTaskDemand,
		)
}

// ----------------------------------------------------------------------------
// Balanced repair-peer assignment flow
//
// source      -> task         capacity 1
// task        -> repair peer  capacity 1
// repair peer -> sink         capacity maxRepairLoad
// ----------------------------------------------------------------------------

func selectiveECAssignRepairPeersWithLimit(
	tasks []selectiveECTask,
	allRepairPeers []peer.ID,
	maxRepairLoad int,
) (
	[]peer.ID,
	map[peer.ID]int,
	bool,
) {
	if len(tasks) == 0 {
		return []peer.ID{},
			map[peer.ID]int{},
			true
	}

	repairPeerIndex :=
		make(
			map[peer.ID]int,
			len(allRepairPeers),
		)

	for index, repairPeer := range allRepairPeers {

		repairPeerIndex[repairPeer] =
			index
	}

	taskCount := len(tasks)
	peerCount := len(allRepairPeers)

	source := 0
	taskStart := 1
	peerStart := taskStart + taskCount
	sink := peerStart + peerCount
	nodeCount := sink + 1

	flow :=
		newSelectiveECFlow(nodeCount)

	for taskIndex, task := range tasks {
		taskNode :=
			taskStart + taskIndex

		flow.addEdge(
			source,
			taskNode,
			1,
		)

		for _, candidate := range task.RepairCandidates {

			index, exists :=
				repairPeerIndex[candidate]

			if !exists {
				continue
			}

			peerNode :=
				peerStart + index

			flow.addEdge(
				taskNode,
				peerNode,
				1,
			)
		}
	}

	for peerIndex := range allRepairPeers {

		peerNode :=
			peerStart + peerIndex

		flow.addEdge(
			peerNode,
			sink,
			maxRepairLoad,
		)
	}

	obtainedFlow :=
		flow.maxFlow(
			source,
			sink,
		)

	if obtainedFlow != len(tasks) {
		return nil, nil, false
	}

	assignedPeers :=
		make(
			[]peer.ID,
			taskCount,
		)

	repairLoads :=
		make(
			map[peer.ID]int,
			peerCount,
		)

	for taskIndex := range tasks {
		taskNode :=
			taskStart + taskIndex

		for _, edge := range flow.graph[taskNode] {

			if edge.to < peerStart ||
				edge.to >= sink {

				continue
			}

			if edge.original == 1 &&
				edge.capacity == 0 {

				index :=
					edge.to - peerStart

				selectedPeer :=
					allRepairPeers[index]

				assignedPeers[taskIndex] =
					selectedPeer

				repairLoads[selectedPeer]++

				break
			}
		}

		if assignedPeers[taskIndex] == "" {
			return nil, nil, false
		}
	}

	return assignedPeers,
		repairLoads,
		true
}

func selectiveECAssignBalancedRepairPeers(
	tasks []selectiveECTask,
) (
	[]peer.ID,
	map[peer.ID]int,
	int,
	error,
) {
	allRepairPeers :=
		selectiveECUnionRepairPeers(tasks)

	if len(allRepairPeers) == 0 {
		return nil, nil, 0,
			fmt.Errorf(
				"SelectiveEC: no repair peers are available",
			)
	}

	for _, task := range tasks {
		if len(task.RepairCandidates) == 0 {
			return nil, nil, 0,
				fmt.Errorf(
					"SelectiveEC: shard %s has no "+
						"eligible repair peer",
					task.Shard.Cid.String(),
				)
		}
	}

	lowerBound :=
		selectiveECCeilDivide(
			len(tasks),
			len(allRepairPeers),
		)

	if lowerBound < 1 {
		lowerBound = 1
	}

	upperBound := len(tasks)

	for limit := lowerBound; limit <= upperBound; limit++ {

		assigned,
			loads,
			feasible :=
			selectiveECAssignRepairPeersWithLimit(
				tasks,
				allRepairPeers,
				limit,
			)

		if feasible {
			return assigned,
				loads,
				limit,
				nil
		}
	}

	return nil, nil, 0,
		fmt.Errorf(
			"SelectiveEC: no feasible repair-peer assignment "+
				"for %d tasks and %d candidate peers",
			len(tasks),
			len(allRepairPeers),
		)
}

// ----------------------------------------------------------------------------
// Main SelectiveEC scheduler
// ----------------------------------------------------------------------------

func ScheduleSelectiveECOneBatch(
	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,

	getSameStripe func(api.Pin) (
		[]api.Pin,
		[]peer.ID,
		int,
		int,
	),
) (
	map[peer.ID][]api.Pin,
	[]SelectiveECAssignment,
	error,
) {
	fmt.Println(
		"In SELECTIVE-EC ONE-BATCH strategy: " +
			"balanced repair peers and helpers",
	)

	assignmentsByRepairPeer :=
		make(map[peer.ID][]api.Pin)

	results :=
		make(
			[]SelectiveECAssignment,
			0,
			len(failedShards),
		)

	if len(failedShards) == 0 {
		return assignmentsByRepairPeer,
			results,
			nil
	}

	candidatePeers =
		sortedUniquePeers(candidatePeers)

	if len(candidatePeers) == 0 {
		return nil, nil,
			fmt.Errorf(
				"SelectiveEC: candidatePeers is empty",
			)
	}

	// ---------------------------------------------------------------------
	// Build all repair tasks.
	// ---------------------------------------------------------------------

	tasks :=
		make(
			[]selectiveECTask,
			0,
			len(failedShards),
		)

	for _, originalShard := range failedShards {

		shard :=
			selectiveECCopyPin(
				originalShard,
			)

		sameStripeShards,
			stripePeers,
			n,
			k :=
			getSameStripe(shard)

		if n <= 0 {
			return nil, nil,
				fmt.Errorf(
					"SelectiveEC: invalid n=%d for shard %s",
					n,
					shard.Cid.String(),
				)
		}

		if k < 0 {
			return nil, nil,
				fmt.Errorf(
					"SelectiveEC: invalid k=%d for shard %s",
					k,
					shard.Cid.String(),
				)
		}

		totalStripeShards := n + k

		if totalStripeShards <= 0 {
			return nil, nil,
				fmt.Errorf(
					"SelectiveEC: invalid stripe size n+k=%d "+
						"for shard %s",
					totalStripeShards,
					shard.Cid.String(),
				)
		}

		if len(sameStripeShards) == 0 {
			return nil, nil,
				fmt.Errorf(
					"SelectiveEC: no surviving stripe shards "+
						"were returned for shard %s",
					shard.Cid.String(),
				)
		}

		/*
			Map every surviving helper peer to its zero-based
			Reed-Solomon stripe index.

			Example:

			    global shard number = 11
			    n+k                 = 9

			    stripe index = (11 - 1) % 9
			                 = 1
		*/
		helperIndexes :=
			make(map[peer.ID]int)

		allStripePeers :=
			make([]peer.ID, 0)

		for _, sameStripeShard := range sameStripeShards {

			globalShardNumber,
				_,
				err :=
				getShardNumber(
					sameStripeShard.Name,
				)

			if err != nil {
				return nil, nil,
					fmt.Errorf(
						"SelectiveEC: cannot extract shard "+
							"number from %q: %w",
						sameStripeShard.Name,
						err,
					)
			}

			if globalShardNumber <= 0 {
				return nil, nil,
					fmt.Errorf(
						"SelectiveEC: invalid global shard "+
							"number %d from %q",
						globalShardNumber,
						sameStripeShard.Name,
					)
			}

			stripeIndex :=
				(globalShardNumber - 1) %
					totalStripeShards

			for _, allocation := range sameStripeShard.Allocations {

				if allocation == "" ||
					allocation == failedPeer {

					continue
				}

				/*
					A peer must map to exactly one shard index
					inside this stripe.

					If this error occurs, the placement contains
					multiple shards from the same stripe on one
					peer, and peer-only helper scheduling is
					ambiguous.
				*/
				oldIndex,
					alreadyExists :=
					helperIndexes[allocation]

				if alreadyExists &&
					oldIndex != stripeIndex {

					return nil, nil,
						fmt.Errorf(
							"SelectiveEC: peer %s maps to "+
								"multiple indexes in one stripe: "+
								"%d and %d for failed shard %s",
							allocation.String(),
							oldIndex,
							stripeIndex,
							shard.Cid.String(),
						)
				}

				helperIndexes[allocation] =
					stripeIndex

				allStripePeers = append(
					allStripePeers,
					allocation,
				)
			}
		}

		allStripePeers =
			sortedUniquePeers(
				allStripePeers,
			)

		/*
			Build helper candidates from stripePeers, but only
			retain peers for which a concrete RS index was found.
		*/
		helperCandidates :=
			make([]peer.ID, 0)

		for _, helper := range sortedUniquePeers(
			stripePeers,
		) {

			if helper == "" ||
				helper == failedPeer {

				continue
			}

			if _, exists :=
				helperIndexes[helper]; !exists {

				fmt.Printf(
					"SELECTIVE-EC warning: helper %s "+
						"was returned in stripePeers but no "+
						"matching shard index was found\n",
					helper.String(),
				)

				continue
			}

			helperCandidates = append(
				helperCandidates,
				helper,
			)
		}

		/*
			Include valid surviving allocations discovered from
			sameStripeShards even if stripePeers omitted them.
		*/
		for helper := range helperIndexes {

			if helper == "" ||
				helper == failedPeer {

				continue
			}

			if !selectiveECContainsPeer(
				helperCandidates,
				helper,
			) {
				helperCandidates =
					append(
						helperCandidates,
						helper,
					)
			}
		}

		helperCandidates =
			sortedUniquePeers(
				helperCandidates,
			)

		if len(helperCandidates) < n {
			return nil, nil,
				fmt.Errorf(
					"SelectiveEC: shard %s requires %d "+
						"helpers but only %d indexed surviving "+
						"helpers were found",
					shard.Cid.String(),
					n,
					len(helperCandidates),
				)
		}

		// SelectiveEC replacement candidates:
		//
		// The reconstructed shard must not be placed on:
		//
		//   1. the failed peer;
		//   2. any peer already storing a surviving shard
		//      from this stripe.
		repairCandidates :=
			make([]peer.ID, 0)

		for _, candidate := range candidatePeers {

			if candidate == "" ||
				candidate == failedPeer {

				continue
			}

			if selectiveECContainsPeer(
				allStripePeers,
				candidate,
			) {
				continue
			}

			repairCandidates =
				append(
					repairCandidates,
					candidate,
				)
		}

		repairCandidates =
			sortedUniquePeers(
				repairCandidates,
			)

		if len(repairCandidates) == 0 {
			return nil, nil,
				fmt.Errorf(
					"SelectiveEC: shard %s has no repair "+
						"candidate outside its current stripe peers",
					shard.Cid.String(),
				)
		}

		tasks = append(
			tasks,
			selectiveECTask{
				Shard: shard,
				N:     n,
				K:     k,

				HelperCandidates: helperCandidates,

				HelperIndexes: helperIndexes,

				StripePeers: allStripePeers,

				RepairCandidates: repairCandidates,
			},
		)
	}

	// ---------------------------------------------------------------------
	// Phase 1: balance helper usage over the complete batch.
	// ---------------------------------------------------------------------

	helpersByTask,
		helperLoads,
		maxHelperLoad,
		err :=
		selectiveECSelectBalancedHelpers(
			tasks,
		)

	if err != nil {
		return nil, nil, err
	}

	// ---------------------------------------------------------------------
	// Phase 2: balance repair destinations over the complete batch.
	// ---------------------------------------------------------------------

	repairPeerByTask,
		repairPeerLoads,
		maxRepairPeerLoad,
		err :=
		selectiveECAssignBalancedRepairPeers(
			tasks,
		)

	if err != nil {
		return nil, nil, err
	}

	// ---------------------------------------------------------------------
	// Build final assignments and metadata.
	// ---------------------------------------------------------------------

	for taskIndex, task := range tasks {

		shard :=
			selectiveECCopyPin(
				task.Shard,
			)

		if taskIndex >=
			len(repairPeerByTask) {

			return nil, nil,
				fmt.Errorf(
					"SelectiveEC: no repair-peer result "+
						"for task %d",
					taskIndex,
				)
		}

		repairPeer :=
			repairPeerByTask[taskIndex]

		if repairPeer == "" {
			return nil, nil,
				fmt.Errorf(
					"SelectiveEC: empty repair peer "+
						"for task %d",
					taskIndex,
				)
		}

		if taskIndex >= len(helpersByTask) {
			return nil, nil,
				fmt.Errorf(
					"SelectiveEC: no helper result "+
						"for task %d",
					taskIndex,
				)
		}

		helpers :=
			sortedUniquePeers(
				helpersByTask[taskIndex],
			)

		if len(helpers) != task.N {
			return nil, nil,
				fmt.Errorf(
					"SelectiveEC: task %d selected %d "+
						"helpers, expected %d",
					taskIndex,
					len(helpers),
					task.N,
				)
		}

		selectedIndexes :=
			make(
				[]int,
				0,
				len(helpers),
			)

		for _, helper := range helpers {
			stripeIndex, exists :=
				task.HelperIndexes[helper]

			if !exists {
				return nil, nil,
					fmt.Errorf(
						"SelectiveEC: selected helper %s "+
							"has no shard index for task %d",
						helper.String(),
						taskIndex,
					)
			}

			if stripeIndex < 0 ||
				stripeIndex >= task.N+task.K {

				return nil, nil,
					fmt.Errorf(
						"SelectiveEC: helper %s has invalid "+
							"stripe index %d for RS(%d,%d)",
						helper.String(),
						stripeIndex,
						task.N,
						task.K,
					)
			}

			if selectiveECContainsInt(
				selectedIndexes,
				stripeIndex,
			) {
				return nil, nil,
					fmt.Errorf(
						"SelectiveEC: duplicate selected "+
							"stripe index %d for task %d",
						stripeIndex,
						taskIndex,
					)
			}

			selectedIndexes =
				append(
					selectedIndexes,
					stripeIndex,
				)
		}

		/*
			Sorting is safe because each index identifies the
			Reed-Solomon position independently. The repair
			function does not need the helper peer order.
		*/
		sort.Ints(selectedIndexes)

		indexStrings :=
			make(
				[]string,
				0,
				len(selectedIndexes),
			)

		for _, stripeIndex := range selectedIndexes {

			indexStrings =
				append(
					indexStrings,
					strconv.Itoa(
						stripeIndex,
					),
				)
		}

		if shard.Metadata == nil {
			shard.Metadata =
				make(map[string]string)
		}

		shard.Metadata["Strategy"] =
			"SELECTIVE_EC"

		/*
			Store only the selected zero-based Reed-Solomon
			indexes needed by the repair function.

			Example:

			    helper_indexes = "0,2,3,5,7,8"
		*/
		shard.Metadata["helper_indexes"] =
			strings.Join(
				indexStrings,
				",",
			)

		// Store the explicitly selected repair destination.
		shard.Metadata["repair_peer"] =
			repairPeer.String()

		/*
			Do not leave old helper peer IDs in the metadata.
			The SelectiveEC repair path now reads helper_indexes.
		*/
		delete(
			shard.Metadata,
			"allocs",
		)

		delete(
			shard.Metadata,
			"helpers",
		)

		assignmentsByRepairPeer[repairPeer] =
			append(
				assignmentsByRepairPeer[repairPeer],
				shard,
			)

		loadCopy :=
			make(
				map[peer.ID]int,
				len(helperLoads),
			)

		for helper, load := range helperLoads {

			loadCopy[helper] = load
		}

		helperCopy :=
			append(
				[]peer.ID(nil),
				helpers...,
			)

		indexCopy :=
			append(
				[]int(nil),
				selectedIndexes...,
			)

		results = append(
			results,
			SelectiveECAssignment{
				Shard: shard,

				RepairPeer: repairPeer,

				Helpers: helperCopy,

				HelperIndexes: indexCopy,

				RepairPeerLoad: repairPeerLoads[repairPeer],

				HelperLoads: loadCopy,
			},
		)

		fmt.Printf(
			"SELECTIVE-EC assigned "+
				"shard=%s cid=%s repairPeer=%s "+
				"repairPeerLoad=%d helperIndexes=%s\n",
			shard.Name,
			shard.Cid.String(),
			repairPeer.String(),
			repairPeerLoads[repairPeer],
			shard.Metadata["helper_indexes"],
		)

		for _, helper := range helpers {

			fmt.Printf(
				"    helper=%s stripeIndex=%d "+
					"finalHelperLoad=%d\n",
				helper.String(),
				task.HelperIndexes[helper],
				helperLoads[helper],
			)
		}
	}

	fmt.Printf(
		"SELECTIVE-EC completed tasks=%d "+
			"maxRepairPeerLoad=%d maxHelperLoad=%d\n",
		len(tasks),
		maxRepairPeerLoad,
		maxHelperLoad,
	)

	return assignmentsByRepairPeer,
		results,
		nil
}
