package ipfscluster

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/libp2p/go-libp2p/core/peer"
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

func buildUniqueMatches(peerMatchedCIDs map[peer.ID][]string) map[string]bool {
	unique := make(map[string]bool)

	for _, cids := range peerMatchedCIDs {
		for _, c := range cids {
			c = cleanCIDString(c)
			if c != "" {
				unique[c] = true
			}
		}
	}

	return unique
}

func peerHasCID(peerMatchedCIDs map[peer.ID][]string, p peer.ID, cidStr string) bool {
	cidStr = cleanCIDString(cidStr)

	for _, c := range peerMatchedCIDs[p] {
		if cleanCIDString(c) == cidStr {
			return true
		}
	}
	return false
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

func topologyPairwise(t *NetworkTopology, src, dst peer.ID) uint64 {
	if t == nil {
		return 0
	}
	return t.PairwiseBandwidth(src, dst)
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

func cloneSteps(steps []RepairStep) []RepairStep {
	out := make([]RepairStep, 0, len(steps))

	for _, st := range steps {
		transfers := make([]Transfer, len(st.Transfers))
		copy(transfers, st.Transfers)

		out = append(out, RepairStep{
			Kind:      st.Kind,
			Transfers: transfers,
		})
	}

	return out
}

func appendJob(jobs []RepairJob, job RepairJob) []RepairJob {
	out := make([]RepairJob, 0, len(jobs)+1)
	out = append(out, jobs...)
	out = append(out, job)
	return out
}

func sourcesForCID(
	cidStr string,
	repairPeer peer.ID,
	failedPeer peer.ID,
	peerMatchedCIDs map[peer.ID][]string,
	topology *NetworkTopology,
) []peer.ID {
	cidStr = cleanCIDString(cidStr)

	sources := make([]peer.ID, 0)

	for p, cids := range peerMatchedCIDs {
		if p == failedPeer || p == repairPeer {
			continue
		}

		if topologyPairwise(topology, p, repairPeer) == 0 {
			continue
		}

		for _, c := range cids {
			if cleanCIDString(c) == cidStr {
				sources = append(sources, p)
				break
			}
		}
	}

	return sortedUniquePeers(sources)
}

func activateJobStep(jobIndex int, state *SimJobState) []SimTransfer {
	if state.Finished {
		return nil
	}

	for state.StepIndex < len(state.Job.Steps) &&
		len(state.Job.Steps[state.StepIndex].Transfers) == 0 {
		state.StepIndex++
	}

	if state.StepIndex >= len(state.Job.Steps) {
		state.Finished = true
		return nil
	}

	step := state.Job.Steps[state.StepIndex]
	active := make([]SimTransfer, 0, len(step.Transfers))

	for _, tr := range step.Transfers {
		active = append(active, SimTransfer{
			Src:         tr.Src,
			Dst:         tr.Dst,
			SizeMB:      tr.SizeMB,
			RemainingMB: tr.SizeMB,
			JobIndex:    jobIndex,
			Step:        state.StepIndex,
			Kind:        tr.Kind,
			CID:         tr.CID,
		})
	}

	return active
}

// Event-driven simulator.
// Bandwidth model:
//   - pairwise link bandwidth is shared equally among active transfers on that link
//   - repair peer incoming bandwidth is shared equally among active transfers entering it
//   - source outgoing bandwidth is ignored
func simulateRepairJobs(
	jobs []RepairJob,
	topology *NetworkTopology,
) SimulationResult {
	jobStates := make([]*SimJobState, 0, len(jobs))
	active := make([]SimTransfer, 0)

	for i, job := range jobs {
		st := &SimJobState{
			Job:       job,
			StepIndex: 0,
			Finished:  false,
		}
		jobStates = append(jobStates, st)

		newTransfers := activateJobStep(i, st)
		active = append(active, newTransfers...)

		if len(job.Steps) == 0 {
			st.Finished = true
			st.FinishTime = 0
		}
	}

	now := 0.0
	const eps = 1e-9

	for len(active) > 0 {
		inCount := make(map[peer.ID]int)
		linkCount := make(map[peer.ID]map[peer.ID]int)

		for _, tr := range active {
			inCount[tr.Dst]++

			if linkCount[tr.Src] == nil {
				linkCount[tr.Src] = make(map[peer.ID]int)
			}
			linkCount[tr.Src][tr.Dst]++
		}

		rates := make([]float64, len(active))
		minDelta := math.Inf(1)

		for i, tr := range active {
			dstIn := topologyNodeIn(topology, tr.Dst)
			pair := topologyPairwise(topology, tr.Src, tr.Dst)

			if dstIn == 0 || pair == 0 {
				return SimulationResult{
					TotalFinishTime: math.Inf(1),
					JobFinishTimes:  map[string]float64{},
				}
			}

			dstShare := float64(dstIn) / float64(inCount[tr.Dst])
			linkShare := float64(pair) / float64(linkCount[tr.Src][tr.Dst])

			rate := dstShare
			if linkShare < rate {
				rate = linkShare
			}

			if rate <= 0 {
				return SimulationResult{
					TotalFinishTime: math.Inf(1),
					JobFinishTimes:  map[string]float64{},
				}
			}

			rates[i] = rate

			delta := tr.RemainingMB / rate
			if delta < minDelta {
				minDelta = delta
			}
		}

		if math.IsInf(minDelta, 1) {
			return SimulationResult{
				TotalFinishTime: math.Inf(1),
				JobFinishTimes:  map[string]float64{},
			}
		}

		now += minDelta

		nextActive := make([]SimTransfer, 0)
		completedJobs := make(map[int]bool)

		for i := range active {
			active[i].RemainingMB -= rates[i] * minDelta

			if active[i].RemainingMB <= eps {
				completedJobs[active[i].JobIndex] = true
			} else {
				nextActive = append(nextActive, active[i])
			}
		}

		for jobIndex := range completedJobs {
			st := jobStates[jobIndex]
			if st.Finished {
				continue
			}

			stepStillActive := false
			for _, tr := range nextActive {
				if tr.JobIndex == jobIndex && tr.Step == st.StepIndex {
					stepStillActive = true
					break
				}
			}

			if !stepStillActive {
				st.StepIndex++

				if st.StepIndex >= len(st.Job.Steps) {
					st.Finished = true
					st.FinishTime = now
				} else {
					newTransfers := activateJobStep(jobIndex, st)
					nextActive = append(nextActive, newTransfers...)
				}
			}
		}

		active = nextActive
	}

	jobFinishTimes := make(map[string]float64)
	total := 0.0

	for _, st := range jobStates {
		key := st.Job.Shard.Cid.String()

		if !st.Finished {
			st.Finished = true
			st.FinishTime = now
		}

		jobFinishTimes[key] = st.FinishTime

		if st.FinishTime > total {
			total = st.FinishTime
		}
	}

	return SimulationResult{
		TotalFinishTime: total,
		JobFinishTimes:  jobFinishTimes,
	}
}

func buildProbeJob(
	shard api.Pin,
	repairPeer peer.ID,
	steps []RepairStep,
	localChunkCount int,
	directCount int,
	missingCount int,
	otherSources []peer.ID,
	selectedHelpers []peer.ID,
	repairPeerLocalHelper bool,
	neededRemoteHelpers int,
) RepairJob {
	srcs := make([]peer.ID, len(otherSources))
	copy(srcs, otherSources)

	helpers := make([]peer.ID, len(selectedHelpers))
	copy(helpers, selectedHelpers)

	return RepairJob{
		Shard:                 shard,
		RepairPeer:            repairPeer,
		Steps:                 cloneSteps(steps),
		LocalChunkCount:       localChunkCount,
		DirectChunkCount:      directCount,
		MissingChunkCount:     missingCount,
		OtherElementSources:   srcs,
		SelectedHelpers:       helpers,
		RepairPeerLocalHelper: repairPeerLocalHelper,
		NeededRemoteHelpers:   neededRemoteHelpers,
	}
}

// Chooses the best source for one direct chunk by testing each possible source
// against scheduledJobs + the partial candidate job.
func chooseDynamicDirectSource(
	scheduledJobs []RepairJob,
	shard api.Pin,
	repairPeer peer.ID,
	cidStr string,
	possibleSources []peer.ID,
	currentSteps []RepairStep,
	localChunkCount int,
	directCount int,
	missingCount int,
	otherSources []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,
	repairPeerLocalHelper bool,
	neededRemoteHelpers int,
) (peer.ID, bool) {
	bestSrc := peer.ID("")
	bestMakespan := math.Inf(1)

	for _, src := range sortedUniquePeers(possibleSources) {
		step := RepairStep{
			Kind: "direct",
			Transfers: []Transfer{
				{
					Src:    src,
					Dst:    repairPeer,
					SizeMB: chunkMB,
					Kind:   "direct",
					CID:    cidStr,
				},
			},
		}

		testSteps := cloneSteps(currentSteps)
		testSteps = append(testSteps, step)

		testSources := make([]peer.ID, 0, len(otherSources)+1)
		testSources = append(testSources, otherSources...)
		testSources = append(testSources, src)

		probeJob := buildProbeJob(
			shard,
			repairPeer,
			testSteps,
			localChunkCount,
			directCount+1,
			missingCount,
			testSources,
			nil,
			repairPeerLocalHelper,
			neededRemoteHelpers,
		)

		res := simulateRepairJobs(appendJob(scheduledJobs, probeJob), topology)

		if res.TotalFinishTime < bestMakespan ||
			(res.TotalFinishTime == bestMakespan && (bestSrc == "" || src.String() < bestSrc.String())) {
			bestMakespan = res.TotalFinishTime
			bestSrc = src
		}
	}

	if bestSrc == "" || math.IsInf(bestMakespan, 1) {
		return peer.ID(""), false
	}

	return bestSrc, true
}

func buildAggregatedMissingStep(
	selectedHelpers []peer.ID,
	repairPeer peer.ID,
	missingCount int,
	chunkMB float64,
) RepairStep {
	transfers := make([]Transfer, 0, len(selectedHelpers))

	// Aggregated missing model:
	// Instead of one reconstruct step per missing chunk, we create one reconstruct step.
	// Each helper sends missingCount * chunkMB.
	// Since the reconstruction is bottlenecked by the slowest helper, this gives:
	// time = missingCount * chunkMB / slowest_effective_helper_rate.
	for _, h := range selectedHelpers {
		transfers = append(transfers, Transfer{
			Src:    h,
			Dst:    repairPeer,
			SizeMB: float64(missingCount) * chunkMB,
			Kind:   "reconstruct",
		})
	}

	return RepairStep{
		Kind:      "reconstruct",
		Transfers: transfers,
	}
}

// Greedy dynamic helper selection.
// No helper combinations are generated.
// It scores each helper by simulating scheduledJobs + candidate job with only that helper's
// aggregated missing transfer, then selects the top neededRemoteHelpers helpers.
//
// This keeps helper selection dynamic because the score depends on already scheduled jobs,
// pairwise contention, and incoming contention.
func chooseGreedyDynamicHelpers(
	scheduledJobs []RepairJob,
	shard api.Pin,
	repairPeer peer.ID,
	remoteHelpers []peer.ID,
	neededRemoteHelpers int,
	baseSteps []RepairStep,
	localChunkCount int,
	directCount int,
	missingCount int,
	otherSources []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,
	repairPeerLocalHelper bool,
) ([]peer.ID, bool) {
	if missingCount == 0 {
		return nil, true
	}

	remoteHelpers = sortedUniquePeers(remoteHelpers)

	if len(remoteHelpers) < neededRemoteHelpers {
		return nil, false
	}

	if neededRemoteHelpers == 0 {
		return []peer.ID{}, true
	}

	type helperScore struct {
		Helper   peer.ID
		Makespan float64
	}

	scores := make([]helperScore, 0, len(remoteHelpers))

	for _, h := range remoteHelpers {
		if topologyPairwise(topology, h, repairPeer) == 0 {
			continue
		}

		testSteps := cloneSteps(baseSteps)
		testSteps = append(testSteps, buildAggregatedMissingStep(
			[]peer.ID{h},
			repairPeer,
			missingCount,
			chunkMB,
		))

		probeJob := buildProbeJob(
			shard,
			repairPeer,
			testSteps,
			localChunkCount,
			directCount,
			missingCount,
			otherSources,
			[]peer.ID{h},
			repairPeerLocalHelper,
			neededRemoteHelpers,
		)

		res := simulateRepairJobs(appendJob(scheduledJobs, probeJob), topology)

		if math.IsInf(res.TotalFinishTime, 1) {
			continue
		}

		scores = append(scores, helperScore{
			Helper:   h,
			Makespan: res.TotalFinishTime,
		})
	}

	if len(scores) < neededRemoteHelpers {
		return nil, false
	}

	sort.Slice(scores, func(i, j int) bool {
		if scores[i].Makespan == scores[j].Makespan {
			return scores[i].Helper.String() < scores[j].Helper.String()
		}
		return scores[i].Makespan < scores[j].Makespan
	})

	selected := make([]peer.ID, 0, neededRemoteHelpers)
	for i := 0; i < neededRemoteHelpers; i++ {
		selected = append(selected, scores[i].Helper)
	}

	return selected, true
}

func setCommonMetadata_here(pin *api.Pin, common []string) {
	cleaned := make([]string, 0, len(common))
	seen := make(map[string]bool)

	for _, c := range common {
		c = cleanCIDString(c)

		if c == "" || seen[c] {
			continue
		}

		seen[c] = true
		cleaned = append(cleaned, c)
	}

	if pin.Metadata == nil {
		pin.Metadata = make(map[string]string)
	}

	pin.Metadata["common"] = strings.Join(cleaned, ",")
}

func buildDynamicRepairJobForPeer_old(
	scheduledJobs []RepairJob,
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
	chunkMB float64,
) (RepairJob, bool) {
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

		if topologyPairwise(topology, h, repairPeer) > 0 {
			remoteHelpers = append(remoteHelpers, h)
		}
	}

	neededRemoteHelpers := n
	if repairPeerLocalHelper {
		neededRemoteHelpers = n - 1
	}
	if neededRemoteHelpers < 0 {
		neededRemoteHelpers = 0
	}

	uniqueMatches := buildUniqueMatches(peerMatchedCIDs)

	steps := make([]RepairStep, 0)
	otherElementSources := make([]peer.ID, 0)
	selectedHelpers := make([]peer.ID, 0)

	directCount := 0
	missingCount := 0

	// Direct chunks always come first.
	// Each direct chunk is one sequential step with one transfer.
	for _, c := range shardCIDs {
		c = cleanCIDString(c)
		if c == "" {
			continue
		}

		if peerHasCID(peerMatchedCIDs, repairPeer, c) {
			continue
		}

		if uniqueMatches[c] {
			possibleSources := sourcesForCID(
				c,
				repairPeer,
				failedPeer,
				peerMatchedCIDs,
				topology,
			)

			if len(possibleSources) > 0 {
				src, ok := chooseDynamicDirectSource(
					scheduledJobs,
					shard,
					repairPeer,
					c,
					possibleSources,
					steps,
					localChunkCount,
					directCount,
					missingCount,
					otherElementSources,
					topology,
					chunkMB,
					repairPeerLocalHelper,
					neededRemoteHelpers,
				)

				if ok {
					directCount++
					otherElementSources = append(otherElementSources, src)

					steps = append(steps, RepairStep{
						Kind: "direct",
						Transfers: []Transfer{
							{
								Src:    src,
								Dst:    repairPeer,
								SizeMB: chunkMB,
								Kind:   "direct",
								CID:    c,
							},
						},
					})
					continue
				}
			}
		}

		missingCount++
	}

	if missingCount > 0 {
		if len(remoteHelpers) < neededRemoteHelpers {
			return RepairJob{}, false
		}

		helpers, ok := chooseGreedyDynamicHelpers(
			scheduledJobs,
			shard,
			repairPeer,
			remoteHelpers,
			neededRemoteHelpers,
			steps,
			localChunkCount,
			directCount,
			missingCount,
			otherElementSources,
			topology,
			chunkMB,
			repairPeerLocalHelper,
		)

		if !ok {
			return RepairJob{}, false
		}

		selectedHelpers = helpers

		// Aggregated missing step:
		// all missing chunks are represented as one reconstruction step.
		// Each helper sends missingCount * chunkMB.
		steps = append(steps, buildAggregatedMissingStep(
			selectedHelpers,
			repairPeer,
			missingCount,
			chunkMB,
		))
	}

	job := RepairJob{
		Shard:      shard,
		RepairPeer: repairPeer,
		Steps:      steps,

		LocalChunkCount:   localChunkCount,
		DirectChunkCount:  directCount,
		MissingChunkCount: missingCount,

		OtherElementSources: otherElementSources,
		SelectedHelpers:     selectedHelpers,

		RepairPeerLocalHelper: repairPeerLocalHelper,
		NeededRemoteHelpers:   neededRemoteHelpers,
	}

	_ = shardSize

	return job, true
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
	fmt.Println("In DYNAMIC BANDWIDTH-AWARE MAX-MIN Repair Strategy with Greedy Helpers and Aggregated Missing Step !!!")

	assignments := make(map[peer.ID][]api.Pin)
	assignedPairs := make([]MaxMinAssignment, 0)

	if len(failedShards) == 0 || len(candidatePeers) == 0 {
		return assignments, assignedPairs
	}

	candidatePeers = sortedUniquePeers(candidatePeers)

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

		_, _, peerMatches, peerMatchedCIDs := getSimilarity(shard)

		precomputed[shardKey] = ShardPrecompute{
			Shard: shard,

			ShardCIDs: shardCIDs,
			ShardSize: shardSize,

			HelperCandidates: sortedUniquePeers(helperCandidates),
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

	scheduledJobs := make([]RepairJob, 0)

	for len(unscheduled) > 0 {
		type CandidateBest struct {
			Shard api.Pin
			Peer  peer.ID
			Job   RepairJob

			Makespan float64
		}

		bestForShard := make(map[string]CandidateBest)

		for _, shard := range unscheduled {
			pc := precomputed[shard.Cid.String()]

			bestPeer := peer.ID("")
			var bestJob RepairJob
			bestMakespan := math.Inf(1)

			for _, repairPeer := range candidatePeers {
				if repairPeer == failedPeer {
					continue
				}

				if topologyNodeIn(topology, repairPeer) == 0 {
					continue
				}

				job, ok := buildDynamicRepairJobForPeer(
					scheduledJobs,
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
					chunkMB,
				)

				if !ok {
					continue
				}

				res := simulateRepairJobs(appendJob(scheduledJobs, job), topology)

				if math.IsInf(res.TotalFinishTime, 1) {
					continue
				}

				if res.TotalFinishTime < bestMakespan ||
					(res.TotalFinishTime == bestMakespan && (bestPeer == "" || repairPeer.String() < bestPeer.String())) {
					bestMakespan = res.TotalFinishTime
					bestPeer = repairPeer
					bestJob = job
				}
			}

			if bestPeer != "" && !math.IsInf(bestMakespan, 1) {
				bestForShard[shard.Cid.String()] = CandidateBest{
					Shard:    shard,
					Peer:     bestPeer,
					Job:      bestJob,
					Makespan: bestMakespan,
				}
			}
		}

		if len(bestForShard) == 0 {
			break
		}

		chosenIndex := -1
		chosenMakespan := -1.0
		chosenKey := ""

		for idx, shard := range unscheduled {
			key := shard.Cid.String()
			cand, ok := bestForShard[key]
			if !ok {
				continue
			}

			if chosenIndex == -1 ||
				cand.Makespan > chosenMakespan ||
				(cand.Makespan == chosenMakespan && key < chosenKey) {
				chosenIndex = idx
				chosenMakespan = cand.Makespan
				chosenKey = key
			}
		}

		if chosenIndex == -1 {
			break
		}

		chosenShard := unscheduled[chosenIndex]
		chosen := bestForShard[chosenShard.Cid.String()]

		scheduledJobs = append(scheduledJobs, chosen.Job)

		finalRes := simulateRepairJobs(scheduledJobs, topology)
		jobFinish := finalRes.JobFinishTimes[chosenShard.Cid.String()]
		chosen.Job.FinishTime = jobFinish

		assignments[chosen.Peer] = append(assignments[chosen.Peer], chosenShard)

		pc := precomputed[chosenShard.Cid.String()]

		estimate := MaxMinEstimate{
			Shard:      chosenShard,
			RepairPeer: chosen.Peer,

			ProcessingTime: jobFinish,
			FinishTime:     jobFinish,

			ShardSize: pc.ShardSize,

			LocalChunkCount:   chosen.Job.LocalChunkCount,
			DirectChunkCount:  chosen.Job.DirectChunkCount,
			MissingChunkCount: chosen.Job.MissingChunkCount,

			OtherElementSources: chosen.Job.OtherElementSources,
			SelectedHelpers:     chosen.Job.SelectedHelpers,

			RepairPeerLocalHelper: chosen.Job.RepairPeerLocalHelper,
			NeededRemoteHelpers:   chosen.Job.NeededRemoteHelpers,

			Job: chosen.Job,
		}

		assignedPairs = append(assignedPairs, MaxMinAssignment{
			Shard:    chosenShard,
			Estimate: estimate,
		})

		fmt.Printf(
			"DYNAMIC MAX-MIN assigned shard=%s repairPeer=%s jobFinish=%f currentTotalMakespan=%f local=%d direct=%d missing=%d steps=%d helpers=%d\n",
			chosenShard.Name,
			chosen.Peer.String(),
			jobFinish,
			finalRes.TotalFinishTime,
			chosen.Job.LocalChunkCount,
			chosen.Job.DirectChunkCount,
			chosen.Job.MissingChunkCount,
			len(chosen.Job.Steps),
			len(chosen.Job.SelectedHelpers),
		)

		unscheduled = append(
			unscheduled[:chosenIndex],
			unscheduled[chosenIndex+1:]...,
		)
	}

	return assignments, assignedPairs
}
func ScheduleGlobalSauff2(
	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,

	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (peer.ID, []string, map[peer.ID]int, map[peer.ID][]string),
) (map[peer.ID][]api.Pin, []MaxMinAssignment) {
	fmt.Println("In DYNAMIC BANDWIDTH-AWARE SAUFF2 Repair Strategy with Greedy Helpers and Aggregated Missing Step !!!")

	assignments := make(map[peer.ID][]api.Pin)
	assignedPairs := make([]MaxMinAssignment, 0)

	if len(failedShards) == 0 || len(candidatePeers) == 0 {
		return assignments, assignedPairs
	}

	candidatePeers = sortedUniquePeers(candidatePeers)

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

		_, _, peerMatches, peerMatchedCIDs := getSimilarity(shard)

		precomputed[shardKey] = ShardPrecompute{
			Shard: shard,

			ShardCIDs: shardCIDs,
			ShardSize: shardSize,

			HelperCandidates: sortedUniquePeers(helperCandidates),
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

	scheduledJobs := make([]RepairJob, 0)

	for len(unscheduled) > 0 {
		type CandidateOption struct {
			Peer     peer.ID
			Job      RepairJob
			Makespan float64
		}

		type SauffBest struct {
			Shard api.Pin

			BestPeer     peer.ID
			BestJob      RepairJob
			BestMakespan float64

			SecondMakespan float64

			MinProcessingTime float64
			WeightedScore     float64
		}

		bestForShard := make(map[string]SauffBest)

		for _, shard := range unscheduled {
			pc := precomputed[shard.Cid.String()]

			options := make([]CandidateOption, 0)

			for _, repairPeer := range candidatePeers {
				if repairPeer == failedPeer {
					continue
				}

				if topologyNodeIn(topology, repairPeer) == 0 {
					continue
				}

				job, ok := buildDynamicRepairJobForPeer(
					scheduledJobs,
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
					chunkMB,
				)

				if !ok {
					continue
				}

				res := simulateRepairJobs(appendJob(scheduledJobs, job), topology)

				if math.IsInf(res.TotalFinishTime, 1) {
					continue
				}

				options = append(options, CandidateOption{
					Peer:     repairPeer,
					Job:      job,
					Makespan: res.TotalFinishTime,
				})
			}

			if len(options) == 0 {
				continue
			}

			sort.Slice(options, func(i, j int) bool {
				if options[i].Makespan == options[j].Makespan {
					return options[i].Peer.String() < options[j].Peer.String()
				}
				return options[i].Makespan < options[j].Makespan
			})

			best := options[0]
			secondMakespan := best.Makespan

			if len(options) >= 2 {
				secondMakespan = options[1].Makespan
			}

			bestJobAlone := simulateRepairJobs([]RepairJob{best.Job}, topology)
			minProcessingTime := bestJobAlone.TotalFinishTime

			weightedScore := math.Inf(1)

			if len(options) >= 2 && best.Makespan > 0 {
				weightedScore =
					(secondMakespan - best.Makespan) *
						(minProcessingTime / best.Makespan)
			}

			bestForShard[shard.Cid.String()] = SauffBest{
				Shard: shard,

				BestPeer:     best.Peer,
				BestJob:      best.Job,
				BestMakespan: best.Makespan,

				SecondMakespan: secondMakespan,

				MinProcessingTime: minProcessingTime,
				WeightedScore:     weightedScore,
			}
		}

		if len(bestForShard) == 0 {
			break
		}

		chosenIndex := -1
		chosenKey := ""
		chosenWeighted := -1.0
		chosenBestMakespan := math.Inf(1)

		for idx, shard := range unscheduled {
			key := shard.Cid.String()
			cand, ok := bestForShard[key]
			if !ok {
				continue
			}

			if chosenIndex == -1 ||
				cand.WeightedScore > chosenWeighted ||
				(cand.WeightedScore == chosenWeighted && cand.BestMakespan < chosenBestMakespan) ||
				(cand.WeightedScore == chosenWeighted && cand.BestMakespan == chosenBestMakespan && key < chosenKey) {
				chosenIndex = idx
				chosenKey = key
				chosenWeighted = cand.WeightedScore
				chosenBestMakespan = cand.BestMakespan
			}
		}

		if chosenIndex == -1 {
			break
		}

		chosenShard := unscheduled[chosenIndex]
		chosen := bestForShard[chosenShard.Cid.String()]

		scheduledJobs = append(scheduledJobs, chosen.BestJob)

		finalRes := simulateRepairJobs(scheduledJobs, topology)
		jobFinish := finalRes.JobFinishTimes[chosenShard.Cid.String()]
		chosen.BestJob.FinishTime = jobFinish

		assignments[chosen.BestPeer] = append(assignments[chosen.BestPeer], chosenShard)

		pc := precomputed[chosenShard.Cid.String()]

		estimate := MaxMinEstimate{
			Shard:      chosenShard,
			RepairPeer: chosen.BestPeer,

			ProcessingTime: jobFinish,
			FinishTime:     jobFinish,

			ShardSize: pc.ShardSize,

			LocalChunkCount:   chosen.BestJob.LocalChunkCount,
			DirectChunkCount:  chosen.BestJob.DirectChunkCount,
			MissingChunkCount: chosen.BestJob.MissingChunkCount,

			OtherElementSources: chosen.BestJob.OtherElementSources,
			SelectedHelpers:     chosen.BestJob.SelectedHelpers,

			RepairPeerLocalHelper: chosen.BestJob.RepairPeerLocalHelper,
			NeededRemoteHelpers:   chosen.BestJob.NeededRemoteHelpers,

			Job: chosen.BestJob,
		}

		assignedPairs = append(assignedPairs, MaxMinAssignment{
			Shard:    chosenShard,
			Estimate: estimate,
		})

		fmt.Printf(
			"SAUFF2 assigned shard=%s repairPeer=%s jobFinish=%f currentTotalMakespan=%f weightedScore=%f bestMakespan=%f secondMakespan=%f minProcessingTime=%f local=%d direct=%d missing=%d steps=%d helpers=%d\n",
			chosenShard.Name,
			chosen.BestPeer.String(),
			jobFinish,
			finalRes.TotalFinishTime,
			chosen.WeightedScore,
			chosen.BestMakespan,
			chosen.SecondMakespan,
			chosen.MinProcessingTime,
			chosen.BestJob.LocalChunkCount,
			chosen.BestJob.DirectChunkCount,
			chosen.BestJob.MissingChunkCount,
			len(chosen.BestJob.Steps),
			len(chosen.BestJob.SelectedHelpers),
		)

		unscheduled = append(
			unscheduled[:chosenIndex],
			unscheduled[chosenIndex+1:]...,
		)
	}

	return assignments, assignedPairs
}

func buildDynamicRepairJobForPeer(
	scheduledJobs []RepairJob,
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
	chunkMB float64,
) (RepairJob, bool) {
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

		if topologyPairwise(topology, h, repairPeer) > 0 {
			remoteHelpers = append(remoteHelpers, h)
		}
	}

	neededRemoteHelpers := n

	if repairPeerLocalHelper {
		neededRemoteHelpers = n - 1
	}

	if neededRemoteHelpers < 0 {
		neededRemoteHelpers = 0
	}

	uniqueMatches := buildUniqueMatches(peerMatchedCIDs)

	steps := make([]RepairStep, 0)

	otherElementSources := make([]peer.ID, 0)
	selectedHelpers := make([]peer.ID, 0)

	directCount := 0
	missingCount := 0

	// NEW:
	// Store all chunks already found locally or directly in the system.
	commonCIDs := make([]string, 0)

	// Direct chunks are counted and their sources are stored,
	// but they are NOT added as simulation steps.
	for _, c := range shardCIDs {
		c = cleanCIDString(c)

		if c == "" {
			continue
		}

		// LOCAL CHUNK
		if peerHasCID(peerMatchedCIDs, repairPeer, c) {
			commonCIDs = append(commonCIDs, c)
			continue
		}

		// DIRECT CHUNK
		if uniqueMatches[c] {
			possibleSources := sourcesForCID(
				c,
				repairPeer,
				failedPeer,
				peerMatchedCIDs,
				topology,
			)

			if len(possibleSources) > 0 {
				// Choose best source using highest pairwise bandwidth.
				bestSrc := possibleSources[0]
				bestBw := topologyPairwise(topology, bestSrc, repairPeer)

				for _, src := range possibleSources[1:] {
					bw := topologyPairwise(topology, src, repairPeer)

					if bw > bestBw ||
						(bw == bestBw && src.String() < bestSrc.String()) {
						bestSrc = src
						bestBw = bw
					}
				}

				directCount++
				otherElementSources = append(otherElementSources, bestSrc)

				// NEW:
				// Direct chunk found in the system.
				commonCIDs = append(commonCIDs, c)

				// IMPORTANT:
				// No direct RepairStep is appended.
				continue
			}
		}

		// MISSING CHUNK
		missingCount++
	}

	if missingCount > 0 {
		if len(remoteHelpers) < neededRemoteHelpers {
			return RepairJob{}, false
		}

		helpers, ok := chooseGreedyDynamicHelpers(
			scheduledJobs,
			shard,
			repairPeer,
			remoteHelpers,
			neededRemoteHelpers,
			steps,
			localChunkCount,
			directCount,
			missingCount,
			otherElementSources,
			topology,
			chunkMB,
			repairPeerLocalHelper,
		)

		if !ok {
			return RepairJob{}, false
		}

		selectedHelpers = helpers

		// Only missing chunks are simulated.
		steps = append(steps, buildAggregatedMissingStep(
			selectedHelpers,
			repairPeer,
			missingCount,
			chunkMB,
		))
	}

	// NEW:
	// Save local + direct chunks into shard metadata.
	setCommonMetadata_here(&shard, commonCIDs)

	job := RepairJob{
		Shard:      shard,
		RepairPeer: repairPeer,
		Steps:      steps,

		LocalChunkCount:   localChunkCount,
		DirectChunkCount:  directCount,
		MissingChunkCount: missingCount,

		OtherElementSources: otherElementSources,
		SelectedHelpers:     selectedHelpers,

		RepairPeerLocalHelper: repairPeerLocalHelper,
		NeededRemoteHelpers:   neededRemoteHelpers,
	}

	_ = shardSize

	return job, true
}
func setCommonMetadataSimple(pin *api.Pin, common []string) {
	cleaned := make([]string, 0, len(common))
	seen := make(map[string]bool)

	for _, c := range common {
		c = cleanCIDString(c)
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true
		cleaned = append(cleaned, c)
	}

	if pin.Metadata == nil {
		pin.Metadata = make(map[string]string)
	}

	pin.Metadata["common"] = strings.Join(cleaned, ",")
}

func linkKey(src, dst peer.ID) string {
	return src.String() + "->" + dst.String()
}

func simpleEffectiveRateWithLoads(
	src peer.ID,
	dst peer.ID,
	topology *NetworkTopology,
	receiverLoad int,
	linkLoad int,
	parallelIn int,
) float64 {
	pair := topologyPairwise(topology, src, dst)
	in := topologyNodeIn(topology, dst)

	if pair == 0 || in == 0 {
		return 0
	}

	linkDenom := 1 + linkLoad
	if linkDenom <= 0 {
		linkDenom = 1
	}

	inDenom := parallelIn + receiverLoad
	if inDenom <= 0 {
		inDenom = 1
	}

	linkShare := float64(pair) / float64(linkDenom)
	inShare := float64(in) / float64(inDenom)

	if linkShare < inShare {
		return linkShare
	}

	return inShare
}

func selectSimpleHelpersWithLoads(
	remoteHelpers []peer.ID,
	repairPeer peer.ID,
	neededRemoteHelpers int,
	topology *NetworkTopology,
	peerLoad int,
	linkLoad map[string]int,
) ([]peer.ID, bool) {
	if neededRemoteHelpers == 0 {
		return []peer.ID{}, true
	}

	remoteHelpers = sortedUniquePeers(remoteHelpers)

	type helperScore struct {
		Helper peer.ID
		Rate   float64
	}

	scores := make([]helperScore, 0)

	for _, h := range remoteHelpers {
		key := linkKey(h, repairPeer)

		rate := simpleEffectiveRateWithLoads(
			h,
			repairPeer,
			topology,
			peerLoad,
			linkLoad[key],
			neededRemoteHelpers,
		)

		if rate <= 0 {
			continue
		}

		scores = append(scores, helperScore{
			Helper: h,
			Rate:   rate,
		})
	}

	if len(scores) < neededRemoteHelpers {
		return nil, false
	}

	sort.Slice(scores, func(i, j int) bool {
		if scores[i].Rate == scores[j].Rate {
			return scores[i].Helper.String() < scores[j].Helper.String()
		}
		return scores[i].Rate > scores[j].Rate
	})

	selected := make([]peer.ID, 0, neededRemoteHelpers)
	for i := 0; i < neededRemoteHelpers; i++ {
		selected = append(selected, scores[i].Helper)
	}

	return selected, true
}

func buildSimpleRepairJobEstimate(
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
	chunkMB float64,
	peerLoad int,
	linkLoad map[string]int,
) (RepairJob, float64, []string, bool) {
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

		if topologyPairwise(topology, h, repairPeer) > 0 {
			remoteHelpers = append(remoteHelpers, h)
		}
	}

	neededRemoteHelpers := n
	if repairPeerLocalHelper {
		neededRemoteHelpers = n - 1
	}
	if neededRemoteHelpers < 0 {
		neededRemoteHelpers = 0
	}

	uniqueMatches := buildUniqueMatches(peerMatchedCIDs)

	directCount := 0
	missingCount := 0

	directTime := 0.0
	reconstructTime := 0.0

	otherElementSources := make([]peer.ID, 0)
	selectedHelpers := make([]peer.ID, 0)
	steps := make([]RepairStep, 0)

	commonCIDs := make([]string, 0)
	usedLinks := make([]string, 0)

	for _, c := range shardCIDs {
		c = cleanCIDString(c)
		if c == "" {
			continue
		}

		if peerHasCID(peerMatchedCIDs, repairPeer, c) {
			commonCIDs = append(commonCIDs, c)
			continue
		}

		if uniqueMatches[c] {
			possibleSources := sourcesForCID(
				c,
				repairPeer,
				failedPeer,
				peerMatchedCIDs,
				topology,
			)

			if len(possibleSources) > 0 {
				bestSrc := peer.ID("")
				bestRate := 0.0
				bestKey := ""

				for _, src := range possibleSources {
					key := linkKey(src, repairPeer)

					rate := simpleEffectiveRateWithLoads(
						src,
						repairPeer,
						topology,
						peerLoad,
						linkLoad[key],
						1,
					)

					if rate > bestRate ||
						(rate == bestRate && (bestSrc == "" || src.String() < bestSrc.String())) {
						bestSrc = src
						bestRate = rate
						bestKey = key
					}
				}

				if bestSrc != "" && bestRate > 0 {
					directCount++
					otherElementSources = append(otherElementSources, bestSrc)
					commonCIDs = append(commonCIDs, c)
					usedLinks = append(usedLinks, bestKey)

					directTime += chunkMB / bestRate
					continue
				}
			}
		}

		missingCount++
	}

	if missingCount > 0 {
		if len(remoteHelpers) < neededRemoteHelpers {
			return RepairJob{}, math.Inf(1), nil, false
		}

		helpers, ok := selectSimpleHelpersWithLoads(
			remoteHelpers,
			repairPeer,
			neededRemoteHelpers,
			topology,
			peerLoad,
			linkLoad,
		)

		if !ok {
			return RepairJob{}, math.Inf(1), nil, false
		}

		selectedHelpers = helpers

		slowestRate := math.Inf(1)

		for _, h := range selectedHelpers {
			key := linkKey(h, repairPeer)

			rate := simpleEffectiveRateWithLoads(
				h,
				repairPeer,
				topology,
				peerLoad,
				linkLoad[key],
				neededRemoteHelpers,
			)

			if rate <= 0 {
				return RepairJob{}, math.Inf(1), nil, false
			}

			if rate < slowestRate {
				slowestRate = rate
			}

			usedLinks = append(usedLinks, key)
		}

		if math.IsInf(slowestRate, 1) || slowestRate <= 0 {
			return RepairJob{}, math.Inf(1), nil, false
		}

		reconstructTime = (float64(missingCount) * chunkMB) / slowestRate

		steps = append(steps, buildAggregatedMissingStep(
			selectedHelpers,
			repairPeer,
			missingCount,
			chunkMB,
		))
	}

	processingTime := directTime + reconstructTime

	setCommonMetadataSimple(&shard, commonCIDs)

	job := RepairJob{
		Shard:      shard,
		RepairPeer: repairPeer,
		Steps:      steps,

		LocalChunkCount:   localChunkCount,
		DirectChunkCount:  directCount,
		MissingChunkCount: missingCount,

		OtherElementSources: otherElementSources,
		SelectedHelpers:     selectedHelpers,

		RepairPeerLocalHelper: repairPeerLocalHelper,
		NeededRemoteHelpers:   neededRemoteHelpers,
	}

	_ = shardSize

	return job, processingTime, usedLinks, true
}

func ScheduleGlobalMaxMinSimple(
	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,

	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (peer.ID, []string, map[peer.ID]int, map[peer.ID][]string),
) (map[peer.ID][]api.Pin, []MaxMinAssignment) {
	fmt.Println("In SIMPLE BANDWIDTH-AWARE MAX-MIN Repair Strategy with Direct + Missing Estimate !!!")

	assignments := make(map[peer.ID][]api.Pin)
	assignedPairs := make([]MaxMinAssignment, 0)

	if len(failedShards) == 0 || len(candidatePeers) == 0 {
		return assignments, assignedPairs
	}

	candidatePeers = sortedUniquePeers(candidatePeers)

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

		_, _, peerMatches, peerMatchedCIDs := getSimilarity(shard)

		precomputed[shardKey] = ShardPrecompute{
			Shard:            shard,
			ShardCIDs:        shardCIDs,
			ShardSize:        shardSize,
			HelperCandidates: sortedUniquePeers(helperCandidates),
			N:                n,
			PeerMatches:      peerMatches,
			PeerMatchedCIDs:  peerMatchedCIDs,
		}
	}

	unscheduled := make([]api.Pin, 0)
	for _, shard := range failedShards {
		if _, ok := precomputed[shard.Cid.String()]; ok {
			unscheduled = append(unscheduled, shard)
		}
	}

	peerEndTime := make(map[peer.ID]float64)
	peerLoad := make(map[peer.ID]int)
	linkLoad := make(map[string]int)

	for _, p := range candidatePeers {
		peerEndTime[p] = 0
		peerLoad[p] = 0
	}

	for len(unscheduled) > 0 {
		type CandidateBest struct {
			Shard api.Pin
			Peer  peer.ID
			Job   RepairJob

			ProcessingTime float64
			CompletionTime float64
			UsedLinks      []string
		}

		bestForShard := make(map[string]CandidateBest)

		for _, shard := range unscheduled {
			pc := precomputed[shard.Cid.String()]

			bestPeer := peer.ID("")
			var bestJob RepairJob
			bestProcessing := math.Inf(1)
			bestCompletion := math.Inf(1)
			bestUsedLinks := []string{}

			for _, repairPeer := range candidatePeers {
				if repairPeer == failedPeer {
					continue
				}

				if topologyNodeIn(topology, repairPeer) == 0 {
					continue
				}

				job, processing, usedLinks, ok := buildSimpleRepairJobEstimate(
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
					chunkMB,
					peerLoad[repairPeer],
					linkLoad,
				)

				if !ok {
					continue
				}

				completion := peerEndTime[repairPeer] + processing

				if completion < bestCompletion ||
					(completion == bestCompletion && (bestPeer == "" || repairPeer.String() < bestPeer.String())) {
					bestCompletion = completion
					bestProcessing = processing
					bestPeer = repairPeer
					bestJob = job
					bestUsedLinks = usedLinks
				}
			}

			if bestPeer != "" && !math.IsInf(bestCompletion, 1) {
				bestForShard[shard.Cid.String()] = CandidateBest{
					Shard:          shard,
					Peer:           bestPeer,
					Job:            bestJob,
					ProcessingTime: bestProcessing,
					CompletionTime: bestCompletion,
					UsedLinks:      bestUsedLinks,
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

		peerEndTime[chosen.Peer] += chosen.ProcessingTime
		peerLoad[chosen.Peer]++

		for _, lk := range chosen.UsedLinks {
			linkLoad[lk]++
		}

		chosen.Job.FinishTime = peerEndTime[chosen.Peer]

		assignments[chosen.Peer] = append(assignments[chosen.Peer], chosenShard)

		pc := precomputed[chosenShard.Cid.String()]

		estimate := MaxMinEstimate{
			Shard:          chosenShard,
			RepairPeer:     chosen.Peer,
			ProcessingTime: chosen.ProcessingTime,
			FinishTime:     chosen.Job.FinishTime,
			ShardSize:      pc.ShardSize,

			LocalChunkCount:   chosen.Job.LocalChunkCount,
			DirectChunkCount:  chosen.Job.DirectChunkCount,
			MissingChunkCount: chosen.Job.MissingChunkCount,

			OtherElementSources: chosen.Job.OtherElementSources,
			SelectedHelpers:     chosen.Job.SelectedHelpers,

			RepairPeerLocalHelper: chosen.Job.RepairPeerLocalHelper,
			NeededRemoteHelpers:   chosen.Job.NeededRemoteHelpers,

			Job: chosen.Job,
		}

		assignedPairs = append(assignedPairs, MaxMinAssignment{
			Shard:    chosenShard,
			Estimate: estimate,
		})

		fmt.Printf(
			"SIMPLE MAX-MIN assigned shard=%s repairPeer=%s processing=%f finish=%f local=%d direct=%d missing=%d helpers=%d usedLinks=%d\n",
			chosenShard.Name,
			chosen.Peer.String(),
			chosen.ProcessingTime,
			chosen.Job.FinishTime,
			chosen.Job.LocalChunkCount,
			chosen.Job.DirectChunkCount,
			chosen.Job.MissingChunkCount,
			len(chosen.Job.SelectedHelpers),
			len(chosen.UsedLinks),
		)

		unscheduled = append(
			unscheduled[:chosenIndex],
			unscheduled[chosenIndex+1:]...,
		)
	}

	return assignments, assignedPairs
}

func ScheduleGlobalSauff2Simple(
	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,

	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (peer.ID, []string, map[peer.ID]int, map[peer.ID][]string),
) (map[peer.ID][]api.Pin, []MaxMinAssignment) {
	fmt.Println("In SIMPLE BANDWIDTH-AWARE SAUFF2 Repair Strategy with Direct + Missing Estimate !!!")

	assignments := make(map[peer.ID][]api.Pin)
	assignedPairs := make([]MaxMinAssignment, 0)

	if len(failedShards) == 0 || len(candidatePeers) == 0 {
		return assignments, assignedPairs
	}

	candidatePeers = sortedUniquePeers(candidatePeers)

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

		_, _, peerMatches, peerMatchedCIDs := getSimilarity(shard)

		precomputed[shardKey] = ShardPrecompute{
			Shard:            shard,
			ShardCIDs:        shardCIDs,
			ShardSize:        shardSize,
			HelperCandidates: sortedUniquePeers(helperCandidates),
			N:                n,
			PeerMatches:      peerMatches,
			PeerMatchedCIDs:  peerMatchedCIDs,
		}
	}

	unscheduled := make([]api.Pin, 0)
	for _, shard := range failedShards {
		if _, ok := precomputed[shard.Cid.String()]; ok {
			unscheduled = append(unscheduled, shard)
		}
	}

	peerEndTime := make(map[peer.ID]float64)
	peerLoad := make(map[peer.ID]int)
	linkLoad := make(map[string]int)

	for _, p := range candidatePeers {
		peerEndTime[p] = 0
		peerLoad[p] = 0
	}

	for len(unscheduled) > 0 {
		type CandidateOption struct {
			Peer peer.ID
			Job  RepairJob

			ProcessingTime float64
			CompletionTime float64
			UsedLinks      []string
		}

		type SauffBest struct {
			Shard api.Pin

			BestPeer       peer.ID
			BestJob        RepairJob
			BestProcessing float64
			BestCompletion float64
			BestUsedLinks  []string

			SecondCompletion float64
			MinProcessing    float64
			WeightedScore    float64
		}

		bestForShard := make(map[string]SauffBest)

		for _, shard := range unscheduled {
			pc := precomputed[shard.Cid.String()]

			options := make([]CandidateOption, 0)

			for _, repairPeer := range candidatePeers {
				if repairPeer == failedPeer {
					continue
				}

				if topologyNodeIn(topology, repairPeer) == 0 {
					continue
				}

				job, processing, usedLinks, ok := buildSimpleRepairJobEstimate(
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
					chunkMB,
					peerLoad[repairPeer],
					linkLoad,
				)

				if !ok {
					continue
				}

				completion := peerEndTime[repairPeer] + processing

				options = append(options, CandidateOption{
					Peer:           repairPeer,
					Job:            job,
					ProcessingTime: processing,
					CompletionTime: completion,
					UsedLinks:      usedLinks,
				})
			}

			if len(options) == 0 {
				continue
			}

			sort.Slice(options, func(i, j int) bool {
				if options[i].CompletionTime == options[j].CompletionTime {
					return options[i].Peer.String() < options[j].Peer.String()
				}
				return options[i].CompletionTime < options[j].CompletionTime
			})

			best := options[0]
			secondCompletion := best.CompletionTime

			if len(options) >= 2 {
				secondCompletion = options[1].CompletionTime
			}

			minProcessing := math.Inf(1)
			for _, opt := range options {
				if opt.ProcessingTime < minProcessing {
					minProcessing = opt.ProcessingTime
				}
			}

			weighted := math.Inf(1)
			if len(options) >= 2 && best.CompletionTime > 0 {
				weighted = (secondCompletion - best.CompletionTime) *
					(minProcessing / best.CompletionTime)
			}

			bestForShard[shard.Cid.String()] = SauffBest{
				Shard:            shard,
				BestPeer:         best.Peer,
				BestJob:          best.Job,
				BestProcessing:   best.ProcessingTime,
				BestCompletion:   best.CompletionTime,
				BestUsedLinks:    best.UsedLinks,
				SecondCompletion: secondCompletion,
				MinProcessing:    minProcessing,
				WeightedScore:    weighted,
			}
		}

		if len(bestForShard) == 0 {
			break
		}

		chosenIndex := -1
		chosenKey := ""
		chosenWeighted := -1.0
		chosenBestCompletion := math.Inf(1)

		for idx, shard := range unscheduled {
			key := shard.Cid.String()
			cand, ok := bestForShard[key]
			if !ok {
				continue
			}

			if chosenIndex == -1 ||
				cand.WeightedScore > chosenWeighted ||
				(cand.WeightedScore == chosenWeighted && cand.BestCompletion < chosenBestCompletion) ||
				(cand.WeightedScore == chosenWeighted && cand.BestCompletion == chosenBestCompletion && key < chosenKey) {
				chosenIndex = idx
				chosenKey = key
				chosenWeighted = cand.WeightedScore
				chosenBestCompletion = cand.BestCompletion
			}
		}

		if chosenIndex == -1 {
			break
		}

		chosenShard := unscheduled[chosenIndex]
		chosen := bestForShard[chosenShard.Cid.String()]

		peerEndTime[chosen.BestPeer] += chosen.BestProcessing
		peerLoad[chosen.BestPeer]++

		for _, lk := range chosen.BestUsedLinks {
			linkLoad[lk]++
		}

		chosen.BestJob.FinishTime = peerEndTime[chosen.BestPeer]

		assignments[chosen.BestPeer] = append(assignments[chosen.BestPeer], chosenShard)

		pc := precomputed[chosenShard.Cid.String()]

		estimate := MaxMinEstimate{
			Shard:          chosenShard,
			RepairPeer:     chosen.BestPeer,
			ProcessingTime: chosen.BestProcessing,
			FinishTime:     chosen.BestJob.FinishTime,
			ShardSize:      pc.ShardSize,

			LocalChunkCount:   chosen.BestJob.LocalChunkCount,
			DirectChunkCount:  chosen.BestJob.DirectChunkCount,
			MissingChunkCount: chosen.BestJob.MissingChunkCount,

			OtherElementSources: chosen.BestJob.OtherElementSources,
			SelectedHelpers:     chosen.BestJob.SelectedHelpers,

			RepairPeerLocalHelper: chosen.BestJob.RepairPeerLocalHelper,
			NeededRemoteHelpers:   chosen.BestJob.NeededRemoteHelpers,

			Job: chosen.BestJob,
		}

		assignedPairs = append(assignedPairs, MaxMinAssignment{
			Shard:    chosenShard,
			Estimate: estimate,
		})

		fmt.Printf(
			"SIMPLE SAUFF2 assigned shard=%s repairPeer=%s processing=%f finish=%f weighted=%f local=%d direct=%d missing=%d helpers=%d usedLinks=%d\n",
			chosenShard.Name,
			chosen.BestPeer.String(),
			chosen.BestProcessing,
			chosen.BestJob.FinishTime,
			chosen.WeightedScore,
			chosen.BestJob.LocalChunkCount,
			chosen.BestJob.DirectChunkCount,
			chosen.BestJob.MissingChunkCount,
			len(chosen.BestJob.SelectedHelpers),
			len(chosen.BestUsedLinks),
		)

		unscheduled = append(
			unscheduled[:chosenIndex],
			unscheduled[chosenIndex+1:]...,
		)
	}

	return assignments, assignedPairs
}
