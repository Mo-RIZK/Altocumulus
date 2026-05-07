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

func topologyNodeOut(t *NetworkTopology, p peer.ID) uint64 {
	if t == nil || t.NodesByPeer == nil {
		return 0
	}
	n := t.NodesByPeer[p]
	if n == nil {
		return 0
	}
	return n.GlobalOut
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

func selectBestHelpersByBandwidth(
	helpers []peer.ID,
	repairPeer peer.ID,
	n int,
	topology *NetworkTopology,
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
		bwi := topology.EffectiveBandwidth(filtered[i], repairPeer)
		bwj := topology.EffectiveBandwidth(filtered[j], repairPeer)

		if bwi == bwj {
			return filtered[i].String() < filtered[j].String()
		}
		return bwi > bwj
	})

	if len(filtered) > n {
		filtered = filtered[:n]
	}

	return filtered
}

func chooseBestSourceForCID(
	cidStr string,
	repairPeer peer.ID,
	failedPeer peer.ID,
	peerMatchedCIDs map[peer.ID][]string,
	topology *NetworkTopology,
) peer.ID {
	cidStr = cleanCIDString(cidStr)

	bestPeer := peer.ID("")
	bestBw := uint64(0)

	for p, cids := range peerMatchedCIDs {
		if p == failedPeer || p == repairPeer {
			continue
		}

		hasCID := false
		for _, c := range cids {
			if cleanCIDString(c) == cidStr {
				hasCID = true
				break
			}
		}

		if !hasCID {
			continue
		}

		bw := topology.EffectiveBandwidth(p, repairPeer)
		if bw == 0 {
			continue
		}

		if bestPeer == "" || bw > bestBw || (bw == bestBw && p.String() < bestPeer.String()) {
			bestPeer = p
			bestBw = bw
		}
	}

	return bestPeer
}

func buildRepairJobForPeer(
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

	steps := make([]RepairStep, 0)
	otherElementSources := make([]peer.ID, 0)
	selectedHelpers := make([]peer.ID, 0)

	directCount := 0
	missingCount := 0

	// Direct chunks are sequential:
	// one direct chunk = one step = one transfer.
	for _, c := range shardCIDs {
		c = cleanCIDString(c)
		if c == "" {
			continue
		}

		if peerHasCID(peerMatchedCIDs, repairPeer, c) {
			continue
		}

		if uniqueMatches[c] {
			src := chooseBestSourceForCID(
				c,
				repairPeer,
				failedPeer,
				peerMatchedCIDs,
				topology,
			)

			if src != "" {
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

		missingCount++
	}

	if missingCount > 0 {
		if len(remoteHelpers) < neededRemoteHelpers {
			return RepairJob{}, false
		}

		selectedHelpers = selectBestHelpersByBandwidth(
			remoteHelpers,
			repairPeer,
			neededRemoteHelpers,
			topology,
		)

		if len(selectedHelpers) < neededRemoteHelpers {
			return RepairJob{}, false
		}

		for _, h := range selectedHelpers {
			if topology.EffectiveBandwidth(h, repairPeer) == 0 {
				return RepairJob{}, false
			}
		}

		// Missing chunks are sequential.
		// For each missing chunk:
		// n helpers download in parallel in one step.
		for i := 0; i < missingCount; i++ {
			stepTransfers := make([]Transfer, 0)

			for _, h := range selectedHelpers {
				stepTransfers = append(stepTransfers, Transfer{
					Src:    h,
					Dst:    repairPeer,
					SizeMB: chunkMB,
					Kind:   "reconstruct",
				})
			}

			steps = append(steps, RepairStep{
				Kind:      "reconstruct",
				Transfers: stepTransfers,
			})
		}
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

	return job, true
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
		outCount := make(map[peer.ID]int)
		inCount := make(map[peer.ID]int)
		linkCount := make(map[peer.ID]map[peer.ID]int)

		for _, tr := range active {
			outCount[tr.Src]++
			inCount[tr.Dst]++

			if linkCount[tr.Src] == nil {
				linkCount[tr.Src] = make(map[peer.ID]int)
			}
			linkCount[tr.Src][tr.Dst]++
		}

		rates := make([]float64, len(active))
		minDelta := math.Inf(1)

		for i, tr := range active {
			srcOut := topologyNodeOut(topology, tr.Src)
			dstIn := topologyNodeIn(topology, tr.Dst)
			pair := topologyPairwise(topology, tr.Src, tr.Dst)

			if srcOut == 0 || dstIn == 0 || pair == 0 {
				return SimulationResult{
					TotalFinishTime: math.Inf(1),
					JobFinishTimes:  map[string]float64{},
				}
			}

			srcShare := float64(srcOut) / float64(outCount[tr.Src])
			dstShare := float64(dstIn) / float64(inCount[tr.Dst])
			linkShare := float64(pair) / float64(linkCount[tr.Src][tr.Dst])

			rate := srcShare
			if dstShare < rate {
				rate = dstShare
			}
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

		// A job step is complete only when no transfer from that same job+step remains active.
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

func ScheduleGlobalMaxMin(
	failedPeer peer.ID,
	failedShards []api.Pin,
	candidatePeers []peer.ID,
	topology *NetworkTopology,
	chunkMB float64,

	getSameStripe func(api.Pin) ([]api.Pin, []peer.ID, int, int),
	getSimilarity func(api.Pin) (peer.ID, []string, map[peer.ID]int, map[peer.ID][]string),
) (map[peer.ID][]api.Pin, []MaxMinAssignment) {
	fmt.Println("In PARALLEL-SIMULATION Max-Min Repair Strategy !!!")

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

	// Precompute once.
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

				job, ok := buildRepairJobForPeer(
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

				testJobs := make([]RepairJob, 0, len(scheduledJobs)+1)
				testJobs = append(testJobs, scheduledJobs...)
				testJobs = append(testJobs, job)

				res := simulateRepairJobs(testJobs, topology)

				if res.TotalFinishTime < bestMakespan ||
					(res.TotalFinishTime == bestMakespan && repairPeer.String() < bestPeer.String()) {
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

		// Max-Min:
		// for every shard, we already selected the peer with minimum makespan.
		// now choose the shard whose best makespan is maximum.
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

		estimate := MaxMinEstimate{
			Shard:      chosenShard,
			RepairPeer: chosen.Peer,

			ProcessingTime: jobFinish,
			FinishTime:     jobFinish,

			ShardSize: precomputed[chosenShard.Cid.String()].ShardSize,

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
			"MAX-MIN assigned shard=%s repairPeer=%s jobFinish=%f currentTotalMakespan=%f local=%d direct=%d missing=%d steps=%d\n",
			chosenShard.Name,
			chosen.Peer.String(),
			jobFinish,
			finalRes.TotalFinishTime,
			chosen.Job.LocalChunkCount,
			chosen.Job.DirectChunkCount,
			chosen.Job.MissingChunkCount,
			len(chosen.Job.Steps),
		)

		unscheduled = append(
			unscheduled[:chosenIndex],
			unscheduled[chosenIndex+1:]...,
		)
	}

	return assignments, assignedPairs
}
