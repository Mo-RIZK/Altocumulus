// Package sharding implements a sharding ClusterDAGService places
// content in different shards while it's being added, creating
// a final Cluster DAG and pinning it.
package sharding

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"time"

	"github.com/ipfs-cluster/ipfs-cluster/adder"
	"github.com/ipfs-cluster/ipfs-cluster/api"

	humanize "github.com/dustin/go-humanize"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

var logger = logging.Logger("shardingdags")

// DAGService is an implementation of a ClusterDAGService which
// shards content while adding among several IPFS Cluster peers,
// creating a Cluster DAG to track and pin that content selectively
// in the IPFS daemons allocated to it.
type DAGService struct {
	adder.BaseDAGService

	ctx       context.Context
	rpcClient *rpc.Client

	addParams api.AddParams
	output    chan<- api.AddedOutput

	addedSet *cid.Set

	// shard tracking
	shards map[string]cid.Cid

	// Current shard being built
	currentShard []*shard
	// Last flushed shard CID
	previousShard cid.Cid

	// shard tracking
	shardrep map[string]cid.Cid

	startTime time.Time
	totalSize uint64

	//in case of Erasure coding
	internal      uint64
	internalnodes []ipld.Node
	wg            sync.WaitGroup
	wait          bool
	current       int
	shardPINtime  time.Duration
	nodeparallel  []ipld.Node
	howmany       int
	flushtimes    int
	original      int
	parity        int
	lastCid       cid.Cid
	mu            sync.Mutex
	// Current shard being built
	currentShardrep *shard
	together        int
	intgor          []int
	nodegor         []ipld.Node
	shardgor        []*shard
	lengthsgor      []int
	try             ipld.Node
	topin           []every
	seq             bool
}

// New returns a new ClusterDAGService, which uses the given rpc client to perform
// Allocate, IPFSStream and Pin requests to other cluster components.
func New(ctx context.Context, rpc *rpc.Client, opts api.AddParams, out chan<- api.AddedOutput) *DAGService {
	// use a default value for this regardless of what is provided.
	opts.Mode = api.PinModeRecursive
	return &DAGService{
		ctx:       ctx,
		rpcClient: rpc,
		addParams: opts,
		output:    out,
		addedSet:  cid.NewSet(),
		shards:    make(map[string]cid.Cid),
		startTime: time.Now(),

		internal:      0,
		internalnodes: make([]ipld.Node, 0),
		wait:          false,
		current:       0,
		nodeparallel:  make([]ipld.Node, 0, opts.O+opts.P),
		currentShard:  make([]*shard, opts.O+opts.P),
		howmany:       0,
		flushtimes:    0,
		original:      opts.O,
		parity:        opts.P,
		shardrep:      make(map[string]cid.Cid),
		together:      0,
		nodegor:       make([]ipld.Node, 0, opts.O+opts.P),
		intgor:        make([]int, 0, opts.O+opts.P),
		lengthsgor:    make([]int, 0, opts.O+opts.P),
		shardgor:      make([]*shard, 0, opts.O+opts.P),
		try:           nil,
		topin:         make([]every, 0),
		seq:           opts.Seq,
	}
}

// Add puts the given node in its corresponding shard and sends it to the
// destination peers.
func (dgs *DAGService) Add(ctx context.Context, node ipld.Node) error {
	// FIXME: This will grow in memory
	/*if !dgs.addedSet.Visit(node.Cid()) {
	        return nil
	}*/
	if dgs.original == 1 || dgs.seq {
		fmt.Fprintf(os.Stdout, "Entered to sequentialllllllllllllll \n")
		return dgs.ingestBlockREP(ctx, node)
	} else {
		fmt.Fprintf(os.Stdout, "Entered to parallelllllll \n")
		return dgs.ingestBlock(ctx, node)
	}

}

// Close performs cleanup and should be called when the DAGService is not
// going to be used anymore.
func (dgs *DAGService) Close() error {
	if dgs.original == 1 || dgs.seq {
		if dgs.currentShardrep != nil {
			dgs.currentShardrep.Close()
		}
		return nil
	} else {
		if dgs.currentShard[dgs.current] != nil {
			for _, shard := range dgs.currentShard {
				shard.Close()
			}
		}
	}
	return nil
}

// Finalize finishes sharding, creates the cluster DAG and pins it along
// with the meta pin for the root node of the content.
func (dgs *DAGService) Finalize(ctx context.Context, dataRoot api.Cid) (api.Cid, error) {
	if dgs.original == 1 || dgs.seq {
		return dgs.FinalizeRep(ctx, dataRoot)
	}
	dgs.ingestLastBlocks(dgs.ctx)
	/*for i := 0; i < dgs.original+dgs.parity; i++ {
	        dgs.lastCid, _ = dgs.flushCurrentShard(ctx)
	        //if err != nil {
	        //      return api.NewCid(lastCid), err
	        //}
	        dgs.current = (dgs.current + 1) % (dgs.original + dgs.parity)
	}*/
	//dgs.lastCid , _ = dgs.flushCurrentShard(ctx)
	//if err != nil {
	//      return api.NewCid(lastCid), err
	//}
	dgs.lastCid, _ = dgs.flushCurrentShards(ctx)
	if !dgs.lastCid.Equals(dataRoot.Cid) {
		logger.Warnf("the last added CID (%s) is not the IPFS data root (%s). This is only normal when adding a single file without wrapping in directory.", dgs.lastCid, dataRoot)
	}
	fmt.Fprintf(os.Stdout, "Creating Cluster DAG %s\n", time.Now().Format("15:04:05.000"))

	start := time.Now()
	clusterDAGNodes, err := makeDAG(ctx, dgs.shards)
	if err != nil {
		return dataRoot, err
	}
	end := time.Now()
	fmt.Fprintf(os.Stdout, "CLUSTER DAG CREATION took %s \n", end.Sub(start).String())
	st := time.Now()
	// PutDAG to ourselves
	blocks := make(chan api.NodeWithMeta, 256)
	fmt.Fprintf(os.Stdout, "Sending cluster dag nodes and internal nodes %s\n", time.Now().Format("15:04:05.000"))

	go func() {
		defer close(blocks)
		for _, n := range clusterDAGNodes {
			select {
			case <-ctx.Done():
				logger.Error(ctx.Err())
				return //abort
			case blocks <- adder.IpldNodeToNodeWithMeta(n):
			}
		}

		for _, n := range dgs.internalnodes {
			select {
			case <-ctx.Done():
				logger.Error(ctx.Err())
				return //abort
			case blocks <- adder.IpldNodeToNodeWithMeta(n):
			}
		}
	}()

	// Stream these blocks and wait until we are done.
	bs := adder.NewBlockStreamer(ctx, dgs.rpcClient, []peer.ID{""}, blocks)
	select {
	case <-ctx.Done():
		return dataRoot, ctx.Err()
	case <-bs.Done():
	}
	for _, n := range dgs.internalnodes {
		pinn := api.PinWithOpts(api.NewCid(n.Cid()), dgs.addParams.PinOptions)
		pinn.ReplicationFactorMin = -1
		pinn.ReplicationFactorMax = -1
		pinn.MaxDepth = 0
		errr := adder.Pin(ctx, dgs.rpcClient, pinn)
		if errr != nil {
			return dataRoot, errr
		}
	}

	if err := bs.Err(); err != nil {
		return dataRoot, err
	}

	clusterDAG := clusterDAGNodes[0].Cid()

	dgs.sendOutput(api.AddedOutput{
		Name:        fmt.Sprintf("%s-clusterDAG", dgs.addParams.Name),
		Cid:         api.NewCid(clusterDAG),
		Size:        dgs.totalSize,
		Allocations: nil,
	})
	// Pin the ClusterDAG
	clusterDAGPin := api.PinWithOpts(api.NewCid(clusterDAG), dgs.addParams.PinOptions)
	clusterDAGPin.ReplicationFactorMin = -1
	clusterDAGPin.ReplicationFactorMax = -1
	clusterDAGPin.MaxDepth = 0 // pin direct
	clusterDAGPin.Name = fmt.Sprintf("%s-clusterDAG-EC()-chunksize", dgs.addParams.Name)
	clusterDAGPin.Type = api.ClusterDAGType
	clusterDAGPin.Reference = &dataRoot
	// Update object with response.
	err = adder.Pin(ctx, dgs.rpcClient, clusterDAGPin)
	if err != nil {
		return dataRoot, err
	}
	et := time.Now()
	fmt.Fprintf(os.Stdout, "PINNING shards took : %s\n", dgs.shardPINtime.String())
	fmt.Fprintf(os.Stdout, "SENDING Cluster dag nodes and INTERNAL NODES and pinning them took : %s \n", et.Sub(st).String())
	fmt.Fprintf(os.Stdout, "Overall ADD took : %s \n", et.Sub(dgs.startTime).String())

	// Pin the META pin
	/*metaPin := api.PinWithOpts(dataRoot, dgs.addParams.PinOptions)
	  metaPin.Type = api.MetaType
	  ref := api.NewCid(clusterDAG)
	  metaPin.Reference = &ref
	  metaPin.MaxDepth = 0 // irrelevant. Meta-pins are not pinned
	  err = adder.Pin(ctx, dgs.rpcClient, metaPin)
	  if err != nil {
	          return dataRoot, err
	  }

	  // Log some stats
	  dgs.logStats(metaPin.Cid, clusterDAGPin.Cid)*/

	// Consider doing this? Seems like overkill
	//
	// // Amend ShardPins to reference clusterDAG root hash as a Parent
	// shardParents := cid.NewSet()
	// shardParents.Add(clusterDAG)
	// for shardN, shard := range dgs.shardNodes {
	//      pin := api.PinWithOpts(shard, dgs.addParams)
	//      pin.Name := fmt.Sprintf("%s-shard-%s", pin.Name, shardN)
	//      pin.Type = api.ShardType
	//      pin.Parents = shardParents
	//      // FIXME: We don't know anymore the shard pin maxDepth
	//      // so we'd need to get the pin first.
	//      err := dgs.pin(pin)
	//      if err != nil {
	//              return err
	//      }
	// }

	return dataRoot, nil
}

// Allocations returns the current allocations for the current shard.
func (dgs *DAGService) Allocations() []peer.ID {
	// FIXME: this is probably not safe in concurrency?  However, there is
	// no concurrent execution of any code in the DAGService I think.
	if dgs.original == 1 || dgs.seq {
		if dgs.currentShardrep != nil {
			return dgs.currentShardrep.Allocations()
		}
		return nil
	} else {
		if dgs.currentShard[dgs.current] != nil {
			return dgs.currentShard[dgs.current].Allocations()
		}
		return nil
	}
}

func (dgs *DAGService) ingestBlock(ctx context.Context, n ipld.Node) error {
	dgs.howmany++
	//fmt.Fprintf(os.Stdout, " NOOOOMEEEEEEERRRRRRRROOOOO %d \n", dgs.howmany)
	shardd := dgs.currentShard

	// if we have no currentShard, create one
	if shardd[dgs.current] == nil {
		start := time.Now()
		logger.Infof("new shard for '%s': #%d", dgs.addParams.Name, len(dgs.shards))
		var err error
		// important: shards use the DAGService context.
		oppts := dgs.addParams.PinOptions
		oppts.ReplicationFactorMin = dgs.addParams.O + dgs.addParams.P
		oppts.ReplicationFactorMax = dgs.addParams.O + dgs.addParams.P
		allocs, err := adder.BlockAllocate(ctx, dgs.rpcClient, oppts)
		if err != nil {
			return err
		}
		for i := 0; i < dgs.addParams.O+dgs.addParams.P; i++ {
			shardd[i], err = NewShard(dgs.ctx, ctx, dgs.rpcClient, dgs.addParams.PinOptions, allocs[i])
			if err != nil {
				return err
			}
			dgs.currentShard[i] = shardd[i]
		}
		end := time.Now()
		fmt.Fprintf(os.Stdout, "ALLOCATING PART %s \n", end.Sub(start).String())
	}

	size := uint64(len(n.RawData()))

	if len(n.Links())>0{
		fmt.Fprintf(os.Stdout, "Inner nodeeeeeeeeeeeeeeeeeeeeeeeee with sizeeeeeeee %d \n",size)
	} else {
		fmt.Fprintf(os.Stdout, "LLEEEEEEAAAAAAAAFFFFFFFFFFF nodeeeeeeeeeeeeeeeeeeeeeeeee with sizeeeeeeee %d \n",size)
	}
	

	if dgs.internal == 0 {
		dgs.internal = size
		dgs.try = n
	} else {
		if dgs.internal != size {
			//save the internal nodes
			//FIXME: This will grow in memory
			fmt.Fprintf(os.Stdout, "Internal node save in memory %s cid : %s\n", time.Now().Format("15:04:05.000"), n.Cid().String())
			//dgs.internalnodes = append(dgs.internalnodes, n)
			return nil
		}
	}

	if len(dgs.nodeparallel) == dgs.addParams.O+dgs.addParams.P {
		//send nodes concurrently
		for _, node := range dgs.nodeparallel {
			sizee := uint64(len(node.RawData()))
			fmt.Fprintf(os.Stdout, "NODEEEEEEEEEEEEEEEEEE CIDDDDDDDD %s of  SIIIIIIZEEEEEEEEEE %d \n", node.Cid(), sizee)
			fmt.Fprintf(os.Stdout, "SHAAAAAAARRRRRRRRDDDDDDDDDDDDD %s of  SIIIIIIZEEEEEEEEEE %d \n", shardd[dgs.current].pinOptions.Name, shardd[dgs.current].Size())
			if shardd[dgs.current].Size()+sizee < shardd[dgs.current].Limit() {
				shardd[dgs.current].AddLink(ctx, node.Cid(), sizee)
				dgs.wg.Add(1)
				go func(shards []*shard, cur int, node ipld.Node) {
					defer dgs.wg.Done()
					shards[cur].sendBlock(ctx, node)
				}(dgs.currentShard, dgs.current, node)
				dgs.current = (dgs.current + 1) % (dgs.addParams.O + dgs.addParams.P)
			} else {
				fmt.Fprintf(os.Stdout, "FLUSSSSSSSSSSHHHHHHHHHHHHHHHHHHHHHHHHHHHH !!!!!!!!!1\n")
				_, err := dgs.flushCurrentShards(ctx)
				if err != nil {
					return err
				}
				return dgs.ingestBlock(ctx, n) // <-- retry ingest
			}
		}
		dgs.nodeparallel = make([]ipld.Node, 0)
		dgs.wg.Wait()
		return dgs.ingestBlock(ctx, n) // <-- retry ingest
	} else {
		fmt.Fprintf(os.Stdout, "Save in memory %s\n", time.Now().Format("15:04:05.000"))
		dgs.nodeparallel = append(dgs.nodeparallel, n)
		return nil
	}

}

// ingest the last n+k blocks
func (dgs *DAGService) ingestLastBlocks(ctx context.Context) error {
	shardd := dgs.currentShard
	//send nodes concurrently
	start := time.Now()
	if len(dgs.nodeparallel) == dgs.addParams.O+dgs.addParams.P {

		for _, node := range dgs.nodeparallel {
			sizee := uint64(len(node.RawData()))
			shardd[dgs.current].AddLink(ctx, node.Cid(), sizee)
			dgs.wg.Add(1)
			go func(shards []*shard, cur int, node ipld.Node) {
				defer dgs.wg.Done()
				shards[cur].sendBlock(ctx, node)
			}(dgs.currentShard, dgs.current, node)
			dgs.current = (dgs.current + 1) % (dgs.addParams.O + dgs.addParams.P)
		}
		dgs.nodeparallel = make([]ipld.Node, 0)
		dgs.wg.Wait()
		end := time.Now()
		fmt.Fprintf(os.Stdout, "SENDING PART %s \n", end.Sub(start).String())
		//fmt.Fprintf(os.Stdout, " AAAAAAADDDDDDDDEEEEEEEEDDDDDDDDDDDD %d \n", dgs.howmany)
	} else {
		//fmt.Fprintf(os.Stdout, " EEEEERRRRRRRRRRRRRRRR %d \n", dgs.howmany)
	}
	return nil
}

func (dgs *DAGService) logStats(metaPin, clusterDAGPin api.Cid) {
	duration := time.Since(dgs.startTime)
	seconds := uint64(duration) / uint64(time.Second)
	var rate string
	if seconds == 0 {
		rate = "∞ B"
	} else {
		rate = humanize.Bytes(dgs.totalSize / seconds)
	}

	statsFmt := `sharding session successful:
CID: %s
ClusterDAG: %s
Total shards: %d
Total size: %s
Total time: %s
Ingest Rate: %s/s
`

	logger.Infof(
		statsFmt,
		metaPin,
		clusterDAGPin,
		len(dgs.shards),
		humanize.Bytes(dgs.totalSize),
		duration,
		rate,
	)

}

func (dgs *DAGService) sendOutput(ao api.AddedOutput) {
	if dgs.output != nil {
		dgs.output <- ao
	}
}

// flushes the dgs.currentShard and returns the LastLink()
func (dgs *DAGService) flushCurrentShard(ctx context.Context) (cid.Cid, error) {

	shard := dgs.currentShard
	if shard[dgs.current] == nil {
		return cid.Undef, errors.New("cannot flush a nil shard")
	}

	lens := len(dgs.shards)

	//TODO: Flush the n+k shards root nodes together here instead of flushing them one by one and return some metadata to tell us how they must be linked in the consensus

	shardCid, _, err := shard[dgs.current].Flush(ctx, lens, dgs.previousShard)
	if err != nil {
		return shardCid, err
	}
	dgs.totalSize += shard[dgs.current].Size()
	dgs.shards[fmt.Sprintf("%d", lens)] = shardCid
	dgs.previousShard = shardCid
	dgs.sendOutput(api.AddedOutput{
		Name:        fmt.Sprintf("shard-%d", lens),
		Cid:         api.NewCid(shardCid),
		Size:        shard[dgs.current].Size(),
		Allocations: shard[dgs.current].Allocations(),
	})

	return shard[dgs.current].LastLink(), nil
}

// AddMany calls Add for every given node.
func (dgs *DAGService) AddMany(ctx context.Context, nodes []ipld.Node) error {
	for _, node := range nodes {
		err := dgs.Add(ctx, node)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dgs *DAGService) ingestBlockREP(ctx context.Context, n ipld.Node) error {
	shard := dgs.currentShardrep

	// if we have no currentShard, create one
	if shard == nil {
		logger.Infof("new shard for '%s': #%d", dgs.addParams.Name, len(dgs.shards))
		var err error
		// important: shards use the DAGService context.
		shard, err = NewShards(dgs.ctx, ctx, dgs.rpcClient, dgs.addParams.PinOptions)
		if err != nil {
			return err
		}
		dgs.currentShardrep = shard
	}

	logger.Debugf("ingesting block %s in shard %d (%s)", n.Cid(), len(dgs.shards), dgs.addParams.Name)

	// this is not same as n.Size()
	size := uint64(len(n.RawData()))

	// add the block to it if it fits and return
	if shard.Size()+size < shard.Limit() {
		shard.AddLink(ctx, n.Cid(), size)
		return dgs.currentShardrep.sendBlock(ctx, n)
	}

	logger.Debugf("shard %d full: block: %d. shard: %d. limit: %d",
		len(dgs.shards),
		size,
		shard.Size(),
		shard.Limit(),
	)

	// -------
	// Below: block DOES NOT fit in shard
	// Flush and retry

	// if shard is empty, error
	if shard.Size() == 0 {
		return errors.New("block doesn't fit in empty shard: shard size too small?")
	}
	_, err := dgs.flushCurrentShardRep(ctx)
	if err != nil {
		return err
	}
	return dgs.ingestBlockREP(ctx, n) // <-- retry ingest
}

func (dgs *DAGService) flushCurrentShardRep(ctx context.Context) (cid.Cid, error) {
	shard := dgs.currentShardrep
	if shard == nil {
		return cid.Undef, errors.New("cannot flush a nil shard")
	}

	lens := len(dgs.shardrep)

	shardCid, tt, err := shard.Flush(ctx, lens, dgs.previousShard)
	if err != nil {
		return shardCid, err
	}
	dgs.shardPINtime += tt
	dgs.totalSize += shard.Size()
	dgs.shardrep[fmt.Sprintf("%d", lens)] = shardCid
	dgs.previousShard = shardCid
	dgs.currentShardrep = nil
	dgs.sendOutput(api.AddedOutput{
		Name:        fmt.Sprintf("shard-%d", lens),
		Cid:         api.NewCid(shardCid),
		Size:        shard.Size(),
		Allocations: shard.Allocations(),
	})

	return shard.LastLink(), nil
}

// Finalize finishes sharding, creates the cluster DAG and pins it along
// with the meta pin for the root node of the content.
func (dgs *DAGService) FinalizeRep(ctx context.Context, dataRoot api.Cid) (api.Cid, error) {
	lastCid, err := dgs.flushCurrentShardRep(ctx)
	if err != nil {
		return api.NewCid(lastCid), err
	}

	if !lastCid.Equals(dataRoot.Cid) {
		logger.Warnf("the last added CID (%s) is not the IPFS data root (%s). This is only normal when adding a single file without wrapping in directory.", lastCid, dataRoot)
	}

	stt := time.Now()
	clusterDAGNodes, err := makeDAG(ctx, dgs.shardrep)
	if err != nil {
		return dataRoot, err
	}

	// PutDAG to ourselves
	blocks := make(chan api.NodeWithMeta, 256)
	go func() {
		defer close(blocks)
		for _, n := range clusterDAGNodes {
			select {
			case <-ctx.Done():
				logger.Error(ctx.Err())
				return //abort
			case blocks <- adder.IpldNodeToNodeWithMeta(n):
			}
		}
	}()

	// Stream these blocks and wait until we are done.
	bs := adder.NewBlockStreamer(ctx, dgs.rpcClient, []peer.ID{""}, blocks)
	select {
	case <-ctx.Done():
		return dataRoot, ctx.Err()
	case <-bs.Done():
	}

	if err := bs.Err(); err != nil {
		return dataRoot, err
	}

	clusterDAG := clusterDAGNodes[0].Cid()

	dgs.sendOutput(api.AddedOutput{
		Name:        fmt.Sprintf("%s-clusterDAG", dgs.addParams.Name),
		Cid:         api.NewCid(clusterDAG),
		Size:        dgs.totalSize,
		Allocations: nil,
	})

	// Pin the ClusterDAG
	clusterDAGPin := api.PinWithOpts(api.NewCid(clusterDAG), dgs.addParams.PinOptions)
	clusterDAGPin.ReplicationFactorMin = -1
	clusterDAGPin.ReplicationFactorMax = -1
	clusterDAGPin.MaxDepth = 0 // pin direct
	clusterDAGPin.Name = fmt.Sprintf("%s-clusterDAG", dgs.addParams.Name)
	clusterDAGPin.Type = api.ClusterDAGType
	clusterDAGPin.Reference = &dataRoot
	// Update object with response.
	err = adder.Pin(ctx, dgs.rpcClient, clusterDAGPin)
	if err != nil {
		return dataRoot, err
	}

	// Pin the META pin
	metaPin := api.PinWithOpts(dataRoot, dgs.addParams.PinOptions)
	metaPin.Type = api.MetaType
	ref := api.NewCid(clusterDAG)
	metaPin.Reference = &ref
	metaPin.MaxDepth = 0 // irrelevant. Meta-pins are not pinned
	err = adder.Pin(ctx, dgs.rpcClient, metaPin)
	if err != nil {
		return dataRoot, err
	}

	// Log some stats
	dgs.logStats(metaPin.Cid, clusterDAGPin.Cid)
	fmt.Fprintf(os.Stdout, "PINNING shards took : %s\n", dgs.shardPINtime.String())
	fmt.Fprintf(os.Stdout, "SENDING Cluster dag nodes and INTERNAL NODES and pinning them took : %s \n", time.Since(stt).String())
	fmt.Fprintf(os.Stdout, "Overall ADD took %s \n", time.Since(dgs.startTime).String())
	// Consider doing this? Seems like overkill
	//
	// // Amend ShardPins to reference clusterDAG root hash as a Parent
	// shardParents := cid.NewSet()
	// shardParents.Add(clusterDAG)
	// for shardN, shard := range dgs.shardNodes {
	//      pin := api.PinWithOpts(shard, dgs.addParams)
	//      pin.Name := fmt.Sprintf("%s-shard-%s", pin.Name, shardN)
	//      pin.Type = api.ShardType
	//      pin.Parents = shardParents
	//      // FIXME: We don't know anymore the shard pin maxDepth
	//      // so we'd need to get the pin first.
	//      err := dgs.pin(pin)
	//      if err != nil {
	//              return err
	//      }
	// }

	return dataRoot, nil
}

// flushes the dgs.currentShard and returns the LastLink()
func (dgs *DAGService) flushCurrentShards(ctx context.Context) (cid.Cid, error) {
	fmt.Fprintf(os.Stdout, "Creating Shard DAG and sending roots %s\n", time.Now().Format("15:04:05.000"))
	st := time.Now()
	var LastLink cid.Cid
	sharedCbor := make([]cid.Cid, dgs.original+dgs.parity)
	lennodes := make([]int, dgs.original+dgs.parity)
	shardd := dgs.currentShard
	var mu sync.Mutex
	if shardd[dgs.current] == nil {
		return cid.Undef, errors.New("cannot flush a nil shard")
	}

	lens := len(dgs.shards)
	fmt.Fprintf(os.Stdout, "lensss %d \n", lens)
	for shardN := lens + 1; shardN <= lens+(dgs.original+dgs.parity); shardN++ {
		dgs.wg.Add(1)
		go func(shardN int) {
			defer dgs.wg.Done()
			shardCid, lennode, nodes, err := shardd[(shardN-1)%(dgs.original+dgs.parity)].FlushNew(ctx)
			if err != nil {
				return
			}
			mu.Lock()
			black := make([]peer.ID, 0)
			black = append(black, shardd[(shardN-1)%(dgs.original+dgs.parity)].allocations...)
			ee := every{black: black, nodes: nodes}
			dgs.topin = append(dgs.topin, ee)
			sharedCbor[(shardN-1)%(dgs.original+dgs.parity)] = shardCid
			lennodes[(shardN-1)%(dgs.original+dgs.parity)] = lennode
			mu.Unlock()
		}(shardN)
	}
	dgs.wg.Wait()
	en := time.Now()
	fmt.Fprintf(os.Stdout, "This set of shards sending overhead took : %s\n", en.Sub(st).String())
	fmt.Fprintf(os.Stdout, "Pinning requests %s\n", time.Now().Format("15:04:05.000"))
	for shardN := lens + 1; shardN <= lens+(dgs.original+dgs.parity); shardN++ {
		rootCid := sharedCbor[(shardN-1)%(dgs.original+dgs.parity)]
		pin := api.PinWithOpts(api.NewCid(rootCid), shardd[(shardN-1)%(dgs.original+dgs.parity)].pinOptions)
		pin.Name = fmt.Sprintf("%s-shard-EC(%d,%d)-%d", shardd[(shardN-1)%(dgs.original+dgs.parity)].pinOptions.Name, dgs.original, dgs.parity, shardN)
		// this sets allocations as priority allocation
		pin.Allocations = shardd[(shardN-1)%(dgs.original+dgs.parity)].allocations

		pin.Type = api.ShardType
		ref := api.NewCid(dgs.previousShard)
		pin.Reference = &ref
		pin.MaxDepth = 1
		pin.ShardSize = shardd[(shardN-1)%(dgs.original+dgs.parity)].Size()                                               // use current size, not the limit
		if lennodes[(shardN-1)%(dgs.original+dgs.parity)] > len(shardd[(shardN-1)%(dgs.original+dgs.parity)].dagNode)+1 { // using an indirect graph
			pin.MaxDepth = 2
		}

		logger.Infof("shard #%d (%s) completed. Total size: %s. Links: %d",
			shardN,
			rootCid,
			humanize.Bytes(shardd[(shardN-1)%(dgs.original+dgs.parity)].Size()),
			len(shardd[(shardN-1)%(dgs.original+dgs.parity)].dagNode),
		)

		adder.Pin(ctx, shardd[(shardN-1)%(dgs.original+dgs.parity)].rpc, pin)
		//TODO: Flush the n+k shards root nodes together here instead of flushing them one by one and return some metadata to tell us how they must be linked in the consensus
		dgs.totalSize += shardd[(shardN-1)%(dgs.original+dgs.parity)].Size()
		dgs.shards[fmt.Sprintf("%d", shardN-1)] = rootCid
		dgs.previousShard = rootCid
		dgs.sendOutput(api.AddedOutput{
			Name:        fmt.Sprintf("shard-%d", shardN-1),
			Cid:         api.NewCid(rootCid),
			Size:        shardd[(shardN-1)%(dgs.original+dgs.parity)].Size(),
			Allocations: shardd[(shardN-1)%(dgs.original+dgs.parity)].Allocations(),
		})

		LastLink = shardd[(shardN-1)%(dgs.original+dgs.parity)].LastLink()
	}
	dgs.currentShard = make([]*shard, dgs.original+dgs.parity)
	enn := time.Now()
	dgs.shardPINtime += enn.Sub(en)
	fmt.Fprintf(os.Stdout, "This set of shards pinning took : %s\n", enn.Sub(en).String())
	return LastLink, nil
}

type every struct {
	black []peer.ID
	nodes []ipld.Node
}
