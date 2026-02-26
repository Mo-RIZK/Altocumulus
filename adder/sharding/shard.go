package sharding

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/adder"
	"github.com/ipfs-cluster/ipfs-cluster/api"
	ipld "github.com/ipfs/go-ipld-format"

	cid "github.com/ipfs/go-cid"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p/core/peer"

	humanize "github.com/dustin/go-humanize"
)

// a shard represents a set of blocks (or bucket) which have been assigned
// a peer to be block-put and will be part of the same shard in the
// cluster DAG.
type shard struct {
	ctx             context.Context
	rpc             *rpc.Client
	allocations     []peer.ID
	pinOptions      api.PinOptions
	bs              *adder.BlockStreamer
	blocks          chan api.NodeWithMeta
	closeBlocksOnce sync.Once
	// dagNode represents a node with links and will be converted
	// to Cbor.
	dagNode     map[string]cid.Cid
	currentSize uint64
	sizeLimit   uint64
	blocksCIDs  []cid.Cid
}

func NewShard(globalCtx context.Context, ctx context.Context, rpc *rpc.Client, opts api.PinOptions, alloc peer.ID) (*shard, error) {

	allocs := []peer.ID{alloc}
	if opts.ReplicationFactorMin > 0 && len(allocs) == 0 {
		// This would mean that the empty cid is part of the shared state somehow.
		panic("allocations for new shard cannot be empty without error")
	}

	if opts.ReplicationFactorMin < 0 {
		logger.Warn("Shard is set to replicate everywhere ,which doesn't make sense for sharding")
	}

	// TODO (hector): get latest metrics for allocations, adjust sizeLimit
	// to minimum. This can be done later.

	blocks := make(chan api.NodeWithMeta, 256)

	return &shard{
		ctx:         globalCtx,
		rpc:         rpc,
		allocations: allocs,
		pinOptions:  opts,
		bs:          adder.NewBlockStreamer(globalCtx, rpc, allocs, blocks),
		blocks:      blocks,
		dagNode:     make(map[string]cid.Cid),
		currentSize: 0,
		sizeLimit:   opts.ShardSize,
		blocksCIDs:  make([]cid.Cid, 0),
	}, nil
}

func NewShards(globalCtx context.Context, ctx context.Context, rpc *rpc.Client, opts api.PinOptions) (*shard, error) {
	allocs, err := adder.BlockAllocate(ctx, rpc, opts)
	if err != nil {
		return nil, err
	}

	if opts.ReplicationFactorMin > 0 && len(allocs) == 0 {
		// This would mean that the empty cid is part of the shared state somehow.
		panic("allocations for new shard cannot be empty without error")
	}

	if opts.ReplicationFactorMin < 0 {
		logger.Warn("Shard is set to replicate everywhere ,which doesn't make sense for sharding")
	}

	// TODO (hector): get latest metrics for allocations, adjust sizeLimit
	// to minimum. This can be done later.

	blocks := make(chan api.NodeWithMeta, 256)

	return &shard{
		ctx:         globalCtx,
		rpc:         rpc,
		allocations: allocs,
		pinOptions:  opts,
		bs:          adder.NewBlockStreamer(globalCtx, rpc, allocs, blocks),
		blocks:      blocks,
		dagNode:     make(map[string]cid.Cid),
		currentSize: 0,
		sizeLimit:   opts.ShardSize,
	}, nil
}

func NewShardsBlack(globalCtx context.Context, ctx context.Context, rpc *rpc.Client, black []peer.ID, opts api.PinOptions) (*shard, error) {
	allocs, err := adder.BlockAllocateWithBlack(ctx, rpc, black, opts)
	if err != nil {
		return nil, err
	}

	if opts.ReplicationFactorMin > 0 && len(allocs) == 0 {
		// This would mean that the empty cid is part of the shared state somehow.
		panic("allocations for new shard cannot be empty without error")
	}

	if opts.ReplicationFactorMin < 0 {
		logger.Warn("Shard is set to replicate everywhere ,which doesn't make sense for sharding")
	}

	// TODO (hector): get latest metrics for allocations, adjust sizeLimit
	// to minimum. This can be done later.

	blocks := make(chan api.NodeWithMeta, 256)

	return &shard{
		ctx:         globalCtx,
		rpc:         rpc,
		allocations: allocs,
		pinOptions:  opts,
		bs:          adder.NewBlockStreamer(globalCtx, rpc, allocs, blocks),
		blocks:      blocks,
		dagNode:     make(map[string]cid.Cid),
		currentSize: 0,
		sizeLimit:   opts.ShardSize,
	}, nil
}

// AddLink tries to add a new block to this shard if it's not full.
// Returns true if the block was added
func (sh *shard) AddLink(ctx context.Context, c cid.Cid, s uint64) {
	linkN := len(sh.dagNode)
	linkName := fmt.Sprintf("%d", linkN)
	logger.Debugf("shard: add link: %s", linkName)

	sh.dagNode[linkName] = c
	sh.currentSize += s
}

// Allocations returns the peer IDs on which blocks are put for this shard.
func (sh *shard) Allocations() []peer.ID {
	if len(sh.allocations) == 1 && sh.allocations[0] == "" {
		return nil
	}
	return sh.allocations
}

func (sh *shard) sendBlock(ctx context.Context, n ipld.Node) error {
	sh.blocksCIDs = append(sh.blocksCIDs, n.Cid())
	select {
	case <-ctx.Done():
		return ctx.Err()
	case sh.blocks <- adder.IpldNodeToNodeWithMeta(n):
		return nil
	}
}
func (sh *shard) SendBlock(ctx context.Context, n ipld.Node) error {
	sh.blocksCIDs = append(sh.blocksCIDs, n.Cid())
	select {
	case <-ctx.Done():
		return ctx.Err()
	case sh.blocks <- adder.IpldNodeToNodeWithMeta(n):
		return nil
	}
}

// Close stops any ongoing block streaming.
func (sh *shard) Close() error {
	sh.closeBlocksOnce.Do(func() {
		close(sh.blocks)
	})
	return nil
}

// Flush completes the allocation of this shard by building a CBOR node
// and adding it to IPFS, then pinning it in cluster. It returns the Cid of the
// shard.
func (sh *shard) Flush(ctx context.Context, shardN int, prev cid.Cid) (cid.Cid, time.Duration, error) {
	logger.Debugf("shard %d: flush", shardN)

	start := time.Now()
	nodes, err := makeDAG(ctx, sh.dagNode)
	end := time.Now()
	fmt.Fprintf(os.Stdout, "SHARD DAG CREATION %s \n", end.Sub(start).String())
	if err != nil {
		return cid.Undef, 0, err
	}
	//startt := time.Now()
	//fmt.Fprintf(os.Stdout, "SEND SHARD DAG %d", len(nodes))
	for _, n := range nodes {
		fmt.Fprintf(os.Stdout, "SHARD NODE SIZE IS: %d \n", uint64(len(n.RawData())))
		err = sh.sendBlock(ctx, n)
		if err != nil {
			close(sh.blocks)
			return cid.Undef, 0, err
		}
	}
	sh.Close()

	select {
	case <-ctx.Done():
		return cid.Undef, 0, ctx.Err()
	case <-sh.bs.Done():
	}
	//endd := time.Now()
	//fmt.Fprintf(os.Stdout, "SEND SHARD DAG %s \n", endd.Sub(startt).String())

	if err := sh.bs.Err(); err != nil {
		return cid.Undef, 0, err
	}
	st := time.Now()
	rootCid := nodes[0].Cid()
	pin := api.PinWithOpts(api.NewCid(rootCid), sh.pinOptions)
	pin.Name = fmt.Sprintf("%s-shard-%d", sh.pinOptions.Name, shardN)
	// this sets allocations as priority allocation
	pin.Allocations = sh.allocations
	pin.Type = api.ShardType
	ref := api.NewCid(prev)
	pin.Reference = &ref
	pin.MaxDepth = 1
	pin.ShardSize = sh.Size()           // use current size, not the limit
	if len(nodes) > len(sh.dagNode)+1 { // using an indirect graph
		pin.MaxDepth = 2
	}
	en := time.Since(st)
	logger.Infof("shard #%d (%s) completed. Total size: %s. Links: %d",
		shardN,
		rootCid,
		humanize.Bytes(sh.Size()),
		len(sh.dagNode),
	)

	return rootCid, en, adder.Pin(ctx, sh.rpc, pin)
}

// Size returns this shard's current size.
func (sh *shard) Size() uint64 {
	return sh.currentSize
}

// Size returns this shard's size limit.
func (sh *shard) Limit() uint64 {
	return sh.sizeLimit
}

// Last returns the last added link. When finishing sharding,
// the last link of the last shard is the data root for the
// full sharded DAG (the CID that would have resulted from
// adding the content to a single IPFS daemon).
func (sh *shard) LastLink() cid.Cid {
	l := len(sh.dagNode)
	lastLink := fmt.Sprintf("%d", l-1)
	return sh.dagNode[lastLink]
}

func (sh *shard) FlushForStateless(ctx context.Context, pin api.Pin) error {

	start := time.Now()
	nodes, err := makeDAG(ctx, sh.dagNode)
	end := time.Now()
	fmt.Fprintf(os.Stdout, "SHARD DAG CREATION %s \n", end.Sub(start).String())
	if err != nil {
		return err
	}
	//startt := time.Now()
	//fmt.Fprintf(os.Stdout, "SEND SHARD DAG %d", len(nodes))
	for _, n := range nodes {
		fmt.Fprintf(os.Stdout, "SHARD NODE SIZE IS: %d \n", uint64(len(n.RawData())))
		err = sh.sendBlock(ctx, n)
		if err != nil {
			close(sh.blocks)
			return err
		}
	}
	sh.Close()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sh.bs.Done():
	}
	//endd := time.Now()
	//fmt.Fprintf(os.Stdout, "SEND SHARD DAG %s \n", endd.Sub(startt).String())

	if err := sh.bs.Err(); err != nil {
		return err
	}

	return adder.Pin(ctx, sh.rpc, pin)
}

func (sh *shard) FlushNew(ctx context.Context) (cid.Cid, int, []ipld.Node, error) {
	nodes, err := makeDAG(ctx, sh.dagNode)
	if err != nil {
		return cid.Undef, 0, nil, err
	}
	for _, n := range nodes {
		err = sh.sendBlock(ctx, n)
		if err != nil {
			close(sh.blocks)
			return cid.Undef, 0, nil, err
		}
	}
	sh.Close()

	select {
	case <-ctx.Done():
		return cid.Undef, 0, nil, ctx.Err()
	case <-sh.bs.Done():
	}
	if err := sh.bs.Err(); err != nil {
		return cid.Undef, 0, nil, err
	}

	return nodes[0].Cid(), len(nodes), nodes, nil
}
