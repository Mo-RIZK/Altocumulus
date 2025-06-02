// Package stateless implements a PinTracker component for IPFS Cluster, which
// aims to reduce the memory footprint when handling really large cluster
// states.
package stateless

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ipfs-cluster/ipfs-cluster/adder/ipfsadd"
	"github.com/ipfs-cluster/ipfs-cluster/adder/sharding"
	"github.com/ipfs-cluster/ipfs-cluster/ipfsconn/ipfshttp"
	"github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/go-cid"
	"github.com/klauspost/reedsolomon"
	"github.com/multiformats/go-multihash"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs-cluster/ipfs-cluster/pintracker/optracker"
	"github.com/ipfs-cluster/ipfs-cluster/state"

	logging "github.com/ipfs/go-log/v2"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p/core/peer"

	"go.opencensus.io/trace"
)

var logger = logging.Logger("pintracker")

const pinsChannelSize = 1024

var (
	// ErrFullQueue is the error used when pin or unpin operation channel is full.
	ErrFullQueue = errors.New("pin/unpin operation queue is full. Try increasing max_pin_queue_size")

	// items with this error should be recovered
	errUnexpectedlyUnpinned = errors.New("the item should be pinned but it is not")
)

// Tracker uses the optracker.OperationTracker to manage
// transitioning shared ipfs-cluster state (Pins) to the local IPFS node.
type Tracker struct {
	config *Config

	optracker *optracker.OperationTracker

	peerID   peer.ID
	peerName string

	ctx    context.Context
	cancel func()

	getState func(ctx context.Context) (state.ReadOnly, error)

	rpcClient *rpc.Client
	rpcReady  chan struct{}

	priorityPinCh chan *optracker.Operation
	pinCh         chan *optracker.Operation
	unpinCh       chan *optracker.Operation

	shutdownMu sync.Mutex
	shutdown   bool
	wg         sync.WaitGroup
	connector  *ipfshttp.Connector
}

// New creates a new StatelessPinTracker.
func New(cfg *Config, pid peer.ID, peerName string, getState func(ctx context.Context) (state.ReadOnly, error), connector *ipfshttp.Connector) *Tracker {
	ctx, cancel := context.WithCancel(context.Background())

	spt := &Tracker{
		config:        cfg,
		peerID:        pid,
		peerName:      peerName,
		ctx:           ctx,
		cancel:        cancel,
		getState:      getState,
		optracker:     optracker.NewOperationTracker(ctx, pid, peerName),
		rpcReady:      make(chan struct{}, 1),
		priorityPinCh: make(chan *optracker.Operation, cfg.MaxPinQueueSize),
		pinCh:         make(chan *optracker.Operation, cfg.MaxPinQueueSize),
		unpinCh:       make(chan *optracker.Operation, cfg.MaxPinQueueSize),
		connector:     connector,
	}

	for i := 0; i < spt.config.ConcurrentPins; i++ {
		go spt.opWorker(spt.pin, spt.priorityPinCh, spt.pinCh)
	}
	go spt.opWorker(spt.unpin, spt.unpinCh, nil)

	return spt
}

// we can get our IPFS id from our own monitor ping metrics which
// are refreshed regularly.
func (spt *Tracker) getIPFSID(ctx context.Context) api.IPFSID {
	// Wait until RPC is ready
	<-spt.rpcReady

	var ipfsid api.IPFSID
	err := spt.rpcClient.CallContext(
		ctx,
		"",
		"Cluster",
		"IPFSID",
		peer.ID(""), // local peer
		&ipfsid,
	)
	if err != nil {
		logger.Error(err)
	}
	return ipfsid
}

// receives a pin Function (pin or unpin) and channels.  Used for both pinning
// and unpinning.
func (spt *Tracker) opWorker(pinF func(*optracker.Operation) error, prioCh, normalCh chan *optracker.Operation) {

	var op *optracker.Operation

	for {
		// Process the priority channel first.
		select {
		case op = <-prioCh:
			goto APPLY_OP
		case <-spt.ctx.Done():
			return
		default:
		}

		// Then process things on the other channels.
		// Block if there are no things to process.
		select {
		case op = <-prioCh:
			goto APPLY_OP
		case op = <-normalCh:
			goto APPLY_OP
		case <-spt.ctx.Done():
			return
		}

		// apply operations that came from some channel
	APPLY_OP:
		if clean := applyPinF(pinF, op); clean {
			spt.optracker.Clean(op.Context(), op)
		}
	}
}

// applyPinF returns true if the operation can be considered "DONE".
func applyPinF(pinF func(*optracker.Operation) error, op *optracker.Operation) bool {
	if op.Canceled() {
		// operation was canceled. Move on.
		// This saves some time, but not 100% needed.
		return false
	}
	op.SetPhase(optracker.PhaseInProgress)
	op.IncAttempt()
	err := pinF(op) // call pin/unpin
	if err != nil {
		if op.Canceled() {
			// there was an error because
			// we were canceled. Move on.
			return false
		}
		op.SetError(err)
		op.Cancel()
		return false
	}
	op.SetPhase(optracker.PhaseDone)
	op.Cancel()
	return true // this tells the opWorker to clean the operation from the tracker.
}

func (spt *Tracker) pin(op *optracker.Operation) error {
	if strings.Contains(op.Pin().Name, "Repair") {
		fmt.Fprintf(os.Stdout, "Date start inside the pintracker repair %s : %s \n", op.Pin().Name, time.Now().Format("2006-01-02 15:04:05.000"))
		download, repair, waittosend := spt.repinUsingRS(op)
		fmt.Fprintf(os.Stdout, "Time Taken to download chunks is : %s and to repair chunks is : %s and additional time to wait to complete sending the shard : %s \n", download.String(), repair.String(), waittosend.String())
		fmt.Fprintf(os.Stdout, "Date end inside the pintracker repair %s : %s \n", op.Pin().Name, time.Now().Format("2006-01-02 15:04:05.000"))
		return nil
	} else {
		ctx, span := trace.StartSpan(op.Context(), "tracker/stateless/pin")
		defer span.End()

		logger.Debugf("is uing pin call for %s", op.Cid())
		err := spt.rpcClient.CallContext(
			ctx,
			"",
			"IPFSConnector",
			"Pin",
			op.Pin(),
			&struct{}{},
		)
		if err != nil {
			return err
		}
		return nil
	}
}

func (spt *Tracker) repinUsingRS(op *optracker.Operation) (time.Duration, time.Duration, time.Duration) {
	repairShards := make([]pinwithmeta, 0)
	//start := time.Now()
	var timedownloadchunks, timetorepairchunksonly time.Duration
	ctx, span := trace.StartSpan(spt.ctx, "pintracker/repinFromPeer")
	defer span.End()
	pin := op.Pin()
	p := pin.Allocations
	f1 := strings.Split(pin.Name, "(")[1]
	f2 := strings.Split(f1, ")")[0]
	or, _ := strconv.Atoi(strings.Split(f2, ",")[0])
	par, _ := strconv.Atoi(strings.Split(f2, ",")[1])
	logger.Debugf("repinning %s from peer %s", pin.Cid, p)
	//blacklist := make([]peer.ID, 0)
	//blacklist = append(blacklist, pin.Allocations...)-
	prefix, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		return 0, 0, 0
	}

	hashFunCode, _ := multihash.Names[strings.ToLower("sha2-256")]
	prefix.MhType = hashFunCode
	prefix.MhLength = -1
	//here we want to recreate the missing shard
	cState, err := spt.getState(spt.ctx)
	if err != nil {
		logger.Warn(err)
		return 0, 0, 0
	}
	pinCh := make(chan api.Pin, 1024)
	go func() {
		err = cState.List(spt.ctx, pinCh)
		if err != nil {
			logger.Warn(err)
		}
	}()
	fmt.Fprintf(os.Stdout, "getShardNumber of pin named : %s", pin.Name)
	numpin, name, err := getShardNumber(pin.Name)
	if err != nil {
		fmt.Println("Error:", err)
		return 0, 0, 0
	}
	tosend := (numpin - 1) % (or + par)
	fmt.Printf("number of the shard to repair is : %d \n", numpin)
	mod := numpin % (or + par)
	before := (numpin - 1) % (or + par)
	after := (or + par - mod) % (or + par)
	//fmt.Printf("taking shards between %d and %d \n", numpin-before, numpin+after)
	for pinn := range pinCh {
		if strings.Contains(pinn.Name, "-shard-") {
			pinnShardNum, namee, err := getShardNumber(pinn.Name)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			//fmt.Printf("Current shard number : %d\n", pinnShardNum)
			if pinnShardNum >= numpin-before && pinnShardNum <= numpin+after && pinnShardNum != numpin && name == namee {
				// This shard is within the range, proceed with retrieval logic
				//fmt.Printf("Retrieving shard %d: %s with index: %d \n", pinnShardNum, pinn.Name, pinnShardNum%(c.or+c.par))
				pinnn := pinwithmeta{pin: pinn, index: pinnShardNum, cids: make([]string, 0)}
				repairShards = append(repairShards, pinnn)
			}
		}
	}
	// Sort repairShard by Index in ascending order
	sortRepairShardsByIndex(repairShards)

	wgg := new(sync.WaitGroup)
	wgg.Add(or)
	muu := new(sync.Mutex)
	ret := 0
	fmt.Printf("STEEEEEEEEEPPPPPPPPPP RRRRRRRRRREEEEEEETTTTTTTTT with length of repair shards is : %d \n", len(repairShards))
	for i, pinwm := range repairShards {
		go func(pinwm pinwithmeta, i int) {
			cidss := spt.RetrieveCids(pinwm)
			muu.Lock()
			if ret < or {
				ret++
				muu.Unlock()
				for j, _ := range cidss {
					repairShards[i].cids = append(repairShards[i].cids, cidss[j].cid)
				}
				wgg.Done()
			} else {
				muu.Unlock()
			}

		}(pinwm, i)
	}
	wgg.Wait()
	shh, _ := sharding.NewShard(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions,spt.peerID)
	//shh, _ := sharding.NewShards(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions)
	enc, _ := reedsolomon.New(or, par)
	k := 0
	for {
		if len(repairShards[k].cids) == 0 {

			k++
		} else {
			break
		}
	}
	times := len(repairShards[k].cids)
	//open gourotines to retrieve data in parallel
	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)
	for i := 0; i < times; i++ {
		retrieved := 0
		sttt := time.Now()
		reconstructshards := make([][]byte, or+par)
		wg.Add(or)
		ctxx, cancel := context.WithCancel(context.Background())
		for _, shard := range repairShards {
			if len(shard.cids) > 0 {
				go func(i int, shard pinwithmeta) {
					bytess := spt.getData(ctxx, shard.cids[i])
					fmt.Printf("REPAIR GOT HERE \n")
					mu.Lock()
					if retrieved < or {
						retrieved++
						reconstructshards[(shard.index-1)%(or+par)] = bytess
						mu.Unlock()
						wg.Done()
					} else {
						cancel()
						mu.Unlock()
					}

				}(i, shard)
			}
		}
		wg.Wait()
		// Find where to allocate this file
		stt := time.Now()
		timedownloadchunks += stt.Sub(sttt)
		enc.Reconstruct(reconstructshards)
		enn := time.Since(stt)
		timetorepairchunksonly += enn
		//rawnode, _ := merkledag.NewRawNodeWPrefix(reconstructshards[tosend], prefix)
		nodee := ipfsadd.NewFSNodeOverDagC(ft.TFile, prefix)
		nodee.SetFileData(reconstructshards[tosend])
		rawnode, _ := nodee.Commit()
		//zid l blacklist heyye list li other pins kamen fiha
		shh.SendBlock(spt.ctx, rawnode)
		size := uint64(len(rawnode.RawData()))
		shh.AddLink(ctx, rawnode.Cid(), size)
	}
	wait1 := time.Now()
	shh.FlushNew(spt.ctx)
	wait2 := time.Since(wait1)
	pin.Name = strings.Split(pin.Name, "Rep")[0]

	errr := spt.rpcClient.CallContext(
		ctx,
		"",
		"IPFSConnector",
		"Pin",
		pin,
		&struct{}{},
	)
	if errr != nil {
		return 0, 0, 0
	}
	return timedownloadchunks, timetorepairchunksonly, wait2
}

func (spt *Tracker) getData(ctx context.Context, Cid string) []byte {
	CidNew, _ := cid.Decode(Cid)
	nnn, _ := spt.connector.ChunkGet(ctx, CidNew)
	return nnn
}

func (spt *Tracker) RetrieveCids(pinwm pinwithmeta) []Chunk {
	s, _ := spt.connector.NodeGet(spt.ctx, pinwm.pin.Cid.Cid)
	cids := doTheProcess(s)
	return cids
}

func doTheProcess(nn string) []Chunk {
	parsedData, _ := ConvertStringToJSON(nn)
	cidss := make([]Chunk, 0)
	for key, value := range parsedData {
		// if the get node format do not contain data then we will be passing through the nodes inside each shard

		ke, _ := strconv.Atoi(key)
		// Print the CID value from the nested map
		if Cid, exists := value["/"]; exists {
			fmt.Printf("%s\n", Cid)
			ch := Chunk{index: ke, cid: Cid}
			cidss = append(cidss, ch)
			sort.Slice(cidss, func(i, j int) bool {
				return cidss[i].index < cidss[j].index
			})

			//GetBytesFromData(nnn)

		}
	}
	return cidss
}

// ConvertStringToJSON parses the input string and converts it to JSON.
func ConvertStringToJSON(input string) (Data, error) {
	// Define a variable to hold the parsed JSON data
	var result Data

	// Parse the input string into JSON format
	err := json.Unmarshal([]byte(input), &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type Data map[string]map[string]string
type DataChunk struct {
	Dataa struct {
		Slash struct {
			Bytes string `json:"bytes"`
		} `json:"/"`
	} `json:"Data"`
}

func (spt *Tracker) unpin(op *optracker.Operation) error {
	ctx, span := trace.StartSpan(op.Context(), "tracker/stateless/unpin")
	defer span.End()

	logger.Debugf("issuing unpin call for %s", op.Cid())
	err := spt.rpcClient.CallContext(
		ctx,
		"",
		"IPFSConnector",
		"Unpin",
		op.Pin(),
		&struct{}{},
	)
	if err != nil {
		return err
	}
	return nil
}

// Enqueue puts a new operation on the queue, unless ongoing exists.
func (spt *Tracker) enqueue(ctx context.Context, c api.Pin, typ optracker.OperationType) error {
	ctx, span := trace.StartSpan(ctx, "tracker/stateless/enqueue")
	defer span.End()

	logger.Debugf("entering enqueue: pin: %+v", c)
	op := spt.optracker.TrackNewOperation(ctx, c, typ, optracker.PhaseQueued)
	if op == nil {
		return nil // the operation exists and must be queued already.
	}

	var ch chan *optracker.Operation

	switch typ {
	case optracker.OperationPin:
		isPriorityPin := time.Now().Before(c.Timestamp.Add(spt.config.PriorityPinMaxAge)) &&
			op.AttemptCount() <= spt.config.PriorityPinMaxRetries
		op.SetPriorityPin(isPriorityPin)

		if isPriorityPin {
			ch = spt.priorityPinCh
		} else {
			ch = spt.pinCh
		}
	case optracker.OperationUnpin:
		ch = spt.unpinCh
	}

	select {
	case ch <- op:
	default:
		err := ErrFullQueue
		op.SetError(err)
		op.Cancel()
		logger.Error(err.Error())
		return err
	}
	return nil
}

// SetClient makes the StatelessPinTracker ready to perform RPC requests to
// other components.
func (spt *Tracker) SetClient(c *rpc.Client) {
	spt.rpcClient = c
	close(spt.rpcReady)
}

// Shutdown finishes the services provided by the StatelessPinTracker
// and cancels any active context.
func (spt *Tracker) Shutdown(ctx context.Context) error {
	ctx, span := trace.StartSpan(ctx, "tracker/stateless/Shutdown")
	_ = ctx
	defer span.End()

	spt.shutdownMu.Lock()
	defer spt.shutdownMu.Unlock()

	if spt.shutdown {
		logger.Debug("already shutdown")
		return nil
	}

	logger.Info("stopping StatelessPinTracker")
	spt.cancel()
	spt.wg.Wait()
	spt.shutdown = true
	return nil
}

// Track tells the StatelessPinTracker to start managing a Cid,
// possibly triggering Pin operations on the IPFS daemon.
func (spt *Tracker) Track(ctx context.Context, c api.Pin) error {
	ctx, span := trace.StartSpan(ctx, "tracker/stateless/Track")
	defer span.End()

	logger.Debugf("tracking %s", c.Cid)

	// Sharded pins are never pinned. A sharded pin cannot turn into
	// something else or viceversa like it happens with Remote pins so
	// we just ignore them.
	if c.Type == api.MetaType {
		return nil
	}

	// Trigger unpin whenever something remote is tracked
	// Note, IPFSConn checks with pin/ls before triggering
	// pin/rm.
	if c.IsRemotePin(spt.peerID) {
		op := spt.optracker.TrackNewOperation(ctx, c, optracker.OperationRemote, optracker.PhaseInProgress)
		if op == nil {
			return nil // ongoing unpin
		}
		err := spt.unpin(op)
		op.Cancel()
		if err != nil {
			op.SetError(err)
			return nil
		}

		op.SetPhase(optracker.PhaseDone)
		spt.optracker.Clean(ctx, op)
		return nil
	}

	return spt.enqueue(ctx, c, optracker.OperationPin)
}

// Untrack tells the StatelessPinTracker to stop managing a Cid.
// If the Cid is pinned locally, it will be unpinned.
func (spt *Tracker) Untrack(ctx context.Context, c api.Cid) error {
	ctx, span := trace.StartSpan(ctx, "tracker/stateless/Untrack")
	defer span.End()

	logger.Debugf("untracking %s", c)
	return spt.enqueue(ctx, api.PinCid(c), optracker.OperationUnpin)
}

// StatusAll returns information for all Cids pinned to the local IPFS node.
func (spt *Tracker) StatusAll(ctx context.Context, filter api.TrackerStatus, out chan<- api.PinInfo) error {
	ctx, span := trace.StartSpan(ctx, "tracker/stateless/StatusAll")
	defer span.End()

	ipfsid := spt.getIPFSID(ctx)

	// Any other states are just operation-tracker states, so we just give
	// those and return.
	if !filter.Match(
		api.TrackerStatusPinned | api.TrackerStatusUnexpectedlyUnpinned |
			api.TrackerStatusSharded | api.TrackerStatusRemote) {
		return spt.optracker.GetAllChannel(ctx, filter, ipfsid, out)
	}

	defer close(out)

	// get global state - cluster pinset
	st, err := spt.getState(ctx)
	if err != nil {
		logger.Error(err)
		return err
	}

	var ipfsRecursivePins map[api.Cid]api.IPFSPinStatus
	// Only query IPFS if we want to status for pinned items
	if filter.Match(api.TrackerStatusPinned | api.TrackerStatusUnexpectedlyUnpinned) {
		ipfsRecursivePins = make(map[api.Cid]api.IPFSPinStatus)
		// At some point we need a full map of what we have and what
		// we don't. The IPFS pinset is the smallest thing we can keep
		// on memory.
		ipfsPinsCh, errCh := spt.ipfsPins(ctx)
		for ipfsPinInfo := range ipfsPinsCh {
			ipfsRecursivePins[ipfsPinInfo.Cid] = ipfsPinInfo.Type
		}
		// If there was an error listing recursive pins then abort.
		err := <-errCh
		if err != nil {
			err := fmt.Errorf("could not get pinset from IPFS: %w", err)
			logger.Error(err)
			return err
		}
	}

	// Prepare pinset streaming
	statePins := make(chan api.Pin, pinsChannelSize)
	go func() {
		err = st.List(ctx, statePins)
		if err != nil {
			logger.Error(err)
		}
	}()

	// a shorthand for this select.
	trySend := func(info api.PinInfo) bool {
		select {
		case <-ctx.Done():
			return false
		case <-spt.ctx.Done():
			return false
		case out <- info:
			return true
		}
	}

	// For every item in the state.
	for p := range statePins {
		select {
		case <-ctx.Done():
		case <-spt.ctx.Done():
		default:
		}

		// if there is an operation, issue that and move on
		info, ok := spt.optracker.GetExists(ctx, p.Cid, ipfsid)
		if ok && filter.Match(info.Status) {
			if !trySend(info) {
				return fmt.Errorf("error issuing PinInfo: %w", ctx.Err())
			}
			continue // next pin
		}

		// Preliminary PinInfo for this Pin.
		info = api.PinInfo{
			Cid:         p.Cid,
			Name:        p.Name,
			Peer:        spt.peerID,
			Allocations: p.Allocations,
			Origins:     p.Origins,
			Created:     p.Timestamp,
			Metadata:    p.Metadata,

			PinInfoShort: api.PinInfoShort{
				PeerName:      spt.peerName,
				IPFS:          ipfsid.ID,
				IPFSAddresses: ipfsid.Addresses,
				Status:        api.TrackerStatusUndefined, // TBD
				TS:            p.Timestamp,
				Error:         "",
				AttemptCount:  0,
				PriorityPin:   false,
			},
		}

		ipfsStatus, pinnedInIpfs := ipfsRecursivePins[api.Cid(p.Cid)]

		switch {
		case p.Type == api.MetaType:
			info.Status = api.TrackerStatusSharded
		case p.IsRemotePin(spt.peerID):
			info.Status = api.TrackerStatusRemote
		case pinnedInIpfs:
			// No need to filter. pinnedInIpfs is false
			// unless the filter is Pinned |
			// UnexpectedlyUnpinned. We filter at the end.
			info.Status = ipfsStatus.ToTrackerStatus()
		default:
			// Not on an operation
			// Not a meta pin
			// Not a remote pin
			// Not a pin on ipfs

			// We understand that this is something that
			// should be pinned on IPFS and it is not.
			info.Status = api.TrackerStatusUnexpectedlyUnpinned
			info.Error = errUnexpectedlyUnpinned.Error()
		}
		if !filter.Match(info.Status) {
			continue
		}

		if !trySend(info) {
			return fmt.Errorf("error issuing PinInfo: %w", ctx.Err())
		}
	}
	return nil
}

// Status returns information for a Cid pinned to the local IPFS node.
func (spt *Tracker) Status(ctx context.Context, c api.Cid) api.PinInfo {
	ctx, span := trace.StartSpan(ctx, "tracker/stateless/Status")
	defer span.End()

	ipfsid := spt.getIPFSID(ctx)

	// check if c has an inflight operation or errorred operation in optracker
	if oppi, ok := spt.optracker.GetExists(ctx, c, ipfsid); ok {
		return oppi
	}

	pinInfo := api.PinInfo{
		Cid:  c,
		Peer: spt.peerID,
		Name: "", // etc to be filled later
		PinInfoShort: api.PinInfoShort{
			PeerName:      spt.peerName,
			IPFS:          ipfsid.ID,
			IPFSAddresses: ipfsid.Addresses,
			TS:            time.Now(),
			AttemptCount:  0,
			PriorityPin:   false,
		},
	}

	// check global state to see if cluster should even be caring about
	// the provided cid
	var gpin api.Pin
	st, err := spt.getState(ctx)
	if err != nil {
		logger.Error(err)
		addError(&pinInfo, err)
		return pinInfo
	}

	gpin, err = st.Get(ctx, c)
	if err == state.ErrNotFound {
		pinInfo.Status = api.TrackerStatusUnpinned
		return pinInfo
	}
	if err != nil {
		logger.Error(err)
		addError(&pinInfo, err)
		return pinInfo
	}
	// The pin IS in the state.
	pinInfo.Name = gpin.Name
	pinInfo.TS = gpin.Timestamp
	pinInfo.Allocations = gpin.Allocations
	pinInfo.Origins = gpin.Origins
	pinInfo.Created = gpin.Timestamp
	pinInfo.Metadata = gpin.Metadata

	// check if pin is a meta pin
	if gpin.Type == api.MetaType {
		pinInfo.Status = api.TrackerStatusSharded
		return pinInfo
	}

	// check if pin is a remote pin
	if gpin.IsRemotePin(spt.peerID) {
		pinInfo.Status = api.TrackerStatusRemote
		return pinInfo
	}

	// else attempt to get status from ipfs node
	var ips api.IPFSPinStatus
	err = spt.rpcClient.CallContext(
		ctx,
		"",
		"IPFSConnector",
		"PinLsCid",
		gpin,
		&ips,
	)
	if err != nil {
		logger.Error(err)
		addError(&pinInfo, err)
		return pinInfo
	}

	ipfsStatus := ips.ToTrackerStatus()
	switch ipfsStatus {
	case api.TrackerStatusUnpinned:
		// The item is in the state but not in IPFS:
		// PinError. Should be pinned.
		pinInfo.Status = api.TrackerStatusUnexpectedlyUnpinned
		pinInfo.Error = errUnexpectedlyUnpinned.Error()
	default:
		pinInfo.Status = ipfsStatus
	}
	return pinInfo
}

// RecoverAll attempts to recover all items tracked by this peer. It returns
// any errors or when it is done re-tracking.
func (spt *Tracker) RecoverAll(ctx context.Context, out chan<- api.PinInfo) error {
	defer close(out)

	ctx, span := trace.StartSpan(ctx, "tracker/stateless/RecoverAll")
	defer span.End()

	statusesCh := make(chan api.PinInfo, 1024)
	go func() {
		err := spt.StatusAll(ctx, api.TrackerStatusUndefined, statusesCh)
		if err != nil {
			logger.Error(err)
		}
	}()

	for st := range statusesCh {
		// Break out if we shutdown. We might be going through
		// a very long list of statuses.
		select {
		case <-spt.ctx.Done():
			err := fmt.Errorf("RecoverAll aborted: %w", ctx.Err())
			logger.Error(err)
			return err
		default:
			p, err := spt.recoverWithPinInfo(ctx, st)
			if err != nil {
				err = fmt.Errorf("RecoverAll error: %w", err)
				logger.Error(err)
				return err
			}
			if p.Defined() {
				select {
				case <-ctx.Done():
					err = fmt.Errorf("RecoverAll aborted: %w", ctx.Err())
					logger.Error(err)
					return err
				case out <- p:
				}
			}
		}
	}
	return nil
}

// Recover will trigger pinning or unpinning for items in
// PinError or UnpinError states.
func (spt *Tracker) Recover(ctx context.Context, c api.Cid) (api.PinInfo, error) {
	ctx, span := trace.StartSpan(ctx, "tracker/stateless/Recover")
	defer span.End()

	pi := spt.Status(ctx, c)

	recPi, err := spt.recoverWithPinInfo(ctx, pi)
	// if it was not enqueued, no updated pin-info is returned.
	// Use the one we had.
	if !recPi.Defined() {
		recPi = pi
	}
	return recPi, err
}

func (spt *Tracker) recoverWithPinInfo(ctx context.Context, pi api.PinInfo) (api.PinInfo, error) {
	st, err := spt.getState(ctx)
	if err != nil {
		logger.Error(err)
		return api.PinInfo{}, err
	}

	var pin api.Pin

	switch pi.Status {
	case api.TrackerStatusPinError, api.TrackerStatusUnexpectedlyUnpinned:
		pin, err = st.Get(ctx, pi.Cid)
		if err != nil { // ignore error - in case pin was removed while recovering
			logger.Warn(err)
			return spt.Status(ctx, pi.Cid), nil
		}
		logger.Infof("Restarting pin operation for %s", pi.Cid)
		err = spt.enqueue(ctx, pin, optracker.OperationPin)
	case api.TrackerStatusUnpinError:
		logger.Infof("Restarting unpin operation for %s", pi.Cid)
		err = spt.enqueue(ctx, api.PinCid(pi.Cid), optracker.OperationUnpin)
	default:
		// We do not return any information when recover was a no-op
		return api.PinInfo{}, nil
	}
	if err != nil {
		return spt.Status(ctx, pi.Cid), err
	}

	// This status call should be cheap as it would normally come from the
	// optracker and does not need to hit ipfs.
	return spt.Status(ctx, pi.Cid), nil
}

func (spt *Tracker) ipfsPins(ctx context.Context) (<-chan api.IPFSPinInfo, <-chan error) {
	ctx, span := trace.StartSpan(ctx, "tracker/stateless/ipfspins")
	defer span.End()

	in := make(chan []string, 1) // type filter.
	in <- []string{"recursive", "direct"}
	close(in)
	out := make(chan api.IPFSPinInfo, pinsChannelSize)
	errCh := make(chan error)

	go func() {
		err := spt.rpcClient.Stream(
			ctx,
			"",
			"IPFSConnector",
			"PinLs",
			in,
			out,
		)
		errCh <- err
		close(errCh)
	}()
	return out, errCh
}

// PinQueueSize returns the current size of the pinning queue.
func (spt *Tracker) PinQueueSize(ctx context.Context) (int64, error) {
	return spt.optracker.PinQueueSize(), nil
}

// func (spt *Tracker) getErrorsAll(ctx context.Context) []api.PinInfo {
// 	return spt.optracker.Filter(ctx, optracker.PhaseError)
// }

// OpContext exports the internal optracker's OpContext method.
// For testing purposes only.
func (spt *Tracker) OpContext(ctx context.Context, c api.Cid) context.Context {
	return spt.optracker.OpContext(ctx, c)
}

func addError(pinInfo *api.PinInfo, err error) {
	pinInfo.Error = err.Error()
	pinInfo.Status = api.TrackerStatusClusterError
}

func getShardNumber(pinName string) (int, string, error) {
	// Assuming the format of pin.Name() is something like "xxx-shard-i"
	parts := strings.Split(pinName, "-shard-")
	if len(parts) < 2 {
		return -1, "", fmt.Errorf("invalid shard format 1")
	}
	// Convert shard number (i) to integer
	num1 := strings.Split(pinName, ")-")
	if len(num1) < 2 {
		return -1, "", fmt.Errorf("invalid shard format 2")
	}
	num11 := num1[1]
	if strings.Contains(pinName, "Rep") {
		num2 := strings.Split(num11, "Rep")
		if len(num2) < 2 {
			return -1, "", fmt.Errorf("invalid shard format 3")
		}
		num, err := strconv.Atoi(num2[0])
		name := parts[0]
		fmt.Fprintf(os.Stdout, "getShardNumber : name is %s and number is : %d \n", name, num)
		return num, name, err
	} else {
		num, err := strconv.Atoi(num11)
		name := parts[0]
		fmt.Fprintf(os.Stdout, "getShardNumber : name is %s and number is : %d \n", name, num)
		return num, name, err
	}
}

type pinwithmeta struct {
	pin   api.Pin
	index int
	cids  []string
}
type Chunk struct {
	cid   string
	index int
}

func sortRepairShardsByIndex(repairShards []pinwithmeta) {
	// Sorting c.repairShards slice by index field in increasing order
	sort.Slice(repairShards, func(i, j int) bool {
		return repairShards[i].index < repairShards[j].index
	})
}
