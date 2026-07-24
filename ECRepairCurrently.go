package ipfscluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ipfs-cluster/ipfs-cluster/adder/ipfsadd"
	"github.com/ipfs-cluster/ipfs-cluster/adder/sharding"
	"github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/go-cid"
	"github.com/klauspost/reedsolomon"
	"github.com/multiformats/go-multihash"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p/core/peer"

	"go.opencensus.io/trace"
)

const (
	DefaultMaxPinQueueSize       = 1000000
	DefaultConcurrentPins        = 20
	DefaultPriorityPinMaxAge     = 24 * time.Hour
	DefaultPriorityPinMaxRetries = 5
)

//var logger = logging.Logger("ECRepair")

const pinsChannelSize = 1024

var (
	// ErrFullQueue is the error used when pin or unpin operation channel is full.
	ErrFullQueue = errors.New("pin/unpin operation queue is full. Try increasing max_pin_queue_size")

	// items with this error should be recovered
	errUnexpectedlyUnpinned = errors.New("the item should be pinned but it is not")
)

// Tracker uses the optracker.OperationTracker to manage
// transitioning shared ipfs-cluster state (Pins) to the local IPFS node.
type ECRepairS struct {
	config *Config

	peerID peer.ID

	ctx    context.Context
	cancel func()

	rpcClient *rpc.Client
	rpcReady  chan struct{}

	//priorityPinCh chan *optracker.Operation
	//pinCh         chan *optracker.Operation
	//unpinCh       chan *optracker.Operation
	RepairCh   chan *api.Pin
	cons       Consensus
	shutdownMu sync.Mutex
	shutdown   bool
	wg         sync.WaitGroup
	connector  IPFSConnector
}

// New creates a new StatelessPinTracker.
func NewECrep(cfg *Config, pid peer.ID, cons Consensus, connector IPFSConnector, rpc *rpc.Client) *ECRepairS {
	ctx, cancel := context.WithCancel(context.Background())

	spt := &ECRepairS{
		config:   cfg,
		peerID:   pid,
		ctx:      ctx,
		cancel:   cancel,
		rpcReady: make(chan struct{}, 1),
		//priorityPinCh: make(chan *optracker.Operation, cfg.MaxPinQueueSize),
		//pinCh:         make(chan *optracker.Operation, cfg.MaxPinQueueSize),
		//unpinCh:       make(chan *optracker.Operation, cfg.MaxPinQueueSize),
		rpcClient: rpc,
		RepairCh:  make(chan *api.Pin, DefaultMaxPinQueueSize),
		cons:      cons,
		connector: connector,
	}

	for i := 0; i < DefaultConcurrentPins; i++ {
		go spt.opWorker(spt.RepairCh)
	}
	return spt
}

// we can get our IPFS id from our own monitor ping metrics which
// are refreshed regularly.
func (spt *ECRepairS) getIPFSID(ctx context.Context) api.IPFSID {
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
func (spt *ECRepairS) opWorker(RepairCh chan *api.Pin) {
	var op *api.Pin
	for {
		// Process the channel
		select {
		case op = <-RepairCh:
			if op == nil {
				// Skip nil pins to prevent panic
				continue
			}
			spt.pin(op)
		case <-spt.ctx.Done():
			return
		}
	}
}

func (spt *ECRepairS) pin(op *api.Pin) error {
	fmt.Fprintf(os.Stdout, "Date start inside the pintracker repair %s : %s \n", op.Name, time.Now().Format("2006-01-02 15:04:05.000"))
	////////////////////////////////////
	//              TODO: USE this in the new repair with respect to similarities.
	//
	//CIDs := op.Metadata["Cids"]
	var download, repair, waittosend time.Duration
	if op.Metadata["Strategy"] == "SELECTIVE_EC" {
		download, repair, waittosend = spt.repinUsingRSSelectiveEC(op)
	} else {
		if op.Metadata["Strategy"] == "ASCLEPIUS" {
			//download, repair, waittosend = spt.repinUsingRSASCLEPIUS(op)
			download, repair, waittosend = spt.repinUsingRSWithSwitching_updated(op)
		} else {
			download, repair, waittosend = spt.repinUsingRSWithSwitching1(op)
		}
	}

	//download, repair, waittosend := spt.repinUsingRSrelatedWork(op)
	fmt.Fprintf(os.Stdout, "Time Taken to download chunks is : %s and to repair chunks is : %s and additional time to wait to complete sending the shard : %s \n", download.String(), repair.String(), waittosend.String())
	fmt.Fprintf(os.Stdout, "Date end inside the pintracker repair %s : %s \n", op.Name, time.Now().Format("2006-01-02 15:04:05.000"))
	return nil

}

func startTimerNew5(ctx context.Context, toskip *bool) {
	ticker := time.NewTicker(time.Duration(1 * float64(time.Second)))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Timer stopped")
			*toskip = false
			return
		case <-ticker.C:
			*toskip = true

		}
	}
}

// Map to track occurrences
type cidInfo struct {
	Count   int
	Indexes []int
}

// This will keep track of the fastest peers to use every 1 second, in addition to that, it will add the minimal interference to the system since it will ask for six data chunks during 1 sec interval before update
// to the fastest n again.
func (spt *ECRepairS) repinUsingRSWithSwitching(pin *api.Pin) (time.Duration, time.Duration, time.Duration) {
	overallstart := time.Now()
	ssss := time.Now()
	repairShards := make([]pinwithmeta, 0)
	cidString := pin.Metadata["Cids"]
	CIDs := strings.Split(cidString, ",")
	//start := time.Now()
	var timedownloadchunks, timetorepairchunksonly time.Duration
	ctx, span := trace.StartSpan(spt.ctx, "pintracker/repinFromPeer")
	defer span.End()
	p := pin.Allocations
	f1 := strings.Split(pin.Name, "(")[1]
	f2 := strings.Split(f1, ")")[0]
	or, _ := strconv.Atoi(strings.Split(f2, ",")[0])
	par, _ := strconv.Atoi(strings.Split(f2, ",")[1])
	logger.Debugf("repinning %s from peer %s", pin.Cid, p)
	blacklist := make([]peer.ID, 0)
	blacklist = append(blacklist, pin.Allocations[0])
	//blacklist = append(blacklist, pin.Allocations...)-
	prefix, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		return 0, 0, 0
	}

	hashFunCode, _ := multihash.Names[strings.ToLower("sha2-256")]
	prefix.MhType = hashFunCode
	prefix.MhLength = -1
	//here we want to recreate the missing shard
	cState, err := spt.cons.State(spt.ctx)
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
	numpin, _, err := getShardNumber(pin.Name)
	if err != nil {
		fmt.Println("Error:", err)
		return 0, 0, 0
	}
	tosend := (numpin - 1) % (or + par)
	fmt.Printf("number of the shard to repair is : %d \n", numpin)
	//mod := numpin % (or + par)
	//before := (numpin - 1) % (or + par)
	//after := (or + par - mod) % (or + par)
	Local := true
	shardCids := make([]Chunk, 0)
	clustername := strings.Split(pin.Name, "-shard")[0] + "-clusterDAG-EC()-chunksize"
	for pinn := range pinCh {
		if pinn.Name == clustername {
			pinnn := pinwithmeta{pinn, 0, make([]string, 0)}
			shardCids = spt.retrieveCids(pinnn)
			break
		}
	}

	for _, sh := range shardCids {
		fmt.Printf("Shardsss Cids : %s \n", sh.cid)
	}
	stripeSize := or + par

	// convert shard number to stripe index
	stripeIndex := (numpin - 1) / stripeSize

	start := stripeIndex * stripeSize
	end := start + stripeSize

	if end > len(shardCids) {
		end = len(shardCids)
	}

	selectedShardCids := shardCids[start:end]
	var wggs sync.WaitGroup
	var mugs sync.Mutex
	wggs.Add(or)
	for i, sh := range selectedShardCids {
		go func() {
			cc, _ := cid.Decode(sh.cid)
			gg, errr := cState.Get(ctx, api.Cid{cc})
			if errr != nil {
				return
			}
			mugs.Lock()
			pinnn := pinwithmeta{gg, i + 1, make([]string, 0)}
			repairShards = append(repairShards, pinnn)
			wggs.Done()
			mugs.Unlock()
			return

		}()
		fmt.Printf("Selectedddd Shardsss Cids : %s \n", sh.cid)
	}
	wggs.Wait()

	//fmt.Printf("taking shards between %d and %d \n", numpin-before, numpin+after)
	/*for pinn := range pinCh {
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
				if slices.Contains(pinn.Allocations, spt.peerID) {
					fmt.Printf("the spt ID is : %s Locallllll willl be falseeeeeee shardddd: %s with allocationnn : %s \n", spt.peerID.String(), pinn.Name, pinn.Allocations[0].String())
					Local = false
				}
				pinnn := pinwithmeta{pin: pinn, index: pinnShardNum, cids: make([]string, 0)}
				repairShards = append(repairShards, pinnn)
				for _,ss := range repairShards {
				}
			}
		}
	}

	*/
	// Sort repairShard by Index in ascending order
	sortRepairShardsByIndex(repairShards)

	wgg := new(sync.WaitGroup)
	wgg.Add(or)
	muu := new(sync.Mutex)
	ret := 0
	fmt.Printf("STEEEEEEEEEPPPPPPPPPP RRRRRRRRRREEEEEEETTTTTTTTT with length of repair shards is : %d \n", len(repairShards))
	for i, pinwm := range repairShards {
		go func(pinwm pinwithmeta, i int) {
			cidss := spt.retrieveCids(pinwm)
			muu.Lock()
			if ret < or {
				ret++
				for j, _ := range cidss {
					repairShards[i].cids = append(repairShards[i].cids, cidss[j].cid)
				}
				wgg.Done()
			} else {
				for j, _ := range cidss {
					repairShards[i].cids = append(repairShards[i].cids, cidss[j].cid)
				}
			}
			muu.Unlock()

		}(pinwm, i)
	}
	wgg.Wait()
	fmt.Printf("Extracting !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! everything took : %s and localllllll is %t \n", time.Now().Sub(ssss).String(), Local)
	//Local
	if Local {
		pin.Allocations = make([]peer.ID, 0)
		pin.Allocations = append(pin.Allocations, spt.peerID)
		shh, _ := sharding.NewShards(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions)
		for _, pid := range shh.Allocations() {
			fmt.Printf("Allocationssssssssssssss are: %s \n", pid.String())
		}

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
		toskip := true
		timerlaunched := false
		Indexes := make([]int, 0)
		ctxx, cancell := context.WithCancel(context.Background())
		for i := 0; i < times; i++ {
			cc, _ := cid.Decode(CIDs[i])
			exists, bad := spt.connector.BlockLocalHas(spt.ctx, cc)
			if !exists || (bad != nil) {
				retrieved := 0
				sttt := time.Now()
				reconstructshards := make([][]byte, or+par)
				nbShardsMeta := 0
				readfrom := make([]pinwithmeta, 0)
				for _, shard := range repairShards {
					if len(shard.cids) > 0 {
						nbShardsMeta++
						readfrom = append(readfrom, shard)
					}
				}
				if nbShardsMeta > or {
					//we want to apply the switching every 1 sec
					if !timerlaunched {
						//start the timer that will be responsible of notifying switching
						go startTimerNew5(ctxx, &toskip)
						timerlaunched = true
					}
					if toskip {
						Indexes = make([]int, 0)
						wg.Add(or)
						ctxx, cancel := context.WithCancel(context.Background())
						for _, shard := range readfrom {
							if len(shard.cids) > 0 {
								go func(i int, shard pinwithmeta) {
									sss := time.Now()
									bytess := spt.getData(ctxx, shard.cids[i])
									nnn := time.Since(sss)
									fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
									mu.Lock()
									if retrieved < or {
										retrieved++
										reconstructshards[(shard.index-1)%(or+par)] = bytess
										Indexes = append(Indexes, shard.index)
										mu.Unlock()
										wg.Done()
										if retrieved == or {
											cancel()
										}
									} else {
										cancel()
										mu.Unlock()
									}

								}(i, shard)
							}
						}
						wg.Wait()
						fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
						toskip = false
					} else {
						readfiltered := make([]pinwithmeta, 0)
						for _, shard := range readfrom {
							for _, index := range Indexes {
								if shard.index == index {
									readfiltered = append(readfiltered, shard)
								}
							}
						}
						wg.Add(or)
						ctxx, cancel := context.WithCancel(context.Background())
						for _, shard := range readfiltered {
							if len(shard.cids) > 0 {
								go func(i int, shard pinwithmeta) {
									sss := time.Now()
									bytess := spt.getData(ctxx, shard.cids[i])
									nnn := time.Since(sss)
									fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
									mu.Lock()
									if retrieved < or {
										retrieved++
										reconstructshards[(shard.index-1)%(or+par)] = bytess
										mu.Unlock()
										wg.Done()
										if retrieved == or {
											cancel()
										}
									} else {
										cancel()
										mu.Unlock()
									}

								}(i, shard)
							}
						}
						wg.Wait()
						fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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

				} else {
					//ask for the six out of six because at least we have six shards metadata
					wg.Add(or)
					ctxx, cancel := context.WithCancel(context.Background())
					for _, shard := range readfrom {
						if len(shard.cids) > 0 {
							go func(i int, shard pinwithmeta) {
								sss := time.Now()
								bytess := spt.getData(ctxx, shard.cids[i])
								nnn := time.Since(sss)
								fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
								mu.Lock()
								if retrieved < or {
									retrieved++
									reconstructshards[(shard.index-1)%(or+par)] = bytess
									mu.Unlock()
									wg.Done()
									if retrieved == or {
										cancel()
									}
								} else {
									cancel()
									mu.Unlock()
								}

							}(i, shard)
						}
					}
					wg.Wait()
					fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
			} else {
				fmt.Printf("Entereddddddd to the have localllll part\n")
				bytess := spt.getData(ctxx, cc.String())
				fmt.Printf("This is the dataaaaaaaaaaaa size : %d \n", len(bytess))
				nodee := ipfsadd.NewFSNodeOverDagC(ft.TFile, prefix)
				nodee.SetFileData(bytess)
				rawnode, _ := nodee.Commit()
				//zid l blacklist heyye list li other pins kamen fiha
				shh.SendBlock(spt.ctx, rawnode)
				size := uint64(len(rawnode.RawData()))
				shh.AddLink(ctx, rawnode.Cid(), size)
			}

		}
		wait1 := time.Now()

		for _, al := range pin.Allocations {
			fmt.Printf("000000000 ALLLLL in cluster pin is : %s \n", al.String())
		}
		shh.FlushForStateless(ctx, *pin)
		//if err != nil {
		//return err
		//}
		//shh.FlushForStateless(spt.ctx, *pin)
		wait2 := time.Since(wait1)
		cancell()
		overallend := time.Now()
		fmt.Printf("Overall repair time: %s \n", overallend.Sub(overallstart).String())
		return timedownloadchunks, timetorepairchunksonly, wait2
	} else {
		for _, pi := range repairShards {
			for _, per := range pi.pin.Allocations {
				blacklist = append(blacklist, per)
			}
		}
		if pin.PinOptions.Metadata == nil {
			pin.PinOptions.Metadata = make(map[string]string)
		}

		pin.PinOptions.Metadata["Black"] = ""
		for _, bl := range blacklist {
			fmt.Printf("BBBBLLLLL : %s \n", bl.String())
			pin.PinOptions.Metadata["Black"] = pin.PinOptions.Metadata["Black"] + "," + bl.String()
		}

		shh, _ := sharding.NewShards(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions)
		pin.Allocations = make([]peer.ID, 0)
		for _, all := range shh.Allocations() {
			pin.Allocations = append(pin.Allocations, all)
		}
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
		toskip := true
		timerlaunched := false
		Indexes := make([]int, 0)
		ctxx, cancell := context.WithCancel(context.Background())
		for i := 0; i < times; i++ {
			cc, _ := cid.Decode(CIDs[i])
			exists, bad := spt.connector.BlockLocalHas(spt.ctx, cc)
			if !exists || (bad != nil) {
				retrieved := 0
				sttt := time.Now()
				reconstructshards := make([][]byte, or+par)
				nbShardsMeta := 0
				readfrom := make([]pinwithmeta, 0)
				for _, shard := range repairShards {
					if len(shard.cids) > 0 {
						nbShardsMeta++
						readfrom = append(readfrom, shard)
					}
				}
				if nbShardsMeta > or {
					//we want to apply the switching every 1 sec
					if !timerlaunched {
						//start the timer that will be responsible of notifying switching
						go startTimerNew5(ctxx, &toskip)
						timerlaunched = true
					}
					if toskip {
						Indexes = make([]int, 0)
						wg.Add(or)
						ctxx, cancel := context.WithCancel(context.Background())
						for _, shard := range readfrom {
							if len(shard.cids) > 0 {
								go func(i int, shard pinwithmeta) {
									sss := time.Now()
									bytess := spt.getData(ctxx, shard.cids[i])
									nnn := time.Since(sss)
									fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
									mu.Lock()
									if retrieved < or {
										retrieved++
										reconstructshards[(shard.index-1)%(or+par)] = bytess
										Indexes = append(Indexes, shard.index)
										mu.Unlock()
										wg.Done()
										if retrieved == or {
											cancel()
										}
									} else {
										cancel()
										mu.Unlock()
									}

								}(i, shard)
							}
						}
						wg.Wait()
						fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
						toskip = false
					} else {
						readfiltered := make([]pinwithmeta, 0)
						for _, shard := range readfrom {
							for _, index := range Indexes {
								if shard.index == index {
									readfiltered = append(readfiltered, shard)
								}
							}
						}
						wg.Add(or)
						ctxx, cancel := context.WithCancel(context.Background())
						for _, shard := range readfiltered {
							if len(shard.cids) > 0 {
								go func(i int, shard pinwithmeta) {
									sss := time.Now()
									bytess := spt.getData(ctxx, shard.cids[i])
									nnn := time.Since(sss)
									fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
									mu.Lock()
									if retrieved < or {
										retrieved++
										reconstructshards[(shard.index-1)%(or+par)] = bytess
										mu.Unlock()
										wg.Done()
										if retrieved == or {
											cancel()
										}
									} else {
										cancel()
										mu.Unlock()
									}

								}(i, shard)
							}
						}
						wg.Wait()
						fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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

				} else {
					//ask for the six out of six because at least we have six shards metadata
					wg.Add(or)
					ctxx, cancel := context.WithCancel(context.Background())
					for _, shard := range readfrom {
						if len(shard.cids) > 0 {
							go func(i int, shard pinwithmeta) {
								sss := time.Now()
								bytess := spt.getData(ctxx, shard.cids[i])
								nnn := time.Since(sss)
								fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
								mu.Lock()
								if retrieved < or {
									retrieved++
									reconstructshards[(shard.index-1)%(or+par)] = bytess
									mu.Unlock()
									wg.Done()
									if retrieved == or {
										cancel()
									}
								} else {
									cancel()
									mu.Unlock()
								}

							}(i, shard)
						}
					}
					wg.Wait()
					fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
			} else {
				fmt.Printf("Entereddddddd to the have localllll part\n")
				bytess := spt.getData(ctxx, cc.String())
				fmt.Printf("This is the dataaaaaaaaaaaa size : %d \n", len(bytess))
				nodee := ipfsadd.NewFSNodeOverDagC(ft.TFile, prefix)
				nodee.SetFileData(bytess)
				rawnode, _ := nodee.Commit()
				//zid l blacklist heyye list li other pins kamen fiha
				shh.SendBlock(spt.ctx, rawnode)
				size := uint64(len(rawnode.RawData()))
				shh.AddLink(ctx, rawnode.Cid(), size)
			}
		}
		wait1 := time.Now()
		for _, al := range pin.Allocations {
			fmt.Printf("000000000 ALLLLL in cluster pin is : %s \n", al.String())
		}
		shh.FlushForStateless(spt.ctx, *pin)
		wait2 := time.Since(wait1)
		cancell()
		overallend := time.Now()
		fmt.Printf("Overall repair time: %s \n", overallend.Sub(overallstart).String())
		return timedownloadchunks, timetorepairchunksonly, wait2
	}

	//MFS
	//shh, _ := sharding.NewShards(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions)

}

/*
	func (spt *ECRepairS) locally(ctx context.Context, Cid cid.Cid) ([]byte, bool) {
		ci := api.Cid{Cid: Cid}
		rpcCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		bytes, err := spt.connector.BlockGet(rpcCtx, ci)
		if err != nil {
			// handle RPC / connection error
			cancel()
			return nil, false
		}
		cancel()
		return bytes, true
	}
*/
func contains(slice []string, id string) bool {
	for _, v := range slice {
		if v == id {
			return true
		}
	}
	return false
}

func (spt *ECRepairS) getData(ctx context.Context, Cid string) []byte {
	CidNew, _ := cid.Decode(Cid)
	nnn, _ := spt.connector.ChunkGet(ctx, CidNew)
	return nnn
}

func (spt *ECRepairS) retrieveCids(pinwm pinwithmeta) []Chunk {
	s, _ := spt.connector.NodeGet(spt.ctx, pinwm.pin.Cid.Cid)
	cids := doTheProcess(s)
	return cids
}

func doTheProcess(nn string) []Chunk {
	parsedData, _ := convertStringToJSON(nn)
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
func convertStringToJSON(input string) (Data, error) {
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

// Enqueue puts a new operation on the queue, unless ongoing exists.
func (spt *ECRepairS) Enqueue(ctx context.Context, c api.Pin) error {
	fmt.Fprintf(os.Stdout, "Enqueuing pin: %s\n", c.Cid.Cid.String())

	select {
	case spt.RepairCh <- &c:
		// Successfully enqueued
		return nil
	default:
		// Queue is full
		return ErrFullQueue
	}
}

// SetClient makes the StatelessPinTracker ready to perform RPC requests to
// other components.
func (spt *ECRepairS) setClient(c *rpc.Client) {
	spt.rpcClient = c
	close(spt.rpcReady)
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

// This will keep track of the fastest peers to use every 1 second, in addition to that, it will add the minimal interference to the system since it will ask for six data chunks during 1 sec interval before update
// to the fastest n again.
func (spt *ECRepairS) repinUsingRSWithSwitchingNew(pin *api.Pin) (time.Duration, time.Duration, time.Duration) {
	overallstart := time.Now()
	ssss := time.Now()
	repairShards := make([]api.Pin, 0)
	cidString := pin.Metadata["Cids"]
	CIDs := strings.Split(cidString, ",")
	//start := time.Now()
	var timedownloadchunks, timetorepairchunksonly time.Duration
	ctx, span := trace.StartSpan(spt.ctx, "pintracker/repinFromPeer")
	defer span.End()
	p := pin.Allocations
	f1 := strings.Split(pin.Name, "(")[1]
	f2 := strings.Split(f1, ")")[0]
	or, _ := strconv.Atoi(strings.Split(f2, ",")[0])
	par, _ := strconv.Atoi(strings.Split(f2, ",")[1])
	logger.Debugf("repinning %s from peer %s", pin.Cid, p)
	blacklist := make([]peer.ID, 0)
	blacklist = append(blacklist, pin.Allocations[0])
	//blacklist = append(blacklist, pin.Allocations...)-
	prefix, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		return 0, 0, 0
	}

	hashFunCode, _ := multihash.Names[strings.ToLower("sha2-256")]
	prefix.MhType = hashFunCode
	prefix.MhLength = -1
	//here we want to recreate the missing shard
	cState, err := spt.cons.State(spt.ctx)
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
	numpin, _, err := getShardNumber(pin.Name)
	if err != nil {
		fmt.Println("Error:", err)
		return 0, 0, 0
	}
	tosend := (numpin - 1) % (or + par)
	fmt.Printf("number of the shard to repair is : %d \n", numpin)
	//mod := numpin % (or + par)
	//before := (numpin - 1) % (or + par)
	//after := (or + par - mod) % (or + par)
	Local := true
	shardCids := make([]Chunk, 0)
	clustername := strings.Split(pin.Name, "-shard")[0] + "-clusterDAG-EC()-chunksize"
	for pinn := range pinCh {
		if pinn.Name == clustername {
			pinnn := pinwithmeta{pinn, 0, make([]string, 0)}
			shardCids = spt.retrieveCids(pinnn)
			break
		}
	}

	for _, sh := range shardCids {
		fmt.Printf("Shardsss Cids : %s \n", sh.cid)
	}
	stripeSize := or + par

	// convert shard number to stripe index
	stripeIndex := (numpin - 1) / stripeSize

	start := stripeIndex * stripeSize
	end := start + stripeSize

	if end > len(shardCids) {
		end = len(shardCids)
	}

	selectedShardCids := shardCids[start:end]
	for _, sh := range selectedShardCids {
		cc, _ := cid.Decode(sh.cid)
		gg, _ := cState.Get(ctx, api.Cid{cc})
		repairShards = append(repairShards, gg)
		fmt.Printf("Selectedddd Shardsss Cids : %s \n", sh.cid)
	}

	//fmt.Printf("taking shards between %d and %d \n", numpin-before, numpin+after)
	/*for pinn := range pinCh {
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
				if slices.Contains(pinn.Allocations, spt.peerID) {
					fmt.Printf("the spt ID is : %s Locallllll willl be falseeeeeee shardddd: %s with allocationnn : %s \n", spt.peerID.String(), pinn.Name, pinn.Allocations[0].String())
					Local = false
				}
				pinnn := pinwithmeta{pin: pinn, index: pinnShardNum, cids: make([]string, 0)}
				repairShards = append(repairShards, pinnn)
				for _,ss := range repairShards {
				}
			}
		}
	}

	*/
	// Sort repairShard by Index in ascending order

	shardCidds := make([][]string, 0)
	for _, sh := range repairShards {
		cc := strings.Split(sh.Metadata["Cids"], ",")
		shardCidds = append(shardCidds, cc)
	}
	fmt.Printf("Extracting !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! everything took : %s and localllllll is %t \n", time.Now().Sub(ssss).String(), Local)
	//Local
	if Local {
		pin.Allocations = make([]peer.ID, 0)
		pin.Allocations = append(pin.Allocations, spt.peerID)
		shh, _ := sharding.NewShards(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions)
		for _, pid := range shh.Allocations() {
			fmt.Printf("Allocationssssssssssssss are: %s \n", pid.String())
		}

		enc, _ := reedsolomon.New(or, par)
		times := len(shardCidds[0])
		//open gourotines to retrieve data in parallel
		wg := new(sync.WaitGroup)
		mu := new(sync.Mutex)
		toskip := true
		timerlaunched := false
		Indexes := make([]int, 0)
		ctxx, cancell := context.WithCancel(context.Background())
		for i := 0; i < times; i++ {
			cc, _ := cid.Decode(CIDs[i])
			exists, bad := spt.connector.BlockLocalHas(spt.ctx, cc)
			if !exists || (bad != nil) {
				retrieved := 0
				sttt := time.Now()
				reconstructshards := make([][]byte, or+par)
				//we want to apply the switching every 1 sec
				if !timerlaunched {
					//start the timer that will be responsible of notifying switching
					go startTimerNew5(ctxx, &toskip)
					timerlaunched = true
				}
				if toskip {
					Indexes = make([]int, 0)
					wg.Add(or)
					ctxx, cancel := context.WithCancel(context.Background())
					for j := 0; j < or+par; j++ {
						go func(i int, j int) {
							bytess := spt.getData(ctxx, shardCidds[j][i])
							fmt.Printf("GOTTTTTTTTTTT: %s \n", shardCidds[j][i])
							mu.Lock()
							if retrieved < or {
								retrieved++
								reconstructshards[j] = bytess
								Indexes = append(Indexes, j)
								mu.Unlock()
								wg.Done()
								if retrieved == or {
									cancel()
								}
							} else {
								cancel()
								mu.Unlock()
							}

						}(i, j)
					}
					wg.Wait()
					fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
					toskip = false
				} else {
					wg.Add(or)
					ctxx, cancel := context.WithCancel(context.Background())
					for _, j := range Indexes {
						go func(i int, j int) {
							bytess := spt.getData(ctxx, shardCidds[j][i])
							fmt.Printf("GOTTTTTTTTTTT: %s \n", shardCidds[j][i])
							mu.Lock()
							if retrieved < or {
								retrieved++
								reconstructshards[j] = bytess
								Indexes = append(Indexes, j)
								mu.Unlock()
								wg.Done()
								if retrieved == or {
									cancel()
								}
							} else {
								cancel()
								mu.Unlock()
							}

						}(i, j)
					}

					wg.Wait()
					fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
			} else {
				fmt.Printf("Entereddddddd to the have localllll part\n")
				bytess := spt.getData(ctxx, cc.String())
				fmt.Printf("This is the dataaaaaaaaaaaa size : %d \n", len(bytess))
				nodee := ipfsadd.NewFSNodeOverDagC(ft.TFile, prefix)
				nodee.SetFileData(bytess)
				rawnode, _ := nodee.Commit()
				//zid l blacklist heyye list li other pins kamen fiha
				shh.SendBlock(spt.ctx, rawnode)
				size := uint64(len(rawnode.RawData()))
				shh.AddLink(ctx, rawnode.Cid(), size)
			}

		}
		wait1 := time.Now()

		for _, al := range pin.Allocations {
			fmt.Printf("000000000 ALLLLL in cluster pin is : %s \n", al.String())
		}
		shh.FlushForStateless(ctx, *pin)
		//if err != nil {
		//return err
		//}
		//shh.FlushForStateless(spt.ctx, *pin)
		wait2 := time.Since(wait1)
		cancell()
		overallend := time.Now()
		fmt.Printf("Overall repair time: %s \n", overallend.Sub(overallstart).String())
		return timedownloadchunks, timetorepairchunksonly, wait2
	}
	/*else {
	for _, pi := range repairShards {
		for _, per := range pi.pin.Allocations {
			blacklist = append(blacklist, per)
		}
	}
	if pin.PinOptions.Metadata == nil {
		pin.PinOptions.Metadata = make(map[string]string)
	}

	pin.PinOptions.Metadata["Black"] = ""
	for _, bl := range blacklist {
		fmt.Printf("BBBBLLLLL : %s \n", bl.String())
		pin.PinOptions.Metadata["Black"] = pin.PinOptions.Metadata["Black"] + "," + bl.String()
	}

	shh, _ := sharding.NewShards(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions)
	pin.Allocations = make([]peer.ID, 0)
	for _, all := range shh.Allocations() {
		pin.Allocations = append(pin.Allocations, all)
	}
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
	toskip := true
	timerlaunched := false
	Indexes := make([]int, 0)
	ctxx, cancell := context.WithCancel(context.Background())
	for i := 0; i < times; i++ {
		cc, _ := cid.Decode(CIDs[i])
		exists, bad := spt.connector.BlockLocalHas(spt.ctx, cc)
		if !exists || (bad != nil) {
			retrieved := 0
			sttt := time.Now()
			reconstructshards := make([][]byte, or+par)
			nbShardsMeta := 0
			readfrom := make([]pinwithmeta, 0)
			for _, shard := range repairShards {
				if len(shard.cids) > 0 {
					nbShardsMeta++
					readfrom = append(readfrom, shard)
				}
			}
			if nbShardsMeta > or {
				//we want to apply the switching every 1 sec
				if !timerlaunched {
					//start the timer that will be responsible of notifying switching
					go startTimerNew5(ctxx, &toskip)
					timerlaunched = true
				}
				if toskip {
					Indexes = make([]int, 0)
					wg.Add(or)
					ctxx, cancel := context.WithCancel(context.Background())
					for _, shard := range readfrom {
						if len(shard.cids) > 0 {
							go func(i int, shard pinwithmeta) {
								sss := time.Now()
								bytess := spt.getData(ctxx, shard.cids[i])
								nnn := time.Since(sss)
								fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
								mu.Lock()
								if retrieved < or {
									retrieved++
									reconstructshards[(shard.index-1)%(or+par)] = bytess
									Indexes = append(Indexes, shard.index)
									mu.Unlock()
									wg.Done()
									if retrieved == or {
										cancel()
									}
								} else {
									cancel()
									mu.Unlock()
								}

							}(i, shard)
						}
					}
					wg.Wait()
					fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
					toskip = false
				} else {
					readfiltered := make([]pinwithmeta, 0)
					for _, shard := range readfrom {
						for _, index := range Indexes {
							if shard.index == index {
								readfiltered = append(readfiltered, shard)
							}
						}
					}
					wg.Add(or)
					ctxx, cancel := context.WithCancel(context.Background())
					for _, shard := range readfiltered {
						if len(shard.cids) > 0 {
							go func(i int, shard pinwithmeta) {
								sss := time.Now()
								bytess := spt.getData(ctxx, shard.cids[i])
								nnn := time.Since(sss)
								fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
								mu.Lock()
								if retrieved < or {
									retrieved++
									reconstructshards[(shard.index-1)%(or+par)] = bytess
									mu.Unlock()
									wg.Done()
									if retrieved == or {
										cancel()
									}
								} else {
									cancel()
									mu.Unlock()
								}

							}(i, shard)
						}
					}
					wg.Wait()
					fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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

			} else {
				//ask for the six out of six because at least we have six shards metadata
				wg.Add(or)
				ctxx, cancel := context.WithCancel(context.Background())
				for _, shard := range readfrom {
					if len(shard.cids) > 0 {
						go func(i int, shard pinwithmeta) {
							sss := time.Now()
							bytess := spt.getData(ctxx, shard.cids[i])
							nnn := time.Since(sss)
							fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
							mu.Lock()
							if retrieved < or {
								retrieved++
								reconstructshards[(shard.index-1)%(or+par)] = bytess
								mu.Unlock()
								wg.Done()
								if retrieved == or {
									cancel()
								}
							} else {
								cancel()
								mu.Unlock()
							}

						}(i, shard)
					}
				}
				wg.Wait()
				fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
		} else {
			fmt.Printf("Entereddddddd to the have localllll part\n")
			bytess := spt.getData(ctxx, cc.String())
			fmt.Printf("This is the dataaaaaaaaaaaa size : %d \n", len(bytess))
			nodee := ipfsadd.NewFSNodeOverDagC(ft.TFile, prefix)
			nodee.SetFileData(bytess)
			rawnode, _ := nodee.Commit()
			//zid l blacklist heyye list li other pins kamen fiha
			shh.SendBlock(spt.ctx, rawnode)
			size := uint64(len(rawnode.RawData()))
			shh.AddLink(ctx, rawnode.Cid(), size)
		}
	}
	wait1 := time.Now()
	for _, al := range pin.Allocations {
		fmt.Printf("000000000 ALLLLL in cluster pin is : %s \n", al.String())
	}
	shh.FlushForStateless(spt.ctx, *pin)
	wait2 := time.Since(wait1)
	cancell()
	overallend := time.Now()
	fmt.Printf("Overall repair time: %s \n", overallend.Sub(overallstart).String())
	return timedownloadchunks, timetorepairchunksonly, wait2*/
	//}

	//MFS
	//shh, _ := sharding.NewShards(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions)

	return 0, 0, 0
}

// This will keep track of the fastest peers to use every 1 second, in addition to that, it will add the minimal interference to the system since it will ask for six data chunks during 1 sec interval before update
// to the fastest n again.
func (spt *ECRepairS) repinUsingRSWithSwitching1(pin *api.Pin) (time.Duration, time.Duration, time.Duration) {
	ssss := time.Now()
	repairShards := make([]pinwithmeta, 0)
	start := time.Now()
	cidString := pin.Metadata["Cids"]
	strategy := pin.Metadata["Strategy"]
	CIDs := strings.Split(cidString, ",")
	commonstring := pin.Metadata["common"]
	Common := strings.Split(commonstring, ",")
	fmt.Printf("MAX similarities: %d out of %d \n", len(Common), len(CIDs))
	allmatches := pin.Metadata["allmatches"]
	AllMatches := strings.Split(allmatches, ",")
	var timedownloadchunks, timetorepairchunksonly time.Duration
	ctx, span := trace.StartSpan(spt.ctx, "pintracker/repinFromPeer")
	defer span.End()
	p := pin.Allocations
	f1 := strings.Split(pin.Name, "(")[1]
	f2 := strings.Split(f1, ")")[0]
	or, _ := strconv.Atoi(strings.Split(f2, ",")[0])
	par, _ := strconv.Atoi(strings.Split(f2, ",")[1])
	logger.Debugf("repinning %s from peer %s", pin.Cid, p)
	blacklist := make([]peer.ID, 0)
	//blacklist = append(blacklist, pin.Allocations...)-
	prefix, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		return 0, 0, 0
	}

	hashFunCode, _ := multihash.Names[strings.ToLower("sha2-256")]
	prefix.MhType = hashFunCode
	prefix.MhLength = -1
	//here we want to recreate the missing shard
	cState, err := spt.cons.State(ctx)
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
	Local := true
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
				if slices.Contains(pinn.Allocations, spt.peerID) {
					Local = false
				}
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
			cidss := spt.retrieveCids(pinwm)
			muu.Lock()
			if ret < or {
				ret++
				for j, _ := range cidss {
					repairShards[i].cids = append(repairShards[i].cids, cidss[j].cid)
				}
				wgg.Done()
			} else {
				for j, _ := range cidss {
					repairShards[i].cids = append(repairShards[i].cids, cidss[j].cid)
				}
			}
			muu.Unlock()

		}(pinwm, i)
	}
	wgg.Wait()
	fmt.Printf("Extracting !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! everything took : %s and localllllll is %t \n", time.Now().Sub(ssss).String(), Local)

	if pin.Metadata["allocs"] != spt.peerID.String() {
		Local = false
	}
	//Local
	if Local {
		shh, _ := sharding.NewShard(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions, spt.peerID)
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
		toskip := true
		timerlaunched := false
		Indexes := make([]int, 0)
		ctxx, cancell := context.WithCancel(context.Background())
		for i := 0; i < times; i++ {
			if contains(Common, CIDs[i]) || contains(AllMatches, CIDs[i]) {
				fmt.Printf("Entereddddddd to the have localllll part\n")
				sttt := time.Now()
				bytess := spt.getData(ctxx, CIDs[i])
				st := time.Now().Sub(sttt)
				fmt.Printf("This is the dataaaaaaaaaaaa size : %d \n", len(bytess))
				nodee := ipfsadd.NewFSNodeOverDagC(ft.TFile, prefix)
				nodee.SetFileData(bytess)
				rawnode, _ := nodee.Commit()
				//zid l blacklist heyye list li other pins kamen fiha
				shh.SendBlock(spt.ctx, rawnode)
				size := uint64(len(rawnode.RawData()))
				shh.AddLink(ctx, rawnode.Cid(), size)
				timedownloadchunks += st

			} else {
				retrieved := 0
				reconstructshards := make([][]byte, or+par)
				nbShardsMeta := 0
				readfrom := make([]pinwithmeta, 0)
				for _, shard := range repairShards {
					if len(shard.cids) > 0 {
						nbShardsMeta++
						readfrom = append(readfrom, shard)
					}
				}
				if nbShardsMeta > or {
					//we want to apply the switching every 1 sec
					if !timerlaunched {
						//start the timer that will be responsible of notifying switching
						go startTimerNew5(ctxx, &toskip)
						timerlaunched = true
					}
					if toskip {
						Indexes = make([]int, 0)
						wg.Add(or)
						sttt := time.Now()
						ctxx, cancel := context.WithCancel(context.Background())
						for _, shard := range readfrom {
							if len(shard.cids) > 0 {
								go func(i int, shard pinwithmeta) {
									sss := time.Now()
									bytess := spt.getData(ctxx, shard.cids[i])
									nnn := time.Since(sss)
									fmt.Printf("REPAIR GOT HERE Local FOR shard %d : %s \n", shard.index, nnn.String())
									mu.Lock()
									if retrieved < or {
										retrieved++
										reconstructshards[(shard.index-1)%(or+par)] = bytess
										Indexes = append(Indexes, shard.index)
										mu.Unlock()
										wg.Done()
										if retrieved == or {
											cancel()
										}
									} else {
										cancel()
										mu.Unlock()
									}

								}(i, shard)
							}
						}
						wg.Wait()
						fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
						toskip = false
					} else {
						readfiltered := make([]pinwithmeta, 0)
						for _, shard := range readfrom {
							for _, index := range Indexes {
								if shard.index == index {
									readfiltered = append(readfiltered, shard)
								}
							}
						}
						wg.Add(or)
						sttt := time.Now()
						ctxx, cancel := context.WithCancel(context.Background())
						for _, shard := range readfiltered {
							if len(shard.cids) > 0 {
								go func(i int, shard pinwithmeta) {
									sss := time.Now()
									bytess := spt.getData(ctxx, shard.cids[i])
									nnn := time.Since(sss)
									fmt.Printf("REPAIR GOT HERE Local FOR shard %d : %s \n", shard.index, nnn.String())
									mu.Lock()
									if retrieved < or {
										retrieved++
										reconstructshards[(shard.index-1)%(or+par)] = bytess
										mu.Unlock()
										wg.Done()
										if retrieved == or {
											cancel()
										}
									} else {
										cancel()
										mu.Unlock()
									}

								}(i, shard)
							}
						}
						wg.Wait()
						fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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

				} else {
					//ask for the six out of six because at least we have six shards metadata
					wg.Add(or)
					sttt := time.Now()
					ctxx, cancel := context.WithCancel(context.Background())
					for _, shard := range readfrom {
						if len(shard.cids) > 0 {
							go func(i int, shard pinwithmeta) {
								sss := time.Now()
								bytess := spt.getData(ctxx, shard.cids[i])
								nnn := time.Since(sss)
								fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
								mu.Lock()
								if retrieved < or {
									retrieved++
									reconstructshards[(shard.index-1)%(or+par)] = bytess
									mu.Unlock()
									wg.Done()
									if retrieved == or {
										cancel()
									}
								} else {
									cancel()
									mu.Unlock()
								}

							}(i, shard)
						}
					}
					wg.Wait()
					fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
			}
		}
		wait1 := time.Now()
		wait2 := time.Since(wait1)
		cancell()
		pin.Allocations = make([]peer.ID, 0)
		for _, all := range shh.Allocations() {
			pin.Allocations = append(pin.Allocations, all)
		}
		shh.FlushForStateless(spt.ctx, *pin)
		fmt.Printf("REPAIR TOOK %s \n", time.Now().Sub(start).String())

		return timedownloadchunks, timetorepairchunksonly, wait2
	} else {
		if strategy == "MAXMIN" {
			allocccs := strings.Split(pin.Metadata["allocs"], ",") //metadata of allocs
			for _, pi := range repairShards {
				for _, per := range pi.pin.Allocations {
					blacklist = append(blacklist, per)
				}
			}
			var alloc string
			blacklisted := make(map[string]struct{})

			for _, bl := range blacklist {
				blacklisted[bl.String()] = struct{}{}
			}

			for _, all := range allocccs {
				if _, found := blacklisted[all]; !found {
					alloc = all
					break
				}
			}
			if pin.PinOptions.Metadata == nil {
				pin.PinOptions.Metadata = make(map[string]string)
			}

			pin.PinOptions.Metadata["Black"] = ""
			for _, bl := range blacklist {
				fmt.Printf("BBBBLLLLL : %s \n", bl.String())
				pin.PinOptions.Metadata["Black"] = pin.PinOptions.Metadata["Black"] + "," + bl.String()
			}

			alll, _ := peer.Decode(alloc)
			shh, _ := sharding.NewShard(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions, alll)
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
			toskip := true
			timerlaunched := false
			Indexes := make([]int, 0)
			ctxx, cancell := context.WithCancel(context.Background())
			for i := 0; i < times; i++ {
				if contains(Common, CIDs[i]) || contains(AllMatches, CIDs[i]) {
					fmt.Printf("Entereddddddd to the have localllll part\n")
					sttt := time.Now()
					bytess := spt.getData(ctxx, CIDs[i])
					st := time.Now().Sub(sttt)
					fmt.Printf("This is the dataaaaaaaaaaaa size : %d \n", len(bytess))
					nodee := ipfsadd.NewFSNodeOverDagC(ft.TFile, prefix)
					nodee.SetFileData(bytess)
					rawnode, _ := nodee.Commit()
					//zid l blacklist heyye list li other pins kamen fiha
					shh.SendBlock(spt.ctx, rawnode)
					size := uint64(len(rawnode.RawData()))
					shh.AddLink(ctx, rawnode.Cid(), size)
					timedownloadchunks += st
				} else {
					retrieved := 0
					reconstructshards := make([][]byte, or+par)
					nbShardsMeta := 0
					readfrom := make([]pinwithmeta, 0)
					for _, shard := range repairShards {
						if len(shard.cids) > 0 {
							nbShardsMeta++
							readfrom = append(readfrom, shard)
						}
					}
					if nbShardsMeta > or {
						//we want to apply the switching every 1 sec
						if !timerlaunched {
							//start the timer that will be responsible of notifying switching
							go startTimerNew5(ctxx, &toskip)
							timerlaunched = true
						}
						if toskip {
							Indexes = make([]int, 0)
							wg.Add(or)
							sttt := time.Now()
							ctxx, cancel := context.WithCancel(context.Background())
							for _, shard := range readfrom {
								if len(shard.cids) > 0 {
									go func(i int, shard pinwithmeta) {
										sss := time.Now()
										bytess := spt.getData(ctxx, shard.cids[i])
										nnn := time.Since(sss)
										fmt.Printf("REPAIR GOT HERE Not local FOR shard %d : %s \n", shard.index, nnn.String())
										mu.Lock()
										if retrieved < or {
											retrieved++
											reconstructshards[(shard.index-1)%(or+par)] = bytess
											Indexes = append(Indexes, shard.index)
											mu.Unlock()
											wg.Done()
											if retrieved == or {
												cancel()
											}
										} else {
											cancel()
											mu.Unlock()
										}

									}(i, shard)
								}
							}
							wg.Wait()
							fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
							toskip = false
						} else {
							readfiltered := make([]pinwithmeta, 0)
							for _, shard := range readfrom {
								for _, index := range Indexes {
									if shard.index == index {
										readfiltered = append(readfiltered, shard)
									}
								}
							}
							wg.Add(or)
							sttt := time.Now()
							ctxx, cancel := context.WithCancel(context.Background())
							for _, shard := range readfiltered {
								if len(shard.cids) > 0 {
									go func(i int, shard pinwithmeta) {
										sss := time.Now()
										bytess := spt.getData(ctxx, shard.cids[i])
										nnn := time.Since(sss)
										fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
										mu.Lock()
										if retrieved < or {
											retrieved++
											reconstructshards[(shard.index-1)%(or+par)] = bytess
											mu.Unlock()
											wg.Done()
											if retrieved == or {
												cancel()
											}
										} else {
											cancel()
											mu.Unlock()
										}

									}(i, shard)
								}
							}
							wg.Wait()
							fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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

					} else {
						//ask for the six out of six because at least we have six shards metadata
						wg.Add(or)
						sttt := time.Now()
						ctxx, cancel := context.WithCancel(context.Background())
						for _, shard := range readfrom {
							if len(shard.cids) > 0 {
								go func(i int, shard pinwithmeta) {
									sss := time.Now()
									bytess := spt.getData(ctxx, shard.cids[i])
									nnn := time.Since(sss)
									fmt.Printf("REPAIR GOT HERE Not local,  FOR shard %d : %s \n", shard.index, nnn.String())
									mu.Lock()
									if retrieved < or {
										retrieved++
										reconstructshards[(shard.index-1)%(or+par)] = bytess
										mu.Unlock()
										wg.Done()
										if retrieved == or {
											cancel()
										}
									} else {
										cancel()
										mu.Unlock()
									}

								}(i, shard)
							}
						}
						wg.Wait()
						fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
				}
			}
			wait1 := time.Now()
			pin.Allocations = make([]peer.ID, 0)
			for _, all := range shh.Allocations() {
				pin.Allocations = append(pin.Allocations, all)
			}
			shh.FlushForStateless(spt.ctx, *pin)
			wait2 := time.Since(wait1)
			cancell()
			fmt.Printf("REPAIR TOOK %s \n", time.Now().Sub(start).String())
			return timedownloadchunks, timetorepairchunksonly, wait2

		} else {
			for _, pi := range repairShards {
				for _, per := range pi.pin.Allocations {
					blacklist = append(blacklist, per)
				}
			}
			if pin.PinOptions.Metadata == nil {
				pin.PinOptions.Metadata = make(map[string]string)
			}

			pin.PinOptions.Metadata["Black"] = ""
			for _, bl := range blacklist {
				fmt.Printf("BBBBLLLLL : %s \n", bl.String())
				pin.PinOptions.Metadata["Black"] = pin.PinOptions.Metadata["Black"] + "," + bl.String()
			}

			shh, _ := sharding.NewShards(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions)
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
			toskip := true
			timerlaunched := false
			Indexes := make([]int, 0)
			ctxx, cancell := context.WithCancel(context.Background())
			for i := 0; i < times; i++ {
				if contains(Common, CIDs[i]) || contains(AllMatches, CIDs[i]) {
					fmt.Printf("Entereddddddd to the have localllll part\n")
					sttt := time.Now()
					bytess := spt.getData(ctxx, CIDs[i])
					st := time.Now().Sub(sttt)
					fmt.Printf("This is the dataaaaaaaaaaaa size : %d \n", len(bytess))
					nodee := ipfsadd.NewFSNodeOverDagC(ft.TFile, prefix)
					nodee.SetFileData(bytess)
					rawnode, _ := nodee.Commit()
					//zid l blacklist heyye list li other pins kamen fiha
					shh.SendBlock(spt.ctx, rawnode)
					size := uint64(len(rawnode.RawData()))
					shh.AddLink(ctx, rawnode.Cid(), size)
					timedownloadchunks += st
				} else {
					retrieved := 0
					reconstructshards := make([][]byte, or+par)
					nbShardsMeta := 0
					readfrom := make([]pinwithmeta, 0)
					for _, shard := range repairShards {
						if len(shard.cids) > 0 {
							nbShardsMeta++
							readfrom = append(readfrom, shard)
						}
					}
					if nbShardsMeta > or {
						//we want to apply the switching every 1 sec
						if !timerlaunched {
							//start the timer that will be responsible of notifying switching
							go startTimerNew5(ctxx, &toskip)
							timerlaunched = true
						}
						if toskip {
							Indexes = make([]int, 0)
							wg.Add(or)
							sttt := time.Now()
							ctxx, cancel := context.WithCancel(context.Background())
							for _, shard := range readfrom {
								if len(shard.cids) > 0 {
									go func(i int, shard pinwithmeta) {
										sss := time.Now()
										bytess := spt.getData(ctxx, shard.cids[i])
										nnn := time.Since(sss)
										fmt.Printf("REPAIR GOT HERE Not local FOR shard %d : %s \n", shard.index, nnn.String())
										mu.Lock()
										if retrieved < or {
											retrieved++
											reconstructshards[(shard.index-1)%(or+par)] = bytess
											Indexes = append(Indexes, shard.index)
											mu.Unlock()
											wg.Done()
											if retrieved == or {
												cancel()
											}
										} else {
											cancel()
											mu.Unlock()
										}

									}(i, shard)
								}
							}
							wg.Wait()
							fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
							toskip = false
						} else {
							readfiltered := make([]pinwithmeta, 0)
							for _, shard := range readfrom {
								for _, index := range Indexes {
									if shard.index == index {
										readfiltered = append(readfiltered, shard)
									}
								}
							}
							wg.Add(or)
							sttt := time.Now()
							ctxx, cancel := context.WithCancel(context.Background())
							for _, shard := range readfiltered {
								if len(shard.cids) > 0 {
									go func(i int, shard pinwithmeta) {
										sss := time.Now()
										bytess := spt.getData(ctxx, shard.cids[i])
										nnn := time.Since(sss)
										fmt.Printf("REPAIR GOT HERE FOR shard %d : %s \n", shard.index, nnn.String())
										mu.Lock()
										if retrieved < or {
											retrieved++
											reconstructshards[(shard.index-1)%(or+par)] = bytess
											mu.Unlock()
											wg.Done()
											if retrieved == or {
												cancel()
											}
										} else {
											cancel()
											mu.Unlock()
										}

									}(i, shard)
								}
							}
							wg.Wait()
							fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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

					} else {
						//ask for the six out of six because at least we have six shards metadata
						wg.Add(or)
						sttt := time.Now()
						ctxx, cancel := context.WithCancel(context.Background())
						for _, shard := range readfrom {
							if len(shard.cids) > 0 {
								go func(i int, shard pinwithmeta) {
									sss := time.Now()
									bytess := spt.getData(ctxx, shard.cids[i])
									nnn := time.Since(sss)
									fmt.Printf("REPAIR GOT HERE Not local,  FOR shard %d : %s \n", shard.index, nnn.String())
									mu.Lock()
									if retrieved < or {
										retrieved++
										reconstructshards[(shard.index-1)%(or+par)] = bytess
										mu.Unlock()
										wg.Done()
										if retrieved == or {
											cancel()
										}
									} else {
										cancel()
										mu.Unlock()
									}

								}(i, shard)
							}
						}
						wg.Wait()
						fmt.Printf("REPAIR GOT HERE ENDEDDDD this stripeeeee \n")
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
				}
			}
			wait1 := time.Now()
			pin.Allocations = make([]peer.ID, 0)
			for _, all := range shh.Allocations() {
				pin.Allocations = append(pin.Allocations, all)
			}
			shh.FlushForStateless(spt.ctx, *pin)
			wait2 := time.Since(wait1)
			cancell()
			fmt.Printf("REPAIR TOOK %s \n", time.Now().Sub(start).String())
			return timedownloadchunks, timetorepairchunksonly, wait2
		}

	}
}

// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (spt *ECRepairS) repinUsingRSSelectiveEC(
	pin *api.Pin,
) (
	time.Duration,
	time.Duration,
	time.Duration,
) {
	start := time.Now()

	repairShards := make([]pinwithmeta, 0)

	var timedownloadchunks time.Duration
	var timetorepairchunksonly time.Duration

	// ---------------------------------------------------------------------
	// Validate the received pin and metadata.
	// ---------------------------------------------------------------------

	if pin == nil {
		logger.Errorf(
			"SELECTIVE-EC: received a nil pin",
		)

		return 0, 0, 0
	}

	if pin.Metadata == nil {
		logger.Errorf(
			"SELECTIVE-EC: shard %s has no metadata",
			pin.Cid.String(),
		)

		return 0, 0, 0
	}

	strategy := pin.Metadata["Strategy"]

	if strategy != "SELECTIVE_EC" {
		logger.Errorf(
			"SELECTIVE-EC repair function called with strategy %q",
			strategy,
		)

		return 0, 0, 0
	}

	// Metadata of the failed shard.
	cidString := pin.Metadata["Cids"]
	commonString := pin.Metadata["common"]
	allMatchesString := pin.Metadata["allmatches"]

	CIDs := make([]string, 0)
	Common := make([]string, 0)
	AllMatches := make([]string, 0)

	if strings.TrimSpace(cidString) != "" {
		CIDs = strings.Split(cidString, ",")
	}

	if strings.TrimSpace(commonString) != "" {
		Common = strings.Split(commonString, ",")
	}

	if strings.TrimSpace(allMatchesString) != "" {
		AllMatches = strings.Split(
			allMatchesString,
			",",
		)
	}

	fmt.Printf(
		"SELECTIVE-EC similarities: common=%d allMatches=%d totalChunks=%d\n",
		len(Common),
		len(AllMatches),
		len(CIDs),
	)

	ctx, span := trace.StartSpan(
		spt.ctx,
		"pintracker/repinUsingSelectiveEC",
	)

	defer span.End()

	// ---------------------------------------------------------------------
	// Extract the RS(n,k) parameters from the shard name.
	// ---------------------------------------------------------------------

	openParenthesis := strings.Index(
		pin.Name,
		"(",
	)

	closeParenthesis := strings.Index(
		pin.Name,
		")",
	)

	if openParenthesis == -1 ||
		closeParenthesis == -1 ||
		closeParenthesis <= openParenthesis+1 {

		logger.Errorf(
			"SELECTIVE-EC: cannot extract RS parameters from shard name %q",
			pin.Name,
		)

		return 0, 0, 0
	}

	parametersString := pin.Name[openParenthesis+1 : closeParenthesis]

	parameters := strings.Split(
		parametersString,
		",",
	)

	if len(parameters) != 2 {
		logger.Errorf(
			"SELECTIVE-EC: invalid RS parameter string %q in shard %q",
			parametersString,
			pin.Name,
		)

		return 0, 0, 0
	}

	or, err := strconv.Atoi(
		strings.TrimSpace(parameters[0]),
	)

	if err != nil || or <= 0 {
		logger.Errorf(
			"SELECTIVE-EC: invalid data-shard count %q in shard %q",
			parameters[0],
			pin.Name,
		)

		return 0, 0, 0
	}

	par, err := strconv.Atoi(
		strings.TrimSpace(parameters[1]),
	)

	if err != nil || par < 0 {
		logger.Errorf(
			"SELECTIVE-EC: invalid parity-shard count %q in shard %q",
			parameters[1],
			pin.Name,
		)

		return 0, 0, 0
	}

	totalStripeShards := or + par

	logger.Debugf(
		"SELECTIVE-EC repinning %s with RS(%d,%d)",
		pin.Cid,
		or,
		par,
	)

	// ---------------------------------------------------------------------
	// Parse the zero-based helper indexes selected by the scheduler.
	//
	// Example:
	//
	//     helper_indexes = "0,2,3,5,7,8"
	// ---------------------------------------------------------------------

	helperIndexesString := strings.TrimSpace(
		pin.Metadata["helper_indexes"],
	)

	if helperIndexesString == "" {
		logger.Errorf(
			"SELECTIVE-EC: shard %s has empty helper_indexes metadata",
			pin.Cid.String(),
		)

		return 0, 0, 0
	}

	helperIndexParts := strings.Split(
		helperIndexesString,
		",",
	)

	selectedIndexes := make(
		[]int,
		0,
		len(helperIndexParts),
	)

	selectedIndexSet := make(
		map[int]struct{},
		len(helperIndexParts),
	)

	for _, indexString := range helperIndexParts {
		indexString = strings.TrimSpace(
			indexString,
		)

		if indexString == "" {
			continue
		}

		index, parseErr := strconv.Atoi(
			indexString,
		)

		if parseErr != nil {
			logger.Errorf(
				"SELECTIVE-EC: invalid helper index %q for shard %s: %s",
				indexString,
				pin.Cid.String(),
				parseErr,
			)

			return 0, 0, 0
		}

		if index < 0 ||
			index >= totalStripeShards {

			logger.Errorf(
				"SELECTIVE-EC: helper index %d is outside [0,%d) for shard %s",
				index,
				totalStripeShards,
				pin.Cid.String(),
			)

			return 0, 0, 0
		}

		if _, duplicate := selectedIndexSet[index]; duplicate {
			logger.Errorf(
				"SELECTIVE-EC: duplicate helper index %d for shard %s",
				index,
				pin.Cid.String(),
			)

			return 0, 0, 0
		}

		selectedIndexSet[index] = struct{}{}

		selectedIndexes = append(
			selectedIndexes,
			index,
		)
	}

	sort.Ints(selectedIndexes)

	if len(selectedIndexes) != or {
		logger.Errorf(
			"SELECTIVE-EC: RS(%d,%d) requires exactly %d helpers, "+
				"but helper_indexes contains %d: %v",
			or,
			par,
			or,
			len(selectedIndexes),
			selectedIndexes,
		)

		return 0, 0, 0
	}

	fmt.Printf(
		"SELECTIVE-EC selected helper indexes: %v\n",
		selectedIndexes,
	)

	// ---------------------------------------------------------------------
	// Prepare CID construction.
	// ---------------------------------------------------------------------

	prefix, err := merkledag.PrefixForCidVersion(0)

	if err != nil {
		logger.Errorf(
			"SELECTIVE-EC: cannot create CID prefix: %s",
			err,
		)

		return 0, 0, 0
	}

	hashFunCode, found := multihash.Names[strings.ToLower("sha2-256")]

	if !found {
		logger.Errorf(
			"SELECTIVE-EC: sha2-256 multihash code was not found",
		)

		return 0, 0, 0
	}

	prefix.MhType = hashFunCode
	prefix.MhLength = -1

	// ---------------------------------------------------------------------
	// Get the failed shard number and its zero-based RS position.
	// ---------------------------------------------------------------------

	fmt.Fprintf(
		os.Stdout,
		"getShardNumber of pin named: %s\n",
		pin.Name,
	)

	failedShardNumber,
		fileName,
		err := getShardNumber(pin.Name)

	if err != nil {
		logger.Errorf(
			"SELECTIVE-EC: cannot extract shard number from %q: %s",
			pin.Name,
			err,
		)

		return 0, 0, 0
	}

	if failedShardNumber <= 0 {
		logger.Errorf(
			"SELECTIVE-EC: invalid failed shard number %d",
			failedShardNumber,
		)

		return 0, 0, 0
	}

	// Zero-based position of the missing shard in the RS matrix.
	tosend := (failedShardNumber - 1) %
		totalStripeShards

	if _, selectedMissingShard := selectedIndexSet[tosend]; selectedMissingShard {

		logger.Errorf(
			"SELECTIVE-EC: failed shard index %d was incorrectly selected as a helper",
			tosend,
		)

		return 0, 0, 0
	}

	// Determine the global shard-number boundaries of this stripe.
	before := (failedShardNumber - 1) %
		totalStripeShards

	stripeStart := failedShardNumber - before

	stripeEnd := stripeStart +
		totalStripeShards -
		1

	fmt.Printf(
		"SELECTIVE-EC failedShardNumber=%d missingIndex=%d stripe=[%d,%d]\n",
		failedShardNumber,
		tosend,
		stripeStart,
		stripeEnd,
	)

	// ---------------------------------------------------------------------
	// Read the cluster state.
	// ---------------------------------------------------------------------

	cState, err := spt.cons.State(ctx)

	if err != nil {
		logger.Warn(err)
		return 0, 0, 0
	}

	pinCh := make(
		chan api.Pin,
		1024,
	)

	go func() {
		listErr := cState.List(
			spt.ctx,
			pinCh,
		)

		if listErr != nil {
			logger.Warn(listErr)
		}
	}()

	// ---------------------------------------------------------------------
	// Find only the shards selected by the scheduler.
	//
	// We do not collect all n+k-1 surviving shards.
	// We collect exactly the n indexes in helper_indexes.
	// ---------------------------------------------------------------------

	foundIndexSet := make(
		map[int]struct{},
		or,
	)

	for survivingPin := range pinCh {
		if !strings.Contains(
			survivingPin.Name,
			"-shard-",
		) {
			continue
		}

		survivingShardNumber,
			survivingFileName,
			parseErr := getShardNumber(
			survivingPin.Name,
		)

		if parseErr != nil {
			fmt.Printf(
				"SELECTIVE-EC: cannot parse shard name %q: %s\n",
				survivingPin.Name,
				parseErr,
			)

			continue
		}

		if survivingFileName != fileName {
			continue
		}

		if survivingShardNumber < stripeStart ||
			survivingShardNumber > stripeEnd {

			continue
		}

		if survivingShardNumber ==
			failedShardNumber {

			continue
		}

		stripeIndex := (survivingShardNumber - 1) %
			totalStripeShards

		if _, selected := selectedIndexSet[stripeIndex]; !selected {

			continue
		}

		if _, duplicate := foundIndexSet[stripeIndex]; duplicate {

			logger.Errorf(
				"SELECTIVE-EC: multiple shard pins were found for selected index %d",
				stripeIndex,
			)

			return 0, 0, 0
		}

		foundIndexSet[stripeIndex] = struct{}{}

		/*
			The index stored in pinwithmeta is now already the
			zero-based RS index.

			Therefore later we use:

			    reconstructShards[shard.index] = data

			and not:

			    reconstructShards[(shard.index-1)%(or+par)]
		*/
		repairShards = append(
			repairShards,
			pinwithmeta{
				pin:   survivingPin,
				index: stripeIndex,
				cids:  make([]string, 0),
			},
		)

		fmt.Printf(
			"SELECTIVE-EC selected shard name=%s globalNumber=%d index=%d\n",
			survivingPin.Name,
			survivingShardNumber,
			stripeIndex,
		)
	}

	sort.Slice(
		repairShards,
		func(i int, j int) bool {
			return repairShards[i].index <
				repairShards[j].index
		},
	)

	if len(repairShards) != or {
		foundIndexes := make(
			[]int,
			0,
			len(repairShards),
		)

		for _, repairShard := range repairShards {
			foundIndexes = append(
				foundIndexes,
				repairShard.index,
			)
		}

		logger.Errorf(
			"SELECTIVE-EC: found %d/%d selected helper shards; "+
				"requested=%v found=%v",
			len(repairShards),
			or,
			selectedIndexes,
			foundIndexes,
		)

		return 0, 0, 0
	}

	// ---------------------------------------------------------------------
	// Optional defensive validation.
	//
	// The scheduler should guarantee this condition. The repair always uses
	// the local branch, but this check detects scheduling/placement mistakes.
	// ---------------------------------------------------------------------

	for _, repairShard := range repairShards {
		if slices.Contains(
			repairShard.pin.Allocations,
			spt.peerID,
		) {
			logger.Errorf(
				"SELECTIVE-EC invariant violation: repair peer %s "+
					"already stores selected stripe index %d",
				spt.peerID.String(),
				repairShard.index,
			)

			return 0, 0, 0
		}
	}

	fmt.Printf(
		"SELECTIVE-EC scheduler invariant verified: " +
			"repair peer does not contain a surviving stripe shard\n",
	)

	// ---------------------------------------------------------------------
	// Retrieve CID lists only for the selected n shards.
	// ---------------------------------------------------------------------

	type cidMetadataResult struct {
		position int
		cids     []string
	}

	metadataResults := make(
		chan cidMetadataResult,
		len(repairShards),
	)

	var metadataWG sync.WaitGroup

	metadataWG.Add(
		len(repairShards),
	)

	for position, repairShard := range repairShards {
		positionCopy := position
		shardCopy := repairShard

		go func() {
			defer metadataWG.Done()

			retrievedCIDs := spt.retrieveCids(
				shardCopy,
			)

			cids := make(
				[]string,
				0,
				len(retrievedCIDs),
			)

			for _, retrievedCID := range retrievedCIDs {
				cids = append(
					cids,
					retrievedCID.cid,
				)
			}

			metadataResults <- cidMetadataResult{
				position: positionCopy,
				cids:     cids,
			}
		}()
	}

	metadataWG.Wait()
	close(metadataResults)

	for result := range metadataResults {
		repairShards[result.position].cids =
			result.cids
	}

	for _, repairShard := range repairShards {
		if len(repairShard.cids) == 0 {
			logger.Errorf(
				"SELECTIVE-EC: no CID metadata was found for selected index %d",
				repairShard.index,
			)

			return 0, 0, 0
		}
	}

	times := len(
		repairShards[0].cids,
	)

	if times == 0 {
		logger.Errorf(
			"SELECTIVE-EC: selected shard metadata contains no chunks",
		)

		return 0, 0, 0
	}

	for _, repairShard := range repairShards {
		if len(repairShard.cids) != times {
			logger.Errorf(
				"SELECTIVE-EC: selected index %d has %d CIDs, expected %d",
				repairShard.index,
				len(repairShard.cids),
				times,
			)

			return 0, 0, 0
		}
	}

	if len(CIDs) < times {
		logger.Errorf(
			"SELECTIVE-EC: failed shard metadata contains %d CIDs, "+
				"but selected helpers contain %d",
			len(CIDs),
			times,
		)

		return 0, 0, 0
	}

	fmt.Printf(
		"SELECTIVE-EC CID metadata extraction completed: "+
			"helpers=%d chunksPerShard=%d duration=%s\n",
		len(repairShards),
		times,
		time.Since(start).String(),
	)

	// ---------------------------------------------------------------------
	// Always reconstruct locally on the peer selected by the scheduler.
	// ---------------------------------------------------------------------

	shh, err := sharding.NewShard(
		spt.ctx,
		spt.ctx,
		spt.rpcClient,
		pin.PinOptions,
		spt.peerID,
	)

	if err != nil {
		logger.Errorf(
			"SELECTIVE-EC: cannot create the reconstructed shard: %s",
			err,
		)

		return 0, 0, 0
	}

	enc, err := reedsolomon.New(
		or,
		par,
	)

	if err != nil {
		logger.Errorf(
			"SELECTIVE-EC: cannot create RS(%d,%d) encoder: %s",
			or,
			par,
			err,
		)

		return 0, 0, 0
	}

	downloadContext, cancelDownloads :=
		context.WithCancel(spt.ctx)

	defer cancelDownloads()

	// ---------------------------------------------------------------------
	// Reconstruct every chunk in the failed shard.
	// ---------------------------------------------------------------------

	for chunkPosition := 0; chunkPosition < times; chunkPosition++ {

		/*
			If this exact chunk already exists locally because of
			deduplication/similarity, no RS reconstruction is needed.
		*/
		if contains(
			Common,
			CIDs[chunkPosition],
		) ||
			contains(
				AllMatches,
				CIDs[chunkPosition],
			) {

			localStart := time.Now()

			data := spt.getData(
				downloadContext,
				CIDs[chunkPosition],
			)

			localDuration := time.Since(
				localStart,
			)

			timedownloadchunks +=
				localDuration

			if len(data) == 0 {
				logger.Errorf(
					"SELECTIVE-EC: locally available CID %s returned empty data",
					CIDs[chunkPosition],
				)

				return 0, 0, 0
			}

			node := ipfsadd.NewFSNodeOverDagC(
				ft.TFile,
				prefix,
			)

			node.SetFileData(data)

			rawNode, commitErr := node.Commit()

			if commitErr != nil {
				logger.Errorf(
					"SELECTIVE-EC: cannot commit local chunk %d: %s",
					chunkPosition,
					commitErr,
				)

				return 0, 0, 0
			}

			shh.SendBlock(
				spt.ctx,
				rawNode,
			)

			size := uint64(
				len(rawNode.RawData()),
			)

			shh.AddLink(
				ctx,
				rawNode.Cid(),
				size,
			)

			fmt.Printf(
				"SELECTIVE-EC chunk=%d reused locally duration=%s size=%d\n",
				chunkPosition,
				localDuration.String(),
				len(data),
			)

			continue
		}

		// -------------------------------------------------------------
		// Download exactly n chunks: one from every selected RS index.
		// -------------------------------------------------------------

		reconstructShards := make(
			[][]byte,
			totalStripeShards,
		)

		type downloadResult struct {
			stripeIndex int
			data        []byte
			duration    time.Duration
			err         error
		}

		downloadResults := make(
			chan downloadResult,
			len(repairShards),
		)

		var downloadWG sync.WaitGroup

		downloadWG.Add(
			len(repairShards),
		)

		parallelDownloadStart := time.Now()

		for _, repairShard := range repairShards {
			shardCopy := repairShard
			chunkPositionCopy := chunkPosition

			go func() {
				defer downloadWG.Done()

				if chunkPositionCopy >=
					len(shardCopy.cids) {

					downloadResults <- downloadResult{
						stripeIndex: shardCopy.index,

						err: fmt.Errorf(
							"chunk position %d is unavailable "+
								"for selected index %d",
							chunkPositionCopy,
							shardCopy.index,
						),
					}

					return
				}

				chunkCID :=
					shardCopy.cids[chunkPositionCopy]

				oneDownloadStart := time.Now()

				data := spt.getData(
					downloadContext,
					chunkCID,
				)

				oneDownloadDuration :=
					time.Since(oneDownloadStart)

				if len(data) == 0 {
					downloadResults <- downloadResult{
						stripeIndex: shardCopy.index,
						duration:    oneDownloadDuration,

						err: fmt.Errorf(
							"empty data returned for CID %s "+
								"from selected index %d",
							chunkCID,
							shardCopy.index,
						),
					}

					return
				}

				downloadResults <- downloadResult{
					stripeIndex: shardCopy.index,
					data:        data,
					duration:    oneDownloadDuration,
				}
			}()
		}

		downloadWG.Wait()
		close(downloadResults)

		parallelDownloadDuration :=
			time.Since(parallelDownloadStart)

		timedownloadchunks +=
			parallelDownloadDuration

		successfulDownloads := 0

		for result := range downloadResults {
			if result.err != nil {
				logger.Errorf(
					"SELECTIVE-EC: helper download failed: %s",
					result.err,
				)

				return 0, 0, 0
			}

			if result.stripeIndex < 0 ||
				result.stripeIndex >=
					len(reconstructShards) {

				logger.Errorf(
					"SELECTIVE-EC: invalid returned RS index %d",
					result.stripeIndex,
				)

				return 0, 0, 0
			}

			if reconstructShards[result.stripeIndex] != nil {
				logger.Errorf(
					"SELECTIVE-EC: duplicate downloaded data for RS index %d",
					result.stripeIndex,
				)

				return 0, 0, 0
			}

			/*
				This is the important RS-matrix arrangement:

				    reconstructShards[RS index] = data
			*/
			reconstructShards[result.stripeIndex] =
				result.data

			successfulDownloads++

			fmt.Printf(
				"SELECTIVE-EC downloaded chunk=%d "+
					"fromIndex=%d duration=%s size=%d\n",
				chunkPosition,
				result.stripeIndex,
				result.duration.String(),
				len(result.data),
			)
		}

		if successfulDownloads != or {
			logger.Errorf(
				"SELECTIVE-EC: downloaded %d/%d required chunks "+
					"for chunk position %d",
				successfulDownloads,
				or,
				chunkPosition,
			)

			return 0, 0, 0
		}

		// The missing index must remain nil before reconstruction.
		if reconstructShards[tosend] != nil {
			logger.Errorf(
				"SELECTIVE-EC: missing index %d was populated before reconstruction",
				tosend,
			)

			return 0, 0, 0
		}

		// -------------------------------------------------------------
		// Reconstruct the missing chunk.
		// -------------------------------------------------------------

		reconstructionStart := time.Now()

		reconstructionErr := enc.Reconstruct(
			reconstructShards,
		)

		reconstructionDuration :=
			time.Since(reconstructionStart)

		timetorepairchunksonly +=
			reconstructionDuration

		if reconstructionErr != nil {
			logger.Errorf(
				"SELECTIVE-EC: reconstruction failed for chunk %d: %s",
				chunkPosition,
				reconstructionErr,
			)

			return 0, 0, 0
		}

		if len(reconstructShards[tosend]) == 0 {
			logger.Errorf(
				"SELECTIVE-EC: reconstructed chunk %d at index %d is empty",
				chunkPosition,
				tosend,
			)

			return 0, 0, 0
		}

		node := ipfsadd.NewFSNodeOverDagC(
			ft.TFile,
			prefix,
		)

		node.SetFileData(
			reconstructShards[tosend],
		)

		rawNode, commitErr := node.Commit()

		if commitErr != nil {
			logger.Errorf(
				"SELECTIVE-EC: cannot commit reconstructed chunk %d: %s",
				chunkPosition,
				commitErr,
			)

			return 0, 0, 0
		}

		shh.SendBlock(
			spt.ctx,
			rawNode,
		)

		size := uint64(
			len(rawNode.RawData()),
		)

		shh.AddLink(
			ctx,
			rawNode.Cid(),
			size,
		)

		fmt.Printf(
			"SELECTIVE-EC reconstructed chunk=%d "+
				"missingIndex=%d downloads=%d "+
				"downloadTime=%s reconstructionTime=%s\n",
			chunkPosition,
			tosend,
			successfulDownloads,
			parallelDownloadDuration.String(),
			reconstructionDuration.String(),
		)
	}

	// ---------------------------------------------------------------------
	// Store and flush the newly reconstructed shard.
	// ---------------------------------------------------------------------

	flushStart := time.Now()

	pin.Allocations = make(
		[]peer.ID,
		0,
	)

	for _, allocation := range shh.Allocations() {
		pin.Allocations = append(
			pin.Allocations,
			allocation,
		)
	}

	shh.FlushForStateless(
		spt.ctx,
		*pin,
	)

	flushDuration := time.Since(
		flushStart,
	)

	fmt.Printf(
		"SELECTIVE-EC REPAIR COMPLETED "+
			"shard=%s missingIndex=%d helpers=%v "+
			"total=%s download=%s reconstruction=%s flush=%s\n",
		pin.Name,
		tosend,
		selectedIndexes,
		time.Since(start).String(),
		timedownloadchunks.String(),
		timetorepairchunksonly.String(),
		flushDuration.String(),
	)

	return timedownloadchunks,
		timetorepairchunksonly,
		flushDuration
}

func (spt *ECRepairS) repinUsingRSASCLEPIUS(
	pin *api.Pin,
) (time.Duration, time.Duration, time.Duration) {

	start := time.Now()

	var timedownloadchunks time.Duration
	var timetorepairchunksonly time.Duration

	ctx, span := trace.StartSpan(
		spt.ctx,
		"pintracker/repinUsingRSASCLEPIUS",
	)
	defer span.End()

	// ---------------------------------------------------------------------
	// Read RS parameters from the failed shard name.
	// ---------------------------------------------------------------------

	f1 := strings.Split(pin.Name, "(")
	if len(f1) < 2 {
		logger.Errorf(
			"ASCLEPIUS: cannot extract RS parameters from shard name %s",
			pin.Name,
		)
		return 0, 0, 0
	}

	f2 := strings.Split(f1[1], ")")
	if len(f2) < 1 {
		logger.Errorf(
			"ASCLEPIUS: cannot extract RS parameters from shard name %s",
			pin.Name,
		)
		return 0, 0, 0
	}

	rsParts := strings.Split(f2[0], ",")
	if len(rsParts) != 2 {
		logger.Errorf(
			"ASCLEPIUS: invalid RS parameters in shard name %s",
			pin.Name,
		)
		return 0, 0, 0
	}

	or, err := strconv.Atoi(
		strings.TrimSpace(rsParts[0]),
	)
	if err != nil || or <= 0 {
		logger.Errorf(
			"ASCLEPIUS: invalid original shard count in %s",
			pin.Name,
		)
		return 0, 0, 0
	}

	par, err := strconv.Atoi(
		strings.TrimSpace(rsParts[1]),
	)
	if err != nil || par <= 0 {
		logger.Errorf(
			"ASCLEPIUS: invalid parity shard count in %s",
			pin.Name,
		)
		return 0, 0, 0
	}

	totalStripeShards := or + par

	enc, err := reedsolomon.New(or, par)
	if err != nil {
		logger.Errorf(
			"ASCLEPIUS: cannot create RS encoder: %s",
			err,
		)
		return 0, 0, 0
	}

	// ---------------------------------------------------------------------
	// Determine the failed global shard number and its RS matrix position.
	// ---------------------------------------------------------------------

	failedGlobalShardNumber, _, err :=
		getShardNumber(pin.Name)

	if err != nil {
		logger.Errorf(
			"ASCLEPIUS: cannot extract failed shard number from %s: %s",
			pin.Name,
			err,
		)
		return 0, 0, 0
	}

	if failedGlobalShardNumber <= 0 {
		logger.Errorf(
			"ASCLEPIUS: invalid failed global shard number %d",
			failedGlobalShardNumber,
		)
		return 0, 0, 0
	}

	failedStripe :=
		(failedGlobalShardNumber - 1) /
			totalStripeShards

	tosend :=
		(failedGlobalShardNumber - 1) %
			totalStripeShards

	fmt.Printf(
		"ASCLEPIUS failedShard=%d stripe=%d missingRSIndex=%d\n",
		failedGlobalShardNumber,
		failedStripe,
		tosend,
	)

	// ---------------------------------------------------------------------
	// Parse the scheduler-selected destination.
	//
	// allocs is empty when repairPeer == finalPeer.
	// In that case the repaired shard stays on this peer.
	// ---------------------------------------------------------------------

	destinationPeer := spt.peerID

	destinationString :=
		strings.TrimSpace(pin.Metadata["allocs"])

	if destinationString != "" {
		destinationPeer, err =
			peer.Decode(destinationString)

		if err != nil {
			logger.Errorf(
				"ASCLEPIUS: invalid destination peer %q: %s",
				destinationString,
				err,
			)
			return 0, 0, 0
		}
	}

	fmt.Printf(
		"ASCLEPIUS repairPeer=%s destinationPeer=%s relocated=%v\n",
		spt.peerID.String(),
		destinationPeer.String(),
		destinationPeer != spt.peerID,
	)

	// ---------------------------------------------------------------------
	// Parse helpers metadata.
	//
	// Expected format:
	//
	// helpers=peerID:globalShardNumber,peerID:globalShardNumber,...
	// ---------------------------------------------------------------------

	type selectedHelper struct {
		Peer              peer.ID
		GlobalShardNumber int
		RSIndex           int

		Pin  api.Pin
		CIDs []string
	}

	helpersMetadata :=
		strings.TrimSpace(pin.Metadata["helpers"])

	if helpersMetadata == "" {
		logger.Errorf(
			"ASCLEPIUS: helpers metadata is empty for shard %s",
			pin.Name,
		)
		return 0, 0, 0
	}

	helperEntries :=
		strings.Split(helpersMetadata, ",")

	selectedHelpers := make(
		[]selectedHelper,
		0,
		len(helperEntries),
	)

	seenPeer := make(map[peer.ID]bool)
	seenRSIndex := make(map[int]bool)

	for _, rawEntry := range helperEntries {
		entry := strings.TrimSpace(rawEntry)
		if entry == "" {
			continue
		}

		separator := strings.LastIndex(
			entry,
			":",
		)

		if separator <= 0 ||
			separator >= len(entry)-1 {

			logger.Errorf(
				"ASCLEPIUS: invalid helper entry %q; "+
					"expected peerID:globalShardNumber",
				entry,
			)
			return 0, 0, 0
		}

		helperPeerString :=
			strings.TrimSpace(entry[:separator])

		globalNumberString :=
			strings.TrimSpace(entry[separator+1:])

		helperPeer, decodeErr :=
			peer.Decode(helperPeerString)

		if decodeErr != nil {
			logger.Errorf(
				"ASCLEPIUS: invalid helper peer %q: %s",
				helperPeerString,
				decodeErr,
			)
			return 0, 0, 0
		}

		globalShardNumber, numberErr :=
			strconv.Atoi(globalNumberString)

		if numberErr != nil ||
			globalShardNumber <= 0 {

			logger.Errorf(
				"ASCLEPIUS: invalid global shard number %q "+
					"for helper %s",
				globalNumberString,
				helperPeer.String(),
			)
			return 0, 0, 0
		}

		helperStripe :=
			(globalShardNumber - 1) /
				totalStripeShards

		if helperStripe != failedStripe {
			logger.Errorf(
				"ASCLEPIUS: helper %s globalShard=%d "+
					"belongs to stripe=%d, expected stripe=%d",
				helperPeer.String(),
				globalShardNumber,
				helperStripe,
				failedStripe,
			)
			return 0, 0, 0
		}

		rsIndex :=
			(globalShardNumber - 1) %
				totalStripeShards

		if rsIndex == tosend {
			logger.Errorf(
				"ASCLEPIUS: helper %s points to missing "+
					"RS index %d",
				helperPeer.String(),
				rsIndex,
			)
			return 0, 0, 0
		}

		if seenPeer[helperPeer] {
			logger.Errorf(
				"ASCLEPIUS: duplicate helper peer %s",
				helperPeer.String(),
			)
			return 0, 0, 0
		}

		if seenRSIndex[rsIndex] {
			logger.Errorf(
				"ASCLEPIUS: duplicate RS index %d "+
					"after modulo conversion",
				rsIndex,
			)
			return 0, 0, 0
		}

		seenPeer[helperPeer] = true
		seenRSIndex[rsIndex] = true

		selectedHelpers = append(
			selectedHelpers,
			selectedHelper{
				Peer:              helperPeer,
				GlobalShardNumber: globalShardNumber,
				RSIndex:           rsIndex,
			},
		)
	}

	if len(selectedHelpers) != or {
		logger.Errorf(
			"ASCLEPIUS: expected exactly %d EC helpers, got %d",
			or,
			len(selectedHelpers),
		)
		return 0, 0, 0
	}

	sort.Slice(
		selectedHelpers,
		func(i, j int) bool {
			return selectedHelpers[i].RSIndex <
				selectedHelpers[j].RSIndex
		},
	)

	fmt.Printf(
		"ASCLEPIUS fixed helper indexes for all repaired chunks: ",
	)

	for i, helper := range selectedHelpers {
		if i > 0 {
			fmt.Print(",")
		}

		fmt.Printf(
			"peer=%s:globalShard=%d:rsIndex=%d",
			helper.Peer.String(),
			helper.GlobalShardNumber,
			helper.RSIndex,
		)
	}

	fmt.Println()

	// ---------------------------------------------------------------------
	// Read the selected shard pins from consensus.
	//
	// We locate shards using the exact global numbers returned by the
	// scheduler. Helper peer IDs are also validated against allocations.
	// ---------------------------------------------------------------------

	cState, err := spt.cons.State(spt.ctx)
	if err != nil {
		logger.Errorf(
			"ASCLEPIUS: cannot read consensus state: %s",
			err,
		)
		return 0, 0, 0
	}

	pinCh := make(chan api.Pin, 1024)

	go func() {
		listErr := cState.List(ctx, pinCh)
		if listErr != nil {
			logger.Errorf(
				"ASCLEPIUS: cannot list consensus state: %s",
				listErr,
			)
		}
	}()

	type helperLookupKey struct {
		GlobalShardNumber int
	}

	helperByGlobalNumber := make(
		map[int]int,
		len(selectedHelpers),
	)

	for i := range selectedHelpers {
		helperByGlobalNumber[selectedHelpers[i].GlobalShardNumber] = i
	}

	foundHelpers := 0

	for candidatePin := range pinCh {
		if !strings.Contains(
			candidatePin.Name,
			"-shard-",
		) {
			continue
		}

		candidateNumber, candidateBaseName, numberErr :=
			getShardNumber(candidatePin.Name)

		if numberErr != nil {
			continue
		}

		helperPosition, needed :=
			helperByGlobalNumber[candidateNumber]

		if !needed {
			continue
		}

		_, failedBaseName, failedNameErr :=
			getShardNumber(pin.Name)

		if failedNameErr != nil ||
			candidateBaseName != failedBaseName {

			continue
		}

		expectedPeer :=
			selectedHelpers[helperPosition].Peer

		if !slices.Contains(
			candidatePin.Allocations,
			expectedPeer,
		) {
			logger.Errorf(
				"ASCLEPIUS: selected peer %s does not allocate "+
					"helper shard %s",
				expectedPeer.String(),
				candidatePin.Name,
			)
			return 0, 0, 0
		}

		selectedHelpers[helperPosition].Pin =
			candidatePin

		helperPinWithMeta := pinwithmeta{
			pin:   candidatePin,
			index: candidateNumber,
			cids:  make([]string, 0),
		}

		chunks :=
			spt.retrieveCids(helperPinWithMeta)

		for _, chunk := range chunks {
			selectedHelpers[helperPosition].CIDs =
				append(
					selectedHelpers[helperPosition].CIDs,
					chunk.cid,
				)
		}

		foundHelpers++

		delete(
			helperByGlobalNumber,
			candidateNumber,
		)

		if foundHelpers == len(selectedHelpers) {
			break
		}
	}

	if foundHelpers != len(selectedHelpers) {
		logger.Errorf(
			"ASCLEPIUS: found metadata for %d/%d selected helpers",
			foundHelpers,
			len(selectedHelpers),
		)
		return 0, 0, 0
	}

	// ---------------------------------------------------------------------
	// Determine the common reconstructable chunk range.
	//
	// The scheduler selects only the fixed helper shard indexes. The repair
	// model reuses these same helpers for every chunk position. Therefore,
	// helper selection is not repeated per chunk and no per-helper usage
	// amount is read from metadata.
	//
	// We use the minimum CID count across the selected helper shards because
	// every reconstructed position must exist in every selected helper shard.
	// ---------------------------------------------------------------------

	chunkCount := -1

	for _, helper := range selectedHelpers {
		if len(helper.CIDs) == 0 {
			logger.Errorf(
				"ASCLEPIUS: selected helper %s shard=%d has no CIDs",
				helper.Peer.String(),
				helper.GlobalShardNumber,
			)
			return 0, 0, 0
		}

		if chunkCount == -1 || len(helper.CIDs) < chunkCount {
			chunkCount = len(helper.CIDs)
		}
	}

	if chunkCount <= 0 {
		logger.Errorf(
			"ASCLEPIUS: selected helper indexes expose no common chunks",
		)
		return 0, 0, 0
	}

	// ---------------------------------------------------------------------
	// Read the failed shard CID list.
	// ---------------------------------------------------------------------

	failedCIDsRaw :=
		strings.Split(pin.Metadata["Cids"], ",")

	failedCIDs := make(
		[]string,
		0,
		len(failedCIDsRaw),
	)

	for _, rawCID := range failedCIDsRaw {
		cleaned := strings.TrimSpace(
			strings.Trim(rawCID, "<>"),
		)

		if cleaned != "" {
			failedCIDs = append(
				failedCIDs,
				cleaned,
			)
		}
	}

	if len(failedCIDs) == 0 {
		logger.Errorf(
			"ASCLEPIUS: failed shard %s contains no CIDs metadata",
			pin.Name,
		)
		return 0, 0, 0
	}

	// The failed shard metadata may contain more entries than the common
	// reconstructable helper range (for example, an additional metadata/root
	// entry). The fixed helper indexes are used for all positions available
	// across the selected helper shards.
	//
	// A failed shard with fewer entries than the helpers is invalid because
	// there is no target CID for some helper positions.
	if len(failedCIDs) < chunkCount {
		logger.Errorf(
			"ASCLEPIUS: failed shard has only %d target CIDs, "+
				"but selected helper indexes expose %d chunks",
			len(failedCIDs),
			chunkCount,
		)
		return 0, 0, 0
	}

	if len(failedCIDs) > chunkCount {
		logger.Warnf(
			"ASCLEPIUS: failed shard metadata has %d CIDs while fixed "+
				"helper indexes expose %d reconstructable chunks; "+
				"repairing the common %d chunk positions",
			len(failedCIDs),
			chunkCount,
			chunkCount,
		)
	}

	// ---------------------------------------------------------------------
	// Create the destination shard directly.
	//
	// When destinationPeer == spt.peerID, this stores locally.
	// Otherwise, NewShard sends generated blocks directly to the scheduler's
	// selected destination peer.
	// ---------------------------------------------------------------------

	if pin.PinOptions.Metadata == nil {
		pin.PinOptions.Metadata =
			make(map[string]string)
	}

	shh, err := sharding.NewShard(
		spt.ctx,
		spt.ctx,
		spt.rpcClient,
		pin.PinOptions,
		destinationPeer,
	)

	if err != nil {
		logger.Errorf(
			"ASCLEPIUS: cannot create output shard for destination %s: %s",
			destinationPeer.String(),
			err,
		)
		return 0, 0, 0
	}

	prefix, err :=
		merkledag.PrefixForCidVersion(0)

	if err != nil {
		logger.Errorf(
			"ASCLEPIUS: cannot create CID prefix: %s",
			err,
		)
		return 0, 0, 0
	}

	hashFunCode, exists :=
		multihash.Names[strings.ToLower("sha2-256")]

	if !exists {
		logger.Errorf(
			"ASCLEPIUS: sha2-256 multihash code is unavailable",
		)
		return 0, 0, 0
	}

	prefix.MhType = hashFunCode
	prefix.MhLength = -1

	downloadContext, cancelDownloads :=
		context.WithCancel(spt.ctx)

	defer cancelDownloads()

	// ---------------------------------------------------------------------
	// This section is intentionally almost identical to SelectiveEC.
	// ---------------------------------------------------------------------

	for chunkPosition := 0; chunkPosition < chunkCount; chunkPosition++ {

		targetCID, decodeErr :=
			cid.Decode(failedCIDs[chunkPosition])

		if decodeErr != nil {
			logger.Errorf(
				"ASCLEPIUS: invalid target CID at chunk %d: %s",
				chunkPosition,
				decodeErr,
			)
			return 0, 0, 0
		}

		// Reuse the chunk locally when already available.
		existsLocally, localErr :=
			spt.connector.BlockLocalHas(
				spt.ctx,
				targetCID,
			)

		if localErr == nil && existsLocally {
			localStart := time.Now()

			data := spt.getData(
				downloadContext,
				targetCID.String(),
			)

			localDuration :=
				time.Since(localStart)

			if len(data) == 0 {
				logger.Errorf(
					"ASCLEPIUS: locally available chunk %d is empty",
					chunkPosition,
				)
				return 0, 0, 0
			}

			node :=
				ipfsadd.NewFSNodeOverDagC(
					ft.TFile,
					prefix,
				)

			node.SetFileData(data)

			rawNode, commitErr :=
				node.Commit()

			if commitErr != nil {
				logger.Errorf(
					"ASCLEPIUS: cannot commit local chunk %d: %s",
					chunkPosition,
					commitErr,
				)
				return 0, 0, 0
			}

			shh.SendBlock(
				spt.ctx,
				rawNode,
			)

			size := uint64(
				len(rawNode.RawData()),
			)

			shh.AddLink(
				ctx,
				rawNode.Cid(),
				size,
			)

			timedownloadchunks +=
				localDuration

			continue
		}

		reconstructShards := make(
			[][]byte,
			totalStripeShards,
		)

		type downloadResult struct {
			rsIndex  int
			peer     peer.ID
			data     []byte
			duration time.Duration
			err      error
		}

		results := make(
			chan downloadResult,
			len(selectedHelpers),
		)

		var downloadWG sync.WaitGroup

		downloadWG.Add(
			len(selectedHelpers),
		)

		parallelDownloadStart :=
			time.Now()

		for _, selected := range selectedHelpers {
			helper := selected

			go func() {
				defer downloadWG.Done()

				if chunkPosition >= len(helper.CIDs) {
					results <- downloadResult{
						rsIndex: helper.RSIndex,
						peer:    helper.Peer,
						err: fmt.Errorf(
							"chunk %d unavailable for helper shard %d",
							chunkPosition,
							helper.GlobalShardNumber,
						),
					}
					return
				}

				helperCID :=
					helper.CIDs[chunkPosition]

				oneStart := time.Now()

				data := spt.getData(
					downloadContext,
					helperCID,
				)

				duration :=
					time.Since(oneStart)

				if len(data) == 0 {
					results <- downloadResult{
						rsIndex:  helper.RSIndex,
						peer:     helper.Peer,
						duration: duration,
						err: fmt.Errorf(
							"empty data for CID %s from helper %s",
							helperCID,
							helper.Peer.String(),
						),
					}
					return
				}

				results <- downloadResult{
					rsIndex:  helper.RSIndex,
					peer:     helper.Peer,
					data:     data,
					duration: duration,
				}
			}()
		}

		downloadWG.Wait()
		close(results)

		parallelDownloadDuration :=
			time.Since(parallelDownloadStart)

		timedownloadchunks +=
			parallelDownloadDuration

		successfulDownloads := 0

		for result := range results {
			if result.err != nil {
				logger.Errorf(
					"ASCLEPIUS: helper download failed: %s",
					result.err,
				)
				return 0, 0, 0
			}

			if result.rsIndex < 0 ||
				result.rsIndex >= totalStripeShards {

				logger.Errorf(
					"ASCLEPIUS: invalid RS index %d",
					result.rsIndex,
				)
				return 0, 0, 0
			}

			if reconstructShards[result.rsIndex] != nil {
				logger.Errorf(
					"ASCLEPIUS: duplicate data for RS index %d",
					result.rsIndex,
				)
				return 0, 0, 0
			}

			reconstructShards[result.rsIndex] =
				result.data

			successfulDownloads++

			fmt.Printf(
				"ASCLEPIUS chunk=%d helper=%s rsIndex=%d "+
					"duration=%s size=%d\n",
				chunkPosition,
				result.peer.String(),
				result.rsIndex,
				result.duration.String(),
				len(result.data),
			)
		}

		if successfulDownloads != or {
			logger.Errorf(
				"ASCLEPIUS: downloaded %d/%d required chunks "+
					"for position %d",
				successfulDownloads,
				or,
				chunkPosition,
			)
			return 0, 0, 0
		}

		if reconstructShards[tosend] != nil {
			logger.Errorf(
				"ASCLEPIUS: missing index %d was populated before reconstruction",
				tosend,
			)
			return 0, 0, 0
		}

		reconstructionStart :=
			time.Now()

		err = enc.Reconstruct(
			reconstructShards,
		)

		reconstructionDuration :=
			time.Since(reconstructionStart)

		timetorepairchunksonly +=
			reconstructionDuration

		if err != nil {
			logger.Errorf(
				"ASCLEPIUS: reconstruction failed for chunk %d: %s",
				chunkPosition,
				err,
			)
			return 0, 0, 0
		}

		if len(reconstructShards[tosend]) == 0 {
			logger.Errorf(
				"ASCLEPIUS: reconstructed data is empty "+
					"for chunk=%d index=%d",
				chunkPosition,
				tosend,
			)
			return 0, 0, 0
		}

		node :=
			ipfsadd.NewFSNodeOverDagC(
				ft.TFile,
				prefix,
			)

		node.SetFileData(
			reconstructShards[tosend],
		)

		rawNode, commitErr :=
			node.Commit()

		if commitErr != nil {
			logger.Errorf(
				"ASCLEPIUS: cannot commit reconstructed chunk %d: %s",
				chunkPosition,
				commitErr,
			)
			return 0, 0, 0
		}

		// This is the relocation. shh targets destinationPeer.
		shh.SendBlock(
			spt.ctx,
			rawNode,
		)

		size := uint64(
			len(rawNode.RawData()),
		)

		shh.AddLink(
			ctx,
			rawNode.Cid(),
			size,
		)
	}

	// ---------------------------------------------------------------------
	// Flush the rebuilt shard on the destination selected by the scheduler.
	// ---------------------------------------------------------------------

	flushStart := time.Now()

	pin.Allocations = []peer.ID{
		destinationPeer,
	}

	shh.FlushForStateless(
		spt.ctx,
		*pin,
	)

	flushDuration :=
		time.Since(flushStart)

	fmt.Printf(
		"ASCLEPIUS REPAIR COMPLETED "+
			"shard=%s failedGlobal=%d missingIndex=%d "+
			"repairPeer=%s destination=%s relocated=%v "+
			"total=%s download=%s reconstruction=%s sendFlush=%s\n",
		pin.Name,
		failedGlobalShardNumber,
		tosend,
		spt.peerID.String(),
		destinationPeer.String(),
		destinationPeer != spt.peerID,
		time.Since(start).String(),
		timedownloadchunks.String(),
		timetorepairchunksonly.String(),
		flushDuration.String(),
	)

	return timedownloadchunks,
		timetorepairchunksonly,
		flushDuration
}

func (spt *ECRepairS) repinUsingRSWithSwitching_updated(
	pin *api.Pin,
) (time.Duration, time.Duration, time.Duration) {
	totalStart := time.Now()
	start := time.Now()

	var timedownloadchunks time.Duration
	var timetorepairchunksonly time.Duration

	// ---------------------------------------------------------------------
	// Read metadata.
	// ---------------------------------------------------------------------

	cidString := pin.Metadata["Cids"]
	CIDs := strings.Split(cidString, ",")

	commonString := pin.Metadata["common"]
	Common := strings.Split(commonString, ",")

	allMatchesString := pin.Metadata["allmatches"]
	AllMatches := strings.Split(allMatchesString, ",")

	fmt.Printf(
		"MAX similarities: %d out of %d\n",
		len(Common),
		len(CIDs),
	)

	ctx, span := trace.StartSpan(
		spt.ctx,
		"pintracker/repinFromPeer",
	)
	defer span.End()

	// ---------------------------------------------------------------------
	// Parse EC parameters from the shard name.
	// ---------------------------------------------------------------------

	f1 := strings.Split(pin.Name, "(")[1]
	f2 := strings.Split(f1, ")")[0]

	or, _ := strconv.Atoi(
		strings.Split(f2, ",")[0],
	)

	par, _ := strconv.Atoi(
		strings.Split(f2, ",")[1],
	)

	logger.Debugf(
		"repinning %s from peer %s",
		pin.Cid,
		pin.Allocations,
	)

	prefix, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		logger.Errorf(
			"cannot create CID prefix for shard %s: %s",
			pin.Name,
			err,
		)
		return 0, 0, 0
	}

	hashFunCode, ok :=
		multihash.Names[strings.ToLower("sha2-256")]

	if !ok {
		logger.Errorf(
			"cannot find sha2-256 multihash code",
		)
		return 0, 0, 0
	}

	prefix.MhType = hashFunCode
	prefix.MhLength = -1

	// ---------------------------------------------------------------------
	// Obtain the cluster state.
	// ---------------------------------------------------------------------

	cState, err := spt.cons.State(ctx)
	if err != nil {
		logger.Warn(err)
		return 0, 0, 0
	}

	pinCh := make(chan api.Pin, 1024)

	go func() {
		if listErr := cState.List(spt.ctx, pinCh); listErr != nil {
			logger.Warn(listErr)
		}
	}()

	// ---------------------------------------------------------------------
	// Find the failed shard number and its stripe range.
	// ---------------------------------------------------------------------

	fmt.Fprintf(
		os.Stdout,
		"getShardNumber of pin named: %s\n",
		pin.Name,
	)

	numpin, name, err := getShardNumber(pin.Name)
	if err != nil {
		fmt.Println("Error:", err)
		return 0, 0, 0
	}

	tosend := (numpin - 1) % (or + par)

	fmt.Printf(
		"number of the shard to repair is: %d\n",
		numpin,
	)

	mod := numpin % (or + par)
	before := (numpin - 1) % (or + par)
	after := (or + par - mod) % (or + par)

	// ---------------------------------------------------------------------
	// Obtain the other shards in the same stripe.
	// ---------------------------------------------------------------------

	repairShards := make(
		[]pinwithmeta,
		0,
	)

	for pinn := range pinCh {
		if !strings.Contains(
			pinn.Name,
			"-shard-",
		) {
			continue
		}

		pinnShardNum, namee, shardErr :=
			getShardNumber(pinn.Name)

		if shardErr != nil {
			fmt.Println("Error:", shardErr)
			continue
		}

		sameStripe :=
			pinnShardNum >= numpin-before &&
				pinnShardNum <= numpin+after &&
				pinnShardNum != numpin &&
				name == namee

		if !sameStripe {
			continue
		}

		repairShards = append(
			repairShards,
			pinwithmeta{
				pin:   pinn,
				index: pinnShardNum,
				cids:  make([]string, 0),
			},
		)
	}

	sortRepairShardsByIndex(repairShards)

	// ---------------------------------------------------------------------
	// Retrieve the CID lists of the available shards.
	// ---------------------------------------------------------------------

	retrieveWG := new(sync.WaitGroup)
	retrieveMutex := new(sync.Mutex)

	retrievedMetadata := 0

	retrieveWG.Add(or)

	fmt.Printf(
		"STEEEEEEEEEPPPPPPPPPP RRRRRRRRRREEEEEEETTTTTTTTT "+
			"with length of repair shards: %d\n",
		len(repairShards),
	)

	for i, pinwm := range repairShards {
		go func(
			index int,
			shard pinwithmeta,
		) {
			cidss := spt.retrieveCids(shard)

			retrieveMutex.Lock()
			defer retrieveMutex.Unlock()

			for _, retrievedCID := range cidss {
				repairShards[index].cids = append(
					repairShards[index].cids,
					retrievedCID.cid,
				)
			}

			if retrievedMetadata < or {
				retrievedMetadata++
				retrieveWG.Done()
			}
		}(i, pinwm)
	}

	retrieveWG.Wait()

	fmt.Printf(
		"Extracting everything took %s\n",
		time.Since(totalStart),
	)

	// ---------------------------------------------------------------------
	// Select the final allocation.
	//
	// Local means:
	//     allocs == repairing peer
	//
	// Non-local means:
	//     repair locally, but open the rebuilt shard from the repairing
	//     peer toward the peer specified by allocs.
	// ---------------------------------------------------------------------

	allocString := strings.TrimSpace(
		pin.Metadata["allocs"],
	)

	local := allocString == spt.peerID.String()

	allocationPeer := spt.peerID

	if !local {
		if allocString == "" {
			logger.Errorf(
				"empty allocs metadata for non-local repair of shard %s",
				pin.Name,
			)
			return 0, 0, 0
		}

		allocationPeer, err =
			peer.Decode(allocString)

		if err != nil {
			logger.Errorf(
				"cannot decode allocation peer %q for shard %s: %s",
				allocString,
				pin.Name,
				err,
			)
			return 0, 0, 0
		}
	}

	fmt.Printf(
		"Repair allocation: repairPeer=%s allocationPeer=%s local=%t\n",
		spt.peerID.String(),
		allocationPeer.String(),
		local,
	)

	// ---------------------------------------------------------------------
	// Build blacklist metadata.
	// ---------------------------------------------------------------------

	blacklist := make(
		[]peer.ID,
		0,
	)

	blacklistSet := make(
		map[peer.ID]bool,
	)

	for _, repairShard := range repairShards {
		for _, allocation := range repairShard.pin.Allocations {

			if allocation == "" ||
				blacklistSet[allocation] {
				continue
			}

			blacklistSet[allocation] = true

			blacklist = append(
				blacklist,
				allocation,
			)
		}
	}

	if pin.PinOptions.Metadata == nil {
		pin.PinOptions.Metadata =
			make(map[string]string)
	}

	blacklistStrings := make(
		[]string,
		0,
		len(blacklist),
	)

	for _, blacklistedPeer := range blacklist {
		fmt.Printf(
			"BBBBLLLLL: %s\n",
			blacklistedPeer.String(),
		)

		blacklistStrings = append(
			blacklistStrings,
			blacklistedPeer.String(),
		)
	}

	pin.PinOptions.Metadata["Black"] =
		strings.Join(
			blacklistStrings,
			",",
		)

	// ---------------------------------------------------------------------
	// Open the repaired shard.
	//
	// This is now the only difference between local and relocation:
	// allocationPeer is either spt.peerID or the peer from allocs.
	// ---------------------------------------------------------------------

	shh, err := sharding.NewShard(
		spt.ctx,
		spt.ctx,
		spt.rpcClient,
		pin.PinOptions,
		allocationPeer,
	)

	if err != nil {
		logger.Errorf(
			"cannot create repaired shard %s on peer %s: %s",
			pin.Name,
			allocationPeer.String(),
			err,
		)
		return 0, 0, 0
	}

	enc, err := reedsolomon.New(or, par)
	if err != nil {
		logger.Errorf(
			"cannot initialize Reed-Solomon (%d,%d): %s",
			or,
			par,
			err,
		)
		return 0, 0, 0
	}

	// ---------------------------------------------------------------------
	// Find a shard whose CID metadata was successfully retrieved.
	// ---------------------------------------------------------------------

	firstAvailable := -1

	for i, shard := range repairShards {
		if len(shard.cids) > 0 {
			firstAvailable = i
			break
		}
	}

	if firstAvailable == -1 {
		logger.Errorf(
			"no helper shard metadata available for repairing %s",
			pin.Name,
		)
		return 0, 0, 0
	}

	times := len(
		repairShards[firstAvailable].cids,
	)

	toskip := true
	timerlaunched := false
	selectedIndexes := make([]int, 0)

	repairCtx, cancelRepair :=
		context.WithCancel(
			context.Background(),
		)
	defer cancelRepair()

	// ---------------------------------------------------------------------
	// Helper used to download N shard chunks and reconstruct one chunk.
	// ---------------------------------------------------------------------

	reconstructChunk := func(
		chunkIndex int,
		readFrom []pinwithmeta,
		saveIndexes bool,
	) ([]byte, bool) {
		if len(readFrom) < or {
			logger.Errorf(
				"only %d readable helpers exist; %d are required",
				len(readFrom),
				or,
			)
			return nil, false
		}

		reconstructShards :=
			make([][]byte, or+par)

		var downloadWG sync.WaitGroup
		var downloadMutex sync.Mutex

		retrieved := 0

		fetchCtx, cancelFetch :=
			context.WithCancel(repairCtx)
		defer cancelFetch()

		downloadWG.Add(or)

		downloadStart := time.Now()

		for _, shard := range readFrom {
			if len(shard.cids) <= chunkIndex {
				continue
			}

			go func(shard pinwithmeta) {
				chunkStart := time.Now()

				data := spt.getData(
					fetchCtx,
					shard.cids[chunkIndex],
				)

				fmt.Printf(
					"REPAIR GOT HERE local=%t FOR shard %d: %s\n",
					local,
					shard.index,
					time.Since(chunkStart),
				)

				downloadMutex.Lock()
				defer downloadMutex.Unlock()

				if retrieved >= or {
					return
				}

				retrieved++

				reconstructIndex :=
					(shard.index - 1) %
						(or + par)

				reconstructShards[reconstructIndex] =
					data

				if saveIndexes {
					selectedIndexes = append(
						selectedIndexes,
						shard.index,
					)
				}

				downloadWG.Done()

				if retrieved == or {
					cancelFetch()
				}
			}(shard)
		}

		downloadWG.Wait()

		timedownloadchunks +=
			time.Since(downloadStart)

		fmt.Printf(
			"REPAIR GOT HERE ENDEDDDD this stripeeeee\n",
		)

		reconstructStart := time.Now()

		if reconstructErr :=
			enc.Reconstruct(reconstructShards); reconstructErr != nil {

			logger.Errorf(
				"cannot reconstruct chunk %d of shard %s: %s",
				chunkIndex,
				pin.Name,
				reconstructErr,
			)

			return nil, false
		}

		timetorepairchunksonly +=
			time.Since(reconstructStart)

		return reconstructShards[tosend], true
	}

	// ---------------------------------------------------------------------
	// Repair every chunk.
	// ---------------------------------------------------------------------

	for i := 0; i < times; i++ {
		var chunkData []byte

		// Common or already-matched chunk: retrieve it directly.
		if contains(Common, CIDs[i]) ||
			contains(AllMatches, CIDs[i]) {

			fmt.Printf(
				"Entered to the direct/common chunk part\n",
			)

			chunkStart := time.Now()

			chunkData =
				spt.getData(
					repairCtx,
					CIDs[i],
				)

			timedownloadchunks +=
				time.Since(chunkStart)

			fmt.Printf(
				"Direct/common data size: %d\n",
				len(chunkData),
			)
		} else {
			// Missing chunk: reconstruct it from helper shards.
			readFrom := make(
				[]pinwithmeta,
				0,
				len(repairShards),
			)

			for _, shard := range repairShards {
				if len(shard.cids) > i {
					readFrom = append(
						readFrom,
						shard,
					)
				}
			}

			if len(readFrom) > or {
				if !timerlaunched {
					go startTimerNew5(
						repairCtx,
						&toskip,
					)

					timerlaunched = true
				}

				if toskip {
					selectedIndexes =
						make([]int, 0)

					var reconstructed bool

					chunkData, reconstructed =
						reconstructChunk(
							i,
							readFrom,
							true,
						)

					if !reconstructed {
						return 0, 0, 0
					}

					toskip = false
				} else {
					readFiltered := make(
						[]pinwithmeta,
						0,
						or,
					)

					selectedSet :=
						make(map[int]bool)

					for _, selectedIndex := range selectedIndexes {

						selectedSet[selectedIndex] =
							true
					}

					for _, shard := range readFrom {

						if selectedSet[shard.index] {
							readFiltered = append(
								readFiltered,
								shard,
							)
						}
					}

					var reconstructed bool

					chunkData, reconstructed =
						reconstructChunk(
							i,
							readFiltered,
							false,
						)

					if !reconstructed {
						return 0, 0, 0
					}
				}
			} else {
				var reconstructed bool

				chunkData, reconstructed =
					reconstructChunk(
						i,
						readFrom,
						false,
					)

				if !reconstructed {
					return 0, 0, 0
				}
			}
		}

		// -----------------------------------------------------------------
		// Add the direct or reconstructed chunk to the repaired shard.
		// -----------------------------------------------------------------

		nodee := ipfsadd.NewFSNodeOverDagC(
			ft.TFile,
			prefix,
		)

		nodee.SetFileData(chunkData)

		rawnode, commitErr :=
			nodee.Commit()

		if commitErr != nil {
			logger.Errorf(
				"cannot create repaired block %d for shard %s: %s",
				i,
				pin.Name,
				commitErr,
			)
			return 0, 0, 0
		}

		if sendErr :=
			shh.SendBlock(
				spt.ctx,
				rawnode,
			); sendErr != nil {

			logger.Errorf(
				"cannot send repaired block %s: %s",
				rawnode.Cid(),
				sendErr,
			)

			return 0, 0, 0
		}

		size :=
			uint64(
				len(rawnode.RawData()),
			)

		shh.AddLink(
			ctx,
			rawnode.Cid(),
			size,
		)
	}

	// ---------------------------------------------------------------------
	// Flush the repaired shard.
	// ---------------------------------------------------------------------

	waitStart := time.Now()

	pin.Allocations =
		make([]peer.ID, 0)

	for _, allocation := range shh.Allocations() {

		pin.Allocations = append(
			pin.Allocations,
			allocation,
		)
	}

	shh.FlushForStateless(
		spt.ctx,
		*pin,
	)

	waitTime := time.Since(waitStart)

	fmt.Printf(
		"REPAIR TOOK %s local=%t repairPeer=%s allocationPeer=%s\n",
		time.Since(start),
		local,
		spt.peerID.String(),
		allocationPeer.String(),
	)

	return timedownloadchunks,
		timetorepairchunksonly,
		waitTime
}
