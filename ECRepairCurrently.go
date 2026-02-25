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
func NewECrep(cfg *Config, pid peer.ID, cons Consensus, connector IPFSConnector) *ECRepairS {
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
		RepairCh:  make(chan *api.Pin, DefaultMaxPinQueueSize),
		cons:      cons,
		connector: connector,
	}

	for i := 0; i < DefaultConcurrentPins; i++ {
		//go spt.opWorker(spt.RepairCh)
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
		// Process the priority channel first.
		select {
		case op = <-RepairCh:
			goto APPLY_OP
		case <-spt.ctx.Done():
			return
		default:
		}

		// apply operations that came from some channel
	APPLY_OP:
		spt.pin(op)
	}
}

func (spt *ECRepairS) pin(op *api.Pin) error {
	fmt.Fprintf(os.Stdout, "Date start inside the pintracker repair %s : %s \n", op.Name, time.Now().Format("2006-01-02 15:04:05.000"))
	download, repair, waittosend := spt.repinUsingRSWithSwitching(op)
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

// This will keep track of the fastest peers to use every 1 second, in addition to that, it will add the minimal interference to the system since it will ask for six data chunks during 1 sec interval before update
// to the fastest n again.
func (spt *ECRepairS) repinUsingRSWithSwitching(pin *api.Pin) (time.Duration, time.Duration, time.Duration) {
	ssss := time.Now()
	repairShards := make([]pinwithmeta, 0)
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
			cidss := spt.RetrieveCids(pinwm)
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
		}
		wait1 := time.Now()
		shh.FlushNew(spt.ctx)
		wait2 := time.Since(wait1)
		cancell()

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
		}
		wait1 := time.Now()
		shh.FlushForStateless(spt.ctx, *pin)
		wait2 := time.Since(wait1)
		cancell()
		return timedownloadchunks, timetorepairchunksonly, wait2
	}

	//MFS
	//shh, _ := sharding.NewShards(spt.ctx, spt.ctx, spt.rpcClient, pin.PinOptions)

}
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

func (spt *ECRepairS) RetrieveCids(pinwm pinwithmeta) []Chunk {
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

// Enqueue puts a new operation on the queue, unless ongoing exists.
func (spt *ECRepairS) Enqueue(ctx context.Context, c *api.Pin) error {
	ctx, span := trace.StartSpan(ctx, "tracker/stateless/enqueue")
	defer span.End()

	ch := spt.RepairCh

	select {
	case ch <- c:
	default:
		err := ErrFullQueue
		logger.Error(err.Error())
		return err
	}
	return nil
}

// SetClient makes the StatelessPinTracker ready to perform RPC requests to
// other components.
func (spt *ECRepairS) SetClient(c *rpc.Client) {
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
