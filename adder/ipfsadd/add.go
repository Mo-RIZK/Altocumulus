// Package ipfsadd is a simplified copy of go-ipfs/core/coreunix/add.go
package ipfsadd

import (
	"context"
	"errors"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"io"
	"math"
	"os"
	gopath "path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ipfs-cluster/ipfs-cluster/api"

	chunker "github.com/ipfs/boxo/chunker"
	files "github.com/ipfs/boxo/files"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	unixfs "github.com/ipfs/boxo/ipld/unixfs"
	balanced "github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	ihelper "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	trickle "github.com/ipfs/boxo/ipld/unixfs/importer/trickle"
	mfs "github.com/ipfs/boxo/mfs"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("coreunix")

// how many bytes of progress to wait before sending a progress update message
const progressReaderIncrement = 1024 * 256

// cluster: we need to cache all to be able to output intermediate folders
//var liveCacheSize = uint64(256 << 10)

// NewAdder Returns a new Adder used for a file add operation.
func NewAdder(ctx context.Context, ds ipld.DAGService, allocs func() []peer.ID, Original int, Parity int, striped bool, shardsize uint64) (*Adder, error) {
	// Cluster: we don't use pinner nor GCLocker.
	return &Adder{
		ctx:        ctx,
		dagService: ds,
		allocsFun:  allocs,
		Progress:   false,
		Trickle:    false,
		Chunker:    "",
		Original:   Original,
		Parity:     Parity,
		Striped:    striped,
		ShardSize:  shardsize,
	}, nil
}

// Adder holds the switches passed to the `add` command.
type Adder struct {
	ctx        context.Context
	dagService ipld.DAGService
	allocsFun  func() []peer.ID
	Out        chan api.AddedOutput
	Progress   bool
	Trickle    bool
	RawLeaves  bool
	Silent     bool
	NoCopy     bool
	Chunker    string
	mroot      *mfs.Root
	tempRoot   cid.Cid
	CidBuilder cid.Builder
	// liveNodes  uint64 // cluster: we do not clear mfs cache.
	lastFile  mfs.FSNode
	Original  int
	Parity    int
	Striped   bool
	ShardSize uint64
	// Cluster: ipfs does a hack in commands/add.go to set the filenames
	// in emitted events correctly. We carry a root folder name (or a
	// filename in the case of single files here and emit those events
	// correctly from the beginning).
	OutputPrefix string
}

func (adder *Adder) mfsRoot() (*mfs.Root, error) {
	if adder.mroot != nil {
		return adder.mroot, nil
	}
	rnode := unixfs.EmptyDirNode()
	rnode.SetCidBuilder(adder.CidBuilder)
	mr, err := mfs.NewRoot(adder.ctx, adder.dagService, rnode, nil)
	if err != nil {
		return nil, err
	}
	adder.mroot = mr
	return adder.mroot, nil
}

// SetMfsRoot sets `r` as the root for Adder.
func (adder *Adder) SetMfsRoot(r *mfs.Root) {
	adder.mroot = r
}

// Constructs a node from reader's data, and adds it. Doesn't pin.
func (adder *Adder) add(reader io.Reader) (ipld.Node, error) {

	chnk, err := chunker.FromString(reader, adder.Chunker)
	if err != nil {
		return nil, err
	}
	// Cluster: we don't do batching/use BufferedDS.
	fmt.Fprintf(os.Stdout, " repliii %d \n", adder.Original)
	if adder.Original <= 1 {
		nd := adder.addRep(chnk)
		return nd, nil
	} else {
		if adder.Striped {
			nd := adder.addEC(chnk)
			return nd, nil
		} else {
			// create merkle dag without sending data to destinations
			nd := adder.addECC(chnk, reader)

			return nd, nil
		}

	}
}

func GenerateParityShards(shards [][]byte, dataShards, parityShards int, shardSize, chunkSize int) error {
	totalShards := dataShards + parityShards

	// --- Safety check: allocate parity shards if missing ---
	for i := dataShards; i < totalShards; i++ {
		if shards[i] == nil || len(shards[i]) < shardSize {
			shards[i] = make([]byte, shardSize)
		}
	}

	// Create the Reed-Solomon encoder
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	// Number of stripes per shard
	numStripes := int(math.Ceil(float64(shardSize) / float64(chunkSize)))
	fmt.Fprintf(os.Stdout, "Encoding with %d stripes (chunk size %d bytes)\n", numStripes, chunkSize)

	for stripe := 0; stripe < numStripes; stripe++ {
		offset := stripe * chunkSize
		stripeSize := chunkSize
		if offset+chunkSize > shardSize {
			stripeSize = shardSize - offset
		}

		// Prepare data and parity chunks for this stripe
		chunkBlock := make([][]byte, totalShards)

		// --- Copy data chunks safely ---
		for i := 0; i < dataShards; i++ {
			end := offset + stripeSize
			if end > len(shards[i]) {
				end = len(shards[i])
			}
			chunkBlock[i] = make([]byte, stripeSize)
			copy(chunkBlock[i], shards[i][offset:end])
		}

		// --- Allocate parity buffers ---
		for i := dataShards; i < totalShards; i++ {
			chunkBlock[i] = make([]byte, stripeSize)
		}

		// --- Encode this stripe ---
		if err := enc.Encode(chunkBlock); err != nil {
			return fmt.Errorf("encoding failed at stripe %d: %w", stripe, err)
		}

		// --- Copy parity chunks safely ---
		for i := 0; i < parityShards; i++ {
			parityIndex := dataShards + i
			end := offset + stripeSize
			if end > len(shards[parityIndex]) {
				end = len(shards[parityIndex])
			}
			copy(shards[parityIndex][offset:end], chunkBlock[parityIndex])
		}
	}

	fmt.Fprintf(os.Stdout, "✅ Parity encoding completed successfully\n")
	return nil
}


// Cluster: commented as it is unused
// // RootNode returns the mfs root node
// func (adder *Adder) curRootNode() (ipld.Node, error) {
// 	mr, err := adder.mfsRoot()
// 	if err != nil {
// 		return nil, err
// 	}
// 	root, err := mr.GetDirectory().GetNode()
// 	if err != nil {
// 		return nil, err
// 	}

// 	// if one root file, use that hash as root.
// 	if len(root.Links()) == 1 {
// 		nd, err := root.Links()[0].GetNode(adder.ctx, adder.dagService)
// 		if err != nil {
// 			return nil, err
// 		}

// 		root = nd
// 	}

// 	return root, err
// }

// PinRoot recursively pins the root node of Adder and
// writes the pin state to the backing datastore.
// Cluster: we don't pin. Former Finalize().
func (adder *Adder) PinRoot(root ipld.Node) error {
	rnk := root.Cid()

	err := adder.dagService.Add(adder.ctx, root)
	if err != nil {
		return err
	}

	if adder.tempRoot.Defined() {
		adder.tempRoot = rnk
	}

	return nil
}

func (adder *Adder) outputDirs(path string, fsn mfs.FSNode) error {
	switch fsn := fsn.(type) {
	case *mfs.File:
		return nil
	case *mfs.Directory:
		names, err := fsn.ListNames(adder.ctx)
		if err != nil {
			return err
		}

		for _, name := range names {
			child, err := fsn.Child(name)
			if err != nil {
				// This fails when Child is of type *mfs.File
				// because it tries to get them from the DAG
				// service (does not implement this and returns
				// a "not found" error)
				// *mfs.Files are ignored in the recursive call
				// anyway.
				// For Cluster, we just ignore errors here.
				continue
			}

			childpath := gopath.Join(path, name)
			err = adder.outputDirs(childpath, child)
			if err != nil {
				return err
			}

			fsn.Uncache(name)
		}
		nd, err := fsn.GetNode()
		if err != nil {
			return err
		}

		return adder.outputDagnode(adder.Out, path, nd)
	default:
		return fmt.Errorf("unrecognized fsn type: %#v", fsn)
	}
}

func (adder *Adder) addNode(node ipld.Node, path string) error {
	// patch it into the root
	outputName := path
	if path == "" {
		path = node.Cid().String()
		outputName = ""
	}

	if pi, ok := node.(*posinfo.FilestoreNode); ok {
		node = pi.Node
	}

	mr, err := adder.mfsRoot()
	if err != nil {
		return err
	}
	dir := gopath.Dir(path)
	if dir != "." {
		opts := mfs.MkdirOpts{
			Mkparents:  true,
			Flush:      false,
			CidBuilder: adder.CidBuilder,
		}
		if err := mfs.Mkdir(mr, dir, opts); err != nil {
			return err
		}
	}

	if err := mfs.PutNode(mr, path, node); err != nil {
		return err
	}

	// Cluster: cache the last file added.
	// This avoids using the DAGService to get the first children
	// if the MFS root when not wrapping.
	lastFile, err := mfs.NewFile(path, node, nil, adder.dagService)
	if err != nil {
		return err
	}
	adder.lastFile = lastFile

	if !adder.Silent {
		return adder.outputDagnode(adder.Out, outputName, node)
	}
	return nil
}

// AddAllAndPin adds the given request's files and pin them.
// Cluster: we don'pin. Former AddFiles.
func (adder *Adder) AddAllAndPin(file files.Node) (ipld.Node, error) {
	if err := adder.addFileNode("", file, true); err != nil {
		return nil, err
	}

	// get root
	mr, err := adder.mfsRoot()
	if err != nil {
		return nil, err
	}
	var root mfs.FSNode
	rootdir := mr.GetDirectory()
	root = rootdir

	// if adding a file without wrapping, swap the root to it (when adding a
	// directory, mfs root is the directory)
	_, dir := file.(files.Directory)
	var name string
	if !dir {
		children, err := rootdir.ListNames(adder.ctx)
		if err != nil {
			return nil, err
		}

		if len(children) == 0 {
			return nil, fmt.Errorf("expected at least one child dir, got none")
		}

		// Replace root with the first child
		name = children[0]
		root, err = rootdir.Child(name)
		if err != nil {
			// Cluster: use the last file we added
			// if we have one.
			if adder.lastFile == nil {
				return nil, err
			}
			root = adder.lastFile
		}
	}

	err = mr.Close()
	if err != nil {
		return nil, err
	}

	nd, err := root.GetNode()
	if err != nil {
		return nil, err
	}

	// output directory events
	err = adder.outputDirs(name, root)
	if err != nil {
		return nil, err
	}

	// Flush the MFS directories. This must happen after outputDirs as
	// otherwise we will have no cached directories in MFS, and we cannot
	// fetch from the DAGService.
	err = rootdir.Flush()
	if err != nil {
		return nil, err
	}

	// Cluster: call PinRoot which adds the root cid to the DAGService.
	// Unsure if this a bug in IPFS when not pinning. Or it would get added
	// twice.
	return nd, adder.PinRoot(nd)
}

// Cluster: we don't Pause for GC
func (adder *Adder) addFileNode(path string, file files.Node, toplevel bool) error {
	defer file.Close()

	// cluster: flushing MFS will cause issues when outputting intermediary
	// mfs folders.
	// if adder.liveNodes >= liveCacheSize {
	// 	// TODO: A smarter cache that uses some sort of lru cache with an eviction handler
	// 	mr, err := adder.mfsRoot()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if err := mr.FlushMemFree(adder.ctx); err != nil {
	// 		return err
	// 	}

	// 	adder.liveNodes = 0
	// }
	// adder.liveNodes++

	switch f := file.(type) {
	case files.Directory:
		return adder.addDir(path, f, toplevel)
	case *files.Symlink:
		return adder.addSymlink(path, f)
	case files.File:
		return adder.addFile(path, f)
	default:
		return errors.New("unknown file type")
	}
}

func (adder *Adder) addSymlink(path string, l *files.Symlink) error {
	sdata, err := unixfs.SymlinkData(l.Target)
	if err != nil {
		return err
	}

	dagnode := dag.NodeWithData(sdata)
	dagnode.SetCidBuilder(adder.CidBuilder)
	err = adder.dagService.Add(adder.ctx, dagnode)
	if err != nil {
		return err
	}

	return adder.addNode(dagnode, path)
}

func (adder *Adder) addFile(path string, file files.File) error {
	// if the progress flag was specified, wrap the file so that we can send
	// progress updates to the client (over the output channel)
	var reader io.Reader = file
	if adder.Progress {
		rdr := &progressReader{file: reader, path: path, out: adder.Out}
		if fi, ok := file.(files.FileInfo); ok {
			reader = &progressReader2{rdr, fi}
		} else {
			reader = rdr
		}
	}

	dagnode, err := adder.add(reader)
	if err != nil {
		return err
	}

	// patch it into the root
	return adder.addNode(dagnode, path)
}

func (adder *Adder) addEC(chnk chunker.Splitter) ipld.Node {

	params := DagBuilderParams{
		Dagserv:    adder.dagService,
		RawLeaves:  adder.RawLeaves,
		Maxlinks:   30 * (adder.Original + adder.Parity),
		NoCopy:     adder.NoCopy,
		CidBuilder: adder.CidBuilder,
	}

	db, err := params.New(chnk)
	if err != nil {
		return nil
	}
	var nd ipld.Node
	sizeStr := strings.Split(adder.Chunker, "-")[1]
	size, _ := strconv.Atoi(sizeStr)
	if adder.Striped {
		nd, err = Layout(db, adder.Original, adder.Parity, size)
		if err != nil {
			return nil
		}
	}

	return nd
}
func (adder *Adder) addECC(chnk chunker.Splitter, reader io.Reader) ipld.Node {

	params := DagBuilderParams{
		Dagserv:    adder.dagService,
		RawLeaves:  adder.RawLeaves,
		Maxlinks:   30 * (adder.Original + adder.Parity),
		NoCopy:     adder.NoCopy,
		CidBuilder: adder.CidBuilder,
	}

	db, err := params.New(chnk)
	if err != nil {
		return nil
	}
	var nd ipld.Node
	fmt.Fprintf(os.Stdout, "Starttttt merkleeeeee DAGGGGG \n")
	nd, err = LayoutC(db)
	fmt.Fprintf(os.Stdout, "endddddddd merkleeeeee DAGGGGG \n")
	//align data
	shards := make([][]byte, adder.Original+adder.Parity)
	fmt.Fprintf(os.Stdout, "Starttttt fillinggggggggg \n")
	for i := 0; i < adder.Original; i++ {
		shards[i] = make([]byte, adder.ShardSize)
		n, errr := io.ReadFull(reader, shards[i])
		if errr != nil && errr != io.EOF && errr != io.ErrUnexpectedEOF {
			return nil
		}
		if uint64(n) < adder.ShardSize {
			fmt.Fprintf(os.Stdout, "PADDDDDINGGGGGG \n")
			// pad the last shard if necessary
			for j := n; uint64(j) < adder.ShardSize; j++ {
				shards[i][j] = 0
			}
		}
	}
	fmt.Fprintf(os.Stdout, "endddddddd fillinggggggggg \n")
	sizeStr := strings.Split(adder.Chunker, "-")[1]
	size, _ := strconv.Atoi(sizeStr)
	fmt.Fprintf(os.Stdout, "Starttttt encodinggggggggg \n")
	errr := GenerateParityShards(shards, adder.Original, adder.Parity, int(adder.ShardSize), size)
	if errr != nil {
		return nil
	}
	fmt.Fprintf(os.Stdout, "enddddddd encodinggggggggg \n")
	//create nodes and send to destination
	//AddShardsToDB(adder.ctx, shards, adder.Original, adder.Parity, int(adder.ShardSize), size, db)
	//nd here is the root node of the merkle DAG
	return nd
}

// AddShardsToDB reads each shard chunk-by-chunk and adds them as leaf nodes to the DB.
func AddShardsToDB(
	ctx context.Context,
	shards [][]byte,
	dataShards, parityShards int,
	shardSize, chunkSize int,
	db *DagBuilderHelper, // your DB object
) error {
	totalShards := dataShards + parityShards

	// Calculate number of stripes (ceil division)
	numStripes := (shardSize + chunkSize - 1) / chunkSize

	for stripe := 0; stripe < numStripes; stripe++ {
		offset := stripe * chunkSize
		stripeLength := chunkSize
		if offset+chunkSize > shardSize {
			stripeLength = shardSize - offset
		}

		for shardIndex := 0; shardIndex < totalShards; shardIndex++ {
			// Read the chunk from this shard
			dataread := make([]byte, stripeLength)
			copy(dataread, shards[shardIndex][offset:offset+stripeLength])

			// Create a leaf node
			node, err := db.NewLeafNode(dataread, ft.TFile)
			if err != nil {
				return fmt.Errorf("failed to create leaf node for shard %d stripe %d: %w",
					shardIndex, stripe, err)
			}

			// Add the node to the DB/service
			err = db.dserv.Add(ctx, node)
			if err != nil {
				return fmt.Errorf("failed to add node for shard %d stripe %d: %w",
					shardIndex, stripe, err)
			}
		}
	}

	return nil
}

func (adder *Adder) addRep(chnk chunker.Splitter) ipld.Node {
	params := ihelper.DagBuilderParams{
		Dagserv:    adder.dagService,
		RawLeaves:  adder.RawLeaves,
		Maxlinks:   ihelper.DefaultLinksPerBlock,
		NoCopy:     adder.NoCopy,
		CidBuilder: adder.CidBuilder,
	}

	db, err := params.New(chnk)
	if err != nil {
		return nil
	}

	var nd ipld.Node
	if adder.Trickle {
		nd, err = trickle.Layout(db)
	} else {
		//paramss := DagBuilderParams{
		//	Dagserv:    adder.dagService,
		//	RawLeaves:  adder.RawLeaves,
		//	Maxlinks:   ihelper.DefaultLinksPerBlock,
		//	NoCopy:     adder.NoCopy,
		//	CidBuilder: adder.CidBuilder,
		//}

		//dbb, _ := paramss.New(chnk)
		nd, err = balanced.Layout(db)
	}
	if err != nil {
		return nil
	}
	return nd
}

func (adder *Adder) addDir(path string, dir files.Directory, toplevel bool) error {
	log.Infof("adding directory: %s", path)

	if !(toplevel && path == "") {
		mr, err := adder.mfsRoot()
		if err != nil {
			return err
		}
		err = mfs.Mkdir(mr, path, mfs.MkdirOpts{
			Mkparents:  true,
			Flush:      false,
			CidBuilder: adder.CidBuilder,
		})
		if err != nil {
			return err
		}
	}

	it := dir.Entries()
	for it.Next() {
		fpath := gopath.Join(path, it.Name())
		err := adder.addFileNode(fpath, it.Node(), false)
		if err != nil {
			return err
		}
	}

	return it.Err()
}

// outputDagnode sends dagnode info over the output channel.
// Cluster: we use api.AddedOutput instead of coreiface events
// and make this an adder method to be able to prefix.
func (adder *Adder) outputDagnode(out chan api.AddedOutput, name string, dn ipld.Node) error {
	if out == nil {
		return nil
	}

	s, err := dn.Size()
	if err != nil {
		return err
	}

	// When adding things in a folder: "OutputPrefix/name"
	// When adding a single file: "OutputPrefix" (name is unset)
	// When adding a single thing with no name: ""
	// Note: ipfs sets the name of files received on stdin to the CID,
	// but cluster does not support stdin-adding so we do not
	// account for this here.
	name = filepath.Join(adder.OutputPrefix, name)

	out <- api.AddedOutput{
		Cid:         api.NewCid(dn.Cid()),
		Name:        name,
		Size:        s,
		Allocations: adder.allocsFun(),
	}

	return nil
}

type progressReader struct {
	file         io.Reader
	path         string
	out          chan api.AddedOutput
	bytes        int64
	lastProgress int64
}

func (i *progressReader) Read(p []byte) (int, error) {
	n, err := i.file.Read(p)

	i.bytes += int64(n)
	if i.bytes-i.lastProgress >= progressReaderIncrement || err == io.EOF {
		i.lastProgress = i.bytes
		i.out <- api.AddedOutput{
			Name:  i.path,
			Bytes: uint64(i.bytes),
		}
	}

	return n, err
}

type progressReader2 struct {
	*progressReader
	files.FileInfo
}

func (i *progressReader2) Read(p []byte) (int, error) {
	return i.progressReader.Read(p)
}
