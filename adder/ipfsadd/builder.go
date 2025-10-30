package ipfsadd

import (
	"errors"
	"fmt"
	"os"
	"time"

	ft "github.com/ipfs/boxo/ipld/unixfs"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/klauspost/reedsolomon"
)

// Package balanced provides methods to build balanced DAGs, which are generalistic
// DAGs in which all leaves (nodes representing chunks of data) are at the same
// distance from the root. Nodes can have only a maximum number of children; to be
// able to store more leaf data nodes balanced DAGs are extended by increasing its
// depth (and having more intermediary nodes).
//
// Internal nodes are always represented by UnixFS nodes (of type `File`) encoded
// inside DAG nodes (see the `go-unixfs` package for details of UnixFS). In
// contrast, leaf nodes with data have multiple possible representations: UnixFS
// nodes as above, raw nodes with just the file data (no format) and Filestore
// nodes (that directly link to the file on disk using a format stored on a raw
// node, see the `go-ipfs/filestore` package for details of Filestore.)
//
// In the case the entire file fits into just one node it will be formatted as a
// (single) leaf node (without parent) with the possible representations already
// mentioned. This is the only scenario where the root can be of a type different
// that the UnixFS node.
//
// Notes:
//
//  1. In the implementation. `FSNodeOverDag` structure is used for representing
//     the UnixFS node encoded inside the DAG node.
//     (see https://github.com/ipfs/go-ipfs/pull/5118.)
//
//  2. `TFile` is used for backwards-compatibility. It was a bug causing the leaf
//     nodes to be generated with this type instead of `TRaw`. The former one
//     should be used (like the trickle builder does).
//     (See https://github.com/ipfs/go-ipfs/pull/5120.)
//
//     +-------------+
//     |   Root 4    |
//     +-------------+
//     |
//     +--------------------------+----------------------------+
//     |                                                       |
//     +-------------+                                         +-------------+
//     |   Node 2    |                                         |   Node 5    |
//     +-------------+                                         +-------------+
//     |                                                       |
//     +-------------+-------------+                           +-------------+
//     |                           |                           |
//     +-------------+             +-------------+             +-------------+
//     |   Node 1    |             |   Node 3    |             |   Node 6    |
//     +-------------+             +-------------+             +-------------+
//     |                           |                           |
//     +------+------+             +------+------+             +------+
//     |             |             |             |             |
//     +=========+   +=========+   +=========+   +=========+   +=========+
//     | Chunk 1 |   | Chunk 2 |   | Chunk 3 |   | Chunk 4 |   | Chunk 5 |
//     +=========+   +=========+   +=========+   +=========+   +=========+

// Layout builds a balanced DAG layout. In a balanced DAG of depth 1, leaf nodes
// with data are added to a single `root` until the maximum number of links is
// reached. Then, to continue adding more data leaf nodes, a `newRoot` is created
// pointing to the old `root` (which will now become and intermediary node),
// increasing the depth of the DAG to 2. This will increase the maximum number of
// data leaf nodes the DAG can have (`Maxlinks() ^ depth`). The `fillNodeRec`
// function will add more intermediary child nodes to `newRoot` (which already has
// `root` as child) that in turn will have leaf nodes with data added to them.
// After that process is completed (the maximum number of links is reached),
// `fillNodeRec` will return and the loop will be repeated: the `newRoot` created
// will become the old `root` and a new root will be created again to increase the
// depth of the DAG. The process is repeated until there is no more data to add
// (i.e. the DagBuilderHelper’s Done() function returns true).
//
// The nodes are filled recursively, so the DAG is built from the bottom up. Leaf
// nodes are created first using the chunked file data and its size. The size is
// then bubbled up to the parent (internal) node, which aggregates all the sizes of
// its children and bubbles that combined size up to its parent, and so on up to
// the root. This way, a balanced DAG acts like a B-tree when seeking to a byte
// offset in the file the graph represents: each internal node uses the file size
// of its children as an index when seeking.
//
//	`Layout` creates a root and hands it off to be filled:
//
//	       +-------------+
//	       |   Root 1    |
//	       +-------------+
//	              |
//	 ( fillNodeRec fills in the )
//	 ( chunks on the root.      )
//	              |
//	       +------+------+
//	       |             |
//	  + - - - - +   + - - - - +
//	  | Chunk 1 |   | Chunk 2 |
//	  + - - - - +   + - - - - +
//
//	                     ↓
//	When the root is full but there's more data...
//	                     ↓
//
//	       +-------------+
//	       |   Root 1    |
//	       +-------------+
//	              |
//	       +------+------+
//	       |             |
//	  +=========+   +=========+   + - - - - +
//	  | Chunk 1 |   | Chunk 2 |   | Chunk 3 |
//	  +=========+   +=========+   + - - - - +
//
//	                     ↓
//	...Layout's job is to create a new root.
//	                     ↓
//
//	                      +-------------+
//	                      |   Root 2    |
//	                      +-------------+
//	                            |
//	              +-------------+ - - - - - - - - +
//	              |                               |
//	       +-------------+            ( fillNodeRec creates the )
//	       |   Node 1    |            ( branch that connects    )
//	       +-------------+            ( "Root 2" to "Chunk 3."  )
//	              |                               |
//	       +------+------+             + - - - - -+
//	       |             |             |
//	  +=========+   +=========+   + - - - - +
//	  | Chunk 1 |   | Chunk 2 |   | Chunk 3 |
//	  +=========+   +=========+   + - - - - +
func Layout(db *DagBuilderHelper, or int, par int, size int) (ipld.Node, error) {
	//Array that contains the chunks to encode
	toEncode := [][]byte{}
	//Array that will contain the parity chunks
	parity := [][]byte{}
	//Used at the EOF for padding and encoding
	last := [][]byte{}
	var timetakenEncode time.Duration
	var timetakenPad time.Duration
	//Will be used in encoding original blocks to create the parity ones
	enc, errr := reedsolomon.New(or, par)
	checkErr(errr)
	if db.Done() {
		// No data, return just an empty node.
		root, err := db.NewLeafNode(nil, ft.TFile)
		if err != nil {
			return nil, err
		}
		// This works without Filestore support (`ProcessFileStore`).
		// TODO: Why? Is there a test case missing?

		return root, db.Add(root)
	}

	// The first `root` will be a single leaf node with data
	// (corner case), after that subsequent `root` nodes will
	// always be internal nodes (with a depth > 0) that can
	// be handled by the loop.
	//TODO : Get the chunk size from the splitter and remove this line of code
	db.leafsize = uint64(size)
	first := 0
	root, fileSize, data, err := db.NewLeafDataNodeNew(ft.TFile, db.leafsize)
	if fileSize <= db.leafsize {
		first = 1
	}

	fmt.Fprintf(os.Stdout, "Leaf size : %d\n", size)
	toEncode = append(toEncode, data)

	// Each time a DAG of a certain `depth` is filled (because it
	// has reached its maximum capacity of `db.Maxlinks()` per node)
	// extend it by making it a sub-DAG of a bigger DAG with `depth+1`.
	for depth := 1; !db.Done() || len(parity) > 0 || len(last) > 0 || first == 1; depth++ {
		fmt.Fprintf(os.Stdout, "Entered top \n")
		// Add the old `root` as a child of the `newRoot`.
		//fmt.Fprintf(os.Stdout, "kam parity fi : %d kam toEncode fi : %d kam nodenb saro : %d kam last fi : %d \n", parity, len(toEncode), nodenb, len(last))
		newRoot := db.NewFSNodeOverDag(ft.TFile)
		err = newRoot.AddChild(root, fileSize, db)
		if err != nil {
			return nil, err
		}

		// Fill the `newRoot` (that has the old `root` already as child)
		// and make it the current `root` for the next iteration (when
		// it will become "old").
		root, fileSize, parity, toEncode, last, timetakenEncode, timetakenPad, err = fillNodeRec(db, newRoot, depth, toEncode, enc, parity, last, or, timetakenEncode, timetakenPad, first)
		if err != nil {
			return nil, err
		}
		first = 0
	}
	// TODO:: if size of file less than chunk size
	fmt.Fprintf(os.Stdout, "Overall Time Taken To Encode : %s \n", timetakenEncode.String())
	fmt.Fprintf(os.Stdout, "Overall Time Taken To Pad : %s \n", timetakenPad.String())
	fmt.Fprintf(os.Stdout, "Overall Time Taken To create nodes : %s \n", db.createnode.String())
	fmt.Fprintf(os.Stdout, "Overall Time Taken To add link to the DAG : %s \n", db.dagadd.String())
	fmt.Fprintf(os.Stdout, "Overall Time Taken To send data and block reading the data : %s \n", db.timetakensending.String())
	fmt.Fprintf(os.Stdout, "Overall Time Taken To read data : %s \n", db.readtime.String())
	return root, db.Add(root)
}

// fillNodeRec will "fill" the given internal (non-leaf) `node` with data by
// adding child nodes to it, either leaf data nodes (if `depth` is 1) or more
// internal nodes with higher depth (and calling itself recursively on them
// until *they* are filled with data). The data to fill the node with is
// provided by DagBuilderHelper.
//
// `node` represents a (sub-)DAG root that is being filled. If called recursively,
// it is `nil`, a new node is created. If it has been called from `Layout` (see
// diagram below) it points to the new root (that increases the depth of the DAG),
// it already has a child (the old root). New children will be added to this new
// root, and those children will in turn be filled (calling `fillNodeRec`
// recursively).
//
//	                    +-------------+
//	                    |   `node`    |
//	                    |  (new root) |
//	                    +-------------+
//	                          |
//	            +-------------+ - - - - - - + - - - - - - - - - - - +
//	            |                           |                       |
//	    +--------------+             + - - - - -  +           + - - - - -  +
//	    |  (old root)  |             |  new child |           |            |
//	    +--------------+             + - - - - -  +           + - - - - -  +
//	            |                          |                        |
//	     +------+------+             + - - + - - - +
//	     |             |             |             |
//	+=========+   +=========+   + - - - - +    + - - - - +
//	| Chunk 1 |   | Chunk 2 |   | Chunk 3 |    | Chunk 4 |
//	+=========+   +=========+   + - - - - +    + - - - - +
//
// The `node` to be filled uses the `FSNodeOverDag` abstraction that allows adding
// child nodes without packing/unpacking the UnixFS layer node (having an internal
// `ft.FSNode` cache).
//
// It returns the `ipld.Node` representation of the passed `node` filled with
// children and the `nodeFileSize` with the total size of the file chunk (leaf)
// nodes stored under this node (parent nodes store this to enable efficient
// seeking through the DAG when reading data later).
//
// warning: **children** pinned indirectly, but input node IS NOT pinned.
func fillNodeRec(db *DagBuilderHelper, node *FSNodeOverDag, depth int, ToEncode [][]byte, enc reedsolomon.Encoder, parity [][]byte, last [][]byte, or int, timetakn time.Duration, timetaknpad time.Duration, first int) (filledNode ipld.Node, nodeFileSize uint64, p [][]byte, e [][]byte, l [][]byte, timetaken time.Duration, timetaknPad time.Duration, err error) {
	if depth < 1 {
		return nil, 0, nil, nil, nil, 0, 0, errors.New("attempt to fillNode at depth < 1")
	}

	if node == nil {
		node = db.NewFSNodeOverDag(ft.TFile)
	}

	fmt.Fprintf(os.Stdout, "Entered bottttt \n")
	// Child node created on every iteration to add to parent `node`.
	// It can be a leaf node or another internal node.
	var childNode ipld.Node
	// File size from the child node needed to update the `FSNode`
	// in `node` when adding the child.
	var childFileSize uint64
	var data []byte
	ff := first
	// While we have room and there is data available to be added.
	for node.NumChildren() < db.Maxlinks() && (!db.Done() || len(parity) > 0 || len(last) > 0 || ff == 1) {
		pp := 0
		ff = 0
		if depth == 1 {
			if len(last) == 0 {
				if len(ToEncode) == or {
					//encode the data and clear to Encode
					fmt.Fprintf(os.Stdout, "Encode %s\n", time.Now().Format("15:04:05.000"))
					parity, timetakn = encodeTest(ToEncode, enc, or, timetakn)
					ToEncode = [][]byte{}
				}
				// Base case: add leaf node with data.
				if len(parity) == 0 {
					childNode, childFileSize, data, err = db.NewLeafDataNode(ft.TFile)
					if err != nil {
						return nil, 0, nil, nil, nil, 0, 0, err
					}
					if db.Done() {
						fmt.Fprintf(os.Stdout, " DDDDOOOOOOONNNNNEEEEEEEEEE \n")
						lastsize := childFileSize
						//manna n3mel padding w n3abbi l to encode w n3mel encode w nkammel
						start := time.Now()
						fmt.Fprintf(os.Stdout, " PPPPPAAAAAAADDDDDDDDDD \n")
						for uint64(len(data)) < db.leafsize {
							data = append(data, 0)
						}
						end := time.Now()
						timetaknpad += end.Sub(start)
						childNode, _ = db.NewLeafNode(data, ft.TFile)
						childFileSize = lastsize
						childNode = db.ProcessFileStore(childNode, childFileSize)
						ToEncode = append(ToEncode, data)
						startt := time.Now()
						fmt.Fprintf(os.Stdout, "Padding %s\n", time.Now().Format("15:04:05.000"))
						for len(ToEncode) < or {
							pad := make([]byte, db.leafsize)
							ToEncode = append(ToEncode, pad)
							last = append(last, pad)
						}
						endd := time.Now()
						timetaknpad += endd.Sub(startt)
						fmt.Fprintf(os.Stdout, "Encode %s\n", time.Now().Format("15:04:05.000"))
						parity, timetakn = encodeTest(ToEncode, enc, or, timetakn)
						last = append(last, parity...)
						parity = [][]byte{}
						ToEncode = [][]byte{}
					} else {
						ToEncode = append(ToEncode, data)
					}

				} else {
					childNode, _ = db.NewLeafNode(parity[0], ft.TFile)
					childFileSize = uint64(len(parity[0]))
					childNode = db.ProcessFileStoreParity(childNode)
					parity = parity[1:]
					pp = 1
				}
			} else {
				childNode, _ = db.NewLeafNode(last[0], ft.TFile)
				childFileSize = uint64(len(last[0]))
				childNode = db.ProcessFileStore(childNode, childFileSize)
				pp = 1
				last = last[1:]
			}
		} else {
			// Recursion case: create an internal node to in turn keep
			// descending in the DAG and adding child nodes to it.
			childNode, childFileSize, parity, ToEncode, last, timetakn, timetaknpad, err = fillNodeRec(db, nil, depth-1, ToEncode, enc, parity, last, or, timetakn, timetaknpad, first)
			if err != nil {
				return nil, 0, nil, nil, nil, 0, 0, err
			}
		}
		if pp != 0 {
			err = node.AddPChild(childNode, childFileSize, db)
			if err != nil {
				return nil, 0, nil, nil, nil, 0, 0, err
			}
		} else {
			err = node.AddChild(childNode, childFileSize, db)
			if err != nil {
				return nil, 0, nil, nil, nil, 0, 0, err
			}

		}
		//fmt.Fprintf(os.Stdout, "finish add child with cid : %s\n", childNode.Cid().String())
	}

	nodeFileSize = node.FileSize()
	// Get the final `dag.ProtoNode` with the `FSNode` data encoded inside.
	filledNode, err = node.Commit()
	if err != nil {
		return nil, 0, nil, nil, nil, 0, 0, err
	}
	return filledNode, nodeFileSize, parity, ToEncode, last, timetakn, timetaknpad, nil
}

func encodeTest(data [][]byte, enc reedsolomon.Encoder, or int, timetaken time.Duration) ([][]byte, time.Duration) {
	var Shard []byte
	for _, shard := range data {
		Shard = append(Shard, shard...)
	}
	shards1, _ := enc.Split(Shard)
	start := time.Now()
	enc.Encode(shards1)
	end := time.Now()
	timetaken += end.Sub(start)
	return shards1[or:], timetaken
}
func checkErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
		os.Exit(2)
	}
}
