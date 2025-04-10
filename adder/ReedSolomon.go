package adder

import (
	"context"
)

type ReedSolomon struct {
	Context   context.Context
	Original  int
	Parity    int
	Chunker   string
	ShardSize uint64
}

func NewRS(ctx context.Context, original int, parity int, shardSize uint64, chunker string) *ReedSolomon {
	return &ReedSolomon{
		Context:   ctx,
		Original:  original,
		Parity:    parity,
		Chunker:   chunker,
		ShardSize: shardSize,
	}
}
