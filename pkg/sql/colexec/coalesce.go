// Copyright 2020 The Cockroach Authors.
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type coalesceProjOp struct {
	colexecop.InitHelper

	allocator *colmem.Allocator
	buffer    *bufferOp
	ops       []colexecop.Operator

	exprIdxs   []int
	outputIdx  int
	outputType *types.T
	output     coldata.Vec
}

func NewCoalesceProjOp(allocator *colmem.Allocator,
	buffer colexecop.Operator,
	ops []colexecop.Operator,
	exprIdxs []int,
	outputIdx int,
	outputType *types.T,
) colexecop.Operator {
	return &coalesceProjOp{
		allocator:  allocator,
		buffer:     buffer.(*bufferOp),
		ops:        ops,
		exprIdxs:   exprIdxs,
		outputIdx:  outputIdx,
		outputType: outputType,
	}
}

func (c *coalesceProjOp) Init(ctx context.Context) {
	if !c.InitHelper.Init(ctx) {
		return
	}
	for i := range c.ops {
		c.ops[i].Init(c.Ctx)
	}
}

func (c *coalesceProjOp) ChildCount(_ bool) int {
	return 1 + len(c.ops)
}

func (c *coalesceProjOp) Child(nth int, _ bool) execinfra.OpNode {
	if nth == 0 {
		return c.buffer
	} else if nth < len(c.ops) {
		return c.ops[nth]
	}

	colexecerror.InternalError(errors.AssertionFailedf("invalid idx %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil

}

func (c *coalesceProjOp) Next() coldata.Batch {
	for {
		fmt.Println("NEXT")
		c.buffer.advance()
		origLen := c.buffer.batch.Length()
		if origLen == 0 {
			return c.buffer.batch
		}

		outputCol := c.buffer.batch.ColVec(c.outputIdx)

		for _, exprIdx := range c.exprIdxs {
			vec := c.buffer.batch.ColVec(exprIdx)
			fmt.Println("VEC VEC", vec.Int64())
		}

		_ = outputCol
	}
}
