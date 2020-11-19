// Code generated by execgen; DO NOT EDIT.
// Copyright 2020 The Cockroach Authors.
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
)

// Remove unused warning.
var _ = colexecerror.InternalError

func newBoolAndOrderedAggAlloc(
	allocator *colmem.Allocator, allocSize int64,
) aggregateFuncAlloc {
	return &boolAndOrderedAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

type boolAndOrderedAgg struct {
	orderedAggregateFuncBase
	sawNonNull bool
	vec        []bool
	curAgg     bool
}

var _ AggregateFunc = &boolAndOrderedAgg{}

func (a *boolAndOrderedAgg) Init(groups []bool, vec coldata.Vec) {
	a.orderedAggregateFuncBase.Init(groups, vec)
	a.vec = vec.Bool()
	a.Reset()
}

func (a *boolAndOrderedAgg) Reset() {
	a.orderedAggregateFuncBase.Reset()
	// true indicates whether we are doing an AND aggregate or OR aggregate.
	// For bool_and the true is true and for bool_or the true is false.
	a.curAgg = true
}

func (a *boolAndOrderedAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Bool(), vec.Nulls()
	groups := a.groups
	if sel == nil {
		_ = groups[inputLen-1]
		col = col[:inputLen]
		if nulls.MaybeHasNulls() {
			for i := range col {
				if groups[i] {
					if !a.sawNonNull {
						a.nulls.SetNull(a.curIdx)
					} else {
						a.vec[a.curIdx] = a.curAgg
					}
					a.curIdx++
					a.curAgg = true
					a.sawNonNull = false
				}

				var isNull bool
				isNull = nulls.NullAt(i)
				if !isNull {
					a.curAgg = a.curAgg && col[i]
					a.sawNonNull = true
				}

			}
		} else {
			for i := range col {
				if groups[i] {
					if !a.sawNonNull {
						a.nulls.SetNull(a.curIdx)
					} else {
						a.vec[a.curIdx] = a.curAgg
					}
					a.curIdx++
					a.curAgg = true
					a.sawNonNull = false
				}

				var isNull bool
				isNull = false
				if !isNull {
					a.curAgg = a.curAgg && col[i]
					a.sawNonNull = true
				}

			}
		}
	} else {
		sel = sel[:inputLen]
		if nulls.MaybeHasNulls() {
			for _, i := range sel {
				if groups[i] {
					if !a.sawNonNull {
						a.nulls.SetNull(a.curIdx)
					} else {
						a.vec[a.curIdx] = a.curAgg
					}
					a.curIdx++
					a.curAgg = true
					a.sawNonNull = false
				}

				var isNull bool
				isNull = nulls.NullAt(i)
				if !isNull {
					a.curAgg = a.curAgg && col[i]
					a.sawNonNull = true
				}

			}
		} else {
			for _, i := range sel {
				if groups[i] {
					if !a.sawNonNull {
						a.nulls.SetNull(a.curIdx)
					} else {
						a.vec[a.curIdx] = a.curAgg
					}
					a.curIdx++
					a.curAgg = true
					a.sawNonNull = false
				}

				var isNull bool
				isNull = false
				if !isNull {
					a.curAgg = a.curAgg && col[i]
					a.sawNonNull = true
				}

			}
		}
	}
}

func (a *boolAndOrderedAgg) Flush(outputIdx int) {
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	if !a.sawNonNull {
		a.nulls.SetNull(outputIdx)
	} else {
		a.vec[outputIdx] = a.curAgg
	}
}

type boolAndOrderedAggAlloc struct {
	aggAllocBase
	aggFuncs []boolAndOrderedAgg
}

var _ aggregateFuncAlloc = &boolAndOrderedAggAlloc{}

const sizeOfBoolAndOrderedAgg = int64(unsafe.Sizeof(boolAndOrderedAgg{}))
const boolAndOrderedAggSliceOverhead = int64(unsafe.Sizeof([]boolAndOrderedAgg{}))

func (a *boolAndOrderedAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(boolAndOrderedAggSliceOverhead + sizeOfBoolAndOrderedAgg*a.allocSize)
		a.aggFuncs = make([]boolAndOrderedAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

func newBoolOrOrderedAggAlloc(
	allocator *colmem.Allocator, allocSize int64,
) aggregateFuncAlloc {
	return &boolOrOrderedAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

type boolOrOrderedAgg struct {
	orderedAggregateFuncBase
	sawNonNull bool
	vec        []bool
	curAgg     bool
}

var _ AggregateFunc = &boolOrOrderedAgg{}

func (a *boolOrOrderedAgg) Init(groups []bool, vec coldata.Vec) {
	a.orderedAggregateFuncBase.Init(groups, vec)
	a.vec = vec.Bool()
	a.Reset()
}

func (a *boolOrOrderedAgg) Reset() {
	a.orderedAggregateFuncBase.Reset()
	// false indicates whether we are doing an AND aggregate or OR aggregate.
	// For bool_and the false is true and for bool_or the false is false.
	a.curAgg = false
}

func (a *boolOrOrderedAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Bool(), vec.Nulls()
	groups := a.groups
	if sel == nil {
		_ = groups[inputLen-1]
		col = col[:inputLen]
		if nulls.MaybeHasNulls() {
			for i := range col {
				if groups[i] {
					if !a.sawNonNull {
						a.nulls.SetNull(a.curIdx)
					} else {
						a.vec[a.curIdx] = a.curAgg
					}
					a.curIdx++
					a.curAgg = false
					a.sawNonNull = false
				}

				var isNull bool
				isNull = nulls.NullAt(i)
				if !isNull {
					a.curAgg = a.curAgg || col[i]
					a.sawNonNull = true
				}

			}
		} else {
			for i := range col {
				if groups[i] {
					if !a.sawNonNull {
						a.nulls.SetNull(a.curIdx)
					} else {
						a.vec[a.curIdx] = a.curAgg
					}
					a.curIdx++
					a.curAgg = false
					a.sawNonNull = false
				}

				var isNull bool
				isNull = false
				if !isNull {
					a.curAgg = a.curAgg || col[i]
					a.sawNonNull = true
				}

			}
		}
	} else {
		sel = sel[:inputLen]
		if nulls.MaybeHasNulls() {
			for _, i := range sel {
				if groups[i] {
					if !a.sawNonNull {
						a.nulls.SetNull(a.curIdx)
					} else {
						a.vec[a.curIdx] = a.curAgg
					}
					a.curIdx++
					a.curAgg = false
					a.sawNonNull = false
				}

				var isNull bool
				isNull = nulls.NullAt(i)
				if !isNull {
					a.curAgg = a.curAgg || col[i]
					a.sawNonNull = true
				}

			}
		} else {
			for _, i := range sel {
				if groups[i] {
					if !a.sawNonNull {
						a.nulls.SetNull(a.curIdx)
					} else {
						a.vec[a.curIdx] = a.curAgg
					}
					a.curIdx++
					a.curAgg = false
					a.sawNonNull = false
				}

				var isNull bool
				isNull = false
				if !isNull {
					a.curAgg = a.curAgg || col[i]
					a.sawNonNull = true
				}

			}
		}
	}
}

func (a *boolOrOrderedAgg) Flush(outputIdx int) {
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	if !a.sawNonNull {
		a.nulls.SetNull(outputIdx)
	} else {
		a.vec[outputIdx] = a.curAgg
	}
}

type boolOrOrderedAggAlloc struct {
	aggAllocBase
	aggFuncs []boolOrOrderedAgg
}

var _ aggregateFuncAlloc = &boolOrOrderedAggAlloc{}

const sizeOfBoolOrOrderedAgg = int64(unsafe.Sizeof(boolOrOrderedAgg{}))
const boolOrOrderedAggSliceOverhead = int64(unsafe.Sizeof([]boolOrOrderedAgg{}))

func (a *boolOrOrderedAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(boolOrOrderedAggSliceOverhead + sizeOfBoolOrOrderedAgg*a.allocSize)
		a.aggFuncs = make([]boolOrOrderedAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	a.aggFuncs = a.aggFuncs[1:]
	return f
}