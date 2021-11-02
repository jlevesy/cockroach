package colexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestCoalesceProjOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}

	for _, tc := range []struct {
		tuples     colexectestutils.Tuples
		renderExpr string
		expected   colexectestutils.Tuples
		inputTypes []*types.T
	}{
		{
			tuples:     colexectestutils.Tuples{{nil, nil, 3}, {nil, 2, nil}, {1, nil, nil}, {nil, nil, nil}},
			renderExpr: "COALESCE(@1, @2, @3)",
			expected:   colexectestutils.Tuples{{3}, {2}, {1}, {nil}},
			inputTypes: []*types.T{types.Int, types.Int, types.Int},
		},
	} {
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, tc.expected, colexectestutils.OrderedVerifier, func(inputs []colexecop.Operator) (colexecop.Operator, error) {
			coalesceProjOp, err := colexectestutils.CreateTestProjectingOperator(
				ctx, flowCtx, inputs[0], tc.inputTypes, tc.renderExpr,
				false /* canFallbackToRowexec */, testMemAcc,
			)

			if err != nil {
				return nil, err
			}

			// We will project out the input columns in order to have test
			// cases be less verbose.
			return colexecbase.NewSimpleProjectOp(coalesceProjOp, len(tc.inputTypes)+1, []uint32{uint32(len(tc.inputTypes))}), nil
		})
	}

}
