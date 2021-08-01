// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

type parquetWriterProcessor struct {
	flowCtx     *execinfra.FlowCtx
	processorID int32
	spec        execinfrapb.ParquetWriterSpec
	input       execinfra.RowSource
	out         execinfra.ProcOutputHelper
	output      execinfra.RowReceiver
}

func newParquetWriterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ParquetWriterSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	p := &parquetWriterProcessor{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		input:       input,
		output:      output,
	}

	semaCtx := tree.MakeSemaContext()
	if err := p.out.Init(&execinfrapb.PostProcessSpec{}, p.OutputTypes(), &semaCtx, flowCtx.NewEvalCtx()); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *parquetWriterProcessor) OutputTypes() []*types.T {
	res := make([]*types.T, len(colinfo.ExportColumns))
	for i := range res {
		res[i] = colinfo.ExportColumns[i].Typ
	}
	return res
}

func (p *parquetWriterProcessor) MustBeStreaming() bool {
	return false
}

func (p *parquetWriterProcessor) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "parquetWriter")
	defer span.Finish()

	err := func() error {
		var (
			alloc     rowenc.DatumAlloc
			out       bytes.Buffer
			totalRows int

			types = p.input.OutputTypes()
		)

		p.input.Start(ctx)
		input := execinfra.MakeNoMetadataRowSource(p.input, p.output)

		writer, err := newParquetWriter(&out, types, p.spec)
		if err != nil {
			return err
		}

		for {
			row, err := input.NextRow()
			if err != nil {
				return err
			}

			if row == nil {
				break
			}

			parquetRow := make(map[string]interface{}, len(row))

			for i, ed := range row {
				if ed.IsNull() {
					return errors.New("Unsupported null value in parquet exporter")
				}

				if err := ed.EnsureDecoded(types[i], &alloc); err != nil {
					return err
				}

				pv, err := toParquetValue(ed.Datum)
				if err != nil {
					return err
				}

				parquetRow[strconv.Itoa(i)] = pv
			}

			if err := writer.AddData(parquetRow); err != nil {
				return err
			}

			totalRows++
		}

		if err := writer.Close(); err != nil {
			return err
		}

		var (
			filename = path.Base(p.spec.Destination)
			dest, _  = path.Split(p.spec.Destination)
			size     = out.Len()
		)

		conf, err := cloud.ExternalStorageConfFromURI(dest, p.spec.User())
		if err != nil {
			return err
		}

		es, err := p.flowCtx.Cfg.ExternalStorage(ctx, conf)
		if err != nil {
			return err
		}

		defer es.Close()

		if err := cloud.WriteFile(ctx, es, filename, bytes.NewReader(out.Bytes())); err != nil {
			return err
		}

		return p.writeResult(ctx, filename, totalRows, size)
	}()

	execinfra.DrainAndClose(
		ctx, p.output, err, func(context.Context) {}, p.input)
}

func (p *parquetWriterProcessor) writeResult(ctx context.Context, filename string, rows, size int) error {
	res := rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(
			types.String,
			tree.NewDString(filename),
		),
		rowenc.DatumToEncDatum(
			types.Int,
			tree.NewDInt(tree.DInt(rows)),
		),
		rowenc.DatumToEncDatum(
			types.Int,
			tree.NewDInt(tree.DInt(size)),
		),
	}

	cs, err := p.out.EmitRow(ctx, res, p.output)
	if err != nil {
		return err
	}
	if cs != execinfra.NeedMoreRows {
		return errors.New("unexpected closure of consumer")
	}

	return nil
}

func newParquetWriter(out io.Writer, outTypes []*types.T, spec execinfrapb.ParquetWriterSpec) (*goparquet.FileWriter, error) {
	//TODO(jly) we don't have schema information at this point, only returned types.
	schema, err := buildParquetSchema(outTypes)
	if err != nil {
		return nil, err
	}

	//TODO(jly): naive implementation, we need to deal with  all the options we want to offer here.
	return goparquet.NewFileWriter(
		out,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schema),
	), nil
}

func toParquetValue(v tree.Datum) (interface{}, error) {
	switch vv := v.(type) {
	case *tree.DInt:
		return int32(*vv), nil
	default:
		return nil, fmt.Errorf("unsupported datum type %T", v)
	}
}

func buildParquetSchema(outTypes []*types.T) (*parquetschema.SchemaDefinition, error) {
	columns := make([]*parquetschema.ColumnDefinition, len(outTypes))

	for i, t := range outTypes {
		sce, err := toSchemaElement(i, t)
		if err != nil {
			return nil, err
		}

		columns[i] = &parquetschema.ColumnDefinition{SchemaElement: sce}
	}

	return parquetschema.SchemaDefinitionFromColumnDefinition(
		&parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{},
			Children:      columns,
		},
	), nil
}

func toSchemaElement(idx int, t *types.T) (*parquet.SchemaElement, error) {
	switch t.Family() {
	case types.IntFamily:
		return &parquet.SchemaElement{
			Type:           parquet.TypePtr(parquet.Type_INT32),
			TypeLength:     int32ptr(int32(32)),
			RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			//TODO(jly) Use index as field name for now, we don't have schema information.
			Name: strconv.Itoa(idx),
			LogicalType: &parquet.LogicalType{
				INTEGER: &parquet.IntType{
					BitWidth: int8(32),
					IsSigned: true,
				},
			},
		}, nil
	// TODO(jly) support more types, goal is to prove that it is possible right now.
	default:
		return nil, fmt.Errorf("unsuported type familly type %q", t.Family())
	}
}

func int32ptr(i int32) *int32 {
	return &i
}

func init() {
	rowexec.NewParquetWriterProcessor = newParquetWriterProcessor
}
