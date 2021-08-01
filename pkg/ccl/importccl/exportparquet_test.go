// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExportParquet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t AS VALUES (1, 2), (3, 4)`)
	sqlDB.Exec(t, `EXPORT INTO PARQUET 'nodelocal://0/t/output.parquet' FROM SELECT * FROM t`)

	f, err := os.Open(filepath.Join(dir, "t", "output.parquet"))
	require.NoError(t, err)

	defer f.Close()

	fr, err := goparquet.NewFileReader(f)
	require.NoError(t, err)

	//TODO(jly): assert parquet schema correctness

	var (
		gotRows  []map[string]interface{}
		wantRows = []map[string]interface{}{
			{
				"0": int32(1),
				"1": int32(2),
			},
			map[string]interface{}{
				"0": int32(3),
				"1": int32(4),
			},
		}
	)

	for {
		row, err := fr.NextRow()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		gotRows = append(gotRows, row)
	}

	assert.Equal(t, wantRows, gotRows)
}
