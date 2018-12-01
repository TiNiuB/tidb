package tiniub

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

type jsonTable struct {
	tiniubTable
	url string
}

// IterRecords implements table.Table Type interface.
func (vt *jsonTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}

	fmt.Println("运行到了 pd config ...!!")
	rows, err := vt.getRows(ctx, cols)
	if err != nil {
		return errors.Trace(err)
	}
	for i, row := range rows {
		more, err := fn(int64(i), row, cols)
		if err != nil {
			return errors.Trace(err)
		}
		if !more {
			break
		}
	}

	return nil
}

func (vt *jsonTable) getRows(ctx sessionctx.Context, cols []*table.Column) (fullRows [][]types.Datum, err error) {
	resp, err := http.Get(vt.url)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	var fromRows []map[string]interface{}
	dec := json.NewDecoder(resp.Body)
	dec.Decode(&fromRows)

	toRows := make([][]types.Datum, 0, len(fromRows))
	for _, row := range fromRows {
		toRows = vt.decodeOneRow(row, toRows)
	}
	return toRows, nil
}

func (vt *jsonTable) decodeOneRow(from map[string]interface{}, toRows [][]types.Datum) [][]types.Datum {
	colInfos := vt.meta.Columns
	to := make([]types.Datum, len(colInfos))
	for i := 0; i < len(colInfos); i++ {
		key := colInfos[i].Name.L
		if val, ok := from[key]; ok {
			to[i] = types.NewDatum(val)
		}
	}
	toRows = append(toRows, to)
	return toRows
}
