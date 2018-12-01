package tiniub

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
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
	err = dec.Decode(&fromRows)
	if err != nil {
		return nil, errors.Trace(err)
	}

	toRows := make([][]types.Datum, 0, len(fromRows))
	for _, row := range fromRows {
		toRows = vt.decodeOneRow(ctx.GetSessionVars().StmtCtx, row, toRows, cols)
	}
	return toRows, nil
}

func (vt *jsonTable) decodeOneRow(sc *stmtctx.StatementContext, from map[string]interface{}, toRows [][]types.Datum, cols []*table.Column) [][]types.Datum {
	colInfos := vt.meta.Columns
	fmt.Println(" ============================= decode one row", len(cols), len(colInfos))
	to := make([]types.Datum, len(cols))
	for i := 0; i < len(cols); i++ {

		// if cols[i].Name.L != colInfos[i].Name.L {
		// 	fmt.Printf("第 %d 列出， %s %s executor 的跟实际的不一致？\n", i, cols[i].Name.L, colInfos[i].Name.L)
		// }

		key := cols[i].Name.L
		if val, ok := from[key]; ok {
			to[i] = types.NewDatum(val)
			// fmt.Printf("填充列，key  = %s , value = %s \n", key, val)

			var tmp types.Datum
			var err error
			tmp, err = to[i].ConvertTo(sc, &cols[i].FieldType)

			if err == nil {
				to[i] = tmp
			} else {
				fmt.Println("数据类型对不上，", to[i])
			}

		} else {
			fmt.Println("没有填充的 json 里面有,schema 里面没有", key)
		}
	}
	toRows = append(toRows, to)
	return toRows
}
