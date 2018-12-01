// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tiniub

import (
	"fmt"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

var _ table.Table = &tiniubTable{}

// tiniubTable stands for the fake table all its data is in the memory.
type tiniubTable struct {
	meta *model.TableInfo
	cols []*table.Column
}

// createTiniubTable creates all tiniubTables
func TableFromMeta(alloc autoid.Allocator, meta *model.TableInfo) (table.Table, error) {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}

	var t table.Table
	switch meta.Name.L {
	case "pd_config":
		tmp := &pdTable{}
		tmp.init(meta, columns)
		t = tmp
		fmt.Println("!!!!!!!!!!!!! 创建 pd table")
	default:
		tmp := &tiniubTable{
			meta: meta,
			cols: columns,
		}
		tmp.init(meta, columns)
		t = tmp
	}
	fmt.Println(" table from meta 来了一个 tinibb 的表 physicalID = ", meta.ID)
	return t, nil
}

func (vt *tiniubTable) init(meta *model.TableInfo, cols []*table.Column) {
	vt.meta = meta
	vt.cols = cols
}

// IterRecords implements table.Table Type interface.
func (vt *tiniubTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}

	return nil
}

// RowWithCols implements table.Table Type interface.
func (vt *tiniubTable) RowWithCols(ctx sessionctx.Context, h int64, cols []*table.Column) ([]types.Datum, error) {

	panic("not support")
	return nil, table.ErrUnsupportedOp
}

// Row implements table.Table Type interface.
func (vt *tiniubTable) Row(ctx sessionctx.Context, h int64) ([]types.Datum, error) {

	panic("not support")
	return nil, table.ErrUnsupportedOp
}

// Cols implements table.Table Type interface.
func (vt *tiniubTable) Cols() []*table.Column {
	return vt.cols
}

// WritableCols implements table.Table Type interface.
func (vt *tiniubTable) WritableCols() []*table.Column {
	return vt.cols
}

// Indices implements table.Table Type interface.
func (vt *tiniubTable) Indices() []table.Index {

	panic("not support")
	return nil
}

// WritableIndices implements table.Table Type interface.
func (vt *tiniubTable) WritableIndices() []table.Index {

	panic("not support")
	return nil
}

// DeletableIndices implements table.Table Type interface.
func (vt *tiniubTable) DeletableIndices() []table.Index {

	panic("not support")
	return nil
}

// RecordPrefix implements table.Table Type interface.
func (vt *tiniubTable) RecordPrefix() kv.Key {

	panic("not support")
	return nil
}

// IndexPrefix implements table.Table Type interface.
func (vt *tiniubTable) IndexPrefix() kv.Key {

	panic("not support")
	return nil
}

// FirstKey implements table.Table Type interface.
func (vt *tiniubTable) FirstKey() kv.Key {

	panic("not support")
	return nil
}

// RecordKey implements table.Table Type interface.
func (vt *tiniubTable) RecordKey(h int64) kv.Key {

	panic("not support")
	return nil
}

// AddRecord implements table.Table Type interface.
func (vt *tiniubTable) AddRecord(ctx sessionctx.Context, r []types.Datum, skipHandleCheck bool) (recordID int64, err error) {

	panic("not support")
	return 0, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table Type interface.
func (vt *tiniubTable) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {

	panic("not support")
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table Type interface.
func (vt *tiniubTable) UpdateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, touched []bool) error {

	panic("not support")
	return table.ErrUnsupportedOp
}

// AllocAutoID implements table.Table Type interface.
func (vt *tiniubTable) AllocAutoID(ctx sessionctx.Context) (int64, error) {

	panic("not support")
	return 0, table.ErrUnsupportedOp
}

// Allocator implements table.Table Type interface.
func (vt *tiniubTable) Allocator(ctx sessionctx.Context) autoid.Allocator {

	panic("not support")
	return nil
}

// RebaseAutoID implements table.Table Type interface.
func (vt *tiniubTable) RebaseAutoID(ctx sessionctx.Context, newBase int64, isSetStep bool) error {

	panic("not support")
	return table.ErrUnsupportedOp
}

// Meta implements table.Table Type interface.
func (vt *tiniubTable) Meta() *model.TableInfo {
	if vt.meta == nil {
		fmt.Println("这里不科学呀，怎么可能没有设置呢", vt)
	}
	return vt.meta
}

// GetID implements table.Table GetID interface.
func (vt *tiniubTable) GetPhysicalID() int64 {

	panic("not support")
	return vt.meta.ID
}

// Seek implements table.Table Type interface.
func (vt *tiniubTable) Seek(ctx sessionctx.Context, h int64) (int64, bool, error) {

	panic("not support")
	return 0, false, table.ErrUnsupportedOp
}

// Type implements table.Table Type interface.
func (vt *tiniubTable) Type() table.Type {
	return table.VirtualTable
}
