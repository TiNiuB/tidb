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

	pdAPI := map[string]string{
		"pd_config":      "http://127.0.0.1:8080/pd_config",
		"pd_region_peer": "http://127.0.0.1:8080/pd_region_peer",
		"pd_region":      "http://127.0.0.1:8080/pd_region",
		"pd_store_label": "http://127.0.0.1:8080/pd_store_label",
		"pd_store":       "http://127.0.0.1:8080/pd_store",
		"service":        "http://127.0.0.1:8080/service",
		"cpu":            "http://127.0.0.1:8080/psutil_cpu",
		"memory":         "http://127.0.0.1:8080/psutil_memory",
	}

	osQuery := make(map[string]string)
	strs := []string{"acpi_tables", "apt_sources", "arp_cache", "augeas", "authorized_keys", "block_devices", "carbon_black_info", "carves", "chrome_extensions", "cpu_time", "cpuid", "crontab", "curl", "curl_certificate", "deb_packages", "device_file", "device_hash", "device_partitions", "disk_encryption", "dns_resolvers", "elf_dynamic", "elf_info", "elf_sections", "elf_segments", "elf_symbols", "etc_hosts", "etc_protocols", "etc_services", "file_events", "firefox_addons", "groups", "hardware_events", "hash", "intel_me_info", "interface_addresses", "interface_details", "iptables", "kernel_info", "kernel_integrity", "kernel_modules", "known_hosts", "last", "listening_ports", "lldp_neighbors", "load_average", "logged_in_users", "magic", "md_devices", "md_drives", "md_personalities", "memory_array_mapped_addresses", "memory_arrays", "memory_device_mapped_addresses", "memory_devices", "memory_error_info", "memory_info", "memory_map", "mounts", "msr", "npm_packages", "opera_extensions", "os_version", "osquery_events", "osquery_extensions", "osquery_flags", "osquery_info", "osquery_packs", "osquery_registry", "osquery_schedule", "pci_devices", "pd_config", "pd_region", "pd_region_peer", "pd_store", "pd_store_label", "platform_info", "portage_keywords", "portage_packages", "portage_use", "process_envs", "process_events", "process_file_events", "process_memory_map", "process_namespaces", "process_open_files", "process_open_sockets", "processes", "prometheus_metrics", "python_packages", "routes", "selinux_events", "shadow", "shared_memory", "shell_history", "smart_drive_info", "smbios_tables", "socket_events", "ssh_configs", "sudoers", "suid_bin", "syslog_events", "system_controls", "system_info", "time", "ulimit_info", "uptime", "usb_devices", "user_events", "user_groups", "user_ssh_keys", "users", "yara", "yara_events", "yum_sources"}
	for _, str := range strs {
		osQuery[str] = "http://127.0.0.1:8080/osquery_" + str
	}

	var t table.Table
	if url, ok := pdAPI[meta.Name.L]; ok {
		tmp := &jsonTable{
			url: url,
		}
		tmp.init(meta, columns)
		t = tmp
	} else if url, ok := osQuery[meta.Name.L]; ok {
		tmp := &jsonTable{
			url: url,
		}
		tmp.init(meta, columns)
		t = tmp
	} else {
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
