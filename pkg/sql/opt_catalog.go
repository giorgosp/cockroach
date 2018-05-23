// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
)

// optCatalog implements the opt.Catalog interface over the SchemaResolver
// interface for the use of the new optimizer. The interfaces are simplified to
// only include what the optimizer needs, and certain common lookups are cached
// for faster performance.
type optCatalog struct {
	// resolver needs to be set via a call to init before calling other methods.
	resolver SchemaResolver

	statsCache *stats.TableStatisticsCache

	// wrappers is a cache of table wrappers that's used to satisfy repeated
	// calls to the FindTable method for the same table.
	wrappers map[*sqlbase.TableDescriptor]*optTable
}

var _ opt.Catalog = &optCatalog{}

// init allows the optCatalog wrapper to be inlined.
func (oc *optCatalog) init(statsCache *stats.TableStatisticsCache, resolver SchemaResolver) {
	oc.resolver = resolver
	oc.statsCache = statsCache
}

// FindTable is part of the opt.Catalog interface.
func (oc *optCatalog) FindTable(ctx context.Context, name *tree.TableName) (opt.Table, error) {
	desc, err := ResolveExistingObject(ctx, oc.resolver, name, true /*required*/, requireTableDesc)
	if err != nil {
		return nil, err
	}

	// Check to see if there's already a wrapper for this table descriptor.
	if oc.wrappers == nil {
		oc.wrappers = make(map[*sqlbase.TableDescriptor]*optTable)
	}
	wrapper, ok := oc.wrappers[desc]
	if !ok {
		wrapper = newOptTable(oc.statsCache, desc)
		oc.wrappers[desc] = wrapper
	}
	return wrapper, nil
}

// optTable is a wrapper around sqlbase.TableDescriptor that caches index
// wrappers and maintains a ColumnID => Column mapping for fast lookup.
type optTable struct {
	desc *sqlbase.TableDescriptor

	// primary is the inlined wrapper for the table's primary index.
	primary optIndex

	statsCache *stats.TableStatisticsCache

	// stats is nil until StatisticCount is called. After that it will not be nil,
	// even when there are no statistics.
	stats []optTableStat

	// colMap is a mapping from unique ColumnID to column ordinal within the
	// table. This is a common lookup that needs to be fast.
	colMap map[sqlbase.ColumnID]int

	// wrappers is a cache of index wrappers that's used to satisfy repeated
	// calls to the SecondaryIndex method for the same index.
	wrappers map[*sqlbase.IndexDescriptor]*optIndex
}

var _ opt.Table = &optTable{}

func newOptTable(statsCache *stats.TableStatisticsCache, desc *sqlbase.TableDescriptor) *optTable {
	ot := &optTable{}
	ot.init(statsCache, desc)
	return ot
}

// init allows the optTable wrapper to be inlined.
func (ot *optTable) init(statsCache *stats.TableStatisticsCache, desc *sqlbase.TableDescriptor) {
	ot.desc = desc
	ot.primary.init(ot, &desc.PrimaryIndex)
	ot.statsCache = statsCache
}

// TabName is part of the opt.Table interface.
func (ot *optTable) TabName() opt.TableName {
	return opt.TableName(ot.desc.Name)
}

// IsVirtualTable is part of the opt.Table interface.
func (ot *optTable) IsVirtualTable() bool {
	return ot.desc.IsVirtualTable()
}

// ColumnCount is part of the opt.Table interface.
func (ot *optTable) ColumnCount() int {
	return len(ot.desc.Columns)
}

// Column is part of the opt.Table interface.
func (ot *optTable) Column(i int) opt.Column {
	return &ot.desc.Columns[i]
}

// IndexCount is part of the opt.Table interface.
func (ot *optTable) IndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.Indexes)
}

// Index is part of the opt.Table interface.
func (ot *optTable) Index(i int) opt.Index {
	// Primary index is always 0th index.
	if i == opt.PrimaryIndex {
		return &ot.primary
	}

	// Bias i to account for lack of primary index in Indexes slice.
	desc := &ot.desc.Indexes[i-1]

	// Check to see if there's already a wrapper for this index descriptor.
	if ot.wrappers == nil {
		ot.wrappers = make(map[*sqlbase.IndexDescriptor]*optIndex, len(ot.desc.Indexes))
	}
	wrapper, ok := ot.wrappers[desc]
	if !ok {
		wrapper = newOptIndex(ot, desc)
		ot.wrappers[desc] = wrapper
	}
	return wrapper
}

// StatisticCount is part of the opt.Table interface.
func (ot *optTable) StatisticCount() int {
	if ot.stats != nil {
		return len(ot.stats)
	}
	stats, err := ot.statsCache.GetTableStats(context.TODO(), ot.desc.ID)
	if err != nil {
		// Ignore any error. We still want to be able to run queries even if we lose
		// access to the statistics table.
		// TODO(radu): at least log the error.
		ot.stats = make([]optTableStat, 0)
		return 0
	}
	ot.stats = make([]optTableStat, len(stats))
	n := 0
	for i := range stats {
		// We skip any stats that have columns that don't exist in
		// the table anymore.
		if ot.stats[n].init(ot, stats[i]) {
			n++
		}
	}
	ot.stats = ot.stats[:n]
	return n
}

// Statistic is part of the opt.Table interface.
func (ot *optTable) Statistic(i int) opt.TableStatistic {
	return &ot.stats[i]
}

func (ot *optTable) ensureColMap() {
	if ot.colMap == nil {
		ot.colMap = make(map[sqlbase.ColumnID]int, len(ot.desc.Columns))
		for i := range ot.desc.Columns {
			ot.colMap[ot.desc.Columns[i].ID] = i
		}
	}
}

// lookupColumnOrdinal returns the ordinal of the column with the given ID. A
// cache makes the lookup O(1).
func (ot *optTable) lookupColumnOrdinal(colID sqlbase.ColumnID) int {
	ot.ensureColMap()
	return ot.colMap[colID]
}

// optIndex is a wrapper around sqlbase.IndexDescriptor that caches some
// commonly accessed information and keeps a reference to the table wrapper.
type optIndex struct {
	tab           *optTable
	desc          *sqlbase.IndexDescriptor
	numCols       int
	numUniqueCols int
}

var _ opt.Index = &optIndex{}

func newOptIndex(tab *optTable, desc *sqlbase.IndexDescriptor) *optIndex {
	oi := &optIndex{}
	oi.init(tab, desc)
	return oi
}

// init allows the optIndex wrapper to be inlined.
func (oi *optIndex) init(tab *optTable, desc *sqlbase.IndexDescriptor) {
	oi.tab = tab
	oi.desc = desc
	oi.numCols = len(desc.ColumnIDs) + len(desc.ExtraColumnIDs) + len(desc.StoreColumnIDs)

	// If index is not unique, extra key columns are added.
	oi.numUniqueCols = len(desc.ColumnIDs)
	if !desc.Unique {
		oi.numUniqueCols += len(desc.ExtraColumnIDs)
	}
}

// IdxName is part of the opt.Index interface.
func (oi *optIndex) IdxName() string {
	return oi.desc.Name
}

// ColumnCount is part of the opt.Index interface.
func (oi *optIndex) ColumnCount() int {
	return oi.numCols
}

// UniqueColumnCount is part of the opt.Index interface.
func (oi *optIndex) UniqueColumnCount() int {
	return oi.numUniqueCols
}

// Column is part of the opt.Index interface.
func (oi *optIndex) Column(i int) opt.IndexColumn {
	length := len(oi.desc.ColumnIDs)
	if i < length {
		ord := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
		return opt.IndexColumn{
			Column:     oi.tab.Column(ord),
			Ordinal:    ord,
			Descending: oi.desc.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC,
		}
	}

	i -= length
	length = len(oi.desc.ExtraColumnIDs)
	if i < length {
		ord := oi.tab.lookupColumnOrdinal(oi.desc.ExtraColumnIDs[i])
		return opt.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
	}

	i -= length
	ord := oi.tab.lookupColumnOrdinal(oi.desc.StoreColumnIDs[i])
	return opt.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

type optTableStat struct {
	createdAt      time.Time
	columnOrdinals []int
	rowCount       uint64
	distinctCount  uint64
	nullCount      uint64
}

var _ opt.TableStatistic = &optTableStat{}

func (os *optTableStat) init(tab *optTable, stat *stats.TableStatistic) (ok bool) {
	os.createdAt = stat.CreatedAt
	os.rowCount = stat.RowCount
	os.distinctCount = stat.DistinctCount
	os.nullCount = stat.NullCount
	os.columnOrdinals = make([]int, len(stat.ColumnIDs))
	tab.ensureColMap()
	for i, c := range stat.ColumnIDs {
		var ok bool
		os.columnOrdinals[i], ok = tab.colMap[c]
		if !ok {
			// Column not in table (this is possible if the column was removed since
			// the statistic was calculated).
			return false
		}
	}
	return true
}

// CreatedAt is part of the opt.TableStatistic interface.
func (os *optTableStat) CreatedAt() time.Time {
	return os.createdAt
}

// ColumnCount is part of the opt.TableStatistic interface.
func (os *optTableStat) ColumnCount() int {
	return len(os.columnOrdinals)
}

// ColumnOrdinal is part of the opt.TableStatistic interface.
func (os *optTableStat) ColumnOrdinal(i int) int {
	return os.columnOrdinals[i]
}

// RowCount is part of the opt.TableStatistic interface.
func (os *optTableStat) RowCount() uint64 {
	return os.rowCount
}

// DistinctCount is part of the opt.TableStatistic interface.
func (os *optTableStat) DistinctCount() uint64 {
	return os.distinctCount
}

// NullCount is part of the opt.TableStatistic interface.
func (os *optTableStat) NullCount() uint64 {
	return os.nullCount
}
