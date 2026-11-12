package e2e

import (
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type definedDef struct {
	name string
	typ  tablestore.DefinedColumnType
}

func (e *env) createTableDefined(t *testing.T, name string, pks []pkDef, defined []definedDef) {
	t.Helper()
	req := new(tablestore.CreateTableRequest)
	meta := new(tablestore.TableMeta)
	meta.TableName = name
	for _, p := range pks {
		meta.AddPrimaryKeyColumn(p.name, p.keyType)
	}
	for _, d := range defined {
		meta.AddDefinedColumn(d.name, d.typ)
	}
	req.TableMeta = meta
	req.TableOption = &tablestore.TableOption{TimeToAlive: -1, MaxVersion: 1}
	req.ReservedThroughput = &tablestore.ReservedThroughput{Readcap: 0, Writecap: 0}
	if _, err := e.client.CreateTable(req); err != nil {
		t.Fatalf("create table %s: %v", name, err)
	}
}

func (e *env) createIndex(t *testing.T, main string, idx *tablestore.IndexMeta, includeBaseData bool) {
	t.Helper()
	req := &tablestore.CreateIndexRequest{
		MainTableName:   main,
		IndexMeta:       idx,
		IncludeBaseData: includeBaseData,
	}
	if _, err := e.client.CreateIndex(req); err != nil {
		t.Fatalf("create index %s: %v", idx.IndexName, err)
	}
}

func (e *env) deleteIndex(t *testing.T, main, index string) {
	t.Helper()
	req := &tablestore.DeleteIndexRequest{MainTableName: main, IndexName: index}
	if _, err := e.client.DeleteIndex(req); err != nil {
		t.Fatalf("delete index %s: %v", index, err)
	}
}

func (e *env) updateRowAttrs(t *testing.T, table string, key *tablestore.PrimaryKey, cols ...tablestore.AttributeColumn) {
	t.Helper()
	change := new(tablestore.UpdateRowChange)
	change.TableName = table
	change.PrimaryKey = key
	for _, col := range cols {
		change.PutColumn(col.ColumnName, col.Value)
	}
	change.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	req := new(tablestore.UpdateRowRequest)
	req.UpdateRowChange = change
	if _, err := e.client.UpdateRow(req); err != nil {
		t.Fatalf("update row: %v", err)
	}
}

func (e *env) deleteRow(t *testing.T, table string, key *tablestore.PrimaryKey) {
	t.Helper()
	change := new(tablestore.DeleteRowChange)
	change.TableName = table
	change.PrimaryKey = key
	change.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	req := new(tablestore.DeleteRowRequest)
	req.DeleteRowChange = change
	if _, err := e.client.DeleteRow(req); err != nil {
		t.Fatalf("delete row: %v", err)
	}
}

func (e *env) rangeAll(t *testing.T, table string, pkNames ...string) []*tablestore.Row {
	t.Helper()
	start := new(tablestore.PrimaryKey)
	end := new(tablestore.PrimaryKey)
	for _, name := range pkNames {
		start.AddPrimaryKeyColumnWithMinValue(name)
		end.AddPrimaryKeyColumnWithMaxValue(name)
	}
	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = table
	criteria.StartPrimaryKey = start
	criteria.EndPrimaryKey = end
	criteria.Direction = tablestore.FORWARD
	criteria.MaxVersion = 1
	req := new(tablestore.GetRangeRequest)
	req.RangeRowQueryCriteria = criteria
	resp, err := e.client.GetRange(req)
	if err != nil {
		t.Fatalf("get range %s: %v", table, err)
	}
	return resp.Rows
}

func intPK(name string, value int64) *tablestore.PrimaryKey {
	key := new(tablestore.PrimaryKey)
	key.AddPrimaryKeyColumn(name, value)
	return key
}

func attr(name string, value interface{}) tablestore.AttributeColumn {
	return tablestore.AttributeColumn{ColumnName: name, Value: value}
}

func rowColumn(row *tablestore.Row, name string) (interface{}, bool) {
	for _, col := range row.Columns {
		if col.ColumnName == name {
			return col.Value, true
		}
	}
	return nil, false
}

func TestSecondaryIndexGlobal(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "idxmain")
	e.createTableDefined(t, name,
		[]pkDef{pk("uid", tablestore.PrimaryKeyType_INTEGER)},
		[]definedDef{
			{"city", tablestore.DefinedColumn_STRING},
			{"age", tablestore.DefinedColumn_INTEGER},
			{"note", tablestore.DefinedColumn_STRING},
		})
	idxName := name + "_idx_city"
	e.createIndex(t, name, &tablestore.IndexMeta{
		IndexName:      idxName,
		Primarykey:     []string{"city"},
		DefinedColumns: []string{"age"},
		IndexType:      tablestore.IT_GLOBAL_INDEX,
	}, false)

	e.putRow(t, name, intPK("uid", 1), attr("city", "bj"), attr("age", int64(10)), attr("note", "n1"))
	e.putRow(t, name, intPK("uid", 2), attr("city", "sh"), attr("age", int64(20)))
	e.putRow(t, name, intPK("uid", 3), attr("city", "bj"), attr("age", int64(30)))

	rows := e.rangeAll(t, idxName, "city", "uid")
	if len(rows) != 3 {
		t.Fatalf("expected 3 index rows, got %d", len(rows))
	}
	wantCity := []string{"bj", "bj", "sh"}
	wantUID := []int64{1, 3, 2}
	for i, row := range rows {
		if row.PrimaryKey.PrimaryKeys[0].Value != wantCity[i] || row.PrimaryKey.PrimaryKeys[1].Value != wantUID[i] {
			t.Fatalf("row %d order mismatch: %v", i, row.PrimaryKey.PrimaryKeys)
		}
		if _, ok := rowColumn(row, "age"); !ok {
			t.Fatalf("row %d missing covering column age", i)
		}
		if _, ok := rowColumn(row, "note"); ok {
			t.Fatalf("row %d should not contain non-covering column note", i)
		}
	}

	key := new(tablestore.PrimaryKey)
	key.AddPrimaryKeyColumn("city", "bj")
	key.AddPrimaryKeyColumn("uid", int64(1))
	cols := columnMap(e.getRow(t, idxName, key, 1))
	if cols["age"] != int64(10) {
		t.Fatalf("index GetRow age mismatch: %v", cols)
	}
}

func TestSecondaryIndexWritePropagation(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "idxprop")
	e.createTableDefined(t, name,
		[]pkDef{pk("uid", tablestore.PrimaryKeyType_INTEGER)},
		[]definedDef{{"city", tablestore.DefinedColumn_STRING}, {"age", tablestore.DefinedColumn_INTEGER}})
	idxName := name + "_idx"
	e.createIndex(t, name, &tablestore.IndexMeta{
		IndexName:      idxName,
		Primarykey:     []string{"city"},
		DefinedColumns: []string{"age"},
		IndexType:      tablestore.IT_GLOBAL_INDEX,
	}, false)

	e.putRow(t, name, intPK("uid", 1), attr("city", "bj"), attr("age", int64(10)))
	rows := e.rangeAll(t, idxName, "city", "uid")
	if len(rows) != 1 || rows[0].PrimaryKey.PrimaryKeys[0].Value != "bj" {
		t.Fatalf("expected single bj entry, got %d rows", len(rows))
	}

	e.updateRowAttrs(t, name, intPK("uid", 1), attr("city", "sh"))
	rows = e.rangeAll(t, idxName, "city", "uid")
	if len(rows) != 1 || rows[0].PrimaryKey.PrimaryKeys[0].Value != "sh" {
		t.Fatalf("expected single sh entry after city change, got %d rows", len(rows))
	}
	if age, _ := rowColumn(rows[0], "age"); age != int64(10) {
		t.Fatalf("age should be preserved across index key change, got %v", age)
	}

	e.updateRowAttrs(t, name, intPK("uid", 1), attr("age", int64(99)))
	rows = e.rangeAll(t, idxName, "city", "uid")
	if age, _ := rowColumn(rows[0], "age"); age != int64(99) {
		t.Fatalf("covering age should update to 99, got %v", age)
	}

	e.deleteRow(t, name, intPK("uid", 1))
	rows = e.rangeAll(t, idxName, "city", "uid")
	if len(rows) != 0 {
		t.Fatalf("expected empty index after delete, got %d rows", len(rows))
	}
}

func TestSecondaryIndexSparse(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "idxsparse")
	e.createTableDefined(t, name,
		[]pkDef{pk("uid", tablestore.PrimaryKeyType_INTEGER)},
		[]definedDef{{"city", tablestore.DefinedColumn_STRING}, {"age", tablestore.DefinedColumn_INTEGER}})
	idxName := name + "_idx"
	e.createIndex(t, name, &tablestore.IndexMeta{
		IndexName:  idxName,
		Primarykey: []string{"city"},
		IndexType:  tablestore.IT_GLOBAL_INDEX,
	}, false)

	e.putRow(t, name, intPK("uid", 1), attr("age", int64(10)))
	e.putRow(t, name, intPK("uid", 2), attr("city", "bj"), attr("age", int64(20)))
	rows := e.rangeAll(t, idxName, "city", "uid")
	if len(rows) != 1 || rows[0].PrimaryKey.PrimaryKeys[1].Value != int64(2) {
		t.Fatalf("expected only uid=2 indexed (sparse), got %d rows", len(rows))
	}

	e.updateRowAttrs(t, name, intPK("uid", 1), attr("city", "sh"))
	rows = e.rangeAll(t, idxName, "city", "uid")
	if len(rows) != 2 {
		t.Fatalf("expected uid=1 to appear once city is set, got %d rows", len(rows))
	}
}

func TestSecondaryIndexLocal(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "idxlocal")
	e.createTableDefined(t, name,
		[]pkDef{pk("pk1", tablestore.PrimaryKeyType_STRING), pk("pk2", tablestore.PrimaryKeyType_INTEGER)},
		[]definedDef{{"tag", tablestore.DefinedColumn_STRING}})
	idxName := name + "_lidx"
	e.createIndex(t, name, &tablestore.IndexMeta{
		IndexName:  idxName,
		Primarykey: []string{"pk1", "tag"},
		IndexType:  tablestore.IT_LOCAL_INDEX,
	}, false)

	put := func(pk1 string, pk2 int64, tag string) {
		key := new(tablestore.PrimaryKey)
		key.AddPrimaryKeyColumn("pk1", pk1)
		key.AddPrimaryKeyColumn("pk2", pk2)
		e.putRow(t, name, key, attr("tag", tag))
	}
	put("a", 1, "x")
	put("a", 2, "y")
	put("b", 1, "x")

	rows := e.rangeAll(t, idxName, "pk1", "tag", "pk2")
	if len(rows) != 3 {
		t.Fatalf("expected 3 local index rows, got %d", len(rows))
	}
	if rows[0].PrimaryKey.PrimaryKeys[0].Value != "a" || rows[0].PrimaryKey.PrimaryKeys[1].Value != "x" {
		t.Fatalf("local index order mismatch: %v", rows[0].PrimaryKey.PrimaryKeys)
	}
}

func TestCreateTableWithIndexAndDescribe(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "idxcreate")
	req := new(tablestore.CreateTableRequest)
	meta := new(tablestore.TableMeta)
	meta.TableName = name
	meta.AddPrimaryKeyColumn("uid", tablestore.PrimaryKeyType_INTEGER)
	meta.AddDefinedColumn("city", tablestore.DefinedColumn_STRING)
	req.TableMeta = meta
	req.TableOption = &tablestore.TableOption{TimeToAlive: -1, MaxVersion: 1}
	req.ReservedThroughput = &tablestore.ReservedThroughput{Readcap: 0, Writecap: 0}
	idxName := name + "_idx"
	req.IndexMetas = []*tablestore.IndexMeta{{
		IndexName:  idxName,
		Primarykey: []string{"city"},
		IndexType:  tablestore.IT_GLOBAL_INDEX,
	}}
	if _, err := e.client.CreateTable(req); err != nil {
		t.Fatalf("create table with index: %v", err)
	}

	e.putRow(t, name, intPK("uid", 7), attr("city", "gz"))
	rows := e.rangeAll(t, idxName, "city", "uid")
	if len(rows) != 1 || rows[0].PrimaryKey.PrimaryKeys[1].Value != int64(7) {
		t.Fatalf("expected index populated from CreateTable, got %d rows", len(rows))
	}

	descReq := &tablestore.DescribeTableRequest{TableName: name}
	desc, err := e.client.DescribeTable(descReq)
	if err != nil {
		t.Fatalf("describe table: %v", err)
	}
	if len(desc.IndexMetas) != 1 || desc.IndexMetas[0].IndexName != idxName {
		t.Fatalf("expected DescribeTable to list index %s, got %+v", idxName, desc.IndexMetas)
	}
	if desc.IndexMetas[0].IndexType != tablestore.IT_GLOBAL_INDEX {
		t.Fatalf("expected GLOBAL index type, got %v", desc.IndexMetas[0].IndexType)
	}
}

func TestDeleteIndex(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "idxdrop")
	e.createTableDefined(t, name,
		[]pkDef{pk("uid", tablestore.PrimaryKeyType_INTEGER)},
		[]definedDef{{"city", tablestore.DefinedColumn_STRING}})
	idxName := name + "_idx"
	e.createIndex(t, name, &tablestore.IndexMeta{
		IndexName:  idxName,
		Primarykey: []string{"city"},
		IndexType:  tablestore.IT_GLOBAL_INDEX,
	}, false)
	e.putRow(t, name, intPK("uid", 1), attr("city", "bj"))
	e.deleteIndex(t, name, idxName)

	start := new(tablestore.PrimaryKey)
	start.AddPrimaryKeyColumnWithMinValue("city")
	start.AddPrimaryKeyColumnWithMinValue("uid")
	end := new(tablestore.PrimaryKey)
	end.AddPrimaryKeyColumnWithMaxValue("city")
	end.AddPrimaryKeyColumnWithMaxValue("uid")
	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = idxName
	criteria.StartPrimaryKey = start
	criteria.EndPrimaryKey = end
	criteria.Direction = tablestore.FORWARD
	criteria.MaxVersion = 1
	req := new(tablestore.GetRangeRequest)
	req.RangeRowQueryCriteria = criteria
	if _, err := e.client.GetRange(req); err == nil {
		t.Fatalf("expected error reading dropped index")
	} else if code := otsErrorCode(t, err); code != "OTSObjectNotExist" {
		t.Fatalf("expected OTSObjectNotExist, got %s", code)
	}

	if cols := columnMap(e.getRow(t, name, intPK("uid", 1), 1)); cols["city"] != "bj" {
		t.Fatalf("main table should remain readable after DeleteIndex: %v", cols)
	}
}

func TestCreateIndexIncludeBaseData(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "idxbackfill")
	e.createTableDefined(t, name,
		[]pkDef{pk("uid", tablestore.PrimaryKeyType_INTEGER)},
		[]definedDef{{"city", tablestore.DefinedColumn_STRING}, {"age", tablestore.DefinedColumn_INTEGER}})
	e.putRow(t, name, intPK("uid", 1), attr("city", "bj"), attr("age", int64(10)))
	e.putRow(t, name, intPK("uid", 2), attr("city", "sh"), attr("age", int64(20)))
	e.putRow(t, name, intPK("uid", 3), attr("age", int64(30)))

	idxName := name + "_idx"
	e.createIndex(t, name, &tablestore.IndexMeta{
		IndexName:      idxName,
		Primarykey:     []string{"city"},
		DefinedColumns: []string{"age"},
		IndexType:      tablestore.IT_GLOBAL_INDEX,
	}, true)

	rows := e.rangeAll(t, idxName, "city", "uid")
	if len(rows) != 2 {
		t.Fatalf("expected 2 backfilled rows (uid=3 sparse), got %d", len(rows))
	}
	if rows[0].PrimaryKey.PrimaryKeys[1].Value != int64(1) || rows[1].PrimaryKey.PrimaryKeys[1].Value != int64(2) {
		t.Fatalf("backfill order mismatch: %v / %v", rows[0].PrimaryKey.PrimaryKeys, rows[1].PrimaryKey.PrimaryKeys)
	}
	if age, _ := rowColumn(rows[0], "age"); age != int64(10) {
		t.Fatalf("backfilled covering age mismatch: %v", age)
	}
}

func TestCreateIndexValidation(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "idxvalid")
	e.createTableDefined(t, name,
		[]pkDef{pk("pk1", tablestore.PrimaryKeyType_STRING), pk("pk2", tablestore.PrimaryKeyType_INTEGER)},
		[]definedDef{{"city", tablestore.DefinedColumn_STRING}})

	badReq := &tablestore.CreateIndexRequest{
		MainTableName: name,
		IndexMeta: &tablestore.IndexMeta{
			IndexName:  name + "_bad",
			Primarykey: []string{"nonexistent"},
			IndexType:  tablestore.IT_GLOBAL_INDEX,
		},
	}
	if _, err := e.client.CreateIndex(badReq); err == nil {
		t.Fatalf("expected error for unknown index column")
	} else if code := otsErrorCode(t, err); code != "OTSParameterInvalid" {
		t.Fatalf("expected OTSParameterInvalid, got %s", code)
	}

	localReq := &tablestore.CreateIndexRequest{
		MainTableName: name,
		IndexMeta: &tablestore.IndexMeta{
			IndexName:  name + "_badlocal",
			Primarykey: []string{"city"},
			IndexType:  tablestore.IT_LOCAL_INDEX,
		},
	}
	if _, err := e.client.CreateIndex(localReq); err == nil {
		t.Fatalf("expected error for local index whose first PK differs from main table")
	} else if code := otsErrorCode(t, err); code != "OTSParameterInvalid" {
		t.Fatalf("expected OTSParameterInvalid for local index, got %s", code)
	}
}

func TestSecondaryIndexAutoIncrement(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "idxauto")
	req := new(tablestore.CreateTableRequest)
	meta := new(tablestore.TableMeta)
	meta.TableName = name
	meta.AddPrimaryKeyColumn("pk1", tablestore.PrimaryKeyType_STRING)
	meta.AddPrimaryKeyColumnOption("pk2", tablestore.PrimaryKeyType_INTEGER, tablestore.AUTO_INCREMENT)
	meta.AddDefinedColumn("city", tablestore.DefinedColumn_STRING)
	req.TableMeta = meta
	req.TableOption = &tablestore.TableOption{TimeToAlive: -1, MaxVersion: 1}
	req.ReservedThroughput = &tablestore.ReservedThroughput{Readcap: 0, Writecap: 0}
	if _, err := e.client.CreateTable(req); err != nil {
		t.Fatalf("create table: %v", err)
	}
	idxName := name + "_idx"
	e.createIndex(t, name, &tablestore.IndexMeta{
		IndexName:  idxName,
		Primarykey: []string{"city"},
		IndexType:  tablestore.IT_GLOBAL_INDEX,
	}, false)

	key := new(tablestore.PrimaryKey)
	key.AddPrimaryKeyColumn("pk1", "a")
	key.AddPrimaryKeyColumnWithAutoIncrement("pk2")
	change := new(tablestore.PutRowChange)
	change.TableName = name
	change.PrimaryKey = key
	change.AddColumn("city", "bj")
	change.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	putReq := new(tablestore.PutRowRequest)
	putReq.PutRowChange = change
	if _, err := e.client.PutRow(putReq); err != nil {
		t.Fatalf("put row: %v", err)
	}

	rows := e.rangeAll(t, idxName, "city", "pk1", "pk2")
	if len(rows) != 1 {
		t.Fatalf("expected 1 index row for auto-inc pk, got %d", len(rows))
	}
	if rows[0].PrimaryKey.PrimaryKeys[0].Value != "bj" || rows[0].PrimaryKey.PrimaryKeys[1].Value != "a" {
		t.Fatalf("index pk mismatch: %v", rows[0].PrimaryKey.PrimaryKeys)
	}
	if _, ok := rows[0].PrimaryKey.PrimaryKeys[2].Value.(int64); !ok {
		t.Fatalf("expected resolved auto-inc pk2 in index, got %v", rows[0].PrimaryKey.PrimaryKeys[2].Value)
	}
}

func TestSecondaryIndexBatchWrite(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "idxbatch")
	e.createTableDefined(t, name,
		[]pkDef{pk("uid", tablestore.PrimaryKeyType_INTEGER)},
		[]definedDef{{"city", tablestore.DefinedColumn_STRING}})
	idxName := name + "_idx"
	e.createIndex(t, name, &tablestore.IndexMeta{
		IndexName:  idxName,
		Primarykey: []string{"city"},
		IndexType:  tablestore.IT_GLOBAL_INDEX,
	}, false)
	e.putRow(t, name, intPK("uid", 1), attr("city", "bj"))
	e.putRow(t, name, intPK("uid", 3), attr("city", "sz"))

	put := new(tablestore.PutRowChange)
	put.TableName = name
	put.PrimaryKey = intPK("uid", 2)
	put.AddColumn("city", "sh")
	put.SetCondition(tablestore.RowExistenceExpectation_IGNORE)

	update := new(tablestore.UpdateRowChange)
	update.TableName = name
	update.PrimaryKey = intPK("uid", 1)
	update.PutColumn("city", "gz")
	update.SetCondition(tablestore.RowExistenceExpectation_IGNORE)

	del := new(tablestore.DeleteRowChange)
	del.TableName = name
	del.PrimaryKey = intPK("uid", 3)
	del.SetCondition(tablestore.RowExistenceExpectation_IGNORE)

	req := new(tablestore.BatchWriteRowRequest)
	req.AddRowChange(put)
	req.AddRowChange(update)
	req.AddRowChange(del)
	req.IsAtomic = true
	resp, err := e.client.BatchWriteRow(req)
	if err != nil {
		t.Fatalf("batch write: %v", err)
	}
	for i, row := range resp.TableToRowsResult[name] {
		if !row.IsSucceed {
			t.Fatalf("batch row %d failed: %+v", i, row)
		}
	}

	rows := e.rangeAll(t, idxName, "city", "uid")
	if len(rows) != 2 {
		t.Fatalf("expected 2 index rows after atomic batch, got %d", len(rows))
	}
	if rows[0].PrimaryKey.PrimaryKeys[0].Value != "gz" || rows[0].PrimaryKey.PrimaryKeys[1].Value != int64(1) {
		t.Fatalf("expected gz/1 first, got %v", rows[0].PrimaryKey.PrimaryKeys)
	}
	if rows[1].PrimaryKey.PrimaryKeys[0].Value != "sh" || rows[1].PrimaryKey.PrimaryKeys[1].Value != int64(2) {
		t.Fatalf("expected sh/2 second, got %v", rows[1].PrimaryKey.PrimaryKeys)
	}
}
