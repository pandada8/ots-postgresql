// Package e2e exercises the OTS protocol surface end to end using the official
// Tablestore SDK against an in-process server backed by a real PostgreSQL database.
//
// Set OTS_POSTGRES_TEST_DSN to run, e.g.
//
//	OTS_POSTGRES_TEST_DSN='postgres://postgres:test@127.0.0.1:54321/ots?sslmode=disable' go test ./e2e/
package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/pandada8/ots-postgresql/internal/server"
	"github.com/pandada8/ots-postgresql/internal/storage"
)

const (
	testAccessKeyID     = "e2e-access-key-id"
	testAccessKeySecret = "e2e-access-key-secret"
	testInstanceName    = "e2e-instance"
)

var tableSeq atomic.Int64

type env struct {
	client   *tablestore.TableStoreClient
	engine   *storage.Engine
	endpoint string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("OTS_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("OTS_POSTGRES_TEST_DSN not set; skipping PostgreSQL-backed e2e tests")
	}
	ctx := context.Background()
	engine, err := storage.Open(ctx, dsn, "")
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv := server.New(server.Config{
		Engine:          engine,
		AccessKeyID:     testAccessKeyID,
		AccessKeySecret: testAccessKeySecret,
		InstanceName:    testInstanceName,
		Logger:          logger,
	})
	httpServer := httptest.NewServer(srv.Handler())
	client := tablestore.NewClient(httpServer.URL, testInstanceName, testAccessKeyID, testAccessKeySecret)
	t.Cleanup(func() {
		httpServer.Close()
		engine.Close()
	})
	return &env{client: client, engine: engine, endpoint: httpServer.URL}
}

func (e *env) tableName(t *testing.T, label string) string {
	t.Helper()
	name := fmt.Sprintf("e2e_%s_%d_%d", label, time.Now().UnixNano()%1000000, tableSeq.Add(1))
	t.Cleanup(func() {
		req := new(tablestore.DeleteTableRequest)
		req.TableName = name
		e.client.DeleteTable(req)
	})
	return name
}

func (e *env) createTable(t *testing.T, name string, maxVersion int, ttl int, pks ...pkDef) {
	t.Helper()
	req := new(tablestore.CreateTableRequest)
	meta := new(tablestore.TableMeta)
	meta.TableName = name
	for _, pk := range pks {
		if pk.option != nil {
			meta.AddPrimaryKeyColumnOption(pk.name, pk.keyType, *pk.option)
		} else {
			meta.AddPrimaryKeyColumn(pk.name, pk.keyType)
		}
	}
	req.TableMeta = meta
	req.TableOption = &tablestore.TableOption{TimeToAlive: ttl, MaxVersion: maxVersion}
	req.ReservedThroughput = &tablestore.ReservedThroughput{Readcap: 0, Writecap: 0}
	if _, err := e.client.CreateTable(req); err != nil {
		t.Fatalf("create table %s: %v", name, err)
	}
}

type pkDef struct {
	name    string
	keyType tablestore.PrimaryKeyType
	option  *tablestore.PrimaryKeyOption
}

func pk(name string, keyType tablestore.PrimaryKeyType) pkDef {
	return pkDef{name: name, keyType: keyType}
}

func pkAuto(name string) pkDef {
	opt := tablestore.AUTO_INCREMENT
	return pkDef{name: name, keyType: tablestore.PrimaryKeyType_INTEGER, option: &opt}
}

func stringPK(name, value string) *tablestore.PrimaryKey {
	key := new(tablestore.PrimaryKey)
	key.AddPrimaryKeyColumn(name, value)
	return key
}

func (e *env) putRow(t *testing.T, table string, key *tablestore.PrimaryKey, cols ...tablestore.AttributeColumn) {
	t.Helper()
	change := new(tablestore.PutRowChange)
	change.TableName = table
	change.PrimaryKey = key
	change.Columns = cols
	change.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	req := new(tablestore.PutRowRequest)
	req.PutRowChange = change
	if _, err := e.client.PutRow(req); err != nil {
		t.Fatalf("put row: %v", err)
	}
}

func (e *env) getRow(t *testing.T, table string, key *tablestore.PrimaryKey, maxVersion int32) *tablestore.GetRowResponse {
	t.Helper()
	criteria := new(tablestore.SingleRowQueryCriteria)
	criteria.TableName = table
	criteria.PrimaryKey = key
	criteria.MaxVersion = maxVersion
	req := new(tablestore.GetRowRequest)
	req.SingleRowQueryCriteria = criteria
	resp, err := e.client.GetRow(req)
	if err != nil {
		t.Fatalf("get row: %v", err)
	}
	return resp
}

func columnMap(resp *tablestore.GetRowResponse) map[string]interface{} {
	out := make(map[string]interface{})
	for _, col := range resp.Columns {
		out[col.ColumnName] = col.Value
	}
	return out
}

func otsErrorCode(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	otsErr, ok := err.(*tablestore.OtsError)
	if !ok {
		t.Fatalf("expected *tablestore.OtsError, got %T: %v", err, err)
	}
	return otsErr.Code
}

func TestTableManagement(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "mgmt")
	e.createTable(t, name, 3, -1, pk("pk1", tablestore.PrimaryKeyType_STRING), pk("pk2", tablestore.PrimaryKeyType_INTEGER))

	listResp, err := e.client.ListTable()
	if err != nil {
		t.Fatalf("list table: %v", err)
	}
	found := false
	for _, n := range listResp.TableNames {
		if n == name {
			found = true
		}
	}
	if !found {
		t.Fatalf("table %s not in list %v", name, listResp.TableNames)
	}

	descReq := new(tablestore.DescribeTableRequest)
	descReq.TableName = name
	desc, err := e.client.DescribeTable(descReq)
	if err != nil {
		t.Fatalf("describe table: %v", err)
	}
	if len(desc.TableMeta.SchemaEntry) != 2 {
		t.Fatalf("expected 2 pk columns, got %d", len(desc.TableMeta.SchemaEntry))
	}
	if desc.TableOption.MaxVersion != 3 {
		t.Fatalf("expected max version 3, got %d", desc.TableOption.MaxVersion)
	}

	updateReq := new(tablestore.UpdateTableRequest)
	updateReq.TableName = name
	updateReq.TableOption = &tablestore.TableOption{TimeToAlive: -1, MaxVersion: 5}
	updateReq.ReservedThroughput = &tablestore.ReservedThroughput{Readcap: 0, Writecap: 0}
	if _, err := e.client.UpdateTable(updateReq); err != nil {
		t.Fatalf("update table: %v", err)
	}
	desc, err = e.client.DescribeTable(descReq)
	if err != nil {
		t.Fatalf("describe after update: %v", err)
	}
	if desc.TableOption.MaxVersion != 5 {
		t.Fatalf("expected max version 5 after update, got %d", desc.TableOption.MaxVersion)
	}

	delReq := new(tablestore.DeleteTableRequest)
	delReq.TableName = name
	if _, err := e.client.DeleteTable(delReq); err != nil {
		t.Fatalf("delete table: %v", err)
	}
	if _, err := e.client.DescribeTable(descReq); err == nil {
		t.Fatalf("expected error describing deleted table")
	}
}

func TestPutGetAllTypes(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "types")
	e.createTable(t, name, 1, -1, pk("pk", tablestore.PrimaryKeyType_STRING))
	key := stringPK("pk", "row1")
	e.putRow(t, name, key,
		tablestore.AttributeColumn{ColumnName: "s", Value: "héllo 世界"},
		tablestore.AttributeColumn{ColumnName: "i", Value: int64(-9223372036854775808)},
		tablestore.AttributeColumn{ColumnName: "d", Value: float64(3.14159)},
		tablestore.AttributeColumn{ColumnName: "bt", Value: true},
		tablestore.AttributeColumn{ColumnName: "bf", Value: false},
		tablestore.AttributeColumn{ColumnName: "bin", Value: []byte{0x00, 0xff, 0x10}},
	)
	resp := e.getRow(t, name, key, 1)
	if len(resp.PrimaryKey.PrimaryKeys) != 1 || resp.PrimaryKey.PrimaryKeys[0].Value != "row1" {
		t.Fatalf("primary key mismatch: %+v", resp.PrimaryKey.PrimaryKeys)
	}
	cols := columnMap(resp)
	if cols["s"] != "héllo 世界" {
		t.Fatalf("string mismatch: %v", cols["s"])
	}
	if cols["i"] != int64(-9223372036854775808) {
		t.Fatalf("integer mismatch: %v", cols["i"])
	}
	if cols["d"] != float64(3.14159) {
		t.Fatalf("double mismatch: %v", cols["d"])
	}
	if cols["bt"] != true || cols["bf"] != false {
		t.Fatalf("boolean mismatch: %v %v", cols["bt"], cols["bf"])
	}
	bin, ok := cols["bin"].([]byte)
	if !ok || len(bin) != 3 || bin[0] != 0x00 || bin[1] != 0xff || bin[2] != 0x10 {
		t.Fatalf("binary mismatch: %v", cols["bin"])
	}

	missing := e.getRow(t, name, stringPK("pk", "absent"), 1)
	if len(missing.PrimaryKey.PrimaryKeys) != 0 || len(missing.Columns) != 0 {
		t.Fatalf("expected empty row for absent pk, got %+v", missing)
	}
}

func TestColumnsToGet(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "colget")
	e.createTable(t, name, 1, -1, pk("pk", tablestore.PrimaryKeyType_STRING))
	key := stringPK("pk", "row1")
	e.putRow(t, name, key,
		tablestore.AttributeColumn{ColumnName: "a", Value: int64(1)},
		tablestore.AttributeColumn{ColumnName: "b", Value: int64(2)},
		tablestore.AttributeColumn{ColumnName: "c", Value: int64(3)},
	)
	criteria := new(tablestore.SingleRowQueryCriteria)
	criteria.TableName = name
	criteria.PrimaryKey = key
	criteria.MaxVersion = 1
	criteria.AddColumnToGet("a")
	criteria.AddColumnToGet("c")
	req := new(tablestore.GetRowRequest)
	req.SingleRowQueryCriteria = criteria
	resp, err := e.client.GetRow(req)
	if err != nil {
		t.Fatalf("get row: %v", err)
	}
	cols := columnMap(resp)
	if len(cols) != 2 || cols["a"] != int64(1) || cols["c"] != int64(3) {
		t.Fatalf("columnsToGet mismatch: %v", cols)
	}
}

func TestVersionsAndTimeRange(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "versions")
	e.createTable(t, name, 3, -1, pk("pk", tablestore.PrimaryKeyType_STRING))
	key := stringPK("pk", "row1")
	base := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		update := new(tablestore.UpdateRowChange)
		update.TableName = name
		update.PrimaryKey = key
		update.PutColumnWithTimestamp("v", int64(i), base+int64(i))
		update.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		req := new(tablestore.UpdateRowRequest)
		req.UpdateRowChange = update
		if _, err := e.client.UpdateRow(req); err != nil {
			t.Fatalf("update version %d: %v", i, err)
		}
	}
	resp := e.getRow(t, name, key, 10)
	count := 0
	for _, col := range resp.Columns {
		if col.ColumnName == "v" {
			count++
		}
	}
	if count != 3 {
		t.Fatalf("expected 3 versions (maxVersions=3), got %d", count)
	}
	if resp.Columns[0].Value != int64(4) {
		t.Fatalf("expected latest version 4, got %v", resp.Columns[0].Value)
	}

	criteria := new(tablestore.SingleRowQueryCriteria)
	criteria.TableName = name
	criteria.PrimaryKey = key
	criteria.MaxVersion = 10
	criteria.TimeRange = &tablestore.TimeRange{Start: base + 2, End: base + 4}
	req := new(tablestore.GetRowRequest)
	req.SingleRowQueryCriteria = criteria
	resp, err := e.client.GetRow(req)
	if err != nil {
		t.Fatalf("get row with time range: %v", err)
	}
	var got []int64
	for _, col := range resp.Columns {
		if col.ColumnName == "v" {
			got = append(got, col.Value.(int64))
		}
	}
	if len(got) != 2 || got[0] != 3 || got[1] != 2 {
		t.Fatalf("time range [base+2,base+4) expected versions 3,2 got %v", got)
	}

	criteria.TimeRange = &tablestore.TimeRange{Specific: base + 2}
	resp, err = e.client.GetRow(req)
	if err != nil {
		t.Fatalf("get row specific time: %v", err)
	}
	if len(resp.Columns) != 1 || resp.Columns[0].Value != int64(2) {
		t.Fatalf("specific time expected version 2, got %+v", resp.Columns)
	}
}

func TestTTL(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "ttl")
	e.createTable(t, name, 5, 1, pk("pk", tablestore.PrimaryKeyType_STRING))
	key := stringPK("pk", "row1")
	now := time.Now().UnixMilli()
	e.putRow(t, name, key,
		tablestore.AttributeColumn{ColumnName: "old", Value: int64(1), Timestamp: now - 5000},
		tablestore.AttributeColumn{ColumnName: "fresh", Value: int64(2), Timestamp: now},
	)
	resp := e.getRow(t, name, key, 10)
	cols := columnMap(resp)
	if _, ok := cols["old"]; ok {
		t.Fatalf("expected expired column to be filtered, got %v", cols)
	}
	if cols["fresh"] != int64(2) {
		t.Fatalf("expected fresh column to remain, got %v", cols)
	}
}

func TestRowConditions(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "cond")
	e.createTable(t, name, 1, -1, pk("pk", tablestore.PrimaryKeyType_STRING))
	key := stringPK("pk", "row1")

	change := new(tablestore.PutRowChange)
	change.TableName = name
	change.PrimaryKey = key
	change.AddColumn("v", int64(1))
	change.SetCondition(tablestore.RowExistenceExpectation_EXPECT_NOT_EXIST)
	req := new(tablestore.PutRowRequest)
	req.PutRowChange = change
	if _, err := e.client.PutRow(req); err != nil {
		t.Fatalf("put with expect-not-exist on new row: %v", err)
	}
	if _, err := e.client.PutRow(req); err == nil {
		t.Fatalf("expected condition failure putting existing row with expect-not-exist")
	} else if code := otsErrorCode(t, err); code != "OTSConditionCheckFail" {
		t.Fatalf("expected OTSConditionCheckFail, got %s", code)
	}

	update := new(tablestore.UpdateRowChange)
	update.TableName = name
	update.PrimaryKey = stringPK("pk", "absent")
	update.PutColumn("v", int64(1))
	update.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)
	updateReq := new(tablestore.UpdateRowRequest)
	updateReq.UpdateRowChange = update
	if _, err := e.client.UpdateRow(updateReq); err == nil {
		t.Fatalf("expected condition failure updating absent row with expect-exist")
	} else if code := otsErrorCode(t, err); code != "OTSConditionCheckFail" {
		t.Fatalf("expected OTSConditionCheckFail, got %s", code)
	}

	del := new(tablestore.DeleteRowChange)
	del.TableName = name
	del.PrimaryKey = key
	del.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)
	del.SetColumnCondition(tablestore.NewSingleColumnCondition("v", tablestore.CT_EQUAL, int64(999)))
	delReq := new(tablestore.DeleteRowRequest)
	delReq.DeleteRowChange = del
	if _, err := e.client.DeleteRow(delReq); err == nil {
		t.Fatalf("expected column condition failure")
	} else if code := otsErrorCode(t, err); code != "OTSConditionCheckFail" {
		t.Fatalf("expected OTSConditionCheckFail, got %s", code)
	}

	del.SetColumnCondition(tablestore.NewSingleColumnCondition("v", tablestore.CT_EQUAL, int64(1)))
	if _, err := e.client.DeleteRow(delReq); err != nil {
		t.Fatalf("delete with matching column condition: %v", err)
	}
	resp := e.getRow(t, name, key, 1)
	if len(resp.PrimaryKey.PrimaryKeys) != 0 {
		t.Fatalf("expected row deleted, got %+v", resp.PrimaryKey.PrimaryKeys)
	}
}

func TestUpdateRowOperations(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "update")
	e.createTable(t, name, 3, -1, pk("pk", tablestore.PrimaryKeyType_STRING))
	key := stringPK("pk", "row1")
	ts := time.Now().UnixMilli()
	e.putRow(t, name, key,
		tablestore.AttributeColumn{ColumnName: "keep", Value: "kept"},
		tablestore.AttributeColumn{ColumnName: "del_all", Value: int64(1), Timestamp: ts},
		tablestore.AttributeColumn{ColumnName: "del_all", Value: int64(2), Timestamp: ts + 1},
		tablestore.AttributeColumn{ColumnName: "del_one", Value: int64(1), Timestamp: ts},
		tablestore.AttributeColumn{ColumnName: "del_one", Value: int64(2), Timestamp: ts + 1},
	)

	update := new(tablestore.UpdateRowChange)
	update.TableName = name
	update.PrimaryKey = key
	update.PutColumn("added", "new")
	update.DeleteColumn("del_all")
	update.DeleteColumnWithTimestamp("del_one", ts)
	update.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)
	req := new(tablestore.UpdateRowRequest)
	req.UpdateRowChange = update
	if _, err := e.client.UpdateRow(req); err != nil {
		t.Fatalf("update row: %v", err)
	}

	resp := e.getRow(t, name, key, 10)
	cols := columnMap(resp)
	if cols["keep"] != "kept" || cols["added"] != "new" {
		t.Fatalf("keep/added mismatch: %v", cols)
	}
	if _, ok := cols["del_all"]; ok {
		t.Fatalf("expected del_all removed entirely, got %v", cols)
	}
	var delOneVersions []int64
	for _, col := range resp.Columns {
		if col.ColumnName == "del_one" {
			delOneVersions = append(delOneVersions, col.Value.(int64))
		}
	}
	if len(delOneVersions) != 1 || delOneVersions[0] != 2 {
		t.Fatalf("expected del_one to retain only version 2, got %v", delOneVersions)
	}
}

func TestIncrementAndReturnAfterModify(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "incr")
	e.createTable(t, name, 1, -1, pk("pk", tablestore.PrimaryKeyType_STRING))
	key := stringPK("pk", "row1")
	e.putRow(t, name, key, tablestore.AttributeColumn{ColumnName: "counter", Value: int64(10)})

	update := new(tablestore.UpdateRowChange)
	update.TableName = name
	update.PrimaryKey = key
	update.IncrementColumn("counter", 5)
	update.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	update.SetReturnIncrementValue()
	update.AppendIncrementColumnToReturn("counter")
	req := new(tablestore.UpdateRowRequest)
	req.UpdateRowChange = update
	resp, err := e.client.UpdateRow(req)
	if err != nil {
		t.Fatalf("increment update: %v", err)
	}
	if len(resp.Columns) != 1 || resp.Columns[0].ColumnName != "counter" || resp.Columns[0].Value != int64(15) {
		t.Fatalf("expected returned counter=15, got %+v", resp.Columns)
	}
	got := e.getRow(t, name, key, 1)
	if columnMap(got)["counter"] != int64(15) {
		t.Fatalf("expected persisted counter=15, got %v", columnMap(got))
	}
}

func TestAutoIncrementPrimaryKey(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "autoinc")
	e.createTable(t, name, 1, -1, pk("gid", tablestore.PrimaryKeyType_STRING), pkAuto("id"))

	var generated []int64
	for i := 0; i < 3; i++ {
		key := new(tablestore.PrimaryKey)
		key.AddPrimaryKeyColumn("gid", "group")
		key.AddPrimaryKeyColumnWithAutoIncrement("id")
		change := new(tablestore.PutRowChange)
		change.TableName = name
		change.PrimaryKey = key
		change.AddColumn("v", int64(int64(i)))
		change.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		change.SetReturnPk()
		req := new(tablestore.PutRowRequest)
		req.PutRowChange = change
		resp, err := e.client.PutRow(req)
		if err != nil {
			t.Fatalf("auto-inc put: %v", err)
		}
		if len(resp.PrimaryKey.PrimaryKeys) != 2 {
			t.Fatalf("expected returned pk with 2 columns, got %+v", resp.PrimaryKey.PrimaryKeys)
		}
		id, ok := resp.PrimaryKey.PrimaryKeys[1].Value.(int64)
		if !ok || id <= 0 {
			t.Fatalf("expected positive generated id, got %v", resp.PrimaryKey.PrimaryKeys[1].Value)
		}
		generated = append(generated, id)
	}
	if generated[0] == generated[1] || generated[1] == generated[2] {
		t.Fatalf("expected distinct auto-inc ids, got %v", generated)
	}
	key := new(tablestore.PrimaryKey)
	key.AddPrimaryKeyColumn("gid", "group")
	key.AddPrimaryKeyColumn("id", generated[0])
	resp := e.getRow(t, name, key, 1)
	if columnMap(resp)["v"] != int64(0) {
		t.Fatalf("expected to read back first auto-inc row, got %v", columnMap(resp))
	}
}

func TestBatchGetRow(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "batchget")
	e.createTable(t, name, 1, -1, pk("pk", tablestore.PrimaryKeyType_STRING))
	for i := 0; i < 5; i++ {
		e.putRow(t, name, stringPK("pk", fmt.Sprintf("k%d", i)), tablestore.AttributeColumn{ColumnName: "v", Value: int64(int64(i))})
	}
	criteria := new(tablestore.MultiRowQueryCriteria)
	criteria.TableName = name
	criteria.MaxVersion = 1
	for i := 0; i < 5; i++ {
		criteria.AddRow(stringPK("pk", fmt.Sprintf("k%d", i)))
	}
	criteria.AddRow(stringPK("pk", "absent"))
	req := new(tablestore.BatchGetRowRequest)
	req.MultiRowQueryCriteria = append(req.MultiRowQueryCriteria, criteria)
	resp, err := e.client.BatchGetRow(req)
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	rows := resp.TableToRowsResult[name]
	if len(rows) != 6 {
		t.Fatalf("expected 6 results, got %d", len(rows))
	}
	for i := 0; i < 5; i++ {
		if !rows[i].IsSucceed || len(rows[i].Columns) != 1 || rows[i].Columns[0].Value != int64(int64(i)) {
			t.Fatalf("row %d mismatch: %+v", i, rows[i])
		}
		if rows[i].Index != int32(i) {
			t.Fatalf("row %d index mismatch: %d", i, rows[i].Index)
		}
	}
	if !rows[5].IsSucceed || len(rows[5].Columns) != 0 {
		t.Fatalf("expected absent row to succeed with no columns, got %+v", rows[5])
	}
}

func TestBatchWriteRow(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "batchwrite")
	e.createTable(t, name, 1, -1, pk("pk", tablestore.PrimaryKeyType_STRING))
	e.putRow(t, name, stringPK("pk", "update-me"), tablestore.AttributeColumn{ColumnName: "v", Value: int64(1)})
	e.putRow(t, name, stringPK("pk", "delete-me"), tablestore.AttributeColumn{ColumnName: "v", Value: int64(1)})

	put1 := new(tablestore.PutRowChange)
	put1.TableName = name
	put1.PrimaryKey = stringPK("pk", "put1")
	put1.AddColumn("v", "a")
	put1.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	put1.SetReturnPk()

	put2 := new(tablestore.PutRowChange)
	put2.TableName = name
	put2.PrimaryKey = stringPK("pk", "put2")
	put2.AddColumn("v", "b")
	put2.SetCondition(tablestore.RowExistenceExpectation_IGNORE)

	update := new(tablestore.UpdateRowChange)
	update.TableName = name
	update.PrimaryKey = stringPK("pk", "update-me")
	update.PutColumn("v", int64(2))
	update.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)

	del := new(tablestore.DeleteRowChange)
	del.TableName = name
	del.PrimaryKey = stringPK("pk", "delete-me")
	del.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)

	req := new(tablestore.BatchWriteRowRequest)
	req.AddRowChange(put1)
	req.AddRowChange(put2)
	req.AddRowChange(update)
	req.AddRowChange(del)
	resp, err := e.client.BatchWriteRow(req)
	if err != nil {
		t.Fatalf("batch write: %v", err)
	}
	rows := resp.TableToRowsResult[name]
	if len(rows) != 4 {
		t.Fatalf("expected 4 results, got %d", len(rows))
	}
	for i, row := range rows {
		if !row.IsSucceed {
			t.Fatalf("row %d failed: %+v", i, row)
		}
	}
	if len(rows[0].PrimaryKey.PrimaryKeys) != 1 || rows[0].PrimaryKey.PrimaryKeys[0].Value != "put1" {
		t.Fatalf("expected returned pk for put1, got %+v", rows[0].PrimaryKey.PrimaryKeys)
	}
	if len(rows[1].PrimaryKey.PrimaryKeys) != 0 {
		t.Fatalf("expected no returned pk for put2, got %+v", rows[1].PrimaryKey.PrimaryKeys)
	}
	if columnMap(e.getRow(t, name, stringPK("pk", "update-me"), 1))["v"] != int64(2) {
		t.Fatalf("expected updated value 2")
	}
	if len(e.getRow(t, name, stringPK("pk", "delete-me"), 1).PrimaryKey.PrimaryKeys) != 0 {
		t.Fatalf("expected delete-me removed")
	}
}

func TestBatchWriteRowAtomic(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "batchatomic")
	e.createTable(t, name, 1, -1, pk("pk", tablestore.PrimaryKeyType_STRING))

	good := new(tablestore.PutRowChange)
	good.TableName = name
	good.PrimaryKey = stringPK("pk", "good")
	good.AddColumn("v", "a")
	good.SetCondition(tablestore.RowExistenceExpectation_IGNORE)

	bad := new(tablestore.PutRowChange)
	bad.TableName = name
	bad.PrimaryKey = stringPK("pk", "bad")
	bad.AddColumn("v", "b")
	bad.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)

	req := new(tablestore.BatchWriteRowRequest)
	req.AddRowChange(good)
	req.AddRowChange(bad)
	req.IsAtomic = true
	resp, err := e.client.BatchWriteRow(req)
	if err != nil {
		t.Fatalf("atomic batch write: %v", err)
	}
	rows := resp.TableToRowsResult[name]
	for i, row := range rows {
		if row.IsSucceed {
			t.Fatalf("atomic batch: expected row %d to fail, got %+v", i, row)
		}
	}
	if len(e.getRow(t, name, stringPK("pk", "good"), 1).PrimaryKey.PrimaryKeys) != 0 {
		t.Fatalf("atomic batch should have rolled back the good row")
	}
}

func TestGetRangeForwardBackward(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "range")
	e.createTable(t, name, 1, -1, pk("pk", tablestore.PrimaryKeyType_STRING))
	for i := 0; i < 9; i++ {
		e.putRow(t, name, stringPK("pk", fmt.Sprintf("r%d", i)), tablestore.AttributeColumn{ColumnName: "v", Value: int64(int64(i))})
	}

	start := stringPK("pk", "r1")
	end := stringPK("pk", "r8")
	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = name
	criteria.StartPrimaryKey = start
	criteria.EndPrimaryKey = end
	criteria.Direction = tablestore.FORWARD
	criteria.MaxVersion = 1
	req := new(tablestore.GetRangeRequest)
	req.RangeRowQueryCriteria = criteria
	resp, err := e.client.GetRange(req)
	if err != nil {
		t.Fatalf("get range forward: %v", err)
	}
	if len(resp.Rows) != 7 {
		t.Fatalf("expected 7 rows [r1,r8), got %d", len(resp.Rows))
	}
	if resp.Rows[0].PrimaryKey.PrimaryKeys[0].Value != "r1" || resp.Rows[6].PrimaryKey.PrimaryKeys[0].Value != "r7" {
		t.Fatalf("forward order mismatch: first=%v last=%v", resp.Rows[0].PrimaryKey.PrimaryKeys[0].Value, resp.Rows[6].PrimaryKey.PrimaryKeys[0].Value)
	}
	if resp.NextStartPrimaryKey != nil {
		t.Fatalf("expected no next pk, got %+v", resp.NextStartPrimaryKey)
	}

	criteria.StartPrimaryKey = end
	criteria.EndPrimaryKey = start
	criteria.Direction = tablestore.BACKWARD
	resp, err = e.client.GetRange(req)
	if err != nil {
		t.Fatalf("get range backward: %v", err)
	}
	if len(resp.Rows) != 7 {
		t.Fatalf("expected 7 backward rows, got %d", len(resp.Rows))
	}
	if resp.Rows[0].PrimaryKey.PrimaryKeys[0].Value != "r8" || resp.Rows[6].PrimaryKey.PrimaryKeys[0].Value != "r2" {
		t.Fatalf("backward order mismatch: first=%v last=%v", resp.Rows[0].PrimaryKey.PrimaryKeys[0].Value, resp.Rows[6].PrimaryKey.PrimaryKeys[0].Value)
	}
}

func TestGetRangeInfAndPagination(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "rangepage")
	e.createTable(t, name, 1, -1, pk("pk", tablestore.PrimaryKeyType_INTEGER))
	for i := 0; i < 10; i++ {
		key := new(tablestore.PrimaryKey)
		key.AddPrimaryKeyColumn("pk", int64(i))
		e.putRow(t, name, key, tablestore.AttributeColumn{ColumnName: "v", Value: int64(int64(i))})
	}
	start := new(tablestore.PrimaryKey)
	start.AddPrimaryKeyColumnWithMinValue("pk")
	end := new(tablestore.PrimaryKey)
	end.AddPrimaryKeyColumnWithMaxValue("pk")

	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = name
	criteria.StartPrimaryKey = start
	criteria.EndPrimaryKey = end
	criteria.Direction = tablestore.FORWARD
	criteria.MaxVersion = 1
	criteria.Limit = 3
	req := new(tablestore.GetRangeRequest)
	req.RangeRowQueryCriteria = criteria

	var collected []int64
	pages := 0
	for {
		resp, err := e.client.GetRange(req)
		if err != nil {
			t.Fatalf("get range page: %v", err)
		}
		for _, row := range resp.Rows {
			collected = append(collected, row.PrimaryKey.PrimaryKeys[0].Value.(int64))
		}
		pages++
		if resp.NextStartPrimaryKey == nil {
			break
		}
		criteria.StartPrimaryKey = resp.NextStartPrimaryKey
		if pages > 10 {
			t.Fatalf("pagination did not terminate")
		}
	}
	if len(collected) != 10 {
		t.Fatalf("expected 10 rows across pages, got %d: %v", len(collected), collected)
	}
	for i, v := range collected {
		if v != int64(i) {
			t.Fatalf("expected ascending order, got %v", collected)
		}
	}
	if pages < 2 {
		t.Fatalf("expected multiple pages with limit 3, got %d", pages)
	}
}

func TestGetRangeFilter(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "rangefilter")
	e.createTable(t, name, 1, -1, pk("pk", tablestore.PrimaryKeyType_INTEGER))
	for i := 0; i < 10; i++ {
		key := new(tablestore.PrimaryKey)
		key.AddPrimaryKeyColumn("pk", int64(i))
		e.putRow(t, name, key, tablestore.AttributeColumn{ColumnName: "v", Value: int64(int64(i))})
	}
	start := new(tablestore.PrimaryKey)
	start.AddPrimaryKeyColumnWithMinValue("pk")
	end := new(tablestore.PrimaryKey)
	end.AddPrimaryKeyColumnWithMaxValue("pk")
	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = name
	criteria.StartPrimaryKey = start
	criteria.EndPrimaryKey = end
	criteria.Direction = tablestore.FORWARD
	criteria.MaxVersion = 1
	filter := tablestore.NewSingleColumnCondition("v", tablestore.CT_GREATER_EQUAL, int64(5))
	filter.FilterIfMissing = true
	filter.LatestVersionOnly = true
	criteria.Filter = filter
	req := new(tablestore.GetRangeRequest)
	req.RangeRowQueryCriteria = criteria
	resp, err := e.client.GetRange(req)
	if err != nil {
		t.Fatalf("get range with filter: %v", err)
	}
	if len(resp.Rows) != 5 {
		t.Fatalf("expected 5 rows with v>=5, got %d", len(resp.Rows))
	}
	for _, row := range resp.Rows {
		if row.Columns[0].Value.(int64) < 5 {
			t.Fatalf("filter leaked row with v<5: %+v", row)
		}
	}
}

func TestMultiColumnPrimaryKeyRange(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "multipk")
	e.createTable(t, name, 1, -1, pk("gid", tablestore.PrimaryKeyType_STRING), pk("id", tablestore.PrimaryKeyType_INTEGER))
	for _, gid := range []string{"a", "b"} {
		for i := 0; i < 3; i++ {
			key := new(tablestore.PrimaryKey)
			key.AddPrimaryKeyColumn("gid", gid)
			key.AddPrimaryKeyColumn("id", int64(i))
			e.putRow(t, name, key, tablestore.AttributeColumn{ColumnName: "v", Value: gid})
		}
	}
	start := new(tablestore.PrimaryKey)
	start.AddPrimaryKeyColumn("gid", "a")
	start.AddPrimaryKeyColumnWithMinValue("id")
	end := new(tablestore.PrimaryKey)
	end.AddPrimaryKeyColumn("gid", "a")
	end.AddPrimaryKeyColumnWithMaxValue("id")
	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = name
	criteria.StartPrimaryKey = start
	criteria.EndPrimaryKey = end
	criteria.Direction = tablestore.FORWARD
	criteria.MaxVersion = 1
	req := new(tablestore.GetRangeRequest)
	req.RangeRowQueryCriteria = criteria
	resp, err := e.client.GetRange(req)
	if err != nil {
		t.Fatalf("multi-pk range: %v", err)
	}
	if len(resp.Rows) != 3 {
		t.Fatalf("expected 3 rows in group a, got %d", len(resp.Rows))
	}
	for _, row := range resp.Rows {
		if row.PrimaryKey.PrimaryKeys[0].Value != "a" {
			t.Fatalf("range leaked other group: %+v", row.PrimaryKey.PrimaryKeys)
		}
	}
}

func TestAuthRejection(t *testing.T) {
	e := newEnv(t)
	name := e.tableName(t, "auth")
	e.createTable(t, name, 1, -1, pk("pk", tablestore.PrimaryKeyType_STRING))

	badClient := tablestore.NewClient(e.endpoint, testInstanceName, testAccessKeyID, "wrong-secret")
	criteria := new(tablestore.SingleRowQueryCriteria)
	criteria.TableName = name
	criteria.PrimaryKey = stringPK("pk", "x")
	criteria.MaxVersion = 1
	req := new(tablestore.GetRowRequest)
	req.SingleRowQueryCriteria = criteria
	if _, err := badClient.GetRow(req); err == nil {
		t.Fatalf("expected auth failure with wrong secret")
	} else if code := otsErrorCode(t, err); code != "OTSAuthFailed" {
		t.Fatalf("expected OTSAuthFailed, got %s", code)
	}
}

func TestTableNotExist(t *testing.T) {
	e := newEnv(t)
	criteria := new(tablestore.SingleRowQueryCriteria)
	criteria.TableName = "e2e_no_such_table_xyz"
	criteria.PrimaryKey = stringPK("pk", "x")
	criteria.MaxVersion = 1
	req := new(tablestore.GetRowRequest)
	req.SingleRowQueryCriteria = criteria
	if _, err := e.client.GetRow(req); err == nil {
		t.Fatalf("expected error for missing table")
	} else if code := otsErrorCode(t, err); code != "OTSObjectNotExist" {
		t.Fatalf("expected OTSObjectNotExist, got %s", code)
	}
}
