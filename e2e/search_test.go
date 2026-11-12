package e2e

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"github.com/golang/protobuf/proto"
)

const searchTimeFormat = "2006-01-02 15:04:05.000"

func searchField(name string, fieldType tablestore.FieldType) *tablestore.FieldSchema {
	return &tablestore.FieldSchema{
		FieldName:        proto.String(name),
		FieldType:        fieldType,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
}

func (e *env) createWalletSearchIndex(t *testing.T, table, index string) {
	t.Helper()
	dateField := searchField("created_at", tablestore.FieldType_DATE)
	dateField.DateFormats = []string{"yyyy-MM-dd HH:mm:ss.SSS"}
	schema := &tablestore.IndexSchema{
		FieldSchemas: []*tablestore.FieldSchema{
			searchField("user_id", tablestore.FieldType_KEYWORD),
			searchField("session_id", tablestore.FieldType_KEYWORD),
			searchField("turn_id", tablestore.FieldType_KEYWORD),
			searchField("amount", tablestore.FieldType_LONG),
			dateField,
			searchField("description", tablestore.FieldType_KEYWORD),
		},
		IndexSort: &search.Sort{Sorters: []search.Sorter{
			search.NewFieldSort("created_at", search.SortOrder_DESC),
		}},
	}
	req := &tablestore.CreateSearchIndexRequest{
		TableName:   table,
		IndexName:   index,
		IndexSchema: schema,
	}
	if _, err := e.client.CreateSearchIndex(req); err != nil {
		t.Fatalf("create search index: %v", err)
	}
}

func walletRow(id, userID, sessionID, turnID string, amount int64, createdAt time.Time) (*tablestore.PrimaryKey, []tablestore.AttributeColumn) {
	key := stringPK("transaction_id", id)
	cols := []tablestore.AttributeColumn{
		attr("user_id", userID),
		attr("session_id", sessionID),
		attr("turn_id", turnID),
		attr("amount", amount),
		attr("created_at", createdAt.Format(searchTimeFormat)),
		attr("description", "test "+id),
	}
	return key, cols
}

func searchRequest(table, index string, query search.SearchQuery) *tablestore.SearchRequest {
	req := new(tablestore.SearchRequest)
	req.SetTableName(table)
	req.SetIndexName(index)
	req.SetSearchQuery(query)
	req.SetColumnsToGet(&tablestore.ColumnsToGet{ReturnAll: true})
	return req
}

func searchPKs(rows []*tablestore.Row) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		for _, pk := range row.PrimaryKey.PrimaryKeys {
			if pk.ColumnName == "transaction_id" {
				ids = append(ids, pk.Value.(string))
			}
		}
	}
	return ids
}

func TestSearchIndexLifecycle(t *testing.T) {
	e := newEnv(t)
	table := e.tableName(t, "si_life")
	e.createTable(t, table, 1, -1, pk("transaction_id", tablestore.PrimaryKeyType_STRING))
	e.createWalletSearchIndex(t, table, "si_query")

	listResp, err := e.client.ListSearchIndex(&tablestore.ListSearchIndexRequest{TableName: table})
	if err != nil {
		t.Fatalf("list search index: %v", err)
	}
	if len(listResp.IndexInfo) != 1 || listResp.IndexInfo[0].IndexName != "si_query" {
		t.Fatalf("unexpected search index list: %+v", listResp.IndexInfo)
	}

	descResp, err := e.client.DescribeSearchIndex(&tablestore.DescribeSearchIndexRequest{
		TableName:       table,
		IndexName:       "si_query",
		IncludeSyncStat: proto.Bool(true),
	})
	if err != nil {
		t.Fatalf("describe search index: %v", err)
	}
	if descResp.SyncStat == nil || descResp.SyncStat.SyncPhase != tablestore.SyncPhase_INCR {
		t.Fatalf("expected INCR sync phase, got %+v", descResp.SyncStat)
	}
	if len(descResp.Schema.FieldSchemas) != 6 {
		t.Fatalf("expected 6 field schemas, got %d", len(descResp.Schema.FieldSchemas))
	}
	for _, field := range descResp.Schema.FieldSchemas {
		if field.FieldName != nil && *field.FieldName == "created_at" {
			if field.FieldType != tablestore.FieldType_DATE || len(field.DateFormats) != 1 {
				t.Fatalf("unexpected created_at schema: %+v", field)
			}
		}
	}
	if descResp.Schema.IndexSort == nil || len(descResp.Schema.IndexSort.Sorters) != 1 {
		t.Fatalf("expected index sort with one sorter, got %+v", descResp.Schema.IndexSort)
	}

	if _, err := e.client.DeleteSearchIndex(&tablestore.DeleteSearchIndexRequest{TableName: table, IndexName: "si_query"}); err != nil {
		t.Fatalf("delete search index: %v", err)
	}
	listResp, err = e.client.ListSearchIndex(&tablestore.ListSearchIndexRequest{TableName: table})
	if err != nil {
		t.Fatalf("list search index after delete: %v", err)
	}
	if len(listResp.IndexInfo) != 0 {
		t.Fatalf("expected empty search index list, got %+v", listResp.IndexInfo)
	}
	_, err = e.client.DescribeSearchIndex(&tablestore.DescribeSearchIndexRequest{TableName: table, IndexName: "si_query"})
	if code := otsErrorCode(t, err); code != "OTSObjectNotExist" {
		t.Fatalf("expected OTSObjectNotExist, got %s", code)
	}
}

func TestSearchIndexQueryPatterns(t *testing.T) {
	e := newEnv(t)
	table := e.tableName(t, "si_query")
	e.createTable(t, table, 1, -1, pk("transaction_id", tablestore.PrimaryKeyType_STRING))
	e.createWalletSearchIndex(t, table, "si_query")

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []struct {
		id      string
		user    string
		session string
		turn    string
		amount  int64
	}{
		{"tx1", "u1", "s1", "t1", 10},
		{"tx2", "u1", "s1", "t1", 20},
		{"tx3", "u1", "s1", "t2", 30},
		{"tx4", "u1", "s2", "t3", -5},
		{"tx5", "u2", "s3", "t4", 40},
		{"tx6", "u2", "s3", "t4", 50},
	}
	for i, row := range rows {
		key, cols := walletRow(row.id, row.user, row.session, row.turn, row.amount, base.Add(time.Duration(i)*time.Hour))
		e.putRow(t, table, key, cols...)
	}

	t.Run("BoolMustWithTokenPagination", func(t *testing.T) {
		query := search.NewSearchQuery().
			SetQuery(&search.BoolQuery{MustQueries: []search.Query{
				&search.TermQuery{FieldName: "user_id", Term: "u1"},
			}}).
			SetLimit(2).
			SetGetTotalCount(true).
			SetSort(&search.Sort{Sorters: []search.Sorter{
				search.NewFieldSort("created_at", search.SortOrder_ASC),
				search.NewPrimaryKeySort(),
			}})
		req := searchRequest(table, "si_query", query)
		var got []string
		var previousToken []byte
		pages := 0
		for {
			resp, err := e.client.Search(req)
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			if !resp.IsAllSuccess {
				t.Fatalf("expected IsAllSuccess")
			}
			if resp.TotalCount != 4 {
				t.Fatalf("expected total 4, got %d", resp.TotalCount)
			}
			got = append(got, searchPKs(resp.Rows)...)
			pages++
			if len(resp.NextToken) == 0 {
				break
			}
			if bytes.Equal(previousToken, resp.NextToken) {
				t.Fatalf("repeated next token")
			}
			previousToken = resp.NextToken
			query.SetToken(resp.NextToken)
			if pages > 10 {
				t.Fatalf("pagination did not terminate")
			}
		}
		if pages != 2 {
			t.Fatalf("expected 2 pages, got %d", pages)
		}
		want := []string{"tx1", "tx2", "tx3", "tx4"}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("DateRange", func(t *testing.T) {
		query := search.NewSearchQuery().
			SetQuery(&search.RangeQuery{
				FieldName:    "created_at",
				From:         base.Add(time.Hour).Format(searchTimeFormat),
				To:           base.Add(3 * time.Hour).Format(searchTimeFormat),
				IncludeLower: true,
				IncludeUpper: true,
			}).
			SetLimit(100).
			SetGetTotalCount(true).
			SetSort(&search.Sort{Sorters: []search.Sorter{
				search.NewFieldSort("created_at", search.SortOrder_ASC),
			}})
		resp, err := e.client.Search(searchRequest(table, "si_query", query))
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if resp.TotalCount != 3 {
			t.Fatalf("expected total 3, got %d", resp.TotalCount)
		}
		if got := searchPKs(resp.Rows); fmt.Sprint(got) != fmt.Sprint([]string{"tx2", "tx3", "tx4"}) {
			t.Fatalf("unexpected rows %v", got)
		}
	})

	t.Run("MustNot", func(t *testing.T) {
		query := search.NewSearchQuery().
			SetQuery(&search.BoolQuery{
				MustQueries: []search.Query{
					&search.TermQuery{FieldName: "user_id", Term: "u1"},
				},
				MustNotQueries: []search.Query{
					&search.TermQuery{FieldName: "session_id", Term: "s2"},
				},
			}).
			SetLimit(100).
			SetGetTotalCount(true)
		resp, err := e.client.Search(searchRequest(table, "si_query", query))
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if resp.TotalCount != 3 {
			t.Fatalf("expected total 3, got %d", resp.TotalCount)
		}
	})

	t.Run("Terms", func(t *testing.T) {
		query := search.NewSearchQuery().
			SetQuery(&search.TermsQuery{FieldName: "session_id", Terms: []interface{}{"s1", "s3"}}).
			SetLimit(100).
			SetGetTotalCount(true)
		resp, err := e.client.Search(searchRequest(table, "si_query", query))
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if resp.TotalCount != 5 {
			t.Fatalf("expected total 5, got %d", resp.TotalCount)
		}
	})

	t.Run("SumAggregation", func(t *testing.T) {
		query := search.NewSearchQuery().
			SetQuery(&search.TermQuery{FieldName: "user_id", Term: "u1"}).
			SetLimit(0).
			Aggregation(search.NewSumAggregation("amount_sum", "amount"))
		resp, err := e.client.Search(searchRequest(table, "si_query", query))
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if len(resp.Rows) != 0 {
			t.Fatalf("expected no rows with limit 0, got %d", len(resp.Rows))
		}
		sum, err := resp.AggregationResults.Sum("amount_sum")
		if err != nil {
			t.Fatalf("missing sum result: %v", err)
		}
		if sum.Value != 55 {
			t.Fatalf("expected sum 55, got %v", sum.Value)
		}
	})

	t.Run("GroupByFieldWithSubAgg", func(t *testing.T) {
		group := search.NewGroupByField("by_user", "user_id").
			Size(10).
			GroupBySorters([]search.GroupBySorter{&search.GroupKeyGroupBySort{Order: search.SortOrder_ASC.Enum()}}).
			SubAggregation(search.NewSumAggregation("amount_sum", "amount"))
		query := search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(0).
			GroupBy(group)
		resp, err := e.client.Search(searchRequest(table, "si_query", query))
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		groupResult, err := resp.GroupByResults.GroupByField("by_user")
		if err != nil {
			t.Fatalf("missing group by result: %v", err)
		}
		if len(groupResult.Items) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(groupResult.Items))
		}
		first := groupResult.Items[0]
		if first.Key != "u1" || first.RowCount != 4 {
			t.Fatalf("unexpected first group: %+v", first)
		}
		if sum, err := first.SubAggregations.Sum("amount_sum"); err != nil || sum.Value != 55 {
			t.Fatalf("unexpected u1 sub agg: %v %+v", err, first.SubAggregations)
		}
		second := groupResult.Items[1]
		if second.Key != "u2" || second.RowCount != 2 {
			t.Fatalf("unexpected second group: %+v", second)
		}
		if sum, err := second.SubAggregations.Sum("amount_sum"); err != nil || sum.Value != 90 {
			t.Fatalf("unexpected u2 sub agg: %v %+v", err, second.SubAggregations)
		}
	})

	t.Run("CountAggregation", func(t *testing.T) {
		query := search.NewSearchQuery().
			SetQuery(&search.TermQuery{FieldName: "user_id", Term: "u2"}).
			SetLimit(0).
			Aggregation(search.NewCountAggregation("amount_count", "amount"))
		resp, err := e.client.Search(searchRequest(table, "si_query", query))
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		count, err := resp.AggregationResults.Count("amount_count")
		if err != nil || count.Value != 2 {
			t.Fatalf("unexpected count result: %v", err)
		}
	})

	t.Run("ColumnsToGet", func(t *testing.T) {
		query := search.NewSearchQuery().
			SetQuery(&search.TermQuery{FieldName: "description", Term: "test tx1"}).
			SetLimit(10)
		req := searchRequest(table, "si_query", query)
		req.SetColumnsToGet(&tablestore.ColumnsToGet{Columns: []string{"amount"}})
		resp, err := e.client.Search(req)
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if len(resp.Rows) != 1 || len(resp.Rows[0].Columns) != 1 || resp.Rows[0].Columns[0].ColumnName != "amount" {
			t.Fatalf("unexpected specified columns: %+v", resp.Rows)
		}
		req.SetColumnsToGet(&tablestore.ColumnsToGet{})
		resp, err = e.client.Search(req)
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if len(resp.Rows) != 1 || len(resp.Rows[0].Columns) != 0 {
			t.Fatalf("expected pk-only row, got %+v", resp.Rows)
		}
	})

	t.Run("WritePropagation", func(t *testing.T) {
		update := new(tablestore.UpdateRowChange)
		update.TableName = table
		update.PrimaryKey = stringPK("transaction_id", "tx4")
		update.PutColumn("amount", int64(100))
		update.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)
		updateReq := new(tablestore.UpdateRowRequest)
		updateReq.UpdateRowChange = update
		if _, err := e.client.UpdateRow(updateReq); err != nil {
			t.Fatalf("update row: %v", err)
		}
		e.deleteRow(t, table, stringPK("transaction_id", "tx1"))

		query := search.NewSearchQuery().
			SetQuery(&search.TermQuery{FieldName: "user_id", Term: "u1"}).
			SetLimit(0).
			SetGetTotalCount(true).
			Aggregation(search.NewSumAggregation("amount_sum", "amount"))
		resp, err := e.client.Search(searchRequest(table, "si_query", query))
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if resp.TotalCount != 3 {
			t.Fatalf("expected total 3 after delete, got %d", resp.TotalCount)
		}
		sum, err := resp.AggregationResults.Sum("amount_sum")
		if err != nil || sum.Value != 150 {
			t.Fatalf("expected sum 150 after update, got %v", err)
		}
	})
}

func TestSearchIndexBackfill(t *testing.T) {
	e := newEnv(t)
	table := e.tableName(t, "si_backfill")
	e.createTable(t, table, 1, -1, pk("transaction_id", tablestore.PrimaryKeyType_STRING))
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		key, cols := walletRow(fmt.Sprintf("bf%d", i), "u1", "s1", "t1", int64(i+1), base.Add(time.Duration(i)*time.Hour))
		e.putRow(t, table, key, cols...)
	}
	e.createWalletSearchIndex(t, table, "si_query")
	query := search.NewSearchQuery().
		SetQuery(&search.MatchAllQuery{}).
		SetLimit(100).
		SetGetTotalCount(true)
	resp, err := e.client.Search(searchRequest(table, "si_query", query))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if resp.TotalCount != 3 {
		t.Fatalf("expected backfilled total 3, got %d", resp.TotalCount)
	}
}

func TestSearchIndexDescSort(t *testing.T) {
	e := newEnv(t)
	table := e.tableName(t, "si_desc")
	e.createTable(t, table, 1, -1, pk("transaction_id", tablestore.PrimaryKeyType_STRING))
	e.createWalletSearchIndex(t, table, "si_query")
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		key, cols := walletRow(fmt.Sprintf("d%d", i), "u1", "s1", "t1", int64(i), base.Add(time.Duration(i)*time.Hour))
		e.putRow(t, table, key, cols...)
	}
	query := search.NewSearchQuery().
		SetQuery(&search.BoolQuery{MustQueries: []search.Query{
			&search.TermQuery{FieldName: "user_id", Term: "u1"},
			&search.RangeQuery{FieldName: "created_at", To: base.Add(3 * time.Hour).Format(searchTimeFormat), IncludeUpper: true},
		}}).
		SetLimit(2).
		SetSort(&search.Sort{Sorters: []search.Sorter{
			search.NewFieldSort("created_at", search.SortOrder_DESC),
			search.NewPrimaryKeySort(),
		}})
	req := searchRequest(table, "si_query", query)
	var got []string
	for {
		resp, err := e.client.Search(req)
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		got = append(got, searchPKs(resp.Rows)...)
		if len(resp.NextToken) == 0 {
			break
		}
		query.SetToken(resp.NextToken)
	}
	if fmt.Sprint(got) != fmt.Sprint([]string{"d3", "d2", "d1", "d0"}) {
		t.Fatalf("expected descending order, got %v", got)
	}
}

func TestSearchIndexPrimaryKeyFields(t *testing.T) {
	e := newEnv(t)
	table := e.tableName(t, "si_pk")
	e.createTable(t, table, 1, -1,
		pk("session_id", tablestore.PrimaryKeyType_STRING),
		pk("turn_id", tablestore.PrimaryKeyType_STRING))

	dateField := searchField("created_at", tablestore.FieldType_DATE)
	dateField.DateFormats = []string{"yyyy-MM-dd HH:mm:ss.SSS"}
	schema := &tablestore.IndexSchema{
		FieldSchemas: []*tablestore.FieldSchema{
			searchField("session_id", tablestore.FieldType_KEYWORD),
			searchField("turn_id", tablestore.FieldType_KEYWORD),
			searchField("user_id", tablestore.FieldType_KEYWORD),
			searchField("user_cost", tablestore.FieldType_LONG),
			dateField,
		},
		IndexSort: &search.Sort{Sorters: []search.Sorter{
			search.NewFieldSort("created_at", search.SortOrder_DESC),
		}},
	}
	if _, err := e.client.CreateSearchIndex(&tablestore.CreateSearchIndexRequest{
		TableName: table, IndexName: "si_query", IndexSchema: schema,
	}); err != nil {
		t.Fatalf("create search index with primary key fields: %v", err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []struct {
		sessionID string
		turnID    string
		userID    string
		cost      int64
		offset    time.Duration
	}{
		{"s1", "t1", "u1", -10, 0},
		{"s1", "t2", "u1", -20, time.Second},
		{"s1", "t3", "u2", -30, 2 * time.Second},
		{"s2", "t1", "u1", -5, 3 * time.Second},
	}
	for _, row := range rows {
		key := new(tablestore.PrimaryKey)
		key.AddPrimaryKeyColumn("session_id", row.sessionID)
		key.AddPrimaryKeyColumn("turn_id", row.turnID)
		e.putRow(t, table, key,
			attr("user_id", row.userID),
			attr("user_cost", row.cost),
			attr("created_at", base.Add(row.offset).Format(searchTimeFormat)),
			attr("prompt_preview", "preview "+row.sessionID+"/"+row.turnID))
	}

	descResp, err := e.client.DescribeSearchIndex(&tablestore.DescribeSearchIndexRequest{
		TableName: table, IndexName: "si_query",
	})
	if err != nil {
		t.Fatalf("describe search index: %v", err)
	}
	if len(descResp.Schema.FieldSchemas) != 5 {
		t.Fatalf("expected 5 field schemas, got %d", len(descResp.Schema.FieldSchemas))
	}

	t.Run("TermOnPrimaryKeyField", func(t *testing.T) {
		query := search.NewSearchQuery().
			SetQuery(&search.TermQuery{FieldName: "session_id", Term: "s1"}).
			SetLimit(10).
			SetGetTotalCount(true)
		resp, err := e.client.Search(searchRequest(table, "si_query", query))
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if resp.TotalCount != 3 || len(resp.Rows) != 3 {
			t.Fatalf("expected 3 rows for s1, got total=%d rows=%d", resp.TotalCount, len(resp.Rows))
		}
	})

	t.Run("BoolRangeOnPrimaryKeyScoped", func(t *testing.T) {
		query := search.NewSearchQuery().
			SetQuery(&search.BoolQuery{MustQueries: []search.Query{
				&search.TermQuery{FieldName: "session_id", Term: "s1"},
				&search.RangeQuery{FieldName: "user_cost", To: int64(0), IncludeUpper: false},
			}}).
			SetLimit(10).
			SetGetTotalCount(true)
		resp, err := e.client.Search(searchRequest(table, "si_query", query))
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if resp.TotalCount != 3 {
			t.Fatalf("expected 3 negative-cost rows for s1, got %d", resp.TotalCount)
		}
	})

	t.Run("GroupByOnPrimaryKeyField", func(t *testing.T) {
		group := search.NewGroupByField("by_turn", "turn_id").
			Size(10).
			GroupBySorters([]search.GroupBySorter{&search.GroupKeyGroupBySort{Order: search.SortOrder_ASC.Enum()}}).
			SubAggregation(search.NewSumAggregation("cost_sum", "user_cost"))
		query := search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(0).
			GroupBy(group)
		resp, err := e.client.Search(searchRequest(table, "si_query", query))
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		groupResult, err := resp.GroupByResults.GroupByField("by_turn")
		if err != nil {
			t.Fatalf("missing group by result: %v", err)
		}
		if len(groupResult.Items) != 3 {
			t.Fatalf("expected 3 turn groups, got %d", len(groupResult.Items))
		}
		first := groupResult.Items[0]
		if first.Key != "t1" || first.RowCount != 2 {
			t.Fatalf("unexpected t1 group: %+v", first)
		}
		if sum, err := first.SubAggregations.Sum("cost_sum"); err != nil || sum.Value != -15 {
			t.Fatalf("unexpected t1 sum: %v %+v", err, sum)
		}
	})

	t.Run("PrimaryKeySortTiebreak", func(t *testing.T) {
		query := search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(10).
			SetSort(&search.Sort{Sorters: []search.Sorter{
				search.NewFieldSort("created_at", search.SortOrder_ASC),
				search.NewPrimaryKeySort(),
			}})
		resp, err := e.client.Search(searchRequest(table, "si_query", query))
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		want := []string{"s1/t1", "s1/t2", "s1/t3", "s2/t1"}
		if len(resp.Rows) != len(want) {
			t.Fatalf("expected %d rows, got %d", len(want), len(resp.Rows))
		}
		for index, row := range resp.Rows {
			values := map[string]string{}
			for _, pk := range row.PrimaryKey.PrimaryKeys {
				values[pk.ColumnName] = pk.Value.(string)
			}
			got := values["session_id"] + "/" + values["turn_id"]
			if got != want[index] {
				t.Fatalf("row %d: expected %s, got %s", index, want[index], got)
			}
		}
	})

	t.Run("ColumnsToGetFromMainRow", func(t *testing.T) {
		query := search.NewSearchQuery().
			SetQuery(&search.TermQuery{FieldName: "turn_id", Term: "t3"}).
			SetLimit(10)
		req := new(tablestore.SearchRequest)
		req.SetTableName(table)
		req.SetIndexName("si_query")
		req.SetSearchQuery(query)
		req.SetColumnsToGet(&tablestore.ColumnsToGet{Columns: []string{"prompt_preview", "user_cost"}})
		resp, err := e.client.Search(req)
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if len(resp.Rows) != 1 {
			t.Fatalf("expected 1 row for t3, got %d", len(resp.Rows))
		}
		preview, ok := rowColumn(resp.Rows[0], "prompt_preview")
		if !ok || preview != "preview s1/t3" {
			t.Fatalf("expected prompt_preview from main row, got %v (present=%v)", preview, ok)
		}
	})
}
