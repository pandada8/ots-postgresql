package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"

	"github.com/pandada8/ots-postgresql/internal/storage"
)

func bootstrapSpecJSON(tableName string) string {
	return fmt.Sprintf(`{
		"tables": [{
			"name": %q,
			"primary_key": [{"name": "transaction_id", "type": "STRING"}],
			"defined_columns": [
				{"name": "user_id", "type": "STRING"},
				{"name": "session_id", "type": "STRING"},
				{"name": "turn_id", "type": "STRING"},
				{"name": "amount", "type": "INTEGER"},
				{"name": "created_at", "type": "STRING"}
			],
			"options": {"time_to_live": -1, "max_versions": 1},
			"search_indexes": [
				{
					"name": "si_query",
					"fields": [
						{"name": "transaction_id", "type": "KEYWORD", "index": true, "sort_and_agg": true},
						{"name": "user_id", "type": "KEYWORD", "index": true, "sort_and_agg": true},
						{"name": "amount", "type": "LONG", "index": true, "sort_and_agg": true},
						{"name": "created_at", "type": "DATE", "index": true, "sort_and_agg": true, "date_formats": ["yyyy-MM-dd HH:mm:ss.SSS"]}
					],
					"index_sort": [{"field": "created_at", "descending": true}, {"descending": false}]
				},
				{
					"name": "si_billing_turns",
					"fields": [
						{"name": "user_id", "type": "KEYWORD", "index": true, "sort_and_agg": true},
						{"name": "session_id", "type": "KEYWORD", "index": true, "sort_and_agg": true},
						{"name": "turn_id", "type": "KEYWORD", "index": true, "sort_and_agg": true},
						{"name": "amount", "type": "LONG", "index": true, "sort_and_agg": true},
						{"name": "created_at", "type": "DATE", "index": true, "sort_and_agg": true, "date_formats": ["yyyy-MM-dd HH:mm:ss.SSS"]}
					],
					"index_sort": [{"field": "created_at", "descending": true}, {"descending": false}]
				}
			]
		}]
	}`, tableName)
}

func TestBootstrapCreatesSchemaAndIsIdempotent(t *testing.T) {
	env := newEnv(t)
	ctx := context.Background()
	tableName := env.tableName(t, "bootstrap")

	spec, err := storage.ParseBootstrapSpec([]byte(bootstrapSpecJSON(tableName)))
	if err != nil {
		t.Fatalf("parse spec: %v", err)
	}
	if err := env.engine.Bootstrap(ctx, spec); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	describe, err := env.client.DescribeTable(&tablestore.DescribeTableRequest{TableName: tableName})
	if err != nil {
		t.Fatalf("describe table: %v", err)
	}
	if len(describe.TableMeta.SchemaEntry) != 1 || describe.TableMeta.SchemaEntry[0].Name == nil || *describe.TableMeta.SchemaEntry[0].Name != "transaction_id" {
		t.Fatalf("unexpected primary key: %+v", describe.TableMeta.SchemaEntry)
	}

	indexes, err := env.client.ListSearchIndex(&tablestore.ListSearchIndexRequest{TableName: tableName})
	if err != nil {
		t.Fatalf("list search indexes: %v", err)
	}
	if len(indexes.IndexInfo) != 2 {
		t.Fatalf("expected 2 search indexes, got %d", len(indexes.IndexInfo))
	}
	for _, indexName := range []string{"si_query", "si_billing_turns"} {
		describeIndex, err := env.client.DescribeSearchIndex(&tablestore.DescribeSearchIndexRequest{
			TableName: tableName,
			IndexName: indexName,
		})
		if err != nil {
			t.Fatalf("describe search index %s: %v", indexName, err)
		}
		if describeIndex.SyncStat == nil || describeIndex.SyncStat.SyncPhase != tablestore.SyncPhase_INCR {
			t.Fatalf("index %s not in INCR phase: %+v", indexName, describeIndex.SyncStat)
		}
	}

	key := new(tablestore.PrimaryKey)
	key.AddPrimaryKeyColumn("transaction_id", "txn-bootstrap-1")
	env.updateRowAttrs(t, tableName, key,
		tablestore.AttributeColumn{ColumnName: "user_id", Value: "u-bootstrap"},
		tablestore.AttributeColumn{ColumnName: "session_id", Value: "s-bootstrap"},
		tablestore.AttributeColumn{ColumnName: "turn_id", Value: "turn-1"},
		tablestore.AttributeColumn{ColumnName: "amount", Value: int64(42)},
		tablestore.AttributeColumn{ColumnName: "created_at", Value: "2026-11-14 10:00:00.000"},
	)

	if err := env.engine.Bootstrap(ctx, spec); err != nil {
		t.Fatalf("second bootstrap must be idempotent: %v", err)
	}

	row := env.getRow(t, tableName, key, 1)
	if len(row.Columns) == 0 {
		t.Fatal("row data lost after re-bootstrap")
	}

	query := search.NewSearchQuery().
		SetQuery(&search.TermQuery{FieldName: "turn_id", Term: "turn-1"}).
		SetLimit(10).
		SetGetTotalCount(true)
	searchResponse, err := env.client.Search(searchRequest(tableName, "si_billing_turns", query))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if searchResponse.TotalCount != 1 || len(searchResponse.Rows) != 1 {
		t.Fatalf("expected 1 search hit, got total=%d rows=%d", searchResponse.TotalCount, len(searchResponse.Rows))
	}
}

func TestBootstrapRejectsInvalidSpec(t *testing.T) {
	env := newEnv(t)
	if _, err := storage.ParseBootstrapSpec([]byte(`{"tables": []}`)); err == nil {
		t.Fatal("expected parse error for empty spec")
	}
	tableName := env.tableName(t, "bootstrap_bad")
	spec, err := storage.ParseBootstrapSpec([]byte(fmt.Sprintf(`{
		"tables": [{
			"name": %q,
			"primary_key": [{"name": "pk", "type": "STRING"}],
			"search_indexes": [{"name": "si_bad", "fields": [{"name": "missing_sort_field", "type": "KEYWORD", "index": true}], "index_sort": [{"field": "not_a_field"}]}]
		}]
	}`, tableName)))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := env.engine.Bootstrap(context.Background(), spec); err == nil {
		t.Fatal("expected bootstrap error for invalid index sort field")
	}
}
