package storage

import (
	"strings"
	"testing"
)

func TestParseBootstrapSpec(t *testing.T) {
	spec, err := ParseBootstrapSpec([]byte(`{
		"tables": [{
			"name": "transactions",
			"primary_key": [{"name": "transaction_id", "type": "STRING"}],
			"defined_columns": [{"name": "amount", "type": "INTEGER"}],
			"options": {"time_to_live": -1, "max_versions": 1},
			"search_indexes": [{
				"name": "si_billing_turns",
				"fields": [
					{"name": "user_id", "type": "KEYWORD", "index": true, "sort_and_agg": true},
					{"name": "amount", "type": "LONG", "index": true, "sort_and_agg": true}
				],
				"index_sort": [{"field": "amount", "descending": true}]
			}]
		}]
	}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(spec.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(spec.Tables))
	}
	table := spec.Tables[0]
	if table.Name != "transactions" || len(table.PrimaryKey) != 1 || len(table.SearchIndexes) != 1 {
		t.Fatalf("unexpected spec content: %+v", table)
	}
	primaryKey, err := bootstrapPrimaryKey(table.PrimaryKey)
	if err != nil {
		t.Fatalf("primary key: %v", err)
	}
	if primaryKey[0].Name != "transaction_id" || primaryKey[0].Type.String() != "STRING" {
		t.Fatalf("unexpected primary key: %+v", primaryKey[0])
	}
}

func TestParseBootstrapSpecRejectsInvalidInput(t *testing.T) {
	cases := map[string]string{
		"malformed json":      `{`,
		"no tables":           `{"tables": []}`,
		"missing primary key": `{"tables": [{"name": "t"}]}`,
		"unknown pk type":     `{"tables": [{"name": "t", "primary_key": [{"name": "pk", "type": "FLOAT"}]}]}`,
	}
	for label, input := range cases {
		if _, err := ParseBootstrapSpec([]byte(input)); err == nil {
			t.Errorf("%s: expected error, got nil", label)
		}
	}
}

func TestParseBootstrapSpecErrorMentionsTable(t *testing.T) {
	_, err := ParseBootstrapSpec([]byte(`{"tables": [{"name": "bad_table", "primary_key": [{"name": "pk", "type": "FLOAT"}]}]}`))
	if err == nil || !strings.Contains(err.Error(), "bad_table") {
		t.Fatalf("expected error naming bad_table, got %v", err)
	}
}
