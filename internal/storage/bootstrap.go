package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/pandada8/ots-postgresql/internal/ots"
)

// BootstrapPKColumn describes a primary key column in a bootstrap spec. Types
// are written as STRING, INTEGER or BINARY so specs stay readable by hand.
type BootstrapPKColumn struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	AutoIncrement bool   `json:"auto_increment,omitempty"`
}

// BootstrapTable describes one table and the search indexes that must exist
// on it before clients start issuing requests.
type BootstrapTable struct {
	Name           string              `json:"name"`
	PrimaryKey     []BootstrapPKColumn `json:"primary_key"`
	DefinedColumns []DefinedColumn     `json:"defined_columns,omitempty"`
	Options        TableOptions        `json:"options"`
	SearchIndexes  []SearchIndexMeta   `json:"search_indexes,omitempty"`
}

type BootstrapSpec struct {
	Tables []BootstrapTable `json:"tables"`
}

func ParseBootstrapSpec(data []byte) (*BootstrapSpec, error) {
	spec := new(BootstrapSpec)
	if err := json.Unmarshal(data, spec); err != nil {
		return nil, fmt.Errorf("parse bootstrap spec: %w", err)
	}
	if len(spec.Tables) == 0 {
		return nil, fmt.Errorf("bootstrap spec contains no tables")
	}
	for _, table := range spec.Tables {
		if len(table.PrimaryKey) == 0 {
			return nil, fmt.Errorf("table %q: primary_key is required", table.Name)
		}
		if _, err := bootstrapPrimaryKey(table.PrimaryKey); err != nil {
			return nil, fmt.Errorf("table %q: %w", table.Name, err)
		}
	}
	return spec, nil
}

func bootstrapPrimaryKey(columns []BootstrapPKColumn) ([]PKColumn, error) {
	primaryKey := make([]PKColumn, 0, len(columns))
	for _, column := range columns {
		var columnType ots.PrimaryKeyType
		switch strings.ToUpper(column.Type) {
		case "INTEGER":
			columnType = ots.PrimaryKeyInteger
		case "STRING":
			columnType = ots.PrimaryKeyString
		case "BINARY":
			columnType = ots.PrimaryKeyBinary
		default:
			return nil, fmt.Errorf("invalid primary key type %q for column %q", column.Type, column.Name)
		}
		primaryKey = append(primaryKey, PKColumn{
			Name:          column.Name,
			Type:          columnType,
			AutoIncrement: column.AutoIncrement,
		})
	}
	return primaryKey, nil
}

// Bootstrap idempotently ensures every table and search index in the spec
// exists. Objects that already exist are left untouched, so restarting the
// proxy or rerunning a deploy never disturbs data or index state.
func (e *Engine) Bootstrap(ctx context.Context, spec *BootstrapSpec) error {
	for _, table := range spec.Tables {
		primaryKey, err := bootstrapPrimaryKey(table.PrimaryKey)
		if err != nil {
			return fmt.Errorf("table %q: %w", table.Name, err)
		}
		meta := &TableMeta{
			Name:           table.Name,
			PrimaryKey:     primaryKey,
			DefinedColumns: table.DefinedColumns,
			Options:        table.Options,
		}
		if err := e.CreateTable(ctx, meta); err != nil {
			if ots.AsError(err).Code != ots.CodeObjectAlreadyExist {
				return fmt.Errorf("bootstrap table %q: %w", table.Name, err)
			}
			slog.Info("bootstrap table already exists", "table", table.Name)
		} else {
			slog.Info("bootstrap table created", "table", table.Name)
		}
		for index := range table.SearchIndexes {
			searchIndex := table.SearchIndexes[index]
			if err := e.CreateSearchIndex(ctx, table.Name, &searchIndex); err != nil {
				if ots.AsError(err).Code != ots.CodeObjectAlreadyExist {
					return fmt.Errorf("bootstrap search index %q on table %q: %w", searchIndex.Name, table.Name, err)
				}
				slog.Info("bootstrap search index already exists", "table", table.Name, "index", searchIndex.Name)
			} else {
				slog.Info("bootstrap search index created", "table", table.Name, "index", searchIndex.Name)
			}
		}
	}
	return nil
}
