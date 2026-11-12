// Package storage implements the OTS data model on top of PostgreSQL.
package storage

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/pandada8/ots-postgresql/internal/ots"
)

const metaTableName = "__ots_tables"

var tableNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,254}$`)
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type PKColumn struct {
	Name          string             `json:"name"`
	Type          ots.PrimaryKeyType `json:"type"`
	AutoIncrement bool               `json:"auto_increment,omitempty"`
}

type DefinedColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type TableOptions struct {
	TimeToLive                int32 `json:"time_to_live"`
	MaxVersions               int32 `json:"max_versions"`
	AllowUpdate               bool  `json:"allow_update"`
	UpdateFullRow             bool  `json:"update_full_row"`
	DeviationCellVersionInSec int64 `json:"deviation_cell_version_in_sec"`
}

type TableMeta struct {
	Name           string          `json:"-"`
	PGName         string          `json:"-"`
	PrimaryKey     []PKColumn      `json:"primary_key"`
	DefinedColumns []DefinedColumn `json:"defined_columns,omitempty"`
	Options        TableOptions    `json:"options"`
	CreatedAt      int64           `json:"-"`
	Indexes        []*IndexMeta    `json:"-"`
}

func (m *TableMeta) autoIncrementColumn() (int, bool) {
	for i, col := range m.PrimaryKey {
		if col.AutoIncrement {
			return i, true
		}
	}
	return 0, false
}

type Engine struct {
	db     pgDB
	prefix string
}

func Open(ctx context.Context, dsn string, prefix string) (*Engine, error) {
	if prefix != "" && !identifierPattern.MatchString(prefix) {
		return nil, fmt.Errorf("invalid table prefix %q", prefix)
	}
	rawDB, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	rawDB.SetMaxOpenConns(16)
	rawDB.SetMaxIdleConns(8)
	rawDB.SetConnMaxLifetime(5 * time.Minute)
	if err := rawDB.PingContext(ctx); err != nil {
		rawDB.Close()
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	db := pgDB{db: rawDB}
	engine := &Engine{db: db, prefix: prefix}
	if err := engine.ensureMetaTable(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := engine.ensureIndexRegistry(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := engine.ensureSearchRegistry(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return engine, nil
}

func (e *Engine) Close() error {
	return e.db.Close()
}

func (e *Engine) ensureMetaTable(ctx context.Context) error {
	_, err := e.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS \""+metaTableName+"\" ("+
		"\"table_name\" VARCHAR(255) NOT NULL,"+
		"\"pg_name\" VARCHAR(63) NOT NULL,"+
		"\"meta\" JSONB NOT NULL,"+
		"\"created_at\" BIGINT NOT NULL,"+
		"PRIMARY KEY (\"table_name\"),"+
		"CONSTRAINT uq_tables_pg_name UNIQUE (\"pg_name\")"+
		")")
	if err != nil {
		return fmt.Errorf("create meta table: %w", err)
	}
	return nil
}

func (e *Engine) pgName(tableName string) string {
	name := e.prefix + tableName
	if len(name) <= 63 && identifierPattern.MatchString(name) {
		return name
	}
	sum := sha1.Sum([]byte(name))
	return e.prefix + "t_" + hex.EncodeToString(sum[:])[:32]
}

func quoteIdent(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

func validateTableName(name string) error {
	if !tableNamePattern.MatchString(name) {
		return ots.ErrParameterInvalid("invalid table name %q: must match [A-Za-z_][A-Za-z0-9_]{0,254}", name)
	}
	return nil
}

func (e *Engine) CreateTable(ctx context.Context, meta *TableMeta) error {
	if err := validateTableName(meta.Name); err != nil {
		return err
	}
	if len(meta.PrimaryKey) == 0 || len(meta.PrimaryKey) > 4 {
		return ots.ErrParameterInvalid("table must have 1 to 4 primary key columns")
	}
	seen := make(map[string]bool)
	autoCount := 0
	for _, col := range meta.PrimaryKey {
		if !identifierPattern.MatchString(col.Name) || len(col.Name) > 255 {
			return ots.ErrParameterInvalid("invalid primary key column name %q", col.Name)
		}
		if seen[col.Name] {
			return ots.ErrParameterInvalid("duplicate primary key column %q", col.Name)
		}
		seen[col.Name] = true
		switch col.Type {
		case ots.PrimaryKeyInteger, ots.PrimaryKeyString, ots.PrimaryKeyBinary:
		default:
			return ots.ErrParameterInvalid("invalid primary key type for column %q", col.Name)
		}
		if col.AutoIncrement {
			autoCount++
			if col.Type != ots.PrimaryKeyInteger {
				return ots.ErrParameterInvalid("auto increment column %q must be INTEGER", col.Name)
			}
		}
	}
	if autoCount > 1 {
		return ots.ErrParameterInvalid("table can have at most one auto increment primary key column")
	}
	for _, col := range meta.DefinedColumns {
		if seen[col.Name] {
			return ots.ErrParameterInvalid("defined column %q conflicts with primary key", col.Name)
		}
		seen[col.Name] = true
	}
	if meta.Options.MaxVersions <= 0 {
		meta.Options.MaxVersions = 1
	}
	if meta.Options.TimeToLive == 0 {
		meta.Options.TimeToLive = -1
	}

	pgName := e.pgName(meta.Name)
	meta.PGName = pgName
	meta.CreatedAt = time.Now().UnixMicro()

	ddl := dataTableDDL(pgName, meta.PrimaryKey)

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return ots.ErrInternal("marshal table meta: %v", err)
	}

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return ots.ErrInternal("begin tx: %v", err)
	}
	defer tx.Rollback()

	var exists int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM \""+metaTableName+"\" WHERE table_name = ?", meta.Name).Scan(&exists); err != nil {
		return ots.ErrInternal("check table existence: %v", err)
	}
	if exists > 0 {
		return ots.ErrObjectAlreadyExist("table %q already exists", meta.Name)
	}
	if _, err := tx.ExecContext(ctx, ddl); err != nil {
		if isDuplicateTable(err) {
			return ots.ErrObjectAlreadyExist("table %q already exists", meta.Name)
		}
		return execError(err, "create table")
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO \""+metaTableName+"\" (table_name, pg_name, meta, created_at) VALUES (?, ?, ?, ?)",
		meta.Name, pgName, string(metaJSON), meta.CreatedAt); err != nil {
		if isUniqueViolation(err) {
			return ots.ErrObjectAlreadyExist("table %q already exists", meta.Name)
		}
		return execError(err, "save table meta")
	}
	for _, idx := range meta.Indexes {
		if err := e.prepareIndex(meta, idx); err != nil {
			return err
		}
		if err := e.createIndexStorage(ctx, tx, meta, idx); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return ots.ErrInternal("commit: %v", err)
	}
	return nil
}

func dataTableDDL(pgName string, pk []PKColumn) string {
	var columns []string
	var pkNames []string
	for _, col := range pk {
		var def string
		switch col.Type {
		case ots.PrimaryKeyInteger:
			def = quoteIdent(col.Name) + " BIGINT NOT NULL"
			if col.AutoIncrement {
				def = quoteIdent(col.Name) + " BIGINT GENERATED ALWAYS AS IDENTITY"
			}
		case ots.PrimaryKeyString, ots.PrimaryKeyBinary:
			def = quoteIdent(col.Name) + " BYTEA NOT NULL"
		}
		columns = append(columns, def)
		pkNames = append(pkNames, quoteIdent(col.Name))
	}
	columns = append(columns, "\"__ots_attrs\" JSONB NOT NULL")
	ddl := "CREATE TABLE " + quoteIdent(pgName) + " (" + strings.Join(columns, ", ") +
		", PRIMARY KEY (" + strings.Join(pkNames, ", ") + ")"
	ddl += ")"
	return ddl
}

func (e *Engine) GetTableMeta(ctx context.Context, name string) (*TableMeta, error) {
	var pgName string
	var metaJSON []byte
	var createdAt int64
	err := e.db.QueryRowContext(ctx,
		"SELECT pg_name, meta, created_at FROM \""+metaTableName+"\" WHERE table_name = ?", name).
		Scan(&pgName, &metaJSON, &createdAt)
	if err == sql.ErrNoRows {
		return nil, ots.ErrObjectNotExist("table %q does not exist", name)
	}
	if err != nil {
		return nil, ots.ErrInternal("load table meta: %v", err)
	}
	meta := new(TableMeta)
	if err := json.Unmarshal(metaJSON, meta); err != nil {
		return nil, ots.ErrInternal("parse table meta: %v", err)
	}
	meta.Name = name
	meta.PGName = pgName
	meta.CreatedAt = createdAt
	return meta, nil
}

func (e *Engine) ListTables(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, "SELECT table_name FROM \""+metaTableName+"\" ORDER BY table_name")
	if err != nil {
		return nil, ots.ErrInternal("list tables: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, ots.ErrInternal("list tables scan: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, ots.ErrInternal("list tables: %v", err)
	}
	return names, nil
}

func (e *Engine) DeleteTable(ctx context.Context, name string) error {
	meta, err := e.GetTableMeta(ctx, name)
	if err != nil {
		return err
	}
	indexes, err := e.indexesForTable(ctx, e.db, name)
	if err != nil {
		return err
	}
	searchIndexes, err := e.searchIndexesForTable(ctx, e.db, name)
	if err != nil {
		return err
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return ots.ErrInternal("begin tx: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteIdent(meta.PGName)); err != nil {
		return ots.ErrInternal("drop table: %v", err)
	}
	for _, idx := range indexes {
		if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteIdent(idx.PGName)); err != nil {
			return ots.ErrInternal("drop index table: %v", err)
		}
	}
	for _, idx := range searchIndexes {
		if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteIdent(idx.PGName)); err != nil {
			return ots.ErrInternal("drop search index table: %v", err)
		}
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM \""+indexRegistryTable+"\" WHERE main_table = ?", name); err != nil {
		return ots.ErrInternal("delete index meta: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM \""+searchRegistryTable+"\" WHERE main_table = ?", name); err != nil {
		return ots.ErrInternal("delete search index meta: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM \""+metaTableName+"\" WHERE table_name = ?", name); err != nil {
		return ots.ErrInternal("delete table meta: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return ots.ErrInternal("commit: %v", err)
	}
	return nil
}

func (e *Engine) UpdateTable(ctx context.Context, name string, options *TableOptions) (*TableMeta, error) {
	meta, err := e.GetTableMeta(ctx, name)
	if err != nil {
		return nil, err
	}
	if options != nil {
		if options.UpdateFullRow != meta.Options.UpdateFullRow {
			return nil, ots.ErrParameterInvalid("update_full_row cannot be changed")
		}
		if options.MaxVersions <= 0 {
			return nil, ots.ErrParameterInvalid("max_versions must be positive")
		}
		if options.TimeToLive != -1 && options.TimeToLive <= 0 {
			return nil, ots.ErrParameterInvalid("time_to_live must be -1 or positive")
		}
		meta.Options = *options
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, ots.ErrInternal("marshal table meta: %v", err)
	}
	if _, err := e.db.ExecContext(ctx,
		"UPDATE \""+metaTableName+"\" SET meta = ? WHERE table_name = ?", string(metaJSON), name); err != nil {
		return nil, ots.ErrInternal("update table meta: %v", err)
	}
	return meta, nil
}
