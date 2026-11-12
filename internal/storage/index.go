package storage

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pandada8/ots-postgresql/internal/ots"
)

const indexRegistryTable = "__ots_indexes"

const (
	IndexTypeGlobal = "GLOBAL"
	IndexTypeLocal  = "LOCAL"
)

type IndexMeta struct {
	Name           string   `json:"name"`
	MainTable      string   `json:"main_table"`
	PGName         string   `json:"pg_name"`
	PrimaryKey     []string `json:"primary_key"`
	DefinedColumns []string `json:"defined_columns,omitempty"`
	IndexType      string   `json:"index_type"`
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func (e *Engine) ensureIndexRegistry(ctx context.Context) error {
	_, err := e.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS \""+indexRegistryTable+"\" ("+
		"\"index_name\" VARCHAR(255) NOT NULL,"+
		"\"main_table\" VARCHAR(255) NOT NULL,"+
		"\"pg_name\" VARCHAR(63) NOT NULL,"+
		"\"meta\" JSONB NOT NULL,"+
		"PRIMARY KEY (\"index_name\")"+
		")")
	if err != nil {
		return fmt.Errorf("create index registry: %w", err)
	}
	if _, err := e.db.ExecContext(ctx, "CREATE INDEX IF NOT EXISTS idx_main_table ON \""+indexRegistryTable+"\" (\"main_table\")"); err != nil {
		return fmt.Errorf("create index registry index: %w", err)
	}
	return nil
}

func (e *Engine) indexesForTable(ctx context.Context, q queryer, mainName string) ([]*IndexMeta, error) {
	rows, err := q.QueryContext(ctx, "SELECT meta FROM \""+indexRegistryTable+"\" WHERE main_table = ? ORDER BY index_name", mainName)
	if err != nil {
		return nil, ots.ErrInternal("list indexes: %v", err)
	}
	defer rows.Close()
	var indexes []*IndexMeta
	for rows.Next() {
		var metaJSON []byte
		if err := rows.Scan(&metaJSON); err != nil {
			return nil, ots.ErrInternal("scan index meta: %v", err)
		}
		idx := new(IndexMeta)
		if err := json.Unmarshal(metaJSON, idx); err != nil {
			return nil, ots.ErrInternal("parse index meta: %v", err)
		}
		indexes = append(indexes, idx)
	}
	if err := rows.Err(); err != nil {
		return nil, ots.ErrInternal("list indexes: %v", err)
	}
	return indexes, nil
}

func (e *Engine) getIndexByName(ctx context.Context, name string) (*IndexMeta, error) {
	var metaJSON []byte
	err := e.db.QueryRowContext(ctx, "SELECT meta FROM \""+indexRegistryTable+"\" WHERE index_name = ?", name).Scan(&metaJSON)
	if err == sql.ErrNoRows {
		return nil, ots.ErrObjectNotExist("index %q does not exist", name)
	}
	if err != nil {
		return nil, ots.ErrInternal("load index meta: %v", err)
	}
	idx := new(IndexMeta)
	if err := json.Unmarshal(metaJSON, idx); err != nil {
		return nil, ots.ErrInternal("parse index meta: %v", err)
	}
	return idx, nil
}

func (e *Engine) ListIndexes(ctx context.Context, mainName string) ([]*IndexMeta, error) {
	return e.indexesForTable(ctx, e.db, mainName)
}

func (m *TableMeta) columnType(name string) (ots.PrimaryKeyType, bool) {
	for _, col := range m.PrimaryKey {
		if col.Name == name {
			return col.Type, true
		}
	}
	for _, col := range m.DefinedColumns {
		if col.Name == name {
			switch col.Type {
			case "DCT_INTEGER", "INTEGER":
				return ots.PrimaryKeyInteger, true
			case "DCT_STRING", "STRING":
				return ots.PrimaryKeyString, true
			case "DCT_BLOB", "BINARY":
				return ots.PrimaryKeyBinary, true
			}
			return 0, false
		}
	}
	return 0, false
}

func (m *TableMeta) hasColumn(name string) bool {
	for _, col := range m.PrimaryKey {
		if col.Name == name {
			return true
		}
	}
	for _, col := range m.DefinedColumns {
		if col.Name == name {
			return true
		}
	}
	return false
}

func (e *Engine) indexPGName(name string) string {
	candidate := e.prefix + "idx_" + name
	if len(candidate) <= 63 && identifierPattern.MatchString(candidate) {
		return candidate
	}
	sum := sha1.Sum([]byte(name))
	return e.prefix + "i_" + hex.EncodeToString(sum[:])[:32]
}

func (e *Engine) prepareIndex(main *TableMeta, idx *IndexMeta) error {
	if idx == nil {
		return ots.ErrParameterInvalid("index meta is required")
	}
	if err := validateTableName(idx.Name); err != nil {
		return err
	}
	idx.MainTable = main.Name
	if idx.IndexType == "" {
		idx.IndexType = IndexTypeGlobal
	}
	if idx.IndexType != IndexTypeGlobal && idx.IndexType != IndexTypeLocal {
		return ots.ErrParameterInvalid("invalid index type %q", idx.IndexType)
	}
	if err := validateIndex(main, idx); err != nil {
		return err
	}
	idx.PGName = e.indexPGName(idx.Name)
	return nil
}

func validateIndex(main *TableMeta, idx *IndexMeta) error {
	if len(idx.PrimaryKey) == 0 {
		return ots.ErrParameterInvalid("index %q must have at least one primary key column", idx.Name)
	}
	seen := make(map[string]bool)
	for _, name := range idx.PrimaryKey {
		if seen[name] {
			return ots.ErrParameterInvalid("index %q has duplicate primary key column %q", idx.Name, name)
		}
		seen[name] = true
		if _, ok := main.columnType(name); !ok {
			return ots.ErrParameterInvalid("index %q primary key column %q must be a main table primary key or a pre-defined INTEGER/STRING/BINARY column", idx.Name, name)
		}
	}
	if idx.IndexType == IndexTypeLocal {
		if len(main.PrimaryKey) == 0 || idx.PrimaryKey[0] != main.PrimaryKey[0].Name {
			return ots.ErrParameterInvalid("local index %q first primary key must equal the main table's first primary key", idx.Name)
		}
	}
	for _, name := range idx.DefinedColumns {
		if !main.hasColumn(name) {
			return ots.ErrParameterInvalid("index %q covering column %q is not a main table column", idx.Name, name)
		}
	}
	return nil
}

func (e *Engine) indexPKColumns(main *TableMeta, idx *IndexMeta) ([]PKColumn, error) {
	cols := make([]PKColumn, 0, len(idx.PrimaryKey)+len(main.PrimaryKey))
	seen := make(map[string]bool)
	for _, name := range idx.PrimaryKey {
		typ, ok := main.columnType(name)
		if !ok {
			return nil, ots.ErrParameterInvalid("index column %q has no resolvable type", name)
		}
		cols = append(cols, PKColumn{Name: name, Type: typ})
		seen[name] = true
	}
	for _, col := range main.PrimaryKey {
		if seen[col.Name] {
			continue
		}
		cols = append(cols, PKColumn{Name: col.Name, Type: col.Type})
		seen[col.Name] = true
	}
	return cols, nil
}

func (e *Engine) buildIndexTableMeta(main *TableMeta, idx *IndexMeta) (*TableMeta, error) {
	pk, err := e.indexPKColumns(main, idx)
	if err != nil {
		return nil, err
	}
	return &TableMeta{
		Name:       idx.Name,
		PGName:     idx.PGName,
		PrimaryKey: pk,
		Options: TableOptions{
			MaxVersions: 1,
			TimeToLive:  -1,
			AllowUpdate: true,
		},
	}, nil
}

func (e *Engine) createIndexStorage(ctx context.Context, tx *pgTx, main *TableMeta, idx *IndexMeta) error {
	pk, err := e.indexPKColumns(main, idx)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, dataTableDDL(idx.PGName, pk)); err != nil {
		if isDuplicateTable(err) {
			return ots.ErrObjectAlreadyExist("index %q already exists", idx.Name)
		}
		return execError(err, "create index table")
	}
	metaJSON, err := json.Marshal(idx)
	if err != nil {
		return ots.ErrInternal("marshal index meta: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO \""+indexRegistryTable+"\" (index_name, main_table, pg_name, meta) VALUES (?, ?, ?, ?)",
		idx.Name, idx.MainTable, idx.PGName, string(metaJSON)); err != nil {
		if isUniqueViolation(err) {
			return ots.ErrObjectAlreadyExist("index %q already exists", idx.Name)
		}
		return execError(err, "save index meta")
	}
	return nil
}

func (e *Engine) CreateIndex(ctx context.Context, mainName string, idx *IndexMeta, includeBaseData bool) error {
	main, err := e.GetTableMeta(ctx, mainName)
	if err != nil {
		return err
	}
	if err := e.prepareIndex(main, idx); err != nil {
		return err
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return ots.ErrInternal("begin tx: %v", err)
	}
	defer tx.Rollback()
	var exists int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM \""+indexRegistryTable+"\" WHERE index_name = ?", idx.Name).Scan(&exists); err != nil {
		return ots.ErrInternal("check index existence: %v", err)
	}
	if exists > 0 {
		return ots.ErrObjectAlreadyExist("index %q already exists", idx.Name)
	}
	if err := e.createIndexStorage(ctx, tx, main, idx); err != nil {
		return err
	}
	if includeBaseData {
		if err := e.backfillIndex(ctx, tx, main, idx); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return ots.ErrInternal("commit: %v", err)
	}
	return nil
}

func (e *Engine) DeleteIndex(ctx context.Context, mainName string, indexName string) error {
	idx, err := e.getIndexByName(ctx, indexName)
	if err != nil {
		return err
	}
	if idx.MainTable != mainName {
		return ots.ErrObjectNotExist("index %q does not exist on table %q", indexName, mainName)
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return ots.ErrInternal("begin tx: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteIdent(idx.PGName)); err != nil {
		return ots.ErrInternal("drop index table: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM \""+indexRegistryTable+"\" WHERE index_name = ?", indexName); err != nil {
		return ots.ErrInternal("delete index meta: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return ots.ErrInternal("commit: %v", err)
	}
	return nil
}

func (e *Engine) GetReadMeta(ctx context.Context, name string) (*TableMeta, error) {
	meta, err := e.GetTableMeta(ctx, name)
	if err == nil {
		return meta, nil
	}
	if ots.AsError(err).Code != ots.CodeObjectNotExist {
		return nil, err
	}
	idx, idxErr := e.getIndexByName(ctx, name)
	if idxErr != nil {
		return nil, err
	}
	main, mainErr := e.GetTableMeta(ctx, idx.MainTable)
	if mainErr != nil {
		return nil, mainErr
	}
	return e.buildIndexTableMeta(main, idx)
}

func valueMatchesPKType(t ots.PrimaryKeyType, v *ots.Value) bool {
	if v == nil {
		return false
	}
	switch t {
	case ots.PrimaryKeyInteger:
		return v.Type == ots.ValueInteger
	case ots.PrimaryKeyString:
		return v.Type == ots.ValueString
	case ots.PrimaryKeyBinary:
		return v.Type == ots.ValueBinary
	}
	return false
}

func (e *Engine) computeIndexPK(main *TableMeta, idx *IndexMeta, pk ots.PrimaryKey, raw attrs) (ots.PrimaryKey, bool, error) {
	pkCols, err := e.indexPKColumns(main, idx)
	if err != nil {
		return nil, false, err
	}
	mainPK := make(map[string]*ots.Value, len(pk))
	for i := range pk {
		mainPK[pk[i].Name] = pk[i].Value
	}
	out := make(ots.PrimaryKey, len(pkCols))
	for i, col := range pkCols {
		out[i] = ots.PrimaryKeyColumn{Name: col.Name, Type: col.Type}
		if val, ok := mainPK[col.Name]; ok && val != nil {
			out[i].Value = val
			continue
		}
		attrCol, ok := raw[col.Name]
		if !ok || attrCol == nil || len(attrCol.Versions) == 0 {
			return nil, false, nil
		}
		val, err := decodeAttrValue(attrCol.Type, attrCol.Versions[0].Val)
		if err != nil {
			return nil, false, ots.ErrInternal("decode index column %q: %v", col.Name, err)
		}
		if !valueMatchesPKType(col.Type, val) {
			return nil, false, nil
		}
		out[i].Value = val
	}
	return out, true, nil
}

func coveringAttrs(idx *IndexMeta, raw attrs) attrs {
	out := make(attrs)
	if raw == nil {
		return out
	}
	for _, name := range idx.DefinedColumns {
		col, ok := raw[name]
		if !ok || col == nil || len(col.Versions) == 0 {
			continue
		}
		out[name] = &attrColumn{
			Type:     col.Type,
			Versions: []attrVersion{col.Versions[0]},
		}
	}
	return out
}

func cloneAttrs(raw attrs) attrs {
	if raw == nil {
		return nil
	}
	out := make(attrs, len(raw))
	for name, col := range raw {
		if col == nil {
			out[name] = nil
			continue
		}
		versions := make([]attrVersion, len(col.Versions))
		copy(versions, col.Versions)
		out[name] = &attrColumn{Type: col.Type, Versions: versions}
	}
	return out
}

func (e *Engine) upsertIndexRow(ctx context.Context, tx *pgTx, idx *IndexMeta, pkCols []PKColumn, indexPK ots.PrimaryKey, data []byte) error {
	cols := make([]string, 0, len(pkCols)+1)
	placeholders := make([]string, 0, len(pkCols)+1)
	args := make([]interface{}, 0, len(pkCols)+1)
	for i, col := range pkCols {
		cols = append(cols, quoteIdent(col.Name))
		placeholders = append(placeholders, "?")
		args = append(args, pkArg(col, indexPK[i].Value))
	}
	cols = append(cols, "\"__ots_attrs\"")
	placeholders = append(placeholders, "?")
	args = append(args, string(data))
	conflictCols := make([]string, len(pkCols))
	for i, col := range pkCols {
		conflictCols[i] = quoteIdent(col.Name)
	}
	query := "INSERT INTO " + quoteIdent(idx.PGName) + " (" + strings.Join(cols, ", ") + ") VALUES (" +
		strings.Join(placeholders, ", ") + ") ON CONFLICT (" + strings.Join(conflictCols, ", ") + ") DO UPDATE SET \"__ots_attrs\" = EXCLUDED.\"__ots_attrs\""
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return execError(err, "upsert index row")
	}
	return nil
}

func (e *Engine) deleteIndexRow(ctx context.Context, tx *pgTx, idx *IndexMeta, pkCols []PKColumn, indexPK ots.PrimaryKey) error {
	parts := make([]string, len(pkCols))
	args := make([]interface{}, len(pkCols))
	for i, col := range pkCols {
		parts[i] = quoteIdent(col.Name) + " = ?"
		args[i] = pkArg(col, indexPK[i].Value)
	}
	query := "DELETE FROM " + quoteIdent(idx.PGName) + " WHERE " + strings.Join(parts, " AND ")
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return execError(err, "delete index row")
	}
	return nil
}

func (e *Engine) syncIndexForRow(ctx context.Context, tx *pgTx, main *TableMeta, idx *IndexMeta, pk ots.PrimaryKey, oldRaw, newRaw attrs) error {
	pkCols, err := e.indexPKColumns(main, idx)
	if err != nil {
		return err
	}
	oldPK, oldOK, err := e.computeIndexPK(main, idx, pk, oldRaw)
	if err != nil {
		return err
	}
	newPK, newOK, err := e.computeIndexPK(main, idx, pk, newRaw)
	if err != nil {
		return err
	}
	if oldOK {
		if err := e.deleteIndexRow(ctx, tx, idx, pkCols, oldPK); err != nil {
			return err
		}
	}
	if newOK {
		data, err := encodeAttrs(coveringAttrs(idx, newRaw))
		if err != nil {
			return ots.ErrInternal("encode index attrs: %v", err)
		}
		if err := e.upsertIndexRow(ctx, tx, idx, pkCols, newPK, data); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) syncIndexes(ctx context.Context, tx *pgTx, main *TableMeta, pk ots.PrimaryKey, oldRaw, newRaw attrs) error {
	indexes, err := e.indexesForTable(ctx, tx, main.Name)
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		if err := e.syncIndexForRow(ctx, tx, main, idx, pk, oldRaw, newRaw); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) backfillIndex(ctx context.Context, tx *pgTx, main *TableMeta, idx *IndexMeta) error {
	pkCols, err := e.indexPKColumns(main, idx)
	if err != nil {
		return err
	}
	cols := make([]string, 0, len(main.PrimaryKey)+1)
	for _, col := range main.PrimaryKey {
		cols = append(cols, quoteIdent(col.Name))
	}
	cols = append(cols, "\"__ots_attrs\"")
	rows, err := tx.QueryContext(ctx, "SELECT "+strings.Join(cols, ", ")+" FROM "+quoteIdent(main.PGName))
	if err != nil {
		return ots.ErrInternal("backfill scan: %v", err)
	}
	type entry struct {
		pk  ots.PrimaryKey
		raw attrs
	}
	var entries []entry
	for rows.Next() {
		pk, attrData, err := e.scanRowPK(main, rows.Scan)
		if err != nil {
			rows.Close()
			return ots.ErrInternal("backfill scan row: %v", err)
		}
		raw, err := decodeAttrs(attrData)
		if err != nil {
			rows.Close()
			return ots.ErrInternal("backfill decode row: %v", err)
		}
		entries = append(entries, entry{pk: pk, raw: raw})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ots.ErrInternal("backfill scan: %v", err)
	}
	rows.Close()
	for _, item := range entries {
		indexPK, ok, err := e.computeIndexPK(main, idx, item.pk, item.raw)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		data, err := encodeAttrs(coveringAttrs(idx, item.raw))
		if err != nil {
			return ots.ErrInternal("encode index attrs: %v", err)
		}
		if err := e.upsertIndexRow(ctx, tx, idx, pkCols, indexPK, data); err != nil {
			return err
		}
	}
	return nil
}
