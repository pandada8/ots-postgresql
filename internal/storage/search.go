package storage

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pandada8/ots-postgresql/internal/ots"
)

const searchRegistryTable = "__ots_search_indexes"

const (
	SearchFieldLong    = "LONG"
	SearchFieldDouble  = "DOUBLE"
	SearchFieldBoolean = "BOOLEAN"
	SearchFieldKeyword = "KEYWORD"
	SearchFieldText    = "TEXT"
	SearchFieldDate    = "DATE"
)

type SearchFieldSchema struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Index       bool     `json:"index"`
	SortAndAgg  bool     `json:"sort_and_agg"`
	DateFormats []string `json:"date_formats,omitempty"`
}

type SearchSorter struct {
	Field      string `json:"field"`
	Descending bool   `json:"descending,omitempty"`
}

type SearchIndexMeta struct {
	Name      string              `json:"name"`
	MainTable string              `json:"main_table"`
	PGName    string              `json:"pg_name"`
	Fields    []SearchFieldSchema `json:"fields"`
	IndexSort []SearchSorter      `json:"index_sort,omitempty"`
}

func (m *SearchIndexMeta) field(name string) (SearchFieldSchema, bool) {
	for _, field := range m.Fields {
		if field.Name == name {
			return field, true
		}
	}
	return SearchFieldSchema{}, false
}

func (e *Engine) ensureSearchRegistry(ctx context.Context) error {
	_, err := e.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS \""+searchRegistryTable+"\" ("+
		"\"main_table\" VARCHAR(255) NOT NULL,"+
		"\"index_name\" VARCHAR(255) NOT NULL,"+
		"\"pg_name\" VARCHAR(63) NOT NULL,"+
		"\"meta\" JSONB NOT NULL,"+
		"PRIMARY KEY (\"main_table\", \"index_name\"),"+
		"CONSTRAINT uq_search_pg_name UNIQUE (\"pg_name\")"+
		")")
	if err != nil {
		return fmt.Errorf("create search index registry: %w", err)
	}
	return nil
}

func (e *Engine) searchIndexesForTable(ctx context.Context, q queryer, mainName string) ([]*SearchIndexMeta, error) {
	rows, err := q.QueryContext(ctx, "SELECT meta FROM \""+searchRegistryTable+"\" WHERE main_table = ? ORDER BY index_name", mainName)
	if err != nil {
		return nil, ots.ErrInternal("list search indexes: %v", err)
	}
	defer rows.Close()
	var indexes []*SearchIndexMeta
	for rows.Next() {
		var metaJSON []byte
		if err := rows.Scan(&metaJSON); err != nil {
			return nil, ots.ErrInternal("scan search index meta: %v", err)
		}
		idx := new(SearchIndexMeta)
		if err := json.Unmarshal(metaJSON, idx); err != nil {
			return nil, ots.ErrInternal("parse search index meta: %v", err)
		}
		indexes = append(indexes, idx)
	}
	if err := rows.Err(); err != nil {
		return nil, ots.ErrInternal("list search indexes: %v", err)
	}
	return indexes, nil
}

func (e *Engine) GetSearchIndex(ctx context.Context, mainName, indexName string) (*SearchIndexMeta, error) {
	var metaJSON []byte
	err := e.db.QueryRowContext(ctx,
		"SELECT meta FROM \""+searchRegistryTable+"\" WHERE main_table = ? AND index_name = ?", mainName, indexName).Scan(&metaJSON)
	if err == sql.ErrNoRows {
		return nil, ots.ErrObjectNotExist("search index %q does not exist on table %q", indexName, mainName)
	}
	if err != nil {
		return nil, ots.ErrInternal("load search index meta: %v", err)
	}
	idx := new(SearchIndexMeta)
	if err := json.Unmarshal(metaJSON, idx); err != nil {
		return nil, ots.ErrInternal("parse search index meta: %v", err)
	}
	return idx, nil
}

func (e *Engine) ListSearchIndexes(ctx context.Context, mainName string) ([]*SearchIndexMeta, error) {
	if _, err := e.GetTableMeta(ctx, mainName); err != nil {
		return nil, err
	}
	return e.searchIndexesForTable(ctx, e.db, mainName)
}

func (e *Engine) searchIndexPGName(mainName, indexName string) string {
	sum := sha1.Sum([]byte(mainName + "/" + indexName))
	candidate := e.prefix + "si_" + indexName + "_" + hex.EncodeToString(sum[:])[:8]
	if len(candidate) <= 63 && identifierPattern.MatchString(candidate) {
		return candidate
	}
	return e.prefix + "s_" + hex.EncodeToString(sum[:])[:32]
}

func validSearchFieldType(t string) bool {
	switch t {
	case SearchFieldLong, SearchFieldDouble, SearchFieldBoolean, SearchFieldKeyword, SearchFieldText, SearchFieldDate:
		return true
	}
	return false
}

func (e *Engine) prepareSearchIndex(main *TableMeta, idx *SearchIndexMeta) error {
	if idx == nil {
		return ots.ErrParameterInvalid("search index meta is required")
	}
	if err := validateTableName(idx.Name); err != nil {
		return err
	}
	idx.MainTable = main.Name
	if len(idx.Fields) == 0 {
		return ots.ErrParameterInvalid("search index %q must have at least one field", idx.Name)
	}
	seen := make(map[string]bool)
	for _, field := range idx.Fields {
		if !identifierPattern.MatchString(field.Name) || len(field.Name) > 255 {
			return ots.ErrParameterInvalid("invalid search index field name %q", field.Name)
		}
		if strings.HasPrefix(field.Name, "__ots_") {
			return ots.ErrParameterInvalid("search index field name %q is reserved", field.Name)
		}
		if seen[field.Name] {
			return ots.ErrParameterInvalid("search index %q has duplicate field %q", idx.Name, field.Name)
		}
		seen[field.Name] = true
		if !validSearchFieldType(field.Type) {
			return ots.ErrParameterInvalid("unsupported search index field type %q for field %q", field.Type, field.Name)
		}
	}
	for _, sorter := range idx.IndexSort {
		if sorter.Field == "" {
			continue
		}
		if _, ok := idx.field(sorter.Field); !ok {
			return ots.ErrParameterInvalid("index sort field %q is not part of search index %q", sorter.Field, idx.Name)
		}
	}
	idx.PGName = e.searchIndexPGName(main.Name, idx.Name)
	return nil
}

func searchFieldDDL(field SearchFieldSchema) string {
	switch field.Type {
	case SearchFieldLong, SearchFieldDate:
		return quoteIdent(field.Name) + " BIGINT NULL"
	case SearchFieldDouble:
		return quoteIdent(field.Name) + " DOUBLE PRECISION NULL"
	case SearchFieldBoolean:
		return quoteIdent(field.Name) + " BOOLEAN NULL"
	case SearchFieldKeyword:
		return quoteIdent(field.Name) + " VARCHAR(1024) NULL"
	case SearchFieldText:
		return quoteIdent(field.Name) + " TEXT NULL"
	}
	return ""
}

func searchPKColumn(index int) string {
	return fmt.Sprintf("__ots_pk_%d", index)
}

func searchIndexDDL(pgName string, pk []PKColumn, fields []SearchFieldSchema) (string, []string) {
	var columns []string
	var pkNames []string
	for i, col := range pk {
		def := quoteIdent(searchPKColumn(i))
		if col.Type == ots.PrimaryKeyInteger {
			def += " BIGINT NOT NULL"
		} else {
			def += " BYTEA NOT NULL"
		}
		columns = append(columns, def)
		pkNames = append(pkNames, quoteIdent(searchPKColumn(i)))
	}
	for _, field := range fields {
		columns = append(columns, searchFieldDDL(field))
	}
	ddl := "CREATE TABLE " + quoteIdent(pgName) + " (" + strings.Join(columns, ", ") +
		", PRIMARY KEY (" + strings.Join(pkNames, ", ") + ")"
	ddl += ")"
	sum := sha1.Sum([]byte(pgName))
	keyPrefix := "k_" + hex.EncodeToString(sum[:])[:8]
	var indexes []string
	for i, field := range fields {
		keyName := fmt.Sprintf("%s_%d", keyPrefix, i)
		indexes = append(indexes, "CREATE INDEX "+quoteIdent(keyName)+" ON "+quoteIdent(pgName)+" ("+quoteIdent(field.Name)+")")
	}
	return ddl, indexes
}

func (e *Engine) createSearchIndexStorage(ctx context.Context, tx *pgTx, main *TableMeta, idx *SearchIndexMeta) error {
	ddl, indexes := searchIndexDDL(idx.PGName, main.PrimaryKey, idx.Fields)
	if _, err := tx.ExecContext(ctx, ddl); err != nil {
		if isDuplicateTable(err) {
			return ots.ErrObjectAlreadyExist("search index %q already exists", idx.Name)
		}
		return execError(err, "create search index table")
	}
	for _, stmt := range indexes {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return execError(err, "create search index key")
		}
	}
	metaJSON, err := json.Marshal(idx)
	if err != nil {
		return ots.ErrInternal("marshal search index meta: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO \""+searchRegistryTable+"\" (main_table, index_name, pg_name, meta) VALUES (?, ?, ?, ?)",
		idx.MainTable, idx.Name, idx.PGName, string(metaJSON)); err != nil {
		if isUniqueViolation(err) {
			return ots.ErrObjectAlreadyExist("search index %q already exists", idx.Name)
		}
		return execError(err, "save search index meta")
	}
	return nil
}

func (e *Engine) CreateSearchIndex(ctx context.Context, mainName string, idx *SearchIndexMeta) error {
	main, err := e.GetTableMeta(ctx, mainName)
	if err != nil {
		return err
	}
	if err := e.prepareSearchIndex(main, idx); err != nil {
		return err
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return ots.ErrInternal("begin tx: %v", err)
	}
	defer tx.Rollback()
	var exists int
	if err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM \""+searchRegistryTable+"\" WHERE main_table = ? AND index_name = ?", mainName, idx.Name).Scan(&exists); err != nil {
		return ots.ErrInternal("check search index existence: %v", err)
	}
	if exists > 0 {
		return ots.ErrObjectAlreadyExist("search index %q already exists on table %q", idx.Name, mainName)
	}
	if err := e.createSearchIndexStorage(ctx, tx, main, idx); err != nil {
		return err
	}
	if err := e.backfillSearchIndex(ctx, tx, main, idx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return ots.ErrInternal("commit: %v", err)
	}
	return nil
}

func (e *Engine) DeleteSearchIndex(ctx context.Context, mainName, indexName string) error {
	idx, err := e.GetSearchIndex(ctx, mainName, indexName)
	if err != nil {
		return err
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return ots.ErrInternal("begin tx: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteIdent(idx.PGName)); err != nil {
		return ots.ErrInternal("drop search index table: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		"DELETE FROM \""+searchRegistryTable+"\" WHERE main_table = ? AND index_name = ?", mainName, indexName); err != nil {
		return ots.ErrInternal("delete search index meta: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return ots.ErrInternal("commit: %v", err)
	}
	return nil
}

func javaDateLayout(format string) string {
	var out strings.Builder
	for pos := 0; pos < len(format); {
		rest := format[pos:]
		matched := false
		for _, rule := range []struct{ java, goLayout string }{
			{"yyyy", "2006"}, {"yy", "06"},
			{"MMMM", "January"}, {"MMM", "Jan"}, {"MM", "01"}, {"M", "1"},
			{"dd", "02"}, {"d", "2"},
			{"EEEE", "Monday"}, {"EEE", "Mon"},
			{"HH", "15"}, {"H", "15"}, {"hh", "03"}, {"h", "3"},
			{"mm", "04"}, {"m", "4"},
			{"ss", "05"}, {"s", "5"},
			{"SSS", "000"}, {"SS", "00"}, {"S", "0"},
			{"a", "PM"},
			{"zzz", "MST"}, {"zz", "MST"}, {"z", "MST"},
			{"XXX", "Z07:00"}, {"XX", "-0700"}, {"X", "Z07"},
			{"Z", "-0700"},
		} {
			if strings.HasPrefix(rest, rule.java) {
				out.WriteString(rule.goLayout)
				pos += len(rule.java)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		if rest[0] == '\'' {
			end := strings.IndexByte(rest[1:], '\'')
			if end >= 0 {
				out.WriteString(rest[1 : end+1])
				pos += end + 2
				continue
			}
		}
		out.WriteByte(rest[0])
		pos++
	}
	return out.String()
}

func parseSearchDate(value string, formats []string) (int64, bool) {
	for _, format := range formats {
		if format == "epoch_millis" {
			if millis, err := strconv.ParseInt(value, 10, 64); err == nil {
				return millis, true
			}
			continue
		}
		if parsed, err := time.ParseInLocation(javaDateLayout(format), value, time.UTC); err == nil {
			return parsed.UnixMilli(), true
		}
	}
	if millis, err := strconv.ParseInt(value, 10, 64); err == nil {
		return millis, true
	}
	return 0, false
}

func searchFieldValue(field SearchFieldSchema, raw attrs) interface{} {
	col, ok := raw[field.Name]
	if !ok || col == nil || len(col.Versions) == 0 {
		return nil
	}
	value := col.Versions[0].Val
	switch field.Type {
	case SearchFieldLong:
		number, err := rawInt64(value)
		if err != nil {
			return nil
		}
		return number
	case SearchFieldDouble:
		number, err := rawFloat64(value)
		if err != nil {
			return nil
		}
		return number
	case SearchFieldBoolean:
		boolean, ok := value.(bool)
		if !ok {
			return nil
		}
		return boolean
	case SearchFieldKeyword, SearchFieldText:
		text, ok := value.(string)
		if !ok {
			return nil
		}
		return text
	case SearchFieldDate:
		switch typed := value.(type) {
		case string:
			millis, ok := parseSearchDate(typed, field.DateFormats)
			if !ok {
				return nil
			}
			return millis
		default:
			number, err := rawInt64(value)
			if err != nil {
				return nil
			}
			return number
		}
	}
	return nil
}

func searchPKFieldValue(field SearchFieldSchema, value *ots.Value) interface{} {
	if value == nil {
		return nil
	}
	switch field.Type {
	case SearchFieldLong:
		return value.Integer
	case SearchFieldDouble:
		return value.Double
	case SearchFieldBoolean:
		return value.Boolean
	case SearchFieldKeyword, SearchFieldText:
		if value.Type == ots.ValueString {
			return value.String
		}
		return string(value.Binary)
	case SearchFieldDate:
		if value.Type == ots.ValueString {
			if millis, ok := parseSearchDate(value.String, field.DateFormats); ok {
				return millis
			}
			return nil
		}
		return value.Integer
	}
	return nil
}

func searchFieldValueFromRow(main *TableMeta, field SearchFieldSchema, pk ots.PrimaryKey, raw attrs) interface{} {
	for i, col := range main.PrimaryKey {
		if col.Name == field.Name {
			return searchPKFieldValue(field, pk[i].Value)
		}
	}
	return searchFieldValue(field, raw)
}

func (e *Engine) upsertSearchRow(ctx context.Context, tx *pgTx, main *TableMeta, idx *SearchIndexMeta, pk ots.PrimaryKey, raw attrs) error {
	cols := make([]string, 0, len(main.PrimaryKey)+len(idx.Fields))
	placeholders := make([]string, 0, len(main.PrimaryKey)+len(idx.Fields))
	args := make([]interface{}, 0, len(main.PrimaryKey)+len(idx.Fields))
	updates := make([]string, 0, len(idx.Fields))
	for i, col := range main.PrimaryKey {
		cols = append(cols, quoteIdent(searchPKColumn(i)))
		placeholders = append(placeholders, "?")
		args = append(args, pkArg(col, pk[i].Value))
	}
	for _, field := range idx.Fields {
		cols = append(cols, quoteIdent(field.Name))
		placeholders = append(placeholders, "?")
		args = append(args, searchFieldValueFromRow(main, field, pk, raw))
		updates = append(updates, quoteIdent(field.Name)+" = EXCLUDED."+quoteIdent(field.Name))
	}
	conflictCols := make([]string, len(main.PrimaryKey))
	for i := range main.PrimaryKey {
		conflictCols[i] = quoteIdent(searchPKColumn(i))
	}
	query := "INSERT INTO " + quoteIdent(idx.PGName) + " (" + strings.Join(cols, ", ") + ") VALUES (" +
		strings.Join(placeholders, ", ") + ") ON CONFLICT (" + strings.Join(conflictCols, ", ") + ") DO UPDATE SET " + strings.Join(updates, ", ")
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return execError(err, "upsert search index row")
	}
	return nil
}

func (e *Engine) deleteSearchRow(ctx context.Context, tx *pgTx, main *TableMeta, idx *SearchIndexMeta, pk ots.PrimaryKey) error {
	parts := make([]string, len(main.PrimaryKey))
	args := make([]interface{}, len(main.PrimaryKey))
	for i, col := range main.PrimaryKey {
		parts[i] = quoteIdent(searchPKColumn(i)) + " = ?"
		args[i] = pkArg(col, pk[i].Value)
	}
	query := "DELETE FROM " + quoteIdent(idx.PGName) + " WHERE " + strings.Join(parts, " AND ")
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return execError(err, "delete search index row")
	}
	return nil
}

func (e *Engine) syncSearchIndexes(ctx context.Context, tx *pgTx, main *TableMeta, pk ots.PrimaryKey, newRaw attrs) error {
	indexes, err := e.searchIndexesForTable(ctx, tx, main.Name)
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		if newRaw == nil {
			if err := e.deleteSearchRow(ctx, tx, main, idx, pk); err != nil {
				return err
			}
			continue
		}
		if err := e.upsertSearchRow(ctx, tx, main, idx, pk, newRaw); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) backfillSearchIndex(ctx context.Context, tx *pgTx, main *TableMeta, idx *SearchIndexMeta) error {
	cols := make([]string, 0, len(main.PrimaryKey)+1)
	for _, col := range main.PrimaryKey {
		cols = append(cols, quoteIdent(col.Name))
	}
	cols = append(cols, "\"__ots_attrs\"")
	rows, err := tx.QueryContext(ctx, "SELECT "+strings.Join(cols, ", ")+" FROM "+quoteIdent(main.PGName))
	if err != nil {
		return ots.ErrInternal("search index backfill scan: %v", err)
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
			return ots.ErrInternal("search index backfill scan row: %v", err)
		}
		raw, err := decodeAttrs(attrData)
		if err != nil {
			rows.Close()
			return ots.ErrInternal("search index backfill decode row: %v", err)
		}
		entries = append(entries, entry{pk: pk, raw: raw})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ots.ErrInternal("search index backfill scan: %v", err)
	}
	rows.Close()
	for _, item := range entries {
		if err := e.upsertSearchRow(ctx, tx, main, idx, item.pk, item.raw); err != nil {
			return err
		}
	}
	return nil
}
