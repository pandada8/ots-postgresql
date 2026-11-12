package storage

import (
	"context"
	"database/sql"
	"errors"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/pandada8/ots-postgresql/internal/ots"
	"github.com/pandada8/ots-postgresql/internal/otsfilter"
	"github.com/pandada8/ots-postgresql/internal/plainbuffer"
)

const (
	DirectionForward  = 0
	DirectionBackward = 1

	defaultRangeLimit = 5000
	rangeScanBatch    = 200
)

type TimeRange struct {
	Start       int64
	End         int64
	Specific    int64
	HasSpecific bool
}

type ReadCriteria struct {
	ColumnsToGet []string
	MaxVersions  int32
	TimeRange    *TimeRange
	Filter       otsfilter.RowFilter
	Pagination   *otsfilter.Pagination
	StartColumn  *string
	EndColumn    *string
}

type UpdateCell struct {
	Name         string
	Value        *ots.Value
	Timestamp    int64
	HasTimestamp bool
	Op           plainbuffer.CellOp
}

type OperationType int

const (
	OperationPut OperationType = iota + 1
	OperationUpdate
	OperationDelete
)

func validatePrimaryKey(meta *TableMeta, pk ots.PrimaryKey, allowAuto bool) error {
	if len(pk) != len(meta.PrimaryKey) {
		return ots.ErrParameterInvalid("primary key count mismatch: expect %d, got %d", len(meta.PrimaryKey), len(pk))
	}
	for i, col := range meta.PrimaryKey {
		if pk[i].Name != col.Name {
			return ots.ErrParameterInvalid("primary key column %d: expect %q, got %q", i, col.Name, pk[i].Name)
		}
		if pk[i].Special == ots.SpecialAutoIncrement {
			if !allowAuto || !col.AutoIncrement {
				return ots.ErrParameterInvalid("column %q is not an auto increment primary key", col.Name)
			}
			continue
		}
		if pk[i].Special != ots.SpecialNone {
			return ots.ErrParameterInvalid("INF_MIN/INF_MAX are only allowed in range boundaries")
		}
		if pk[i].Value == nil {
			return ots.ErrParameterInvalid("primary key column %q has no value", col.Name)
		}
		switch col.Type {
		case ots.PrimaryKeyInteger:
			if pk[i].Value.Type != ots.ValueInteger {
				return ots.ErrParameterInvalid("primary key column %q must be INTEGER", col.Name)
			}
		case ots.PrimaryKeyString:
			if pk[i].Value.Type != ots.ValueString {
				return ots.ErrParameterInvalid("primary key column %q must be STRING", col.Name)
			}
			if len(pk[i].Value.String) > 1024 {
				return ots.ErrOutOfColumnSizeLimit("primary key column %q exceeds 1KB", col.Name)
			}
		case ots.PrimaryKeyBinary:
			if pk[i].Value.Type != ots.ValueBinary {
				return ots.ErrParameterInvalid("primary key column %q must be BINARY", col.Name)
			}
			if len(pk[i].Value.Binary) > 1024 {
				return ots.ErrOutOfColumnSizeLimit("primary key column %q exceeds 1KB", col.Name)
			}
		}
	}
	return nil
}

func pkArg(col PKColumn, value *ots.Value) interface{} {
	switch col.Type {
	case ots.PrimaryKeyInteger:
		return value.Integer
	default:
		if value.Type == ots.ValueString {
			return []byte(value.String)
		}
		return value.Binary
	}
}

func pkEqualArgs(meta *TableMeta, pk ots.PrimaryKey) []interface{} {
	args := make([]interface{}, len(pk))
	for i, col := range meta.PrimaryKey {
		args[i] = pkArg(col, pk[i].Value)
	}
	return args
}

func pkWhereClause(meta *TableMeta) string {
	parts := make([]string, len(meta.PrimaryKey))
	for i, col := range meta.PrimaryKey {
		parts[i] = quoteIdent(col.Name) + " = ?"
	}
	return strings.Join(parts, " AND ")
}

func (e *Engine) scanRowPK(meta *TableMeta, scan func(dest ...interface{}) error) (ots.PrimaryKey, []byte, error) {
	dest := make([]interface{}, 0, len(meta.PrimaryKey)+1)
	intTargets := make([]int64, len(meta.PrimaryKey))
	blobTargets := make([][]byte, len(meta.PrimaryKey))
	for i, col := range meta.PrimaryKey {
		if col.Type == ots.PrimaryKeyInteger {
			dest = append(dest, &intTargets[i])
		} else {
			dest = append(dest, &blobTargets[i])
		}
	}
	var attrData []byte
	dest = append(dest, &attrData)
	if err := scan(dest...); err != nil {
		return nil, nil, err
	}
	pk := make(ots.PrimaryKey, len(meta.PrimaryKey))
	for i, col := range meta.PrimaryKey {
		pk[i] = ots.PrimaryKeyColumn{Name: col.Name, Type: col.Type}
		if col.Type == ots.PrimaryKeyInteger {
			pk[i].Value = ots.IntegerValue(intTargets[i])
		} else if col.Type == ots.PrimaryKeyString {
			pk[i].Value = ots.StringValue(string(blobTargets[i]))
		} else {
			pk[i].Value = ots.BinaryValue(blobTargets[i])
		}
	}
	return pk, attrData, nil
}

func (e *Engine) selectVersions(meta *TableMeta, raw attrs, criteria ReadCriteria, nowMs int64) (otsfilter.Versions, []string, error) {
	versions := make(otsfilter.Versions, len(raw))
	var names []string
	var ttlCutoff int64
	if meta.Options.TimeToLive > 0 {
		ttlCutoff = nowMs - int64(meta.Options.TimeToLive)*1000
	}
	for name, col := range raw {
		selected := make([]ots.AttributeColumn, 0, len(col.Versions))
		for _, version := range col.Versions {
			if criteria.TimeRange != nil {
				if criteria.TimeRange.HasSpecific {
					if version.Ts != criteria.TimeRange.Specific {
						continue
					}
				} else {
					if version.Ts < criteria.TimeRange.Start || version.Ts >= criteria.TimeRange.End {
						continue
					}
				}
			}
			if ttlCutoff > 0 && version.Ts < ttlCutoff {
				continue
			}
			value, err := decodeAttrValue(col.Type, version.Val)
			if err != nil {
				return nil, nil, ots.ErrInternal("column %q: %v", name, err)
			}
			selected = append(selected, ots.AttributeColumn{Name: name, Value: value, Timestamp: version.Ts})
			if criteria.MaxVersions > 0 && int32(len(selected)) >= criteria.MaxVersions {
				break
			}
		}
		if len(selected) > 0 {
			versions[name] = selected
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return versions, names, nil
}

func projectColumns(versions otsfilter.Versions, names []string, criteria ReadCriteria) []ots.AttributeColumn {
	var wanted map[string]bool
	if len(criteria.ColumnsToGet) > 0 {
		wanted = make(map[string]bool, len(criteria.ColumnsToGet))
		for _, name := range criteria.ColumnsToGet {
			wanted[name] = true
		}
	}
	var selected []string
	for _, name := range names {
		if wanted != nil && !wanted[name] {
			continue
		}
		if criteria.StartColumn != nil && name < *criteria.StartColumn {
			continue
		}
		if criteria.EndColumn != nil && name >= *criteria.EndColumn {
			continue
		}
		selected = append(selected, name)
	}
	if criteria.Pagination != nil {
		offset := int(criteria.Pagination.Offset)
		if offset > len(selected) {
			offset = len(selected)
		}
		selected = selected[offset:]
		if criteria.Pagination.Limit >= 0 && int32(len(selected)) > criteria.Pagination.Limit {
			selected = selected[:criteria.Pagination.Limit]
		}
	}
	var result []ots.AttributeColumn
	for _, name := range selected {
		result = append(result, versions[name]...)
	}
	return result
}

func (e *Engine) buildDisplayRow(meta *TableMeta, pk ots.PrimaryKey, raw attrs, criteria ReadCriteria, nowMs int64) (*ots.Row, error) {
	versions, names, err := e.selectVersions(meta, raw, criteria, nowMs)
	if err != nil {
		return nil, err
	}
	if len(raw) > 0 && len(names) == 0 {
		return nil, nil
	}
	if criteria.Filter != nil && !criteria.Filter.Pass(versions) {
		return nil, nil
	}
	return &ots.Row{PrimaryKey: pk, Attributes: projectColumns(versions, names, criteria)}, nil
}

func (e *Engine) GetRow(ctx context.Context, meta *TableMeta, pk ots.PrimaryKey, criteria ReadCriteria) (*ots.Row, error) {
	if err := validatePrimaryKey(meta, pk, false); err != nil {
		return nil, err
	}
	query := "SELECT \"__ots_attrs\" FROM " + quoteIdent(meta.PGName) + " WHERE " + pkWhereClause(meta)
	var attrData []byte
	err := e.db.QueryRowContext(ctx, query, pkEqualArgs(meta, pk)...).Scan(&attrData)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, ots.ErrInternal("get row: %v", err)
	}
	raw, err := decodeAttrs(attrData)
	if err != nil {
		return nil, ots.ErrInternal("decode row: %v", err)
	}
	return e.buildDisplayRow(meta, pk, raw, criteria, time.Now().UnixMilli())
}

func conjunction(parts []string) string {
	if len(parts) == 0 {
		return "1=1"
	}
	return "(" + strings.Join(parts, " AND ") + ")"
}

// rangeBranches builds a lexicographic comparison against a PK bound.
// op is ">" or "<"; inclusive adds the equality branch.
func rangeBranches(meta *TableMeta, bound ots.PrimaryKey, op string, inclusive bool) (string, []interface{}) {
	var branches []string
	var args []interface{}
	var eqParts []string
	var eqArgs []interface{}
	stopped := false
	for i, col := range meta.PrimaryKey {
		cell := bound[i]
		if cell.Special == ots.SpecialInfMin || cell.Special == ots.SpecialInfMax {
			alwaysTrue := (op == ">" && cell.Special == ots.SpecialInfMin) ||
				(op == "<" && cell.Special == ots.SpecialInfMax)
			if alwaysTrue {
				branches = append(branches, conjunction(eqParts))
				args = append(args, eqArgs...)
			}
			stopped = true
			break
		}
		arg := pkArg(col, cell.Value)
		parts := append(append([]string{}, eqParts...), quoteIdent(col.Name)+" "+op+" ?")
		branchArgs := append(append([]interface{}{}, eqArgs...), arg)
		branches = append(branches, conjunction(parts))
		args = append(args, branchArgs...)
		eqParts = append(eqParts, quoteIdent(col.Name)+" = ?")
		eqArgs = append(eqArgs, arg)
	}
	if !stopped && inclusive {
		branches = append(branches, conjunction(eqParts))
		args = append(args, eqArgs...)
	}
	if len(branches) == 0 {
		return "1=0", nil
	}
	return "(" + strings.Join(branches, " OR ") + ")", args
}

func validateRangeBound(meta *TableMeta, bound ots.PrimaryKey) error {
	if len(bound) != len(meta.PrimaryKey) {
		return ots.ErrParameterInvalid("range boundary primary key count mismatch: expect %d, got %d", len(meta.PrimaryKey), len(bound))
	}
	for i, col := range meta.PrimaryKey {
		if bound[i].Name != col.Name {
			return ots.ErrParameterInvalid("range boundary column %d: expect %q, got %q", i, col.Name, bound[i].Name)
		}
		if bound[i].Special != ots.SpecialNone {
			continue
		}
		if bound[i].Value == nil {
			return ots.ErrParameterInvalid("range boundary column %q has no value", col.Name)
		}
		switch col.Type {
		case ots.PrimaryKeyInteger:
			if bound[i].Value.Type != ots.ValueInteger {
				return ots.ErrParameterInvalid("range boundary column %q must be INTEGER", col.Name)
			}
		case ots.PrimaryKeyString:
			if bound[i].Value.Type != ots.ValueString {
				return ots.ErrParameterInvalid("range boundary column %q must be STRING", col.Name)
			}
		case ots.PrimaryKeyBinary:
			if bound[i].Value.Type != ots.ValueBinary {
				return ots.ErrParameterInvalid("range boundary column %q must be BINARY", col.Name)
			}
		}
	}
	return nil
}

func (e *Engine) rangeQuery(meta *TableMeta, start, end ots.PrimaryKey, direction int, startInclusive bool, limit int) (string, []interface{}) {
	var lower, upper string
	var lowerArgs, upperArgs []interface{}
	if direction == DirectionForward {
		lower, lowerArgs = rangeBranches(meta, start, ">", startInclusive)
		upper, upperArgs = rangeBranches(meta, end, "<", false)
	} else {
		lower, lowerArgs = rangeBranches(meta, start, "<", startInclusive)
		upper, upperArgs = rangeBranches(meta, end, ">", false)
	}
	order := make([]string, len(meta.PrimaryKey))
	dir := " ASC"
	if direction == DirectionBackward {
		dir = " DESC"
	}
	cols := make([]string, 0, len(meta.PrimaryKey)+1)
	for i, col := range meta.PrimaryKey {
		cols = append(cols, quoteIdent(col.Name))
		order[i] = quoteIdent(col.Name) + dir
	}
	cols = append(cols, "\"__ots_attrs\"")
	query := "SELECT " + strings.Join(cols, ", ") + " FROM " + quoteIdent(meta.PGName) +
		" WHERE " + lower + " AND " + upper +
		" ORDER BY " + strings.Join(order, ", ") +
		" LIMIT ?"
	args := append(append([]interface{}{}, lowerArgs...), upperArgs...)
	args = append(args, limit)
	return query, args
}

func (e *Engine) GetRange(ctx context.Context, meta *TableMeta, start, end ots.PrimaryKey, direction int, limit int32, criteria ReadCriteria) ([]*ots.Row, ots.PrimaryKey, error) {
	if err := validateRangeBound(meta, start); err != nil {
		return nil, nil, err
	}
	if err := validateRangeBound(meta, end); err != nil {
		return nil, nil, err
	}
	if direction != DirectionForward && direction != DirectionBackward {
		return nil, nil, ots.ErrParameterInvalid("invalid direction %d", direction)
	}
	if limit <= 0 {
		limit = defaultRangeLimit
	}
	nowMs := time.Now().UnixMilli()
	var results []*ots.Row
	cursor := start
	inclusive := true
	for {
		query, args := e.rangeQuery(meta, cursor, end, direction, inclusive, rangeScanBatch)
		rows, err := e.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, nil, ots.ErrInternal("get range: %v", err)
		}
		type scannedRow struct {
			pk  ots.PrimaryKey
			raw attrs
		}
		var batch []scannedRow
		for rows.Next() {
			pk, attrData, err := e.scanRowPK(meta, rows.Scan)
			if err != nil {
				rows.Close()
				return nil, nil, ots.ErrInternal("scan range row: %v", err)
			}
			raw, err := decodeAttrs(attrData)
			if err != nil {
				rows.Close()
				return nil, nil, ots.ErrInternal("decode range row: %v", err)
			}
			batch = append(batch, scannedRow{pk: pk, raw: raw})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, nil, ots.ErrInternal("get range: %v", err)
		}
		for idx, item := range batch {
			display, err := e.buildDisplayRow(meta, item.pk, item.raw, criteria, nowMs)
			if err != nil {
				return nil, nil, err
			}
			if display != nil {
				results = append(results, display)
			}
			if int32(len(results)) >= limit {
				var nextPK ots.PrimaryKey
				if idx+1 < len(batch) {
					nextPK = batch[idx+1].pk
				} else {
					nextPK, err = e.nextRowPK(ctx, meta, item.pk, end, direction)
					if err != nil {
						return nil, nil, err
					}
				}
				return results, nextPK, nil
			}
		}
		if len(batch) < rangeScanBatch {
			return results, nil, nil
		}
		cursor = batch[len(batch)-1].pk
		inclusive = false
	}
}

func (e *Engine) nextRowPK(ctx context.Context, meta *TableMeta, after, end ots.PrimaryKey, direction int) (ots.PrimaryKey, error) {
	query, args := e.rangeQuery(meta, after, end, direction, false, 1)
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, ots.ErrInternal("get range next: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	pk, _, err := e.scanRowPK(meta, rows.Scan)
	if err != nil {
		return nil, ots.ErrInternal("scan range next: %v", err)
	}
	return pk, nil
}

func (e *Engine) loadRowForUpdate(ctx context.Context, tx *pgTx, meta *TableMeta, pk ots.PrimaryKey) (attrs, bool, error) {
	query := "SELECT \"__ots_attrs\" FROM " + quoteIdent(meta.PGName) + " WHERE " + pkWhereClause(meta) + " FOR UPDATE"
	var attrData []byte
	err := tx.QueryRowContext(ctx, query, pkEqualArgs(meta, pk)...).Scan(&attrData)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, execError(err, "lock row")
	}
	raw, err := decodeAttrs(attrData)
	if err != nil {
		return nil, false, ots.ErrInternal("decode row: %v", err)
	}
	return raw, true, nil
}

func currentVersions(raw attrs) (otsfilter.Versions, error) {
	if raw == nil {
		return otsfilter.Versions{}, nil
	}
	return raw.toVersions()
}

func (e *Engine) insertRow(ctx context.Context, tx *pgTx, meta *TableMeta, pk ots.PrimaryKey, data []byte, autoPlaceholder bool) (int64, error) {
	cols := make([]string, 0, len(meta.PrimaryKey)+1)
	placeholders := make([]string, 0, len(meta.PrimaryKey)+1)
	args := make([]interface{}, 0, len(meta.PrimaryKey)+1)
	autoCol := ""
	for i, col := range meta.PrimaryKey {
		if autoPlaceholder && col.AutoIncrement {
			autoCol = quoteIdent(col.Name)
			continue
		}
		cols = append(cols, quoteIdent(col.Name))
		placeholders = append(placeholders, "?")
		args = append(args, pkArg(col, pk[i].Value))
	}
	cols = append(cols, "\"__ots_attrs\"")
	placeholders = append(placeholders, "?")
	args = append(args, string(data))
	query := "INSERT INTO " + quoteIdent(meta.PGName) + " (" + strings.Join(cols, ", ") + ") VALUES (" + strings.Join(placeholders, ", ") + ")"
	if autoPlaceholder {
		query += " RETURNING " + autoCol
		var id int64
		if err := tx.QueryRowContext(ctx, query, args...).Scan(&id); err != nil {
			if isUniqueViolation(err) {
				return 0, errConcurrentInsert
			}
			return 0, execError(err, "insert row")
		}
		return id, nil
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		if isUniqueViolation(err) {
			return 0, errConcurrentInsert
		}
		return 0, execError(err, "insert row")
	}
	return 0, nil
}

func (e *Engine) updateRowAttrs(ctx context.Context, tx *pgTx, meta *TableMeta, pk ots.PrimaryKey, data []byte) error {
	query := "UPDATE " + quoteIdent(meta.PGName) + " SET \"__ots_attrs\" = ? WHERE " + pkWhereClause(meta)
	args := append([]interface{}{string(data)}, pkEqualArgs(meta, pk)...)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return execError(err, "update row")
	}
	return nil
}

func cellTimestamp(cell UpdateCell, nowMs int64) int64 {
	if cell.HasTimestamp {
		return cell.Timestamp
	}
	return nowMs
}

func (e *Engine) putRowTx(ctx context.Context, tx *pgTx, meta *TableMeta, pk ots.PrimaryKey, cells []UpdateCell, cond *otsfilter.Condition, returnPK bool) (ots.PrimaryKey, error) {
	if err := validatePrimaryKey(meta, pk, true); err != nil {
		return nil, err
	}
	autoIdx, hasAuto := meta.autoIncrementColumn()
	placeholder := hasAuto && pk[autoIdx].Special == ots.SpecialAutoIncrement
	var current attrs
	var exists bool
	var err error
	if !placeholder {
		current, exists, err = e.loadRowForUpdate(ctx, tx, meta, pk)
		if err != nil {
			return nil, err
		}
	}
	versions, err := currentVersions(current)
	if err != nil {
		return nil, ots.ErrInternal("decode row: %v", err)
	}
	if err := cond.Check(exists, versions); err != nil {
		return nil, err
	}
	nowMs := time.Now().UnixMilli()
	newAttrs := make(attrs)
	for _, cell := range cells {
		if cell.Value == nil {
			return nil, ots.ErrParameterInvalid("put cell %q has no value", cell.Name)
		}
		if err := newAttrs.put(cell.Name, cell.Value, cellTimestamp(cell, nowMs), meta.Options.MaxVersions); err != nil {
			return nil, err
		}
	}
	data, err := encodeAttrs(newAttrs)
	if err != nil {
		return nil, ots.ErrInternal("encode row: %v", err)
	}
	resultPK := pk.Clone()
	if placeholder {
		id, err := e.insertRow(ctx, tx, meta, pk, data, true)
		if err != nil {
			return nil, err
		}
		resultPK[autoIdx] = ots.PrimaryKeyColumn{
			Name:  meta.PrimaryKey[autoIdx].Name,
			Type:  ots.PrimaryKeyInteger,
			Value: ots.IntegerValue(id),
		}
	} else if exists {
		if err := e.updateRowAttrs(ctx, tx, meta, pk, data); err != nil {
			return nil, err
		}
	} else {
		if _, err := e.insertRow(ctx, tx, meta, pk, data, false); err != nil {
			return nil, err
		}
	}
	if err := e.syncIndexes(ctx, tx, meta, resultPK, current, newAttrs); err != nil {
		return nil, err
	}
	if err := e.syncSearchIndexes(ctx, tx, meta, resultPK, newAttrs); err != nil {
		return nil, err
	}
	if returnPK {
		return resultPK, nil
	}
	return nil, nil
}

const txMaxAttempts = 20

func isRetryableTxError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errConcurrentInsert) {
		return true
	}
	switch pgErrorCode(err) {
	case "40P01", "55P03", "40001":
		return true
	}
	return false
}

func (e *Engine) runTx(ctx context.Context, fn func(tx *pgTx) error) error {
	var lastErr error
	for attempt := 0; attempt < txMaxAttempts; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * 5 * time.Millisecond
			if backoff > 100*time.Millisecond {
				backoff = 100 * time.Millisecond
			}
			delay := backoff/2 + time.Duration(rand.Int64N(int64(backoff/2)+1))
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		lastErr = e.runTxOnce(ctx, fn)
		if lastErr == nil {
			return nil
		}
		if !isRetryableTxError(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

func (e *Engine) runTxOnce(ctx context.Context, fn func(tx *pgTx) error) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return ots.ErrInternal("begin tx: %v", err)
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return execError(err, "commit")
	}
	return nil
}

func (e *Engine) PutRow(ctx context.Context, meta *TableMeta, pk ots.PrimaryKey, cells []UpdateCell, cond *otsfilter.Condition, returnPK bool) (ots.PrimaryKey, error) {
	var resultPK ots.PrimaryKey
	err := e.runTx(ctx, func(tx *pgTx) error {
		var err error
		resultPK, err = e.putRowTx(ctx, tx, meta, pk, cells, cond, returnPK)
		return err
	})
	return resultPK, err
}

func (e *Engine) updateRowTx(ctx context.Context, tx *pgTx, meta *TableMeta, pk ots.PrimaryKey, cells []UpdateCell, cond *otsfilter.Condition, returnAfterModify bool, returnColumns []string) ([]ots.AttributeColumn, error) {
	if err := validatePrimaryKey(meta, pk, false); err != nil {
		return nil, err
	}
	current, exists, err := e.loadRowForUpdate(ctx, tx, meta, pk)
	if err != nil {
		return nil, err
	}
	versions, err := currentVersions(current)
	if err != nil {
		return nil, ots.ErrInternal("decode row: %v", err)
	}
	if err := cond.Check(exists, versions); err != nil {
		return nil, err
	}
	oldRaw := cloneAttrs(current)
	if current == nil {
		current = make(attrs)
	}
	nowMs := time.Now().UnixMilli()
	for _, cell := range cells {
		ts := cellTimestamp(cell, nowMs)
		switch cell.Op {
		case plainbuffer.OpPut:
			if cell.Value == nil {
				return nil, ots.ErrParameterInvalid("update cell %q has no value", cell.Name)
			}
			if err := current.put(cell.Name, cell.Value, ts, meta.Options.MaxVersions); err != nil {
				return nil, err
			}
		case plainbuffer.OpDeleteAllVersion:
			current.deleteAll(cell.Name)
		case plainbuffer.OpDeleteOneVersion:
			if !cell.HasTimestamp {
				return nil, ots.ErrParameterInvalid("delete one version of %q requires a timestamp", cell.Name)
			}
			current.deleteOne(cell.Name, ts)
		case plainbuffer.OpIncrement:
			if cell.Value == nil || cell.Value.Type != ots.ValueInteger {
				return nil, ots.ErrParameterInvalid("increment cell %q must have an INTEGER value", cell.Name)
			}
			if _, err := current.increment(cell.Name, cell.Value.Integer, ts, meta.Options.MaxVersions); err != nil {
				return nil, err
			}
		default:
			return nil, ots.ErrParameterInvalid("unknown cell operation %d", cell.Op)
		}
	}
	data, err := encodeAttrs(current)
	if err != nil {
		return nil, ots.ErrInternal("encode row: %v", err)
	}
	if exists {
		if err := e.updateRowAttrs(ctx, tx, meta, pk, data); err != nil {
			return nil, err
		}
	} else {
		if _, err := e.insertRow(ctx, tx, meta, pk, data, false); err != nil {
			return nil, err
		}
	}
	if err := e.syncIndexes(ctx, tx, meta, pk, oldRaw, current); err != nil {
		return nil, err
	}
	if err := e.syncSearchIndexes(ctx, tx, meta, pk, current); err != nil {
		return nil, err
	}
	if !returnAfterModify {
		return nil, nil
	}
	allVersions, err := current.toVersions()
	if err != nil {
		return nil, ots.ErrInternal("decode row: %v", err)
	}
	names := returnColumns
	if len(names) == 0 {
		for name := range allVersions {
			names = append(names, name)
		}
		sort.Strings(names)
	}
	var returned []ots.AttributeColumn
	for _, name := range names {
		if versions, ok := allVersions[name]; ok && len(versions) > 0 {
			returned = append(returned, versions[0])
		}
	}
	return returned, nil
}

func (e *Engine) UpdateRow(ctx context.Context, meta *TableMeta, pk ots.PrimaryKey, cells []UpdateCell, cond *otsfilter.Condition, returnAfterModify bool, returnColumns []string) ([]ots.AttributeColumn, error) {
	var returned []ots.AttributeColumn
	err := e.runTx(ctx, func(tx *pgTx) error {
		var err error
		returned, err = e.updateRowTx(ctx, tx, meta, pk, cells, cond, returnAfterModify, returnColumns)
		return err
	})
	return returned, err
}

func (e *Engine) deleteRowTx(ctx context.Context, tx *pgTx, meta *TableMeta, pk ots.PrimaryKey, cond *otsfilter.Condition) error {
	if err := validatePrimaryKey(meta, pk, false); err != nil {
		return err
	}
	current, exists, err := e.loadRowForUpdate(ctx, tx, meta, pk)
	if err != nil {
		return err
	}
	versions, err := currentVersions(current)
	if err != nil {
		return ots.ErrInternal("decode row: %v", err)
	}
	if err := cond.Check(exists, versions); err != nil {
		return err
	}
	if exists {
		query := "DELETE FROM " + quoteIdent(meta.PGName) + " WHERE " + pkWhereClause(meta)
		if _, err := tx.ExecContext(ctx, query, pkEqualArgs(meta, pk)...); err != nil {
			return execError(err, "delete row")
		}
		if err := e.syncIndexes(ctx, tx, meta, pk, current, nil); err != nil {
			return err
		}
		if err := e.syncSearchIndexes(ctx, tx, meta, pk, nil); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) DeleteRow(ctx context.Context, meta *TableMeta, pk ots.PrimaryKey, cond *otsfilter.Condition) error {
	return e.runTx(ctx, func(tx *pgTx) error {
		return e.deleteRowTx(ctx, tx, meta, pk, cond)
	})
}

type BatchWriteOp struct {
	Meta              *TableMeta
	Type              OperationType
	PrimaryKey        ots.PrimaryKey
	Cells             []UpdateCell
	Condition         *otsfilter.Condition
	ReturnPK          bool
	ReturnAfterModify bool
	ReturnColumns     []string
}

type BatchWriteResult struct {
	Err        error
	PrimaryKey ots.PrimaryKey
	Columns    []ots.AttributeColumn
}

func (e *Engine) applyBatchOp(ctx context.Context, tx *pgTx, op BatchWriteOp) BatchWriteResult {
	switch op.Type {
	case OperationPut:
		pk, err := e.putRowTx(ctx, tx, op.Meta, op.PrimaryKey, op.Cells, op.Condition, op.ReturnPK)
		return BatchWriteResult{Err: err, PrimaryKey: pk}
	case OperationUpdate:
		cols, err := e.updateRowTx(ctx, tx, op.Meta, op.PrimaryKey, op.Cells, op.Condition, op.ReturnAfterModify, op.ReturnColumns)
		return BatchWriteResult{Err: err, Columns: cols}
	case OperationDelete:
		err := e.deleteRowTx(ctx, tx, op.Meta, op.PrimaryKey, op.Condition)
		return BatchWriteResult{Err: err}
	default:
		return BatchWriteResult{Err: ots.ErrParameterInvalid("unknown batch operation type %d", op.Type)}
	}
}

func (e *Engine) BatchWriteRow(ctx context.Context, ops []BatchWriteOp, atomic bool) []BatchWriteResult {
	results := make([]BatchWriteResult, len(ops))
	if !atomic {
		for i, op := range ops {
			var result BatchWriteResult
			err := e.runTx(ctx, func(tx *pgTx) error {
				result = e.applyBatchOp(ctx, tx, op)
				return result.Err
			})
			if err != nil {
				result.Err = err
			}
			results[i] = result
		}
		return results
	}
	err := e.runTx(ctx, func(tx *pgTx) error {
		var firstErr error
		for i, op := range ops {
			result := e.applyBatchOp(ctx, tx, op)
			results[i] = result
			if result.Err != nil && firstErr == nil {
				firstErr = result.Err
			}
		}
		if firstErr != nil {
			for i := range results {
				results[i] = BatchWriteResult{Err: firstErr}
			}
			return firstErr
		}
		return nil
	})
	if err != nil {
		for i := range results {
			results[i] = BatchWriteResult{Err: err}
		}
	}
	return results
}
