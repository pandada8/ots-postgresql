package storage

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/pandada8/ots-postgresql/internal/ots"
)

var errConcurrentInsert = errors.New("concurrent insert detected")

func rebind(query string) string {
	var out strings.Builder
	out.Grow(len(query) + 8)
	param := 0
	inLiteral := false
	for pos := 0; pos < len(query); pos++ {
		char := query[pos]
		if inLiteral {
			out.WriteByte(char)
			if char == '\'' {
				if pos+1 < len(query) && query[pos+1] == '\'' {
					out.WriteByte('\'')
					pos++
					continue
				}
				inLiteral = false
			}
			continue
		}
		switch char {
		case '\'':
			inLiteral = true
			out.WriteByte(char)
		case '?':
			param++
			out.WriteByte('$')
			out.WriteString(strconv.Itoa(param))
		default:
			out.WriteByte(char)
		}
	}
	return out.String()
}

type pgDB struct {
	db *sql.DB
}

func (d pgDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return d.db.ExecContext(ctx, rebind(query), args...)
}

func (d pgDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, rebind(query), args...)
}

func (d pgDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return d.db.QueryRowContext(ctx, rebind(query), args...)
}

func (d pgDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*pgTx, error) {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &pgTx{tx: tx}, nil
}

func (d pgDB) Close() error {
	return d.db.Close()
}

type pgTx struct {
	tx *sql.Tx
}

func (t *pgTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.tx.ExecContext(ctx, rebind(query), args...)
}

func (t *pgTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, rebind(query), args...)
}

func (t *pgTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRowContext(ctx, rebind(query), args...)
}

func (t *pgTx) Rollback() error {
	return t.tx.Rollback()
}

func (t *pgTx) Commit() error {
	return t.tx.Commit()
}

func pgErrorCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func isUniqueViolation(err error) bool {
	return pgErrorCode(err) == "23505"
}

func isDuplicateTable(err error) bool {
	return pgErrorCode(err) == "42P07"
}

func execError(err error, action string) error {
	switch pgErrorCode(err) {
	case "40P01", "55P03", "40001":
		return err
	}
	return ots.ErrInternal("%s: %v", action, err)
}
