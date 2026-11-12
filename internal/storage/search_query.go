package storage

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pandada8/ots-postgresql/internal/ots"
)

const (
	SearchQueryAll      = "all"
	SearchQueryTerm     = "term"
	SearchQueryTerms    = "terms"
	SearchQueryRange    = "range"
	SearchQueryBool     = "bool"
	SearchQueryExists   = "exists"
	SearchQueryPrefix   = "prefix"
	SearchQueryWildcard = "wildcard"
	SearchQueryMatch    = "match"
)

type SearchQueryNode struct {
	Kind               string
	Field              string
	Value              *ots.Value
	Values             []*ots.Value
	From               *ots.Value
	To                 *ots.Value
	IncludeLower       bool
	IncludeUpper       bool
	Must               []*SearchQueryNode
	MustNot            []*SearchQueryNode
	Filter             []*SearchQueryNode
	Should             []*SearchQueryNode
	MinimumShouldMatch int
	Text               string
}

type SearchAggSpec struct {
	Name  string
	Kind  string
	Field string
}

type SearchGroupSorter struct {
	Kind       string
	Name       string
	Descending bool
}

type SearchGroupSpec struct {
	Name    string
	Field   string
	Size    int
	Sorters []SearchGroupSorter
	SubAggs []SearchAggSpec
}

type SearchRequest struct {
	Query         *SearchQueryNode
	Sort          []SearchSorter
	Offset        int
	Limit         int
	Token         []byte
	GetTotalCount bool
	Aggs          []SearchAggSpec
	Groups        []SearchGroupSpec
}

type SearchAggResult struct {
	Name     string
	Kind     string
	Value    float64
	Count    int64
	HasValue bool
}

type SearchGroupItem struct {
	Key      string
	RowCount int64
	SubAggs  []SearchAggResult
}

type SearchGroupResult struct {
	Name  string
	Items []SearchGroupItem
}

type SearchResult struct {
	TotalCount int64
	PKs        []ots.PrimaryKey
	NextToken  []byte
	Aggs       []SearchAggResult
	Groups     []SearchGroupResult
}

type searchToken struct {
	Offset      int            `json:"offset"`
	Fingerprint string         `json:"fingerprint"`
	Sort        []SearchSorter `json:"sort,omitempty"`
}

func searchCoerceValue(field SearchFieldSchema, value *ots.Value) (interface{}, error) {
	if value == nil {
		return nil, ots.ErrParameterInvalid("query on field %q has no value", field.Name)
	}
	switch field.Type {
	case SearchFieldLong:
		if value.Type == ots.ValueInteger {
			return value.Integer, nil
		}
	case SearchFieldDouble:
		switch value.Type {
		case ots.ValueInteger:
			return float64(value.Integer), nil
		case ots.ValueDouble:
			return value.Double, nil
		}
	case SearchFieldBoolean:
		if value.Type == ots.ValueBoolean {
			return value.Boolean, nil
		}
	case SearchFieldKeyword, SearchFieldText:
		if value.Type == ots.ValueString {
			return value.String, nil
		}
	case SearchFieldDate:
		switch value.Type {
		case ots.ValueInteger:
			return value.Integer, nil
		case ots.ValueString:
			millis, ok := parseSearchDate(value.String, field.DateFormats)
			if ok {
				return millis, nil
			}
		}
	}
	return nil, ots.ErrParameterInvalid("value of type %s does not match search field %q (%s)", value.Type, field.Name, field.Type)
}

func escapeLikePattern(text string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(text)
}

func wildcardToLikePattern(pattern string) string {
	var out strings.Builder
	for _, char := range pattern {
		switch char {
		case '*':
			out.WriteByte('%')
		case '?':
			out.WriteByte('_')
		default:
			out.WriteString(escapeLikePattern(string(char)))
		}
	}
	return out.String()
}

func buildSearchWhere(idx *SearchIndexMeta, node *SearchQueryNode) (string, []interface{}, error) {
	if node == nil || node.Kind == SearchQueryAll {
		return "1=1", nil, nil
	}
	switch node.Kind {
	case SearchQueryTerm:
		field, column, err := searchFieldColumnOf(idx, node.Field)
		if err != nil {
			return "", nil, err
		}
		arg, err := searchCoerceValue(field, node.Value)
		if err != nil {
			return "", nil, err
		}
		return column + " = ?", []interface{}{arg}, nil
	case SearchQueryTerms:
		field, column, err := searchFieldColumnOf(idx, node.Field)
		if err != nil {
			return "", nil, err
		}
		if len(node.Values) == 0 {
			return "1=0", nil, nil
		}
		placeholders := make([]string, 0, len(node.Values))
		args := make([]interface{}, 0, len(node.Values))
		for _, value := range node.Values {
			arg, err := searchCoerceValue(field, value)
			if err != nil {
				return "", nil, err
			}
			placeholders = append(placeholders, "?")
			args = append(args, arg)
		}
		return column + " IN (" + strings.Join(placeholders, ", ") + ")", args, nil
	case SearchQueryRange:
		field, column, err := searchFieldColumnOf(idx, node.Field)
		if err != nil {
			return "", nil, err
		}
		var parts []string
		var args []interface{}
		if node.From != nil {
			arg, err := searchCoerceValue(field, node.From)
			if err != nil {
				return "", nil, err
			}
			op := " > ?"
			if node.IncludeLower {
				op = " >= ?"
			}
			parts = append(parts, column+op)
			args = append(args, arg)
		}
		if node.To != nil {
			arg, err := searchCoerceValue(field, node.To)
			if err != nil {
				return "", nil, err
			}
			op := " < ?"
			if node.IncludeUpper {
				op = " <= ?"
			}
			parts = append(parts, column+op)
			args = append(args, arg)
		}
		if len(parts) == 0 {
			return "1=1", nil, nil
		}
		return strings.Join(parts, " AND "), args, nil
	case SearchQueryExists:
		_, column, err := searchFieldColumnOf(idx, node.Field)
		if err != nil {
			return "", nil, err
		}
		return column + " IS NOT NULL", nil, nil
	case SearchQueryPrefix:
		field, column, err := searchFieldColumnOf(idx, node.Field)
		if err != nil {
			return "", nil, err
		}
		if field.Type != SearchFieldKeyword && field.Type != SearchFieldText {
			return "", nil, ots.ErrParameterInvalid("prefix query requires a string field, got %q (%s)", node.Field, field.Type)
		}
		return column + " LIKE ?", []interface{}{escapeLikePattern(node.Text) + "%"}, nil
	case SearchQueryWildcard:
		field, column, err := searchFieldColumnOf(idx, node.Field)
		if err != nil {
			return "", nil, err
		}
		if field.Type != SearchFieldKeyword && field.Type != SearchFieldText {
			return "", nil, ots.ErrParameterInvalid("wildcard query requires a string field, got %q (%s)", node.Field, field.Type)
		}
		return column + " LIKE ?", []interface{}{wildcardToLikePattern(node.Text)}, nil
	case SearchQueryMatch:
		field, column, err := searchFieldColumnOf(idx, node.Field)
		if err != nil {
			return "", nil, err
		}
		if field.Type == SearchFieldText {
			return column + " LIKE ?", []interface{}{"%" + escapeLikePattern(node.Text) + "%"}, nil
		}
		arg, err := searchCoerceValue(field, ots.StringValue(node.Text))
		if err != nil {
			return "", nil, err
		}
		return column + " = ?", []interface{}{arg}, nil
	case SearchQueryBool:
		return buildBoolWhere(idx, node)
	}
	return "", nil, ots.ErrUnsupported("search query kind %q is not supported", node.Kind)
}

func searchFieldColumnOf(idx *SearchIndexMeta, name string) (SearchFieldSchema, string, error) {
	field, ok := idx.field(name)
	if !ok {
		return SearchFieldSchema{}, "", ots.ErrParameterInvalid("field %q is not part of search index %q", name, idx.Name)
	}
	return field, quoteIdent(name), nil
}

func buildBoolWhere(idx *SearchIndexMeta, node *SearchQueryNode) (string, []interface{}, error) {
	var parts []string
	var args []interface{}
	appendClauses := func(clauses []*SearchQueryNode, wrap func(string) string) error {
		for _, clause := range clauses {
			sub, subArgs, err := buildSearchWhere(idx, clause)
			if err != nil {
				return err
			}
			parts = append(parts, wrap(sub))
			args = append(args, subArgs...)
		}
		return nil
	}
	if err := appendClauses(node.Must, func(sub string) string { return "(" + sub + ")" }); err != nil {
		return "", nil, err
	}
	if err := appendClauses(node.Filter, func(sub string) string { return "(" + sub + ")" }); err != nil {
		return "", nil, err
	}
	if err := appendClauses(node.MustNot, func(sub string) string { return "COALESCE(NOT (" + sub + "), TRUE)" }); err != nil {
		return "", nil, err
	}
	if len(node.Should) > 0 {
		minimum := node.MinimumShouldMatch
		if minimum <= 0 && len(node.Must) == 0 && len(node.MustNot) == 0 && len(node.Filter) == 0 {
			minimum = 1
		}
		if minimum > 0 {
			counts := make([]string, 0, len(node.Should))
			for _, clause := range node.Should {
				sub, subArgs, err := buildSearchWhere(idx, clause)
				if err != nil {
					return "", nil, err
				}
				counts = append(counts, "CASE WHEN ("+sub+") THEN 1 ELSE 0 END")
				args = append(args, subArgs...)
			}
			parts = append(parts, "("+strings.Join(counts, " + ")+") >= "+strconv.Itoa(minimum))
		}
	}
	if len(parts) == 0 {
		return "1=1", nil, nil
	}
	return strings.Join(parts, " AND "), args, nil
}

func searchOrderBy(main *TableMeta, idx *SearchIndexMeta, sort []SearchSorter) (string, error) {
	effective := sort
	if len(effective) == 0 {
		effective = idx.IndexSort
	}
	var parts []string
	seenPK := make(map[string]bool)
	for _, sorter := range effective {
		direction := " ASC"
		if sorter.Descending {
			direction = " DESC"
		}
		if sorter.Field == "" {
			for i, col := range main.PrimaryKey {
				parts = append(parts, quoteIdent(searchPKColumn(i))+direction)
				seenPK[col.Name] = true
			}
			continue
		}
		if _, ok := idx.field(sorter.Field); !ok {
			return "", ots.ErrParameterInvalid("sort field %q is not part of search index %q", sorter.Field, idx.Name)
		}
		parts = append(parts, quoteIdent(sorter.Field)+direction)
	}
	for i, col := range main.PrimaryKey {
		if !seenPK[col.Name] {
			parts = append(parts, quoteIdent(searchPKColumn(i))+" ASC")
		}
	}
	return strings.Join(parts, ", "), nil
}

func searchFingerprint(indexName, where string, args []interface{}, orderBy string) string {
	hash := sha1.New()
	fmt.Fprintf(hash, "%s\x00%s\x00%v\x00%s", indexName, where, args, orderBy)
	return hex.EncodeToString(hash.Sum(nil))
}

func encodeSearchToken(token searchToken) ([]byte, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return nil, ots.ErrInternal("encode search token: %v", err)
	}
	return []byte(base64.StdEncoding.EncodeToString(data)), nil
}

func decodeSearchToken(raw []byte) (searchToken, error) {
	var token searchToken
	data, err := base64.StdEncoding.DecodeString(string(raw))
	if err != nil {
		return token, ots.ErrParameterInvalid("invalid search token")
	}
	if err := json.Unmarshal(data, &token); err != nil {
		return token, ots.ErrParameterInvalid("invalid search token")
	}
	if token.Offset < 0 {
		return token, ots.ErrParameterInvalid("invalid search token offset")
	}
	return token, nil
}

func searchAggExpression(idx *SearchIndexMeta, spec SearchAggSpec) (string, bool, error) {
	field, ok := idx.field(spec.Field)
	if !ok {
		return "", false, ots.ErrParameterInvalid("aggregation field %q is not part of search index %q", spec.Field, idx.Name)
	}
	column := quoteIdent(spec.Field)
	numeric := field.Type == SearchFieldLong || field.Type == SearchFieldDouble || field.Type == SearchFieldDate
	switch spec.Kind {
	case "sum":
		if !numeric {
			return "", false, ots.ErrParameterInvalid("sum aggregation requires a numeric field, got %q (%s)", spec.Field, field.Type)
		}
		return "COALESCE(SUM(" + column + "), 0)", false, nil
	case "count":
		return "COUNT(" + column + ")", true, nil
	case "distinct_count":
		return "COUNT(DISTINCT " + column + ")", true, nil
	case "avg":
		if !numeric {
			return "", false, ots.ErrParameterInvalid("avg aggregation requires a numeric field, got %q (%s)", spec.Field, field.Type)
		}
		return "AVG(" + column + ")", false, nil
	case "min":
		if !numeric {
			return "", false, ots.ErrParameterInvalid("min aggregation requires a numeric field, got %q (%s)", spec.Field, field.Type)
		}
		return "MIN(" + column + ")", false, nil
	case "max":
		if !numeric {
			return "", false, ots.ErrParameterInvalid("max aggregation requires a numeric field, got %q (%s)", spec.Field, field.Type)
		}
		return "MAX(" + column + ")", false, nil
	}
	return "", false, ots.ErrUnsupported("aggregation kind %q is not supported", spec.Kind)
}

func scanSearchAggs(specs []SearchAggSpec, scan func(dest ...interface{}) error) ([]SearchAggResult, error) {
	targets := make([]interface{}, 0, len(specs))
	floatTargets := make([]sql.NullFloat64, len(specs))
	intTargets := make([]sql.NullInt64, len(specs))
	countFlags := make([]bool, len(specs))
	for i, spec := range specs {
		switch spec.Kind {
		case "count", "distinct_count":
			targets = append(targets, &intTargets[i])
			countFlags[i] = true
		default:
			targets = append(targets, &floatTargets[i])
		}
	}
	if err := scan(targets...); err != nil {
		return nil, ots.ErrInternal("scan aggregations: %v", err)
	}
	results := make([]SearchAggResult, 0, len(specs))
	for i, spec := range specs {
		result := SearchAggResult{Name: spec.Name, Kind: spec.Kind}
		if countFlags[i] {
			result.Count = intTargets[i].Int64
			result.HasValue = true
		} else {
			result.Value = floatTargets[i].Float64
			result.HasValue = floatTargets[i].Valid
		}
		results = append(results, result)
	}
	return results, nil
}

func (e *Engine) searchAggregations(ctx context.Context, idx *SearchIndexMeta, where string, args []interface{}, specs []SearchAggSpec) ([]SearchAggResult, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	exprs := make([]string, 0, len(specs))
	for _, spec := range specs {
		expr, _, err := searchAggExpression(idx, spec)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}
	query := "SELECT " + strings.Join(exprs, ", ") + " FROM " + quoteIdent(idx.PGName) + " WHERE " + where
	row := e.db.QueryRowContext(ctx, query, args...)
	return scanSearchAggs(specs, row.Scan)
}

func (e *Engine) searchGroupByField(ctx context.Context, idx *SearchIndexMeta, where string, args []interface{}, spec SearchGroupSpec) (*SearchGroupResult, error) {
	field, ok := idx.field(spec.Field)
	if !ok {
		return nil, ots.ErrParameterInvalid("group by field %q is not part of search index %q", spec.Field, idx.Name)
	}
	size := spec.Size
	if size <= 0 {
		size = 10
	}
	if size > 2000 {
		size = 2000
	}
	column := quoteIdent(spec.Field)
	exprs := []string{column + " AS gk", "COUNT(*) AS rc"}
	subAggAliases := make(map[string]string)
	for i, sub := range spec.SubAggs {
		expr, _, err := searchAggExpression(idx, sub)
		if err != nil {
			return nil, err
		}
		alias := fmt.Sprintf("sa_%d", i)
		exprs = append(exprs, expr+" AS "+alias)
		subAggAliases[sub.Name] = alias
	}
	orderParts := make([]string, 0, len(spec.Sorters)+1)
	for _, sorter := range spec.Sorters {
		direction := " ASC"
		if sorter.Descending {
			direction = " DESC"
		}
		switch sorter.Kind {
		case "key":
			orderParts = append(orderParts, "gk"+direction)
		case "row_count":
			orderParts = append(orderParts, "rc"+direction)
		case "sub_agg":
			alias, ok := subAggAliases[sorter.Name]
			if !ok {
				return nil, ots.ErrParameterInvalid("group by sort references unknown sub aggregation %q", sorter.Name)
			}
			orderParts = append(orderParts, alias+direction)
		default:
			return nil, ots.ErrUnsupported("group by sort kind %q is not supported", sorter.Kind)
		}
	}
	if len(orderParts) == 0 {
		orderParts = append(orderParts, "rc DESC")
	}
	orderParts = append(orderParts, "gk ASC")
	query := "SELECT " + strings.Join(exprs, ", ") + " FROM " + quoteIdent(idx.PGName) +
		" WHERE (" + where + ") AND " + column + " IS NOT NULL" +
		" GROUP BY " + column +
		" ORDER BY " + strings.Join(orderParts, ", ") +
		" LIMIT " + strconv.Itoa(size)
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, ots.ErrInternal("group by query: %v", err)
	}
	defer rows.Close()
	result := &SearchGroupResult{Name: spec.Name}
	for rows.Next() {
		targets := make([]interface{}, 0, 2+len(spec.SubAggs))
		var keyTarget interface{}
		switch field.Type {
		case SearchFieldLong, SearchFieldDate:
			keyTarget = new(sql.NullInt64)
		case SearchFieldBoolean:
			keyTarget = new(sql.NullBool)
		case SearchFieldDouble:
			keyTarget = new(sql.NullFloat64)
		default:
			keyTarget = new(sql.NullString)
		}
		var rowCount int64
		targets = append(targets, keyTarget, &rowCount)
		floatTargets := make([]sql.NullFloat64, len(spec.SubAggs))
		intTargets := make([]sql.NullInt64, len(spec.SubAggs))
		for i, sub := range spec.SubAggs {
			if sub.Kind == "count" || sub.Kind == "distinct_count" {
				targets = append(targets, &intTargets[i])
			} else {
				targets = append(targets, &floatTargets[i])
			}
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, ots.ErrInternal("scan group by row: %v", err)
		}
		key, err := searchGroupKeyFromTarget(field, keyTarget)
		if err != nil {
			return nil, err
		}
		item := SearchGroupItem{Key: key, RowCount: rowCount}
		for i, sub := range spec.SubAggs {
			agg := SearchAggResult{Name: sub.Name, Kind: sub.Kind}
			if sub.Kind == "count" || sub.Kind == "distinct_count" {
				agg.Count = intTargets[i].Int64
				agg.HasValue = true
			} else {
				agg.Value = floatTargets[i].Float64
				agg.HasValue = floatTargets[i].Valid
			}
			item.SubAggs = append(item.SubAggs, agg)
		}
		result.Items = append(result.Items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, ots.ErrInternal("group by query: %v", err)
	}
	return result, nil
}

func searchGroupKeyFromTarget(field SearchFieldSchema, target interface{}) (string, error) {
	switch typed := target.(type) {
	case *sql.NullInt64:
		return strconv.FormatInt(typed.Int64, 10), nil
	case *sql.NullBool:
		return strconv.FormatBool(typed.Bool), nil
	case *sql.NullFloat64:
		return strconv.FormatFloat(typed.Float64, 'f', -1, 64), nil
	case *sql.NullString:
		return typed.String, nil
	}
	return "", ots.ErrInternal("unexpected group by key target")
}

func (e *Engine) scanSearchPKs(main *TableMeta, rows *sql.Rows) (ots.PrimaryKey, error) {
	dest := make([]interface{}, 0, len(main.PrimaryKey))
	intTargets := make([]int64, len(main.PrimaryKey))
	blobTargets := make([][]byte, len(main.PrimaryKey))
	for i, col := range main.PrimaryKey {
		if col.Type == ots.PrimaryKeyInteger {
			dest = append(dest, &intTargets[i])
		} else {
			dest = append(dest, &blobTargets[i])
		}
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, ots.ErrInternal("scan search row: %v", err)
	}
	pk := make(ots.PrimaryKey, 0, len(main.PrimaryKey))
	for i, col := range main.PrimaryKey {
		var value *ots.Value
		switch col.Type {
		case ots.PrimaryKeyInteger:
			value = ots.IntegerValue(intTargets[i])
		case ots.PrimaryKeyString:
			value = ots.StringValue(string(blobTargets[i]))
		default:
			value = ots.BinaryValue(blobTargets[i])
		}
		pk = append(pk, ots.PrimaryKeyColumn{Name: col.Name, Type: col.Type, Value: value})
	}
	return pk, nil
}

func (e *Engine) Search(ctx context.Context, main *TableMeta, idx *SearchIndexMeta, req *SearchRequest) (*SearchResult, error) {
	where, args, err := buildSearchWhere(idx, req.Query)
	if err != nil {
		return nil, err
	}
	offset := req.Offset
	if offset < 0 {
		return nil, ots.ErrParameterInvalid("search offset must not be negative")
	}
	sort := req.Sort
	if len(sort) == 0 {
		sort = idx.IndexSort
	}
	var token *searchToken
	if len(req.Token) > 0 {
		decoded, err := decodeSearchToken(req.Token)
		if err != nil {
			return nil, err
		}
		token = &decoded
		sort = token.Sort
		offset = token.Offset
	}
	orderBy, err := searchOrderBy(main, idx, sort)
	if err != nil {
		return nil, err
	}
	fingerprint := searchFingerprint(idx.Name, where, args, orderBy)
	if token != nil && token.Fingerprint != fingerprint {
		return nil, ots.ErrParameterInvalid("search token does not match the current query")
	}
	limit := req.Limit
	if limit < 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	result := new(SearchResult)
	countQuery := "SELECT COUNT(*) FROM " + quoteIdent(idx.PGName) + " WHERE " + where
	if err := e.db.QueryRowContext(ctx, countQuery, args...).Scan(&result.TotalCount); err != nil {
		return nil, ots.ErrInternal("search count: %v", err)
	}
	if limit > 0 {
		cols := make([]string, 0, len(main.PrimaryKey))
		for i := range main.PrimaryKey {
			cols = append(cols, quoteIdent(searchPKColumn(i)))
		}
		query := "SELECT " + strings.Join(cols, ", ") + " FROM " + quoteIdent(idx.PGName) +
			" WHERE " + where + " ORDER BY " + orderBy + " LIMIT ? OFFSET ?"
		queryArgs := append(append([]interface{}{}, args...), limit+1, offset)
		rows, err := e.db.QueryContext(ctx, query, queryArgs...)
		if err != nil {
			return nil, ots.ErrInternal("search query: %v", err)
		}
		for rows.Next() {
			pk, err := e.scanSearchPKs(main, rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			result.PKs = append(result.PKs, pk)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, ots.ErrInternal("search query: %v", err)
		}
		rows.Close()
		if len(result.PKs) > limit {
			result.PKs = result.PKs[:limit]
			result.NextToken, err = encodeSearchToken(searchToken{Offset: offset + limit, Fingerprint: fingerprint, Sort: sort})
			if err != nil {
				return nil, err
			}
		}
	}
	result.Aggs, err = e.searchAggregations(ctx, idx, where, args, req.Aggs)
	if err != nil {
		return nil, err
	}
	for _, spec := range req.Groups {
		group, err := e.searchGroupByField(ctx, idx, where, args, spec)
		if err != nil {
			return nil, err
		}
		result.Groups = append(result.Groups, *group)
	}
	return result, nil
}
