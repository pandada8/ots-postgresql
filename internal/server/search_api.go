package server

import (
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"

	"github.com/pandada8/ots-postgresql/internal/ots"
	"github.com/pandada8/ots-postgresql/internal/storage"
)

func (s *Server) createSearchIndex(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.CreateSearchIndexRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid CreateSearchIndex request: %v", err)
	}
	idx, err := searchIndexMetaFromPB(req.GetIndexName(), req.GetSchema())
	if err != nil {
		return nil, err
	}
	if err := s.engine.CreateSearchIndex(ctx, req.GetTableName(), idx); err != nil {
		return nil, err
	}
	return &otsprotocol.CreateSearchIndexResponse{}, nil
}

func (s *Server) listSearchIndex(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.ListSearchIndexRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid ListSearchIndex request: %v", err)
	}
	indexes, err := s.engine.ListSearchIndexes(ctx, req.GetTableName())
	if err != nil {
		return nil, err
	}
	resp := new(otsprotocol.ListSearchIndexResponse)
	for _, idx := range indexes {
		resp.Indices = append(resp.Indices, &otsprotocol.IndexInfo{
			TableName: proto.String(idx.MainTable),
			IndexName: proto.String(idx.Name),
		})
	}
	return resp, nil
}

func (s *Server) describeSearchIndex(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.DescribeSearchIndexRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid DescribeSearchIndex request: %v", err)
	}
	idx, err := s.engine.GetSearchIndex(ctx, req.GetTableName(), req.GetIndexName())
	if err != nil {
		return nil, err
	}
	return &otsprotocol.DescribeSearchIndexResponse{
		Schema: searchIndexSchemaToPB(idx),
		SyncStat: &otsprotocol.SyncStat{
			SyncPhase:            otsprotocol.SyncPhase_INCR.Enum(),
			CurrentSyncTimestamp: proto.Int64(time.Now().UnixNano()),
		},
	}, nil
}

func (s *Server) deleteSearchIndex(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.DeleteSearchIndexRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid DeleteSearchIndex request: %v", err)
	}
	if err := s.engine.DeleteSearchIndex(ctx, req.GetTableName(), req.GetIndexName()); err != nil {
		return nil, err
	}
	return &otsprotocol.DeleteSearchIndexResponse{}, nil
}

func (s *Server) search(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.SearchRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid Search request: %v", err)
	}
	main, err := s.engine.GetTableMeta(ctx, req.GetTableName())
	if err != nil {
		return nil, err
	}
	idx, err := s.engine.GetSearchIndex(ctx, req.GetTableName(), req.GetIndexName())
	if err != nil {
		return nil, err
	}
	queryPB := new(otsprotocol.SearchQuery)
	if err := proto.Unmarshal(req.GetSearchQuery(), queryPB); err != nil {
		return nil, ots.ErrParameterInvalid("invalid search query: %v", err)
	}
	node, err := searchQueryFromPB(queryPB.GetQuery())
	if err != nil {
		return nil, err
	}
	aggs, err := searchAggsFromPB(queryPB.GetAggs())
	if err != nil {
		return nil, err
	}
	groups, err := searchGroupsFromPB(queryPB.GetGroupBys())
	if err != nil {
		return nil, err
	}
	limit := -1
	if queryPB.Limit != nil {
		limit = int(queryPB.GetLimit())
	}
	result, err := s.engine.Search(ctx, main, idx, &storage.SearchRequest{
		Query:         node,
		Sort:          searchSortFromPB(queryPB.GetSort()),
		Offset:        int(queryPB.GetOffset()),
		Limit:         limit,
		Token:         queryPB.GetToken(),
		GetTotalCount: queryPB.GetGetTotalCount(),
		Aggs:          aggs,
		Groups:        groups,
	})
	if err != nil {
		return nil, err
	}
	rows, err := s.searchRows(ctx, main, idx, req.GetColumnsToGet(), result.PKs)
	if err != nil {
		return nil, err
	}
	resp := &otsprotocol.SearchResponse{
		TotalHits:      proto.Int64(result.TotalCount),
		Rows:           rows,
		IsAllSucceeded: proto.Bool(true),
		NextToken:      result.NextToken,
	}
	if len(result.Aggs) > 0 {
		aggsPB, err := searchAggResultsToPB(result.Aggs)
		if err != nil {
			return nil, err
		}
		data, err := proto.Marshal(aggsPB)
		if err != nil {
			return nil, ots.ErrInternal("marshal aggregation results: %v", err)
		}
		resp.Aggs = data
	}
	if len(result.Groups) > 0 {
		groupsPB, err := searchGroupResultsToPB(result.Groups)
		if err != nil {
			return nil, err
		}
		data, err := proto.Marshal(groupsPB)
		if err != nil {
			return nil, ots.ErrInternal("marshal group by results: %v", err)
		}
		resp.GroupBys = data
	}
	return resp, nil
}

func (s *Server) searchRows(ctx context.Context, main *storage.TableMeta, idx *storage.SearchIndexMeta, columnsPB *otsprotocol.ColumnsToGet, pks []ots.PrimaryKey) ([][]byte, error) {
	returnType := columnsPB.GetReturnType()
	var columns []string
	switch returnType {
	case otsprotocol.ColumnReturnType_RETURN_ALL:
	case otsprotocol.ColumnReturnType_RETURN_ALL_FROM_INDEX:
		for _, field := range idx.Fields {
			columns = append(columns, field.Name)
		}
	case otsprotocol.ColumnReturnType_RETURN_SPECIFIED:
		columns = columnsPB.GetColumnNames()
	}
	rows := make([][]byte, 0, len(pks))
	for _, pk := range pks {
		if returnType == otsprotocol.ColumnReturnType_RETURN_NONE || returnType == 0 {
			rows = append(rows, encodeRow(&ots.Row{PrimaryKey: pk}))
			continue
		}
		row, err := s.engine.GetRow(ctx, main, pk, storage.ReadCriteria{ColumnsToGet: columns, MaxVersions: 1})
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		rows = append(rows, encodeRow(row))
	}
	return rows, nil
}

func searchFieldTypeFromPB(t otsprotocol.FieldType) (string, error) {
	switch t {
	case otsprotocol.FieldType_LONG:
		return storage.SearchFieldLong, nil
	case otsprotocol.FieldType_DOUBLE:
		return storage.SearchFieldDouble, nil
	case otsprotocol.FieldType_BOOLEAN:
		return storage.SearchFieldBoolean, nil
	case otsprotocol.FieldType_KEYWORD:
		return storage.SearchFieldKeyword, nil
	case otsprotocol.FieldType_TEXT:
		return storage.SearchFieldText, nil
	case otsprotocol.FieldType_DATE:
		return storage.SearchFieldDate, nil
	}
	return "", ots.ErrUnsupported("search index field type %s is not supported", t)
}

func searchFieldTypeToPB(t string) otsprotocol.FieldType {
	switch t {
	case storage.SearchFieldLong:
		return otsprotocol.FieldType_LONG
	case storage.SearchFieldDouble:
		return otsprotocol.FieldType_DOUBLE
	case storage.SearchFieldBoolean:
		return otsprotocol.FieldType_BOOLEAN
	case storage.SearchFieldKeyword:
		return otsprotocol.FieldType_KEYWORD
	case storage.SearchFieldText:
		return otsprotocol.FieldType_TEXT
	default:
		return otsprotocol.FieldType_DATE
	}
}

func searchIndexMetaFromPB(name string, schema *otsprotocol.IndexSchema) (*storage.SearchIndexMeta, error) {
	if schema == nil {
		return nil, ots.ErrParameterInvalid("search index schema is required")
	}
	idx := &storage.SearchIndexMeta{Name: name}
	for _, fieldPB := range schema.GetFieldSchemas() {
		fieldType, err := searchFieldTypeFromPB(fieldPB.GetFieldType())
		if err != nil {
			return nil, err
		}
		idx.Fields = append(idx.Fields, storage.SearchFieldSchema{
			Name:        fieldPB.GetFieldName(),
			Type:        fieldType,
			Index:       fieldPB.GetIndex(),
			SortAndAgg:  fieldPB.GetSortAndAgg(),
			DateFormats: fieldPB.GetDateFormats(),
		})
	}
	idx.IndexSort = searchSortFromPB(schema.GetIndexSort())
	return idx, nil
}

func searchIndexSchemaToPB(idx *storage.SearchIndexMeta) *otsprotocol.IndexSchema {
	schema := &otsprotocol.IndexSchema{
		IndexSetting: &otsprotocol.IndexSetting{},
		IndexSort:    searchSortToPB(idx.IndexSort),
	}
	for _, field := range idx.Fields {
		schema.FieldSchemas = append(schema.FieldSchemas, &otsprotocol.FieldSchema{
			FieldName:   proto.String(field.Name),
			FieldType:   searchFieldTypeToPB(field.Type).Enum(),
			Index:       proto.Bool(field.Index),
			SortAndAgg:  proto.Bool(field.SortAndAgg),
			DateFormats: field.DateFormats,
		})
	}
	return schema
}

func searchSortFromPB(pb *otsprotocol.Sort) []storage.SearchSorter {
	if pb == nil {
		return nil
	}
	var sorters []storage.SearchSorter
	for _, sorter := range pb.GetSorter() {
		if fieldSort := sorter.GetFieldSort(); fieldSort != nil {
			sorters = append(sorters, storage.SearchSorter{
				Field:      fieldSort.GetFieldName(),
				Descending: fieldSort.GetOrder() == otsprotocol.SortOrder_SORT_ORDER_DESC,
			})
		} else if pkSort := sorter.GetPkSort(); pkSort != nil {
			sorters = append(sorters, storage.SearchSorter{
				Descending: pkSort.GetOrder() == otsprotocol.SortOrder_SORT_ORDER_DESC,
			})
		}
	}
	return sorters
}

func searchSortToPB(sorters []storage.SearchSorter) *otsprotocol.Sort {
	if len(sorters) == 0 {
		return nil
	}
	pb := new(otsprotocol.Sort)
	for _, sorter := range sorters {
		order := otsprotocol.SortOrder_SORT_ORDER_ASC
		if sorter.Descending {
			order = otsprotocol.SortOrder_SORT_ORDER_DESC
		}
		if sorter.Field == "" {
			pb.Sorter = append(pb.Sorter, &otsprotocol.Sorter{
				PkSort: &otsprotocol.PrimaryKeySort{Order: order.Enum()},
			})
		} else {
			pb.Sorter = append(pb.Sorter, &otsprotocol.Sorter{
				FieldSort: &otsprotocol.FieldSort{
					FieldName: proto.String(sorter.Field),
					Order:     order.Enum(),
				},
			})
		}
	}
	return pb
}

func searchVariantValue(raw []byte) (*ots.Value, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	switch raw[0] {
	case 0x0:
		if len(raw) < 9 {
			return nil, ots.ErrParameterInvalid("invalid integer variant value")
		}
		return ots.IntegerValue(int64(binary.LittleEndian.Uint64(raw[1:9]))), nil
	case 0x1:
		if len(raw) < 9 {
			return nil, ots.ErrParameterInvalid("invalid double variant value")
		}
		return ots.DoubleValue(math.Float64frombits(binary.LittleEndian.Uint64(raw[1:9]))), nil
	case 0x2:
		if len(raw) < 2 {
			return nil, ots.ErrParameterInvalid("invalid boolean variant value")
		}
		return ots.BooleanValue(raw[1] != 0), nil
	case 0x3:
		if len(raw) < 5 {
			return nil, ots.ErrParameterInvalid("invalid string variant value")
		}
		length := binary.LittleEndian.Uint32(raw[1:5])
		if uint32(len(raw)-5) < length {
			return nil, ots.ErrParameterInvalid("invalid string variant value length")
		}
		return ots.StringValue(string(raw[5 : 5+length])), nil
	}
	return nil, ots.ErrParameterInvalid("unknown variant value type %d", raw[0])
}

func searchQueryFromPB(pb *otsprotocol.Query) (*storage.SearchQueryNode, error) {
	if pb == nil {
		return nil, nil
	}
	switch pb.GetType() {
	case otsprotocol.QueryType_MATCH_ALL_QUERY:
		return &storage.SearchQueryNode{Kind: storage.SearchQueryAll}, nil
	case otsprotocol.QueryType_TERM_QUERY:
		term := new(otsprotocol.TermQuery)
		if err := proto.Unmarshal(pb.GetQuery(), term); err != nil {
			return nil, ots.ErrParameterInvalid("invalid term query: %v", err)
		}
		value, err := searchVariantValue(term.GetTerm())
		if err != nil {
			return nil, err
		}
		return &storage.SearchQueryNode{Kind: storage.SearchQueryTerm, Field: term.GetFieldName(), Value: value}, nil
	case otsprotocol.QueryType_TERMS_QUERY:
		terms := new(otsprotocol.TermsQuery)
		if err := proto.Unmarshal(pb.GetQuery(), terms); err != nil {
			return nil, ots.ErrParameterInvalid("invalid terms query: %v", err)
		}
		node := &storage.SearchQueryNode{Kind: storage.SearchQueryTerms, Field: terms.GetFieldName()}
		for _, raw := range terms.GetTerms() {
			value, err := searchVariantValue(raw)
			if err != nil {
				return nil, err
			}
			node.Values = append(node.Values, value)
		}
		return node, nil
	case otsprotocol.QueryType_RANGE_QUERY:
		rangeQuery := new(otsprotocol.RangeQuery)
		if err := proto.Unmarshal(pb.GetQuery(), rangeQuery); err != nil {
			return nil, ots.ErrParameterInvalid("invalid range query: %v", err)
		}
		from, err := searchVariantValue(rangeQuery.GetRangeFrom())
		if err != nil {
			return nil, err
		}
		to, err := searchVariantValue(rangeQuery.GetRangeTo())
		if err != nil {
			return nil, err
		}
		return &storage.SearchQueryNode{
			Kind:         storage.SearchQueryRange,
			Field:        rangeQuery.GetFieldName(),
			From:         from,
			To:           to,
			IncludeLower: rangeQuery.GetIncludeLower(),
			IncludeUpper: rangeQuery.GetIncludeUpper(),
		}, nil
	case otsprotocol.QueryType_BOOL_QUERY:
		boolQuery := new(otsprotocol.BoolQuery)
		if err := proto.Unmarshal(pb.GetQuery(), boolQuery); err != nil {
			return nil, ots.ErrParameterInvalid("invalid bool query: %v", err)
		}
		node := &storage.SearchQueryNode{Kind: storage.SearchQueryBool}
		for _, clause := range []struct {
			queries []*otsprotocol.Query
			target  *[]*storage.SearchQueryNode
		}{
			{boolQuery.GetMustQueries(), &node.Must},
			{boolQuery.GetMustNotQueries(), &node.MustNot},
			{boolQuery.GetFilterQueries(), &node.Filter},
			{boolQuery.GetShouldQueries(), &node.Should},
		} {
			for _, queryPB := range clause.queries {
				child, err := searchQueryFromPB(queryPB)
				if err != nil {
					return nil, err
				}
				*clause.target = append(*clause.target, child)
			}
		}
		node.MinimumShouldMatch = int(boolQuery.GetMinimumShouldMatch())
		if text := boolQuery.GetNewMinimumShouldMatch(); text != "" {
			minimum, err := strconv.Atoi(text)
			if err != nil {
				return nil, ots.ErrParameterInvalid("invalid minimum_should_match %q", text)
			}
			node.MinimumShouldMatch = minimum
		}
		return node, nil
	case otsprotocol.QueryType_EXISTS_QUERY:
		exists := new(otsprotocol.ExistsQuery)
		if err := proto.Unmarshal(pb.GetQuery(), exists); err != nil {
			return nil, ots.ErrParameterInvalid("invalid exists query: %v", err)
		}
		return &storage.SearchQueryNode{Kind: storage.SearchQueryExists, Field: exists.GetFieldName()}, nil
	case otsprotocol.QueryType_PREFIX_QUERY:
		prefix := new(otsprotocol.PrefixQuery)
		if err := proto.Unmarshal(pb.GetQuery(), prefix); err != nil {
			return nil, ots.ErrParameterInvalid("invalid prefix query: %v", err)
		}
		return &storage.SearchQueryNode{Kind: storage.SearchQueryPrefix, Field: prefix.GetFieldName(), Text: prefix.GetPrefix()}, nil
	case otsprotocol.QueryType_WILDCARD_QUERY:
		wildcard := new(otsprotocol.WildcardQuery)
		if err := proto.Unmarshal(pb.GetQuery(), wildcard); err != nil {
			return nil, ots.ErrParameterInvalid("invalid wildcard query: %v", err)
		}
		return &storage.SearchQueryNode{Kind: storage.SearchQueryWildcard, Field: wildcard.GetFieldName(), Text: wildcard.GetValue()}, nil
	case otsprotocol.QueryType_MATCH_QUERY:
		match := new(otsprotocol.MatchQuery)
		if err := proto.Unmarshal(pb.GetQuery(), match); err != nil {
			return nil, ots.ErrParameterInvalid("invalid match query: %v", err)
		}
		return &storage.SearchQueryNode{Kind: storage.SearchQueryMatch, Field: match.GetFieldName(), Text: match.GetText()}, nil
	case otsprotocol.QueryType_CONST_SCORE_QUERY:
		constScore := new(otsprotocol.ConstScoreQuery)
		if err := proto.Unmarshal(pb.GetQuery(), constScore); err != nil {
			return nil, ots.ErrParameterInvalid("invalid const score query: %v", err)
		}
		return searchQueryFromPB(constScore.GetFilter())
	}
	return nil, ots.ErrUnsupported("search query type %s is not supported", pb.GetType())
}

func searchAggKindFromPB(t otsprotocol.AggregationType) (string, error) {
	switch t {
	case otsprotocol.AggregationType_AGG_SUM:
		return "sum", nil
	case otsprotocol.AggregationType_AGG_COUNT:
		return "count", nil
	case otsprotocol.AggregationType_AGG_AVG:
		return "avg", nil
	case otsprotocol.AggregationType_AGG_MIN:
		return "min", nil
	case otsprotocol.AggregationType_AGG_MAX:
		return "max", nil
	case otsprotocol.AggregationType_AGG_DISTINCT_COUNT:
		return "distinct_count", nil
	}
	return "", ots.ErrUnsupported("aggregation type %s is not supported", t)
}

func searchAggKindToPB(kind string) otsprotocol.AggregationType {
	switch kind {
	case "sum":
		return otsprotocol.AggregationType_AGG_SUM
	case "count":
		return otsprotocol.AggregationType_AGG_COUNT
	case "avg":
		return otsprotocol.AggregationType_AGG_AVG
	case "min":
		return otsprotocol.AggregationType_AGG_MIN
	case "max":
		return otsprotocol.AggregationType_AGG_MAX
	default:
		return otsprotocol.AggregationType_AGG_DISTINCT_COUNT
	}
}

func searchAggFieldName(kind string, body []byte) (string, error) {
	switch kind {
	case "sum":
		agg := new(otsprotocol.SumAggregation)
		if err := proto.Unmarshal(body, agg); err != nil {
			return "", ots.ErrParameterInvalid("invalid sum aggregation: %v", err)
		}
		return agg.GetFieldName(), nil
	case "count":
		agg := new(otsprotocol.CountAggregation)
		if err := proto.Unmarshal(body, agg); err != nil {
			return "", ots.ErrParameterInvalid("invalid count aggregation: %v", err)
		}
		return agg.GetFieldName(), nil
	case "avg":
		agg := new(otsprotocol.AvgAggregation)
		if err := proto.Unmarshal(body, agg); err != nil {
			return "", ots.ErrParameterInvalid("invalid avg aggregation: %v", err)
		}
		return agg.GetFieldName(), nil
	case "min":
		agg := new(otsprotocol.MinAggregation)
		if err := proto.Unmarshal(body, agg); err != nil {
			return "", ots.ErrParameterInvalid("invalid min aggregation: %v", err)
		}
		return agg.GetFieldName(), nil
	case "max":
		agg := new(otsprotocol.MaxAggregation)
		if err := proto.Unmarshal(body, agg); err != nil {
			return "", ots.ErrParameterInvalid("invalid max aggregation: %v", err)
		}
		return agg.GetFieldName(), nil
	default:
		agg := new(otsprotocol.DistinctCountAggregation)
		if err := proto.Unmarshal(body, agg); err != nil {
			return "", ots.ErrParameterInvalid("invalid distinct count aggregation: %v", err)
		}
		return agg.GetFieldName(), nil
	}
}

func searchAggsFromPB(pb *otsprotocol.Aggregations) ([]storage.SearchAggSpec, error) {
	if pb == nil {
		return nil, nil
	}
	var specs []storage.SearchAggSpec
	for _, agg := range pb.GetAggs() {
		kind, err := searchAggKindFromPB(agg.GetType())
		if err != nil {
			return nil, err
		}
		field, err := searchAggFieldName(kind, agg.GetBody())
		if err != nil {
			return nil, err
		}
		specs = append(specs, storage.SearchAggSpec{Name: agg.GetName(), Kind: kind, Field: field})
	}
	return specs, nil
}

func searchGroupsFromPB(pb *otsprotocol.GroupBys) ([]storage.SearchGroupSpec, error) {
	if pb == nil {
		return nil, nil
	}
	var specs []storage.SearchGroupSpec
	for _, group := range pb.GetGroupBys() {
		if group.GetType() != otsprotocol.GroupByType_GROUP_BY_FIELD {
			return nil, ots.ErrUnsupported("group by type %s is not supported", group.GetType())
		}
		body := new(otsprotocol.GroupByField)
		if err := proto.Unmarshal(group.GetBody(), body); err != nil {
			return nil, ots.ErrParameterInvalid("invalid group by field: %v", err)
		}
		subAggs, err := searchAggsFromPB(body.GetSubAggs())
		if err != nil {
			return nil, err
		}
		spec := storage.SearchGroupSpec{
			Name:    group.GetName(),
			Field:   body.GetFieldName(),
			Size:    int(body.GetSize()),
			SubAggs: subAggs,
		}
		for _, sorter := range body.GetSort().GetSorters() {
			if keySort := sorter.GetGroupKeySort(); keySort != nil {
				spec.Sorters = append(spec.Sorters, storage.SearchGroupSorter{
					Kind:       "key",
					Descending: keySort.GetOrder() == otsprotocol.SortOrder_SORT_ORDER_DESC,
				})
			} else if rowCountSort := sorter.GetRowCountSort(); rowCountSort != nil {
				spec.Sorters = append(spec.Sorters, storage.SearchGroupSorter{
					Kind:       "row_count",
					Descending: rowCountSort.GetOrder() == otsprotocol.SortOrder_SORT_ORDER_DESC,
				})
			} else if subAggSort := sorter.GetSubAggSort(); subAggSort != nil {
				spec.Sorters = append(spec.Sorters, storage.SearchGroupSorter{
					Kind:       "sub_agg",
					Name:       subAggSort.GetSubAggName(),
					Descending: subAggSort.GetOrder() == otsprotocol.SortOrder_SORT_ORDER_DESC,
				})
			}
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

func searchAggResultBodyPB(result storage.SearchAggResult) (proto.Message, error) {
	switch result.Kind {
	case "sum":
		return &otsprotocol.SumAggregationResult{Value: proto.Float64(result.Value)}, nil
	case "count":
		return &otsprotocol.CountAggregationResult{Value: proto.Int64(result.Count)}, nil
	case "distinct_count":
		return &otsprotocol.DistinctCountAggregationResult{Value: proto.Int64(result.Count)}, nil
	case "avg":
		return &otsprotocol.AvgAggregationResult{Value: proto.Float64(searchAggFloat(result))}, nil
	case "min":
		return &otsprotocol.MinAggregationResult{Value: proto.Float64(searchAggFloat(result))}, nil
	case "max":
		return &otsprotocol.MaxAggregationResult{Value: proto.Float64(searchAggFloat(result))}, nil
	}
	return nil, ots.ErrUnsupported("aggregation kind %q is not supported", result.Kind)
}

func searchAggFloat(result storage.SearchAggResult) float64 {
	if !result.HasValue {
		return math.Inf(1)
	}
	return result.Value
}

func searchAggResultsToPB(results []storage.SearchAggResult) (*otsprotocol.AggregationsResult, error) {
	pb := new(otsprotocol.AggregationsResult)
	for _, result := range results {
		body, err := searchAggResultBodyPB(result)
		if err != nil {
			return nil, err
		}
		data, err := proto.Marshal(body)
		if err != nil {
			return nil, ots.ErrInternal("marshal aggregation result: %v", err)
		}
		pb.AggResults = append(pb.AggResults, &otsprotocol.AggregationResult{
			Name:      proto.String(result.Name),
			Type:      searchAggKindToPB(result.Kind).Enum(),
			AggResult: data,
		})
	}
	return pb, nil
}

func searchGroupResultsToPB(groups []storage.SearchGroupResult) (*otsprotocol.GroupBysResult, error) {
	pb := new(otsprotocol.GroupBysResult)
	for _, group := range groups {
		fieldResult := new(otsprotocol.GroupByFieldResult)
		for _, item := range group.Items {
			itemPB := &otsprotocol.GroupByFieldResultItem{
				Key:      proto.String(item.Key),
				RowCount: proto.Int64(item.RowCount),
			}
			if len(item.SubAggs) > 0 {
				subAggs, err := searchAggResultsToPB(item.SubAggs)
				if err != nil {
					return nil, err
				}
				itemPB.SubAggsResult = subAggs
			}
			fieldResult.GroupByFieldResultItems = append(fieldResult.GroupByFieldResultItems, itemPB)
		}
		data, err := proto.Marshal(fieldResult)
		if err != nil {
			return nil, ots.ErrInternal("marshal group by result: %v", err)
		}
		pb.GroupByResults = append(pb.GroupByResults, &otsprotocol.GroupByResult{
			Name:          proto.String(group.Name),
			Type:          otsprotocol.GroupByType_GROUP_BY_FIELD.Enum(),
			GroupByResult: data,
		})
	}
	return pb, nil
}
