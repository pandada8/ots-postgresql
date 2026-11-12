// Package otsfilter parses and evaluates OTS column filters and row conditions.
package otsfilter

import (
	"regexp"
	"strconv"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"

	"github.com/pandada8/ots-postgresql/internal/ots"
	"github.com/pandada8/ots-postgresql/internal/plainbuffer"
)

// Versions maps a column name to its versions, sorted by timestamp descending.
type Versions map[string][]ots.AttributeColumn

type RowFilter interface {
	Pass(cols Versions) bool
}

type Pagination struct {
	Offset int32
	Limit  int32
}

type singleColumnFilter struct {
	comparator      otsprotocol.ComparatorType
	columnName      string
	value           *ots.Value
	filterIfMissing bool
	latestOnly      bool
	transferRegex   *regexp.Regexp
	transferCast    otsprotocol.VariantType
	hasTransfer     bool
}

func compare(comparator otsprotocol.ComparatorType, left, right *ots.Value) bool {
	if left.Type != right.Type {
		return false
	}
	cmp, err := left.Compare(right)
	if err != nil {
		return false
	}
	switch comparator {
	case otsprotocol.ComparatorType_CT_EQUAL:
		return cmp == 0
	case otsprotocol.ComparatorType_CT_NOT_EQUAL:
		return cmp != 0
	case otsprotocol.ComparatorType_CT_GREATER_THAN:
		return cmp > 0
	case otsprotocol.ComparatorType_CT_GREATER_EQUAL:
		return cmp >= 0
	case otsprotocol.ComparatorType_CT_LESS_THAN:
		return cmp < 0
	case otsprotocol.ComparatorType_CT_LESS_EQUAL:
		return cmp <= 0
	}
	return false
}

func (f *singleColumnFilter) transfer(v *ots.Value) (*ots.Value, bool) {
	if !f.hasTransfer {
		return v, true
	}
	if v.Type != ots.ValueString {
		return nil, false
	}
	match := f.transferRegex.FindStringSubmatch(v.String)
	if match == nil {
		return nil, false
	}
	text := match[0]
	if len(match) > 1 {
		text = match[1]
	}
	switch f.transferCast {
	case otsprotocol.VariantType_VT_INTEGER:
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, false
		}
		return ots.IntegerValue(n), true
	case otsprotocol.VariantType_VT_DOUBLE:
		n, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, false
		}
		return ots.DoubleValue(n), true
	default:
		return ots.StringValue(text), true
	}
}

func (f *singleColumnFilter) Pass(cols Versions) bool {
	versions := cols[f.columnName]
	if len(versions) == 0 {
		return !f.filterIfMissing
	}
	if f.latestOnly {
		versions = versions[:1]
	}
	for _, version := range versions {
		value, ok := f.transfer(version.Value)
		if !ok {
			continue
		}
		if compare(f.comparator, value, f.value) {
			return true
		}
	}
	return false
}

type compositeFilter struct {
	combinator otsprotocol.LogicalOperator
	subs       []RowFilter
}

func (f *compositeFilter) Pass(cols Versions) bool {
	switch f.combinator {
	case otsprotocol.LogicalOperator_LO_AND:
		for _, sub := range f.subs {
			if !sub.Pass(cols) {
				return false
			}
		}
		return true
	case otsprotocol.LogicalOperator_LO_OR:
		for _, sub := range f.subs {
			if sub.Pass(cols) {
				return true
			}
		}
		return false
	case otsprotocol.LogicalOperator_LO_NOT:
		if len(f.subs) != 1 {
			return false
		}
		return !f.subs[0].Pass(cols)
	}
	return false
}

// ParseFilter parses a serialized otsprotocol.Filter. The returned pagination
// is non-nil when the filter is a ColumnPaginationFilter.
func ParseFilter(data []byte) (RowFilter, *Pagination, error) {
	if len(data) == 0 {
		return nil, nil, nil
	}
	pbFilter := new(otsprotocol.Filter)
	if err := proto.Unmarshal(data, pbFilter); err != nil {
		return nil, nil, ots.ErrParameterInvalid("invalid filter: %v", err)
	}
	switch pbFilter.GetType() {
	case otsprotocol.FilterType_FT_SINGLE_COLUMN_VALUE:
		pbSingle := new(otsprotocol.SingleColumnValueFilter)
		if err := proto.Unmarshal(pbFilter.GetFilter(), pbSingle); err != nil {
			return nil, nil, ots.ErrParameterInvalid("invalid single column filter: %v", err)
		}
		value, err := plainbuffer.DecodeValue(pbSingle.GetColumnValue())
		if err != nil {
			return nil, nil, ots.ErrParameterInvalid("invalid filter value: %v", err)
		}
		f := &singleColumnFilter{
			comparator:      pbSingle.GetComparator(),
			columnName:      pbSingle.GetColumnName(),
			value:           value,
			filterIfMissing: pbSingle.GetFilterIfMissing(),
			latestOnly:      pbSingle.GetLatestVersionOnly(),
		}
		if rule := pbSingle.GetValueTransRule(); rule != nil {
			re, err := regexp.Compile(rule.GetRegex())
			if err != nil {
				return nil, nil, ots.ErrParameterInvalid("invalid filter regex: %v", err)
			}
			f.transferRegex = re
			f.transferCast = rule.GetCastType()
			f.hasTransfer = true
		}
		return f, nil, nil
	case otsprotocol.FilterType_FT_COMPOSITE_COLUMN_VALUE:
		pbComposite := new(otsprotocol.CompositeColumnValueFilter)
		if err := proto.Unmarshal(pbFilter.GetFilter(), pbComposite); err != nil {
			return nil, nil, ots.ErrParameterInvalid("invalid composite filter: %v", err)
		}
		f := &compositeFilter{combinator: pbComposite.GetCombinator()}
		for _, sub := range pbComposite.GetSubFilters() {
			subData, err := proto.Marshal(sub)
			if err != nil {
				return nil, nil, ots.ErrInternal("marshal sub filter: %v", err)
			}
			subFilter, pagination, err := ParseFilter(subData)
			if err != nil {
				return nil, nil, err
			}
			if pagination != nil {
				return nil, nil, ots.ErrParameterInvalid("pagination filter is not allowed inside composite filter")
			}
			f.subs = append(f.subs, subFilter)
		}
		return f, nil, nil
	case otsprotocol.FilterType_FT_COLUMN_PAGINATION:
		pbPagination := new(otsprotocol.ColumnPaginationFilter)
		if err := proto.Unmarshal(pbFilter.GetFilter(), pbPagination); err != nil {
			return nil, nil, ots.ErrParameterInvalid("invalid pagination filter: %v", err)
		}
		return nil, &Pagination{Offset: pbPagination.GetOffset(), Limit: pbPagination.GetLimit()}, nil
	default:
		return nil, nil, ots.ErrParameterInvalid("unknown filter type %d", pbFilter.GetType())
	}
}

type RowExistence int

const (
	RowExistenceIgnore RowExistence = iota
	RowExistenceExpectExist
	RowExistenceExpectNotExist
)

type Condition struct {
	RowExistence RowExistence
	ColumnFilter RowFilter
}

func ParseCondition(pb *otsprotocol.Condition) (*Condition, error) {
	cond := &Condition{}
	if pb == nil {
		return cond, nil
	}
	switch pb.GetRowExistence() {
	case otsprotocol.RowExistenceExpectation_IGNORE:
		cond.RowExistence = RowExistenceIgnore
	case otsprotocol.RowExistenceExpectation_EXPECT_EXIST:
		cond.RowExistence = RowExistenceExpectExist
	case otsprotocol.RowExistenceExpectation_EXPECT_NOT_EXIST:
		cond.RowExistence = RowExistenceExpectNotExist
	default:
		return nil, ots.ErrParameterInvalid("unknown row existence expectation %d", pb.GetRowExistence())
	}
	if len(pb.GetColumnCondition()) > 0 {
		filter, pagination, err := ParseFilter(pb.GetColumnCondition())
		if err != nil {
			return nil, err
		}
		if pagination != nil {
			return nil, ots.ErrParameterInvalid("pagination filter cannot be used as a row condition")
		}
		cond.ColumnFilter = filter
	}
	return cond, nil
}

// Check verifies the condition against the current row state. Column
// conditions always evaluate against the latest version of each column.
func (c *Condition) Check(rowExists bool, cols Versions) error {
	if c == nil {
		return nil
	}
	switch c.RowExistence {
	case RowExistenceExpectExist:
		if !rowExists {
			return ots.ErrConditionCheckFail("row does not exist")
		}
	case RowExistenceExpectNotExist:
		if rowExists {
			return ots.ErrConditionCheckFail("row already exists")
		}
	}
	if c.ColumnFilter != nil {
		latest := make(Versions, len(cols))
		for name, versions := range cols {
			if len(versions) > 0 {
				latest[name] = versions[:1]
			}
		}
		if !c.ColumnFilter.Pass(latest) {
			return ots.ErrConditionCheckFail("column condition check failed")
		}
	}
	return nil
}
