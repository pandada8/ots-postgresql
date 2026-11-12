package server

import (
	"math"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"

	"github.com/pandada8/ots-postgresql/internal/ots"
	"github.com/pandada8/ots-postgresql/internal/otsfilter"
	"github.com/pandada8/ots-postgresql/internal/plainbuffer"
	"github.com/pandada8/ots-postgresql/internal/storage"
)

func decodePlainRow(data []byte) (plainbuffer.Row, error) {
	row, err := plainbuffer.DecodeRow(data)
	if err != nil {
		return plainbuffer.Row{}, ots.ErrParameterInvalid("invalid plain buffer row: %v", err)
	}
	return row, nil
}

func primaryKeyFromCells(cells []plainbuffer.Cell) ots.PrimaryKey {
	pk := make(ots.PrimaryKey, len(cells))
	for i, cell := range cells {
		col := ots.PrimaryKeyColumn{Name: cell.Name, Special: cell.Special}
		if cell.Value != nil {
			col.Value = cell.Value
			switch cell.Value.Type {
			case ots.ValueInteger:
				col.Type = ots.PrimaryKeyInteger
			case ots.ValueString:
				col.Type = ots.PrimaryKeyString
			case ots.ValueBinary:
				col.Type = ots.PrimaryKeyBinary
			}
		}
		pk[i] = col
	}
	return pk
}

func decodePrimaryKey(data []byte) (ots.PrimaryKey, error) {
	row, err := decodePlainRow(data)
	if err != nil {
		return nil, err
	}
	return primaryKeyFromCells(row.PrimaryKey), nil
}

func updateCellsFromRow(row plainbuffer.Row) []storage.UpdateCell {
	cells := make([]storage.UpdateCell, len(row.Cells))
	for i, cell := range row.Cells {
		cells[i] = storage.UpdateCell{
			Name:         cell.Name,
			Value:        cell.Value,
			Timestamp:    cell.Timestamp,
			HasTimestamp: cell.HasTimestamp,
			Op:           cell.Op,
		}
	}
	return cells
}

func primaryKeyToCells(pk ots.PrimaryKey) []plainbuffer.Cell {
	cells := make([]plainbuffer.Cell, len(pk))
	for i, col := range pk {
		cells[i] = plainbuffer.Cell{Name: col.Name, Value: col.Value, Special: col.Special}
	}
	return cells
}

func encodePrimaryKey(pk ots.PrimaryKey) []byte {
	return plainbuffer.EncodePrimaryKey(primaryKeyToCells(pk), false)
}

func encodeRow(row *ots.Row) []byte {
	pkCells := primaryKeyToCells(row.PrimaryKey)
	cells := make([]plainbuffer.Cell, len(row.Attributes))
	for i, attr := range row.Attributes {
		cells[i] = plainbuffer.Cell{
			Name:         attr.Name,
			Value:        attr.Value,
			Timestamp:    attr.Timestamp,
			HasTimestamp: true,
		}
	}
	return plainbuffer.EncodeRow(plainbuffer.Row{PrimaryKey: pkCells, Cells: cells})
}

func encodeRows(rows []*ots.Row) []byte {
	if len(rows) == 0 {
		return []byte{}
	}
	pbRows := make([]plainbuffer.Row, len(rows))
	for i, row := range rows {
		pkCells := primaryKeyToCells(row.PrimaryKey)
		cells := make([]plainbuffer.Cell, len(row.Attributes))
		for j, attr := range row.Attributes {
			cells[j] = plainbuffer.Cell{
				Name:         attr.Name,
				Value:        attr.Value,
				Timestamp:    attr.Timestamp,
				HasTimestamp: true,
			}
		}
		pbRows[i] = plainbuffer.Row{PrimaryKey: pkCells, Cells: cells}
	}
	return plainbuffer.EncodeRows(pbRows)
}

func buildTimeRange(pb *otsprotocol.TimeRange) *storage.TimeRange {
	if pb == nil {
		return nil
	}
	if pb.SpecificTime != nil {
		return &storage.TimeRange{Specific: pb.GetSpecificTime(), HasSpecific: true}
	}
	tr := &storage.TimeRange{Start: pb.GetStartTime(), End: math.MaxInt64}
	if pb.EndTime != nil && pb.GetEndTime() > 0 {
		tr.End = pb.GetEndTime()
	}
	return tr
}

func buildReadCriteria(meta *storage.TableMeta, columnsToGet []string, maxVersions int32, pbTimeRange *otsprotocol.TimeRange, filter []byte, startColumn, endColumn *string) (storage.ReadCriteria, error) {
	criteria := storage.ReadCriteria{
		ColumnsToGet: columnsToGet,
		MaxVersions:  maxVersions,
		TimeRange:    buildTimeRange(pbTimeRange),
		StartColumn:  startColumn,
		EndColumn:    endColumn,
	}
	if criteria.MaxVersions <= 0 {
		criteria.MaxVersions = meta.Options.MaxVersions
	}
	if len(filter) > 0 {
		rowFilter, pagination, err := otsfilter.ParseFilter(filter)
		if err != nil {
			return criteria, err
		}
		criteria.Filter = rowFilter
		criteria.Pagination = pagination
	}
	return criteria, nil
}

func parseCondition(pb *otsprotocol.Condition) (*otsfilter.Condition, error) {
	return otsfilter.ParseCondition(pb)
}

func consumedCapacity(read, write int32) *otsprotocol.ConsumedCapacity {
	return &otsprotocol.ConsumedCapacity{
		CapacityUnit: &otsprotocol.CapacityUnit{
			Read:  proto.Int32(read),
			Write: proto.Int32(write),
		},
	}
}

func pbError(err error) *otsprotocol.Error {
	otsErr := ots.AsError(err)
	return &otsprotocol.Error{
		Code:    proto.String(otsErr.Code),
		Message: proto.String(otsErr.Message),
	}
}

func primaryKeyTypeToPB(t ots.PrimaryKeyType) otsprotocol.PrimaryKeyType {
	switch t {
	case ots.PrimaryKeyInteger:
		return otsprotocol.PrimaryKeyType_INTEGER
	case ots.PrimaryKeyString:
		return otsprotocol.PrimaryKeyType_STRING
	case ots.PrimaryKeyBinary:
		return otsprotocol.PrimaryKeyType_BINARY
	}
	return otsprotocol.PrimaryKeyType_INTEGER
}

func primaryKeyTypeFromPB(t otsprotocol.PrimaryKeyType) ots.PrimaryKeyType {
	switch t {
	case otsprotocol.PrimaryKeyType_INTEGER:
		return ots.PrimaryKeyInteger
	case otsprotocol.PrimaryKeyType_STRING:
		return ots.PrimaryKeyString
	case otsprotocol.PrimaryKeyType_BINARY:
		return ots.PrimaryKeyBinary
	}
	return 0
}
