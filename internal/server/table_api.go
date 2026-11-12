package server

import (
	"context"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"

	"github.com/pandada8/ots-postgresql/internal/ots"
	"github.com/pandada8/ots-postgresql/internal/storage"
)

func (s *Server) createTable(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.CreateTableRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid CreateTable request: %v", err)
	}
	if req.GetStreamSpec().GetEnableStream() {
		return nil, ots.ErrUnsupported("stream is not supported by ots-postgresql")
	}
	if req.GetSseSpec().GetEnable() {
		return nil, ots.ErrUnsupported("server-side encryption is not supported by ots-postgresql")
	}
	if req.GetEnableLocalTxn() {
		return nil, ots.ErrUnsupported("local transaction is not supported by ots-postgresql")
	}
	pbMeta := req.GetTableMeta()
	if pbMeta == nil {
		return nil, ots.ErrParameterInvalid("table_meta is required")
	}
	meta := &storage.TableMeta{Name: pbMeta.GetTableName()}
	for _, pk := range pbMeta.GetPrimaryKey() {
		col := storage.PKColumn{
			Name:          pk.GetName(),
			Type:          primaryKeyTypeFromPB(pk.GetType()),
			AutoIncrement: pk.Option != nil && pk.GetOption() == otsprotocol.PrimaryKeyOption_AUTO_INCREMENT,
		}
		meta.PrimaryKey = append(meta.PrimaryKey, col)
	}
	for _, dc := range pbMeta.GetDefinedColumn() {
		meta.DefinedColumns = append(meta.DefinedColumns, storage.DefinedColumn{
			Name: dc.GetName(),
			Type: dc.GetType().String(),
		})
	}
	meta.Options = tableOptionsFromPB(req.GetTableOptions())
	for _, pbIdx := range req.GetIndexMetas() {
		meta.Indexes = append(meta.Indexes, indexMetaFromPB(pbIdx))
	}
	for _, pbIdx := range pbMeta.GetIndexMeta() {
		meta.Indexes = append(meta.Indexes, indexMetaFromPB(pbIdx))
	}
	if err := s.engine.CreateTable(ctx, meta); err != nil {
		return nil, err
	}
	return &otsprotocol.CreateTableResponse{}, nil
}

func tableOptionsFromPB(pb *otsprotocol.TableOptions) storage.TableOptions {
	opts := storage.TableOptions{
		TimeToLive:                pb.GetTimeToLive(),
		MaxVersions:               pb.GetMaxVersions(),
		DeviationCellVersionInSec: pb.GetDeviationCellVersionInSec(),
		AllowUpdate:               true,
		UpdateFullRow:             pb.GetUpdateFullRow(),
	}
	if pb != nil && pb.AllowUpdate != nil {
		opts.AllowUpdate = pb.GetAllowUpdate()
	}
	return opts
}

func tableOptionsToPB(opts storage.TableOptions) *otsprotocol.TableOptions {
	return &otsprotocol.TableOptions{
		TimeToLive:                proto.Int32(opts.TimeToLive),
		MaxVersions:               proto.Int32(opts.MaxVersions),
		DeviationCellVersionInSec: proto.Int64(opts.DeviationCellVersionInSec),
		AllowUpdate:               proto.Bool(opts.AllowUpdate),
		UpdateFullRow:             proto.Bool(opts.UpdateFullRow),
	}
}

func (s *Server) listTable(ctx context.Context, body []byte) (proto.Message, error) {
	names, err := s.engine.ListTables(ctx)
	if err != nil {
		return nil, err
	}
	return &otsprotocol.ListTableResponse{TableNames: names}, nil
}

func (s *Server) describeTable(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.DescribeTableRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid DescribeTable request: %v", err)
	}
	meta, err := s.engine.GetTableMeta(ctx, req.GetTableName())
	if err != nil {
		return nil, err
	}
	pbMeta := &otsprotocol.TableMeta{TableName: proto.String(meta.Name)}
	for _, col := range meta.PrimaryKey {
		schema := &otsprotocol.PrimaryKeySchema{
			Name: proto.String(col.Name),
			Type: primaryKeyTypeToPB(col.Type).Enum(),
		}
		if col.AutoIncrement {
			schema.Option = otsprotocol.PrimaryKeyOption_AUTO_INCREMENT.Enum()
		}
		pbMeta.PrimaryKey = append(pbMeta.PrimaryKey, schema)
	}
	for _, dc := range meta.DefinedColumns {
		dcType := otsprotocol.DefinedColumnType(otsprotocol.DefinedColumnType_value[dc.Type])
		pbMeta.DefinedColumn = append(pbMeta.DefinedColumn, &otsprotocol.DefinedColumnSchema{
			Name: proto.String(dc.Name),
			Type: dcType.Enum(),
		})
	}
	indexes, err := s.engine.ListIndexes(ctx, meta.Name)
	if err != nil {
		return nil, err
	}
	var pbIndexes []*otsprotocol.IndexMeta
	for _, idx := range indexes {
		pbIndexes = append(pbIndexes, indexMetaToPB(idx))
	}
	return &otsprotocol.DescribeTableResponse{
		TableMeta: pbMeta,
		ReservedThroughputDetails: &otsprotocol.ReservedThroughputDetails{
			CapacityUnit:     &otsprotocol.CapacityUnit{Read: proto.Int32(0), Write: proto.Int32(0)},
			LastIncreaseTime: proto.Int64(0),
		},
		TableOptions: tableOptionsToPB(meta.Options),
		TableStatus:  otsprotocol.TableStatus_ACTIVE.Enum(),
		CreationTime: proto.Int64(meta.CreatedAt),
		IndexMetas:   pbIndexes,
	}, nil
}

func (s *Server) deleteTable(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.DeleteTableRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid DeleteTable request: %v", err)
	}
	if err := s.engine.DeleteTable(ctx, req.GetTableName()); err != nil {
		return nil, err
	}
	return &otsprotocol.DeleteTableResponse{}, nil
}

func (s *Server) updateTable(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.UpdateTableRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid UpdateTable request: %v", err)
	}
	if req.GetStreamSpec().GetEnableStream() {
		return nil, ots.ErrUnsupported("stream is not supported by ots-postgresql")
	}
	var opts *storage.TableOptions
	if req.GetTableOptions() != nil {
		parsed := tableOptionsFromPB(req.GetTableOptions())
		opts = &parsed
	}
	meta, err := s.engine.UpdateTable(ctx, req.GetTableName(), opts)
	if err != nil {
		return nil, err
	}
	return &otsprotocol.UpdateTableResponse{
		ReservedThroughputDetails: &otsprotocol.ReservedThroughputDetails{
			CapacityUnit:     &otsprotocol.CapacityUnit{Read: proto.Int32(0), Write: proto.Int32(0)},
			LastIncreaseTime: proto.Int64(0),
		},
		TableOptions:  tableOptionsToPB(meta.Options),
		StreamDetails: &otsprotocol.StreamDetails{EnableStream: proto.Bool(false)},
	}, nil
}
