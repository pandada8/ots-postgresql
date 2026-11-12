package server

import (
	"context"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"

	"github.com/pandada8/ots-postgresql/internal/ots"
)

func (s *Server) putRow(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.PutRowRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid PutRow request: %v", err)
	}
	meta, err := s.engine.GetTableMeta(ctx, req.GetTableName())
	if err != nil {
		return nil, err
	}
	row, err := decodePlainRow(req.GetRow())
	if err != nil {
		return nil, err
	}
	pk := primaryKeyFromCells(row.PrimaryKey)
	cells := updateCellsFromRow(row)
	cond, err := parseCondition(req.GetCondition())
	if err != nil {
		return nil, err
	}
	returnPK := req.GetReturnContent().GetReturnType() == otsprotocol.ReturnType_RT_PK
	resultPK, err := s.engine.PutRow(ctx, meta, pk, cells, cond, returnPK)
	if err != nil {
		return nil, err
	}
	resp := &otsprotocol.PutRowResponse{Consumed: consumedCapacity(0, 1)}
	if returnPK && resultPK != nil {
		resp.Row = encodePrimaryKey(resultPK)
	}
	return resp, nil
}

func (s *Server) getRow(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.GetRowRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid GetRow request: %v", err)
	}
	meta, err := s.engine.GetReadMeta(ctx, req.GetTableName())
	if err != nil {
		return nil, err
	}
	pk, err := decodePrimaryKey(req.GetPrimaryKey())
	if err != nil {
		return nil, err
	}
	criteria, err := buildReadCriteria(meta, req.GetColumnsToGet(), req.GetMaxVersions(), req.GetTimeRange(), req.GetFilter(), req.StartColumn, req.EndColumn)
	if err != nil {
		return nil, err
	}
	row, err := s.engine.GetRow(ctx, meta, pk, criteria)
	if err != nil {
		return nil, err
	}
	resp := &otsprotocol.GetRowResponse{Consumed: consumedCapacity(1, 0), Row: []byte{}}
	if row != nil {
		resp.Row = encodeRow(row)
	}
	return resp, nil
}

func (s *Server) updateRow(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.UpdateRowRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid UpdateRow request: %v", err)
	}
	meta, err := s.engine.GetTableMeta(ctx, req.GetTableName())
	if err != nil {
		return nil, err
	}
	row, err := decodePlainRow(req.GetRowChange())
	if err != nil {
		return nil, err
	}
	pk := primaryKeyFromCells(row.PrimaryKey)
	cells := updateCellsFromRow(row)
	cond, err := parseCondition(req.GetCondition())
	if err != nil {
		return nil, err
	}
	returnAfterModify := req.GetReturnContent().GetReturnType() == otsprotocol.ReturnType_RT_AFTER_MODIFY
	returned, err := s.engine.UpdateRow(ctx, meta, pk, cells, cond, returnAfterModify, req.GetReturnContent().GetReturnColumnNames())
	if err != nil {
		return nil, err
	}
	resp := &otsprotocol.UpdateRowResponse{Consumed: consumedCapacity(0, 1)}
	if returnAfterModify {
		resp.Row = encodeRow(&ots.Row{PrimaryKey: pk, Attributes: returned})
	}
	return resp, nil
}

func (s *Server) deleteRow(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.DeleteRowRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid DeleteRow request: %v", err)
	}
	meta, err := s.engine.GetTableMeta(ctx, req.GetTableName())
	if err != nil {
		return nil, err
	}
	pk, err := decodePrimaryKey(req.GetPrimaryKey())
	if err != nil {
		return nil, err
	}
	cond, err := parseCondition(req.GetCondition())
	if err != nil {
		return nil, err
	}
	if err := s.engine.DeleteRow(ctx, meta, pk, cond); err != nil {
		return nil, err
	}
	return &otsprotocol.DeleteRowResponse{Consumed: consumedCapacity(0, 1)}, nil
}
