package server

import (
	"context"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"

	"github.com/pandada8/ots-postgresql/internal/ots"
)

func (s *Server) getRange(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.GetRangeRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid GetRange request: %v", err)
	}
	meta, err := s.engine.GetReadMeta(ctx, req.GetTableName())
	if err != nil {
		return nil, err
	}
	start, err := decodePrimaryKey(req.GetInclusiveStartPrimaryKey())
	if err != nil {
		return nil, err
	}
	end, err := decodePrimaryKey(req.GetExclusiveEndPrimaryKey())
	if err != nil {
		return nil, err
	}
	criteria, err := buildReadCriteria(meta, req.GetColumnsToGet(), req.GetMaxVersions(), req.GetTimeRange(), req.GetFilter(), req.StartColumn, req.EndColumn)
	if err != nil {
		return nil, err
	}
	rows, nextPK, err := s.engine.GetRange(ctx, meta, start, end, int(req.GetDirection()), req.GetLimit(), criteria)
	if err != nil {
		return nil, err
	}
	resp := &otsprotocol.GetRangeResponse{
		Consumed:      consumedCapacity(1, 0),
		Rows:          encodeRows(rows),
		DataBlockType: otsprotocol.DataBlockType_DBT_PLAIN_BUFFER.Enum(),
	}
	if nextPK != nil {
		resp.NextStartPrimaryKey = encodePrimaryKey(nextPK)
	}
	return resp, nil
}
