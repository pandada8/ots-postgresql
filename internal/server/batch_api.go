package server

import (
	"context"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"

	"github.com/pandada8/ots-postgresql/internal/ots"
	"github.com/pandada8/ots-postgresql/internal/storage"
)

func (s *Server) batchGetRow(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.BatchGetRowRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid BatchGetRow request: %v", err)
	}
	resp := &otsprotocol.BatchGetRowResponse{}
	for _, table := range req.GetTables() {
		tableResp := &otsprotocol.TableInBatchGetRowResponse{TableName: proto.String(table.GetTableName())}
		meta, metaErr := s.engine.GetReadMeta(ctx, table.GetTableName())
		var criteria storage.ReadCriteria
		var criteriaErr error
		if metaErr == nil {
			criteria, criteriaErr = buildReadCriteria(meta, table.GetColumnsToGet(), table.GetMaxVersions(), table.GetTimeRange(), table.GetFilter(), table.StartColumn, table.EndColumn)
		}
		for _, pkBytes := range table.GetPrimaryKey() {
			tableResp.Rows = append(tableResp.Rows, s.batchGetOneRow(ctx, meta, metaErr, criteria, criteriaErr, pkBytes))
		}
		resp.Tables = append(resp.Tables, tableResp)
	}
	return resp, nil
}

func (s *Server) batchGetOneRow(ctx context.Context, meta *storage.TableMeta, metaErr error, criteria storage.ReadCriteria, criteriaErr error, pkBytes []byte) *otsprotocol.RowInBatchGetRowResponse {
	if metaErr != nil {
		return &otsprotocol.RowInBatchGetRowResponse{IsOk: proto.Bool(false), Error: pbError(metaErr), Consumed: consumedCapacity(0, 0)}
	}
	if criteriaErr != nil {
		return &otsprotocol.RowInBatchGetRowResponse{IsOk: proto.Bool(false), Error: pbError(criteriaErr), Consumed: consumedCapacity(0, 0)}
	}
	pk, err := decodePrimaryKey(pkBytes)
	if err != nil {
		return &otsprotocol.RowInBatchGetRowResponse{IsOk: proto.Bool(false), Error: pbError(err), Consumed: consumedCapacity(0, 0)}
	}
	row, err := s.engine.GetRow(ctx, meta, pk, criteria)
	if err != nil {
		return &otsprotocol.RowInBatchGetRowResponse{IsOk: proto.Bool(false), Error: pbError(err), Consumed: consumedCapacity(0, 0)}
	}
	result := &otsprotocol.RowInBatchGetRowResponse{IsOk: proto.Bool(true), Consumed: consumedCapacity(1, 0)}
	if row != nil {
		result.Row = encodeRow(row)
	}
	return result
}

type batchWriteEntry struct {
	op      storage.BatchWriteOp
	err     error
	content *otsprotocol.ReturnContent
}

func (s *Server) batchWriteRow(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.BatchWriteRowRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid BatchWriteRow request: %v", err)
	}
	atomic := req.GetIsAtomic()
	var entries []batchWriteEntry
	tableSizes := make([]int, 0, len(req.GetTables()))
	var firstPreErr error
	for _, table := range req.GetTables() {
		meta, metaErr := s.engine.GetTableMeta(ctx, table.GetTableName())
		for _, row := range table.GetRows() {
			entry := s.decodeBatchWriteRow(meta, metaErr, row)
			if entry.err != nil && firstPreErr == nil {
				firstPreErr = entry.err
			}
			entries = append(entries, entry)
		}
		tableSizes = append(tableSizes, len(table.GetRows()))
	}

	results := make([]storage.BatchWriteResult, len(entries))
	if atomic && firstPreErr != nil {
		for i := range results {
			results[i] = storage.BatchWriteResult{Err: firstPreErr}
		}
	} else {
		var validOps []storage.BatchWriteOp
		var validIdx []int
		for i, entry := range entries {
			if entry.err != nil {
				results[i] = storage.BatchWriteResult{Err: entry.err}
				continue
			}
			validOps = append(validOps, entry.op)
			validIdx = append(validIdx, i)
		}
		engineResults := s.engine.BatchWriteRow(ctx, validOps, atomic)
		for j, idx := range validIdx {
			results[idx] = engineResults[j]
		}
	}

	resp := &otsprotocol.BatchWriteRowResponse{}
	cursor := 0
	for ti, table := range req.GetTables() {
		tableResp := &otsprotocol.TableInBatchWriteRowResponse{TableName: proto.String(table.GetTableName())}
		for ri := 0; ri < tableSizes[ti]; ri++ {
			entry := entries[cursor]
			result := results[cursor]
			cursor++
			tableResp.Rows = append(tableResp.Rows, batchWriteRowResponse(entry, result))
		}
		resp.Tables = append(resp.Tables, tableResp)
	}
	return resp, nil
}

func (s *Server) decodeBatchWriteRow(meta *storage.TableMeta, metaErr error, row *otsprotocol.RowInBatchWriteRowRequest) batchWriteEntry {
	if metaErr != nil {
		return batchWriteEntry{err: metaErr}
	}
	pbRow, err := decodePlainRow(row.GetRowChange())
	if err != nil {
		return batchWriteEntry{err: err}
	}
	cond, err := parseCondition(row.GetCondition())
	if err != nil {
		return batchWriteEntry{err: err}
	}
	op := storage.BatchWriteOp{
		Meta:       meta,
		PrimaryKey: primaryKeyFromCells(pbRow.PrimaryKey),
		Cells:      updateCellsFromRow(pbRow),
		Condition:  cond,
	}
	content := row.GetReturnContent()
	switch row.GetType() {
	case otsprotocol.OperationType_PUT:
		op.Type = storage.OperationPut
		op.ReturnPK = content.GetReturnType() == otsprotocol.ReturnType_RT_PK
	case otsprotocol.OperationType_UPDATE:
		op.Type = storage.OperationUpdate
		op.ReturnAfterModify = content.GetReturnType() == otsprotocol.ReturnType_RT_AFTER_MODIFY
		op.ReturnColumns = content.GetReturnColumnNames()
	case otsprotocol.OperationType_DELETE:
		op.Type = storage.OperationDelete
	default:
		return batchWriteEntry{err: ots.ErrParameterInvalid("unknown batch operation type %d", row.GetType())}
	}
	return batchWriteEntry{op: op, content: content}
}

func batchWriteRowResponse(entry batchWriteEntry, result storage.BatchWriteResult) *otsprotocol.RowInBatchWriteRowResponse {
	if result.Err != nil {
		return &otsprotocol.RowInBatchWriteRowResponse{IsOk: proto.Bool(false), Error: pbError(result.Err), Consumed: consumedCapacity(0, 0)}
	}
	resp := &otsprotocol.RowInBatchWriteRowResponse{IsOk: proto.Bool(true), Consumed: consumedCapacity(0, 1)}
	switch entry.content.GetReturnType() {
	case otsprotocol.ReturnType_RT_PK:
		if result.PrimaryKey != nil {
			resp.Row = encodePrimaryKey(result.PrimaryKey)
		}
	case otsprotocol.ReturnType_RT_AFTER_MODIFY:
		resp.Row = encodeRow(&ots.Row{PrimaryKey: entry.op.PrimaryKey, Attributes: result.Columns})
	}
	return resp
}
