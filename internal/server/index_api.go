package server

import (
	"context"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"

	"github.com/pandada8/ots-postgresql/internal/ots"
	"github.com/pandada8/ots-postgresql/internal/storage"
)

func (s *Server) createIndex(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.CreateIndexRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid CreateIndex request: %v", err)
	}
	if req.GetIndexMeta() == nil {
		return nil, ots.ErrParameterInvalid("index_meta is required")
	}
	idx := indexMetaFromPB(req.GetIndexMeta())
	if err := s.engine.CreateIndex(ctx, req.GetMainTableName(), idx, req.GetIncludeBaseData()); err != nil {
		return nil, err
	}
	return &otsprotocol.CreateIndexResponse{}, nil
}

func (s *Server) deleteIndex(ctx context.Context, body []byte) (proto.Message, error) {
	req := new(otsprotocol.DropIndexRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		return nil, ots.ErrParameterInvalid("invalid DeleteIndex request: %v", err)
	}
	if err := s.engine.DeleteIndex(ctx, req.GetMainTableName(), req.GetIndexName()); err != nil {
		return nil, err
	}
	return &otsprotocol.DropIndexResponse{}, nil
}

func indexMetaFromPB(pb *otsprotocol.IndexMeta) *storage.IndexMeta {
	idx := &storage.IndexMeta{
		Name:           pb.GetName(),
		PrimaryKey:     pb.GetPrimaryKey(),
		DefinedColumns: pb.GetDefinedColumn(),
		IndexType:      storage.IndexTypeGlobal,
	}
	if pb.GetIndexType() == otsprotocol.IndexType_IT_LOCAL_INDEX {
		idx.IndexType = storage.IndexTypeLocal
	}
	return idx
}

func indexMetaToPB(idx *storage.IndexMeta) *otsprotocol.IndexMeta {
	pb := &otsprotocol.IndexMeta{
		Name:           proto.String(idx.Name),
		PrimaryKey:     idx.PrimaryKey,
		DefinedColumn:  idx.DefinedColumns,
		IndexSyncPhase: otsprotocol.IndexSyncPhase_ISP_INCR.Enum(),
	}
	if idx.IndexType == storage.IndexTypeLocal {
		pb.IndexType = otsprotocol.IndexType_IT_LOCAL_INDEX.Enum()
		pb.IndexUpdateMode = otsprotocol.IndexUpdateMode_IUM_SYNC_INDEX.Enum()
	} else {
		pb.IndexType = otsprotocol.IndexType_IT_GLOBAL_INDEX.Enum()
		pb.IndexUpdateMode = otsprotocol.IndexUpdateMode_IUM_ASYNC_INDEX.Enum()
	}
	return pb
}
