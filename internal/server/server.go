// Package server exposes the OTS HTTP + protobuf protocol.
package server

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"

	"github.com/pandada8/ots-postgresql/internal/ots"
	"github.com/pandada8/ots-postgresql/internal/storage"
)

const maxRequestBody = 32 << 20

type Config struct {
	Engine          *storage.Engine
	AccessKeyID     string
	AccessKeySecret string
	InstanceName    string
	Logger          *slog.Logger
}

type Server struct {
	engine *storage.Engine
	auth   *authConfig
	logger *slog.Logger
}

type authConfig struct {
	accessKeyID     string
	accessKeySecret string
	instanceName    string
}

func New(cfg Config) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{engine: cfg.Engine, logger: logger}
	if cfg.AccessKeyID != "" || cfg.AccessKeySecret != "" || cfg.InstanceName != "" {
		s.auth = &authConfig{
			accessKeyID:     cfg.AccessKeyID,
			accessKeySecret: cfg.AccessKeySecret,
			instanceName:    cfg.InstanceName,
		}
	}
	return s
}

type apiHandler func(ctx context.Context, body []byte) (proto.Message, error)

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	routes := map[string]apiHandler{
		"/CreateTable":         s.createTable,
		"/ListTable":           s.listTable,
		"/DescribeTable":       s.describeTable,
		"/DeleteTable":         s.deleteTable,
		"/UpdateTable":         s.updateTable,
		"/PutRow":              s.putRow,
		"/GetRow":              s.getRow,
		"/UpdateRow":           s.updateRow,
		"/DeleteRow":           s.deleteRow,
		"/BatchGetRow":         s.batchGetRow,
		"/BatchWriteRow":       s.batchWriteRow,
		"/GetRange":            s.getRange,
		"/CreateIndex":         s.createIndex,
		"/DropIndex":           s.deleteIndex,
		"/CreateSearchIndex":   s.createSearchIndex,
		"/ListSearchIndex":     s.listSearchIndex,
		"/DescribeSearchIndex": s.describeSearchIndex,
		"/DeleteSearchIndex":   s.deleteSearchIndex,
		"/Search":              s.search,
	}
	for path, handler := range routes {
		mux.HandleFunc("POST "+path, s.wrap(path, handler))
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		requestID := newRequestID()
		s.writeError(w, requestID, ots.ErrUnsupported("api %s %s is not supported by ots-postgresql", r.Method, r.URL.Path))
	})
	return mux
}

func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}

func (s *Server) wrap(path string, handler apiHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		requestID := newRequestID()
		body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
		if err != nil {
			s.writeError(w, requestID, ots.ErrParameterInvalid("read request body: %v", err))
			return
		}
		if len(body) > maxRequestBody {
			s.writeError(w, requestID, ots.ErrParameterInvalid("request body too large"))
			return
		}
		if err := s.authenticate(r, path, body); err != nil {
			s.writeError(w, requestID, ots.AsError(err))
			s.logRequest(r, path, requestID, ots.AsError(err).HTTPStatus, start)
			return
		}
		resp, err := handler(r.Context(), body)
		if err != nil {
			otsErr := ots.AsError(err)
			s.writeError(w, requestID, otsErr)
			s.logRequest(r, path, requestID, otsErr.HTTPStatus, start)
			return
		}
		data, err := proto.Marshal(resp)
		if err != nil {
			s.writeError(w, requestID, ots.ErrInternal("marshal response: %v", err))
			s.logRequest(r, path, requestID, 500, start)
			return
		}
		s.writeResponse(w, requestID, data)
		s.logRequest(r, path, requestID, http.StatusOK, start)
	}
}

func (s *Server) logRequest(r *http.Request, path, requestID string, status int, start time.Time) {
	s.logger.Info("ots request",
		"api", strings.TrimPrefix(path, "/"),
		"status", status,
		"duration_ms", time.Since(start).Milliseconds(),
		"request_id", requestID,
		"remote", r.RemoteAddr,
	)
}

func (s *Server) writeResponse(w http.ResponseWriter, requestID string, data []byte) {
	sum := md5.Sum(data)
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("x-ots-requestid", requestID)
	w.Header().Set("x-ots-contentmd5", base64.StdEncoding.EncodeToString(sum[:]))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (s *Server) writeError(w http.ResponseWriter, requestID string, err *ots.Error) {
	data, marshalErr := proto.Marshal(&otsprotocol.Error{
		Code:    proto.String(err.Code),
		Message: proto.String(err.Message),
	})
	if marshalErr != nil {
		data = []byte(err.Message)
	}
	sum := md5.Sum(data)
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("x-ots-requestid", requestID)
	w.Header().Set("x-ots-contentmd5", base64.StdEncoding.EncodeToString(sum[:]))
	w.WriteHeader(err.HTTPStatus)
	w.Write(data)
}

func (s *Server) authenticate(r *http.Request, path string, body []byte) error {
	if sum := r.Header.Get("x-ots-contentmd5"); sum != "" {
		expected := md5.Sum(body)
		if base64.StdEncoding.EncodeToString(expected[:]) != sum {
			return ots.ErrParameterInvalid("content md5 mismatch")
		}
	}
	if s.auth == nil {
		return nil
	}
	if s.auth.instanceName != "" && r.Header.Get("x-ots-instancename") != s.auth.instanceName {
		return ots.ErrAuthFailed("instance name mismatch")
	}
	if s.auth.accessKeyID == "" && s.auth.accessKeySecret == "" {
		return nil
	}
	for _, header := range []string{"x-ots-date", "x-ots-apiversion", "x-ots-accesskeyid", "x-ots-contentmd5", "x-ots-instancename"} {
		if r.Header.Get(header) == "" {
			return ots.ErrAuthFailed("missing required header %s", header)
		}
	}
	if r.Header.Get("x-ots-accesskeyid") != s.auth.accessKeyID {
		return ots.ErrAuthFailed("access key id mismatch")
	}
	return verifySignature(r, path, s.auth.accessKeySecret)
}
