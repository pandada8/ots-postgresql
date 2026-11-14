// Command ots-postgresql exposes the Alibaba Tablestore (OTS) protocol and proxies
// requests to a PostgreSQL database, for development and testing scenarios.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/pandada8/ots-postgresql/internal/server"
	"github.com/pandada8/ots-postgresql/internal/storage"
)

func envDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func main() {
	listen := flag.String("listen", envDefault("OTS_LISTEN", ":8080"), "HTTP listen address")
	dsn := flag.String("postgres-dsn", envDefault("POSTGRES_DSN", "postgres://postgres:postgres@127.0.0.1:5432/ots?sslmode=disable"), "PostgreSQL DSN")
	prefix := flag.String("table-prefix", envDefault("OTS_TABLE_PREFIX", ""), "prefix for PostgreSQL table names")
	accessKeyID := flag.String("access-key-id", envDefault("OTS_ACCESS_KEY_ID", ""), "access key id required from clients (empty disables auth)")
	accessKeySecret := flag.String("access-key-secret", envDefault("OTS_ACCESS_KEY_SECRET", ""), "access key secret used to verify signatures")
	instance := flag.String("instance", envDefault("OTS_INSTANCE_NAME", ""), "instance name required from clients (empty disables check)")
	logLevel := flag.String("log-level", envDefault("OTS_LOG_LEVEL", "info"), "log level: debug, info, warn, error")
	bootstrapFile := flag.String("bootstrap", envDefault("OTS_BOOTSTRAP_FILE", ""), "JSON file describing tables and search indexes to ensure at startup (empty disables bootstrap)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: parseLogLevel(*logLevel)}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	engine, err := storage.Open(ctx, *dsn, *prefix)
	if err != nil {
		logger.Error("failed to initialize storage", "error", err)
		os.Exit(1)
	}
	defer engine.Close()

	if *bootstrapFile != "" {
		if err := runBootstrap(ctx, engine, *bootstrapFile); err != nil {
			logger.Error("bootstrap failed", "error", err)
			os.Exit(1)
		}
	}

	srv := server.New(server.Config{
		Engine:          engine,
		AccessKeyID:     *accessKeyID,
		AccessKeySecret: *accessKeySecret,
		InstanceName:    *instance,
		Logger:          logger,
	})

	httpServer := &http.Server{
		Addr:              *listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown error", "error", err)
		}
	}()

	logger.Info("ots-postgresql listening", "addr", *listen, "auth", *accessKeyID != "" || *accessKeySecret != "" || *instance != "")
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
	logger.Info("ots-postgresql stopped")
}

func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func runBootstrap(ctx context.Context, engine *storage.Engine, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read bootstrap file: %w", err)
	}
	spec, err := storage.ParseBootstrapSpec(data)
	if err != nil {
		return err
	}
	return engine.Bootstrap(ctx, spec)
}
