package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"

	"go.uber.org/zap"

	ver "github.com/miaojuncn/etcd-ops/pkg/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	version bool
)

func printVersionInfo() {
	fmt.Printf("etcd-ops Version: %s\n", ver.Version)
	fmt.Printf("GIT SHA: %s\n", ver.GitSHA)
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Printf("Go OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
}

type Handler struct {
	server *http.Server
	Port   uint
	logger *zap.Logger
}

func (h *Handler) RegisterHandler() {
	handler := http.NewServeMux()
	handler.Handle("/metrics", promhttp.Handler())

	h.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", h.Port),
		Handler: handler,
	}
}

func (h *Handler) Start() {
	h.logger.Info("Starting HTTP server.", zap.String("address", h.server.Addr))
	err := h.server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		h.logger.Fatal("Failed to start http server", zap.NamedError("error", err))
	}
	h.logger.Info("HTTP server closed gracefully.")
	return
}

// Stop stops the http server
func (h *Handler) Stop() error {
	return h.server.Close()
}

func metricsServer(ctx context.Context, logger *zap.Logger) {
	ms := Handler{Port: 6200, logger: logger.With(zap.String("actor", "metrics"))}
	ms.RegisterHandler()
	go ms.Start()
	<-ctx.Done()
	if err := ms.Stop(); err != nil {
		logger.Error("Failed to stop metrics server.", zap.NamedError("error", err))
	}
}
