package cmd

import (
	"context"
	"fmt"
	"net/http"
	"runtime"

	ver "github.com/miaojuncn/etcd-ops/pkg/version"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
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
	zlog.Logger.Infof("Starting HTTP server at addr: %s", h.server.Addr)
	err := h.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		zlog.Logger.Fatalf("Failed to start http server: %v", err)
	}
	zlog.Logger.Infof("HTTP server closed gracefully.")
	return
}

// Stop stops the http server
func (h *Handler) Stop() error {
	return h.server.Close()
}

func metricsServer(ctx context.Context) {
	ms := Handler{Port: 6200}
	ms.RegisterHandler()
	go ms.Start()
	<-ctx.Done()
	if err := ms.Stop(); err != nil {
		zlog.Logger.Errorf("Failed to stop metrics server: %v", err)
	}
}
