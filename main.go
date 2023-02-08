package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/miaojuncn/etcd-ops/cmd"
	"go.uber.org/zap"
)

var onlyOneSignalHandler = make(chan struct{})

func main() {
	if len(os.Getenv("GOMAXPROCS")) == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	logger := initLogger()
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	ctx := setupSignalHandler()
	command := cmd.RootCommand(ctx)
	if err := command.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func setupSignalHandler() context.Context {
	close(onlyOneSignalHandler) // panics when called twice

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cancel()
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return ctx
}

func initLogger() *zap.Logger {
	logger, _ := zap.NewDevelopment()
	return logger
}
