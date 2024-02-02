package main

import (
	"os"
	"runtime"

	"github.com/miaojuncn/etcd-ops/cmd"
	"github.com/miaojuncn/etcd-ops/pkg/log"
	"go.uber.org/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

func main() {
	if len(os.Getenv("GOMAXPROCS")) == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	logger := log.NewLogger()
	ctx := signals.SetupSignalHandler()
	command := cmd.RootCommand(ctx)
	if err := command.Execute(); err != nil {
		logger.Fatal("Something error.", zap.NamedError("error", err))
	}
}
