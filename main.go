package main

import (
	"os"
	"runtime"

	"github.com/miaojuncn/etcd-ops/cmd"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

func main() {
	if len(os.Getenv("GOMAXPROCS")) == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	ctx := signals.SetupSignalHandler()
	command := cmd.RootCommand(ctx)
	if err := command.Execute(); err != nil {
		zlog.Logger.Fatalf("Something error: %v", err)
	}
}
