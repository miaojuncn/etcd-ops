package cmd

import (
	"fmt"
	"runtime"

	ver "github.com/miaojuncn/etcd-ops/pkg/version"
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
