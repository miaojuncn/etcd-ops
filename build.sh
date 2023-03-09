set -e

VCS="github.com"
ORGANIZATION="miaojuncn"
PROJECT="etcd-ops"
REPOSITORY=${VCS}/${ORGANIZATION}/${PROJECT}

SOURCE_PATH=$(pwd)
BINARY_PATH=${SOURCE_PATH}/bin
export GO111MODULE=on
cd "${SOURCE_PATH}"

VERSION_FILE="${SOURCE_PATH}/VERSION"
VERSION="$(cat "${VERSION_FILE}")"
GIT_SHA=$(git rev-parse --short HEAD || echo "GitNotFound")

CGO_ENABLED=0 go build \
  -a -o "${BINARY_PATH}/etcd-ops" \
  -ldflags "-w -X ${REPOSITORY}/pkg/version.Version=${VERSION} -X ${REPOSITORY}/pkg/version.GitSHA=${GIT_SHA}" \
  main.go
