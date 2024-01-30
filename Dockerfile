FROM registry.devops.rivtower.com/library/golang:1.21 as builder
WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download
COPY . .
RUN make build

FROM registry.devops.rivtower.com/google_containers/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/bin/etcd-ops etcd-ops
USER 65532:65532
ENTRYPOINT ["/etcd-ops"]
