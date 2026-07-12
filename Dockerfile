# Multi-stage build for a single kill-my-cluster node (Raft + KV + metrics).
# Works on Apple Silicon and Oracle Ampere (linux/arm64).

FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /kmc-node ./cmd/node

FROM gcr.io/distroless/static-debian12
COPY --from=build /kmc-node /kmc-node
# Named volumes mount as root-owned; the node process needs to create WAL files.
# Drop privileges later via a control-plane/security pass if needed.
EXPOSE 7000 8000 9100
VOLUME ["/data"]
ENTRYPOINT ["/kmc-node"]
