# build context is the repo root (needs ../../proto for the replace directive)
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY proto/go.mod proto/go.sum proto/
COPY services/worker/go.mod services/worker/go.sum services/worker/
RUN cd services/worker && go mod download
COPY proto/ proto/
COPY services/worker/ services/worker/
RUN cd services/worker && CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /out/worker /usr/local/bin/worker
ENTRYPOINT ["worker"]
