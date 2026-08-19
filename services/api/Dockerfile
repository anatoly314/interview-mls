# build context is the repo root (needs ../../proto for the replace directive)
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY proto/go.mod proto/go.sum proto/
COPY services/api/go.mod services/api/go.sum services/api/
RUN cd services/api && go mod download
COPY proto/ proto/
COPY services/api/ services/api/
RUN cd services/api && CGO_ENABLED=0 go build -o /out/api ./cmd/api

FROM alpine:3.21
RUN apk add --no-cache ca-certificates curl
COPY --from=build /out/api /usr/local/bin/api
ENTRYPOINT ["api"]
