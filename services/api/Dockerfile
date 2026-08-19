# build context is the repo root (needs ../../proto for the replace directive)
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY proto/go.mod proto/go.sum proto/
COPY services/api/go.mod services/api/go.sum services/api/
RUN cd services/api && go mod download
COPY proto/ proto/
COPY services/api/ services/api/
RUN cd services/api && CGO_ENABLED=0 go build -o /out/api ./cmd/api

# The React UI. Lockfile first so an unchanged web/package-lock.json reuses the
# install layer across source edits.
FROM node:22-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM alpine:3.21
RUN apk add --no-cache ca-certificates curl
COPY --from=build /out/api /usr/local/bin/api
# httpapi serves ./web/dist relative to the process working directory, which is
# / here (no WORKDIR is set), so the build lands at /web/dist.
COPY --from=web /web/dist /web/dist
ENTRYPOINT ["api"]
