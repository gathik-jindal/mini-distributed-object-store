# ---- build: static binaries (pure-Go SQLite, no cgo) ----
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# ---- runtime: one image, three roles (controller | worker | dosctl) ----
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/ /usr/local/bin/
CMD ["controller"]
