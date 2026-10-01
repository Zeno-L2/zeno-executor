FROM golang:1.23-alpine@sha256:383395b794dffa5b53012a212365d40c8e37109a626ca30d6151c8348d380b5f AS build
ENV CGO_ENABLED=0
WORKDIR /src/go-executor
COPY --from=go-zenon . /src/go-zenon
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/ ./cmd/...

FROM alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8 AS runtime
COPY --from=build /out/ /usr/local/bin/
ENTRYPOINT ["executor"]
