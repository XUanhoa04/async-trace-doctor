# syntax=docker/dockerfile:1.7
FROM golang:1.25.7-alpine3.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/async-trace-doctor ./cmd/async-trace-doctor \
 && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/demo-producer ./demo/producer \
 && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/demo-consumer ./demo/consumer

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/* /usr/local/bin/
COPY config/rules.yaml /etc/async-trace-doctor/rules.yaml
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/async-trace-doctor"]
CMD ["serve", "--rules", "/etc/async-trace-doctor/rules.yaml"]
