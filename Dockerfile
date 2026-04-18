# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
ENV CGO_ENABLED=0
RUN --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/world-zap .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/world-zap /world-zap
EXPOSE 9999
USER nonroot
ENTRYPOINT ["/world-zap"]
