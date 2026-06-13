# 單一映像,內含三個 binary(discovery / demo / gateway),
# docker-compose 用 command 切換要跑哪個。多階段建置讓最終映像精簡。

FROM golang:1.26 AS build
WORKDIR /src
# 先抓相依,利用 layer 快取(go.mod 沒變就不重抓)
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# 靜態連結,讓 binary 能跑在 distroless/static 上
ENV CGO_ENABLED=0
RUN go build -trimpath -o /out/discovery ./cmd/discovery && \
    go build -trimpath -o /out/demo ./cmd/demo && \
    go build -trimpath -o /out/gateway ./cmd/gateway
# 標準 gRPC 健康探測工具,供 compose healthcheck 與手動驗證(PLAN M6)
RUN GOBIN=/out go install github.com/grpc-ecosystem/grpc-health-probe@v0.4.34

# 用 distroless static:無 shell、無套件管理器,攻擊面最小
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/discovery /out/demo /out/gateway /out/grpc-health-probe /usr/local/bin/
# 用 CMD 而非 ENTRYPOINT:compose 的 command 與 `docker run` 位置參數會「完整取代」
# 這行,各服務才能切到 discovery/demo;預設(無覆蓋)仍跑 gateway。
# (若用 ENTRYPOINT,command 會被「附加」成 gateway 的參數而非取代,全部跑成 gateway。)
CMD ["/usr/local/bin/gateway"]
