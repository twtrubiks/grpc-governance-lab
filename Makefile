# 開發常用指令入口。提交前至少跑一次 `make check`。

.PHONY: fmt vet lint test race cover check proto

# 格式化(gofmt 標準格式,CI 會驗證)
fmt:
	gofmt -s -w .

vet:
	go vet ./...

# 需要本機安裝 golangci-lint v2:https://golangci-lint.run/welcome/install/
lint:
	golangci-lint run ./...

test:
	go test ./...

# -race 是本專案的底線(balancer / registry 都是併發重災區)
race:
	go test -race ./...

# 覆蓋率報告;pkg/ 目標 >= 80%(見 docs/CODE_QUALITY.md §5)
cover:
	go test -coverprofile=coverage.out ./pkg/...
	go tool cover -func=coverage.out | tail -1

# 提交前自查:格式 + 靜態檢查 + lint + race 測試
check: fmt vet lint race

# 由 .proto 生成 Go 代碼;需要 buf + protoc-gen-go + protoc-gen-go-grpc:
#   go install github.com/bufbuild/buf/cmd/buf@latest
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
proto:
	buf lint
	buf generate
