# proto 約定

- 目錄結構:`<服務名>/v1/<服務名>.proto`,package 名 `<服務名>.v1`,
  從第一天就帶版本號(升不相容版本時開 v2 目錄,不改 v1)。
- `ecode/v1`:錯誤碼 detail 的 proto 定義,由 pkg/ecode 夾進
  grpc status.Details 跨服務傳遞。
- 生成指令:`make proto`(M2 接入 buf 後生效;生成的 *.pb.go
  放各自目錄,提交進版控,讓 clone 下來不裝 protoc 也能編譯)。
