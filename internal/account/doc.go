// Package account 是 demo 業務服務:使用者基本資料(里程碑 M6)。
//
// 分層:handler(gRPC 介面)→ service(業務邏輯)→ dao(記憶體儲存)。
// 業務本身刻意極簡(GetUser 回固定假資料),它存在的目的是
// 證明 pkg/ 下的治理元件「真的能用」:錯誤一律走 ecode、
// 透過 pkg/registry 註冊自己、被 gateway 經服務發現呼叫。
package account
