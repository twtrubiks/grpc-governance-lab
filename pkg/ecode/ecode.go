package ecode

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
)

// Code 是業務錯誤碼,實作 error 介面,可直接當錯誤回傳、
// 也可被 fmt.Errorf("...: %w", code) 包裹後由 Cause 反解。
//
// 值型別、欄位不可變:建立後不會被併發修改,跨 goroutine 傳遞安全。
type Code struct {
	code    int
	message string
}

// Code 回傳錯誤碼數值。
func (c Code) Code() int { return c.code }

// Message 回傳給人看的錯誤訊息。
func (c Code) Message() string { return c.message }

// Error 實作 error 介面,格式 "<code>: <message>",方便日誌直接閱讀。
func (c Code) Error() string {
	return strconv.Itoa(c.code) + ": " + c.message
}

// mu 保護 registry;只在 New 註冊時寫入(通常發生在 package init),
// 讀取路徑不查表(message 隨 Code 值本身攜帶),所以不需要 RWMutex。
var (
	mu       sync.Mutex
	registry = make(map[int]string)
)

// New 註冊一個新的業務錯誤碼並回傳。
//
// 同一個 code 重複註冊即 panic:錯誤碼撞號是程式設計錯誤,
// 必須在啟動瞬間 fail-fast,這是 pkg/ 下唯一允許的 panic
// (見 docs/CODE_QUALITY.md §3)。
func New(code int, message string) Code {
	mu.Lock()
	defer mu.Unlock()
	if prev, ok := registry[code]; ok {
		panic(fmt.Sprintf("ecode: code %d 已註冊為 %q,不可重複註冊", code, prev))
	}
	registry[code] = message
	return Code{code: code, message: message}
}

// Cause 從任意 error 反解出業務錯誤碼:
//   - nil → OK
//   - 錯誤鏈上帶 Code(含被 %w 包裹)→ 該 Code
//   - 其他未知錯誤 → ServerErr(對外不洩漏內部錯誤細節)
func Cause(err error) Code {
	if err == nil {
		return OK
	}
	var c Code
	if errors.As(err, &c) {
		return c
	}
	return ServerErr
}

// Equal 判斷 err 反解後是否為指定錯誤碼。
// 只比較 code 數值不比較 message:跨網路傳回的 Code 不經過本地註冊表,
// message 可能因版本差異不同,但 code 數值是雙方的契約。
func Equal(err error, c Code) bool {
	return Cause(err).code == c.code
}
