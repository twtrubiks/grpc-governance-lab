package ecode

import (
	"errors"
	"fmt"
	"testing"
)

func TestNew_DuplicatePanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("重複註冊同一個 code 應該 panic,實際沒有")
		}
	}()
	// -500 已在 codes.go 註冊為 ServerErr,再註冊一次必須 panic(回傳值不會用到)
	_ = New(-500, "撞號的錯誤碼")
}

func TestCause(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Code
	}{
		{"nil 錯誤返回 OK", nil, OK},
		{"裸 Code 直接反解", NothingFound, NothingFound},
		{"被 %w 包裹一層的 Code", fmt.Errorf("查無使用者: %w", NothingFound), NothingFound},
		{"被 %w 包裹兩層的 Code", fmt.Errorf("外層: %w", fmt.Errorf("內層: %w", Deadline)), Deadline},
		{"普通 error 兜底為 ServerErr", errors.New("資料庫炸了"), ServerErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Cause(tt.err); got != tt.want {
				t.Errorf("Cause() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEqual(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code Code
		want bool
	}{
		{"包裹後仍判定相等", fmt.Errorf("wrap: %w", NothingFound), NothingFound, true},
		{"不同碼判定不等", NothingFound, ServerErr, false},
		{"nil 等於 OK", nil, OK, true},
		{"跨網路重建的 Code 只比數值", Code{code: -404, message: "訊息不一樣"}, NothingFound, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Equal(tt.err, tt.code); got != tt.want {
				t.Errorf("Equal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCode_Error(t *testing.T) {
	if got, want := NothingFound.Error(), "-404: 資源不存在"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}
