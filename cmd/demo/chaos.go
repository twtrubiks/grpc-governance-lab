package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// chaos 是 demo 用的故障注入器:可在執行期動態加延遲、注錯誤、改 CPU 回報,
// 讓 demo 場景 3(注入延遲)不必重啟服務。所有欄位 atomic,執行緒安全。
type chaos struct {
	delayMillis atomic.Int64 // 每個請求額外睡眠的毫秒數
	failCode    atomic.Int32 // 非 0 時讓請求回對應 grpc code 錯誤
	cpuMilli    atomic.Int64 // 經 trailer 回報的 CPU 使用率(千分比)
}

// interceptor 是注入延遲與錯誤的 server 攔截器(放在業務 handler 外層)。
func (c *chaos) interceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if d := c.delayMillis.Load(); d > 0 {
			t := time.NewTimer(time.Duration(d) * time.Millisecond)
			defer t.Stop()
			select {
			case <-t.C:
			case <-ctx.Done(): // 上游已逾時/取消,別白睡
				return nil, status.FromContextError(ctx.Err()).Err()
			}
		}
		if code := c.failCode.Load(); code != 0 {
			return nil, status.Error(codes.Code(code), "chaos 注入的錯誤")
		}
		return handler(ctx, req)
	}
}

// cpuReporter 回傳供 wrr.ReportLoadInterceptor 用的 CPU 取值函式。
func (c *chaos) cpuReporter() func() int64 {
	return func() int64 { return c.cpuMilli.Load() }
}

// adminHandler 是 chaos 控制端點:
//
//	POST /inject?delay=200&fail=14&cpu=800   設定(缺的參數不動)
//	POST /reset                              清空所有注入
//	GET  /state                              查目前注入狀態
func (c *chaos) adminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /inject", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if v := q.Get("delay"); v != "" {
			c.delayMillis.Store(parseInt(v))
		}
		if v := q.Get("fail"); v != "" {
			c.failCode.Store(int32(parseInt(v)))
		}
		if v := q.Get("cpu"); v != "" {
			c.cpuMilli.Store(parseInt(v))
		}
		c.writeState(w)
	})
	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, _ *http.Request) {
		c.delayMillis.Store(0)
		c.failCode.Store(0)
		c.cpuMilli.Store(0) // 連 CPU 也歸零,否則注入過的 CPU 會殘留、持續影響權重
		c.writeState(w)
	})
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, _ *http.Request) {
		c.writeState(w)
	})
	return mux
}

func (c *chaos) writeState(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int64{
		"delay_ms":  c.delayMillis.Load(),
		"fail_code": int64(c.failCode.Load()),
		"cpu_milli": c.cpuMilli.Load(),
	})
}

func parseInt(s string) int64 {
	var n int64
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0
		}
		n = n*10 + int64(ch-'0')
	}
	return n
}
