package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"

	accountv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/account/v1"
	relationv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/relation/v1"
	"github.com/twtrubiks/grpc-governance-lab/pkg/balancer/wrr"
	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// fakeAccount / fakeRelation 用注入的函式決定行為,模擬各種下游狀態。
type fakeAccount struct {
	accountv1.AccountServiceClient
	fn func(id int64) (*accountv1.GetUserResponse, error)
}

func (f *fakeAccount) GetUser(_ context.Context, req *accountv1.GetUserRequest, _ ...grpcCallOption) (*accountv1.GetUserResponse, error) {
	return f.fn(req.GetId())
}

type fakeRelation struct {
	relationv1.RelationServiceClient
	fn func(id int64) (*relationv1.GetFollowerCountResponse, error)
}

func (f *fakeRelation) GetFollowerCount(_ context.Context, req *relationv1.GetFollowerCountRequest, _ ...grpcCallOption) (*relationv1.GetFollowerCountResponse, error) {
	return f.fn(req.GetUserId())
}

// grpcCallOption 別名,讓 fake 的簽名對上 grpc.CallOption(避免直接 import 噪音)。
type grpcCallOption = grpc.CallOption

// TestAggregator_BothSucceed 驗證正常聚合。
func TestAggregator_BothSucceed(t *testing.T) {
	agg := NewAggregator(
		&fakeAccount{fn: func(id int64) (*accountv1.GetUserResponse, error) {
			return &accountv1.GetUserResponse{User: &accountv1.User{Id: id, Name: "u"}}, nil
		}},
		&fakeRelation{fn: func(int64) (*relationv1.GetFollowerCountResponse, error) {
			return &relationv1.GetFollowerCountResponse{Count: 42}, nil
		}},
		200*time.Millisecond, nil)

	p, err := agg.GetProfile(context.Background(), 1)
	if err != nil {
		t.Fatalf("聚合不該失敗: %v", err)
	}
	if p.FollowerCount != 42 || p.Degraded {
		t.Errorf("應拿到真實粉絲數且未降級,得 %+v", p)
	}
}

// TestAggregator_RelationDegrades 是 M6 驗收:relation 全掛時仍回 200 +
// 降級資料(粉絲數 -1),account 主資料完好。
func TestAggregator_RelationDegrades(t *testing.T) {
	agg := NewAggregator(
		&fakeAccount{fn: func(id int64) (*accountv1.GetUserResponse, error) {
			return &accountv1.GetUserResponse{User: &accountv1.User{Id: id, Name: "u"}}, nil
		}},
		&fakeRelation{fn: func(int64) (*relationv1.GetFollowerCountResponse, error) {
			return nil, ecode.ServiceUnavailable // relation 掛了
		}},
		200*time.Millisecond, nil)

	p, err := agg.GetProfile(context.Background(), 1)
	if err != nil {
		t.Fatalf("relation 掛掉不該讓聚合失敗: %v", err)
	}
	if p.FollowerCount != DegradedFollowerCount || !p.Degraded {
		t.Errorf("relation 失敗應降級為 -1,得 %+v", p)
	}
	if p.Name != "u" {
		t.Errorf("account 主資料應完好,得 %+v", p)
	}
}

// TestAggregator_AccountFailsPropagates 驗證 account 失敗時整個請求帶錯誤碼返回。
func TestAggregator_AccountFailsPropagates(t *testing.T) {
	relationCalled := make(chan struct{}, 1)
	agg := NewAggregator(
		&fakeAccount{fn: func(int64) (*accountv1.GetUserResponse, error) {
			return nil, ecode.NothingFound
		}},
		&fakeRelation{fn: func(int64) (*relationv1.GetFollowerCountResponse, error) {
			relationCalled <- struct{}{}
			return &relationv1.GetFollowerCountResponse{Count: 1}, nil
		}},
		200*time.Millisecond, nil)

	_, err := agg.GetProfile(context.Background(), 9999)
	if !ecode.Equal(err, ecode.NothingFound) {
		t.Fatalf("account 的 -404 應透傳,得 %v", err)
	}
}

// TestAggregator_NilUserIsServerErr 驗證:account 回成功卻沒帶 user
// (上游契約被破壞)時當成伺服器錯誤,而不是默默回 id=0 / 空 Name 的檔案。
func TestAggregator_NilUserIsServerErr(t *testing.T) {
	agg := NewAggregator(
		&fakeAccount{fn: func(int64) (*accountv1.GetUserResponse, error) {
			return &accountv1.GetUserResponse{User: nil}, nil // 成功但無 user
		}},
		&fakeRelation{fn: func(int64) (*relationv1.GetFollowerCountResponse, error) {
			return &relationv1.GetFollowerCountResponse{Count: 1}, nil
		}},
		200*time.Millisecond, nil)

	p, err := agg.GetProfile(context.Background(), 1)
	if p != nil {
		t.Fatalf("缺 user 不該回出檔案,實際 %+v", p)
	}
	if !ecode.Equal(err, ecode.ServerErr) {
		t.Fatalf("account 成功但缺 user 應回 -500,實際 %v", err)
	}
}

// TestHTTP_ProfileAndErrors 驗證 HTTP 層:成功回 200、業務碼透傳。
func TestHTTP_ProfileAndErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	agg := NewAggregator(
		&fakeAccount{fn: func(id int64) (*accountv1.GetUserResponse, error) {
			if id == 9999 {
				return nil, ecode.NothingFound
			}
			return &accountv1.GetUserResponse{User: &accountv1.User{Id: id, Name: "u"}}, nil
		}},
		&fakeRelation{fn: func(int64) (*relationv1.GetFollowerCountResponse, error) {
			return &relationv1.GetFollowerCountResponse{Count: 7}, nil
		}},
		200*time.Millisecond, nil)

	r := gin.New()
	RegisterRoutes(r, agg, func() []wrr.Stat { return []wrr.Stat{{Service: "account", Addr: "x", EffectiveWeight: 10}} })

	// 成功
	w := doGet(r, "/user/profile?id=1")
	if w.Code != http.StatusOK {
		t.Fatalf("應回 200,得 %d", w.Code)
	}
	var p Profile
	mustJSON(t, w.Body.Bytes(), &p)
	if p.FollowerCount != 7 {
		t.Errorf("粉絲數應為 7,得 %+v", p)
	}

	// 業務碼透傳:-404 → HTTP 404 + body code -404(場景 4 的 HTTP 出口)
	w = doGet(r, "/user/profile?id=9999")
	if w.Code != http.StatusNotFound {
		t.Errorf("查無使用者應回 404,得 %d", w.Code)
	}
	var e struct {
		Code int `json:"code"`
	}
	mustJSON(t, w.Body.Bytes(), &e)
	if e.Code != -404 {
		t.Errorf("body 應帶業務碼 -404,得 %d", e.Code)
	}

	// 缺 id → 400
	if w := doGet(r, "/user/profile"); w.Code != http.StatusBadRequest {
		t.Errorf("缺 id 應回 400,得 %d", w.Code)
	}

	// /debug/backends
	w = doGet(r, "/debug/backends")
	if w.Code != http.StatusOK {
		t.Fatalf("/debug/backends 應回 200,得 %d", w.Code)
	}
	if body := w.Body.String(); body == "" || body == "{}" {
		t.Errorf("/debug/backends 應有內容,得 %q", body)
	}
}

func doGet(r *gin.Engine, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	r.ServeHTTP(w, req)
	return w
}

func mustJSON(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("解析 JSON 失敗: %v (raw=%s)", err, raw)
	}
}
