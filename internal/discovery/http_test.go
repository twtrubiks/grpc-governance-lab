package discovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newTestServer 起一個掛好 HTTP API 的註冊中心。
func newTestServer(t *testing.T) (*httptest.Server, *Registry) {
	t.Helper()
	r := New(testConfig(-1))
	t.Cleanup(r.Close)
	srv := httptest.NewServer(NewHandler(r))
	t.Cleanup(srv.Close)
	return srv, r
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestHTTP_RegisterFetchCancel(t *testing.T) {
	srv, _ := newTestServer(t)

	if resp := postJSON(t, srv.URL+"/register", ins("account", "a1")); resp.StatusCode != http.StatusOK {
		t.Fatalf("register 應回 200,實際: %d", resp.StatusCode)
	}

	resp, err := http.Get(srv.URL + "/fetch?service=account")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var snap snapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.Instances) != 1 || snap.Instances[0].ID != "a1" {
		t.Fatalf("fetch 應拿到剛註冊的副本,實際: %+v", snap)
	}

	if resp := postJSON(t, srv.URL+"/cancel", ins("account", "a1")); resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel 應回 200,實際: %d", resp.StatusCode)
	}
	resp2, err := http.Get(srv.URL + "/fetch?service=account")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	var snap2 snapshotResponse
	if err := json.NewDecoder(resp2.Body).Decode(&snap2); err != nil {
		t.Fatal(err)
	}
	if len(snap2.Instances) != 0 {
		t.Fatalf("cancel 後 fetch 應為空,實際: %+v", snap2.Instances)
	}
}

func TestHTTP_ErrorShapes(t *testing.T) {
	srv, _ := newTestServer(t)

	// 續約不存在的副本 → 404 + 業務碼 -404
	resp := postJSON(t, srv.URL+"/renew", ins("account", "ghost"))
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("renew 不存在副本應回 404,實際: %d", resp.StatusCode)
	}
	var e errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	if e.Code != -404 {
		t.Errorf("錯誤 body 應帶業務碼 -404,實際: %+v", e)
	}

	// fetch 不存在的服務 → 404
	respGet, err := http.Get(srv.URL + "/fetch?service=ghost")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = respGet.Body.Close() }()
	if respGet.StatusCode != http.StatusNotFound {
		t.Errorf("fetch 不存在服務應回 404,實際: %d", respGet.StatusCode)
	}

	// 非法 JSON → 400
	respBad, err := http.Post(srv.URL+"/register", "application/json", bytes.NewReader([]byte("{誰是json")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = respBad.Body.Close() }()
	if respBad.StatusCode != http.StatusBadRequest {
		t.Errorf("非法 JSON 應回 400,實際: %d", respBad.StatusCode)
	}
}

func TestHTTP_PollLongPolling(t *testing.T) {
	srv, r := newTestServer(t)

	if err := r.Register(ins("account", "a1")); err != nil {
		t.Fatal(err)
	}
	// 測試全程超過 a1 的 TTL(120ms),要持續心跳,
	// 否則喚醒 poll 的會是「a1 被剔除」而不是「a2 註冊」
	startBeating(t, r, 10*time.Millisecond, []Instance{ins("account", "a1")})
	_, version, _ := r.Fetch("account")

	// 帶最新版本號 poll、期間無變化 → 304
	respTimeout, err := http.Get(fmt.Sprintf("%s/poll?service=account&version=%d", srv.URL, version))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = respTimeout.Body.Close() }()
	if respTimeout.StatusCode != http.StatusNotModified {
		t.Fatalf("無變化的 poll 逾時應回 304,實際: %d", respTimeout.StatusCode)
	}

	// 期間有變化 → 200 + 新快照
	type pollResult struct {
		status int
		snap   snapshotResponse
	}
	got := make(chan pollResult, 1)
	go func() {
		resp, err := http.Get(fmt.Sprintf("%s/poll?service=account&version=%d", srv.URL, version))
		if err != nil {
			return
		}
		defer func() { _ = resp.Body.Close() }()
		var snap snapshotResponse
		_ = json.NewDecoder(resp.Body).Decode(&snap)
		got <- pollResult{resp.StatusCode, snap}
	}()
	time.Sleep(20 * time.Millisecond) // 讓 poll 先掛上
	if err := r.Register(ins("account", "a2")); err != nil {
		t.Fatal(err)
	}

	select {
	case res := <-got:
		if res.status != http.StatusOK || len(res.snap.Instances) != 2 {
			t.Fatalf("變化後 poll 應回 200 + 2 副本,實際: %d %+v", res.status, res.snap)
		}
	case <-time.After(time.Second):
		t.Fatal("地址變化後 poll 應在 1 秒內返回")
	}
}
