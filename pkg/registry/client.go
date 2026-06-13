package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// Config 是 SDK 的全部時間參數;零值欄位採生產預設,測試注入毫秒級。
type Config struct {
	// Endpoint 註冊中心位址,例如 "http://127.0.0.1:7171",必填。
	Endpoint string
	// HeartbeatInterval 心跳週期,預設 30s(與 server 端期望一致)。
	HeartbeatInterval time.Duration
	// RequestTimeout 一般請求(register/renew/cancel/fetch)逾時,預設 5s。
	RequestTimeout time.Duration
	// PollTimeout 長輪詢的 client 端上限,必須大於 server 端的
	// poll-timeout(預設 30s),否則永遠等不到 304;預設 40s。
	PollTimeout time.Duration
	// BackoffBase poll/重新註冊失敗的退避起點,預設 1s。
	BackoffBase time.Duration
	// BackoffMax 指數退避上限,預設 15s。
	BackoffMax time.Duration
	// Logger 結構化日誌;nil 用 slog.Default()。
	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 30 * time.Second
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.PollTimeout <= 0 {
		c.PollTimeout = 40 * time.Second
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = time.Second
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 15 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// Instance 是要註冊或訂閱到的服務副本,JSON 形狀與註冊中心的 API 一致。
type Instance struct {
	// Service 服務名,例如 "account"。
	Service string `json:"service"`
	// ID 副本唯一識別;空值時預設用 Addr。
	ID string `json:"id"`
	// Addr gRPC 位址,host:port。
	Addr string `json:"addr"`
	// Metadata 附加資訊,原樣透傳給訂閱者。
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Client 是註冊中心的 HTTP client SDK。
// 方法皆為併發安全(內部只有不可變設定與 *http.Client)。
type Client struct {
	cfg Config
	hc  *http.Client
}

// New 建立 SDK client;cfg.Endpoint 必填,其餘零值補生產預設。
func New(cfg Config) *Client {
	return &Client{
		cfg: cfg.withDefaults(),
		// 不設 http.Client.Timeout:長輪詢請求要掛比一般請求久,
		// 逾時一律由各呼叫的 ctx 控制
		hc: &http.Client{},
	}
}

// Fetch 拉取服務目前的副本列表(一次性,不訂閱)。
func (c *Client) Fetch(ctx context.Context, service string) ([]Instance, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	snap, _, err := c.get(ctx, "/fetch?service="+url.QueryEscape(service))
	if err != nil {
		return nil, err
	}
	return snap.Instances, nil
}

// errNotModified 表示長輪詢逾時無變化(HTTP 304),呼叫方直接再來一輪。
var errNotModified = errors.New("registry: 無變化")

// poll 長輪詢一次;有變化回傳新快照與版本,逾時回傳 errNotModified。
func (c *Client) poll(ctx context.Context, service string, version int64) ([]Instance, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.PollTimeout)
	defer cancel()
	snap, status, err := c.get(ctx,
		"/poll?service="+url.QueryEscape(service)+"&version="+strconv.FormatInt(version, 10))
	if status == http.StatusNotModified {
		return nil, version, errNotModified
	}
	if err != nil {
		return nil, version, err
	}
	return snap.Instances, snap.Version, nil
}

func (c *Client) register(ctx context.Context, ins Instance) error {
	return c.post(ctx, "/register", ins)
}

func (c *Client) renew(ctx context.Context, ins Instance) error {
	return c.post(ctx, "/renew", ins)
}

func (c *Client) cancel(ctx context.Context, ins Instance) error {
	return c.post(ctx, "/cancel", ins)
}

// snapshotResponse 對應 server 端 fetch/poll 的回應形狀。
type snapshotResponse struct {
	Version   int64      `json:"version"`
	Instances []Instance `json:"instances"`
}

// errorResponse 對應 server 端的錯誤回應形狀。
type errorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (c *Client) post(ctx context.Context, path string, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("registry: 序列化 %s body: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint+path, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("registry: 建立 %s 請求: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("registry: %s 請求失敗: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	return checkResponse(path, resp)
}

func (c *Client) get(ctx context.Context, pathAndQuery string) (*snapshotResponse, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.Endpoint+pathAndQuery, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("registry: 建立請求: %w", err)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("registry: 請求失敗: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotModified {
		return nil, resp.StatusCode, nil
	}
	if err := checkResponse(pathAndQuery, resp); err != nil {
		return nil, resp.StatusCode, err
	}
	var snap snapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("registry: 解析回應: %w", err)
	}
	return &snap, resp.StatusCode, nil
}

// checkResponse 把非 2xx 回應轉成帶 ecode 的錯誤,
// 心跳迴圈靠 ecode.Equal(err, ecode.NothingFound) 判斷要不要重新註冊。
func checkResponse(path string, resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	var body errorResponse
	// 解析失敗就拿不到細節,照樣按 HTTP 狀態映射
	_ = json.NewDecoder(resp.Body).Decode(&body)

	mapped := ecode.ServerErr
	switch body.Code {
	case ecode.NothingFound.Code():
		mapped = ecode.NothingFound
	case ecode.RequestErr.Code():
		mapped = ecode.RequestErr
	}
	return fmt.Errorf("registry: %s 回應 HTTP %d(%s): %w",
		path, resp.StatusCode, body.Message, mapped)
}
