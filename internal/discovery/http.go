package discovery

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// NewHandler 把 Registry 包成 HTTP API:
//
//	POST /register  {service,id,addr,metadata}
//	POST /renew     {service,id}
//	POST /cancel    {service,id}
//	GET  /fetch?service=
//	GET  /poll?service=&version=   (長輪詢;無變化逾時回 304)
//
// 成功回 200 JSON;失敗回對應 HTTP 狀態 + {"code","message"}。
func NewHandler(r *Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /register", func(w http.ResponseWriter, req *http.Request) {
		var ins Instance
		if !decodeBody(w, req, &ins) {
			return
		}
		if err := r.Register(ins); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, struct{}{})
	})
	mux.HandleFunc("POST /renew", func(w http.ResponseWriter, req *http.Request) {
		var ins Instance
		if !decodeBody(w, req, &ins) {
			return
		}
		if err := r.Renew(ins.Service, ins.ID); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, struct{}{})
	})
	mux.HandleFunc("POST /cancel", func(w http.ResponseWriter, req *http.Request) {
		var ins Instance
		if !decodeBody(w, req, &ins) {
			return
		}
		if err := r.Cancel(ins.Service, ins.ID); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, struct{}{})
	})
	mux.HandleFunc("GET /fetch", func(w http.ResponseWriter, req *http.Request) {
		service := req.URL.Query().Get("service")
		instances, version, ok := r.Fetch(service)
		if !ok {
			writeError(w, ecode.NothingFound)
			return
		}
		writeJSON(w, http.StatusOK, snapshotResponse{Version: version, Instances: instances})
	})
	mux.HandleFunc("GET /poll", func(w http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		service := q.Get("service")
		since, _ := strconv.ParseInt(q.Get("version"), 10, 64) // 解析失敗視為 0(拿全量)
		instances, version, changed := r.Poll(req.Context(), service, since)
		if !changed {
			// 逾時無變化:304 讓 client 帶同一版本號立刻再來
			w.WriteHeader(http.StatusNotModified)
			return
		}
		writeJSON(w, http.StatusOK, snapshotResponse{Version: version, Instances: instances})
	})
	return mux
}

// snapshotResponse 是 fetch/poll 的回應形狀。
type snapshotResponse struct {
	Version   int64      `json:"version"`
	Instances []Instance `json:"instances"`
}

// errorResponse 是錯誤回應形狀,code 即 pkg/ecode 的業務碼。
type errorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// decodeBody 解析 JSON body;失敗時直接回 400 並回報 false。
func decodeBody(w http.ResponseWriter, req *http.Request, v any) bool {
	if err := json.NewDecoder(req.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Code: ecode.RequestErr.Code(), Message: "body 不是合法 JSON",
		})
		return false
	}
	return true
}

// writeError 依業務碼選 HTTP 狀態:-404 → 404、-400 → 400、其餘 500。
func writeError(w http.ResponseWriter, err error) {
	c := ecode.Cause(err)
	status := http.StatusInternalServerError
	switch c.Code() {
	case ecode.NothingFound.Code():
		status = http.StatusNotFound
	case ecode.RequestErr.Code():
		status = http.StatusBadRequest
	}
	writeJSON(w, status, errorResponse{Code: c.Code(), Message: c.Message()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// 編碼到 ResponseWriter 失敗代表連線已斷,無事可做
	_ = json.NewEncoder(w).Encode(v)
}
