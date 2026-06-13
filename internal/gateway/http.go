package gateway

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/twtrubiks/grpc-governance-lab/pkg/balancer/wrr"
	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// BackendStatsFunc 回傳各後端節點的即時快照,供 /debug/backends 輸出。
// 預設接 wrr.Stats;抽成函式型別讓測試可注入。
type BackendStatsFunc func() []wrr.Stat

// RegisterRoutes 把聚合 API、/debug/backends 掛到 gin engine。
func RegisterRoutes(r *gin.Engine, agg *Aggregator, stats BackendStatsFunc) {
	if stats == nil {
		stats = wrr.Stats
	}

	// GET /user/profile?id= :併發聚合 account + relation
	r.GET("/user/profile", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Query("id"), 10, 64)
		if err != nil || id <= 0 {
			writeEcode(c, ecode.RequestErr)
			return
		}
		profile, err := agg.GetProfile(c.Request.Context(), id)
		if err != nil {
			writeEcode(c, ecode.Cause(err))
			return
		}
		c.JSON(http.StatusOK, profile)
	})

	// GET /debug/backends :各後端節點即時權重/QPS/成功率(demo 觀測面)
	r.GET("/debug/backends", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"backends": stats()})
	})
}

// writeEcode 把業務錯誤碼轉成 HTTP 回應:HTTP status 取語意相近的碼,
// body 一律帶業務碼 {"code","message"}——錯誤碼透傳(demo 場景 4)的 HTTP 出口。
func writeEcode(c *gin.Context, code ecode.Code) {
	c.JSON(httpStatus(code), gin.H{"code": code.Code(), "message": code.Message()})
}

// httpStatus 把業務碼映射成 HTTP 狀態碼(僅供 HTTP 語意,body 的 code 才權威)。
func httpStatus(code ecode.Code) int {
	switch code.Code() {
	case ecode.OK.Code():
		return http.StatusOK
	case ecode.RequestErr.Code():
		return http.StatusBadRequest
	case ecode.Unauthorized.Code():
		return http.StatusUnauthorized
	case ecode.AccessDenied.Code():
		return http.StatusForbidden
	case ecode.NothingFound.Code():
		return http.StatusNotFound
	case ecode.Deadline.Code():
		return http.StatusGatewayTimeout
	case ecode.ServiceUnavailable.Code():
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
