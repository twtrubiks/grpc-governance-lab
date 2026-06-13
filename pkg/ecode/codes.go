package ecode

// 全站通用錯誤碼。負數碼採 HTTP 狀態碼取負的慣例,
// 一眼能對應語意;各業務服務自己的錯誤碼(正數、分段)在各自套件註冊。
var (
	// OK 表示成功,code 為 0。
	OK = New(0, "ok")

	// RequestErr 請求參數錯誤。
	RequestErr = New(-400, "請求錯誤")
	// Unauthorized 未通過身分驗證。
	Unauthorized = New(-401, "未認證")
	// AccessDenied 已認證但權限不足。
	AccessDenied = New(-403, "權限不足")
	// NothingFound 資源不存在。
	NothingFound = New(-404, "資源不存在")
	// Canceled 客戶端取消請求。
	Canceled = New(-498, "客戶端取消請求")
	// ServerErr 伺服器內部錯誤,也是所有未知錯誤的兜底碼。
	ServerErr = New(-500, "伺服器錯誤")
	// ServiceUnavailable 服務暫時不可用(過載或維護中)。
	ServiceUnavailable = New(-503, "服務暫不可用")
	// Deadline 請求逾時(deadline 已到)。
	Deadline = New(-504, "請求逾時")
	// LimitExceed 觸發限流。
	LimitExceed = New(-509, "請求過於頻繁")
)
