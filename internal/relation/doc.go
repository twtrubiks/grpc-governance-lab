// Package relation 是 demo 業務服務:粉絲數查詢(里程碑 M6)。
//
// 與 account 共用一個 binary(cmd/demo 以 flag 切換角色),
// 在 gateway 的聚合 API 裡扮演「可降級的依賴」:
// relation 全掛時,聚合回應的粉絲數欄位回 -1,HTTP 仍回 200。
package relation
