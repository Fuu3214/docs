package common

// MediaInfo 存放在ckv+中的媒体ID数据结构
type MediaInfo struct {
	MediaId    int64 `json:"mediaId"`
	PublicTime int64 `json:"publicTime"`
	ExStatus   int64 `json:"exStatus"`
}

// MsiInfo IP信息配置数据
type MsiInfo struct {
	Ip         int64   `json:"ip"`         // ip
	MediaTypes []int32 `json:"mediaTypes"` // 此ip对于哪些媒资类型生效
	LinkTypes  []int64 `json:"linkTypes"`  // 此ip对于哪些关联关系类型生效
	Score      float64 `json:"score"`      // 最小生效的置信区间
}
