package models

import "github.com/GoFurry/gofurry-nav-collector/common/models"

type PingVo struct {
	Domain string `json:"domain"`
}

type PingModel struct {
	Name         string           `json:"name"`         // 对象名称
	PingTime     models.LocalTime `json:"pingTime"`     // ping时间
	AvgLossRate  float64          `json:"avgLossRate"`  // 平均丢包率
	AvgDelayTime int64            `json:"avgDelayTime"` // 平均延迟
}

type PingSaveModel struct {
	Status string           `json:"status"` // 状态
	Time   models.LocalTime `json:"time"`   // ping时间
	Loss   string           `json:"loss"`   // 平均丢包率
	Delay  string           `json:"delay"`  // 平均延迟
}
