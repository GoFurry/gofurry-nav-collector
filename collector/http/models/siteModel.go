package models

import (
	cm "github.com/GoFurry/gofurry-nav-collector/common/models"
)

const TableNameGfnCollectorDomain = "gfn_collector_domain"

// GfnCollectorDomain mapped from table <gfn_collector_domain>
type GfnCollectorDomain struct {
	ID     int64   `gorm:"column:id;type:bigint;primaryKey;comment:域名请求表id" json:"id"`                        // 域名请求表id
	Name   string  `gorm:"column:name;type:character varying(255);not null;comment:域名" json:"name"`           // 域名
	Proxy  string  `gorm:"column:proxy;type:character varying(4);not null;comment:是否需要代理加速 1 0" json:"proxy"` // 是否需要代理加速 1 0
	Prefix *string `gorm:"column:prefix;type:character varying(255);comment:是否有前缀" json:"prefix"`             // 是否有前缀
	TLS    string  `gorm:"column:tls;type:character varying(4);not null;comment:是否 https 1 0" json:"tls"`     // 是否 https 1 0
}

// TableName GfnCollectorDomain's table name
func (*GfnCollectorDomain) TableName() string {
	return TableNameGfnCollectorDomain
}

const TableNameGfnCollectorLogHTTP = "gfn_collector_log_http"

// GfnCollectorLogHTTP mapped from table <gfn_collector_log_http>
type GfnCollectorLogHTTP struct {
	ID         int64        `gorm:"column:id;type:bigint;primaryKey;comment:http请求日志表" json:"id"`                                     // http请求日志表
	Name       string       `gorm:"column:name;type:character varying(255);not null;comment:域名" json:"name"`                          // 域名
	Info       string       `gorm:"column:info;type:json;not null;comment:日志内容" json:"info"`                                          // 日志内容
	Status     string       `gorm:"column:status;type:character varying(20);not null;comment:请求状态 success failure" json:"status"`     // 请求状态 success failure
	CreateTime cm.LocalTime `gorm:"column:create_time;type:int;type:unsigned;not null;autoCreateTime;comment:请求时间" json:"createTime"` // 请求时间
}

// TableName GfnCollectorLogHTTP's table name
func (*GfnCollectorLogHTTP) TableName() string {
	return TableNameGfnCollectorLogHTTP
}
