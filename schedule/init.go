package schedule

import (
	pingService "github.com/GoFurry/gofurry-nav-collector/collector/ping/service"
	"github.com/GoFurry/gofurry-nav-collector/common/log"
)

func InitSchedule() {
	defer func() {
		if err := recover(); err != nil {
			log.Error(err)
		}
	}()

	pingService.InitPingOnStart() // ping
	//httpService.InitHTTPOnStart() // http
	//dnsService.InitDNSOnStart()   // dns
}
