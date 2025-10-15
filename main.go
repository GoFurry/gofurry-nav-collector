package main

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/GoFurry/gofurry-nav-collector/common"
	"github.com/GoFurry/gofurry-nav-collector/common/log"
	cs "github.com/GoFurry/gofurry-nav-collector/common/service"
	"github.com/GoFurry/gofurry-nav-collector/roof/env"
	"github.com/GoFurry/gofurry-nav-collector/schedule"
	"github.com/kardianos/service"
)

//@title GoFurry-Collector
//@version v1.0.0
//@description Collector for GoFurry Nav Page

var (
	errChan = make(chan error)
)

func main() {
	svcConfig := &service.Config{
		Name:        common.COMMON_PROJECT_NAME,
		DisplayName: "gf-nav-collector",
		Description: "gf-nav-collector",
	}
	prg := &goFurry{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Error(err)
	}

	if len(os.Args) > 1 {
		if os.Args[1] == "install" {
			s.Install()
			log.Info("服务安装成功.")
			return
		}

		if os.Args[1] == "uninstall" {
			s.Uninstall()
			log.Info("服务卸载成功.")
			return
		}

		if os.Args[1] == "version" {
			log.Info("Ping V1.0.0")
			return
		}
	}

	// 内存限制和 GC 策略
	debug.SetGCPercent(1000)
	debug.SetMemoryLimit(int64(env.GetServerConfig().Server.MemoryLimit << 30))

	InitOnStart()

	// 启动系统
	err = s.Run()
	if err != nil {
		log.Error(err)
	}
}

func InitOnStart() {
	// 初始化 redis
	cs.InitRedisOnStart()
	// 初始化时间调度
	cs.InitTimeWheelOnStart()
}

type goFurry struct{}

func (gf *goFurry) Start(s service.Service) error {
	go gf.run()
	return nil
}

func (gf *goFurry) run() {
	// 启动 collector
	go func() {
		// 初始化 collector
		fmt.Println("gf-nav-collector已启动...")
		schedule.InitSchedule()
	}()
}

func (gf *goFurry) Stop(s service.Service) error {
	return nil
}
