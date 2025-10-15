package service

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/GoFurry/gofurry-nav-collector/collector/ping/dao"
	models2 "github.com/GoFurry/gofurry-nav-collector/collector/ping/models"
	"github.com/GoFurry/gofurry-nav-collector/common"
	"github.com/GoFurry/gofurry-nav-collector/common/log"
	cm "github.com/GoFurry/gofurry-nav-collector/common/models"
	cs "github.com/GoFurry/gofurry-nav-collector/common/service"
	cu "github.com/GoFurry/gofurry-nav-collector/common/util"
	"github.com/GoFurry/gofurry-nav-collector/roof/env"
	"github.com/go-ping/ping"
	"github.com/sourcegraph/conc/pool"
)

var pingThread = pool.New().WithMaxGoroutines(env.GetServerConfig().Collector.Ping.PingThread)
var pingRWLock sync.RWMutex
var wg sync.WaitGroup

// ============== Ping模块 - 初始化部分 ==============

// 初始化
func InitPingOnStart() {
	defer func() {
		if err := recover(); err != nil {
			log.Error(fmt.Sprintf("receive InitPingOnStart recover: %v", err))
		}
	}()
	fmt.Println("Ping 模块初始化开始...")

	// 查询数据库所有 IP 存 redis
	err := addAllIpToPing()
	if err != nil {
		return
	}
	//初始化后执行一次 Ping
	go Ping()
	// 定时任务执行 Ping
	cs.AddCronJob(time.Duration(env.GetServerConfig().Collector.Ping.PingInterval)*time.Second, Ping)

	fmt.Println("Ping 模块初始化结束...")
}

// 添加数据库全部 IP 到 redis
func addAllIpToPing() common.GFError {
	// 查记录
	domainRecords, err := dao.GetPingDao().GetList()
	if err != nil {
		log.Error(fmt.Sprintf("查询IP失败: %v", err.GetMsg()))
		return common.NewServiceError(fmt.Sprintf("查询IP失败: %v", err))
	}

	// 添加 ping 的站点
	var pingList = []string{}
	for _, v := range domainRecords {
		newDomains := models2.Domains{}
		if jsonErr := json.Unmarshal([]byte(v.Domain), &newDomains); jsonErr != nil {
			log.Error(fmt.Sprintf("json转换失败: %v", jsonErr))
			return nil
		}
		for _, domain := range newDomains.Domain {
			pingList = append(pingList, domain)
		}
	}

	// 存入 redis
	pingJsonList, jsonErr := json.Marshal(pingList)
	if jsonErr != nil {
		log.Error(fmt.Sprintf("json转换失败: %v", jsonErr))
		return nil
	}

	err = cs.Del(env.GetServerConfig().Collector.Ping.PingKey)
	if err != nil {
		log.Error("删除ping结果失败: ", err)
		return err
	}

	cs.SetNX(env.GetServerConfig().Collector.Ping.PingKey, pingJsonList, -1)

	return nil
}

// ============== Ping解析 - 执行部分 ==============

// 检测是否在线
func Ping() {
	defer func() {
		if err := recover(); err != nil {
			log.Error(fmt.Sprintf("receive Ping recover: %v", err))
		}
	}()
	// redis 中获取 ping 的站点列表
	var pingKey = env.GetServerConfig().Collector.Ping.PingKey
	domains, err := cs.GetString(pingKey)
	if err != nil {
		log.Error("Ping 获取站点列表失败: " + err.GetMsg())
		return
	}
	// 判空
	if domains == "" || len(domains) < 1 {
		log.Info("Ping 站点列表为空")
		return
	}

	// redis 中获取旧记录
	var resultKey = env.GetServerConfig().Collector.Ping.ResultKey
	data, err := cs.HGetAll(resultKey)
	if err != nil {
		log.Error("获取旧记录失败")
		return
	}
	// 判空
	if data == nil || len(data) < 1 {
		data = map[string]string{}
	}

	var pingList = []string{}
	if jsonErr := json.Unmarshal([]byte(domains), &pingList); jsonErr != nil {
		log.Error(fmt.Sprintf("json转换失败: %v", jsonErr))
		return
	}

	// 复制旧记录中在站点列表中的部分到新纪录
	var nowData = map[string]string{}
	for _, domain := range pingList {
		nowData[domain] = data[domain]
	}

	log.Info("Ping 采集开始")
	// 遍历 IP 列表, 每个 IP 开一个线程执行 Ping
	for _, v := range pingList {
		wg.Add(1)
		pingThread.Go(getPingResult(v, nowData))
	}
	// 等待所有 Ping 执行完毕
	wg.Wait()
	log.Info("Ping 采集结束")

	// 删除旧记录中不在站点列表中的部分
	var deleteList = []string{}
	for k, _ := range data {
		if nowData[k] == "" {
			deleteList = append(deleteList, k)
		}
	}
	if cap(deleteList) != 0 {
		count, delErr := cs.HDel(resultKey, deleteList...)
		if delErr != nil {
			log.Error("删除ping结果失败: ", delErr)
		}
		if count > 0 {
			log.Info("删除站点ping记录: ", count)
		}
	}
	// Ping 结果储存回 redis
	err = cs.HSetMap(resultKey, nowData)
	if err != nil {
		log.Error("存储ping结果失败: ", err)
	}
	log.Info("ping结果储存成功")

	// 每个域名仅保留 5000 条 ping 记录
	count, deleteErr := dao.GetPingDao().DeleteByNum(env.GetServerConfig().Collector.Ping.LogCount)
	if deleteErr != nil {
		log.Error("删除多余Ping记录失败: ", deleteErr)
	} else {
		log.Info("删除多余Ping记录成功, 共删除: ", count)
	}
}

// ============== Ping解析 - 采集和解析部分 ==============

// 执行 ping 采集
func performPing(ip string) models2.PingModel {
	pinger, err := ping.NewPinger(ip)
	defer pinger.Stop()
	// 初始化结果字段
	var pingModel models2.PingModel
	pingModel.PingTime = cm.LocalTime(time.Now())
	if err != nil {
		return pingModel
	}
	pingModel.AvgLossRate = 100
	pingModel.AvgDelayTime = 100000000
	if err != nil {
		return pingModel
	}
	// 初始化 pinger
	pinger.Count = 5
	pinger.Size = 64
	pinger.Interval = time.Second
	pinger.Timeout = time.Second * 5
	pinger.SetPrivileged(true)
	// 运行 Pinger
	err = pinger.Run()
	if err != nil {
		return pingModel
	}
	// 转换数据
	stats := pinger.Statistics()
	pingModel.AvgLossRate = stats.PacketLoss
	pingModel.AvgDelayTime = stats.AvgRtt.Milliseconds()
	if pingModel.AvgDelayTime == 0 && pingModel.AvgLossRate != 100 {
		pingModel.AvgDelayTime = 1
	}
	return pingModel
}

// 解析 ping 采集结果
func getPingResult(ip string, data map[string]string) func() {
	return func() {
		defer func() {
			if err := recover(); err != nil {
				log.Error(fmt.Sprintf("receive PingThread recover: %v", err))
			}
		}()
		defer wg.Done() // 确保线程结束时组数减少

		// 执行 Ping 获取结果
		result := performPing(ip)

		pingRecord := &models2.PingSaveModel{}
		pingRecord.Time = result.PingTime
		pingRecord.Delay = cu.Int642String(result.AvgDelayTime) + "ms"
		pingRecord.Loss = cu.Float642String(result.AvgLossRate)
		if result.AvgLossRate < 99 && result.AvgDelayTime > 0 {
			pingRecord.Status = "up"
		} else {
			pingRecord.Status = "down"
		}
		// 序列化为 json
		jsonResult, _ := json.Marshal(pingRecord)

		// 存数据库
		pindSaveRecord := &models2.GfnCollectorLogPing{
			ID:         cu.GenerateId(),
			Name:       ip,
			Delay:      cu.Int642String(result.AvgDelayTime) + "ms",
			Loss:       cu.Float642String(result.AvgLossRate),
			CreateTime: result.PingTime,
		}
		if result.AvgLossRate < 99 && result.AvgDelayTime > 0 {
			pindSaveRecord.Status = "up"
		} else {
			pindSaveRecord.Status = "down"
		}

		// 开启读写锁
		pingRWLock.Lock()
		defer pingRWLock.Unlock()
		// 更新字典
		data[ip] = string(jsonResult)

		// 存数据库
		dao.GetPingDao().Add(pindSaveRecord)
	}
}
