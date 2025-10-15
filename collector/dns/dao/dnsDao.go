package dao

import (
	"github.com/GoFurry/gofurry-nav-collector/collector/dns/models"
	"github.com/GoFurry/gofurry-nav-collector/common"
	"github.com/GoFurry/gofurry-nav-collector/common/abstract"
)

var newDNSDao = new(dnsDao)

func init() {
	newDNSDao.Init()
}

type dnsDao struct{ abstract.Dao }

func GetDNSDao() *dnsDao { return newDNSDao }

func (dao dnsDao) GetList() ([]models.GfnCollectorDomain, common.GFError) {
	var res []models.GfnCollectorDomain
	db := dao.Gm.Table(models.TableNameGfnCollectorDomain)
	db.Find(&res)
	if err := db.Error; err != nil {
		return nil, common.NewDaoError(err.Error())
	}
	return res, nil
}

// 保留 count 条request历史记录
func (dao dnsDao) DeleteByNum(count string) (int64, common.GFError) {
	sql := `
		DELETE FROM ` + models.TableNameGfnCollectorLogDn + `
		WHERE id NOT IN (
		  SELECT id
		  FROM (
			SELECT 
			  id,
			  ROW_NUMBER() OVER (
				PARTITION BY name 
				ORDER BY create_time DESC
			  ) AS rn
			FROM ` + models.TableNameGfnCollectorLogDn + `
		  ) AS ranked
		  WHERE rn <= ?
		);`

	db := dao.Gm.Table(models.TableNameGfnCollectorLogDn)
	result := db.Exec(sql, count)
	if err := db.Error; err != nil {
		return result.RowsAffected, common.NewDaoError(err.Error())
	}

	return result.RowsAffected, nil
}
