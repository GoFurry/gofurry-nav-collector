package dao

import (
	"github.com/GoFurry/gofurry-nav-collector/collector/ping/models"
	"github.com/GoFurry/gofurry-nav-collector/common"
	"github.com/GoFurry/gofurry-nav-collector/common/abstract"
)

var newPingDao = new(pingDao)

func init() {
	newPingDao.Init()
}

type pingDao struct{ abstract.Dao }

func GetPingDao() *pingDao { return newPingDao }

// 获取站点列表
func (dao pingDao) GetList() ([]models.Domain, common.GFError) {
	var res []models.Domain
	db := dao.Gm.Table(models.TableNameGfnSite).Select("domain")
	db.Find(&res)
	if err := db.Error; err != nil {
		return nil, common.NewDaoError(err.Error())
	}
	return res, nil
}

// 保留 count 条ping历史记录
func (dao pingDao) DeleteByNum(count string) (int64, common.GFError) {
	sql := `
		DELETE FROM ` + models.TableNameGfnCollectorLogPing + `
		WHERE id NOT IN (
		  SELECT id
		  FROM (
			SELECT 
			  id,
			  ROW_NUMBER() OVER (
				PARTITION BY name 
				ORDER BY create_time DESC
			  ) AS rn
			FROM ` + models.TableNameGfnCollectorLogPing + `
		  ) AS ranked
		  WHERE rn <= ?
		);`

	db := dao.Gm.Table(models.TableNameGfnCollectorLogPing)
	result := db.Exec(sql, count)
	if err := db.Error; err != nil {
		return result.RowsAffected, common.NewDaoError(err.Error())
	}

	return result.RowsAffected, nil
}
