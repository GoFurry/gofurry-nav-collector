package dao

import (
	"github.com/GoFurry/gofurry-nav-collector/collector/http/models"
	"github.com/GoFurry/gofurry-nav-collector/common"
	"github.com/GoFurry/gofurry-nav-collector/common/abstract"
)

var newHTTPDao = new(httpDao)

func init() {
	newHTTPDao.Init()
}

type httpDao struct{ abstract.Dao }

func GetHTTPDao() *httpDao { return newHTTPDao }

func (dao httpDao) GetList() ([]models.GfnCollectorDomain, common.GFError) {
	var res []models.GfnCollectorDomain
	db := dao.Gm.Table(models.TableNameGfnCollectorDomain)
	db.Find(&res)
	if err := db.Error; err != nil {
		return nil, common.NewDaoError(err.Error())
	}
	return res, nil
}

// 保留 count 条request历史记录
func (dao httpDao) DeleteByNum(count string) (int64, common.GFError) {
	sql := `
		DELETE FROM ` + models.TableNameGfnCollectorLogHTTP + `
		WHERE id NOT IN (
		  SELECT id
		  FROM (
			SELECT 
			  id,
			  ROW_NUMBER() OVER (
				PARTITION BY name 
				ORDER BY create_time DESC
			  ) AS rn
			FROM ` + models.TableNameGfnCollectorLogHTTP + `
		  ) AS ranked
		  WHERE rn <= ?
		);`

	db := dao.Gm.Table(models.TableNameGfnCollectorLogHTTP)
	result := db.Exec(sql, count)
	if err := db.Error; err != nil {
		return result.RowsAffected, common.NewDaoError(err.Error())
	}

	return result.RowsAffected, nil
}
