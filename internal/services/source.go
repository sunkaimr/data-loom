package services

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"gorm.io/gorm"
	"sort"
	"strconv"
	"strings"
	"time"
)

type SourceService struct {
	// 源端信息
	Model
	Name         string `json:"name"`          // 源端名称
	Description  string `json:"description"`   // 说明
	Bu           string `json:"bu"`            // 资产BU
	ClusterName  string `json:"cluster_name"`  // 集群名称
	ClusterID    string `json:"cluster_id" `   // 集群ID
	DatabaseName string `json:"database_name"` // 源库名
	TablesName   string `json:"tables_name"`   // 源表名
}

func (c *SourceService) CreateSource(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	source := c.ServiceToModel()
	source.CreatedAt = time.Now()
	source.Creator = u.UserName

	//var count int64
	//err := db.Model(source).Where("name =?", source.Name).Count(&count).Error
	//if err != nil {
	//	err = fmt.Errorf("query models.Source(name=%s) from db failed, %s", source.Name, err)
	//	log.Error(err)
	//	return common.CodeServerErr, err
	//}
	//if count != 0 {
	//	err = fmt.Errorf("models.Source(name=%s) exist", source.Name)
	//	log.Error(err)
	//	return common.CodeSourceExist, err
	//}

	// 须确保同一个集群、库、表(ClusterID,DatabaseName,TablesName)只能有一个源，避免源滥用

	var existSources []models.Source
	err := db.Model(source).Where("cluster_id =? AND database_name =? AND tables_name =?",
		source.ClusterID, source.DatabaseName, source.TablesName).Find(&existSources).Error
	if err != nil {
		err = fmt.Errorf("query models.Source(cluster_id=%v AND database_name=%v AND tables_name=%v) from db failed, %s",
			source.ClusterID, source.DatabaseName, source.TablesName, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	// 如果不存在则则创建
	if len(existSources) > 0 {
		source = &existSources[0]
		c.ModelToService(source)
		return common.CodeSourceExist, nil
	} else {
		err = db.Save(source).Error
		if err != nil {
			err = fmt.Errorf("save models.Source(%+v) to db failed, %s", source, err)
			log.Error(err)
			return common.CodeServerErr, err
		}

		err = db.Model(source).Where("cluster_id =? AND database_name =? AND tables_name =?",
			source.ClusterID, source.DatabaseName, source.TablesName).First(&source).Error
		if err != nil {
			err = fmt.Errorf("query models.Source(cluster_id=%v AND database_name=%v AND tables_name=%v) from db failed, %s",
				source.ClusterID, source.DatabaseName, source.TablesName, err)
			log.Error(err)
			return common.CodeServerErr, err
		}
		c.ModelToService(source)
		return common.CodeOK, nil
	}
}

func (c *SourceService) UpdateSource(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	source := &models.Source{}
	err := db.Model(&source).First(source, "id = ?", c.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.Source(id=%d) not exist", source.ID)
			log.Error(err)
			return common.CodeSourceNotExist, err
		}
		err = fmt.Errorf("query models.Source(id=%d) from db failed, %s", source.ID, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	var count int64
	err = db.Model(source).Where("name =? AND id !=?", c.Name, c.ID).Count(&count).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		err = fmt.Errorf("query models.Source(name =%v AND id !=%v) from db failed, %s", c.Name, c.ID, err)
		log.Error(err)
		return common.CodeServerErr, err
	}
	if count != 0 {
		err = fmt.Errorf("models.Source(name=%s) exist", c.Name)
		log.Error(err)
		return common.CodeSourceNameConflict, err
	}

	source.Name = c.Name
	source.Description = c.Description
	source.Editor = u.UserName
	c.ModelToService(source)

	err = db.Save(source).Error
	if err != nil {
		err = fmt.Errorf("update models.Source(%+v) from db failed, %s", source, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	return common.CodeOK, nil
}

func (c *SourceService) DeleteSource(ctx *gin.Context) (common.ServiceCode, error) {
	var err error
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	source := models.Source{}

	// 若ID有效则以ID为准, 否则以name为准
	if !common.InvalidUintID(c.ID) {
		err = db.Model(&source).First(&source, "id = ?", c.ID).Error
	} else {
		err = db.Model(&source).First(&source, "name = ?", c.Name).Error
	}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.Source(%v|%v) not exist", c.ID, c.Name)
			log.Error(err)
			return common.CodeSourceNotExist, err
		}
		err = fmt.Errorf("query models.Source(%v|%v) from db failed, %s", c.ID, c.Name, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	// 校验源端是否被策略引用，如果引用不可删除
	var polices []models.Policy
	err = db.Model(&models.Policy{}).Select("name").Where("src_id =?", source.ID).Find(&polices).Error
	if err != nil {
		err = fmt.Errorf("query models.Job(src_id=%d) from db failed, %s", source.ID, err)
		log.Error(err)
		return common.CodeServerErr, err
	}
	if len(polices) != 0 {
		err = fmt.Errorf("source(%s) has been used form job(%s)", source.Name, polices[0].Name)
		log.Error(err)
		return common.CodeSourceUsing, err
	}

	db.Model(&models.Connection{}).Where("id =?", source.ID).Update("editor", u.UserName)
	err = db.Delete(&models.Source{}, "id =?", source.ID).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		err = fmt.Errorf("delete models.Source(name=%s) from db failed, %s", source.Name, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	return common.CodeOK, nil
}

func (c *SourceService) QuerySource(ctx *gin.Context, queryMap map[string]string) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	res, err := common.NewPageList[[]models.Source](db).
		QueryPaging(ctx).
		Order("id desc").
		Query(
			common.FilterFuzzyStringMap(queryMap),
			common.FilterID(c.ID),
		)
	if err != nil {
		log.Errorf("query source from db faield, %s", err)
		return nil, common.CodeServerErr, err
	}

	ret := common.NewPageList[[]SourceService](db)
	ret.Page = res.Page
	ret.PageSize = res.PageSize
	ret.Total = res.Total
	for i := range res.Items {
		s := &SourceService{}
		s.ModelToService(&res.Items[i])
		ret.Items = append(ret.Items, *s)
	}

	return ret, common.CodeOK, nil
}

func (c *SourceService) CheckParameters(ctx *gin.Context) (bool, common.ServiceCode, error) {
	// 检验源端名字长度
	if len(c.ClusterID) == 0 {
		return false, common.CodeSourceClusterIDNull, fmt.Errorf("cluster_id(%s) cannot be empty", c.ClusterID)
	}

	// 库名
	c.DatabaseName = utils.TrimmingStringList(c.DatabaseName, ",")
	if len(c.DatabaseName) == 0 {
		return false, common.CodeSourceDatabaseNull, fmt.Errorf("database_name(%s) can not be empty", c.DatabaseName)
	}

	c.TablesName = utils.TrimmingStringList(c.TablesName, ",")
	if len(c.TablesName) == 0 {
		return false, common.CodeSourceTableNameNull, fmt.Errorf("tables_name(%s) can not be empty", c.TablesName)
	}

	// TODO 校验用户对集群的操作权限

	clusterSvc, err := GetClusterServiceByClusterID(ctx, c.ClusterID)
	if err != nil {
		return false, common.CodeServerErr, err
	}
	c.ClusterName = clusterSvc.ClusterName
	c.Bu = clusterSvc.Bu

	// 表名支持一个表名，多个表时只支持同一个表的分表场景
	_, baseName, err := common.CheckSameShardingTables(c.TablesName)
	if err != nil {
		return false, common.CodeSourceTableNameErr, fmt.Errorf("check source.table not pass, %s", err)
	}

	// 对表进行排序
	reqTableList := strings.Split(c.TablesName, ",")
	sort.Slice(reqTableList, func(i, j int) bool {
		name1 := strings.TrimPrefix(reqTableList[i], baseName+"_")
		name2 := strings.TrimPrefix(reqTableList[j], baseName+"_")

		num1, _ := strconv.Atoi(name1)
		num2, _ := strconv.Atoi(name2)

		return num1 < num2
	})
	c.TablesName = strings.Join(reqTableList, ",")

	clusterDriver := NewClusterDriver(clusterSvc)

	// 库名存在
	databases, res, err := clusterDriver.GetDatabases(ctx)
	if err != nil {
		return false, res, fmt.Errorf("get cluster(%s) databases failed, %s", clusterSvc.ClusterID, err)
	}
	databases = utils.RemoveSubSlices(databases, strings.Split(Cfg.ClusterExcludeDatabase, ","))

	if !utils.ElementExist(c.DatabaseName, databases) {
		return false, common.CodeSourceDatabaseNotExist, fmt.Errorf("got database(%v) not in reality cluster(%v)", c.DatabaseName, databases)
	}

	// 表存在
	tables, res, err := clusterDriver.GetTables(ctx, c.DatabaseName)
	if err != nil {
		return false, res, fmt.Errorf("get cluster(%s) databases(%s) tabels failed", clusterSvc.ClusterID, c.DatabaseName)
	}
	tables = common.ExcludeTablesFilter(c.DatabaseName, tables, strings.Split(Cfg.ClusterExcludeTables, ","))

	if !utils.IsSubSlices(reqTableList, tables) {
		return false,
			common.CodeSourceTableNotExist,
			fmt.Errorf("got tables(%v) not in reality cluster(%s) tables(%v)", strings.Split(c.TablesName, ","), clusterSvc.ClusterID, tables)
	}

	// 检查表是否都有主键
	_, res, err = clusterDriver.TablesHasPrimaryKey(ctx, c.DatabaseName, reqTableList)
	if err != nil {
		return false, res, fmt.Errorf("check cluster(%s) databases(%s) primary key no pass, %s", clusterSvc.ClusterID, c.DatabaseName, err)
	}

	// 检查磁盘剩余空间
	_, err = clusterDriver.GetFreeDisk(ctx)
	if err != nil {
		return false, common.CodeClusterFreeDiskErr, fmt.Errorf("check cluster(%s) freedisk no pass, %s", clusterSvc.ClusterID, err)
	}

	if c.Name == "" {
		c.Name = fmt.Sprintf("%s_%s_%s", clusterSvc.ClusterName, c.DatabaseName, baseName)
	}

	return true, common.CodeOK, nil
}

func (c *SourceService) ServiceToModel() *models.Source {
	m := &models.Source{}
	m.ID = c.ID
	m.Creator = c.Creator
	m.Editor = c.Editor
	m.CreatedAt, _ = time.ParseInLocation(time.DateTime, c.CreatedAt, time.Now().Location())
	m.UpdatedAt, _ = time.ParseInLocation(time.DateTime, c.UpdatedAt, time.Now().Location())
	m.Name = c.Name
	m.Description = c.Description
	m.Bu = c.Bu
	m.ClusterName = c.ClusterName
	m.ClusterID = c.ClusterID
	m.DatabaseName = c.DatabaseName
	m.TablesName = c.TablesName
	return m
}

func (c *SourceService) ModelToService(m *models.Source) *SourceService {
	c.ID = m.ID
	c.Creator = m.Creator
	c.Editor = m.Editor
	c.CreatedAt = m.CreatedAt.Format(time.DateTime)
	c.UpdatedAt = m.UpdatedAt.Format(time.DateTime)
	c.Name = m.Name
	c.Description = m.Description
	c.Bu = m.Bu
	c.ClusterName = m.ClusterName
	c.ClusterID = m.ClusterID
	c.DatabaseName = m.DatabaseName
	c.TablesName = m.TablesName
	return c
}
