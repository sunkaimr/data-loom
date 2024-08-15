package services

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/configs"
	"github.com/sunkaimr/data-loom/internal/cluster"
	"github.com/sunkaimr/data-loom/internal/cluster/driver/mysql"
	driverun "github.com/sunkaimr/data-loom/internal/cluster/driver/unsupported"
	syncun "github.com/sunkaimr/data-loom/internal/cluster/sync/unsupported"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"gorm.io/gorm"
	"strings"
	"sync"
	"time"
)

type ClusterService struct {
	Model
	ClusterID   string                       `json:"cluster_id"`   // 集群ID
	ClusterName string                       `json:"cluster_name"` // 集群名称
	Description string                       `json:"description"`  // 说明
	Bu          string                       `json:"bu"`           // bu
	Env         string                       `json:"env"`          // 环境
	ImportFrom  common.ClusterImportFromType `json:"import_from"`  // 添加方式：customized:自定义添加 ipaas:从资源中心导入
	ClusterType common.ClusterType           `json:"cluster_type"` // 集群类型（mysql, 其他的）
	ServiceAddr string                       `json:"service_addr"` // 服务地址(vip) ip:port
	WriteAddr   string                       `json:"write_addr"`   // 写库地址 ip:port
	ReadAddr    string                       `json:"read_addr"`    // 读库地址 ip:port
	UserName    string                       `json:"user_name"`    // 用户名（若未提供用户名和密码则以全局的为准）
	Password    string                       `json:"password"`     // 密码
}

func NewClusterDriver(c *ClusterService) cluster.ClusterDriver {
	switch c.ClusterType {
	case common.ClusterTypeMysql:
		return &mysql.ClusterMysql{
			ServiceAddr: c.ServiceAddr,
			WriteAddr:   c.WriteAddr,
			ReadAddr:    c.ReadAddr,
			UserName:    c.UserName,
			Password:    c.Password,
		}
	default:
		return &driverun.ClusterUnknown{}
	}
}

func NewClusterSynchronizer(c *ClusterService) cluster.ClusterSynchronizer {
	switch c.ImportFrom {
	default:
		return &syncun.Sync{}
	}
}

func GetClusterServiceByClusterID(ctx *gin.Context, clusterID string) (*ClusterService, error) {
	_, db := common.ExtractContext(ctx)

	clusterMod := &models.Cluster{}
	err := db.Model(clusterMod).First(clusterMod, "cluster_id = ?", clusterID).Error
	if err != nil {
		return nil, fmt.Errorf("query models.ClusterDriver(cluster_id=%v) from db failed, %s", clusterID, err)
	}
	clusterSvc := &ClusterService{}
	clusterSvc.ModelToService(clusterMod)
	return clusterSvc, nil
}

func (c *ClusterService) CheckParameters(ctx *gin.Context) (bool, common.ServiceCode, error) {
	// 检验源端名字长度
	if len(c.ClusterName) == 0 || len(c.ClusterName) >= 64 {
		return false, common.CodeClusterNameErr, fmt.Errorf("clusetr_name(%s) lenth should be 1-64", c.ClusterName)
	}
	if len(c.ClusterID) == 0 || len(c.ClusterID) >= 64 {
		return false, common.CodeClusterIDErr, fmt.Errorf("ClusterID(%s) lenth should be 1-64", c.ClusterID)
	}

	c.ImportFrom = common.ImportFromCustomized
	if c.ServiceAddr == "" {
		return false, common.CodeClusterServiceAddrErr, fmt.Errorf("ServiceAddr(%s) lenth should be 1-1024", c.ServiceAddr)
	}

	switch c.ClusterType {
	case common.ClusterTypeMysql:
		// 暂时仅支持mysql
	default:
		return false, common.CodeClusterUnsupportedClusterType, fmt.Errorf("unsupporte ClusterType(%s)", c.ClusterType)
	}

	// 如果用户提供了密码则对密码进行加密
	if c.Password != "" {
		var err error
		c.Password, err = utils.EncryptByAES(c.Password, configs.C.Jwt.Secret)
		if err != nil {
			return false, common.CodeEncryptPasswdErr, fmt.Errorf("encrypt passwd failed, %s", err)
		}
	}

	// 校验集群的地址用户名及密码
	code, err := NewClusterDriver(c).TestConnect(ctx)
	if err != nil {
		return false, code, err
	}

	return true, common.CodeOK, nil
}

func (c *ClusterService) CreateCluster(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	clusterMod := c.ServiceToModel()
	clusterMod.CreatedAt = time.Now()
	clusterMod.Creator = u.UserName

	var count int64
	err := db.Model(clusterMod).Where("cluster_name=? OR cluster_id =?", clusterMod.ClusterName, clusterMod.ClusterID).Count(&count).Error
	if err != nil {
		err = fmt.Errorf("query models.Source(cluster_name=%s OR cluster_id=%s) from db failed, %s", clusterMod.ClusterName, clusterMod.ClusterID, err)
		log.Error(err)
		return common.CodeServerErr, err
	}
	if count != 0 {
		err = fmt.Errorf("models.Source(cluster_name=%s OR cluster_id=%s) exist", clusterMod.ClusterName, clusterMod.ClusterID)
		log.Error(err)
		return common.CodeClusterExisted, err
	}

	err = db.Save(clusterMod).Error
	if err != nil {
		err = fmt.Errorf("save models.Source(%+v) to db failed, %s", clusterMod, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	c.ModelToService(clusterMod)

	return common.CodeOK, nil
}

func (c *ClusterService) CheckUpdateParameters(ctx *gin.Context) (bool, common.ServiceCode, error) {
	var err error
	log, db := common.ExtractContext(ctx)

	if common.InvalidUintID(c.ID) {
		return false, common.CodeInvalidID, fmt.Errorf("invalid ClusterDriver.ID(%d)", c.ID)
	}

	if len(c.ClusterName) == 0 || len(c.ClusterName) >= 64 {
		return false, common.CodeClusterNameErr, fmt.Errorf("clusetr_name(%s) lenth should be 1-64", c.ClusterName)
	}

	clusterMod := &models.Cluster{}
	err = db.Model(clusterMod).First(clusterMod, "id = ?", c.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.ClusterDriver(id=%d) not exist", c.ID)
			log.Error(err)
			return false, common.CodeClusterNotExist, err
		}
		err = fmt.Errorf("query models.ClusterDriver(id=%d) from db failed, %s", c.ID, err)
		log.Error(err)
		return false, common.CodeServerErr, err
	}

	if clusterMod.ImportFrom != common.ImportFromCustomized {
		err = fmt.Errorf("cluster(%v) import from %s is unchangeable", clusterMod.ClusterName, clusterMod.ImportFrom)
		log.Error(err)
		return false, common.CodeClusterSyncImmutable, err
	}

	// 可更新字段
	// ClusterName, Description, Bu, Env, UserName, Password
	var count int64
	err = db.Model(clusterMod).Where("id !=? AND cluster_name =?", c.ID, c.ClusterName).Count(&count).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		err = fmt.Errorf("query models.ClusterDriver(id!=%v AND cluster_name=%v)) from db failed, %s", c.ID, c.ClusterName, err)
		log.Error(err)
		return false, common.CodeServerErr, err
	}
	if count != 0 {
		err = fmt.Errorf("models.ClusterDriver(cluster_name=%v) exist", c.ClusterName)
		log.Error(err)
		return false, common.CodeClusterExisted, err
	}

	if c.Password == "" {
		// 1, 用户没有提供密码以库里为准
		c.Password = clusterMod.Password
	} else if c.Password != clusterMod.Password {
		// 2, 如果用户提供了密码, 判断密码和库里是否一致若不一致加密后更新
		c.Password, err = utils.EncryptByAES(c.Password, configs.C.Jwt.Secret)
		if err != nil {
			return false, common.CodeEncryptPasswdErr, fmt.Errorf("encrypt passwd failed, %s", err)
		}
	}

	// 校验集群的地址用户名及密码
	code, err := NewClusterDriver(c).TestConnect(ctx)
	if err != nil {
		return false, code, err
	}

	return true, common.CodeOK, nil
}

func (c *ClusterService) UpdateCluster(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	clusterMod := &models.Cluster{}
	err := db.Model(clusterMod).First(clusterMod, "id = ?", c.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.ClusterDriver(id=%d) not exist", clusterMod.ID)
			log.Error(err)
			return common.CodeSourceNotExist, err
		}
		err = fmt.Errorf("query models.ClusterDriver(id=%d) from db failed, %s", clusterMod.ID, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	clusterMod.Editor = u.UserName
	clusterMod.ClusterName = c.ClusterName
	clusterMod.Description = c.Description
	clusterMod.Bu = c.Bu
	clusterMod.Env = c.Env
	clusterMod.UserName = c.UserName
	clusterMod.Password = c.Password

	c.ModelToService(clusterMod)

	err = db.Save(clusterMod).Error
	if err != nil {
		err = fmt.Errorf("update models.Source(%+v) from db failed, %s", clusterMod, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	return common.CodeOK, nil
}

func (c *ClusterService) DeleteCluster(ctx *gin.Context) (common.ServiceCode, error) {
	var err error
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	clusterMod := models.Cluster{}

	// 若ID有效则以ID为准, 否则以name为准
	if !common.InvalidUintID(c.ID) {
		err = db.Model(&clusterMod).First(&clusterMod, "id = ?", c.ID).Error
	} else if c.ClusterID != "" {
		err = db.Model(&clusterMod).First(&clusterMod, "cluster_id = ?", c.ClusterID).Error
	} else if c.ClusterName != "" {
		err = db.Model(&clusterMod).First(&clusterMod, "cluster_name = ?", c.ClusterName).Error
	} else {
		return common.CodeClusterNameAndIDErr, err
	}

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.ClusterDriver(%v|%v|%v) not exist", c.ID, c.ClusterID, c.ClusterName)
			log.Error(err)
			return common.CodeClusterNotExist, err
		}
		err = fmt.Errorf("query models.ClusterDriver(%v|%v|%v) from db failed, %s", c.ID, c.ClusterID, c.ClusterName, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	// 校验源端是否被策略引用，如果引用不可删除
	var source []models.Source
	err = db.Model(&models.Source{}).Select("name").Where("cluster_id =?", clusterMod.ClusterID).Find(&source).Error
	if err != nil {
		err = fmt.Errorf("query models.ClusterDriver(cluster_id=%v) from db failed, %s", clusterMod.ClusterID, err)
		log.Error(err)
		return common.CodeServerErr, err
	}
	if len(source) != 0 {
		err = fmt.Errorf("cluster(%s) has been used form (%s)", clusterMod.ClusterID, source[0].Name)
		log.Error(err)
		return common.CodeClusterUsing, err
	}

	db.Model(&models.Cluster{}).Where("id =?", clusterMod.ID).Update("editor", u.UserName)
	err = db.Delete(&models.Cluster{}, "id =?", clusterMod.ID).Error
	if err != nil {
		err = fmt.Errorf("delete models.Cluste(%s) from db failed, %s", clusterMod.ClusterName, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	return common.CodeOK, nil
}

func (c *ClusterService) QueryCluster(ctx *gin.Context, queryMap map[string]string) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	res, err := common.NewPageList[[]models.Cluster](db).
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

	// TODO 筛选用户有权限的集群地址

	ret := common.NewPageList[[]ClusterService](db)
	ret.Page = res.Page
	ret.PageSize = res.PageSize
	ret.Total = res.Total
	for i := range res.Items {
		s := &ClusterService{}
		s.ModelToService(&res.Items[i])
		ret.Items = append(ret.Items, *s)
	}

	return ret, common.CodeOK, nil
}

func (c *ClusterService) GetClusterDatabase(ctx *gin.Context) ([]string, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	clusterMod := &models.Cluster{}
	err := db.Model(clusterMod).First(clusterMod, "cluster_id = ?", c.ClusterID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.ClusterDriver(cluster_id=%v) not exist", c.ClusterID)
			log.Error(err)
			return nil, common.CodeClusterNotExist, err
		}

		err = fmt.Errorf("query models.ClusterDriver(cluster_id=%v) failed, %v", c.ClusterID, err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}
	c.ModelToService(clusterMod)
	databases, code, err := NewClusterDriver(c).GetDatabases(ctx)
	databases = utils.RemoveSubSlices(databases, strings.Split(Cfg.ClusterExcludeDatabase, ","))

	return databases, code, err
}

func (c *ClusterService) GetClusterTables(ctx *gin.Context, database string) ([]string, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	clusterMod := &models.Cluster{}
	err := db.Model(clusterMod).First(clusterMod, "cluster_id = ?", c.ClusterID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.ClusterDriver(cluster_id=%v) not exist", c.ClusterID)
			log.Error(err)
			return nil, common.CodeClusterNotExist, err
		}

		err = fmt.Errorf("query models.ClusterDriver(cluster_id=%v) failed, %v", c.ClusterID, err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}
	c.ModelToService(clusterMod)
	tables, code, err := NewClusterDriver(c).GetTables(ctx, database)
	tables = common.ExcludeTablesFilter(database, tables, strings.Split(Cfg.ClusterExcludeTables, ","))
	return tables, code, err
}

func (c *ClusterService) ServiceToModel() *models.Cluster {
	m := &models.Cluster{}
	m.ID = c.ID
	m.Creator = c.Creator
	m.Editor = c.Editor
	m.CreatedAt, _ = time.ParseInLocation(time.DateTime, c.CreatedAt, time.Now().Location())
	m.UpdatedAt, _ = time.ParseInLocation(time.DateTime, c.UpdatedAt, time.Now().Location())
	m.ClusterName = c.ClusterName
	m.ClusterID = c.ClusterID
	m.Description = c.Description
	m.Bu = c.Bu
	m.Env = c.Env
	m.ImportFrom = c.ImportFrom
	m.ClusterType = c.ClusterType
	m.ServiceAddr = c.ServiceAddr
	m.WriteAddr = c.WriteAddr
	m.ReadAddr = c.ReadAddr
	m.UserName = c.UserName
	m.Password = c.Password
	return m
}

func (c *ClusterService) ModelToService(m *models.Cluster) *ClusterService {
	c.ID = m.ID
	c.Creator = m.Creator
	c.Editor = m.Editor
	c.CreatedAt = m.CreatedAt.Format(time.DateTime)
	c.UpdatedAt = m.UpdatedAt.Format(time.DateTime)
	c.ClusterName = m.ClusterName
	c.ClusterID = m.ClusterID
	c.Description = m.Description
	c.Bu = m.Bu
	c.Env = m.Env
	c.ImportFrom = m.ImportFrom
	c.ClusterType = m.ClusterType
	c.ServiceAddr = m.ServiceAddr
	c.WriteAddr = m.WriteAddr
	c.ReadAddr = m.ReadAddr
	c.UserName = m.UserName
	c.Password = m.Password
	return c
}

func (c *ClusterService) SyncCluster(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	newClusters, code, err := NewClusterSynchronizer(c).Sync(ctx)

	newClusterMap := make(map[string]*models.Cluster, len(newClusters))
	for i, v := range newClusters {
		newClusterMap[v.ClusterID] = newClusters[i]
	}

	var clusters []models.Cluster
	err = db.Where("import_from =?", c.ImportFrom).Find(&clusters).Error
	if err != nil {
		// 查询出错，continue
		err = fmt.Errorf("query models.Cluster(import_from=%s) failed, %s", c.ImportFrom, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	// 更新cluster原则
	// 1, 以cluster_id为准不存在则创建，存在则更新
	// 2, 不存在的则删除
	clusterDelete := make([]string, 0, 5)
	for _, clu := range clusters {
		newCluster, exist := newClusterMap[clu.ClusterID]
		if !exist {
			// 1，库里存在但ipaas中不存在 => 删除库里
			clusterDelete = append(clusterDelete, clu.ClusterID)
		} else {
			// 2，库里存在&ipaas中也存在需更新
			if !clusterServiceEqual(&clu, newCluster) {
				//newCluster.ID = clu.ID
				err = db.Model(&models.Cluster{}).Where("cluster_id =?", newCluster.ClusterID).Save(newCluster).Error
				if err != nil {
					log.Errorf("update models.Cluster(%s) failed, %s", newCluster.ClusterID, err)
				} else {
					log.Debugf("update models.Cluster(%+v) success", newCluster)
				}
			}
		}
		// newClusterMap 中剩下的表示需要创建的
		delete(newClusterMap, clu.ClusterID)
	}

	// 库里不存在ipaas中存在需要创建
	for _, newCluster := range newClusterMap {
		err = db.Model(&models.Cluster{}).Save(newCluster).Error
		if err != nil {
			log.Errorf("create models.Cluster(%s) failed, %s", newCluster.ClusterID, err)
		} else {
			log.Debugf("create models.Cluster(%+v) success", newCluster)
		}
	}

	for _, id := range clusterDelete {
		err = db.Delete(&models.Cluster{}, "cluster_id =?", id).Error
		if err != nil {
			log.Errorf("delete models.Cluster(%s) failed, %s", id, err)
		} else {
			log.Debugf("delete models.Cluster(%v) success", id)
		}
	}

	return code, err
}

func clusterServiceEqual(old, new *models.Cluster) bool {
	if old == nil || new == nil {
		return true
	}

	if old.ClusterName == new.ClusterName &&
		old.ClusterID == new.ClusterID &&
		old.Description == new.Description &&
		old.Bu == new.Bu &&
		old.Env == new.Env &&
		old.ImportFrom == new.ImportFrom &&
		old.ClusterType == new.ClusterType &&
		old.ServiceAddr == new.ServiceAddr &&
		old.WriteAddr == new.WriteAddr &&
		old.ReadAddr == new.ReadAddr {
		return true
	}
	return false
}

var RefreshClusterBigTablesLock sync.Mutex

func (_ *ClusterService) RefreshClusterBigTables(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	if !RefreshClusterBigTablesLock.TryLock() {
		err := fmt.Errorf("there is already a task collecting information about the large table of the cluster")
		return common.CodeClusterCollectedTaskExisted, err
	}
	defer RefreshClusterBigTablesLock.Unlock()

	var date = time.Now().Format(time.DateOnly)
	var clusters []models.Cluster

	err := db.Model(models.Cluster{}).Find(&clusters).Error
	if err != nil {
		err = fmt.Errorf("query models.Cluster failed, %s", err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	for _, c := range clusters {
		clusterSvc := &ClusterService{}
		clusterSvc.ModelToService(&c)
		tablesInfo, _, err := NewClusterDriver(clusterSvc).GetClusterBigTables(ctx, 10 /* GB */)
		if err != nil {
			log.Errorf("get cluster(%s) big tables failed, %s", clusterSvc.ClusterID, err)
			continue
		}

		var clusterStatistics []models.ClusterStatistics
		if tableInfo, ok := tablesInfo.([]any); ok {
			for _, info := range tableInfo {
				if v, ok := info.(struct {
					Database string
					Table    string
					Size     int
				}); ok {
					// 查找该table使用的策略
					var res []uint
					err = db.Model(&models.Policy{}).
						Select("policy.id").
						Joins("JOIN source ON policy.src_id = source.id").
						Where("source.cluster_id =? AND source.database_name =? AND source.tables_name LIKE ?",
							c.ClusterID, v.Database, "%"+v.Table+"%").
						Scan(&res).Error
					if err != nil {
						log.Errorf("query models.Policy from db failed, %s", err)
					}

					clusterStatistics = append(clusterStatistics, models.ClusterStatistics{
						Date:        date,
						Bu:          c.Bu,
						ClusterID:   c.ClusterID,
						ClusterName: c.ClusterName,
						Database:    v.Database,
						Table:       v.Table,
						TableSize:   v.Size,
						Policies:    utils.SliceToString(res, ","),
					})
				}
			}
		}

		// 删除之前的
		err = db.Transaction(func(db *gorm.DB) error {
			err = db.
				Unscoped().
				Delete(&models.ClusterStatistics{}, "date =? AND cluster_id=?", date, c.ClusterID).Error
			if err != nil {
				log.Errorf("delete models.ClusterStatistics from db failed, %s", err)
				return err
			}

			if len(clusterStatistics) == 0 {
				return nil
			}

			err = db.Save(&clusterStatistics).Error
			if err != nil {
				log.Errorf("save models.ClusterStatistics from db failed, %s", err)
				return err
			}
			return nil
		})
		if err != nil {
			log.Errorf("update models.ClusterStatistics(%s) from db failed, %s", c.ClusterID, err)
		}
	}

	return common.CodeOK, nil
}
