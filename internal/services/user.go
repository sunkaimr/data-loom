package services

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"gorm.io/gorm"
	"time"
)

type UserService struct {
	Model
	Username  string `json:"username"`   // 用户名
	Password  string `json:"password"`   // 密码
	RealName  string `json:"real_name"`  // 姓名
	Email     string `json:"email"`      // 邮箱
	IsLdap    uint   `json:"is_ldap"`    // 是否是LDAP用户
	LastLogin string `json:"last_login"` // 上次登录时间
	Token     string `json:"token"`      // token，登录成功后返回
}

// UserLogin 用户登录
func (c *UserService) UserLogin(ctx *gin.Context) (*UserService, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	user := models.User{}
	err := db.First(&user, "username = ?", c.Username).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("username(%s) not found", c.Username)
			log.Error(err)
			return nil, common.CodeUserNotExist, err
		}
		err = fmt.Errorf("find username(%s) failed, %s", c.Username, err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	// 本地用户登录
	if c.IsLdap == 0 {
		// 校验密码
		pass, err := utils.ComparePasswords(user.Password, c.Password)
		if err != nil {
			err = fmt.Errorf("password verification failed, %s", err)
			log.Error(err)
			return nil, common.CodeServerErr, err
		}
		if !pass {
			err = fmt.Errorf("user(%s) password verification not pass", c.Username)
			log.Error(err)
			return nil, common.CodeUserPasswdErr, err
		}
	} else {
		// AD 账号登录【暂时不支持】
		err = fmt.Errorf("暂时不支持AD账号登录")
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	// 签发token
	claims := common.Claims{UserName: user.Username, RealName: user.RealName, UserID: user.ID}
	token, err := common.GenerateToken(&claims)
	if err != nil {
		err = fmt.Errorf("generate token failed, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	rUser := &UserService{}
	rUser.ModelToService(&user)
	rUser.Token = token

	// 更新最近登录
	user.LastLogin = time.Now()
	err = db.Save(&user).Error
	if err != nil {
		log.Warnf("update user(%s) last login time failed, %s", user.Username, err)
	}
	log.Infof("user(%s) has login", user.Username)

	return rUser, common.CodeOK, err
}

// ModifyUserInfo 修改用户信息
func (c *UserService) ModifyUserInfo(ctx *gin.Context) (*UserService, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	user := models.User{}
	err := db.First(&user, "username = ?", c.Username).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("username(%s) not found", c.Username)
			log.Error(err)
			return nil, common.CodeUserNotExist, err
		}
		err = fmt.Errorf("find username(%s) failed, %s", c.Username, err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	// AD用户信息不能修改
	if user.IsLdap == 1 || c.IsLdap == 1 {
		err = fmt.Errorf("ldap user(%s) cannot modify user info", user.Username)
		log.Error(err)
		return nil, common.CodeUserDeniedLdap, err
	}

	user.RealName = c.RealName
	user.Email = c.Email
	if c.Password != "" {
		hashedPassword, err := utils.HashPassword(c.Password)
		if err != nil {
			err = fmt.Errorf("hash password failed, %s", err)
			log.Error(err)
			return nil, common.CodeServerErr, err
		}
		user.Password = hashedPassword
	}

	rUser := &UserService{}
	rUser.ModelToService(&user)
	rUser.Password = "******"

	err = db.Save(&user).Error
	if err != nil {
		err = fmt.Errorf("update user(%s) info failed, %s", user.Username, err)
		log.Error(err)
		return rUser, common.CodeServerErr, err
	}
	log.Infof("user(%s) info has updated", user.Username)
	return rUser, common.CodeOK, nil
}

// RegisterUser 注册本地用户
func (c *UserService) RegisterUser(ctx *gin.Context) (*UserService, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	user := &models.User{}
	var count int64
	err := db.Model(user).Where("username = ?", c.Username).Count(&count).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		err = fmt.Errorf("username(%s) not found", c.Username)
		log.Error(err)
		return c, common.CodeServerErr, err
	}

	if count != 0 {
		err = fmt.Errorf("username(%s) has exist", c.Username)
		log.Error(err)
		return nil, common.CodeUserExisted, err
	}

	hashedPassword, err := utils.HashPassword(c.Password)
	if err != nil {
		err = fmt.Errorf("hash password failed, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	user.Username = c.Username
	user.RealName = c.RealName
	user.Email = c.Email
	user.IsLdap = 0
	user.Password = hashedPassword
	user.LastLogin = time.UnixMicro(0)

	err = db.Save(&user).Error
	if err != nil {
		err = fmt.Errorf("save user(%s) to db failed, %s", user.Username, err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}
	log.Infof("user(%s) has registed", user.Username)

	rUser := &UserService{}
	rUser.ModelToService(user)
	rUser.Password = "******"

	return rUser, common.CodeOK, nil
}

// DeleteUser 删除本地用户
func (c *UserService) DeleteUser(ctx *gin.Context) (*UserService, common.ServiceCode, error) {
	var err error
	log, db := common.ExtractContext(ctx)

	user := &models.User{}
	var count int64
	err = db.Model(user).Where("username = ?", c.Username).First(user).Count(&count).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("username(%s) not found", c.Username)
			log.Error(err)
			return c, common.CodeUserNotExist, err
		}
		err = fmt.Errorf("find username(%s) failed, %s", c.Username, err)
		log.Error(err)
		return c, common.CodeServerErr, err
	}

	// AD用户不可删除
	if user.Username == common.AdminUser {
		err = fmt.Errorf("user(%s) cannot delete", common.AdminUser)
		log.Error(err)
		return nil, common.CodeUserAdminDelErr, nil
	}

	err = db.Delete(user).Error
	if err != nil {
		err = fmt.Errorf("delete user(%s) failed, %s", user.Username, err)
		log.Error(err)
		return nil, common.CodeUserDelErr, err
	}
	log.Infof("user(%s) has deleted", user.Username)

	rUser := &UserService{}
	rUser.ModelToService(user)
	rUser.Password = "******"
	return rUser, common.CodeOK, nil
}

// QueryUsers 查询用户
func (c *UserService) QueryUsers(ctx *gin.Context) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	res, err := common.NewPageList[[]models.User](db).
		QueryPaging(ctx).
		Order("id desc").
		Query(
			common.FilterFuzzyString("username", c.Username),
			common.FilterFuzzyString("real_name", c.RealName),
			common.FilterFuzzyString("email", c.Email),
			common.FilterCustomUintID("is_ldap", c.IsLdap),
		)
	if err != nil {
		err = fmt.Errorf("query user from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	ret := common.NewPageList[[]UserService](db)
	ret.Page = res.Page
	ret.PageSize = res.PageSize
	ret.Total = res.Total
	for i := range res.Items {
		s := &UserService{}
		s.ModelToService(&res.Items[i])
		ret.Items = append(ret.Items, *s)
	}

	return ret, common.CodeOK, nil
}

func (c *UserService) ServiceToModel() *models.User {
	m := &models.User{}
	m.ID = c.ID
	m.Creator = c.Creator
	m.Editor = c.Editor
	m.CreatedAt, _ = time.ParseInLocation(time.DateTime, c.CreatedAt, time.Now().Location())
	m.UpdatedAt, _ = time.ParseInLocation(time.DateTime, c.UpdatedAt, time.Now().Location())
	m.Username = c.Username
	m.Password = c.Password
	m.RealName = c.RealName
	m.Email = c.Email
	m.IsLdap = c.IsLdap
	m.LastLogin, _ = time.ParseInLocation(time.DateTime, c.LastLogin, time.Now().Location())
	return m
}

func (c *UserService) ModelToService(m *models.User) *UserService {
	c.ID = m.ID
	c.Creator = m.Creator
	c.Editor = m.Editor
	c.CreatedAt = m.CreatedAt.Format(time.DateTime)
	c.UpdatedAt = m.UpdatedAt.Format(time.DateTime)
	c.Username = m.Username
	c.Password = m.Password
	c.RealName = m.RealName
	c.Email = m.Email
	c.IsLdap = m.IsLdap
	c.LastLogin = m.LastLogin.Format(time.DateTime)
	return c
}
