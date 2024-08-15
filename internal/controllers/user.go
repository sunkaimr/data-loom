package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
	"net/http"
)

type UserController struct{}

// Login 用户登录
// @Router			/user/login [put]
// @Description		用户登录
// @Tags			用户
// @Param			user	body		services.UserService	true 	"username,password,is_ldap"
// @Success			200		{object}	common.Response{data=services.UserService}
// @Failure			401		{object}	common.Response
func (c *UserController) Login(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	user := &services.UserService{}
	if err := ctx.ShouldBindJSON(user); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}

	if user.Username == "" || user.Password == "" {
		log.Errorf("username(%d) or password(%d) empty", len(user.Username), len(user.Password))
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeUserParamErr})
		return
	}

	u, res, err := user.UserLogin(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}

	if u.Token != "" {
		ctx.Header("X-Auth-Token", u.Token)
	}

	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: u})
}

// ModifyUser 		修改用户信息
// @Router			/user [put]
// @Description		修改用户信息
// @Tags			用户
// @Param			user	body		services.UserService	true 	"username"
// @Success			200		{object}	common.Response{data=services.UserService}
// @Failure			401		{object}	common.Response
func (c *UserController) ModifyUser(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	user := &services.UserService{}
	if err := ctx.ShouldBindJSON(user); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}

	// 用户只能修改自己的信息
	if u := common.ExtractUserInfo(ctx); u != nil {
		if u.UserName != common.AdminUser && u.UserName != user.Username {
			log.Errorf("you(%s) cannot modify user(%s) info", u.UserName, user.Username)
			ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeDenied})
			return
		}
	}

	u, res, err := user.ModifyUserInfo(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: u})
}

// RegisterUser		注册用户
// @Router			/manage/user/register [post]
// @Description		注册用户
// @Tags			管理员
// @Param			user	body		services.UserService	true "username,password"
// @Success			200		{object}	common.Response{data=services.UserService}
// @Failure			500		{object}	common.Response
func (c *UserController) RegisterUser(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	user := &services.UserService{}
	if err := ctx.ShouldBindJSON(user); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}

	if user.Username == "" || user.Password == "" {
		log.Errorf("username(%d) or password(%d) empty", len(user.Username), len(user.Password))
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeUserParamErr})
		return
	}

	u, res, err := user.RegisterUser(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: u})
}

// DeleteUser		删除用户
// @Router			/manage/user/{user} [delete]
// @Description		删除用户
// @Tags			管理员
// @Param			user	path		string		true	"用户名"
// @Success			200		{object}	common.Response{data=services.UserService}
// @Failure			500		{object}	common.Response
func (c *UserController) DeleteUser(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	username := ctx.Param("user")
	if username == "" {
		log.Errorf("username(%s) empty", username)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeUserParamErr})
		return
	}

	user := &services.UserService{Username: username}
	u, res, err := user.DeleteUser(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: u})
	return
}

// QueryUser		查询用户
// @Router			/manage/user [get]
// @Description		查询用户
// @Tags			管理员
// @Param   		page		query		int			false  	"page"
// @Param   		pageSize	query		int     	false  	"pageSize"
// @Param			username	query		string		false	"用户名"
// @Param			real_name	query		string		false	"姓名"
// @Param			email		query		string		false	"Email"
// @Param			is_ldap		query		int			false	"是否是LDAP用户(0,1)"
// @Success			200		{object}	common.Response{data=services.UserService}
// @Failure			500		{object}	common.Response
func (c *UserController) QueryUser(ctx *gin.Context) {
	user := &services.UserService{}
	user.Username = ctx.Query("username")
	user.RealName = ctx.Query("real_name")
	user.Email = ctx.Query("email")
	user.IsLdap = common.ParsingQueryUintID(ctx.Query("is_ldap"))

	u, res, err := user.QueryUsers(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: u})
	return
}
