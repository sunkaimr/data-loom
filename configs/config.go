package configs

import (
	"fmt"
	"github.com/spf13/viper"
	"path/filepath"
	"strings"
)

type Config struct {
	Mysql    Mysql
	Server   Server
	Jwt      Jwt
	Log      Log
	Job      Job
	WorkFlow WorkFlow
}

type Job struct {
	PolicyCron string `yaml:"policyCron"`
	TaskCron   string `yaml:"taskCron"`
}

type Jwt struct {
	Secret string `yaml:"secret"`
}

type Log struct {
	// 日志路径
	Path string `yaml:"path"`
	// 仅支持：error warn info debug
	Level string `yaml:"level"`
	// 日志切割大小单位MB
	MaxSize int `yaml:"maxSize"`
	// 最大保留个数
	MaxBackups int `yaml:"maxBackups"`
	//  最大保留天数
	MaxAge int `yaml:"maxAge"`
	// 是否压缩日志
	Compress bool `yaml:"compress"`
}

type Mysql struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DataBase string `yaml:"database"`
}

type Server struct {
	GinMode      string `yaml:"ginMode"`
	Port         string `yaml:"port"`
	ExternalAddr string `yaml:"externalAddr"`
}

type WorkFlow struct {
	Driver string `yaml:"driver"`
	Argo   struct {
		URL       string            `yaml:"url"`
		Token     string            `yaml:"token"`
		Templates map[string]string `yaml:"templates"`
	} `yaml:"argo"`
}

var C *Config

func LoadConfig(filePath string) *Config {
	path := filepath.Dir(filePath)
	filename := filepath.Base(filePath)

	viper.AddConfigPath(path)

	f := strings.Split(filename, ".")
	switch len(f) {
	case 0, 1:
		viper.SetConfigName(filename)
	case 2:
		viper.SetConfigName(f[0])
		viper.SetConfigType(f[1])
	default:
		viper.SetConfigName(strings.Join(f[:len(f)-1], "."))
		viper.SetConfigType(f[len(f)-1])
	}

	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("fatal error config filePath: %w", err))
	}

	config := &Config{}
	if err := viper.Unmarshal(config); err != nil {
		panic(err)
	}

	return config
}

func Init(file string) {
	C = LoadConfig(file)
}
