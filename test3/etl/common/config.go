package common

import (
	"fmt"
	"github.com/BurntSushi/toml"
)

type config struct {
	Common       map[string]interface{} `toml:"Common"`
	LinkDataBase struct {
		L5       string `toml:"l5"`
		Username string `toml:"username"`
		Password string `toml:"password"`
		Db       string `toml:"db"`
		Ip       string `toml:"ip"`
		Port     string `toml:"port"`
	} `toml:"LinkDataBase"`
	AlbumDataBase struct {
		L5       string `toml:"l5"`
		Username string `toml:"username"`
		Password string `toml:"password"`
		Db       string `toml:"db"`
		Ip       string `toml:"ip"`
		Port     string `toml:"port"`
	} `toml:"AlbumDataBase"`
}

var (
	// Conf 读取通用配置
	Conf config
)

func init() {
	if _, err := toml.DecodeFile("../conf/comm.toml", &Conf); err != nil {
		panic(fmt.Sprintf("read file error = %v", err))
	}
	if len(Conf.LinkDataBase.Username) == 0 {
		Conf.LinkDataBase.Username = DataService3
	}
	if len(Conf.LinkDataBase.Password) == 0 {
		Conf.LinkDataBase.Password = DataService3
	}
	if len(Conf.AlbumDataBase.Username) == 0 {
		Conf.LinkDataBase.Password = DataService3
	}
	if len(Conf.AlbumDataBase.Password) == 0 {
		Conf.LinkDataBase.Password = DataService3
	}
}

// GetStrOrDefault 读取common配置并转换为string,不存在key则使用默认值
func GetStrOrDefault(key string, def string) string {
	ret := def
	if val, ok := Conf.Common[key].(string); ok {
		ret = val
	}
	return ret
}

// GetIntOrDefault 读取common配置并转换为int,不存在key则panic
func GetIntOrDefault(key string, def int) int {
	ret := def
	if val, ok := Conf.Common[key].(int); ok {
		ret = val
	}
	return ret
}
