package dao

import (
	"context"
	"errors"
	"fmt"
	l5pkg "git.code.oa.com/going/l5"
	"git.code.oa.com/rainbow/golang-sdk/log"
	"github.com/go-sql-driver/mysql"
	gormMysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"net"
	"strconv"
	"strings"
	"sync"
	"test3/etl/common"
	"time"
)

var (
	l5GetRouteMaxRetryTimes = 5
	// linkDB 关联关系库数据库实例
	linkDB    *gorm.DB
	linkTable = "t_link"
	// albumDb 专辑数据库实例
	albumDb      *gorm.DB
	albumTable   = "t_album"
	once1, once2 = sync.Once{}, sync.Once{}
)

// InitDB 初始化
func InitDB() {
	if _, err := GetAlbumDB(); err != nil {
		panic(fmt.Errorf("failed to init AlbumDB, %v", err))
	}
	if _, err := GetLinkDB(); err != nil {
		panic(fmt.Errorf("failed to init linkDB, %v", err))
	}
}

// GetLinkDB 获取关联关系DB实例
func GetLinkDB() (*gorm.DB, error) {
	once1.Do(func() {
		var err error
		if common.GetBoolOrDefault("DbUseL5", true) {
			linkDB, err = getDbL5(common.Conf.LinkDataBase.Username, common.Conf.LinkDataBase.Password,
				common.Conf.LinkDataBase.L5, common.Conf.LinkDataBase.Db)
		} else {
			linkDB, err = getDb(common.Conf.LinkDataBase.Username, common.Conf.LinkDataBase.Password,
				common.Conf.LinkDataBase.Ip, common.Conf.LinkDataBase.Port, common.Conf.LinkDataBase.Db)
		}
		if err != nil {
			panic(fmt.Sprintf("failed to init ipdb, err = %v", err))
		}
	})
	return linkDB, nil
}

// GetAlbumDB 获取专辑DB实例
func GetAlbumDB() (*gorm.DB, error) {
	once2.Do(func() {
		var err error
		if common.GetBoolOrDefault("DbUseL5", true) {
			albumDb, err = getDbL5(common.Conf.AlbumDataBase.Username, common.Conf.AlbumDataBase.Password,
				common.Conf.AlbumDataBase.L5, common.Conf.AlbumDataBase.Db)
		} else {
			albumDb, err = getDb(common.Conf.AlbumDataBase.Username, common.Conf.AlbumDataBase.Password,
				common.Conf.AlbumDataBase.Ip, common.Conf.AlbumDataBase.Port, common.Conf.AlbumDataBase.Db)
		}
		if err != nil {
			panic(fmt.Sprintf("failed to init ipdb, err = %v", err))
		}
	})
	return linkDB, nil
}

func dailL5Dsn(ctx context.Context, addr string) (conn net.Conn, err error) {
	if len(addr) == 0 {
		return nil, errors.New(`empty address for l5, example: "modId:cmdId"`)
	}

	arr := strings.Split(addr, ":")
	if len(arr) != 2 {
		return nil, errors.New(`wrong address format for l5, example: "modId:cmdId"`)
	}

	modId, err := strconv.Atoi(arr[0])
	if err != nil {
		return nil, errors.New(`wrong address format for l5, example: "modId:cmdId", midId/cmdId should be int32`)
	}

	cmdId, err := strconv.Atoi(arr[1])
	if err != nil {
		return nil, errors.New(`wrong address format for l5, example: "modId:cmdId", midId/cmdId should be int32`)
	}

	var server *l5pkg.Server
	for i := 0; i < l5GetRouteMaxRetryTimes; i++ {
		server, err = l5pkg.ApiGetRoute(int32(modId), int32(cmdId))
		if err == nil {
			log.Warn("get ip port for l5 sid (%v:%v) error", modId, cmdId)
			break
		}
		time.Sleep(1 * time.Second)
	}

	if err != nil || server == nil {
		return nil, errors.New(fmt.Sprintf("get ip port for l5 sid (%d:%d) error: %+v", modId, cmdId, err))
	}

	ip, port := server.Ip(), server.Port()
	nd := net.Dialer{Timeout: 2 * time.Second}
	conn, err = nd.Dial("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err == nil {
		return conn, nil
	}

	return nil, err
}

func getDbL5(username, password, l5, database string) (*gorm.DB, error) {
	mysql.RegisterDialContext("l5", dailL5Dsn)
	connFormat := "%v:%v@l5(%v)/%v?charset=utf8mb4&parseTime=True&loc=Local"
	dsn := fmt.Sprintf(connFormat, username, password, l5, database)
	log.Info("connecting to dsn: %v", dsn)
	db, err := gorm.Open(gormMysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	return db, nil
}

func getDb(username, password, ip, port, database string) (*gorm.DB, error) {
	//mysql.RegisterDialContext("l5", dailL5Dsn)
	connFormat := "%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&parseTime=True&loc=Local"
	dsn := fmt.Sprintf(connFormat, username, password, ip, port, database)
	log.Info("connecting to dsn: %v", dsn)
	db, err := gorm.Open(gormMysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	return db, nil
}
