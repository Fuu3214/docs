package main

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/going/l5"
	"git.code.oa.com/rainbow/golang-sdk/log"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	gormMysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"net"
	"strconv"
	"strings"
	"time"
)

var (
	L5GetRouteMaxRetryTimes = 3
)

func DailL5Dsn(ctx context.Context, addr string) (conn net.Conn, err error) {
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

	var server *l5.Server
	for i := 0; i < L5GetRouteMaxRetryTimes; i++ {
		server, err = l5.ApiGetRoute(int32(modId), int32(cmdId))
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
	mysql.RegisterDialContext("l5", DailL5Dsn)
	dsn := fmt.Sprintf("%v:%v@l5(%v)/%v?charset=utf8mb4&parseTime=True&loc=Local", username, password, l5, database)
	log.Info("connecting to dsn: %v", dsn)
	db, err := gorm.Open(gormMysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	return db, nil
}

func getDb(username, password, ip, port, database string) (*gorm.DB, error) {
	//mysql.RegisterDialContext("l5", DailL5Dsn)
	dsn := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&parseTime=True&loc=Local", username, password, ip, port, database)
	log.Info("connecting to dsn: %v", dsn)
	db, err := gorm.Open(gormMysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	return db, nil
}
