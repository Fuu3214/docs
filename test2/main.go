package main

import (
	"encoding/json"
	"fmt"
	"git.code.oa.com/rainbow/golang-admin/confapi"
	"git.code.oa.com/rainbow/golang-admin/types"
	admin "git.code.oa.com/rainbow/proto/api/admin"
	"gorm.io/gorm"
	"os"
	"strconv"
)

var (
	Rainbow *confapi.ConfAPI
	appId   = "db671eb8-6155-4fe5-ab8b-6d5804066371"
	userId  = "b0de13e4801f4c5abb7ba40a5b570a66"
	userKey = "07ceb338f1f820e2aa5564ecfa1724cfc866"
	//changeGroupId = 2674093
	groupName = "rule_bid"
	user      = "shaneda"

	//dbUser     = "musicdb"
	//dbPassword = "musicdb"
	dbUser     = "data_service_3"
	dbPassword = "data_service_3"
	dbL5       = "1532673:2555904"
	database   = "rule_db"
	table      = "t_media_rule_bid"

	dbIp   = "100.65.202.29"
	dbPort = "3463"
)

const (
	INSERT = iota
	UPDATE = iota
	DELETE = iota
	ALL    = iota
)

func InitRainbow() {
	var err error
	Rainbow, err = confapi.New(
		//  trpc
		// types.ConnectStr("ip://api.rainbow_admin.woa.com:8000"),
		// http
		types.ConnectStr("http://api.rainbow_admin.woa.com:8080"),
		types.OpenSign(true),
		types.AppID(appId),
		types.UserID(userId),
		types.UserKey(userKey),
		types.HmacWay("sha1"),
		// types.RemoteProto("trpc"),
	)
	if err != nil {
		panic(err)
	}
}

func jsonKV(k, v string, op admin.ConfigOpType) *admin.KeyValue {
	return &admin.KeyValue{
		Key:          k,
		Value:        v,
		ValueType:    4,
		ConfigOpType: op,
	}
}

func SendChangeCfgReq(kvs []*admin.KeyValue) {
	req := &admin.ReqChangeKey{
		AppId:     appId,
		GroupName: groupName,
		KeyValues: kvs,
		User:      user,
	}
	rsp, err := Rainbow.ChangeKeyReq(req)

	if err != nil {
		fmt.Printf("error change rainbow config, err = %v", err)
		return
	}
	fmt.Printf("send req success, rsp = %v\n", rsp)
}

// BidRaw CKV+中储存的bid信息
type BidRaw struct {
	Bid         int    `json:"Fbid" gorm:"column:Fbid"`               //bid编号
	MediaType   int    `json:"Fmedia_type" gorm:"column:Fmedia_type"` //媒资类型
	ContentJson string `json:"Fcontent" gorm:"column:Fcontent"`       //允许查询的口径json内容
	Status      int    `json:"Fstatus" gorm:"column:Fstatus"`         //bid信息状态，0:无效, 1:有效
}

func getBidRawFromDb(bid int, db *gorm.DB) *BidRaw {
	var r BidRaw
	db.Debug().Table(table).
		Select("Fbid", "Fmedia_type", "Fcontent", "Fstatus").
		Where("Fbid =?", bid).
		Scan(&r)
	fmt.Printf("selected from db, data = %v\n", r)
	return &r
}

func getMaxBid(db *gorm.DB) int {
	var r BidRaw
	db.Debug().Table(table).
		Order("Fbid desc").
		Limit(1).
		Scan(&r)
	fmt.Printf("selected from db, data = %v\n", r)
	return r.Bid
}

func getAllBids(db *gorm.DB) []*BidRaw {
	r := make([]*BidRaw, 0)
	db.Debug().Table(table).
		Select("Fbid", "Fmedia_type", "Fcontent", "Fstatus").
		Find(&r)
	fmt.Printf("selected from db, data = %v\n", r)
	return r
}

func main() {

	args := os.Args

	if len(args) != 3 {
		panic(fmt.Sprintf("illegal input, %v\n", args))
	}

	bid, err := strconv.Atoi(args[1])
	if err != nil {
		panic(fmt.Sprintf("illegal input, err = %v\n", err))
	}
	op, err := strconv.Atoi(args[2])
	if err != nil {
		panic(fmt.Sprintf("illegal input, err = %v\n", err))
	}

	//db, err := getDbL5(dbUser, dbPassword, dbL5, database)
	db, err := getDb(dbUser, dbPassword, dbIp, dbPort, database)
	if err != nil {
		panic(fmt.Sprintf("failed to query db, err = %v\n", err))
	}
	InitRainbow()

	if op != ALL {
		kvs := make([]*admin.KeyValue, 0)
		r := getBidRawFromDb(bid, db)
		bidJson, err := json.Marshal(r)
		if err != nil {
			panic(fmt.Sprintf("failed to marshal, %v", r))
		}
		val := string(bidJson)
		fmt.Printf("val = %v\n", val)
		var kv *admin.KeyValue
		switch op {
		case INSERT:
			kv = jsonKV(strconv.Itoa(bid), val, admin.ConfigOpType_CONFIG_UPSERT)
		case UPDATE:
			kv = jsonKV(strconv.Itoa(bid), val, admin.ConfigOpType_CONFIG_UPSERT)
		case DELETE:
			kv = jsonKV(strconv.Itoa(bid), val, admin.ConfigOpType_CONFIG_DELETE)
		default:
			panic(fmt.Sprintf("unknown op type, %v", op))
		}
		kvs = append(kvs, kv)
		SendChangeCfgReq(kvs)
	} else {
		kvs := make([]*admin.KeyValue, 0)
		max := getMaxBid(db)
		rs := getAllBids(db)
		bm := make(map[int]struct{})
		for _, r := range rs {
			bm[r.Bid] = struct{}{}
		}
		for i := 0; i <= max; i++ {
			if _, ok := bm[i]; !ok {
				kv := jsonKV(strconv.Itoa(i), "", admin.ConfigOpType_CONFIG_DELETE)
				kvs = append(kvs, kv)
			}
		}
		for _, r := range rs {
			bidJson, err := json.Marshal(r)
			if err != nil {
				panic(fmt.Sprintf("failed to marshal, %v", r))
			}
			val := string(bidJson)
			fmt.Printf("val = %v\n", val)
			kv := jsonKV(strconv.Itoa(r.Bid), val, admin.ConfigOpType_CONFIG_UPSERT)
			kvs = append(kvs, kv)
		}
		SendChangeCfgReq(kvs)
	}

}
