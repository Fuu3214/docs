package redis

import (
	"context"
	"fmt"
	"git.code.oa.com/going/going/codec/redis"
	"test3/etl/common"
)

const (
	ScriptUpdateInfos = `
        local key = KEYS[1]
		redis.call('DEL', key)
		redis.call('ZADD', key, unpack(ARGV))
		return "OK"
    `
	ScriptUpdateAlbums = `
        local key = KEYS[1]
		redis.call('DEL', key)
		redis.call('SADD', key, unpack(ARGV))
		return "OK"
    `
)

// MsiRedis 封装媒体ip关联关系的redis操作
type MsiRedis struct {
}

var CkvPlus *MsiRedis
var Area = ""

func init() {
	CkvPlus = &MsiRedis{}
	var err error
	Area, err = common.GetArea()
	if err != nil {
		panic(fmt.Sprintf("failed to get area, err = %v", err))
	}
}

// LoadInfos 从redis中载入mediaInfo
func (mr *MsiRedis) LoadInfos(key []byte) ([]common.MediaInfo, error) {
	redisClient := redis.New("musicIpLink_" + Area)
	zset, err := redisClient.Do(context.Background(), "ZREVRANGE", string(key), 0, -1, "WITHSCORES")
	if err != nil {
		return nil, fmt.Errorf("error occurs while do zrange. key is %v, err is %w", string(key), err)
	}
	if zset == nil {
		return nil, fmt.Errorf("error occurs while do zrange. key = %v", string(key))
	}

	slices, _ := redis.ByteSlices(zset, nil)
	if len(slices) == 0 || len(slices)%2 != 0 {
		return nil, nil
	}
	mpis := make([]common.MediaInfo, 0)
	for i := 0; i < len(slices); i += 2 {
		elem := slices[i]
		pt := slices[i+1]

		var mpi common.MediaInfo
		mpi.MediaId, _ = redis.Int64(elem, nil)
		mpi.PublicTime, _ = redis.Int64(pt, nil)
		mpis = append(mpis, mpi)
	}

	return mpis, nil
}

// UpdateInfos 将mediaInfo信息存放到redis
func (mr *MsiRedis) UpdateInfos(key []byte, infos []common.MediaInfo) error {
	redisClient := redis.New("musicIpLink_" + Area)
	var vals []interface{}
	for _, inf := range infos {
		vals = append(vals, inf.PublicTime, inf.MediaId)
	}

	var args []interface{}
	//args = append(args, string(key))
	//args = append(args, vals...)
	//_, err := redisClient.Do(context.Background(), "ZADD", args)

	args = append(args, ScriptUpdateInfos, 1, string(key))
	args = append(args, vals...)
	_, err := redisClient.Do(context.Background(), "EVAL", args...)
	if err != nil {
		return fmt.Errorf("failed to store zset, key = %v", key)
	}

	expire := common.GetIntOrDefault("MsiInfoCKVPlusTtlSec", common.MsiInfoCKVPlusTtlSecDefault)
	_, err = redisClient.Do(context.Background(), "EXPIRE", string(key), expire)
	if err != nil {
		return fmt.Errorf("failed to set expire, key = %v", key)
	}
	return nil
}

// UpdateAlbums 将专辑信息存放到redis
func (mr *MsiRedis) UpdateAlbums(key []byte, ids []int64, expireAt int64) error {
	redisClient := redis.New("musicIpLink_" + Area)
	var vals []interface{}
	for _, id := range ids {
		vals = append(vals, id)
	}

	var args []interface{}
	//args = append(args, string(key))
	//args = append(args, vals...)
	//_, err := redisClient.Do(context.Background(), "SADD", args)

	args = append(args, ScriptUpdateAlbums, 1, string(key))
	args = append(args, vals...)
	_, err := redisClient.Do(context.Background(), "EVAL", args...)

	if err != nil {
		return fmt.Errorf("failed to store zset, key = %v", key)
	}
	_, err = redisClient.Do(context.Background(), "EXPIREAT", string(key), expireAt)
	if err != nil {
		return fmt.Errorf("failed to set expire, key = %v", key)
	}
	return nil
}

// RemoveAlbums 去掉redis中记录的专辑
func (mr *MsiRedis) RemoveAlbums(key []byte, ids []int64) error {
	redisClient := redis.New("musicIpLink_" + Area)
	var params []interface{}
	params = append(params, string(key))
	for _, id := range ids {
		params = append(params, id)
	}
	_, err := redisClient.Do(context.Background(), "SREM", params...)
	if err != nil {
		return fmt.Errorf("failed to store zset, key = %v", key)
	}
	return nil
}

// LoadAlbums 从redis加载专辑信息
func (mr *MsiRedis) LoadAlbums(key []byte) ([]int64, error) {
	redisClient := redis.New("musicIpLink_" + Area)
	set, err := redisClient.Do(context.Background(), "SMEMBERS", string(key))
	if err != nil {
		return nil, fmt.Errorf("failed to store zset, key = %v", key)
	}

	slices, _ := redis.ByteSlices(set, nil)
	if len(slices) == 0 {
		return nil, nil
	}
	ids := make([]int64, 0)
	for i := 0; i < len(slices); i++ {
		elem := slices[i]
		id, _ := redis.Int64(elem, nil)
		ids = append(ids, id)
	}

	return ids, nil
}
