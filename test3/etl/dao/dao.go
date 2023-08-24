package dao

import (
	"git.code.oa.com/going/going/log"
	"mediaLink/common"
	"time"
)

// Link 关联关系数据
type Link struct {
	Source     int64   `gorm:"column:Fitem_id1"`
	SourceType int64   `gorm:"column:Fitem_type1"`
	Sink       int64   `gorm:"column:Fitem_id2"`
	SinkType   int64   `gorm:"column:Fitem_type2"`
	LinkType   int64   `gorm:"column:Flink_type"`
	Score      float64 `gorm:"column:Fscore"`
}

// Album 专辑信息数据
type Album struct {
	Id         int64     `gorm:"column:Falbum_id"`
	AlbumType  int64     `gorm:"column:Ftype"`
	Name       string    `gorm:"column:Falbum_name"`
	PublicTime time.Time `gorm:"column:Fpublic_time"`
	ExStatus   int64     `gorm:"column:Fex_status"`
}

// IpAlbum Ip-专辑关联关系
type IpAlbum struct {
	Ip         int64     `gorm:"column:Fitem_id1"`
	Album      int64     `gorm:"column:Fitem_id2"`
	AlbumName  string    `gorm:"column:Falbum_name"`
	PublicTime time.Time `gorm:"column:Fpublic_time"`
}

// Ip IP信息数据
type Ip struct {
	ip   int64
	name string
}

var (
	maxRetry = 3
)

// GetLinksBySourceAndSinkType 通过source查找关联关系
func GetLinksBySourceAndSinkType(sourceId, sourceType, sinkType int64,
	linkTypes []int64, score float64) ([]Link, error) {
	var err error
	for i := 0; i < maxRetry; i++ {
		var dst []Link
		sql := "Fitem_id1 =? and Fitem_type1 =? and Fitem_type2 = ? and Fscore = ? and Flink_type in ?"
		if score < 1 {
			sql = "Fitem_id1 =? and Fitem_type1 =? and Fitem_type2 = ? and Fscore > ? and Flink_type in ?"
		}
		result := linkDB.Table(linkTable).
			Select("Fitem_id1", "Fitem_type1", "Fitem_id2", "Fitem_type2", "Flink_type", "Fscore").
			Where(sql, sourceId, sourceType, sinkType, score, linkTypes).
			Find(&dst)
		err = result.Error
		if err != nil {
			log.Error("failed to load from db, err = %v", err)
		} else {
			return dst, nil
		}
	}
	return nil, err
}

// GetLinksBySinkAndSourceType 通过Sink查找关联关系
func GetLinksBySinkAndSourceType(sourceType, sinkId, sinkType int64,
	linkTypes []int64, score float64) ([]Link, error) {
	var err error
	for i := 0; i < maxRetry; i++ {
		var dst []Link
		sql := "Fitem_type1 =? and Fitem_id2 =? and Fitem_type2 = ? and Fscore = ? and Flink_type in ?"
		if score < 1 {
			sql = "Fitem_type1 =? and Fitem_id2 =? and Fitem_type2 = ? and Fscore > ? and Flink_type in ?"
		}
		result := linkDB.Table(linkTable).
			Select("Fitem_id1", "Fitem_type1", "Fitem_id2", "Fitem_type2", "Flink_type", "Fscore").
			Where(sql, sourceType, sinkId, sinkType, score, linkTypes).
			Find(&dst)
		err = result.Error
		if err != nil {
			log.Error("failed to load from db, err = %v", err)
		} else {
			return dst, nil
		}
	}
	return nil, err
}

// GetAlbumsByIds 通过专辑Id获取专辑信息
func GetAlbumsByIds(ids []int64) ([]Album, error) {
	var err error
	for i := 0; i < maxRetry; i++ {
		var dst []Album
		result := albumDb.Table("t_album").
			Select("t_album.Falbum_id as Falbum_id", "Ftype", "Falbum_name", "Fpublic_time",
				"t_album_data.Fex_status as Fex_status").
			Where("t_album.Falbum_id in ?", ids).
			Joins("JOIN t_album_data ON t_album.Falbum_id = t_album_data.Falbum_id").
			Find(&dst)
		err = result.Error
		if err != nil {
			log.Error("failed to load from db, err = %v", err)
		} else {
			return dst, nil
		}
	}
	return nil, err
}

func init() {
	maxRetry = common.GetIntOrDefault("GormMaxRetry", 3)
}
