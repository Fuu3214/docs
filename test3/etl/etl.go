package main

import (
	"git.code.oa.com/going/going/log"
	"test3/etl/common"
	"test3/etl/dao"
	"test3/etl/redis"
	"time"
)

// AsiEtlSrv 用于媒资IP关联关系ETL
type AsiEtlSrv interface {
	LoadIpAlbum(mis []*common.MsiInfo) (map[int64][]dao.IpAlbum, error)
	CalculateAsi(ipLinks map[int64][]dao.IpAlbum) (map[int64][]common.MediaInfo, error)
	StoreAsi(infMap map[int64][]common.MediaInfo) error
	UpdateFright(infMap map[int64][]common.MediaInfo) error
}

// AsiEtlSrvImpl 服务实现类
type AsiEtlSrvImpl struct{}

var _ AsiEtlSrv = &AsiEtlSrvImpl{}

// LoadIpAlbum 加载关联关系数据,返回每一个ip下,有多少个IpAlbum
func (m *AsiEtlSrvImpl) LoadIpAlbum(mis []*common.MsiInfo) (map[int64][]dao.IpAlbum, error) {
	res := make(map[int64][]dao.IpAlbum, 0)
	for _, mi := range mis {
		for _, mt := range mi.MediaTypes {
			switch mt {
			case common.MediaTypeAlbum:
				// 读取db中的关联关系
				lks, err := dao.GetLinksBySourceAndSinkType(mi.Ip, common.MediaTypeIp,
					common.MediaTypeAlbum, mi.LinkTypes, mi.Score)
				if err != nil {
					return nil, err
				}
				if lks == nil || len(lks) == 0 {
					log.Info("scanned 0 links for IP = %v", mi)
					continue
				} else {
					log.Info("scanned %v links for IP = %v", len(lks), mi)
				}
				//读取专辑信息
				var albumIds []int64
				for _, lk := range lks {
					albumIds = append(albumIds, lk.Sink)
				}
				albums, err := dao.GetAlbumsByIds(albumIds)
				if err != nil {
					return nil, err
				}
				if albums == nil || len(albums) == 0 {
					log.Info("scanned 0 albums for IP = %v", mi)
					continue
				}
				log.Info("scanned %v albums for IP = %v", len(albums), mi)
				var ipAlbums []dao.IpAlbum
				for _, a := range albums {
					// 过滤FexStatus = 0 的专辑
					if a.ExStatus == 1 {
						ipAlbums = append(ipAlbums, dao.IpAlbum{
							Ip:         mi.Ip,
							Album:      a.Id,
							AlbumName:  a.Name,
							PublicTime: a.PublicTime,
						})
					} else {
						log.Info("ignored album where FexStatus = 0, albumId = %v", a.Id)
					}
				}
				res[mi.Ip] = ipAlbums
			default:
				continue
			}
		}
	}
	return res, nil
}

// CalculateAsi 处理关联关系,计算其中每个媒体ID通过IP关联的其他媒体ID
func (m *AsiEtlSrvImpl) CalculateAsi(ipLinks map[int64][]dao.IpAlbum) (map[int64][]common.MediaInfo, error) {
	type ipSet map[int64]struct{}
	// 每个IP关联的专辑
	ipAlbumInfo := make(map[int64][]common.MediaInfo, 0)
	// 每个专辑关联的IP
	albumIpSet := make(map[int64]ipSet)
	for ip, lks := range ipLinks {
		for _, l := range lks {
			ipAlbumInfo[ip] = append(ipAlbumInfo[ip], common.MediaInfo{
				MediaId:    l.Album,
				PublicTime: l.PublicTime.UnixMilli(),
			})
			if _, ok := albumIpSet[l.Album]; !ok {
				albumIpSet[l.Album] = make(map[int64]struct{})
			}
			albumIpSet[l.Album][ip] = struct{}{}
		}
	}
	ret := make(map[int64][]common.MediaInfo)
	for al, ips := range albumIpSet {
		for ip := range ips {
			ret[al] = append(ret[al], ipAlbumInfo[ip]...)
		}
	}
	return ret, nil
}

// StoreAsi 将计算的MSI信息存放到redis中
func (m *AsiEtlSrvImpl) StoreAsi(infMap map[int64][]common.MediaInfo) error {
	for albumId, infos := range infMap {
		k := common.GetMsiKey(common.MediaTypeAlbum, albumId)
		err := redis.CkvPlus.UpdateInfos(k, infos)
		if err != nil {
			return err
		}
	}
	return nil
}

// UpdateFright 更新标志位
func (m *AsiEtlSrvImpl) UpdateFright(infMap map[int64][]common.MediaInfo) error {
	// 使用redis记录已经更改过的专辑ID,减少调用接口次数
	t := time.Now()
	k2 := common.GetAlbumsKey(t)
	m1, m2 := make(map[int64]struct{}, 0), make(map[int64]struct{}, 0)
	// 先读取,如果读不到或者为空,则全量更新
	ids, err := redis.CkvPlus.LoadAlbums(k2)
	if err == nil && ids != nil && len(ids) != 0 {
		for _, id := range ids {
			m1[id] = struct{}{}
		}
	}

	// 获取需要更新标志位的id
	toUpdate := make([]int64, 0)
	for id := range infMap {
		if _, ok := m1[id]; !ok {
			toUpdate = append(toUpdate, id)
		}
		m2[id] = struct{}{}
	}
	// 更新redis
	var nIds []int64
	for k := range m2 {
		nIds = append(nIds, k)
	}
	if len(nIds) != 0 {
		err = redis.CkvPlus.UpdateAlbums(k2, nIds, common.GetAlbumsKeyExpireAt(t))
		if err != nil {
			return err
		}
	}

	// 上报需要更新的数据
	if len(toUpdate) > 0 {
		if err != nil {
			return err
		}
		err = sendFrightUpdate(toUpdate, 1)
		if err != nil {
			return err
		}
	}

	// 获取需要去除标志位的id
	removed := make([]int64, 0)
	for _, id := range ids {
		if _, ok := m2[id]; !ok {
			removed = append(removed, id)
		}
	}
	if len(removed) > 0 {
		err = sendFrightUpdate(removed, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

// updateQukuInfoByIdReq 标志位更新请求
type updateQukuInfoByIdReq struct {
	UpdateType  string        `json:"type"`
	User        string        `json:"user"`
	ContentData []contentData `json:"data"`
}

type contentData struct {
	PkId int64          `json:"pk_id"`
	Data map[string]any `json:"data"`
}

func sendFrightUpdate(albums []int64, status int64) error {
	url := common.GetStrOrDefault("UpdateQukuInfoByIdUrl", common.UpdateQukuInfoByIdReqUrlDefault)
	var data []contentData
	for _, id := range albums {
		data = append(data, contentData{
			PkId: id,
			Data: map[string]any{
				"Fright__ip_linked_album": status,
			},
		})
	}

	updateUser := common.GetStrOrDefault("UpdateQukuInfoByIdReqUser", common.UpdateQukuInfoByIdReqUserDefault)

	body := updateQukuInfoByIdReq{
		UpdateType:  common.ALBUM,
		User:        updateUser,
		ContentData: data,
	}

	return common.HttpPost(url, body)
}
