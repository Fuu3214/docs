package main

import (
	"git.code.oa.com/going/going/log"
	"mediaLink/common"
	"mediaLink/config"
)

// Runner 定时任务执行器
type Runner struct {
	intervalsMin int32
	ae           AsiEtlSrv
}

// NewRunner 新建Runner
func NewRunner(intervalsMin int32) *Runner {
	return &Runner{
		intervalsMin: intervalsMin,
		ae:           &AsiEtlSrvImpl{},
	}
}

// Run 执行ETL
func (r *Runner) Run() {
	cfg := &config.MsiInfos
	msi := make([]*common.MsiInfo, 0)
	cfg.Range(func(k, v any) bool {
		inf, ok := v.(*common.MsiInfo)
		if !ok {
			log.Error("failed to read config, Data = %v", inf)
		}
		msi = append(msi, inf)
		return true
	})

	ipAlbum, err := r.ae.LoadIpAlbum(msi)
	if err != nil {
		log.Error("failed to load IpAlbum, err = %v", err)
		return
	}
	asi, err := r.ae.CalculateAsi(ipAlbum)
	if err != nil {
		log.Error("failed to caculate asi, err = %v", err)
		return
	}
	err = r.ae.StoreAsi(asi)
	if err != nil {
		log.Error("failed to cache asi, err = %v", err)
		return
	}
	err = r.ae.UpdateFright(asi)
	if err != nil {
		log.Error("failed to report fright, err = %v", err)
		return
	}
}

func main() {
	intervalsMin := common.GetIntOrDefault("EtlRunnerIntervals", common.EtlRunnerIntervalsMinDefault)
	R := NewRunner(int32(intervalsMin))
	R.Run()
}
