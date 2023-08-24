package common

import (
	"bufio"
	"fmt"
	"git.code.oa.com/going/going/log"
	"io"
	"os"
	"strings"
	"time"
)

var (
	msiKeyFormat   = CKVPlusMsiKeyFormat
	albumKeyFormat = CKVPlusAlbumKeyFormat
)

// GetArea 通过OS信息获取area
func GetArea() (string, error) {
	fi, err := os.Open("/usr/local/services/etc/ipinfo")
	if err != nil {
		return "", err
	}
	defer func() {
		err := fi.Close()
		if err != nil {
			log.Error("failed to close os file, err = %v", err)
		}
	}()

	br := bufio.NewReader(fi)
	for {
		line, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		if strings.HasPrefix(string(line), "area=") {
			return string(line[5:]), nil
		}
	}

	return "", fmt.Errorf("not found")
}

// GetMsiKey 通过mediaType和mediaId获取key
func GetMsiKey(mediaType int32, mediaId int64) []byte {
	return []byte(fmt.Sprintf(msiKeyFormat, mediaType, mediaId))
}

// GetAlbumsKey 获取ckv+ key
func GetAlbumsKey(t time.Time) []byte {
	return []byte(fmt.Sprintf(albumKeyFormat, t.Format("20060102")))
}

// GetAlbumsKeyExpireAt 获取ckv+ 过期时间,ttl设置为1天
func GetAlbumsKeyExpireAt(t time.Time) int64 {
	yy, mm, dd := t.Date()
	unix := time.Date(yy, mm, dd+2, 0, 0, 0, 0, time.Local).Unix()
	return unix
}
