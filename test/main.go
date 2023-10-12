package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

type Config struct {
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
	// CommVar 读取通用配置
	conf  = Config{}
	conf2 = make(map[string]interface{})
)

type MyError struct {
	message string
	err     error
}

func (e MyError) Error() string {
	return e.message + ": " + e.err.Error()
}

func (e MyError) Unwrap() error {
	return e.err
}

var someErr = errors.New("some error")

func doSomething() error {
	// some code that may return an error
	return MyError{
		message: "msg",
		err:     someErr,
	}
}

type ApiResponse struct {
	ErrCode int         `json:"code"` // 错误码,0表示无错误
	Message string      `json:"msg"`  // 提示信息
	Data    interface{} `json:"data"` // 响应数据,一般从这里前端从这个里面取出数据展示
}

type MyStruct struct {
	A string
	B string
	C int
}
type MMyStruct []*MyStruct

type KafkaRebuildErr error

// Field .
type Field struct {
	Type     string `json:"type"`
	Value    string `json:"value"`
	OldValue string `json:"old_value,omitempty"`
}

// Rows .
type Rows struct {
	Data map[string]Field `json:"data"`
}

// Meta .
type Meta struct {
	Index     *string `json:"index"`
	LastIndex *string `json:"last_index"`
	Type      string  `json:"type"`
	Table     string  `json:"table"`
	Errcode   int     `json:"errcode"`
	Version   int     `json:"version"`
	Timestamp []int64 `json:"timestamp"`
}

// CdcMsg .
type CdcMsg struct {
	Meta Meta                         `json:"meta"`
	Data map[string]map[string]string `json:"data"`
}

func main() {
	str := "{\"meta\":{\"index\":\"2145790-1669-103431782-0\",\"last_index\":\"2145790-1669-103431490-0\",\"type\":\"update\",\"table\":\"music.t_track_data\",\"errcode\":0,\"version\":2,\"timestamp\":[1695183675000,1695183675000,1695183675810,0]},\"data\":{\"Falbum_status\":{\"type\":\"int\",\"value\":\"1\"},\"Falias_name\":{\"type\":\"string\",\"value\":\"\"},\"Fall_singer\":{\"type\":\"string\",\"value\":\"\"},\"Fdesc_status\":{\"type\":\"int\",\"value\":\"0\"},\"Fex_status\":{\"type\":\"int\",\"value\":\"1\"},\"Fex_status1\":{\"type\":\"int\",\"value\":\"0\"},\"Fex_status2\":{\"type\":\"int\",\"value\":\"1\"},\"Fex_status3\":{\"type\":\"int\",\"value\":\"1\"},\"Fex_status4\":{\"type\":\"int\",\"value\":\"1\"},\"Ffm_album_status\":{\"type\":\"int\",\"value\":\"1\"},\"Ffm_ex_status\":{\"type\":\"int\",\"value\":\"0\"},\"Fhk_album_status\":{\"type\":\"int\",\"value\":\"1\"},\"Fhk_ex_status\":{\"type\":\"int\",\"value\":\"0\"},\"Fhk_status\":{\"type\":\"int\",\"value\":\"0\"},\"Flastest_modify_time\":{\"old_value\":\"1695183649\",\"type\":\"int\",\"value\":\"1695183675\"},\"Flisten_count\":{\"type\":\"int\",\"value\":\"10704\"},\"Flisten_count1\":{\"old_value\":\"100155227\",\"type\":\"int\",\"value\":\"100153247\"},\"Fmodify_time\":{\"old_value\":\"2023-09-20 12:18:27\",\"type\":\"string\",\"value\":\"2023-09-20 12:21:15\"},\"Foversea_type\":{\"type\":\"int\",\"value\":\"5\"},\"Fsearch_key\":{\"type\":\"string\",\"value\":\"\"},\"Fshow_status\":{\"type\":\"int\",\"value\":\"1\"},\"Fstop_update\":{\"type\":\"int\",\"value\":\"0\"},\"Ftrack_eq\":{\"type\":\"int\",\"value\":\"2\"},\"Ftrack_id\":{\"type\":\"int\",\"value\":\"4930524\"},\"Ftrack_new_name\":{\"type\":\"string\",\"value\":\"\"},\"Ftrans_name\":{\"type\":\"string\",\"value\":\"\"},\"Ftv_name\":{\"type\":\"string\",\"value\":\"\"},\"Fversion_d\":{\"type\":\"string\",\"value\":\"\"}}}"
	var cm CdcMsg
	err := json.Unmarshal([]byte(str), &cm)
	fmt.Println(cm, err)
	fmt.Println(*cm.Meta.Index)
}

func testCodec() {
	arr := []int64{1, 2, 3, 4, 5, 6, 5, 4}
	// 将 int64 数组编码为字节数组
	buf := new(bytes.Buffer)
	for _, v := range arr {
		binary.Write(buf, binary.LittleEndian, v)
	}
	encoded1 := buf.Bytes()

	// 将字节数组解码为 int64 数组
	decoded1 := make([]int64, len(arr))
	buf = bytes.NewBuffer(encoded1)
	for i := range arr {
		fmt.Println(i)
		binary.Read(buf, binary.LittleEndian, &decoded1[i])
	}

	fmt.Println(encoded1)
	fmt.Println(decoded1)

	encoded, _ := encode(arr)
	fmt.Println(encoded)
	decoded, _ := decode(encoded)
	fmt.Println(decoded)
}

func encode(int64Slice []int64) ([]byte, error) {
	// 将 []int64 切片转换为 []byte
	buf := new(bytes.Buffer)
	for _, v := range int64Slice {
		binary.Write(buf, binary.LittleEndian, v)
	}
	encoded := buf.Bytes()
	return encoded, nil
}

func decode(byteSlice []byte) ([]int64, error) {
	la := len(byteSlice) / 8
	// 将 []byte 切片转换为 []int64
	decoded := make([]int64, la)
	buf := bytes.NewBuffer(byteSlice)
	for i := 0; i < la; i++ {
		binary.Read(buf, binary.LittleEndian, &decoded[i])
	}
	return decoded, nil
}
