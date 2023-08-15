package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/BurntSushi/toml"
)

type Config struct {
	Common     map[string]interface{} `toml:"Common"`
	IpDataBase struct {
		L5       string `toml:"l5"`
		Username string `toml:"username"`
		Password string `toml:"password"`
		Db       string `toml:"db"`
	} `toml:"IpDataBase"`
	AlbumDataBase struct {
		L5       string `toml:"l5"`
		Username string `toml:"username"`
		Password string `toml:"password"`
		Db       string `toml:"db"`
	} `toml:"AlbumDataBase"`
}

var (
	// CommVar 读取通用配置
	conf  = Config{}
	conf2 = make(map[string]interface{})
)

func main() {
	var config Config
	if _, err := toml.DecodeFile("./conf/comm.toml", &config); err != nil {
		panic(err)
	}
	fmt.Println(config)
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
