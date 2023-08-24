package common

import (
	"encoding/json"
	"fmt"
	"git.code.oa.com/going/going/log"
	"io"
	"net/http"
	"strings"
)

// HttpPost 发送http post请求
func HttpPost(url string, body any) error {
	// 创建一个 HTTP 客户端
	client := &http.Client{}

	// 创建一个 HTTP 请求
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")

	bodyJson, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal request body, err = %w", err)
	}

	// 设置请求体
	req.Body = io.NopCloser(strings.NewReader(string(bodyJson)))

	// 发送 HTTP 请求
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			log.Error("failed to close io.Reader, err = %v", err)
		}
	}()

	// 处理 HTTP 响应
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to send http post, status code is not 200")
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response body: %w", err)
	}
	log.Info("Response Body:", string(b))
	return nil
}
