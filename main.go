package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/crypto/ssh"
)

// Config 结构体映射 config.json
type Config struct {
	LogFile         string     `json:"logfile"`
	MQTT            MQTTConfig `json:"mqtt"`
	DefaultTempCmd  string     `json:"default_temp_cmd"`
	Hosts           []Host     `json:"hosts"`
}

type MQTTConfig struct {
	Broker     string `json:"broker"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	ClientID   string `json:"client_id"`
	TopicPrefix string `json:"topic_prefix"`
}

type Host struct {
	Name     string `json:"name"`
	IPAddr   string `json:"ipaddr"`
	User     string `json:"user"`
	Password string `json:"password"`
	TempCmd  string `json:"temp_cmd"`
	// 用于在循环中保持连接状态 (模拟原 PHP 逻辑)
	sshClient *ssh.Client 
}

var (
	config     Config
	mqttClient mqtt.Client
	logger     *log.Logger
	logFile    *os.File
)

func main() {
	// 1. 加载外部 JSON 配置
	configData, err := os.ReadFile("config.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取配置文件失败: %v\n", err)
		os.Exit(1)
	}

	if err := json.Unmarshal(configData, &config); err != nil {
		fmt.Fprintf(os.Stderr, "解析配置文件失败: %v\n", err)
		os.Exit(1)
	}

	// 2. 初始化日志
	logFile, err = os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "打开日志文件失败: %v\n", err)
		os.Exit(1)
	}
	defer logFile.Close()
	logger = log.New(logFile, "", log.LstdFlags)

	// 3. 初始化 MQTT 客户端
	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.MQTT.Broker)
	opts.SetClientID(config.MQTT.ClientID)
	opts.SetUsername(config.MQTT.Username)
	opts.SetPassword(config.MQTT.Password)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryTimeOut(5 * time.Second)
	
	// 简单的连接丢失处理
	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		logger.Printf("MQTT 连接丢失: %v", err)
	})

	mqttClient = mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		logger.Printf("警告：初始 MQTT 连接失败: %v，将在后台重试", token.Error())
	} else {
		logger.Println("MQTT 连接成功")
	}

	// 主循环
	for {
		hasFail := false

		for i := range config.Hosts {
			host := &config.Hosts[i]
			
			// 如果当前没有连接，则尝试建立 (模拟原逻辑)
			if host.sshClient == nil {
				client, err := createSSHConnection(host)
				if err != nil {
					logger.Printf("[%s] SSH 登录失败: %v", host.Name, err)
					hasFail = true
					continue
				}
				host.sshClient = client
				logger.Printf("[%s] SSH 连接建立成功", host.Name)
			}

			// 执行命令获取温度
			cmd := host.TempCmd
			if cmd == "" {
				cmd = config.DefaultTempCmd
			}

			output, err := runSSHCommand(host.sshClient, cmd)
			if err != nil {
				logger.Printf("[%s] 执行命令失败: %v", host.Name, err)
				// 如果执行失败，可能连接已断开，尝试关闭连接以便下次重连
				host.sshClient.Close()
				host.sshClient = nil
				hasFail = true
				continue
			}

			// 处理温度数据
			tempVal, err := parseTemperature(output)
			if err != nil {
				logger.Printf("[%s] 解析温度失败: %v (原始输出: %s)", host.Name, err, strings.TrimSpace(output))
				hasFail = true
				continue
			}

			// 构建 JSON 消息
			msgData := map[string]interface{}{
				"temp0":   tempVal,
				"modtime": time.Now().Unix(),
			}
			jsonBytes, err := json.Marshal(msgData)
			if err != nil {
				logger.Printf("[%s] 序列化 JSON 失败: %v", host.Name, err)
				continue
			}

			// 发布 MQTT 消息
			topic := fmt.Sprintf(config.MQTT.TopicPrefix, host.Name)
			token := mqttClient.Publish(topic, 0, false, string(jsonBytes))
			token.Wait()
			if token.Error() != nil {
				logger.Printf("[%s] MQTT 发布失败: %v", host.Name, token.Error())
			} else {
				logger.Printf("[%s] 已发布温度 %.1f 到 %s", host.Name, tempVal, topic)
			}

			time.Sleep(10 * time.Millisecond) // usleep(10)
		}

		time.Sleep(30 * time.Second) // sleep(30)

		if hasFail {
			logger.Println("检测到失败，退出程序")
			break
		}
	}
}

// createSSHConnection 创建带有 5s 超时的 SSH 连接
func createSSHConnection(host *Host) (*ssh.Client, error) {
	config := &ssh.ClientConfig{
		User: host.User,
		Auth: []ssh.AuthMethod{
			ssh.Password(host.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // 生产环境建议验证主机密钥
		Timeout:         5 * time.Second,             // 设置连接超时为 5s
	}

	addr := fmt.Sprintf("%s:22", host.IPAddr)
	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// runSSHCommand 执行远程命令
func runSSHCommand(client *ssh.Client, cmd string) (string, error) {
	session, err := client.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	output, err := session.CombinedOutput(cmd)
	return string(output), err
}

// parseTemperature 解析温度字符串并转换为浮点数 (除以 1000)
func parseTemperature(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	return float64(val) / 1000.0, nil
}
