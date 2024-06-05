package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"

	"database/sql"
	"net/smtp"

	_ "github.com/go-sql-driver/mysql"
)

var (
	MyEmail        = "1457811364@qq.com" //发送者的邮箱号
	MyPassword     = ""                  //暂时不写，哈哈     //密码
	RecipientEmail = ""                  //接收者的邮箱号
)

type SystemMetrics struct {
	CPUPercent  float64
	MemoryTotal uint64
	MemoryUsed  uint64
	MemoryFree  uint64
}

func main() {
	// 打开日志文件
	logFile, err := os.OpenFile("logfile.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("无法打开日志文件:", err)
	}
	defer logFile.Close()

	// 设置日志输出目标为文件
	log.SetOutput(logFile)

	// 初始化配置
	interval := 5 * time.Second // 监控间隔
	threshold := 80.0           // 告警阈值（如果占用CPU超过百分之八十了就开始报警）

	// 持续监控系统指标
	for {
		metrics := collectSystemMetrics()

		// 存储监控数据到数据库
		if err := storeMetricsToDatabase(metrics); err != nil {
			log.Println("Error storing metrics:", err)
		}

		// 检查告警
		checkAlert(metrics, threshold)

		// 休眠一段时间
		time.Sleep(interval)
	}
}

// collectSystemMetrics 获取KV存储器的各项数据指标
func collectSystemMetrics() SystemMetrics {
	// 获取CPU使用率
	cpuUsage, _ := cpu.Percent(0, false)

	// 获取内存使用情况
	memInfo, _ := mem.VirtualMemory()

	return SystemMetrics{
		CPUPercent:  cpuUsage[0],   // 最新的数据库CPU 占比
		MemoryTotal: memInfo.Total, // 存储引擎的内存总量
		MemoryUsed:  memInfo.Used,  // 存储引擎目前使用的内存量
		MemoryFree:  memInfo.Free,  // 存储引擎当前空闲的内存量
	}
}

// storeMetricsToDatabase 将KV存储器的各项数据指标存储在MySQL数据库中
func storeMetricsToDatabase(metrics SystemMetrics) error {
	// 连接到 MySQL 数据库
	db, err := sql.Open("mysql", "root:wssl20050419.@tcp(localhost:3306)/db3")
	if err != nil {
		return err
	}
	defer db.Close()

	// 插入数据
	_, err = db.Exec("INSERT INTO metrics (cpu_percent, memory_total, memory_used, memory_free) VALUES (?, ?, ?, ?)",
		metrics.CPUPercent, metrics.MemoryTotal, metrics.MemoryUsed, metrics.MemoryFree)
	if err != nil {
		return err
	}

	return nil
}

// checkAlert 检查是否需要发送警报信息
func checkAlert(metrics SystemMetrics, threshold float64) {
	// 检查CPU使用率是否超过阈值
	if metrics.CPUPercent > threshold {
		// 触发告警通知
		sendAlert(fmt.Sprintf("CPU usage exceeds threshold: %.2f%%", metrics.CPUPercent))
	}
}

// sendAlert 发送警报信息
func sendAlert(message string) {
	// 设置发件人邮箱地址和授权码
	from := MyEmail
	password := MyPassword

	// 设置收件人邮箱地址
	to := RecipientEmail

	// 设置 SMTP 服务器地址和端口号
	smtpHost := "smtp.qq.com"
	smtpPort := 587 //安全第一！

	// 构造邮件内容
	subject := "Alert!"
	body := "Alert message: " + message
	msg := "Subject: " + subject + "\r\n" +
		"To: " + to + "\r\n" +
		"From: " + from + "\r\n" +
		"\r\n" +
		body + "\r\n"

	// 配置 SMTP 客户端
	auth := smtp.PlainAuth("", from, password, smtpHost)

	// 连接 SMTP 服务器
	conn, err := smtp.Dial(fmt.Sprintf("%s:%d", smtpHost, smtpPort))
	if err != nil {
		log.Println("连接SMTP服务器失败:", err)
		return
	}
	defer conn.Close()

	// 发送身份认证
	if err = conn.Auth(auth); err != nil {
		log.Println("身份认证失败:", err)
		return
	}

	// 发送邮件
	if err = conn.Mail(from); err != nil {
		log.Println("设置发件人失败:", err)
		return
	}

	if err = conn.Rcpt(to); err != nil {
		log.Println("设置收件人失败:", err)
		return
	}

	data, err := conn.Data()
	if err != nil {
		log.Println("准备发送邮件失败:", err)
		return
	}
	defer data.Close()

	_, err = data.Write([]byte(msg))
	if err != nil {
		log.Println("发送邮件失败:", err)
		return
	}

	log.Println("邮件发送成功")

	log.Println("Sending alert:", message)
}
