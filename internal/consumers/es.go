package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"time"

	elastic "gopkg.in/olivere/elastic.v6"
)

//NgxMessage 定义消费日志结构体
type NgxMessage struct {
	Timestamp time.Time `json:"@timestamp"`
	Metadata  struct {
		Beat    string `json:"beat"`
		Type    string `json:"type"`
		Version string `json:"version"`
		Topic   string `json:"topic"`
	} `json:"@metadata"`
	Message string `json:"message"`
	Msg     struct {
		RemoteAddr           string `json:"remote_addr,omitempty"`
		RemoteUser           string `json:"remote_user,omitempty"`
		Timestamp            string `json:"timestamp,omitempty"`
		Request              string `json:"request,omitempty"`
		Status               string `json:"status,omitempty"`
		BodyBytesSent        string `json:"body_bytes_sent,omitempty"`
		HTTPReferer          string `json:"http_referer,omitempty"`
		HTTPUserAgent        string `json:"http_user_agent,omitempty"`
		HTTPXForwardedFor    string `json:"http_x_forwarded_for,omitempty"`
		RequestTime          string `json:"request_time,omitempty"`
		RemoteHost           string `json:"remote_host,omitempty"`
		UpstreamResponseTime string `json:"upstream_response_time,omitempty"`
		UpstreamAddr         string `json:"upstream_addr,omitempty"`
		URI                  string `json:"uri,omitempty"`
		XKSCACCOUNTID        string `json:"X-KSC-ACCOUNT-ID,omitempty"`
		XKSCREQUESTID        string `json:"X-KSC-REQUEST-ID,omitempty"`
	} `json:"msg"`
	Source     string `json:"source"`
	Offset     int    `json:"offset"`
	Prospector struct {
		Type string `json:"type"`
	} `json:"prospector"`

	Beat struct {
		Name     string `json:"name"`
		Hostname string `json:"hostname"`
		Version  string `json:"version"`
	} `json:"beat"`
}

var (
	esClient *elastic.Client
)

//Elastichandle 操作es
func Elastichandle(addr string, topic string, data []byte) (err error) {

	c, err := elastic.NewClient(
		elastic.SetURL(addr),
		elastic.SetSniff(false),
		elastic.SetHealthcheckInterval(60*time.Second),
		elastic.SetGzip(true),
	)
	if err != nil {
		log.Fatal(err)
	}

	msg := &NgxMessage{}

	//序列化kafka接收的数据
	err = json.Unmarshal(data, &msg)
	if err != nil {
		log.Print("10001 非标准格式，无法序列化 ", err)
		return
	}

	//序列化message字段的信息
	// fmt.Println(reflect.TypeOf(msg.Message))

	err = json.Unmarshal([]byte(msg.Message), &msg.Msg)

	if err != nil {
		log.Print("10002 非标准格式，无法序列化 ", err)
	}

	//获取日志文件名
	filesname := filepath.Base(msg.Source)

	//转换时间为CST时间
	timers := msg.Timestamp.Local()
	t := timers.Format("2006-01-02")

	//拼接索引
	indexname := filesname + "-" + t

	fmt.Println(msg)
	//创建索引以及写入数据
	_, err = c.Index().
		Index(indexname).
		Type(topic).
		BodyJson(msg).
		Do(context.Background())

	if err != nil {
		log.Printf("10003 ES写入错误 %s", err)
	} else {
		log.Printf("10004 ES写入成功!")
	}
	return
}
