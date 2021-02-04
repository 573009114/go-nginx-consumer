package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	elastic "gopkg.in/olivere/elastic.v6"
)

//NgxMessage 结构体定义
type NgxMessage struct {
	RemoteAddr           string `json:"remote_addr"`
	RemoteUser           string `json:"remote_user"`
	Timestamp            string `json:"timestamp"`
	Request              string `json:"request"`
	Status               string `json:"status"`
	BodyBytesSent        string `json:"body_bytes_sent"`
	HTTPReferer          string `json:"http_referer"`
	HTTPUserAgent        string `json:"http_user_agent"`
	HTTPXForwardedFor    string `json:"http_x_forwarded_for"`
	RequestTime          string `json:"request_time"`
	RemoteHost           string `json:"remote_host"`
	UpstreamResponseTime string `json:"upstream_response_time"`
	UpstreamAddr         string `json:"upstream_addr"`
	URI                  string `json:"uri"`
	// XKSCACCOUNTID        string `json:"X-KSC-ACCOUNT-ID"`
	// XKSCREQUESTID        string `json:"X-KSC-REQUEST-ID"`
}

var (
	esClient *elastic.Client
)

//Elastichandle 操作es
func Elastichandle(addr string, topic string, data []byte) (err error) {

	c, err := elastic.NewClient(
		elastic.SetURL(addr),
		elastic.SetSniff(false),
		elastic.SetHealthcheckInterval(10*time.Second),
		elastic.SetGzip(true),
	)
	if err != nil {
		log.Fatal(err)
	}

	msg := &NgxMessage{}
	err = json.Unmarshal(data, &msg)
	if err != nil {
		return
	}

	//创建索引以及写入数据
	_, err = c.Index().
		Index(topic).
		Type(topic).
		BodyJson(msg).
		Do(context.Background())

	if err != nil {
		fmt.Println(err)
	}
	return
}
