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
	Index   string      `json:"_index"`
	Type    string      `json:"_type"`
	ID      string      `json:"_id"`
	Version int         `json:"_version"`
	Score   interface{} `json:"_score"`
	Source  struct {
		Version string `json:"@version"`
		// Timestamps           time.Time `json:"@timestamp"`
		Type                 string  `json:"type"`
		Path                 string  `json:"path"`
		Host                 string  `json:"host"`
		RemoteAddr           string  `json:"remote_addr"`
		RemoteUser           string  `json:"remote_user"`
		Timestamp            string  `json:"timestamp"`
		Request              string  `json:"request"`
		Status               string  `json:"status"`
		HTTPReferer          string  `json:"http_referer"`
		HTTPUserAgent        string  `json:"http_user_agent"`
		HTTPXForwardedFor    string  `json:"http_x_forwarded_for"`
		RemoteHost           string  `json:"remote_host"`
		UpstreamAddr         string  `json:"upstream_addr"`
		URI                  string  `json:"uri"`
		XKSCACCOUNTID        string  `json:"X-KSC-ACCOUNT-ID"`
		XKSCREQUESTID        string  `json:"X-KSC-REQUEST-ID"`
		BodyBytesSent        int     `json:"body_bytes_sent"`
		RequestTime          float64 `json:"request_time"`
		UpstreamResponseTime float64 `json:"upstream_response_time"`
	} `json:"_source"`
	Fields struct {
		Timestamp []time.Time `json:"@timestamp"`
	} `json:"fields"`
	Sort []int64 `json:"sort"`
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
		Index(msg.Index).
		Type(topic).
		BodyJson(msg.Source).
		Do(context.Background())

	if err != nil {
		fmt.Println(msg.Source)
		log.Printf("error: %s", err)
	} else {
		log.Printf("%s insert success!", msg.Index)
	}
	return
}
