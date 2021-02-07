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
		XKSCACCOUNTID        string `json:"X-KSC-ACCOUNT-ID"`
		XKSCREQUESTID        string `json:"X-KSC-REQUEST-ID"`
		Fields               struct {
			Timestamp string `json:"timestamp"`
		} `json:"fields"`
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
	err = json.Unmarshal([]byte(msg.Message), &msg.Msg)

	if err != nil {
		return
	}

	// bf := json.NewEncoder(msg)
	// bf.SetEscapeHTML(false)
	// fmt.Println(bf)
	filesname := filepath.Base(msg.Source)
	// fmt.Println(reflect.TypeOf(msg.Timestamp))

	timers := msg.Timestamp.Local()
	t := timers.Format("2006-01-02")

	indexname := filesname + "-" + t

	// newMsg, _ := json.Marshal(msg)
	// fmt.Println(newMsg)

	fmt.Println(msg.Msg)
	//创建索引以及写入数据
	_, err = c.Index().
		Index(indexname).
		Type(topic).
		BodyJson(msg.Msg).
		Do(context.Background())

	if err != nil {
		log.Printf("error: %s", err)
	} else {
		log.Printf("insert success!")
	}
	return
}
