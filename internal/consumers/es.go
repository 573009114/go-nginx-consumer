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
	Fields struct {
		LogTopics string `json:"log_topics"`
	} `json:"fields"`
	Beat struct {
		Name     string `json:"name"`
		Hostname string `json:"hostname"`
		Version  string `json:"version"`
	} `json:"beat"`
	Source     string `json:"source"`
	Offset     int    `json:"offset"`
	Message    string `json:"message"`
	Prospector struct {
		Type string `json:"type"`
	} `json:"prospector"`
}

//LogBody struct
type LogBody struct {
	Version              string `json:"@version"`
	Timestamp            string `json:"timestamp"`
	Type                 string `json:"type"`
	Path                 string `json:"path"`
	Host                 string `json:"host"`
	RemoteAddr           string `json:"remote_addr"`
	RemoteUser           string `json:"remote_user"`
	Request              string `json:"request"`
	Status               string `json:"status"`
	HTTPReferer          string `json:"http_referer"`
	HTTPUserAgent        string `json:"http_user_agent"`
	HTTPXForwardedFor    string `json:"http_x_forwarded_for"`
	RemoteHost           string `json:"remote_host"`
	UpstreamAddr         string `json:"upstream_addr"`
	URI                  string `json:"uri"`
	XKSCACCOUNTID        string `json:"X-KSC-ACCOUNT-ID"`
	XKSCREQUESTID        string `json:"X-KSC-REQUEST-ID"`
	BodyBytesSent        string `json:"body_bytes_sent"`
	RequestTime          string `json:"request_time"`
	UpstreamResponseTime string `json:"upstream_response_time"`
	Fields               struct {
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

	p := &LogBody{}
	err = json.Unmarshal([]byte(msg.Message), &p)
	if err != nil {
		return
	}
	fmt.Println(p)

	filesname := filepath.Base(msg.Source)
	// fmt.Println(reflect.TypeOf(msg.Timestamp))

	timers := msg.Timestamp.Local()
	t := timers.Format("2006-01-02")

	indexname := filesname + "-" + t

	// newMsg, _ := json.Marshal(msg)
	// fmt.Println(newMsg)

	//创建索引以及写入数据
	_, err = c.Index().
		Index(indexname).
		Type(topic).
		BodyJson(p).
		Do(context.Background())

	if err != nil {
		log.Printf("error: %s", err)
	} else {
		log.Printf("%s insert success!", msg.Message)
	}
	return
}
