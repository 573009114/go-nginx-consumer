package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	Beat struct {
		Name     string `json:"name"`
		Hostname string `json:"hostname"`
		Version  string `json:"version"`
	} `json:"beat"`
	Offset     int    `json:"offset"`
	Message    string `json:"message"`
	Source     string `json:"source"`
	Prospector struct {
		Type string `json:"type"`
	} `json:"prospector"`
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

	fmt.Println(msg.Source)

	//创建索引以及写入数据
	_, err = c.Index().
		Index(msg.Prospector.Type).
		Type(topic).
		BodyJson(msg.Message).
		Do(context.Background())

	if err != nil {
		log.Printf("error: %s", err)
	} else {
		log.Printf("%s insert success!", msg.Message)
	}
	return
}
