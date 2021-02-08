package consumers

import (
	"log"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/astaxie/beego/logs"
)

//KafkaConsumer is func
func KafkaConsumer(addr string, topic string, ES string) (err error) {
	// 生成消费者 实例
	c, err := sarama.NewConsumer(strings.Split(addr, ","), nil)

	if err != nil {
		logs.Error("无法创建消费者", err)
		return
	}
	// 拿到 对应主题下所有分区
	partitionList, err := c.Partitions(topic)
	if err != nil {
		logs.Error("设置分区错误", err)
		return
	}

	var (
		wg sync.WaitGroup
	)

	wg.Add(1)
	// 遍历所有分区
	for partition := range partitionList {
		defer wg.Done()
		//消费者 消费 对应主题的 具体 分区 指定 主题 分区 offset  return 对应分区的对象
		p, err := c.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			logs.Error("循环获取消费分区错误 %d：%s", p, err)
		}

		defer p.AsyncClose()
		go func(sarama.PartitionConsumer) {
			for msg := range p.Messages() {
				data := msg.Value
				//写入es
				err := Elastichandle(ES, topic, data)
				if err != nil {
					log.Printf("%s", err)
					continue
				}
			}

		}(p)
	}
	wg.Wait()
	c.Close()
	return nil
}
