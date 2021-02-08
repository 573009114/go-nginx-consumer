package main

import (
	"go-nginx-consumer/internal/consumers"
	"log"
	"os"
	"runtime"
)

//Cfg struct
type Cfg struct {
	kafkaAddr string
	ESAddr    string
	LogPath   string
	Topic     string
}

var (
	logCfg *Cfg
)

//GetEnv is func
func GetEnv(key string, defaultVale string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultVale
}

func loadEnv() {
	kafkaAddr := GetEnv("KAFKA_ENDPOINT", "10.91.129.250:9092")
	esAddr := GetEnv("ES_ENDPOINT", "HTTP://10.91.129.250:9200")
	topic := GetEnv("KAFKA_TOPIC", "haowen")

	logCfg = &Cfg{
		kafkaAddr: kafkaAddr,
		ESAddr:    esAddr,
		Topic:     topic,
	}
}

func main() {
	loadEnv()
	runtime.GOMAXPROCS(1)

	err := consumers.KafkaConsumer(logCfg.kafkaAddr, logCfg.Topic, logCfg.ESAddr)

	if err != nil {
		log.Fatal("consumers error", err)
	}

	// a := "[2021/02/07 17:51:54 [error] 11305#0: *600029462 connect() failed (111: Connectionrefused) while connecting to upstream  client: 36.112.24.11  server: test-guardians.ksyun.com  request: \"GET / HTTP/1.1\"  upstream:\"http://172.16.16.25:30008/\"  host: \"test-guardians.ksyun.com\"]"
	// fmt.Println(strings.Split(" ", a))

}
