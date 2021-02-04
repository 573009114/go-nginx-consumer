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
	topic := GetEnv("KAFKA_TOPIC", "default")

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

}
