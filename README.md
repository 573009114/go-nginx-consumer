### 功能

获取kafka 数据 -> 存入es -> kibana展示
目前功能基于es 6.x开发

### nginx 日志格式定义
```
log_format  main   '{'
    ' "remote_addr": "$remote_addr",'
    ' "remote_user": "$remote_user",'
    ' "timestamp": "$time_local",'
    ' "request": "$request",'
    ' "status": "$status",'
    ' "body_bytes_sent": "$body_bytes_sent",'
    ' "http_referer": "$http_referer",'
    ' "http_user_agent": "$http_user_agent",'
    ' "http_x_forwarded_for": "$http_x_forwarded_for",'
    ' "request_time": "$request_time",'
    ' "remote_host": "$host",'
    ' "upstream_response_time": "$upstream_response_time",'
    ' "upstream_addr": "$upstream_addr",'
    ' "uri": "$uri"'
'}';

```
### filebeat6.1.1 配置示例
```
filebeat.prospectors:
- type: log
  enabled: true
  paths:
    - /data/logs/nginx/access.log
  exclude_files: ['.supervisor.log$']
  ignore_older: 2h
  tail_files: true
  fields:
    log_topics: nginx
output.kafka:
  hosts: ["172.16.16.8:8000", "172.16.16.16:8002", "172.16.16.19:8004"]
  topic: "%{[fields][log_topics]}"
  worker: 3

```

### 环境变量配置
```
KAFKA_ENDPOINT :  192.168.0.x:9092
ES_ENDPOINT:      http://192.168.1.x:9200
KAFKA_TOPIC:      test-topic
```
