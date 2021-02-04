FROM hub.kce.ksyun.com/ksc-ai/ksc-ki-hub/golang:85d1774-go1.14.7 as builder

WORKDIR /app

ENV GO111MODULE on
ENV GOSUMDB off

RUN go env -w GOPROXY=https://goproxy.cn,direct

COPY cmd/go-consumer /app
CMD [ "cmd/go-consumer" ]