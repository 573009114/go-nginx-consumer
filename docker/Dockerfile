FROM golang

WORKDIR /app

ENV GO111MODULE on
ENV GOSUMDB off

COPY . /app
RUN go env -w GOPROXY=https://goproxy.cn,direct

CMD [ "./bin/go-consumer" ]