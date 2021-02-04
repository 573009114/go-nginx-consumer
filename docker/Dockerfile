FROM golang

WORKDIR /app

ENV GO111MODULE on
ENV GOSUMDB off

COPY bin/go-consumer /app
RUN go env -w GOPROXY=https://goproxy.cn,direct && chmod +x go-consumer

CMD [ "./go-consumer" ]