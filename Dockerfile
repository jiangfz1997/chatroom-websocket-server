# 第一階段：構建 Go 程式
FROM golang:1.24.1 as builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o websocket_server .

# 第二階段：運行
FROM alpine:latest

WORKDIR /root/
COPY --from=builder /app/websocket_server .
# ✅ 複製 config 目錄
COPY --from=builder /app/config ./config
# 容器內執行的端口（8081）
EXPOSE 8081

CMD ["./websocket_server"]