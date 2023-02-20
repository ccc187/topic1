FROM registry-vpc.cn-hangzhou.aliyuncs.com/hubpd/golang-builder:2.0 as builder
ENV GOPRIVATE="dm-gitlab.bolo.me" GOPROXY="https://goproxy.cn,direct" GONOPROXY="dm-gitlab.bolo.me" GONOSUMDB="dm-gitlab.bolo.me" GO111MODULE="on"
WORKDIR /builder/
# 把那些最不容易发生变化的文件的拷贝操作放在较低的镜像层中，这样在重新 build 镜像时就会使用前面 build 产生的缓存。
COPY go.* ./
# can cached
RUN go mod download
COPY . ./
# 使用交叉编译， 这一层除非代码一点没动， 否则不可能使用缓存
RUN CGO_ENABLED=1 go build -o app *.go

FROM registry-vpc.cn-hangzhou.aliyuncs.com/hubpd/alpine-shanghai:2.0
COPY --from=builder /builder/app .
COPY --from=builder /builder/migration ./migration
ENTRYPOINT ["/app"]
