FROM golang:1.12-alpine
ENV GO111MODULE=on
RUN apk update
RUN apk add openssl ca-certificates git make build-base
WORKDIR /go/src/github.com/akkeris/s3-broker
COPY . .
RUN make
CMD ./servicebroker -insecure -logtostderr=1 -stderrthreshold 0 
