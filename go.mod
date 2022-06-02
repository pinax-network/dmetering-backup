module github.com/streamingfast/dmetering

go 1.16

require (
	cloud.google.com/go v0.43.0
	github.com/go-redis/redis v6.15.9+incompatible // indirect
	github.com/go-redis/redis/v8 v8.11.5
	github.com/golang/protobuf v1.5.2
	github.com/streamingfast/dauth v0.0.0-20210812020920-1c83ba29add1
	github.com/streamingfast/logging v0.0.0-20220304214715-bc750a74b424
	github.com/stretchr/testify v1.7.0
	go.uber.org/atomic v1.7.0
	go.uber.org/zap v1.21.0
	google.golang.org/api v0.7.0
	google.golang.org/grpc v1.26.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

replace github.com/streamingfast/dauth => /Users/work/GoLand/pinax-dauth
