package config

import (
	"github.com/koding/multiconfig"
)

type Hubpd struct {
	GrpcServerAddress      string   `required:"true" default:"127.0.0.1:5000"`
	SentryDsn              string   `required:"true" default:"https://6c4df933fae649f586f30cbab96dddd4@sentry-v.bolo.me/59"`
	KafkaHosts             []string `required:"true" default:"127.0.0.1:9092"`
	DBDsn                  string   `default:"root:pwd@tcp(127.0.0.1:3306)/topic_test?charset=utf8&parseTime=True&loc=Asia%2FShanghai&timeout=10s"`
	DBMaxIdleConns         int      `default:"2"`
	DBMaxOpenConns         int      `default:"4"`
	Debug                  bool     `default:"true"`
	Migrate                bool     `default:"false"`
	ExecuteMigration       bool     `default:"false"`
	GRPCClientAddressBI    string   `default:"127.0.0.1:8116"` // bi服务
	EnableCron             bool     `default:"true"`
	RedisDsn               []string `default:"0.0.0.0:7000,0.0.0.0:7001,0.0.0.0:7002"`
	RedisPassword          string   `default:""`
	RedisPrefix            string   `default:"topicSvc"`
	RedisLockExpirationSec int      `default:"120"`
	IsMysql                bool
}

var Cfg = &Hubpd{}

func init() {
	multiconfig.New().MustLoad(Cfg)
}
