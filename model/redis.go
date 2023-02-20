package model

import (
	"context"
	"dm-gitlab.bolo.me/hubpd/basic/logger"
	"dm-gitlab.bolo.me/hubpd/topic/config"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	"github.com/getsentry/sentry-go"
	"github.com/go-redis/redis/v8"
	uuid "github.com/satori/go.uuid"
	"time"
)

var RedisLockSecret string

func init() {
	RedisLockSecret = uuid.NewV1().String()
	logger.GetLogger().Infof("RedisLockSecret: %v", RedisLockSecret)
}

func GetRedis() (clusterRdb *redis.ClusterClient) {
	clusterRdb = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:              config.Cfg.RedisDsn,
		PoolSize:           10,
		IdleTimeout:        5 * time.Minute,
		Password:           config.Cfg.RedisPassword,
		MaxRetries:         3,
		IdleCheckFrequency: time.Minute,
	})
	statusRes, err := clusterRdb.Ping(context.Background()).Result()
	if err != nil {
		pingErr := fmt.Errorf("clusterRdb.Ping fail, err: %v", err)
		sentry.CaptureException(pingErr)
		logger.GetLogger().Fatal(pingErr)
	}
	if statusRes != "PONG" {
		pingErr := fmt.Errorf("clusterRdb.Ping fail, result: %v != PONG", statusRes)
		sentry.CaptureException(pingErr)
		logger.GetLogger().Fatal(pingErr)
	}

	return
}

func GetRedisMock() (clusterRdb *redis.ClusterClient) {
	mock, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	clusterRdb = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        []string{mock.Addr()},
		PoolSize:     100,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		PoolTimeout:  30 * time.Second,
	})

	return
}

// redis key
var (
	UpdateTopicStatistic      = config.Cfg.RedisPrefix + ":lock" + ":updateTopicStatistic"
	UpdateTopicStatus         = config.Cfg.RedisPrefix + ":lock" + ":updateTopicStatus"
	Topic                     = config.Cfg.RedisPrefix + ":topic"
	KeyLockTopic              = config.Cfg.RedisPrefix + ":lock" + ":topic"           // 分布式锁：控制台修改
	KeyLockTopicForGetsByTiDB = config.Cfg.RedisPrefix + ":lockGetsByTiDB" + ":topic" // 缓存击穿锁
	KeyBitMapTopic            = config.Cfg.RedisPrefix + ":bitMap" + ":topic"         // 缓存穿透过滤器
)

func GetKeyForTopic(id int64) string {
	return Topic + fmt.Sprintf(":%v", id)
}

func GetKeyForLockTopic(id int64) string {
	return KeyLockTopic + fmt.Sprintf(":%v", id)
}

func GetKeyForLockTopicForGetsByTiDB(id int64) string {
	return KeyLockTopicForGetsByTiDB + fmt.Sprintf(":%v", id)
}

func GetKeyForTopicsBitMap(id int64) (key string, offset int64) {
	if id == 0 {
		return
	}

	var part int64

	subID := id % 1e10
	if subID >= 1e8 {
		part = subID / 1e8
	} else {
		part = 100
	}

	offset = id % 1e8

	return KeyBitMapTopic + fmt.Sprintf(":%v", part), offset
}
