package dao

import (
	"context"
	"dm-gitlab.bolo.me/hubpd/topic/config"
	"dm-gitlab.bolo.me/hubpd/topic/model"
	"encoding/json"
	"fmt"
	"github.com/getsentry/sentry-go"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"math/rand"
	"time"
)

type Redis struct {
	Log                *logrus.Entry
	RedisClusterClient *redis.ClusterClient
}

var RedisInstance *Redis

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (r *Redis) SetTopics(ctx context.Context, topicInfos []*model.TopicInfo) error {
	pipe := r.RedisClusterClient.Pipeline()
	for _, topicInfo := range topicInfos {
		if topicInfo.TopicDetail != nil {
			topicInfoJson, _ := json.Marshal(topicInfo)
			exp := time.Duration(10+rand.Intn(20)) * time.Hour // [10, 30)
			pipe.Set(ctx, model.GetKeyForTopic(topicInfo.TopicDetail.ID), topicInfoJson, exp)
		}
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		currErr := fmt.Errorf("[dao redis] SetTopics pipe.Set err: %v, topicInfos: %v", err, topicInfos)
		r.Log.Error(currErr)
		sentry.CaptureException(currErr)

		return err
	}

	return nil
}

func (r *Redis) GetTopics(ctx context.Context, topicIds []int64) (map[int64]*model.TopicInfo, error) {
	topicInfoMap := make(map[int64]*model.TopicInfo, 0)

	pipe := r.RedisClusterClient.Pipeline()
	for _, topicId := range topicIds {
		pipe.Get(ctx, model.GetKeyForTopic(topicId))
	}
	res, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		currErr := fmt.Errorf("[dao redis] GetTopics pipe.Get err: %v, topicIds: %v", err, topicIds)
		r.Log.Error(currErr)
		sentry.CaptureException(currErr)

		return topicInfoMap, err
	}

	for _, resItem := range res {
		topicInfo := &model.TopicInfo{}
		if b, ok := resItem.(*redis.StringCmd); ok {
			if b.Val() == "" {
				continue
			}
			if err := json.Unmarshal([]byte(b.Val()), topicInfo); err != nil {
				currErr := fmt.Errorf("[dao redis] GetTopics Unmarshal err: %v, topicIds: %v", err, topicIds)
				r.Log.Error(currErr)
				sentry.CaptureException(currErr)
				continue
			}
		} else {
			continue
		}
		if topicInfo.TopicDetail != nil {
			topicInfoMap[topicInfo.TopicDetail.ID] = topicInfo
		}
	}

	return topicInfoMap, nil
}

func (r *Redis) Lock(ctx context.Context, lockKey string) error {
	r.Log.Infof("[dao] redis lock, will setNX, key: %v, value: %v, expiration: %v",
		lockKey, model.RedisLockSecret, time.Duration(config.Cfg.RedisLockExpirationSec)*time.Second)

	resp := r.RedisClusterClient.SetNX(
		ctx, lockKey, model.RedisLockSecret, time.Duration(config.Cfg.RedisLockExpirationSec)*time.Second)
	lockSuccess, err := resp.Result()
	if err != nil {
		lockFailErr := fmt.Errorf("[dao] redis lock SetNX fail, key: %v, err: %v", lockKey, err)
		r.Log.Error(lockFailErr)
		sentry.CaptureException(lockFailErr)
		return err
	}
	if !lockSuccess {
		lockFailErr := fmt.Errorf("[dao] redis lock SetNX fail, key: %v, result: %v", lockKey, lockSuccess)
		r.Log.Info(lockFailErr)
		return lockFailErr
	}

	return nil
}

func (r *Redis) UnLock(ctx context.Context, lockKey string) error {
	r.Log.Infof("[dao] redis unLock, will checkAndDel key, key: %v, value: %v", lockKey, model.RedisLockSecret)

	script := redis.NewScript(`
		if redis.call('get', KEYS[1]) == ARGV[1] 
			then 
				return redis.call('del', KEYS[1]) 
			else 
				return 0 
			end
		`)
	scriptResp, err := script.Run(ctx, r.RedisClusterClient, []string{lockKey}, []interface{}{model.RedisLockSecret}).Int()
	if err != nil {
		unLockFailErr := fmt.Errorf("[dao] redis unLock checkAndDel key fail, key: %v, err: %v", lockKey, err)
		r.Log.Error(unLockFailErr)
		sentry.CaptureException(unLockFailErr)
		return err
	}
	if scriptResp == 0 {
		unLockFailErr := fmt.Errorf("[dao] redis unLock checkAndDel key fail, key: %v, result: %v", lockKey, scriptResp)
		r.Log.Info(unLockFailErr)
		//sentry.CaptureException(unLockFailErr)
		//return unLockFailErr
	}

	return nil
}

func (r *Redis) LockWrap(ctx context.Context, lockKey string, f func() error) (err error) {
	// redis lock
	if err = r.Lock(ctx, lockKey); err != nil {
		return
	}
	// redis unLock
	defer func() {
		if unLockErr := r.UnLock(ctx, lockKey); unLockErr != nil {
			err = unLockErr
		}
	}()

	if err = f(); err != nil {
		r.Log.Errorf("[redis] LockWrap f() return err: %v", err)
		return
	}

	return
}

func (r *Redis) Del(ctx context.Context, keys []string) error {
	pipe := r.RedisClusterClient.Pipeline()
	for _, key := range keys {
		pipe.Del(ctx, key)
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		currErr := fmt.Errorf("[dao redis] Del pipe.Del err: %v, keys: %v", err, keys)
		r.Log.Error(currErr)
		sentry.CaptureException(currErr)
		return err
	}

	return nil
}

// 注意 bitmap只增勿删
func (r *Redis) SetBitTopics(ctx context.Context, ids []int64) error {
	pipe := r.RedisClusterClient.Pipeline()
	for _, id := range ids {
		if id == 0 {
			return fmt.Errorf("[dao redis] SetBitTopics id == 0, ids: %v", ids)
		}
		key, offset := model.GetKeyForTopicsBitMap(id)
		pipe.SetBit(ctx, key, offset, 1)
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		currErr := fmt.Errorf("[dao redis] SetBitTopics pipe.Set err: %v, ids: %v", err, ids)
		r.Log.Error(currErr)
		sentry.CaptureException(currErr)

		return err
	}

	return nil
}

func (r *Redis) GetBitTopics(ctx context.Context, ids []int64) ([]int64, error) {
	resIDs := make([]int64, 0)

	pipe := r.RedisClusterClient.Pipeline()
	for _, id := range ids {
		if id == 0 {
			return resIDs, fmt.Errorf("[dao redis] GetBitTopics id == 0, ids: %v", ids)
		}
		key, offset := model.GetKeyForTopicsBitMap(id)
		pipe.GetBit(ctx, key, offset)
	}
	res, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		currErr := fmt.Errorf("[dao redis] GetBitTopics pipe.Set err: %v, ids: %v", err, ids)
		r.Log.Error(currErr)
		sentry.CaptureException(currErr)

		return resIDs, err
	}

	for i, resItem := range res {
		if v := resItem.(*redis.IntCmd).Val(); v == 1 {
			if i < len(ids) {
				resIDs = append(resIDs, ids[i])
			} else {
				r.Log.Errorf("[dao redis] GetBitTopics idx >= len(ids), ids: %v", ids)
			}
		}
	}

	return resIDs, nil
}
