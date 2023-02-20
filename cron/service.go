package cron

import (
	"context"
	"dm-gitlab.bolo.me/hubpd/basic/logger"
	"dm-gitlab.bolo.me/hubpd/topic/dao"
	"dm-gitlab.bolo.me/hubpd/topic/model"
	"dm-gitlab.bolo.me/hubpd/topic/service"
)

func (c *Cron) UpdateTopicStatistic() {
	ctx := context.Background()

	f := func() error {
		err := service.Instance.UpdateTopicStatistic(ctx)
		if err != nil {
			return err
		}
		return nil
	}

	// with redis lock
	if err := dao.RedisInstance.LockWrap(ctx, model.UpdateTopicStatistic, f); err != nil {
		logger.GetLogger().Errorf("[cron] UpdateTopicStatistic dao.RedisInstance.LockWrap err: %v", err)
	}
}

func (c *Cron) UpdateTopicStatus() {
	ctx := context.Background()

	f := func() error {
		err := service.Instance.UpdateTopicStatus(ctx)
		if err != nil {
			return err
		}
		return nil
	}

	// with redis lock
	if err := dao.RedisInstance.LockWrap(ctx, model.UpdateTopicStatus, f); err != nil {
		logger.GetLogger().Errorf("[cron] UpdateTopicStatus dao.RedisInstance.LockWrap err: %v", err)
	}
}
