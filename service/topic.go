package service

import (
	"context"
	core "dm-gitlab.bolo.me/hubpd/basic/grpc"
	basicUtil "dm-gitlab.bolo.me/hubpd/basic/util"
	"dm-gitlab.bolo.me/hubpd/proto/bi"
	"dm-gitlab.bolo.me/hubpd/proto/event"
	"dm-gitlab.bolo.me/hubpd/proto/topic"
	pb "dm-gitlab.bolo.me/hubpd/proto/topic_grpc"
	"dm-gitlab.bolo.me/hubpd/topic/common"
	"dm-gitlab.bolo.me/hubpd/topic/dao"
	"dm-gitlab.bolo.me/hubpd/topic/model"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/getsentry/sentry-go"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"time"
)

type Service struct {
	Log               *logrus.Entry
	BIChartDataClient bi.ChartDataClient
	Pub               *core.Publisher
}

var Instance *Service

func (service *Service) CreateTopic(ctx context.Context, topicDetail *model.TopicDetail) error {
	if err := dao.TiDBInstance.CreateTopic(ctx, topicDetail); err != nil {
		currErr := fmt.Errorf("[service] CreateTopic dao.TiDBInstance.CreateTopic err: %v", err)
		service.Log.Error(currErr)
		sentry.CaptureException(currErr)
		return err
	}

	f := func() error {
		// 添加到bloom filter
		if err := dao.RedisInstance.SetBitTopics(ctx, []int64{topicDetail.ID}); err != nil {
			currErr := fmt.Errorf("[service] CreateTopic BloomFilter dao.RedisInstance.SetBitTopics err: %v", err)
			service.Log.Error(currErr)
			sentry.CaptureException(currErr)

			// TiDB 补偿
			rowsAffected, delErr := dao.TiDBInstance.DelTopicByIdsWithoutUserBehavior(ctx, []int64{topicDetail.ID})
			if delErr != nil {
				currDelErr := fmt.Errorf("[service] CreateTopic 补偿 dao.TiDBInstance.DelTopicByIds err: %v ", delErr)
				service.Log.Error(currDelErr)
				sentry.CaptureException(currDelErr)
				return currDelErr
			}
			if rowsAffected != 1 {
				rowsAffectedErr := fmt.Errorf(
					"[service] CreateTopic 补偿 dao.TiDBInstance.DelTopicByIds rowsAffected: %v != 1", rowsAffected)
				service.Log.Error(rowsAffectedErr)
				sentry.CaptureException(rowsAffectedErr)
				return rowsAffectedErr
			}

			return err
		}

		// Index to es
		basicUtil.PubEvent(service.Pub, topic.EV_DM_TOPIC, event.DataEventUint64_NEW, topicDetail.ID)

		return nil
	}

	// lock
	return dao.RedisInstance.LockWrap(ctx, model.GetKeyForLockTopic(topicDetail.ID), f)
}

func (service *Service) UpdateTopic(ctx context.Context, topicDetail *model.TopicDetail) (int64, error) {
	var rowsAffected int64

	f := func() error {
		// Redis
		if err := dao.RedisInstance.Del(ctx, []string{model.GetKeyForTopic(topicDetail.ID)}); err != nil {
			currErr := fmt.Errorf("[service] UpdateTopic dao.RedisInstance.Del err: %v, key: %v",
				err, model.GetKeyForTopic(topicDetail.ID))
			service.Log.Error(currErr)
			sentry.CaptureException(currErr)
			return err
		}

		// TiDB
		var updateErr error
		rowsAffected, updateErr = dao.TiDBInstance.UpdateTopicWithoutUserBehavior(ctx, topicDetail)
		if updateErr != nil {
			currErr := fmt.Errorf("[service] UpdateTopic dao.TiDBInstance.UpdateTopicWithoutUserBehavior err: %v id: %v",
				updateErr, topicDetail.ID)
			service.Log.Error(currErr)
			sentry.CaptureException(currErr)
			return updateErr
		}
		if rowsAffected != 1 {
			service.Log.Infof("[service] UpdateTopic dao.TiDBInstance.UpdateTopic rowsAffected: %v != 1", rowsAffected)
			return nil
		} else {
			// Index to es
			basicUtil.PubEvent(service.Pub, topic.EV_DM_TOPIC, event.DataEventUint64_NEW, topicDetail.ID)
		}

		// 延时双删
		go func() {
			time.Sleep(200 * time.Millisecond)

			ddl := time.Now().Add(time.Duration(60) * time.Second)
			for tries := 0; time.Now().Before(ddl); tries++ {
				if err := dao.RedisInstance.Del(context.Background(), []string{model.GetKeyForTopic(topicDetail.ID)}); err != nil {
					currErr := fmt.Errorf("[service] UpdateTopic 延时双删 dao.RedisInstance.Del err: %v, key: %v",
						err, model.GetKeyForTopic(topicDetail.ID))
					service.Log.Error(currErr)
					sentry.CaptureException(currErr)
				} else {
					return
				}

				time.Sleep(time.Second << uint(tries))
			}
		}()

		return nil
	}

	// lock
	return rowsAffected, dao.RedisInstance.LockWrap(ctx, model.GetKeyForLockTopic(topicDetail.ID), f)
}

func (service *Service) UpdateTopicWithoutLock(ctx context.Context, topicDetail *model.TopicDetail) (int64, error) {
	var rowsAffected int64

	// Redis
	if err := dao.RedisInstance.Del(ctx, []string{model.GetKeyForTopic(topicDetail.ID)}); err != nil {
		currErr := fmt.Errorf("[service] UpdateTopic dao.RedisInstance.Del err: %v, key: %v",
			err, model.GetKeyForTopic(topicDetail.ID))
		service.Log.Error(currErr)
		sentry.CaptureException(currErr)
		return rowsAffected, err
	}

	// TiDB
	var updateErr error
	rowsAffected, updateErr = dao.TiDBInstance.UpdateTopicWithoutUserBehavior(ctx, topicDetail)
	if updateErr != nil {
		currErr := fmt.Errorf("[service] UpdateTopic dao.TiDBInstance.UpdateTopicWithoutUserBehavior err: %v id: %v",
			updateErr, topicDetail.ID)
		service.Log.Error(currErr)
		sentry.CaptureException(currErr)
		return rowsAffected, updateErr
	}
	if rowsAffected != 1 {
		service.Log.Infof("[service] UpdateTopic dao.TiDBInstance.UpdateTopic rowsAffected: %v != 1", rowsAffected)
		return rowsAffected, nil
	} else {
		// Index to es
		basicUtil.PubEvent(service.Pub, topic.EV_DM_TOPIC, event.DataEventUint64_NEW, topicDetail.ID)
	}

	// 延时双删
	go func() {
		time.Sleep(200 * time.Millisecond)

		ddl := time.Now().Add(time.Duration(60) * time.Second)
		for tries := 0; time.Now().Before(ddl); tries++ {
			if err := dao.RedisInstance.Del(context.Background(), []string{model.GetKeyForTopic(topicDetail.ID)}); err != nil {
				currErr := fmt.Errorf("[service] UpdateTopic 延时双删 dao.RedisInstance.Del err: %v, key: %v",
					err, model.GetKeyForTopic(topicDetail.ID))
				service.Log.Error(currErr)
				sentry.CaptureException(currErr)
			} else {
				return
			}

			time.Sleep(time.Second << uint(tries))
		}
	}()

	return rowsAffected, nil
}

func (service *Service) DelTopicById(ctx context.Context, id int64) (int64, error) {
	var rowsAffected int64

	f := func() error {
		// Redis
		if err := dao.RedisInstance.Del(ctx, []string{model.GetKeyForTopic(id)}); err != nil {
			currErr := fmt.Errorf("[service] DelTopicById dao.RedisInstance.Del err: %v key: %v", err, model.GetKeyForTopic(id))
			service.Log.Error(currErr)
			sentry.CaptureException(currErr)
			return err
		}

		// TiDB
		var delErr error
		rowsAffected, delErr = dao.TiDBInstance.DelTopicByIdsWithoutUserBehavior(ctx, []int64{id})
		if delErr != nil {
			currErr := fmt.Errorf("[service] DelTopicById dao.TiDBInstance.Del err: %v id: %v", delErr, id)
			service.Log.Error(currErr)
			sentry.CaptureException(currErr)
			return delErr
		}
		if rowsAffected != 1 {
			rowsAffectedErr := fmt.Errorf(
				"[service] DelTopicByIds dao.TiDBInstance.DelTopicByIds rowsAffected: %v != 1", rowsAffected)
			service.Log.Info(rowsAffectedErr)
			return rowsAffectedErr
		} else {
			basicUtil.PubEvent(service.Pub, topic.EV_DM_TOPIC, event.DataEventUint64_DELETE, id)
		}

		// 延时双删
		go func() {
			time.Sleep(200 * time.Millisecond)

			ddl := time.Now().Add(time.Duration(60) * time.Second)
			for tries := 0; time.Now().Before(ddl); tries++ {
				if err := dao.RedisInstance.Del(context.Background(), []string{model.GetKeyForTopic(id)}); err != nil {
					currErr := fmt.Errorf("[service] DelTopicById 延时双删 dao.RedisInstance.Del err: %v, key: %v",
						err, model.GetKeyForTopic(id))
					service.Log.Error(currErr)
					sentry.CaptureException(currErr)
				} else {
					return
				}

				time.Sleep(time.Second << uint(tries))
			}
		}()

		return nil
	}

	// lock
	return rowsAffected, dao.RedisInstance.LockWrap(ctx, model.GetKeyForLockTopic(id), f)
}

func (service *Service) DelTopicByIds(ctx context.Context, ids []int64) (int64, error) {
	var rowsAffected int64

	for _, id := range ids {
		r, err := service.DelTopicById(ctx, id)
		if err != nil {
			continue
		}
		rowsAffected += r
	}

	return rowsAffected, nil
}

func (service *Service) GetTopicByIds(ctx context.Context, preIds []int64, withStatistics, withUserBehavior bool,
	userID string) (map[int64]*model.TopicInfo, map[int64]*model.TopicStatistic, error) {

	topicInfos := make(map[int64]*model.TopicInfo, 0)
	topicStatistics := make(map[int64]*model.TopicStatistic, 0)

	// TODO 因redis暂不支持withUserBehavior
	if withUserBehavior {
		currErr := fmt.Errorf("[service] GetTopicByIds withUserBehavior 暂不支持")
		service.Log.Error(currErr)
		sentry.CaptureException(currErr)
		return topicInfos, topicStatistics, currErr
	}

	// 缓存穿透
	ids, err := dao.RedisInstance.GetBitTopics(ctx, preIds)
	if err != nil {
		currErr := fmt.Errorf("[service] GetTopicByIds BitMap dao.RedisInstance.GetBitTopics err: %v", err)
		service.Log.Error(currErr)
		sentry.CaptureException(currErr)
		return topicInfos, topicStatistics, currErr
	}
	if len(ids) == 0 {
		return topicInfos, topicStatistics, &common.InternalError{
			ErrCode: int32(pb.GetTopicByIdsResp_NOT_FOUND),
			ErrMsg:  pb.GetTopicByIdsResp_ErrCode_name[int32(pb.GetTopicByIdsResp_NOT_FOUND)],
		}
	}

	sw := sync.WaitGroup{}
	// 从bi-svc获取统计数据
	var biErr error
	if withStatistics {
		sw.Add(1)
		go func() {
			biCtx, _ := context.WithTimeout(ctx, 3*time.Second)
			service.Log.Infof("[service] GetTopicByIds bi-svc.TopicStatisticsFromBI ids: %v", ids)
			topicStatistics, biErr = service.TopicStatisticsFromBI(biCtx, ids)

			sw.Done()
		}()
	}

	// 首次从redis获取
	topicInfosRedis, redisGetErr := dao.RedisInstance.GetTopics(ctx, ids)
	if redisGetErr != nil {
		currErr := fmt.Errorf("[service] GetTopicByIds dao.RedisInstance.GetTopics err: %v", redisGetErr)
		service.Log.Error(currErr)
		sentry.CaptureException(currErr)
		return topicInfos, topicStatistics, redisGetErr
	}

	lackArr := make([]int64, 0)        // redis缺失 首次
	lackMap := make(map[int64]bool, 0) // redis缺失 首次 map
	lastLackArr := make([]int64, 0)    // redis缺失 最终

	for _, id := range ids {
		if v, ok := topicInfosRedis[id]; ok {
			topicInfos[id] = v
		} else {
			lackArr = append(lackArr, id)
			lackMap[id] = true
		}
	}

	// 从TiDB获取
	if len(lackArr) != 0 {
		for _, lackID := range lackArr {
			// 缓存击穿
			lockedIDs := make([]int64, 0) // 锁被抢占
			if err := dao.RedisInstance.Lock(ctx, model.GetKeyForLockTopicForGetsByTiDB(lackID)); err != nil {
				lockedIDs = append(lockedIDs, lackID)
			}

			// 锁被抢占 延时再次从redis获取
			if len(lockedIDs) != 0 {
				time.Sleep(50 * time.Millisecond)

				// again: get topicInfo from redis
				internalTopicInfosRedis, internalRedisGetErr := dao.RedisInstance.GetTopics(ctx, lockedIDs)
				if internalRedisGetErr != nil {
					currErr := fmt.Errorf("[service] GetTopicByIds dao.RedisInstance.GetTopics err: %v", internalRedisGetErr)
					service.Log.Error(currErr)
					sentry.CaptureException(currErr)
					return topicInfos, topicStatistics, internalRedisGetErr
				}

				// 添加到结果，并与lackArr差集生成lastLackArr。包含len(internalTopicInfosRedis)==0的情况
				for _, v := range internalTopicInfosRedis {
					if v.TopicDetail != nil {
						topicInfos[v.TopicDetail.ID] = v
						if _, ok := lackMap[v.TopicDetail.ID]; ok {
							lackMap[v.TopicDetail.ID] = false
						}
					}
				}
				for i, v := range lackMap {
					if v {
						lastLackArr = append(lastLackArr, i)
					}
				}
			} else { // 全部加锁成功
				// 生成lastLackArr
				lastLackArr = lackArr
			}
		}

		// 到TiDB
		if len(lastLackArr) != 0 {
			// 仅对lastLackArr解锁
			defer func() {
				for _, lackID := range lastLackArr {
					if unLockErr := dao.RedisInstance.UnLock(ctx, model.GetKeyForLockTopicForGetsByTiDB(lackID)); unLockErr != nil {
						currErr := fmt.Errorf("[redis] GetTopicByIds Redis UnLock return unLockErr: %v", unLockErr)
						service.Log.Error(currErr)
						sentry.CaptureException(currErr)
					}
				}
			}()

			topicInfosTiDB, err := dao.TiDBInstance.GetTopicByIds(ctx, lastLackArr, withUserBehavior, userID)
			if err != nil {
				var internalError *common.InternalError
				if errors.As(err, &internalError) && internalError.ErrCode == int32(pb.GetTopicByIdsResp_NOT_FOUND) {
					// do nothing
				} else {
					currErr := fmt.Errorf("[service] GetTopicByIds [从tidb补充] dao.TiDBInstance.GetTopicByIds err: %v", err)
					service.Log.Error(currErr)
					sentry.CaptureException(currErr)

					return topicInfos, topicStatistics, err
				}
			}
			// 添加到结果
			for _, v := range topicInfosTiDB {
				if v.TopicDetail != nil {
					topicInfos[v.TopicDetail.ID] = v
				}
			}

			// 写入redis
			topicInfosTiDBArr := make([]*model.TopicInfo, 0)
			for _, v := range topicInfosTiDB {
				topicInfosTiDBArr = append(topicInfosTiDBArr, v)
			}
			redisSetErr := dao.RedisInstance.SetTopics(ctx, topicInfosTiDBArr)
			if redisSetErr != nil {
				currErr := fmt.Errorf("[service] GetTopicByIds [补充到redis] dao.RedisInstance.SetTopics err: %v", redisSetErr)
				service.Log.Error(currErr)
				sentry.CaptureException(currErr)
				return topicInfos, topicStatistics, redisSetErr
			}
		}
	}

	// 从bi-svc获取统计数据
	sw.Wait()
	if biErr != nil {
		currErr := fmt.Errorf("[service] GetTopicByIds bi-svc.TopicStatisticsFromBI err: %v", biErr)
		service.Log.Error(currErr)
		sentry.CaptureException(currErr)

		return topicInfos, topicStatistics, biErr
	}

	return topicInfos, topicStatistics, nil
}

func (service *Service) GetTopicByIdsWithoutRedis(ctx context.Context,
	ids []int64, withStatistics, withUserBehavior bool, userID string) (map[int64]*model.TopicInfo, map[int64]*model.TopicStatistic, error) {

	topicInfos := make(map[int64]*model.TopicInfo, 0)
	topicStatistics := make(map[int64]*model.TopicStatistic, 0)
	var (
		topicInfoErr      error
		topicStatisticErr error
	)

	sw := sync.WaitGroup{}

	sw.Add(1)
	go func() {
		topicInfos, topicInfoErr = dao.TiDBInstance.GetTopicByIds(ctx, ids, withUserBehavior, userID)
		sw.Done()
	}()

	if withStatistics {
		sw.Add(1)
		go func() {
			biCtx, _ := context.WithTimeout(ctx, 3*time.Second)
			topicStatistics, topicStatisticErr = service.TopicStatisticsFromBI(biCtx, ids)
			sw.Done()
		}()
	}

	sw.Wait()

	if topicInfoErr != nil {
		return topicInfos, topicStatistics, topicInfoErr
	}
	if topicStatisticErr != nil {
		return topicInfos, topicStatistics, topicStatisticErr
	}
	return topicInfos, topicStatistics, nil
}

func (service *Service) TopicList(ctx context.Context,
	keyword string, keywordsWithExactlyEqual []string, sortBy pb.TopicListReq_SortByType, orderBy pb.TopicListReq_OrderByType,
	offset, limit int64, startAt, endAt *timestamp.Timestamp, effectStatus pb.TopicListReq_EffectStatus,
	withStatistics, withUserBehavior bool, userID string, manualAudit pb.TopicListReq_ManualAudit, statusSort bool) ([]*model.TopicInfo, int64, map[int64]*model.TopicStatistic, error) {

	topicInfoArr := make([]*model.TopicInfo, 0)
	var total int64
	topicStatisticMap := make(map[int64]*model.TopicStatistic, 0)

	topicInfoArr, total, err := dao.TiDBInstance.TopicList(ctx, keyword, keywordsWithExactlyEqual, sortBy, orderBy, offset, limit, startAt, endAt, effectStatus,
		withUserBehavior, userID, false, manualAudit, statusSort)
	if err != nil {
		return topicInfoArr, total, topicStatisticMap, err
	}

	if withStatistics {
		topicInfoIDs := make([]int64, 0)
		for _, topicInfoArrItem := range topicInfoArr {
			if topicInfoArrItem.TopicDetail == nil {
				continue
			}
			topicInfoIDs = append(topicInfoIDs, topicInfoArrItem.TopicDetail.ID)
		}

		biCtx, _ := context.WithTimeout(ctx, 3*time.Second)
		topicStatisticMap, err = service.TopicStatisticsFromBI(biCtx, topicInfoIDs)
		if err != nil {
			return topicInfoArr, total, topicStatisticMap, err
		}
	}

	return topicInfoArr, total, topicStatisticMap, err
}

func (service *Service) TopicFollowing(ctx context.Context, action bool, topicID int64, userID string) error {
	if action {
		return dao.TiDBInstance.CreateTopicFollowing(ctx, topicID, userID)
	} else {
		return dao.TiDBInstance.DelTopicFollowing(ctx, topicID, userID)
	}
}

func (service *Service) TopicStatisticsFromBI(ctx context.Context, topicIDs []int64) (map[int64]*model.TopicStatistic, error) {
	topicStatisticMap := make(map[int64]*model.TopicStatistic, 0)

	if len(topicIDs) == 0 {
		return topicStatisticMap, nil
	}

	queryValue, _ := json.Marshal(map[string]interface{}{
		"content": map[string]interface{}{
			"contentTopicIds": func(topicIDs []int64) []string {
				topicIDStrs := make([]string, 0)
				for _, topicID := range topicIDs {
					topicIDStrs = append(topicIDStrs, strconv.FormatInt(topicID, 10))
				}
				return topicIDStrs
			}(topicIDs),
		},
	})
	GetConsoleChartReq := &bi.ConsoleChartReq{
		ChartModuleType: "outer",
		ChartModuleId:   "interface6",
		Query:           &any.Any{Value: queryValue},
	}

	biChartDataSvcResp, err := service.BIChartDataClient.GetConsoleChart(ctx, GetConsoleChartReq)
	if err != nil {
		service.Log.Errorf("[service] BIChartDataClient.GetConsoleChart err: %v", err)
		return topicStatisticMap, &common.InternalError{
			ErrCode: common.Code_SvcInternalError,
			ErrMsg:  common.Msg_SvcInternalError,
		}
	}
	if biChartDataSvcResp.GetErrCode() != bi.ConsoleChartRes_NONE {
		service.Log.Errorf("[service] BIChartDataClient.GetConsoleChart errCode: %d", biChartDataSvcResp.GetErrCode())
		return topicStatisticMap, &common.InternalError{
			ErrCode: int32(biChartDataSvcResp.GetErrCode()),
			ErrMsg:  bi.ConsoleChartRes_ErrCode_name[int32(biChartDataSvcResp.GetErrCode())],
		}
	}

	type valueT struct {
		Data []struct {
			TopicID    int64 `json:"topicId,string"`
			ContentNum int64 `json:"contentNum,string"`
			FusionNum  int64 `json:"fusionNum,string"`
			TapNum     int64 `json:"tapNum,string"`
		} `json:"data"`
	}
	biChartDataSvcRespI := &valueT{}
	err = json.Unmarshal(biChartDataSvcResp.GetChartData().GetValue(), biChartDataSvcRespI)
	if err != nil {
		return topicStatisticMap, err
	}
	for _, topicData := range biChartDataSvcRespI.Data {
		topicStatisticMap[topicData.TopicID] = &model.TopicStatistic{
			TopicID:            topicData.TopicID,
			ContentNum:         topicData.ContentNum,
			MpNum:              topicData.FusionNum,
			ContentExposureNum: topicData.TapNum,
		}
	}

	return topicStatisticMap, nil
}

func (service *Service) UpdateTopicStatistic(ctx context.Context) error {
	var offset, limit int64 = 0, 500
	for {
		topicInfoArr, total, err := dao.TiDBInstance.TopicList(ctx, "", []string{}, pb.TopicListReq_CREATED_AT, pb.TopicListReq_ASC,
			offset, limit, nil, nil, pb.TopicListReq_NONE, false, "", true,
			pb.TopicListReq_ManualAudit_None, false)
		if err != nil {
			service.Log.Errorf("[service] UpdateTopicStatistic dao.TiDBInstance.TopicList err: %v", err)
			sentry.CaptureException(fmt.Errorf(
				"[task] updateTopicStatistic fail, offset: %v, limit: %v, total: %v, err: %v", offset, limit, total, err))
			break
		}

		// refresh es
		for _, topicInfo := range topicInfoArr {
			basicUtil.PubEvent(service.Pub, topic.EV_DM_TOPIC, event.DataEventUint64_NEW, topicInfo.TopicDetail.ID)
			service.Log.Debugf("[task] updateTopicStatistic pub kafka, id: %v", topicInfo.TopicDetail.ID)
		}

		service.Log.Infof("[task] updateTopicStatistic current batch success, offset: %v, limit: %v, total: %v", offset, limit, total)

		if total < limit {
			service.Log.Infof("[task] updateTopicStatistic success")
			break
		}

		offset += limit
	}

	return nil
}

func (service *Service) InitTopicBitMap(ctx context.Context) error {
	var offset, limit int64 = 0, 500
	for {
		topicInfoArr, total, err := dao.TiDBInstance.TopicList(ctx, "", []string{}, pb.TopicListReq_CREATED_AT, pb.TopicListReq_ASC,
			offset, limit, nil, nil, pb.TopicListReq_NONE, false, "", true, pb.TopicListReq_ManualAudit_None, false)
		if err != nil {
			service.Log.Errorf("[service] InitTopicBitMap dao.TiDBInstance.TopicList err: %v", err)
			sentry.CaptureException(fmt.Errorf(
				"[task] updateTopicStatistic fail, offset: %v, limit: %v, total: %v, err: %v", offset, limit, total, err))
			break
		}

		// init bitmap
		ids := make([]int64, 0)
		for _, topicInfo := range topicInfoArr {
			if topicInfo.TopicDetail != nil {
				ids = append(ids, topicInfo.TopicDetail.ID)
			}
		}
		if err := dao.RedisInstance.SetBitTopics(ctx, ids); err != nil {
			currErr := fmt.Errorf("[service] InitTopicBitMap dao.RedisInstance.SetBitTopics err: %v", err)
			service.Log.Error(currErr)
			sentry.CaptureException(currErr)
			break
		}

		service.Log.Infof("[task] InitTopicBitMap current batch success, offset: %v, limit: %v, total: %v", offset, limit, total)

		if total < limit {
			service.Log.Infof("[task] InitTopicBitMap success")
			break
		}

		offset += limit
	}

	return nil
}

func (service *Service) MustManualAudit(ctx context.Context, topics []string) ([]string, error) {
	return dao.TiDBInstance.MustManualAudit(ctx, topics)
}

func (service *Service) UpdateTopicStatus(ctx context.Context) error {
	topicDetailArr, err := dao.TiDBInstance.TopicDetailListForUpdateStatus(ctx)
	if err != nil {
		service.Log.Errorf("[service] UpdateTopicStatus dao.TiDBInstance.TopicDetailListForUpdateStatus err: %v", err)
		sentry.CaptureException(fmt.Errorf("[task] UpdateTopicStatus fail, err: %v", err))
		return err
	}

	for _, topicDetail := range topicDetailArr {
		f := func() error {
			// 获取详情
			getTopicDetail, err := dao.TiDBInstance.GetTopicDetail(ctx, topicDetail.ID)
			if err != nil {
				service.Log.Errorf("[service] UpdateTopicStatus dao.TiDBInstance.GetTopicDetail err: %v", err)
				sentry.CaptureException(fmt.Errorf("[task] UpdateTopicStatus fail, err: %v", err))
				return err
			}

			// 更新状态
			getTopicDetail.Status = func() pb.TopicDetail_TopicStatus {
				if time.Now().Unix() < getTopicDetail.StartAt.Unix() {
					return pb.TopicDetail_TopicStatus_NotStarted
				} else if time.Now().Unix() > getTopicDetail.EndAt.Unix() {
					return pb.TopicDetail_TopicStatus_Ended
				} else {
					return pb.TopicDetail_TopicStatus_InProcess
				}
			}()
			if _, err := service.UpdateTopicWithoutLock(ctx, getTopicDetail); err != nil {
				service.Log.Errorf("[service] UpdateTopicStatus service.UpdateTopicWithoutLock err: %v", err)
				sentry.CaptureException(fmt.Errorf("[task] UpdateTopicStatus fail, err: %v", err))
				return err
			}

			return nil
		}

		// lock
		if err := dao.RedisInstance.LockWrap(ctx, model.GetKeyForLockTopic(topicDetail.ID), f); err != nil {
			return err
		}
	}

	return nil
}

func (service *Service) FullUpdateTopicStatus(ctx context.Context) error {
	topicDetailArr, err := dao.TiDBInstance.TopicDetailListForFullUpdateStatus(ctx)
	if err != nil {
		service.Log.Errorf("[service] FullUpdateTopicStatus dao.TiDBInstance.TopicDetailListForFullUpdateStatus err: %v", err)
		sentry.CaptureException(fmt.Errorf("[task] FullUpdateTopicStatus fail, err: %v", err))
		return err
	}

	for _, topicDetail := range topicDetailArr {
		f := func() error {
			// 获取详情
			getTopicDetail, err := dao.TiDBInstance.GetTopicDetail(ctx, topicDetail.ID)
			if err != nil {
				service.Log.Errorf("[service] FullUpdateTopicStatus dao.TiDBInstance.GetTopicDetail err: %v", err)
				sentry.CaptureException(fmt.Errorf("[task] FullUpdateTopicStatus fail, err: %v", err))
				return err
			}

			// 更新状态
			getTopicDetail.Status = func() pb.TopicDetail_TopicStatus {
				if time.Now().Unix() < getTopicDetail.StartAt.Unix() {
					return pb.TopicDetail_TopicStatus_NotStarted
				} else if time.Now().Unix() > getTopicDetail.EndAt.Unix() {
					return pb.TopicDetail_TopicStatus_Ended
				} else {
					return pb.TopicDetail_TopicStatus_InProcess
				}
			}()
			if _, err := service.UpdateTopicWithoutLock(ctx, getTopicDetail); err != nil {
				service.Log.Errorf("[service] FullUpdateTopicStatus service.UpdateTopicWithoutLock err: %v", err)
				sentry.CaptureException(fmt.Errorf("[task] FullUpdateTopicStatus fail, err: %v", err))
				return err
			}

			return nil
		}

		// lock
		if err := dao.RedisInstance.LockWrap(ctx, model.GetKeyForLockTopic(topicDetail.ID), f); err != nil {
			return err
		}
	}

	return nil
}
