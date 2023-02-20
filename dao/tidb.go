package dao

import (
	"context"
	pb "dm-gitlab.bolo.me/hubpd/proto/topic_grpc"
	"dm-gitlab.bolo.me/hubpd/topic/common"
	"dm-gitlab.bolo.me/hubpd/topic/model"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"strings"
	"time"
)

type TiDB struct {
	DB  *gorm.DB
	Log *logrus.Entry
}

var TiDBInstance *TiDB

var (
	PrimaryKeyUnspecifiedErr  = errors.New("primary key unspecified")
	PrimaryKeysUnspecifiedErr = errors.New("primary key unspecified exists in primary key list")
)

func IsDuplicated(err error) bool {
	if mysqlErr, ok := err.(*mysql.MySQLError); ok {
		return mysqlErr.Number == 1062
	}
	return false
}

func IsNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}

func (dao *TiDB) CreateTopic(ctx context.Context, topicDetail *model.TopicDetail) error {
	topicDetail.Uniq = topicDetail.Title
	if err := dao.DB.Create(topicDetail).Error; err != nil {
		dao.Log.Errorf("[dao] Create(topicDetail) err: %v", err)
		if IsDuplicated(err) {
			return &common.InternalError{
				ErrCode: int32(pb.CreateTopicResp_NAME_DUP),
				ErrMsg:  "name dup",
			}
		} else {
			return err
		}
	}

	return nil
}

func (dao *TiDB) UpdateTopicWithoutUserBehavior(ctx context.Context, topicDetail *model.TopicDetail) (rowsAffected int64, err error) {
	if topicDetail.ID == 0 {
		return rowsAffected, PrimaryKeyUnspecifiedErr
	}

	return rowsAffected, dao.DB.Transaction(func(dbTrans *gorm.DB) error {
		db := dbTrans.Model(&model.TopicDetail{}).Where("id = ?", topicDetail.ID).Limit(1).Updates(map[string]interface{}{
			"title":        topicDetail.Title,
			"uniq":         topicDetail.Title,
			"bg_pic":       topicDetail.BGPic,
			"avatar":       topicDetail.Avatar,
			"sort":         topicDetail.Sort,
			"desc":         topicDetail.Desc,
			"catalogue":    topicDetail.Catalogue,
			"start_at":     topicDetail.StartAt,
			"end_at":       topicDetail.EndAt,
			"status":       topicDetail.Status,
			"manual_audit": topicDetail.ManualAudit,
		})
		if db.Error != nil {
			dao.Log.Errorf("[dao] db.Update(TopicDetail) err: %v", db.Error)
			if IsDuplicated(db.Error) {
				return &common.InternalError{
					ErrCode: int32(pb.UpdateTopicResp_NAME_DUP),
					ErrMsg:  "name dup",
				}
			} else {
				return db.Error
			}
		}

		rowsAffected = db.RowsAffected
		return nil
	})
}

func (dao *TiDB) UpdateTopic(ctx context.Context, topicDetail *model.TopicDetail) (rowsAffected int64, err error) {
	if topicDetail.ID == 0 {
		return rowsAffected, PrimaryKeyUnspecifiedErr
	}

	return rowsAffected, dao.DB.Transaction(func(dbTrans *gorm.DB) error {
		// update title field
		db := dbTrans.Model(&model.TopicDetail{}).Where("id = ?", topicDetail.ID).Limit(1).Updates(map[string]interface{}{
			"title": topicDetail.Title,
			"uniq":  topicDetail.Title,
		})
		if db.Error != nil {
			dao.Log.Errorf("[dao] db.Update(TopicDetail.title) err: %v", db.Error)
			if IsDuplicated(db.Error) {
				return &common.InternalError{
					ErrCode: int32(pb.UpdateTopicResp_NAME_DUP),
					ErrMsg:  "name dup",
				}
			} else {
				return db.Error
			}
		}

		// del userBehavior
		if db.RowsAffected != 0 {
			delNow := time.Now()

			err = dbTrans.Model(&model.TopicUserBehavior{}).Where("topic_id = ?", topicDetail.ID).Limit(1).Updates(map[string]interface{}{
				"deleted_at": delNow,
				"uniq":       gorm.Expr("CONCAT_WS('-', topic_id, user_id, ?)", delNow.Unix()),
			}).Error
			if err != nil {
				dao.Log.Errorf("[dao] db.Del(TopicUserBehavior.topicID) err: %v", err)
			}
		}

		rowsAffected = db.RowsAffected

		// update other field
		db = dbTrans.Model(&model.TopicDetail{}).Where("id = ?", topicDetail.ID).Limit(1).Updates(map[string]interface{}{
			"bg_pic":       topicDetail.BGPic,
			"avatar":       topicDetail.Avatar,
			"sort":         topicDetail.Sort,
			"desc":         topicDetail.Desc,
			"catalogue":    topicDetail.Catalogue,
			"start_at":     topicDetail.StartAt,
			"end_at":       topicDetail.EndAt,
			"manual_audit": topicDetail.ManualAudit,
			"status":       topicDetail.Status,
		})
		if db.Error != nil {
			dao.Log.Errorf("[dao] db.Update(TopicDetail.otherField) err: %v", db.Error)
			return db.Error
		}

		if rowsAffected == 0 {
			rowsAffected = db.RowsAffected
		}

		return nil
	})
}

func (dao *TiDB) DelTopicByIdsWithoutUserBehavior(ctx context.Context, ids []int64) (rowsAffected int64, err error) {
	if len(ids) == 0 {
		return rowsAffected, nil
	}
	for _, id := range ids {
		if id == 0 {
			return rowsAffected, PrimaryKeysUnspecifiedErr
		}
	}

	return rowsAffected, dao.DB.Transaction(func(dbTrans *gorm.DB) error {
		delNow := time.Now()

		db := dbTrans.Model(&model.TopicDetail{}).Where("id IN (?)", ids).Updates(map[string]interface{}{
			"deleted_at": delNow,
			"uniq":       gorm.Expr("CONCAT_WS('-', title, ?)", delNow.Unix()),
		})
		if err = db.Error; err != nil {
			dao.Log.Errorf("[dao] db.Del(TopicDetail.ID) err: %v", err)
			return db.Error
		}

		rowsAffected = db.RowsAffected
		return nil
	})
}

func (dao *TiDB) DelTopicByIds(ctx context.Context, ids []int64) (rowsAffected int64, err error) {
	if len(ids) == 0 {
		return rowsAffected, nil
	}
	for _, id := range ids {
		if id == 0 {
			return rowsAffected, PrimaryKeysUnspecifiedErr
		}
	}

	return rowsAffected, dao.DB.Transaction(func(dbTrans *gorm.DB) error {
		delNow := time.Now()

		db := dbTrans.Model(&model.TopicDetail{}).Where("id IN (?)", ids).Updates(map[string]interface{}{
			"deleted_at": delNow,
			"uniq":       gorm.Expr("CONCAT_WS('-', title, ?)", delNow.Unix()),
		})
		if err = db.Error; err != nil {
			dao.Log.Errorf("[dao] db.Del(TopicDetail.ID) err: %v", err)
		}
		rowsAffected = db.RowsAffected

		// del userBehavior
		if db.RowsAffected != 0 {
			err = dbTrans.Model(&model.TopicUserBehavior{}).Where("topic_id IN (?)", ids).Updates(map[string]interface{}{
				"deleted_at": delNow,
				"uniq":       gorm.Expr("CONCAT_WS('-', topic_id, user_id, ?)", delNow.Unix()),
			}).Error
			if err != nil {
				dao.Log.Errorf("[dao] db.Del(TopicUserBehavior.topicID) err: %v", err)
			}
		}

		return nil
	})
}

func (dao *TiDB) GetTopicByIds(ctx context.Context,
	ids []int64, withUserBehavior bool, userID string) (map[int64]*model.TopicInfo, error) {
	topicInfoMap := make(map[int64]*model.TopicInfo, 0)

	if len(ids) == 0 {
		return topicInfoMap, nil
	}
	for _, id := range ids {
		if id == 0 {
			return topicInfoMap, PrimaryKeysUnspecifiedErr
		}
	}
	if withUserBehavior && userID == "" {
		return topicInfoMap, PrimaryKeyUnspecifiedErr
	}

	topicDetailArr := make([]*model.TopicDetail, 0)
	topicUserBehaviorArr := make([]*model.TopicUserBehavior, 0)

	if err := dao.DB.Transaction(func(dbTrans *gorm.DB) error {
		if err := dbTrans.Find(&topicDetailArr, "id in (?)", ids).Error; err != nil {
			dao.Log.Errorf("[dao] db.Find(TopicDetail.id) err: %v", err)
			return err
		}

		if withUserBehavior {
			if err := dbTrans.Find(&topicUserBehaviorArr, "topic_id in (?) AND user_id = ?", ids, userID).Error; err != nil {
				dao.Log.Errorf("[dao] db.Find(TopicUserBehavior.id) err: %v", err)
				return err
			}
		}

		return nil
	}); err != nil {
		dao.Log.Errorf("[dao] db.Transaction err: %v", err)
		return topicInfoMap, err
	}

	if len(topicDetailArr) == 0 {
		return topicInfoMap, &common.InternalError{
			ErrCode: int32(pb.GetTopicByIdsResp_NOT_FOUND),
			ErrMsg:  "not found",
		}
	}

	for _, topicDetail := range topicDetailArr {
		topicInfoMap[topicDetail.ID] = &model.TopicInfo{
			TopicDetail: topicDetail,
		}
	}
	for _, topicUserBehavior := range topicUserBehaviorArr {
		if v, ok := topicInfoMap[topicUserBehavior.TopicID]; ok {
			v.TopicUserBehavior = topicUserBehavior
		}
	}

	return topicInfoMap, nil
}

func (dao *TiDB) TopicList(ctx context.Context,
	keyword string, keywordsWithExactlyEqual []string, sortBy pb.TopicListReq_SortByType, orderBy pb.TopicListReq_OrderByType,
	offset, limit int64, startAt, endAt *timestamp.Timestamp, effectStatus pb.TopicListReq_EffectStatus,
	withUserBehavior bool, userID string, withLatest bool, manualAudit pb.TopicListReq_ManualAudit, statusSort bool) ([]*model.TopicInfo, int64, error) {

	var total int64

	topicInfoMap := make(map[int64]*model.TopicInfo, 0)
	topicInfoArr := make([]*model.TopicInfo, 0)

	if withUserBehavior && userID == "" {
		return topicInfoArr, total, PrimaryKeyUnspecifiedErr
	}

	topicDetailArr := make([]*model.TopicDetail, 0)
	topicUserBehaviorArr := make([]*model.TopicUserBehavior, 0)

	if err := dao.DB.Transaction(func(dbTrans *gorm.DB) error {
		db := dbTrans.Model(&model.TopicDetail{})

		if strings.TrimSpace(keyword) != "" {
			db = db.Where("(title like ?)", "%"+strings.TrimSpace(keyword)+"%")
		}

		if len(keywordsWithExactlyEqual) != 0 {
			db = db.Where("title in (?)", keywordsWithExactlyEqual)
		}

		now, _ := ptypes.Timestamp(ptypes.TimestampNow())
		if effectStatus == pb.TopicListReq_EFFECT {
			db = db.Where("(end_at >= ? and start_at <= ?)", now.Local(), now.Local())
		} else if effectStatus == pb.TopicListReq_INEFFECT {
			db = db.Where("(end_at < ?)", now.Local())
		}

		if withLatest {
			todayZeroTime, _ := time.Parse("2006-01-02", time.Now().Format("2006-01-02"))
			db = db.Where("(created_at < ?)", todayZeroTime)
		}

		if manualAudit == pb.TopicListReq_ManualAudit_False {
			db = db.Where("(manual_audit = false)")
		} else if manualAudit == pb.TopicListReq_ManualAudit_True {
			db = db.Where("(manual_audit = true)")
		}

		// 查询时间交集
		if startAt != nil && endAt != nil {
			start, _ := ptypes.Timestamp(startAt)
			end, _ := ptypes.Timestamp(endAt)
			startLocal := start.Local()
			endLocal := end.Local()
			db = db.Where("((start_at >= ? AND start_at <= ?) OR (start_at <= ? AND end_at >= ?))",
				startLocal, endLocal, startLocal, startLocal)
		}

		var sortStr string
		// 一级排序
		if statusSort {
			sortStr = "status asc, "
		}
		// 二级排序
		switch sortBy {
		case pb.TopicListReq_SORT_NUM:
			sortStr += "sort"
			// 顺序方式
			if orderBy == pb.TopicListReq_ASC {
				sortStr += " asc"
			} else {
				sortStr += " desc"
			}
			sortStr += ", created_at desc" // 三级排序，针对前两级排序区分不出的情况
		default:
			// 原本TopicListReq_CREATED_AT是要按照创建时间排序，需求调整后，sortBy不作调整，实际按照start_at排序
			sortStr += "start_at"
			// 顺序方式
			if orderBy == pb.TopicListReq_ASC {
				sortStr += " asc"
			} else {
				sortStr += " desc"
			}
		}

		db = db.Order(sortStr)

		if err := db.Count(&total).Error; err != nil {
			dao.Log.Errorf("[dao] db.Model(TopicDetail).Count err: %v", err)
			return err
		}
		if limit != -1 {
			if limit != 0 {
				db = db.Limit(int(limit))
			} else {
				db = db.Limit(20)
			}
			if offset != 0 {
				db = db.Offset(int(offset))
			}
		}
		err := db.Find(&topicDetailArr).Error
		if err != nil {
			dao.Log.Errorf("[dao] db.Find(TopicDetail) err: %v", err)
			return err
		}

		topicDetailIds := make([]int64, 0)
		for _, topicDetail := range topicDetailArr {
			topicInfoMap[topicDetail.ID] = &model.TopicInfo{
				TopicDetail: topicDetail,
			}
			topicDetailIds = append(topicDetailIds, topicDetail.ID)
		}

		if withUserBehavior && len(topicDetailIds) != 0 {
			if err := dbTrans.Find(&topicUserBehaviorArr, "topic_id in (?) AND user_id = ?", topicDetailIds, userID).Error; err != nil {
				dao.Log.Errorf("[dao] db.Find(TopicUserBehavior.id) err: %v", err)
				return err
			}
		}

		return nil
	}); err != nil {
		dao.Log.Errorf("[dao] db.Transaction err: %v", err)
		return topicInfoArr, total, err
	}

	for _, topicUserBehavior := range topicUserBehaviorArr {
		if v, ok := topicInfoMap[topicUserBehavior.TopicID]; ok {
			v.TopicUserBehavior = topicUserBehavior
		}
	}

	for _, topicDetail := range topicDetailArr {
		topicInfoArr = append(topicInfoArr, topicInfoMap[topicDetail.ID])
	}

	return topicInfoArr, total, nil
}

func (dao *TiDB) CreateTopicFollowing(ctx context.Context, topicID int64, userID string) error {
	if topicID == 0 || userID == "" {
		return PrimaryKeyUnspecifiedErr
	}

	if err := dao.DB.Create(&model.TopicUserBehavior{
		TopicID: topicID,
		UserID:  userID,
		Uniq:    fmt.Sprintf("%v-%v", topicID, userID),
	}).Error; err != nil {
		if IsDuplicated(err) {
			return &common.InternalError{
				ErrCode: int32(pb.TopicFollowingResp_STATUS_ERR),
				ErrMsg:  "只有未关注，才可关注",
			}
		} else {
			dao.Log.Errorf("[dao] Create(TopicUserBehavior) err: %v", err)
			return err
		}
	}

	return nil
}

func (dao *TiDB) DelTopicFollowing(ctx context.Context, topicID int64, userID string) error {
	if topicID == 0 || userID == "" {
		return PrimaryKeyUnspecifiedErr
	}

	delNow := time.Now()
	db := dao.DB.Model(&model.TopicUserBehavior{}).Where("topic_id = ? AND user_id = ?", topicID, userID).Limit(1).Updates(map[string]interface{}{
		"deleted_at": delNow,
		"uniq":       gorm.Expr("CONCAT_WS('-', topic_id, user_id, ?)", delNow.Unix()),
	})
	if err := db.Error; err != nil {
		dao.Log.Errorf("[dao] db.Del(TopicUserBehavior) err: %v", err)
		return err
	}
	if db.RowsAffected == 0 {
		return &common.InternalError{
			ErrCode: int32(pb.TopicFollowingResp_STATUS_ERR),
			ErrMsg:  "只有已关注，才取消关注",
		}
	}

	return nil
}

func (dao *TiDB) MustManualAudit(ctx context.Context, topics []string) ([]string, error) {
	manualAuditTopics := make([]string, 0)
	topicArr := make([]*model.TopicDetail, 0)
	err := dao.DB.Find(&topicArr, "title in (?) AND manual_audit = ?", topics, true).Error
	if err != nil {
		dao.Log.Errorf("[dao] db.MustManualAudit err: %v", err)
		return manualAuditTopics, err
	}
	for _, topic := range topicArr {
		manualAuditTopics = append(manualAuditTopics, topic.Title)
	}
	return manualAuditTopics, err
}

func (dao *TiDB) TopicDetailListForUpdateStatus(ctx context.Context) ([]*model.TopicDetail, error) {
	topicDetailArr := make([]*model.TopicDetail, 0)

	now := time.Now()
	db := dao.DB.Model(&model.TopicDetail{}).Debug()

	db = db.Where("end_at < ? and status != ?", now, pb.TopicDetail_TopicStatus_Ended).
		Or("start_at > ? and status != ?", now, pb.TopicDetail_TopicStatus_NotStarted).
		Or("start_at <= ? and end_at >= ? and status != ?", now, now, pb.TopicDetail_TopicStatus_InProcess)

	if err := db.Find(&topicDetailArr).Error; err != nil {
		dao.Log.Errorf("[dao] TopicDetailListForUpdateStatus Find err: %v", err)
		return topicDetailArr, err
	}

	return topicDetailArr, nil
}

func (dao *TiDB) TopicDetailListForFullUpdateStatus(ctx context.Context) ([]*model.TopicDetail, error) {
	topicDetailArr := make([]*model.TopicDetail, 0)

	db := dao.DB.Model(&model.TopicDetail{})
	if err := db.Find(&topicDetailArr).Error; err != nil {
		dao.Log.Errorf("[dao] TopicDetailListForUpdateStatus Find err: %v", err)
		return topicDetailArr, err
	}

	return topicDetailArr, nil
}

func (dao *TiDB) GetTopicDetail(ctx context.Context, id int64) (*model.TopicDetail, error) {
	topicDetail := &model.TopicDetail{}

	if err := dao.DB.First(topicDetail, "id = ?", id).Error; err != nil {
		dao.Log.Errorf("[dao] GetTopicDetail First err: %v", err)
		return topicDetail, err
	}

	return topicDetail, nil
}
