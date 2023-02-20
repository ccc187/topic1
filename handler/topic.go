package handler

import (
	"context"
	pb "dm-gitlab.bolo.me/hubpd/proto/topic_grpc"
	"dm-gitlab.bolo.me/hubpd/topic/common"
	"dm-gitlab.bolo.me/hubpd/topic/model"
	"dm-gitlab.bolo.me/hubpd/topic/service"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type Handler struct {
	Log *logrus.Entry
}

var Instance *Handler

func (handler *Handler) CreateTopic(ctx context.Context, req *pb.CreateTopicReq) (*pb.CreateTopicResp, error) {
	topicDetail := &model.TopicDetail{
		Title:       req.GetData().GetTitle(),
		BGPic:       req.GetData().GetBgPic(),
		ManualAudit: req.GetData().GetManualAudit(),
		Avatar:      req.GetData().GetAvatar(),
		Sort:        req.GetData().GetSort(),
		Desc:        req.GetData().GetDesc(),
		Catalogue: func() string {
			catalogueJson, _ := json.Marshal(req.GetData().GetCatalogue())
			return string(catalogueJson)
		}(),
		StartAt: func() time.Time {
			todayZeroTime, _ := time.ParseInLocation(
				"2006-01-02", req.GetData().GetStartAt().AsTime().Local().Format("2006-01-02"), time.Local)
			return todayZeroTime
		}(),
		EndAt: func() time.Time {
			todayZeroTime, _ := time.ParseInLocation(
				"2006-01-02", req.GetData().GetEndAt().AsTime().Local().Format("2006-01-02"), time.Local)
			return todayZeroTime.AddDate(0, 0, 1).Add(-1 * time.Second)
		}(),
		Status: func() pb.TopicDetail_TopicStatus {
			if time.Now().Unix() < req.GetData().GetStartAt().GetSeconds() {
				return pb.TopicDetail_TopicStatus_NotStarted
			} else if time.Now().Unix() > req.GetData().GetEndAt().GetSeconds() {
				return pb.TopicDetail_TopicStatus_Ended
			} else {
				return pb.TopicDetail_TopicStatus_InProcess
			}
		}(),
	}
	err := service.Instance.CreateTopic(ctx, topicDetail)
	if err != nil {
		if internalErr, ok := err.(*common.InternalError); ok {
			return &pb.CreateTopicResp{
				ErrCode: pb.CreateTopicResp_ErrCode(internalErr.ErrCode),
				ErrMsg:  internalErr.ErrMsg,
			}, nil
		}

		return &pb.CreateTopicResp{}, err
	}

	return &pb.CreateTopicResp{Id: topicDetail.ID}, nil
}

func (handler *Handler) UpdateTopic(ctx context.Context, req *pb.UpdateTopicReq) (*pb.UpdateTopicResp, error) {
	topicDetail := &model.TopicDetail{
		Base: model.Base{
			ID: req.GetData().GetId(),
		},
		Title:       req.GetData().GetTitle(),
		BGPic:       req.GetData().GetBgPic(),
		ManualAudit: req.GetData().GetManualAudit(),
		Avatar:      req.GetData().GetAvatar(),
		Sort:        req.GetData().GetSort(),
		Desc:        req.GetData().GetDesc(),
		Catalogue: func() string {
			catalogueJson, _ := json.Marshal(req.GetData().GetCatalogue())
			return string(catalogueJson)
		}(),
		StartAt: func() time.Time {
			todayZeroTime, _ := time.ParseInLocation(
				"2006-01-02", req.GetData().GetStartAt().AsTime().Local().Format("2006-01-02"), time.Local)
			return todayZeroTime
		}(),
		EndAt: func() time.Time {
			todayZeroTime, _ := time.ParseInLocation(
				"2006-01-02", req.GetData().GetEndAt().AsTime().Local().Format("2006-01-02"), time.Local)
			return todayZeroTime.AddDate(0, 0, 1).Add(-1 * time.Second)
		}(),
		Status: func() pb.TopicDetail_TopicStatus {
			if time.Now().Unix() < req.GetData().GetStartAt().GetSeconds() {
				return pb.TopicDetail_TopicStatus_NotStarted
			} else if time.Now().Unix() > req.GetData().GetEndAt().GetSeconds() {
				return pb.TopicDetail_TopicStatus_Ended
			} else {
				return pb.TopicDetail_TopicStatus_InProcess
			}
		}(),
	}
	_, err := service.Instance.UpdateTopic(ctx, topicDetail)
	if err != nil {
		if internalErr, ok := err.(*common.InternalError); ok {
			return &pb.UpdateTopicResp{
				ErrCode: pb.UpdateTopicResp_ErrCode(internalErr.ErrCode),
				ErrMsg:  internalErr.ErrMsg,
			}, nil
		}

		return &pb.UpdateTopicResp{}, err
	}

	return &pb.UpdateTopicResp{}, nil
}

func (handler *Handler) DelTopicByIds(ctx context.Context, req *pb.DelTopicByIdsReq) (*pb.DelTopicByIdsResp, error) {
	_, err := service.Instance.DelTopicByIds(ctx, req.Ids)
	if err != nil {
		if internalErr, ok := err.(*common.InternalError); ok {
			return &pb.DelTopicByIdsResp{
				ErrCode: pb.DelTopicByIdsResp_ErrCode(internalErr.ErrCode),
				ErrMsg:  internalErr.ErrMsg,
			}, nil
		}

		return &pb.DelTopicByIdsResp{}, err
	}

	return &pb.DelTopicByIdsResp{}, nil
}

func (handler *Handler) GetTopicByIds(ctx context.Context, req *pb.GetTopicByIdsReq) (*pb.GetTopicByIdsResp, error) {
	if len(req.Ids) == 0 {
		return &pb.GetTopicByIdsResp{}, nil
	}

	var (
		topicInfoMap      map[int64]*model.TopicInfo
		topicStatisticMap map[int64]*model.TopicStatistic
		err               error
	)
	if req.WithoutRedis {
		topicInfoMap, topicStatisticMap, err = service.Instance.GetTopicByIdsWithoutRedis(ctx, req.Ids, req.WithStatistics, req.WithUserBehavior, req.UserID)
	} else {
		topicInfoMap, topicStatisticMap, err = service.Instance.GetTopicByIds(ctx, req.Ids, req.WithStatistics, req.WithUserBehavior, req.UserID)
	}
	if err != nil {
		if internalErr, ok := err.(*common.InternalError); ok {
			return &pb.GetTopicByIdsResp{
				ErrCode: pb.GetTopicByIdsResp_ErrCode(internalErr.ErrCode),
				ErrMsg:  internalErr.ErrMsg,
			}, nil
		}

		return &pb.GetTopicByIdsResp{}, err
	}

	topicInfoMapPb := make(map[int64]*pb.TopicInfo)
	for k, v := range topicInfoMap {
		topicInfoMapPb[k] = &pb.TopicInfo{
			Detail: &pb.TopicDetail{
				Id:          v.TopicDetail.ID,
				CreatedAt:   timestamppb.New(v.TopicDetail.CreatedAt),
				UpdatedAt:   timestamppb.New(v.TopicDetail.UpdatedAt),
				Title:       v.TopicDetail.Title,
				BgPic:       v.TopicDetail.BGPic,
				ManualAudit: v.TopicDetail.ManualAudit,
				Avatar:      v.TopicDetail.Avatar,
				Sort:        v.TopicDetail.Sort,
				Desc:        v.TopicDetail.Desc,
				Catalogue: func(innerV *model.TopicInfo) []*pb.TopicDetailCatalogueItem {
					topicDetailCatalogueItemArr := make([]*pb.TopicDetailCatalogueItem, 0)
					innerErr := json.Unmarshal([]byte(innerV.TopicDetail.Catalogue), &topicDetailCatalogueItemArr)
					if innerErr != nil {
						handler.Log.Errorf("json.Unmarshal([]byte(innerV.TopicDetail.Catalogue), &topicDetailCatalogueItemArr) err: %v", innerErr)
					}
					return topicDetailCatalogueItemArr
				}(v),
				StartAt: timestamppb.New(v.TopicDetail.StartAt),
				EndAt:   timestamppb.New(v.TopicDetail.EndAt),
				Status:  v.TopicDetail.Status,
			},
			Statistic: func(innerV *model.TopicInfo) *pb.TopicStatistic {
				if topicStatisticMapItem, ok := topicStatisticMap[innerV.TopicDetail.ID]; ok {
					return &pb.TopicStatistic{
						ContentNum:         topicStatisticMapItem.ContentNum,
						MpNum:              topicStatisticMapItem.MpNum,
						ContentExposureNum: topicStatisticMapItem.ContentExposureNum,
					}
				} else {
					return &pb.TopicStatistic{}
				}
			}(v),
			UserBehavior: &pb.TopicUserBehavior{
				IsFollowing: func(innerV *model.TopicInfo) bool {
					if innerV.TopicUserBehavior == nil {
						return false
					} else {
						return true
					}
				}(v),
			},
		}
	}

	return &pb.GetTopicByIdsResp{Data: topicInfoMapPb}, nil
}

func (handler *Handler) TopicList(ctx context.Context, req *pb.TopicListReq) (*pb.TopicListResp, error) {
	topicInfoArr, total, topicStatisticMap, err := service.Instance.TopicList(
		ctx, req.GetKeyword(), []string{}, req.GetSortBy(), req.GetOrderBy(),
		req.GetOffset(), req.GetLimit(), req.GetStartAt(), req.GetEndAt(), req.GetEffectStatus(),
		req.WithStatistics, req.WithUserBehavior, req.UserID, req.GetManualAudit(), req.GetStatusSort())
	if err != nil {
		if internalErr, ok := err.(*common.InternalError); ok {
			return &pb.TopicListResp{
				ErrCode: pb.TopicListResp_ErrCode(internalErr.ErrCode),
				ErrMsg:  internalErr.ErrMsg,
			}, nil
		}

		return &pb.TopicListResp{}, err
	}

	topicInfoArrPb := make([]*pb.TopicInfo, 0)
	for _, v := range topicInfoArr {
		topicInfoArrPb = append(topicInfoArrPb, &pb.TopicInfo{
			Detail: &pb.TopicDetail{
				Id:          v.TopicDetail.ID,
				CreatedAt:   timestamppb.New(v.TopicDetail.CreatedAt),
				UpdatedAt:   timestamppb.New(v.TopicDetail.UpdatedAt),
				Title:       v.TopicDetail.Title,
				BgPic:       v.TopicDetail.BGPic,
				ManualAudit: v.TopicDetail.ManualAudit,
				Avatar:      v.TopicDetail.Avatar,
				Sort:        v.TopicDetail.Sort,
				Desc:        v.TopicDetail.Desc,
				Catalogue: func(innerV *model.TopicInfo) []*pb.TopicDetailCatalogueItem {
					topicDetailCatalogueItemArr := make([]*pb.TopicDetailCatalogueItem, 0)
					innerErr := json.Unmarshal([]byte(innerV.TopicDetail.Catalogue), &topicDetailCatalogueItemArr)
					if innerErr != nil {
						handler.Log.Errorf("json.Unmarshal([]byte(innerV.TopicDetail.Catalogue), &topicDetailCatalogueItemArr) err: %v", innerErr)
					}
					return topicDetailCatalogueItemArr
				}(v),
				StartAt: timestamppb.New(v.TopicDetail.StartAt),
				EndAt:   timestamppb.New(v.TopicDetail.EndAt),
				Status:  v.TopicDetail.Status,
			},
			Statistic: func(innerV *model.TopicInfo) *pb.TopicStatistic {
				if topicStatisticMapItem, ok := topicStatisticMap[innerV.TopicDetail.ID]; ok {
					return &pb.TopicStatistic{
						ContentNum:         topicStatisticMapItem.ContentNum,
						MpNum:              topicStatisticMapItem.MpNum,
						ContentExposureNum: topicStatisticMapItem.ContentExposureNum,
					}
				} else {
					return &pb.TopicStatistic{}
				}
			}(v),
			UserBehavior: &pb.TopicUserBehavior{
				IsFollowing: func(innerV *model.TopicInfo) bool {
					if innerV.TopicUserBehavior == nil {
						return false
					} else {
						return true
					}
				}(v),
			},
		})
	}

	return &pb.TopicListResp{Data: topicInfoArrPb, Total: total}, nil
}

func (handler *Handler) TopicFollowing(ctx context.Context, req *pb.TopicFollowingReq) (*pb.TopicFollowingResp, error) {
	// TODO 关注功能暂时关闭
	return &pb.TopicFollowingResp{
		ErrCode: pb.TopicFollowingResp_STATUS_ERR,
		ErrMsg:  "关注功能暂时关闭",
	}, nil

	//err := service.Instance.TopicFollowing(ctx, req.Action, req.TopicID, req.UserID)
	//if err != nil {
	//	if internalErr, ok := err.(*common.InternalError); ok {
	//		return &pb.TopicFollowingResp{
	//			ErrCode: pb.TopicFollowingResp_ErrCode(internalErr.ErrCode),
	//			ErrMsg:  internalErr.ErrMsg,
	//		}, nil
	//	}
	//
	//	return &pb.TopicFollowingResp{}, err
	//}
	//
	//return &pb.TopicFollowingResp{}, nil
}

func (handler *Handler) HitTopicByTag(ctx context.Context, req *pb.HitTopicByTagReq) (*pb.HitTopicByTagResp, error) {
	topicInfoArr, _, topicStatisticMap, err := service.Instance.TopicList(
		ctx, "", req.GetTags(), pb.TopicListReq_NONE_SORT_TYPE, pb.TopicListReq_NONE_ORDER_TYPE,
		0, -1, nil, nil, pb.TopicListReq_NONE,
		req.WithStatistics, req.WithUserBehavior, req.UserID, pb.TopicListReq_ManualAudit_None, false)
	if err != nil {
		if internalErr, ok := err.(*common.InternalError); ok {
			return &pb.HitTopicByTagResp{
				ErrCode: pb.HitTopicByTagResp_ErrCode(internalErr.ErrCode),
				ErrMsg:  internalErr.ErrMsg,
			}, nil
		}

		return &pb.HitTopicByTagResp{}, err
	}

	topicInfoArrPb := make([]*pb.TopicInfo, 0)
	for _, v := range topicInfoArr {
		topicInfoArrPb = append(topicInfoArrPb, &pb.TopicInfo{
			Detail: &pb.TopicDetail{
				Id:          v.TopicDetail.ID,
				CreatedAt:   timestamppb.New(v.TopicDetail.CreatedAt),
				UpdatedAt:   timestamppb.New(v.TopicDetail.UpdatedAt),
				Title:       v.TopicDetail.Title,
				BgPic:       v.TopicDetail.BGPic,
				ManualAudit: v.TopicDetail.ManualAudit,
				Avatar:      v.TopicDetail.Avatar,
				Sort:        v.TopicDetail.Sort,
				Desc:        v.TopicDetail.Desc,
				Catalogue: func(innerV *model.TopicInfo) []*pb.TopicDetailCatalogueItem {
					topicDetailCatalogueItemArr := make([]*pb.TopicDetailCatalogueItem, 0)
					innerErr := json.Unmarshal([]byte(innerV.TopicDetail.Catalogue), &topicDetailCatalogueItemArr)
					if innerErr != nil {
						handler.Log.Errorf("json.Unmarshal([]byte(innerV.TopicDetail.Catalogue), &topicDetailCatalogueItemArr) err: %v", innerErr)
					}
					return topicDetailCatalogueItemArr
				}(v),
				StartAt: timestamppb.New(v.TopicDetail.StartAt),
				EndAt:   timestamppb.New(v.TopicDetail.EndAt),
				Status:  v.TopicDetail.Status,
			},
			Statistic: func(innerV *model.TopicInfo) *pb.TopicStatistic {
				if topicStatisticMapItem, ok := topicStatisticMap[innerV.TopicDetail.ID]; ok {
					return &pb.TopicStatistic{
						ContentNum:         topicStatisticMapItem.ContentNum,
						MpNum:              topicStatisticMapItem.MpNum,
						ContentExposureNum: topicStatisticMapItem.ContentExposureNum,
					}
				} else {
					return &pb.TopicStatistic{}
				}
			}(v),
			UserBehavior: &pb.TopicUserBehavior{
				IsFollowing: func(innerV *model.TopicInfo) bool {
					if innerV.TopicUserBehavior == nil {
						return false
					} else {
						return true
					}
				}(v),
			},
		})
	}

	return &pb.HitTopicByTagResp{Topics: topicInfoArrPb}, nil
}

func (handler *Handler) MustManualAudit(ctx context.Context, req *pb.MustManualAuditReq) (*pb.MustManualAuditResp, error) {
	if len(req.GetTopics()) > 200 {
		return &pb.MustManualAuditResp{
			ErrCode: 400,
			ErrMsg:  "bad req: len(req.GetTopics()) > 200",
		}, nil
	}

	manualAuditTopics := make([]string, 0)
	manualAuditTopics, err := service.Instance.MustManualAudit(ctx, req.GetTopics())
	if err != nil {
		if internalErr, ok := err.(*common.InternalError); ok {
			return &pb.MustManualAuditResp{
				ErrCode: pb.MustManualAuditResp_ErrCode(internalErr.ErrCode),
				ErrMsg:  internalErr.ErrMsg,
				Topics:  manualAuditTopics,
			}, nil
		}

		return &pb.MustManualAuditResp{}, err
	}

	return &pb.MustManualAuditResp{Topics: manualAuditTopics}, nil
}
