package model

import (
	"dm-gitlab.bolo.me/hubpd/proto/topic_grpc"
	"gorm.io/gorm"
	"time"
)

type Base struct {
	ID        int64     `gorm:"primarykey;<-:false"`
	CreatedAt time.Time `gorm:"index"`
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

type TopicDetail struct {
	Base

	Title       string                             `json:"title" gorm:"size:255;not null;index"`
	BGPic       string                             `json:"bgPic" gorm:"not null"`
	Avatar      string                             `json:"avatar" gorm:"not null"`
	Sort        int32                              `json:"sort" gorm:"not null;index"`
	Desc        string                             `json:"desc" gorm:"type:text;not null"`
	Catalogue   string                             `json:"catalogue" gorm:"not null"`
	StartAt     time.Time                          `json:"startAt" gorm:"not null"`
	EndAt       time.Time                          `json:"endAt" gorm:"not null"`
	ManualAudit bool                               `json:"manualAudit" gorm:"not null"` // 是否必须人工审核：true-必，false-不必
	Status      topic_grpc.TopicDetail_TopicStatus `json:"status" gorm:"not null"`      // 状态

	Uniq string `gorm:"size:255;not null;unique" remark:"Title-DeletedAt"`
}

func (*TopicDetail) Description() string {
	return "话题表"
}

type TopicStatistic struct {
	Base

	TopicID            int64 `json:"topicId" gorm:"not null;index"`
	ContentNum         int64 `json:"contentNum" gorm:"not null"`
	MpNum              int64 `json:"mpNum" gorm:"not null"`
	ContentExposureNum int64 `json:"contentExposureNum" gorm:"not null"`

	Uniq string `gorm:"size:255;not null;unique" remark:"TopicID-DeletedAt"`
}

func (*TopicStatistic) Description() string {
	return "话题统计数据表"
}

type TopicUserBehavior struct {
	Base

	TopicID int64  `json:"topicId" gorm:"index:topicIdUserID"`
	UserID  string `json:"userId" gorm:"size:255;index:topicIdUserID;index"`

	Uniq string `gorm:"size:255;not null;unique" remark:"TopicID-UserID-DeletedAt"`
}

func (*TopicUserBehavior) Description() string {
	return "话题用户行为表"
}

type TopicInfo struct {
	TopicDetail       *TopicDetail       `json:"topicDetail"`
	TopicStatistic    *TopicStatistic    `json:"topicStatistic"`
	TopicUserBehavior *TopicUserBehavior `json:"topicUserBehavior"`
}
