package handler

import (
	"context"
	core "dm-gitlab.bolo.me/hubpd/basic/grpc"
	"dm-gitlab.bolo.me/hubpd/basic/logger"
	"dm-gitlab.bolo.me/hubpd/proto/bi"
	mockBI "dm-gitlab.bolo.me/hubpd/proto/bi/mock"
	pb "dm-gitlab.bolo.me/hubpd/proto/topic_grpc"
	"dm-gitlab.bolo.me/hubpd/topic/dao"
	"dm-gitlab.bolo.me/hubpd/topic/model"
	"dm-gitlab.bolo.me/hubpd/topic/service"
	"github.com/Shopify/sarama/mocks"
	"github.com/go-redis/redis/v8"
	"github.com/go-testfixtures/testfixtures/v3"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"testing"
	"time"
)

var fixtures *testfixtures.Loader

func TestMain(m *testing.M) {
	var err error

	dbInstance := model.GetInstance()
	_ = dbInstance.Migrator().DropTable(model.Tables...)
	_ = dbInstance.AutoMigrate(model.Tables...)

	log := logger.GetLogger()

	Instance = &Handler{
		Log: log,
	}
	service.Instance = &service.Service{
		Log: log,
	}
	dao.TiDBInstance = &dao.TiDB{
		DB:  dbInstance,
		Log: log,
	}

	loc, _ := time.LoadLocation("Asia/Shanghai")
	ddb, _ := dbInstance.DB()
	fixtures, err = testfixtures.New(
		testfixtures.Database(ddb),
		testfixtures.Dialect("mysql"),
		testfixtures.Directory("../fixtures"),
		testfixtures.Location(loc),
	)
	if err != nil {
		log.Fatal(err)
	}

	// redisInstance
	dao.RedisInstance = &dao.Redis{
		Log: logger.GetLogger(),
		RedisClusterClient: func() *redis.ClusterClient {
			if os.Getenv("REDIS_MODE") == "local" {
				return model.GetRedis()
			}
			return model.GetRedisMock()
		}(),
	}

	os.Exit(m.Run())
}

func prepareTestDatabase() {
	if err := fixtures.Load(); err != nil {
		logger.GetLogger().Fatal(err)
	}
}

func Test_CreateTopic(t *testing.T) {
	prepareTestDatabase()

	mockPub := core.NewMockPublisher(t)
	service.Instance.Pub = mockPub

	type args struct {
		req *pb.CreateTopicReq
	}
	tests := []struct {
		name    string
		args    args
		check   func(t *testing.T, resp *pb.CreateTopicResp)
		wantErr bool
	}{
		{
			name: "name dup",
			args: args{
				req: &pb.CreateTopicReq{
					Data: &pb.TopicDetail{
						Title: "test_title_001",
						BgPic: "test_BgPic_002",
						Sort:  2,
						Catalogue: []*pb.TopicDetailCatalogueItem{
							{
								Key:   "k1",
								Value: "v1",
							},
							{
								Key:   "k2",
								Value: "v2",
							},
						},
					},
				},
			},
			check: func(t *testing.T, resp *pb.CreateTopicResp) {
				if resp.ErrCode != pb.CreateTopicResp_NAME_DUP {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
				//if resp.GetId() == 0 {
				//	t.Errorf("id: 0")
				//}
			},
		},
		{
			name: "ok",
			args: args{
				req: &pb.CreateTopicReq{
					Data: &pb.TopicDetail{
						Title: "test_title_0013",
						BgPic: "test_BgPic_003",
						Sort:  3,
						Desc:  "test",
						Catalogue: []*pb.TopicDetailCatalogueItem{
							{
								Key:   "k1",
								Value: "v1",
							},
							{
								Key:   "k2",
								Value: "v2",
							},
						},
					},
				},
			},
			check: func(t *testing.T, resp *pb.CreateTopicResp) {
				if resp.ErrCode != pb.CreateTopicResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
				if resp.GetId() == 0 {
					t.Errorf("id: 0")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockPub.GetProducer().(*mocks.AsyncProducer).ExpectInputAndSucceed()
			got, err := Instance.CreateTopic(context.Background(), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateTopic() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.check(t, got)
		})
	}
}

func Test_UpdateTopic(t *testing.T) {
	prepareTestDatabase()

	mockPub := core.NewMockPublisher(t)
	service.Instance.Pub = mockPub

	type args struct {
		req *pb.UpdateTopicReq
	}
	tests := []struct {
		name    string
		args    args
		check   func(t *testing.T, resp *pb.UpdateTopicResp)
		wantErr bool
	}{
		{
			name: "name dup",
			args: args{
				req: &pb.UpdateTopicReq{
					Data: &pb.TopicDetail{
						Id:     1,
						Title:  "test_title_002",
						BgPic:  "test_BgPic_002",
						Avatar: "",
						Sort:   2,
						Catalogue: []*pb.TopicDetailCatalogueItem{
							{
								Key:   "k1",
								Value: "v1",
							},
							{
								Key:   "k2",
								Value: "v2",
							},
						},
						StartAt: nil,
						EndAt:   nil,
					},
				},
			},
			check: func(t *testing.T, resp *pb.UpdateTopicResp) {
				if resp.ErrCode != pb.UpdateTopicResp_NAME_DUP {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
		{
			name: "ok",
			args: args{
				req: &pb.UpdateTopicReq{
					Data: &pb.TopicDetail{
						Id:    1,
						Title: "test_title_0013",
						BgPic: "test_BgPic_003",
						Sort:  3,
						Desc:  "test01",
						StartAt: func() *timestamppb.Timestamp {
							todayZeroTime, _ := time.Parse("2006-01-02", time.Now().Format("2006-01-02"))
							t, _ := ptypes.TimestampProto(time.Unix(todayZeroTime.AddDate(0, 0, 1).Unix(), 0))
							return t
						}(),
						EndAt: func() *timestamppb.Timestamp {
							todayZeroTime, _ := time.Parse("2006-01-02", time.Now().Format("2006-01-02"))
							t, _ := ptypes.TimestampProto(time.Unix(todayZeroTime.AddDate(0, 0, 2).Unix(), 0))
							return t
						}(),
						Catalogue: []*pb.TopicDetailCatalogueItem{
							{
								Key:   "k1",
								Value: "v1",
							},
							{
								Key:   "k2",
								Value: "v2",
							},
						},
					},
				},
			},
			check: func(t *testing.T, resp *pb.UpdateTopicResp) {
				if resp.ErrCode != pb.UpdateTopicResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockPub.GetProducer().(*mocks.AsyncProducer).ExpectInputAndSucceed()
			got, err := Instance.UpdateTopic(context.Background(), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateTopic() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.check(t, got)
		})
	}
}

func Test_DelTopicByIds(t *testing.T) {
	prepareTestDatabase()

	mockPub := core.NewMockPublisher(t)
	service.Instance.Pub = mockPub

	type args struct {
		req *pb.DelTopicByIdsReq
	}
	tests := []struct {
		name    string
		args    args
		check   func(t *testing.T, resp *pb.DelTopicByIdsResp)
		wantErr bool
	}{
		{
			name: "ok",
			args: args{
				req: &pb.DelTopicByIdsReq{
					Ids: []int64{1},
				},
			},
			check: func(t *testing.T, resp *pb.DelTopicByIdsResp) {
				if resp.ErrCode != pb.DelTopicByIdsResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockPub.GetProducer().(*mocks.AsyncProducer).ExpectInputAndSucceed()
			mockPub.GetProducer().(*mocks.AsyncProducer).ExpectInputAndSucceed()
			got, err := Instance.DelTopicByIds(context.Background(), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("DelTopicByIds() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.check(t, got)
		})
	}
}

func Test_GetTopicByIds(t *testing.T) {
	prepareTestDatabase()

	if err := service.Instance.InitTopicBitMap(context.Background()); err != nil {
		t.Fatal(err)
	}

	biClient := mockBI.NewMockChartDataClient(gomock.NewController(t))
	service.Instance.BIChartDataClient = biClient

	type args struct {
		req *pb.GetTopicByIdsReq
	}
	tests := []struct {
		name    string
		args    args
		check   func(t *testing.T, resp *pb.GetTopicByIdsResp)
		wantErr bool
	}{
		{
			name: "not found",
			args: args{
				req: &pb.GetTopicByIdsReq{
					Ids:              []int64{14},
					WithStatistics:   true,
					WithUserBehavior: false,
					UserID:           "1",
				},
			},
			check: func(t *testing.T, resp *pb.GetTopicByIdsResp) {
				if resp.ErrCode != pb.GetTopicByIdsResp_NOT_FOUND {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
		{
			name: "ok",
			args: args{
				req: &pb.GetTopicByIdsReq{
					Ids:              []int64{1, 2, 3, 4, 5, 6},
					WithStatistics:   false,
					WithUserBehavior: false,
				},
			},
			check: func(t *testing.T, resp *pb.GetTopicByIdsResp) {
				if resp.ErrCode != pb.GetTopicByIdsResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			biClient.EXPECT().GetConsoleChart(gomock.Any(), gomock.Any()).Return(&bi.ConsoleChartRes{
				ErrCode: 0,
				ErrMsg:  "",
				ChartData: &any.Any{
					Value: []byte(`{
						"data": [{
							"topicId": "1",
							"contentNum": "1",
							"fusionNum": "2",
							"tapNum": "3"
						}]
					}`),
				},
			}, nil).AnyTimes()
			got, err := Instance.GetTopicByIds(context.Background(), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTopicByIds() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.check(t, got)
		})
	}
}

func Test_TopicList(t *testing.T) {
	prepareTestDatabase()

	biClient := mockBI.NewMockChartDataClient(gomock.NewController(t))
	service.Instance.BIChartDataClient = biClient

	startAtP, _ := ptypes.TimestampProto(time.Unix(1605369601, 0)) // 2020-11-15 00:00:01
	endAtP, _ := ptypes.TimestampProto(time.Unix(1605801601, 0))   // 2020-11-20 00:00:01

	type args struct {
		req *pb.TopicListReq
	}
	tests := []struct {
		name    string
		args    args
		check   func(t *testing.T, resp *pb.TopicListResp)
		wantErr bool
	}{
		{
			name: "ok",
			args: args{
				req: &pb.TopicListReq{
					Keyword:          "title_001",
					SortBy:           pb.TopicListReq_SORT_NUM,
					OrderBy:          pb.TopicListReq_DESC,
					Offset:           0,
					Limit:            10,
					WithStatistics:   true,
					WithUserBehavior: true,
					UserID:           "1",
					StatusSort:       true,
				},
			},
			check: func(t *testing.T, resp *pb.TopicListResp) {
				if resp.ErrCode != pb.TopicListResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
		{
			name: "nil",
			args: args{
				req: &pb.TopicListReq{
					Keyword:          "title_0011",
					SortBy:           pb.TopicListReq_SORT_NUM,
					OrderBy:          pb.TopicListReq_DESC,
					Offset:           0,
					Limit:            10,
					WithStatistics:   true,
					WithUserBehavior: true,
					UserID:           "1",
				},
			},
			check: func(t *testing.T, resp *pb.TopicListResp) {
				if resp.ErrCode != pb.TopicListResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
		{
			name: "timeFilter ok",
			args: args{
				req: &pb.TopicListReq{
					Offset:  0,
					Limit:   100,
					SortBy:  pb.TopicListReq_SORT_NUM,
					OrderBy: pb.TopicListReq_ASC,

					StartAt: startAtP,
					EndAt:   endAtP,
				},
			},
			check: func(t *testing.T, resp *pb.TopicListResp) {
				if resp.ErrCode != pb.TopicListResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
				if len(resp.Data) != 4 ||
					resp.Data[0].Detail.Id != 3 || resp.Data[1].Detail.Id != 4 ||
					resp.Data[2].Detail.Id != 5 || resp.Data[3].Detail.Id != 6 {
					t.Errorf("resp.Data: %v", resp.Data)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			biClient.EXPECT().GetConsoleChart(gomock.Any(), gomock.Any()).Return(&bi.ConsoleChartRes{
				ErrCode: 0,
				ErrMsg:  "",
				ChartData: &any.Any{
					Value: []byte(`{
						"data": [{
							"topicId": "1",
							"contentNum": "1",
							"fusionNum": "2",
							"tapNum": "3"
						}]
					}`),
				},
			}, nil).AnyTimes()
			got, err := Instance.TopicList(context.Background(), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("TopicList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.check(t, got)
		})
	}
}

/*func Test_TopicFollowing(t *testing.T) {
	prepareTestDatabase()

	type args struct {
		req *pb.TopicFollowingReq
	}
	tests := []struct {
		name    string
		args    args
		check   func(t *testing.T, resp *pb.TopicFollowingResp)
		wantErr bool
	}{
		{
			name: "following and ok",
			args: args{
				req: &pb.TopicFollowingReq{
					TopicID: 1,
					UserID:  "4",
					Action:  true,
				},
			},
			check: func(t *testing.T, resp *pb.TopicFollowingResp) {
				if resp.ErrCode != pb.TopicFollowingResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
		{
			name: "following and not ok",
			args: args{
				req: &pb.TopicFollowingReq{
					TopicID: 1,
					UserID:  "1",
					Action:  true,
				},
			},
			check: func(t *testing.T, resp *pb.TopicFollowingResp) {
				if resp.ErrCode != pb.TopicFollowingResp_STATUS_ERR {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
		{
			name: "not following and ok",
			args: args{
				req: &pb.TopicFollowingReq{
					TopicID: 1,
					UserID:  "1",
					Action:  false,
				},
			},
			check: func(t *testing.T, resp *pb.TopicFollowingResp) {
				if resp.ErrCode != pb.TopicFollowingResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
		{
			name: "not following and not ok",
			args: args{
				req: &pb.TopicFollowingReq{
					TopicID: 1,
					UserID:  "5",
					Action:  false,
				},
			},
			check: func(t *testing.T, resp *pb.TopicFollowingResp) {
				if resp.ErrCode != pb.TopicFollowingResp_STATUS_ERR {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Instance.TopicFollowing(context.Background(), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("TopicFollowing() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.check(t, got)
		})
	}
}*/

func Test_HitTopicByTag(t *testing.T) {
	prepareTestDatabase()

	biClient := mockBI.NewMockChartDataClient(gomock.NewController(t))
	service.Instance.BIChartDataClient = biClient

	type args struct {
		req *pb.HitTopicByTagReq
	}
	tests := []struct {
		name    string
		args    args
		check   func(t *testing.T, resp *pb.HitTopicByTagResp)
		wantErr bool
	}{
		{
			name: "ok",
			args: args{
				req: &pb.HitTopicByTagReq{
					Tags: []string{"test_title_001"},
				},
			},
			check: func(t *testing.T, resp *pb.HitTopicByTagResp) {
				if resp.ErrCode != pb.HitTopicByTagResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
		{
			name: "ok",
			args: args{
				req: &pb.HitTopicByTagReq{
					Tags:             []string{"test_title_001", "test_title_002"},
					WithStatistics:   true,
					WithUserBehavior: true,
					UserID:           "1",
				},
			},
			check: func(t *testing.T, resp *pb.HitTopicByTagResp) {
				if resp.ErrCode != pb.HitTopicByTagResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
		{
			name: "nil",
			args: args{
				req: &pb.HitTopicByTagReq{
					Tags: []string{"test_title"},
				},
			},
			check: func(t *testing.T, resp *pb.HitTopicByTagResp) {
				if resp.ErrCode != pb.HitTopicByTagResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
		{
			name: "nil",
			args: args{
				req: &pb.HitTopicByTagReq{
					Tags:             []string{"test_title"},
					WithStatistics:   true,
					WithUserBehavior: true,
					UserID:           "1",
				},
			},
			check: func(t *testing.T, resp *pb.HitTopicByTagResp) {
				if resp.ErrCode != pb.HitTopicByTagResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			biClient.EXPECT().GetConsoleChart(gomock.Any(), gomock.Any()).Return(&bi.ConsoleChartRes{
				ErrCode: 0,
				ErrMsg:  "",
				ChartData: &any.Any{
					Value: []byte(`{
						"data": [{
							"topicId": "1",
							"contentNum": "1",
							"fusionNum": "2",
							"tapNum": "3"
						}]
					}`),
				},
			}, nil).AnyTimes()
			got, err := Instance.HitTopicByTag(context.Background(), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HitTopicByTag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.check(t, got)
		})
	}
}

func Test_MustManualAudit(t *testing.T) {
	prepareTestDatabase()

	type args struct {
		req *pb.MustManualAuditReq
	}
	tests := []struct {
		name    string
		args    args
		check   func(t *testing.T, resp *pb.MustManualAuditResp)
		wantErr bool
	}{
		{
			name: "ok",
			args: args{
				req: &pb.MustManualAuditReq{
					Topics: []string{"test_title_001", "test_title_001111"},
				},
			},
			check: func(t *testing.T, resp *pb.MustManualAuditResp) {
				if resp.ErrCode != pb.MustManualAuditResp_NONE {
					t.Errorf("errCode: %d, errMsg: %s", resp.ErrCode, resp.ErrMsg)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Instance.MustManualAudit(context.Background(), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("MustManualAudit() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.check(t, got)
		})
	}
}

//func Test_UpdateTopicStatus(t *testing.T) {
//	prepareTestDatabase()
//	mockPub := core.NewMockPublisher(t)
//	service.Instance.Pub = mockPub
//	for sb := 0; sb < 10000; sb++ {
//		mockPub.GetProducer().(*mocks.AsyncProducer).ExpectInputAndSucceed()
//	}
//
//	if err := service.Instance.UpdateTopicStatus(context.Background()); err != nil {
//		t.Fatal(err)
//	}
//}

//func Test_Tmp(t *testing.T) {
//	tmp := func() time.Time {
//		// 1615219198 2021-03-08 23:59:58
//		// tmpStamp是没有时区的，他代表当地时区下的时间戳，所以要使用tmpStamp.AsTime().Local()转化为带时区的time类型
//		tmpStamp := timestamppb.New(time.Unix(1615219198, 0))
//		tmpTime := tmpStamp.AsTime().Local()  // 2021-03-08 23:59:58 +0800
//
//		// Parse会丢失时区，所以要使用ParseInLocation
//		// todayZeroTime, _ := time.Parse("2006-01-02", tmpTime.Format("2006-01-02"))
//		todayZeroTime, _ := time.ParseInLocation("2006-01-02", tmpTime.Format("2006-01-02"), time.Local)  // 2021-03-08 00:00:00 +0800
//
//		res := todayZeroTime.AddDate(0, 0, 1).Add(-1 * time.Second)  // 2021-03-08 23:59:59 +0800
//		return res
//	}()
//
//	t.Log(tmp)
//}
