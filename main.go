package main

import (
	"context"
	"dm-gitlab.bolo.me/hubpd/basic/models"
	"dm-gitlab.bolo.me/hubpd/topic/cron"
	"dm-gitlab.bolo.me/hubpd/topic/dao"
	"dm-gitlab.bolo.me/hubpd/topic/grpcClient"
	"errors"
	"github.com/golang-migrate/migrate/v4"

	core "dm-gitlab.bolo.me/hubpd/basic/grpc"
	"dm-gitlab.bolo.me/hubpd/basic/logger"
	topic_grpc_pb "dm-gitlab.bolo.me/hubpd/proto/topic_grpc"
	"dm-gitlab.bolo.me/hubpd/topic/config"
	"dm-gitlab.bolo.me/hubpd/topic/handler"
	"dm-gitlab.bolo.me/hubpd/topic/model"
	"dm-gitlab.bolo.me/hubpd/topic/service"
)

func main() {
	log := logger.GetLogger()

	// init redis client
	redisClusterClient := model.GetRedis()

	/*------------------------------------------------ DB start ------------------------------------------------*/

	dbInstance := model.GetInstance()

	if config.Cfg.ExecuteMigration {
		if config.Cfg.IsMysql {
			models.Tidb2mysqlSchema("migration/sql")
		}

		m, err := migrate.New("file://migration/sql", "mysql://"+config.Cfg.DBDsn)
		if err != nil {
			log.Fatal(err)
		}
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			log.Fatal(err)
		}

		return
	}

	if config.Cfg.Migrate {
		if err := dbInstance.AutoMigrate(model.Tables...); err != nil {
			log.Fatal(err)
		}

		return
	}

	/*------------------------------------------------ DB end -------------------------------------------------*/

	// init bi-chartData client
	biChartDataClient, err := grpcClient.NewBIChartDataClient()
	if err != nil {
		log.Fatalf("new bi-chartData client: %s", err)
	}

	// new service node
	node, err := core.NewDefaultRunner(config.Cfg.GrpcServerAddress,
		core.WithServiceName("topic"),
		core.WithSentry(config.Cfg.SentryDsn),
		core.WithPub(config.Cfg.KafkaHosts),
	)
	if err != nil {
		log.Fatalf("new node err: %v", err)
	}
	defer node.Close()

	// init instance
	handler.Instance = &handler.Handler{
		Log: log,
	}
	service.Instance = &service.Service{
		Log:               log,
		BIChartDataClient: biChartDataClient,
		Pub:               node.Pub,
	}
	dao.TiDBInstance = &dao.TiDB{
		DB:  dbInstance,
		Log: log,
	}
	dao.RedisInstance = &dao.Redis{
		Log:                log,
		RedisClusterClient: redisClusterClient,
	}

	topic_grpc_pb.RegisterTopicServer(node.GrpcServer, handler.Instance)

	// cron
	if config.Cfg.EnableCron {
		cronInstance := cron.NewCron()
		cronInstance.AddJob("0 0 10 * * ?", cronInstance.UpdateTopicStatistic)
		cronInstance.AddJob("0 0 0 * * ?", cronInstance.UpdateTopicStatus)
		cronInstance.Start()
		defer cronInstance.Stop()
	}

	// TODO 只针对存量数据，等线上存量数据都进bitmap后就可以去掉该方法
	_ = service.Instance.InitTopicBitMap(context.Background())

	// TODO 暂时先启动时全量刷新话题状态
	_ = service.Instance.FullUpdateTopicStatus(context.Background())

	// run node
	if err := node.Run(); err != nil {
		log.Fatalf("run node err: %v", err)
	}
}
