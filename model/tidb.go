package model

import (
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"dm-gitlab.bolo.me/hubpd/basic/logger"
	"dm-gitlab.bolo.me/hubpd/basic/models"
	"dm-gitlab.bolo.me/hubpd/topic/config"
)

var Tables = []interface{}{
	&TopicDetail{},
	&TopicUserBehavior{},
	&TopicStatistic{},
}

func GetInstance() *gorm.DB {
	log := logger.GetLogger()

	var instance *gorm.DB
	dsn := config.Cfg.DBDsn
	if len(dsn) == 0 {
		log.Fatal("[model] dsn is null")
	}
	instance, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		log.Fatalf("[model] open db err: %v", err)
	}

	models.DBTracing(instance)

	if config.Cfg.Debug {
		instance = instance.Debug()
	}

	db, _ := instance.DB()
	db.SetMaxIdleConns(config.Cfg.DBMaxIdleConns)
	db.SetMaxOpenConns(config.Cfg.DBMaxOpenConns)
	log.Infof("maxIdleConns: %d, maxOpenConns: %d", config.Cfg.DBMaxIdleConns, config.Cfg.DBMaxOpenConns)
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for {
			<-ticker.C
			stats := db.Stats()
			log.Printf("[dbconn_stats] idle: %d, inuse: %d, maxIdleClosed: %d, maxLifetimeClosed: %d, maxOpenConnections: %d, openConnections: %d, waitCount: %d, waitDuration: %dms", stats.Idle, stats.InUse, stats.MaxIdleClosed, stats.MaxLifetimeClosed, stats.MaxOpenConnections,
				stats.OpenConnections, stats.WaitCount, stats.WaitDuration.Milliseconds())
		}
	}()

	// gen db doc
	// basicUtil "dm-gitlab.bolo.me/hubpd/basic/util"
	//basicUtil.GenTableDescribe(true, Tables...)

	return instance
}
