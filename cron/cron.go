package cron

import (
	"dm-gitlab.bolo.me/hubpd/basic/logger"
	"github.com/robfig/cron/v3"
)

var log = logger.GetLogger()

type Cron struct {
	cron *cron.Cron
}

func NewCron() *Cron {
	cronLog := cron.VerbosePrintfLogger(log)
	c := cron.New(
		cron.WithLogger(cronLog),
		cron.WithSeconds(),
		cron.WithChain(
			cron.Recover(cronLog),
			cron.SkipIfStillRunning(cronLog)),
	)
	return &Cron{
		cron: c,
	}
}

func (c *Cron) AddJob(spec string, cmd func()) {
	_, err := c.cron.AddFunc(spec, cmd)
	if err != nil {
		log.Fatalf("Add Job Fail; Err: %v", err)
	}
}

func (c *Cron) Start() {
	c.cron.Start()
}

func (c *Cron) Stop() {
	c.cron.Stop()
}
