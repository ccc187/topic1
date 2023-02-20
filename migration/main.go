package main

import (
	"dm-gitlab.bolo.me/hubpd/basic/models"
	"dm-gitlab.bolo.me/hubpd/topic/model"
	"fmt"
	"gorm.io/gorm"
)

func main() {
	fmt.Println(models.AutoGenMigration("root:@tcp(127.0.0.1:4000)/topic_source", "migration/sql",
		"root:@tcp(127.0.0.1:4000)/topic_dest", &models.DbConf{V2: &gorm.Config{}}, model.Tables...))
}
