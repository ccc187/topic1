package grpcClient

import (
	"dm-gitlab.bolo.me/hubpd/basic/grpc"
	"dm-gitlab.bolo.me/hubpd/proto/bi"
	"dm-gitlab.bolo.me/hubpd/topic/config"
)

// BI-ChartData
func NewBIChartDataClient() (bi.ChartDataClient, error) {
	conn, err := grpc.DefaultConn(config.Cfg.GRPCClientAddressBI)
	if err != nil {
		return nil, err
	}
	return bi.NewChartDataClient(conn), nil
}
