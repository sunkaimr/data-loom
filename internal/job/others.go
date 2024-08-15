package job

import (
	"github.com/robfig/cron/v3"
	"github.com/sunkaimr/data-loom/internal/job/workflow"
	"github.com/sunkaimr/data-loom/internal/middlewares"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
	"time"
)

// StartOthersJob 其他定时任务
func StartOthersJob(ctx *common.Context) {
	ctx.Wg.Add(1)
	defer ctx.Wg.Done()
	var err error
	log := ctx.Log

	cleanWorkFlowEntryID := cron.EntryID(0)
	syncClusterEntryID := cron.EntryID(0)
	grabClusterTableSizeEntryID := cron.EntryID(0)

	c := cron.New()

	// 定期清理工作流
	cleanWorkFlowEntryID, err = c.AddFunc(
		"0 23 * * *",
		func() {
			ctx.Wg.Add(1)
			defer ctx.Wg.Done()

			sTime := time.Now()
			log.Debugf("running CleanWorkFlow")
			workflow.CleanWorkFlow(ctx)
			log.Debugf("running CleanWorkFlow done, cost:%v", time.Now().Sub(sTime))
			log.Debugf("next run CleanWorkFlow at %s", c.Entry(cleanWorkFlowEntryID).Next.Format(time.DateTime))
		})
	if err != nil {
		log.Fatalf("add cron job(0 23 * * *) for CleanWorkFlow failed %v", err)
	}

	// 定期同步数集群
	syncClusterEntryID, err = c.AddFunc(
		"0 * * * *",
		func() {
			ctx.Wg.Add(1)
			defer ctx.Wg.Done()

			sTime := time.Now()
			log.Debugf("running SyncClusterJob")
			SyncClusterJob(ctx)
			log.Debugf("running SyncClusterJob done, cost:%v", time.Now().Sub(sTime))
			log.Debugf("next run SyncClusterJob at %s", c.Entry(syncClusterEntryID).Next.Format(time.DateTime))
		})
	if err != nil {
		log.Fatalf("add cron job(0 * * * *) for SyncClusterJob failed %v", err)
	}

	// 定期抓取集群各表的大小， 每月01号执行
	grabClusterTableSizeEntryID, err = c.AddFunc(
		"0 7 1 * *", /* 每月1号早上7点统计一次 */
		//"50 17 * * *",
		func() {
			ctx.Wg.Add(1)
			defer ctx.Wg.Done()

			sTime := time.Now()
			log.Debugf("running GrabClusterTableSize")
			GrabClusterTableSize(ctx)
			log.Debugf("running GrabClusterTableSize done, cost:%v", time.Now().Sub(sTime))
			log.Debugf("next run GrabClusterTableSize at %s", c.Entry(grabClusterTableSizeEntryID).Next.Format(time.DateTime))
		})
	if err != nil {
		log.Fatalf("add cron job(0 5 1 * *) for GrabClusterTableSize failed %v", err)
	}

	c.Start()

	<-ctx.Context.Done()
	c.Stop()
	log.Info("shutdown cron job")
}

func SyncClusterJob(ctx *common.Context) {
	ctx.Wg.Add(1)
	defer ctx.Wg.Done()

	ginCtx := middlewares.NewGinContext(ctx.Log, ctx.DB)
	cluster := &services.ClusterService{}
	_, _ = cluster.SyncCluster(ginCtx)
}

// GrabClusterTableSize 定期抓取集群各表的大小
func GrabClusterTableSize(ctx *common.Context) {
	ctx.Wg.Add(1)
	defer ctx.Wg.Done()

	_, _ = (&services.ClusterService{}).RefreshClusterBigTables(middlewares.NewGinContext(ctx.Log, ctx.DB))
}
