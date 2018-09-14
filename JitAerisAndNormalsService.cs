using JitTopshelf.Scheduled;
using Quartz;
using Quartz.Impl;
using Serilog;
using Serilog.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JitTopshelf
{
    public class JitAerisAndNormalsService
    {
        public void Start()
        {
            string userDir = "C:\\Users\\workweek";
            //string userDir = "C:\\Users\\andy";

            Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
            .MinimumLevel.Override("System", LogEventLevel.Information)
            .MinimumLevel.Override("Quartz", LogEventLevel.Error)
            .Enrich.FromLogContext()
            //to outsite of project
            .WriteTo.File(userDir + "/Logs/MasterLog.log", restrictedToMinimumLevel: LogEventLevel.Information, fileSizeLimitBytes: 512000, 
                            rollOnFileSizeLimit: true, retainedFileCountLimit : 2, shared: true)
            .WriteTo.RollingFile(userDir + "/Logs/Daily/log-{Date}.log", retainedFileCountLimit: 60, shared: true)
            .WriteTo.Console()
            .CreateLogger();

            AerisJobParams aerisJobParams = new AerisJobParams();

            IScheduler scheduler;
            var schedulerFactory = new StdSchedulerFactory();
            scheduler = schedulerFactory.GetScheduler();
            scheduler.Context.Put("aerisJobParams", aerisJobParams);
            scheduler.Start();

            IJobDetail aerisJob = JobBuilder.Create<AerisJob>().Build();

            ITrigger aerisTrigger = TriggerBuilder.Create()
                   .WithSchedule(CronScheduleBuilder.DailyAtHourAndMinute(07, 11))
                   //.StartNow()
                   .Build();

            scheduler.ScheduleJob(aerisJob, aerisTrigger);

            IJobDetail regressionJob = JobBuilder.Create<WNRdngData01RegressionJob>().Build();

            ITrigger regressionTrigger = TriggerBuilder.Create()
                   .WithSchedule(CronScheduleBuilder.DailyAtHourAndMinute(19, 00))
                   //.StartNow()
                   //.StartAt(DateTime.Now.AddSeconds(18))
                   .Build();

            scheduler.ScheduleJob(regressionJob, regressionTrigger);
        }

        public void Stop()
        {
            IScheduler scheduler = StdSchedulerFactory.GetDefaultScheduler();
            scheduler.Shutdown();
        }
    }
}
