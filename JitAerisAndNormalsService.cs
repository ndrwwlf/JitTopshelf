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
            //string userDir = "C:\\Users\\User";

            Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
            .MinimumLevel.Override("System", LogEventLevel.Information)
            .MinimumLevel.Override("Quartz", LogEventLevel.Error)
            .Enrich.FromLogContext()
            //to outsite of project
            .WriteTo.File(userDir + "/Logs/MasterLog.txt", restrictedToMinimumLevel: LogEventLevel.Information, rollOnFileSizeLimit: true)
            .WriteTo.RollingFile(userDir + "/Logs/Daily/log-{Date}.txt", retainedFileCountLimit: null)
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
                   //.WithSimpleSchedule(x => x.WithIntervalInSeconds(5).WithRepeatCount(0))
                   //.StartNow()
                   .Build();

            scheduler.ScheduleJob(aerisJob, aerisTrigger);

            IJobDetail regressionJob = JobBuilder.Create<WNRdngData01RegressionJob>().Build();

            ITrigger regressionTrigger = TriggerBuilder.Create()
                   .WithSchedule(CronScheduleBuilder.DailyAtHourAndMinute(19, 01))
                   //.WithSimpleSchedule(x => x.WithIntervalInSeconds(5).WithRepeatCount(0))
                   //.StartNow()
                   //.StartAt(DateTime.Now.AddSeconds(8))
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
