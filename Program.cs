using JitTopshelf.Scheduled;
using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Topshelf;
using Topshelf.Quartz;

namespace JitTopshelf
{
    class Program
    {
        static void Main(string[] args)
        {
            var rc = HostFactory.Run(config =>
            {
                config.Service<JitAerisAndNormalsService>(s =>
                {
                    s.ConstructUsing(name => new JitAerisAndNormalsService());
                    s.WhenStarted(tc => tc.Start());
                    s.WhenStopped(tc => tc.Stop());

                    //s.ScheduleQuartzJob(q =>
                    //    q.WithJob(() =>
                    //        JobBuilder.Create<AerisJob>().Build())
                    //    .AddTrigger(() => 
                    //        TriggerBuilder.Create()
                    //            //.WithSchedule(CronScheduleBuilder.DailyAtHourAndMinute(7, 11))
                    //            .StartNow()
                    //            .WithSimpleSchedule(x => x.WithIntervalInSeconds(5).WithRepeatCount(0))
                    //            .Build())
                    //    );

                    //s.ScheduleQuartzJob(q =>
                    //    q.WithJob(() =>
                    //        JobBuilder.Create<WNRdngData01RegressionJob>().Build())
                    //    .AddTrigger(()
                    //        => TriggerBuilder.Create()
                    //            //.WithSchedule(CronScheduleBuilder.DailyAtHourAndMinute(19, 01))
                    //            .WithSimpleSchedule(x => x.WithIntervalInSeconds(5).WithRepeatCount(0))
                    //            .Build())
                    //    );
                });

                config.RunAs(".\\workweek", "Jon23505");
                config.StartAutomatically();

                config.SetServiceName("WeatherRegressionService");
                config.SetDescription("WeatherJob calls Aeris for new Weather Data at 7:11 AM then finds new Readings and calculates their WthExpUsage. \n" +
                    "WNRdngData01RegressionJob fires at 7:00 PM. The WNRdngData01 stored procedure is executed to find new Acc/Util/UnitIDs in need of regression " +
                    "analysis. For each new Acc/Util/UnitID the regression model that has the highest R2 value and passes t tests with a confidence of 0.10 is found and inserted into/updated in WthNormalParams. \n" +
                    "Logs in DailyLogs are restricted to Debug Level Logging and have max file count of 60." +
                    "MasterLog is restricted to Information Level Logging and has a max file size of 512 Kb, and max file count of 2.");
            });

            var exitCode = (int) Convert.ChangeType(rc, rc.GetTypeCode());
            Environment.ExitCode = exitCode;
        }
    }
}
