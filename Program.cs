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

                });

                config.RunAs(".\\workweek", "Jon23505");
                //config.RunAs(".\\andy", "j");
                config.StartAutomatically();

                config.SetServiceName("WeatherRegressionService");
                config.SetDescription("Stop and Start the service to run the calls to Aeris Weather, calculate the best-fit regression model " +
                    "for new WthNormalParams, and finally calculate ExpUsage for new Readings, The WeatherData and Regression jobs will " +
                    "remain scheduled at their usual times: 7:11 AM and 7:00 PM, respectively. ExpUsage is calcuated after both jobs.");
            });

            var exitCode = (int) Convert.ChangeType(rc, rc.GetTypeCode());
            Environment.ExitCode = exitCode;
        }
    }
}
