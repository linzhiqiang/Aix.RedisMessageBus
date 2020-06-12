using CommandLine;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NCrontab;
using System;

namespace RedisMessageBusSample
{
    /*
     // m=1生产者 m=2消费者 m=3生产+消费      q=生产数量
     dotnet run -m 1 -q 10      //生产
      dotnet run -m 2    //消费
     */
    class Program
    {
        public static void Main(string[] args)
        {
            Parser parser = new Parser((setting) =>
            {
                setting.CaseSensitive = false;
            });

            var cron = "0/5 * * * * *";
            var options = new CrontabSchedule.ParseOptions
            {
                IncludingSeconds = cron.Split(' ').Length > 5,
            };
            var result = CrontabSchedule.Parse(cron, options);
            var now = DateTime.Now;
            var r1 = result.GetNextOccurrence(now.AddHours(-1));
            var r2 = result.GetNextOccurrence(now.AddHours(-1), now);

            parser.ParseArguments<CmdOptions>(args).WithParsed((options) =>
            {
                CmdOptions.Options = options;
                CreateHostBuilder(args, options).Build().Run();
            });

        }

        public static IHostBuilder CreateHostBuilder(string[] args, CmdOptions options)
        {
            return Host.CreateDefaultBuilder(args)
            .ConfigureHostConfiguration(configurationBuilder =>
            {

            })
           .ConfigureAppConfiguration((hostBulderContext, configurationBuilder) =>
           {
           })
            .ConfigureLogging((hostBulderContext, loggingBuilder) =>
            {
                loggingBuilder.SetMinimumLevel(LogLevel.Information);
                //系统也默认加载了默认的log
                loggingBuilder.ClearProviders();
                loggingBuilder.AddConsole();
            })
            .ConfigureServices(Startup.ConfigureServices);

        }

    }
}
