using Aix.RedisMessageBus.Model;
using Aix.RedisMessageBus.RedisImpl;
using Aix.RedisMessageBus.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NCrontab;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Aix.RedisMessageBus.BackgroundProcess
{
    public class CrontabWorkProcess : IBackgroundProcess
    {
        private IServiceProvider _serviceProvider;
        private ILogger<CrontabWorkProcess> _logger;
        private RedisStorage _redisStorage;
        private RedisMessageBusOptions _options;

        private ConcurrentDictionary<string, CrontabSchedule> CrontabScheduleCache = new ConcurrentDictionary<string, CrontabSchedule>();
        public CrontabWorkProcess(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            _logger = _serviceProvider.GetService<ILogger<CrontabWorkProcess>>();
            _redisStorage = _serviceProvider.GetService<RedisStorage>();
            _options = _serviceProvider.GetService<RedisMessageBusOptions>();
        }
        public async Task Execute(BackgroundProcessContext context)
        {
            List<double> nextExecuteDelays = new List<double>(); //记录每个任务的下次执行时间，取最小的等待
            try
            {
                var lockKey = $"{_options.TopicPrefix}crontab:lock";
                await _redisStorage.Lock(lockKey, TimeSpan.FromMinutes(1), async () =>
                {
                    var list = await _redisStorage.GetAllCrontabJobId();
                    foreach (var jobId in list)
                    {
                        if (context.IsShutdownRequested) return;
                        var now = DateTime.Now;
                        var jobData = await _redisStorage.GetCrontabJobData(jobId);
                        if (jobData == null) continue;
                        if (jobData.Status == (int)CrontabJobStatus.Disabled) continue;  //如果设置启用时，要把LastExecuteTime设置为当前时间

                        //暂未实现
                    }
                }, () => Task.CompletedTask);
            }
            finally
            {
                var minValue = nextExecuteDelays.Any() ? nextExecuteDelays.Min() : TimeSpan.FromSeconds(_options.CrontabIntervalSecond).TotalMilliseconds;
                var delay = minValue;// Math.Max(minValue, 1000); 
                _redisStorage.WaitForCrontabJob(TimeSpan.FromMilliseconds(delay), context.CancellationToken);
            }
        }

        private async Task Enqueue(CrontabJobData crontabJobData)
        {
            var jobData = JobData.CreateJobData(crontabJobData.Topic, crontabJobData.Data);
            await _redisStorage.Enqueue(jobData);
        }

        public CrontabSchedule ParseCron(string cron)
        {
            CrontabSchedule result;
            if (CrontabScheduleCache.TryGetValue(cron, out result))
            {
                return result;
            }
            var options = new CrontabSchedule.ParseOptions
            {
                IncludingSeconds = cron.Split(' ').Length > 5,
            };
            result = CrontabSchedule.Parse(cron, options);
            CrontabScheduleCache.TryAdd(cron, result);
            return result;
        }

        public static TimeSpan GetNextDueTime(CrontabSchedule Schedule, DateTime LastDueTime, DateTime now)
        {
            var nextOccurrence = Schedule.GetNextOccurrence(LastDueTime);
            TimeSpan dueTime = nextOccurrence - now;// DateTime.Now;

            if (dueTime.TotalMilliseconds <= 0)
            {
                dueTime = TimeSpan.Zero;
            }

            return dueTime;
        }

        public void Dispose()
        {

        }
    }
}
