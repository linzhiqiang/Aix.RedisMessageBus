using Aix.RedisMessageBus.Foundation;
using Aix.RedisMessageBus.RedisImpl;
using Aix.RedisMessageBus.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace Aix.RedisMessageBus.BackgroundProcess
{
    /// <summary>
    /// 延迟任务处理
    /// </summary>
    internal class DelayedWorkProcess : IBackgroundProcess
    {
        private IServiceProvider _serviceProvider;
        private ILogger<DelayedWorkProcess> _logger;
        private RedisMessageBusOptions _options;
        private RedisStorage _redisStorage;
        int BatchCount = 100; //一次拉取多少条
        private int PreReadSecond = 5; //提前读取多长数据
        private volatile bool _isStart = true;

        private TimeSpan lockTimeSpan = TimeSpan.FromMinutes(1);
        public DelayedWorkProcess(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            _logger = _serviceProvider.GetService<ILogger<DelayedWorkProcess>>();
            _options = _serviceProvider.GetService<RedisMessageBusOptions>();
            _redisStorage = _serviceProvider.GetService<RedisStorage>();

            PreReadSecond = _options.DelayTaskPreReadSecond;
        }

        public void Dispose()
        {
            _isStart = false;
            _logger.LogInformation("关闭后台任务：redis延迟任务处理");
        }

        public async Task Execute(BackgroundProcessContext context)
        {
            var lockKey = $"{_options.TopicPrefix}delay:lock";
            long delay = 0; //毫秒
            await _redisStorage.Lock(lockKey, lockTimeSpan, async () =>
            {
                var now = DateTime.Now;
                var maxScore = DateUtils.GetTimeStamp(now);
                var list = await _redisStorage.GetTopDueDealyJobId(maxScore + PreReadSecond * 1000, BatchCount); //多查询1秒的数据，便于精确控制延迟
                foreach (var item in list)
                {
                    if (context.IsShutdownRequested) return;
                    if (_isStart == false) return;//已经关闭了 就直接返回吧
                    if (item.Value > maxScore)
                    {
                        delay = item.Value - maxScore;
                        break;
                    }

                    var jobId = item.Key;
                    // 延时任务到期加入即时任务队列
                    await _redisStorage.DueDealyJobEnqueue(jobId);
                }

                if (list.Count == 0)//没有数据时
                {
                    delay = PreReadSecond * 1000;
                }
            }, async () => await TaskEx.DelayNoException(PreReadSecond * 1000, context.CancellationToken)); //出现并发也休息一会

            if (delay > 0)
            {
                var minDelay = Math.Min((int)delay, PreReadSecond * 1000);
                _redisStorage.WaitForDelayJob(TimeSpan.FromMilliseconds(minDelay), context.CancellationToken);
                //await TaskEx.DelayNoException(Math.Min((int)delay, PreReadSecond * 1000), context.CancellationToken);
            }
        }
    }
}
