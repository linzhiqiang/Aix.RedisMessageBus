using Aix.RedisMessageBus.Model;
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
    /// 执行中队列，错误数据处理 重新入队
    /// </summary>
    internal class ErrorWorkerProcess : IBackgroundProcess
    {
        private IServiceProvider _serviceProvider;
        private ILogger<ErrorWorkerProcess> _logger;
        private RedisMessageBusOptions _options;
        private RedisStorage _redisStorage;
        int BatchCount = 100; //一次拉取多少条
        private volatile bool _isStart = true;

        private TimeSpan lockTimeSpan = TimeSpan.FromMinutes(1);
        public ErrorWorkerProcess(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            _logger = _serviceProvider.GetService<ILogger<ErrorWorkerProcess>>();
            _options = _serviceProvider.GetService<RedisMessageBusOptions>();
            _redisStorage = _serviceProvider.GetService<RedisStorage>();
        }

        public async Task Execute(BackgroundProcessContext context)
        {
            try
            {
                //这个context是个全局的，在订阅时把订阅的topic加入到context，这里就能处理到了，每次循环这些队列
                foreach (var topic in context.SubscriberTopics)
                {
                    var lockKey = $"{_options.TopicPrefix}error:lock";
                    await _redisStorage.Lock(lockKey, lockTimeSpan, async () =>
                    {
                        await ProcessQueue(topic);
                    }, () => Task.CompletedTask);
                }
            }
            finally
            {
                //await Task.Delay(_options.ErrorReEnqueueIntervalSecond * 1000);
                var waitTime = TimeSpan.FromSeconds(_options.ErrorReEnqueueIntervalSecond);
                _redisStorage.WaitForErrorJob(waitTime);
            }
        }

        public void Dispose()
        {
            _isStart = false;
            _logger.LogInformation("关闭后台任务：redis失败任务处理");
        }

        private async Task ProcessQueue(string topic)
        {
            var startProcessTime = DateTime.Now;
            int deleteCount = 0;
            var length = 0;

            var start = BatchCount * -1;
            var end = -1;
            do
            {
                var list = await _redisStorage.GetErrorJobId(topic, start, end);// -100,-1  //从队列的尾部抓取    -1=最后一个，-2=倒数第二个，...
                length = list.Length;
                deleteCount = await ProcessFailedJob(topic, list);//倒序处理

                end = 0 - (length + 1 - deleteCount);
                start = end - BatchCount + 1;

                if (DateTime.Now - startProcessTime >= lockTimeSpan)
                {
                    break;
                }
            }
            while (length > 0);
        }

        public async Task<int> ProcessFailedJob(string topic, string[] list)
        {
            int deleteCount = 0;
            for (var i = list.Length - 1; i >= 0; i--)
            {
                if (_isStart == false) return deleteCount;
                var jobId = list[i];
                JobData jobData = await _redisStorage.GetJobData(jobId);
                if (jobData == null)
                {
                    await _redisStorage.RemoveErrorJobId(topic, jobId);
                    deleteCount++;
                    continue;
                }
                if (jobData.ErrorCount > _options.MaxErrorReTryCount)
                {
                    await _redisStorage.RemoveErrorJobId(topic, jobId);
                    deleteCount++;
                    continue;
                }

                if (jobData.Status == 0)
                {
                    //2种情况 1:在队列拉去之后未改状态之前   2:在队列拉出来之后就服务重启了（状态还是没改）

                    if (jobData.CheckedTime == 0)
                    {
                        await _redisStorage.SetJobCheckedTime(topic, jobId, DateUtils.GetTimeStamp());
                        await _redisStorage.ClearJobDataIfNotExists(jobId);//防止垃圾数据，SetJobCheckedTime设置一条空数据
                    }
                    else if ((DateUtils.GetTimeStamp() - jobData.CheckedTime) > TimeSpan.FromSeconds(5).TotalMilliseconds)//一定是重启丢失了，再任务队列拉去完之后，不可能5秒状态还没改成功吧
                    {
                        await _redisStorage.ErrorReEnqueneDelay(topic, jobId, TimeSpan.Zero);
                        deleteCount++;
                    }
                }
                else if (jobData.Status == 1)
                {
                    //1：任务执行中  2：任务把状态改为1之后就重启了 3：执行完改状态失败了
                    if (jobData.ExecuteTime > 0 && (DateUtils.GetTimeStamp() - jobData.ExecuteTime) > _options.ExecuteTimeoutSecond * 1000)//每个job有个超时时间，没有取系统配置
                    {
                        await _redisStorage.ErrorReEnqueneDelay(topic, jobId, TimeSpan.Zero);
                        deleteCount++;
                    }
                }
                else if (jobData.Status == 2)
                {
                    await _redisStorage.SuccessRemoveJobData(topic, jobId);
                    deleteCount++;
                    continue;

                }
                else if (jobData.Status == 9)
                {
                    var delaySecond = GetDelaySecond(jobData.ErrorCount);
                    await _redisStorage.ErrorReEnqueneDelay(topic, jobId, TimeSpan.FromSeconds(delaySecond));
                    deleteCount++;
                }

            }
            return deleteCount;
        }

        private int GetDelaySecond(int errorCount)
        {
            errorCount = errorCount > 0 ? errorCount - 1 : errorCount;
            var retryStrategy = _options.GetRetryStrategy();
            if (errorCount < retryStrategy.Length)
            {
                return retryStrategy[errorCount];
            }
            return retryStrategy[retryStrategy.Length - 1];
        }
        private int GetWaitTimeOld(int errorCount)
        {
            if (errorCount <= 0) errorCount = 1;
            var rand = new Random(Guid.NewGuid().GetHashCode());
            var nextTry = rand.Next(
                (int)Math.Pow(errorCount, 2), (int)Math.Pow(errorCount + 1, 2) + 1);

            return nextTry;
        }
    }
}
