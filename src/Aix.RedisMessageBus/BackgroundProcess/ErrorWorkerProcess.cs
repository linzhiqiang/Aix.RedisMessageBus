using Aix.RedisMessageBus.Model;
using Aix.RedisMessageBus.RedisImpl;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
                    await _redisStorage.Lock(lockKey, TimeSpan.FromMinutes(1), async () =>
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
            int deleteCount = 0;
            var length = 0;

            var start = BatchCount * -1;
            var end = -1;
            do
            {
                var list = await _redisStorage.GetErrorJobId(topic, start, end);// -100,-1
                length = list.Length;
                deleteCount = await ProcessFailedJob(topic, list);

                end = 0 - ((length - deleteCount) + 1);
                start = end - BatchCount + 1;
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
                    if (jobData.CheckedTime.HasValue == false)
                    {
                        await _redisStorage.SetJobCheckedTime(topic, jobId, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                        await _redisStorage.ClearJobDataIfNotExists(jobId);//防止垃圾数据，SetJobCheckedTime设置一条空数据
                    }
                    else if(DateTime.Now - jobData.CheckedTime.Value > TimeSpan.FromSeconds(10))
                    {
                        await _redisStorage.ErrorReEnqueneDelay(topic, jobId, TimeSpan.Zero);
                    }
                }
                else if (jobData.Status == 1)
                {
                    if (jobData.ExecuteTime.HasValue && (DateTime.Now - jobData.ExecuteTime.Value).TotalSeconds > _options.ExecuteTimeoutSecond)
                    {
                        await _redisStorage.ErrorReEnqueneDelay(topic, jobId, TimeSpan.Zero);
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
