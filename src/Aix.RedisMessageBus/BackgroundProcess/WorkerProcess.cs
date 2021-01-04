using Aix.RedisMessageBus.Model;
using Aix.RedisMessageBus.RedisImpl;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Aix.RedisMessageBus.BackgroundProcess
{
    /// <summary>
    /// 及时任务执行
    /// </summary>
    internal class WorkerProcess : IBackgroundProcess
    {
        private IServiceProvider _serviceProvider;
        private ILogger<WorkerProcess> _logger;
        private RedisMessageBusOptions _options;
        private RedisStorage _redisStorage;
        private string _topic;

        public event Func<MessageResult, Task> OnMessage;
        public WorkerProcess(IServiceProvider serviceProvider, string topic)
        {
            _serviceProvider = serviceProvider;
            _logger = _serviceProvider.GetService<ILogger<WorkerProcess>>();
            _options = _serviceProvider.GetService<RedisMessageBusOptions>();
            _topic = topic;
            _redisStorage = _serviceProvider.GetService<RedisStorage>();

        }

        public void Dispose()
        {
            _logger.LogInformation("关闭后台任务：redis即时任务处理");
        }
        public async Task Execute(BackgroundProcessContext context)
        {
            FetchJobData jobData = await _redisStorage.FetchNextJob(this._topic);
            if (jobData == null)
            {
                _redisStorage.WaitForJob(TimeSpan.FromMilliseconds(_options.ConsumePullIntervalMillisecond), context.CancellationToken);
                return;
            }
            if (context.IsShutdownRequested) return;
            var isSuccess = await DoWork(jobData);
            if (isSuccess)
            {
                await _redisStorage.SetSuccess(jobData.Topic, jobData.JobId);
            }
            else
            {
                await _redisStorage.SetFail(jobData.Topic, jobData.JobId);
            }
        }

        private async Task<bool> DoWork(FetchJobData fetchJobData)
        {
            MessageResult messageResult = new MessageResult
            {
                Data = fetchJobData.Data,
                Topic = fetchJobData.Topic,
                JobId = fetchJobData.JobId
            };
            var isSuccess = true;
            try
            {
                var task = OnMessage(messageResult);

                await task.TimeoutAfter(TimeSpan.FromSeconds(_options.ExecuteTimeoutSecond));
            }
            catch (TimeoutException ex) //超时
            {
                _logger.LogWarning(ex, $"redis消费超时,topic:{fetchJobData.Topic}");
                isSuccess = true;//超时不重试
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"redis消费失败,topic:{fetchJobData.Topic}");
                if (_options.IsRetry != null)
                {
                    var isRetry = await _options.IsRetry(ex);
                    isSuccess = !isRetry;
                }
            }
            return isSuccess;
        }
    }
}
