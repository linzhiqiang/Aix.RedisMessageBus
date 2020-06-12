using Aix.RedisMessageBus.BackgroundProcess;
using Aix.RedisMessageBus.Model;
using Aix.RedisMessageBus.RedisImpl;
using Aix.RedisMessageBus.Utils;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.RedisMessageBus
{
    public class RedisMessageBus : IMessageBus
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RedisMessageBus> _logger;
        private RedisMessageBusOptions _options;
        private RedisStorage _redisStorage;

        private HashSet<string> Subscribers = new HashSet<string>();
        ConcurrentDictionary<string, List<SubscriberInfo>> _subscriberDict = new ConcurrentDictionary<string, List<SubscriberInfo>>(); //订阅事件
        ProcessExecuter _processExecuter;
        BackgroundProcessContext backgroundProcessContext;
        private volatile bool _isInit = false;
        public RedisMessageBus(IServiceProvider serviceProvider, ILogger<RedisMessageBus> logger
            , RedisMessageBusOptions options
            , ConnectionMultiplexer connectionMultiplexer
            , RedisStorage redisStorage)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _options = options;
            _redisStorage = redisStorage;

            backgroundProcessContext = new BackgroundProcessContext(default(CancellationToken));
            _processExecuter = new ProcessExecuter(_serviceProvider, backgroundProcessContext);

        }
        public async Task PublishAsync(Type messageType, object message)
        {
            AssertUtils.IsNotNull(message, "消息不能null");
            var topic = GetTopic(messageType);
            var jobData = JobData.CreateJobData(topic, _options.Serializer.Serialize(message));
            var result = await _redisStorage.Enqueue(jobData);
            AssertUtils.IsTrue(result, $"redis生产者数据失败,topic:{topic}");
        }

        public async Task PublishDelayAsync(Type messageType, object message, TimeSpan delay)
        {
            AssertUtils.IsNotNull(message, "消息不能null");
            if (delay <= TimeSpan.Zero)
            {
                await PublishAsync(messageType, message);
                return;
            }
            var topic = GetTopic(messageType);
            var jobData = JobData.CreateJobData(topic, _options.Serializer.Serialize(message));
            var result = await _redisStorage.EnqueueDealy(jobData, delay);
            AssertUtils.IsTrue(result, $"redis生产者数据失败,topic:{topic}");
        }

        public async Task PublishCrontabAsync(Type messageType, object message, CrontabJobInfo crontabJobInfo)
        {
            var isExsits = await _redisStorage.ExistsCrontabJob(crontabJobInfo.JobId);
            AssertUtils.IsTrue(isExsits == false, $"该定时任务已存在 jobId={crontabJobInfo.JobId}");
            //传入redis即可
            var crontabJobData = new CrontabJobData
            {
                JobId = crontabJobInfo.JobId,
                JobName = crontabJobInfo.JobName,
                CrontabExpression = crontabJobInfo.CrontabExpression,
                Data = _options.Serializer.Serialize(message),
                Topic = GetTopic(messageType),
                Status = (int)crontabJobInfo.Status
            };
            var result = await _redisStorage.EnqueueCrontab(crontabJobData);
            AssertUtils.IsTrue(result, $"redis生产定时任务失败,topic:{crontabJobData.Topic}");
        }

        public async Task SubscribeAsync<T>(Func<T, Task> handler, SubscribeOptions subscribeOptions = null, CancellationToken cancellationToken = default(CancellationToken)) where T : class
        {
            InitProcess();
            var topic = GetTopic(typeof(T));
            ValidateSubscribe(topic);

            subscribeOptions = subscribeOptions ?? new SubscribeOptions();
            var threadCount = subscribeOptions.ConsumerThreadCount;
            threadCount = threadCount > 0 ? threadCount : _options.DefaultConsumerThreadCount;
            AssertUtils.IsTrue(threadCount > 0, "消费者线程数必须大于0");
            _logger.LogInformation($"订阅[{topic}],threadcount={threadCount}");


            Func<MessageResult, Task> action = async message =>
            {
                var realObj = _options.Serializer.Deserialize<T>(message.Data);
                await handler(realObj);
            };
            for (int i = 0; i < threadCount; i++)
            {
                var process = new WorkerProcess(_serviceProvider, topic);
                await _processExecuter.AddProcess(process, $"redis即时任务处理：{topic}");
                process.OnMessage += action;
            }
            backgroundProcessContext.SubscriberTopics.Add(topic);//便于ErrorProcess处理
        }

        public void Dispose()
        {
            _processExecuter.Close();
        }

        #region private

        /// <summary>
        /// 只有消费端才启动这些
        /// </summary>
        private void InitProcess()
        {
            if (_isInit) return;
            lock (this)
            {
                if (_isInit) return;
                _isInit = true;
            }

            Task.Run(async () =>
            {
                await _processExecuter.AddProcess(new DelayedWorkProcess(_serviceProvider), "redis延迟任务处理");
                await _processExecuter.AddProcess(new ErrorWorkerProcess(_serviceProvider), "redis失败任务处理");
               // await _processExecuter.AddProcess(new CrontabWorkProcess(_serviceProvider), "redis定时任务处理");
            });
        }

        private string GetTopic(Type type)
        {
            string topicName = type.Name;

            var topicAttr = TopicAttribute.GetTopicAttribute(type);
            if (topicAttr != null && !string.IsNullOrEmpty(topicAttr.Name))
            {
                topicName = topicAttr.Name;
            }

            return $"{_options.TopicPrefix ?? ""}{topicName}";
        }

        private void ValidateSubscribe(string topic)
        {
            lock (Subscribers)
            {
                var key = topic;
                AssertUtils.IsTrue(!Subscribers.Contains(key), "重复订阅");
                Subscribers.Add(key);
            }
        }
        #endregion
    }
}
