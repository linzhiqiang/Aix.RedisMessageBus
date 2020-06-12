﻿using Aix.RedisMessageBus.Model;
using Aix.RedisMessageBus.Utils;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.RedisMessageBus
{
    /// <summary>
    /// 发布订阅实现
    /// </summary>
    public class RedisMessageBus_Subscriber : IMessageBus
    {
        private ILogger<RedisMessageBus_Subscriber> _logger;
        private RedisMessageBusOptions _options;
        ConnectionMultiplexer _connectionMultiplexer;

        ISubscriber _subscriber;

        public RedisMessageBus_Subscriber(ILogger<RedisMessageBus_Subscriber> logger, RedisMessageBusOptions options, ConnectionMultiplexer connectionMultiplexer)
        {
            _logger = logger;
            _options = options;
            _connectionMultiplexer = connectionMultiplexer;
            _subscriber = _connectionMultiplexer.GetSubscriber();
        }
        public Task PublishAsync(Type messageType, object message)
        {
            var data = new RedisMessageBusData { Type = GetHandlerKey(messageType), Data = _options.Serializer.Serialize(message) };
            return _subscriber.PublishAsync(GetTopic(messageType), _options.Serializer.Serialize(data));
        }

        public Task PublishDelayAsync(Type messageType, object message, TimeSpan delay)
        {
            throw new NotImplementedException();
            //await Task.Delay(delay);
            //await this.PublishAsync(messageType, message);
        }

        public Task PublishCrontabAsync(Type messageType, object message, CrontabJobInfo crontabJobInfo)
        {
            throw new NotImplementedException();
        }

        public Task SubscribeAsync<T>(Func<T, Task> handler, SubscribeOptions subscribeOptions = null, CancellationToken cancellationToken = default(CancellationToken)) where T : class
        {
            return _subscriber.SubscribeAsync(GetTopic(typeof(T)), (channel, value) =>
            {
                var task = With.NoException(_logger, async () =>
                {
                    var messageBusData = _options.Serializer.Deserialize<RedisMessageBusData>(value);
                    var obj = _options.Serializer.Deserialize<T>(messageBusData.Data);
                    await handler(obj);
                }, $"消费数据{typeof(T).Name}");

                _subscriber.Wait(task);
            });
        }

        public void Dispose()
        {
            _logger.LogInformation("redis关闭消费者");
            _subscriber.UnsubscribeAll();
        }

        #region private

        private string GetHandlerKey(Type type)
        {
            return String.Concat(type.FullName, ", ", type.Assembly.GetName().Name);
        }

        private string GetTopic(Type type)
        {
            return $"{_options.TopicPrefix ?? ""}{type.Name}";
        }



        #endregion
    }
}
