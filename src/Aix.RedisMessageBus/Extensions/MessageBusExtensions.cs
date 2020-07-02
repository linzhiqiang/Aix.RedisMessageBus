using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.RedisMessageBus
{
    public static class MessageBusExtensions
    {
        public static Task PublishAsync<T>(this IRedisMessageBus messageBus, T message)
        {
            return messageBus.PublishAsync(typeof(T), message);
        }

        public static Task PublishDelayAsync<T>(this IRedisMessageBus messageBus, T message, TimeSpan delay)
        {
            return messageBus.PublishDelayAsync(typeof(T), message, delay);
        }

        //public static Task PublishCrontabAsync<T>(this IMessageBus messageBus, T message, CrontabJobInfo crontabJobInfo)
        //{
        //    return (messageBus as RedisMessageBus).PublishCrontabAsync(typeof(T), message, crontabJobInfo);
        //}
    }

    public class CrontabJobInfo
    {
        /// <summary>
        /// 定时任务标识  不能重复
        /// </summary>
        public string JobId { get; set; }

        /// <summary>
        /// 定时任务名称
        /// </summary>
        public string JobName { get; set; }

        /// <summary>
        /// 定时表达式 6位表达式  * * * * * *
        /// </summary>
        public string CrontabExpression { get; set; }

        /// <summary>
        /// 0=禁用 1=启用
        /// </summary>
        public CrontabJobStatus Status { get; set; }
    }

    public enum CrontabJobStatus
    {
        /// <summary>
        /// 禁用
        /// </summary>
        Disabled = 0,

        /// <summary>
        /// 启用
        /// </summary>
        Enabled = 1
    }
}
