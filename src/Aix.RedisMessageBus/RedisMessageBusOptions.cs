using Aix.RedisMessageBus.Serializer;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.RedisMessageBus
{
    public class RedisMessageBusOptions
    {
        private int[] DefaultRetryStrategy = new int[] { 1, 5, 10, 30, 60, 2 * 60, 2 * 60, 2 * 60, 5 * 60, 5 * 60 };
        public RedisMessageBusOptions()
        {
            this.TopicPrefix = "redis:messagebus:";
            this.Serializer = new MessagePackSerializer();
            this.DataExpireDay = 7;
            this.DefaultConsumerThreadCount = 2;
            this.ErrorReEnqueueIntervalSecond = 60;
            this.ExecuteTimeoutSecond = 120;
            this.MaxErrorReTryCount = 5;
            this.CrontabLockSecond = 60;
        }

        /// <summary>
        /// RedisConnectionString和ConnectionMultiplexer传一个即可
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        ///  RedisConnectionString和ConnectionMultiplexer传一个即可
        /// </summary>
        public ConnectionMultiplexer ConnectionMultiplexer { get; set; }

        /// <summary>
        /// topic前缀，为了防止重复，建议用项目名称
        /// </summary>
        public string TopicPrefix { get; set; }

        /// <summary>
        /// 自定义序列化，默认为MessagePack
        /// </summary>
        public ISerializer Serializer { get; set; }

        /// <summary>
        /// 任务数据有效期 默认7天 单位  天
        /// </summary>
        public int DataExpireDay { get; set; }

        /// <summary>
        /// 定时任务锁定时间
        /// </summary>
        public int CrontabLockSecond { get; set; }

        /// <summary>
        /// 默认每个类型的消费线程数 默认2个
        /// </summary>
        public int DefaultConsumerThreadCount { get; set; }

        /// <summary>
        /// 错误数据重新入队  线程执行间隔
        /// </summary>
        public int ErrorReEnqueueIntervalSecond { get; set; }

        /// <summary>
        /// 执行超时时间，超过该时间，任务执行错误尝试重试 120秒
        /// </summary>
        public int ExecuteTimeoutSecond { get; set; }

        /// <summary>
        /// 最大错误重试次数 默认5次
        /// </summary>
        public int MaxErrorReTryCount { get; set; }

        /// <summary>
        /// 失败重试延迟策略 单位：秒 ,不要直接调用请调用GetRetryStrategy()  默认失败次数对应值延迟时间[ 1,5, 10, 30,  60,  60, 2 * 60, 2 * 60, 5 * 60, 5 * 60  ];
        /// </summary>
        public int[] RetryStrategy { get; set; }

        public int[] GetRetryStrategy()
        {
            if (RetryStrategy == null || RetryStrategy.Length == 0) return DefaultRetryStrategy;
            return RetryStrategy;
        }

        /// <summary>
        /// 是否为重试异常
        /// </summary>
        public Func<Exception, Task<bool>> IsRetry { get; set; }
    }
}
