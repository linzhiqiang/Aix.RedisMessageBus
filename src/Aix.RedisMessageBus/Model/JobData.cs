using Aix.RedisMessageBus.Utils;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.RedisMessageBus.Model
{
    public class JobData
    {
        public static JobData CreateJobData(string topic, byte[] data)
        {
            return new JobData
            {
                Data = data,
                CreateTime = DateUtils.GetTimeStamp(),
                Topic = topic
            };
        }

        public JobData()
        {
            JobId = Guid.NewGuid().ToString().Replace("-", "");
        }
        public string JobId { get; set; }

        /// <summary>
        /// 创建时间
        /// </summary>
        public long CreateTime { get; set; }

        /// <summary>
        /// 业务数据
        /// </summary>
        public byte[] Data { get; set; }

        public long ExecuteTime { get; set; }

        /// <summary>
        /// 0 待执行，1 执行中，2 成功，9 失败
        /// </summary>
        public int Status { get; set; }

        public int ErrorCount { get; set; }

        public string Topic { get; set; }

        public long CheckedTime { get; set; }

        public List<HashEntry> ToDictionary()
        {
            var result = new List<HashEntry>
            {
                new HashEntry("JobId",JobId),
                new HashEntry("CreateTime",CreateTime),
                new HashEntry("ExecuteTime", ExecuteTime),
                new HashEntry("Data",Data),
                new HashEntry("Status",Status),
                new HashEntry("ErrorCount",ErrorCount),
                new HashEntry("Topic",Topic),
                new HashEntry("CheckedTime", CheckedTime)
            };

            return result;
        }

        //private static string TimeToString(DateTime? time)
        //{
        //    if (time != null) return time.Value.ToString("yyyy-MM-dd HH:mm:ss fff");
        //    return string.Empty;
        //}

    }
}
