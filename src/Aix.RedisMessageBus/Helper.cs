using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.RedisMessageBus
{
    internal static class Helper
    {
        public static string GetProcessingQueueName(string queue)
        {
            return $"{queue}:processing";
        }
        public static string GetJobHashId(RedisMessageBusOptions options, string jobId)
        {
            return $"{options.TopicPrefix}jobdata:{jobId}";
        }

        public static string GetDelaySortedSetName(RedisMessageBusOptions options)
        {
            return $"{options.TopicPrefix}delay:jobid";
        }

        public static string GetQueueJobChannel(RedisMessageBusOptions options)
        {
            return $"{options.TopicPrefix}QueueJobChannel";
        }

        public static string GetDelayChannel(RedisMessageBusOptions options)
        {
            return $"{options.TopicPrefix}DelayJobChannel";
        }

        public static string GetErrorChannel(RedisMessageBusOptions options)
        {
            return $"{options.TopicPrefix}ErrorJobChannel";
        }

        public static string GetCrontabChannel(RedisMessageBusOptions options)
        {
            return $"{options.TopicPrefix}CrontabJobChannel";
        }

        public static string GetCrontabHashId(RedisMessageBusOptions options,string jobId)
        {
            return $"{options.TopicPrefix}crontabdata:{jobId}";
        }
        public static string GetCrontabSetName(RedisMessageBusOptions options)
        {
            return $"{options.TopicPrefix}CrontabSet";
        }

    }
}
