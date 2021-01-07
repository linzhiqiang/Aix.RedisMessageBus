using Aix.RedisMessageBus.Foundation;
using Aix.RedisMessageBus.Model;
using Aix.RedisMessageBus.Utils;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.RedisMessageBus.RedisImpl
{
    public class RedisStorage
    {
        private IServiceProvider _serviceProvider;
        private ConnectionMultiplexer _redis = null;
        private IDatabase _database;
        private RedisMessageBusOptions _options;
        private readonly RedisSubscription _queueJobChannelSubscription;
        private readonly RedisSubscription _errorJobChannelSubscription;
        private readonly RedisSubscription _crontabJobChannelSubscription;
        private readonly RedisSubscription _delayJobChannelSubscription;
        public RedisStorage(IServiceProvider serviceProvider, ConnectionMultiplexer redis, RedisMessageBusOptions options)
        {
            _serviceProvider = serviceProvider;
            this._redis = redis;
            this._options = options;
            _database = redis.GetDatabase();
            _queueJobChannelSubscription = new RedisSubscription(_serviceProvider, _redis.GetSubscriber(), Helper.GetQueueJobChannel(_options));
            _errorJobChannelSubscription = new RedisSubscription(_serviceProvider, _redis.GetSubscriber(), Helper.GetErrorChannel(_options));
            _crontabJobChannelSubscription = new RedisSubscription(_serviceProvider, _redis.GetSubscriber(), Helper.GetCrontabChannel(_options));
            _delayJobChannelSubscription = new RedisSubscription(_serviceProvider, _redis.GetSubscriber(), Helper.GetDelayChannel(_options));

        }

        #region 生产者

        /// <summary>
        /// 添加即时任务
        /// </summary>
        /// <param name="job"></param>
        /// <param name="queue"></param>
        /// <returns></returns>
        public Task<bool> Enqueue(JobData jobData)
        {
            var values = jobData.ToDictionary();
            var topic = jobData.Topic;
            var hashJobId = Helper.GetJobHashId(_options, jobData.JobId);

            var trans = _database.CreateTransaction();

            trans.HashSetAsync(hashJobId, values.ToArray());
            trans.KeyExpireAsync(hashJobId, TimeSpan.FromDays(_options.DataExpireDay));
            trans.ListLeftPushAsync(topic, jobData.JobId);
            //trans.PublishAsync(_queueJobChannelSubscription.Channel, jobData.JobId); //除非要求实时非常高的 才这么做

            var result = trans.Execute();

            return Task.FromResult(result);
        }

        /// <summary>
        /// 延时任务
        /// </summary>
        /// <param name="job"></param>
        /// <param name="timeSpan"></param>
        /// <returns></returns>
        public Task<bool> EnqueueDealy(JobData jobData, TimeSpan delay)
        {
            var values = jobData.ToDictionary();
            var hashJobId = Helper.GetJobHashId(_options, jobData.JobId);

            var trans = _database.CreateTransaction();

            trans.HashSetAsync(hashJobId, values.ToArray());
            //trans.KeyExpireAsync(hashJobId, TimeSpan.FromDays(_options.DataExpireDay));
            trans.SortedSetAddAsync(Helper.GetDelaySortedSetName(_options), jobData.JobId, DateUtils.GetTimeStamp(DateTime.Now.Add(delay))); //当前时间戳，
            if (delay < TimeSpan.FromSeconds(_options.DelayTaskPreReadSecond))
            {
                trans.PublishAsync(_delayJobChannelSubscription.Channel, jobData.JobId);
            }

            var result = trans.Execute();

            return Task.FromResult(result);
        }

        public Task<bool> ExistsCrontabJob(string crontabJobId)
        {
            var hashId = Helper.GetCrontabHashId(_options, crontabJobId);
            return _database.HashExistsAsync(hashId, nameof(CrontabJobData.JobId));
        }

        public Task<bool> EnqueueCrontab(CrontabJobData crontabJobData)
        {
            var values = crontabJobData.ToDictionary();
            var hashId = Helper.GetCrontabHashId(_options, crontabJobData.JobId);

            var trans = _database.CreateTransaction();
            trans.KeyDeleteAsync(hashId);
            trans.HashSetAsync(hashId, values.ToArray());
            trans.SetAddAsync(Helper.GetCrontabSetName(_options), crontabJobData.JobId);

            trans.PublishAsync(_crontabJobChannelSubscription.Channel, crontabJobData.JobId);

            var result = trans.Execute();

            return Task.FromResult(result);
        }

        #endregion

        #region 及时任务
        /// <summary>
        /// 获取下一个及时任务
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        public async Task<FetchJobData> FetchNextJob(string topic)
        {
            var processingQueue = Helper.GetProcessingQueueName(topic);
            string jobId = await _database.ListRightPopLeftPushAsync(topic, processingQueue);//加入备份队列，执行完进行移除

            if (string.IsNullOrEmpty(jobId))
            {
                return null;
            }

            await _database.HashSetAsync(Helper.GetJobHashId(_options, jobId), new HashEntry[] {
                     new HashEntry("ExecuteTime",DateUtils.GetTimeStamp()),
                     new HashEntry(nameof(JobData.Status),1) //0 待执行，1 执行中，2 成功，9 失败
             });

            byte[] data = await _database.HashGetAsync(Helper.GetJobHashId(_options, jobId), nameof(JobData.Data));//取出数据字段

            return new FetchJobData { JobId = jobId, Topic = topic, Data = data };
        }

        public Task SetSuccess(string topic, string jobId)
        {
            return SuccessRemoveJobData(topic, jobId);
        }

        public Task SuccessRemoveJobData(string topic, string jobId)
        {
            var trans = _database.CreateTransaction();
            trans.ListRemoveAsync(Helper.GetProcessingQueueName(topic), jobId);
            trans.KeyDeleteAsync(Helper.GetJobHashId(_options, jobId));

            return trans.ExecuteAsync();
        }

        public Task SetFail(string topic, string jobId)
        {
            var trans = _database.CreateTransaction();
            trans.HashIncrementAsync(Helper.GetJobHashId(_options, jobId), nameof(JobData.ErrorCount), 1);
            trans.HashSetAsync(Helper.GetJobHashId(_options, jobId), new HashEntry[] {
                     new HashEntry(nameof(JobData.Status),9) //0 待执行，1 执行中，2 成功，9 失败
             });
            _database.PublishAsync(_errorJobChannelSubscription.Channel, jobId);
            return trans.ExecuteAsync();


        }

        #endregion

        #region 延迟任务

        /// <summary>
        /// 查询到期的延迟任务
        /// </summary>
        /// <param name="timeStamp"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Task<IDictionary<string, long>> GetTopDueDealyJobId(long timeStamp, int count)
        {
            var nowTimeStamp = timeStamp;
            var result = _database.SortedSetRangeByScoreWithScores(Helper.GetDelaySortedSetName(_options), double.NegativeInfinity, nowTimeStamp, Exclude.None, Order.Ascending, 0, count);
            IDictionary<string, long> dict = new Dictionary<string, long>();
            foreach (SortedSetEntry item in result)
            {
                dict.Add(item.Element, (long)item.Score);
            }
            return Task.FromResult(dict);
        }

        /// <summary>
        /// 到期的延迟任务 插入及时任务
        /// </summary>
        /// <param name="jobId"></param>
        /// <returns></returns>
        public async Task DueDealyJobEnqueue(string jobId)
        {
            var hashJobId = Helper.GetJobHashId(_options, jobId);
            string topic = await _database.HashGetAsync(hashJobId, nameof(JobData.Topic));

            var trans = _database.CreateTransaction();
#pragma warning disable CS4014
            trans.ListLeftPushAsync(topic, jobId);
            trans.SortedSetRemoveAsync(Helper.GetDelaySortedSetName(_options), jobId);
            trans.KeyExpireAsync(hashJobId, TimeSpan.FromDays(_options.DataExpireDay));//进入延迟任务就设置有效期
            //trans.PublishAsync(_queueJobChannelSubscription.Channel, jobId);
#pragma warning restore CS4014 // 由于此调用不会等待，因此在调用完成前将继续执行当前方法
            await trans.ExecuteAsync();

        }

        #endregion

        #region 定时任务

        public Task<string[]> GetAllCrontabJobId()
        {
            var list = _database.SetMembers(Helper.GetCrontabSetName(_options));
            return Task.FromResult(list.ToStringArray());
        }

        public async Task<CrontabJobData> GetCrontabJobData(string jobId)
        {
            var data = await _database.HashGetAllAsync(Helper.GetCrontabHashId(_options, jobId));
            if (data == null || data.Length == 0) return null;

            var dict = data.ToDictionary();
            CrontabJobData result = new CrontabJobData
            {
                JobId = dict.ContainsKey(nameof(CrontabJobData.JobId)) ? dict[nameof(CrontabJobData.JobId)] : RedisValue.EmptyString,
                JobName = dict.ContainsKey(nameof(CrontabJobData.JobName)) ? dict[nameof(CrontabJobData.JobName)] : RedisValue.EmptyString,
                CrontabExpression = dict.ContainsKey(nameof(CrontabJobData.CrontabExpression)) ? dict[nameof(CrontabJobData.CrontabExpression)] : RedisValue.EmptyString,
                Data = dict.ContainsKey(nameof(CrontabJobData.Data)) ? dict[nameof(CrontabJobData.Data)] : RedisValue.Null,
                Topic = dict.ContainsKey(nameof(CrontabJobData.Topic)) ? dict[nameof(CrontabJobData.Topic)] : RedisValue.EmptyString,
                LastExecuteTime = NumberUtils.ToLong(dict.GetValue(nameof(CrontabJobData.LastExecuteTime))),
                Status = NumberUtils.ToInt(dict.GetValue(nameof(CrontabJobData.Status)))

                //  dict.ContainsKey(nameof(CrontabJobData.LastExecuteTime)) ? dict[nameof(CrontabJobData.LastExecuteTime)] : RedisValue.EmptyString,
            };
            if (string.IsNullOrEmpty(result.JobId)) result = null;
            return result;
        }

        public Task SetCrontabJobExecuteTime(string jobId, long timestamp)
        {
            _database.HashSet(Helper.GetCrontabHashId(_options, jobId), nameof(CrontabJobData.LastExecuteTime), timestamp);
            return Task.CompletedTask;
        }


        #endregion

        #region 错误的数据处理

        public async Task<string[]> GetErrorJobId(string topic, int start, int end)
        {
            var list = await _database.ListRangeAsync(Helper.GetProcessingQueueName(topic), start, end);
            return list.ToStringArray();
        }

        public Task RemoveErrorJobId(string topic, string jobId)
        {
            var trans = _database.CreateTransaction();
            trans.ListRemoveAsync(Helper.GetProcessingQueueName(topic), jobId);
            trans.KeyDeleteAsync(Helper.GetJobHashId(_options, jobId));
            return trans.ExecuteAsync();
        }

        public Task SetJobCheckedTime(string topic, string jobId, long checkedTime)
        {
            return _database.HashSetAsync(Helper.GetJobHashId(_options, jobId), nameof(JobData.CheckedTime), checkedTime);
        }

        /// <summary>
        /// 错误数据 进入延迟
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="jobId"></param>
        /// <param name="delay"></param>
        /// <returns></returns>
        public Task ErrorReEnqueneDelay(string topic, string jobId, TimeSpan delay)
        {
            var trans = _database.CreateTransaction();

            trans.ListRemoveAsync(Helper.GetProcessingQueueName(topic), jobId);
            trans.HashSetAsync(Helper.GetJobHashId(_options, jobId), new HashEntry[] {
                     new HashEntry(nameof(JobData.ExecuteTime),0),
                     new HashEntry(nameof(JobData.CheckedTime),0),
                     new HashEntry(nameof(JobData.Status),0) //0 待执行，1 执行中，2 成功，9 失败
             });
            if (delay <= TimeSpan.Zero)
            {
                trans.ListLeftPushAsync(topic, jobId);
            }
            else
            {
                trans.SortedSetAddAsync(Helper.GetDelaySortedSetName(_options), jobId, DateUtils.GetTimeStamp(DateTime.Now.AddMilliseconds(delay.TotalMilliseconds))); //当前时间戳，
            }
            return trans.ExecuteAsync();
        }



        #endregion

        public async Task<JobData> GetJobData(string jobId)
        {
            var dict = (await _database.HashGetAllAsync(Helper.GetJobHashId(_options, jobId))).ToDictionary();

            JobData result = new JobData
            {
                JobId = dict.GetValue(nameof(JobData.JobId)),
                CreateTime = NumberUtils.ToLong(dict.GetValue(nameof(JobData.CreateTime))),
                Data = dict.GetValue(nameof(JobData.Data)),
                ExecuteTime = NumberUtils.ToLong(dict.GetValue(nameof(JobData.ExecuteTime))),
                Topic = dict.GetValue(nameof(JobData.Topic)),
                Status = NumberUtils.ToInt(dict.GetValue(nameof(JobData.Status))),
                ErrorCount = NumberUtils.ToInt(dict.GetValue(nameof(JobData.ErrorCount))),
                CheckedTime = NumberUtils.ToLong(dict.GetValue(nameof(JobData.CheckedTime))),
            };

            if (string.IsNullOrEmpty(result.JobId))
            {
                result = null;
            }
            return result;
        }

        public async Task<bool> ClearJobDataIfNotExists(string jobId)
        {
            var value = await _database.HashGetAsync(Helper.GetJobHashId(_options, jobId), nameof(JobData.JobId));
            if (value.HasValue == false)
            {
                await _database.KeyDeleteAsync(Helper.GetJobHashId(_options, jobId));
                return true;
            }
            return false;
        }

        public void WaitForJob(TimeSpan timeSpan, CancellationToken cancellationToken = default(CancellationToken))
        {
            _queueJobChannelSubscription.WaitForJob(timeSpan, cancellationToken);
        }

        public void WaitForErrorJob(TimeSpan timeSpan, CancellationToken cancellationToken = default(CancellationToken))
        {
            _errorJobChannelSubscription.WaitForJob(timeSpan, cancellationToken);
        }

        public void WaitForCrontabJob(TimeSpan timeSpan, CancellationToken cancellationToken = default(CancellationToken))
        {
            _crontabJobChannelSubscription.WaitForJob(timeSpan, cancellationToken);
        }

        public void WaitForDelayJob(TimeSpan timeSpan, CancellationToken cancellationToken = default(CancellationToken))
        {
            _delayJobChannelSubscription.WaitForJob(timeSpan, cancellationToken);
        }



        public async Task Lock(string key, TimeSpan span, Func<Task> action, Func<Task> concurrentCallback = null)
        {
            string token = Guid.NewGuid().ToString();
            if (_database.LockTake(key, token, span))
            {
                try
                {
                    await action();
                }
                catch
                {
                    throw;
                }
                finally
                {
                    _database.LockRelease(key, token);
                }
            }
            else
            {
                if (concurrentCallback != null) await concurrentCallback();
                else throw new Exception($"出现并发key:{key}");
            }
        }
    }
}
