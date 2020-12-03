using Aix.RedisMessageBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RedisMessageBusSample.HostedService;
using RedisMessageBusSample.Model;
using System;
using System.Threading.Tasks;

namespace RedisMessageBusSample
{
    public class Startup
    {
        internal static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
        {
            var options = CmdOptions.Options;
            services.AddSingleton(options);

            var redisMessageBusOptions = context.Configuration.GetSection("redis-messagebus").Get<RedisMessageBusOptions>();
            redisMessageBusOptions.IsRetry = ex =>
            {
                //if (typeof(BizException) != ex.GetType())
                //    return Task.FromResult(true);
                return Task.FromResult(false);
            };
            services.AddRedisMessageBus(redisMessageBusOptions); //list实现
                                                                 //services.AddRedisMessageBusPubSub(redisMessageBusOptions);//发布订阅实现


            if ((options.Mode & (int)ClientMode.Consumer) > 0)
            {
                services.AddHostedService<MessageBusConsumeService>();
            }
            if ((options.Mode & (int)ClientMode.Producer) > 0)
            {
                services.AddHostedService<MessageBusProduerService>();
            }
        }
    }
}
