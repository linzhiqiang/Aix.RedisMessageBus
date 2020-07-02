﻿using Aix.RedisMessageBus.RedisImpl;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using System;

namespace Aix.RedisMessageBus
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddRedisMessageBus(this IServiceCollection services, RedisMessageBusOptions options)
        {
            AddService(services, options);
            services.AddSingleton<IRedisMessageBus, RedisMessageBus>();
            return services;
        }

        public static IServiceCollection AddRedisMessageBusPubSub(this IServiceCollection services, RedisMessageBusOptions options)
        {
            AddService(services, options);
            services.AddSingleton<IRedisMessageBus, RedisMessageBus_Subscriber>();
            return services;
        }

        private static IServiceCollection AddService(IServiceCollection services, RedisMessageBusOptions options)
        {
            if (options.ConnectionMultiplexer != null)
            {
                services.AddSingleton(options.ConnectionMultiplexer);
            }
            else if (!string.IsNullOrEmpty(options.ConnectionString))
            {
                var redis = ConnectionMultiplexer.Connect(options.ConnectionString);
                services.AddSingleton(redis);
            }
            else
            {
                throw new Exception("ConnectionMultiplexer或RedisConnectionString为空");
            }

            services.AddSingleton(options);
            services.AddSingleton<RedisStorage>();
            return services;
        }
    }
}
