using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RouteOptimizationService.Services
{
    public class RedisService
    {
        private readonly IDatabase db;

        public RedisService(string connectionString)
        {
            var redis = ConnectionMultiplexer.Connect(connectionString + ",abortConnect=false");
            db = redis.GetDatabase();
        }

        public async Task SetAsync(string key, string value, TimeSpan? expiry = null)
        {
            await db.StringSetAsync(key, value, expiry);
        }

        public async Task<string> GetAsync(string key)
        {
            return await db.StringGetAsync(key);
        }
    }
}