using Confluent.Kafka;
using Newtonsoft.Json;
using RouteOptimizationService.Models;
using StackExchange.Redis;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace RouteOptimizationService.Services
{
    public class KafkaService
    {
        private readonly ConsumerConfig consumerConfig;
        private readonly IConsumer<Ignore, string> consumer;
        private readonly IProducer<string, string> producer;
        private readonly IDatabase redisDb;
        private readonly RouteProcessor processor;

        public KafkaService()
        {
            var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS");
            var redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTION") + ",abortConnect=false";
            var redis = ConnectionMultiplexer.Connect(redisConnectionString);
            redisDb = redis.GetDatabase();

            consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "route-optimizer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();

            processor = new RouteProcessor(producer, redisDb);
        }

        public async Task StartAsync()
        {
            consumer.Subscribe("gps_data");
            while (true)
            {
                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                if (consumeResult == null) continue;

                try
                {
                    var gpsData = JsonConvert.DeserializeObject<GpsData>(consumeResult.Message.Value);
                    await processor.HandleGpsDataAsync(gpsData);
                    consumer.Commit(consumeResult);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }
            }
        }
    }
}
