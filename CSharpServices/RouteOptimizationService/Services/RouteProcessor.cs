using Newtonsoft.Json;
using Polly;
using RouteOptimizationService.Models;
using StackExchange.Redis;
using Confluent.Kafka;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Net.Http;


namespace RouteOptimizationService.Services
{
    public class RouteProcessor
    {
        private readonly IDatabase redisDb;
        private readonly IProducer<string, string> producer;
        private readonly GoogleRoadsService roadsService = new GoogleRoadsService();
        private readonly List<(double lat, double lng)> coordinateBuffer = new();
        private readonly AsyncRetryPolicy retryPolicy = Policy
            .Handle<HttpRequestException>()
            .WaitAndRetryAsync(3, attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)));

        public RouteProcessor(IProducer<string, string> producer, IDatabase redisDb)
        {
            this.producer = producer;
            this.redisDb = redisDb;
        }

        public async Task HandleGpsDataAsync(GpsData gpsData)
        {
            if (gpsData?.Location == null || gpsData.Location.Length != 2) return;

            coordinateBuffer.Add((gpsData.Location[0], gpsData.Location[1]));

            if (coordinateBuffer.Count >= 3)
            {
                var snappedPoints = await retryPolicy.ExecuteAsync(() =>
                    roadsService.SnapToRoadsAsync(coordinateBuffer.ToArray()));

                coordinateBuffer.Clear();

                if (snappedPoints?.Length > 0)
                {
                    var key = $"route:{gpsData.Location[0]}:{gpsData.Location[1]}";
                    await redisDb.StringSetAsync(key, JsonConvert.SerializeObject(snappedPoints[0]), TimeSpan.FromHours(1));
                    await ProduceResult(gpsData, snappedPoints[0]);
                }
            }
        }

        private async Task ProduceResult(GpsData data, SnappedPoint point)
        {
            var message = new
            {
                data.DeviceId,
                point.SpeedLimit,
                Timestamp = DateTime.UtcNow
            };

            await producer.ProduceAsync("optimized_routes", new Message<string, string>
            {
                Key = data.DeviceId,
                Value = JsonConvert.SerializeObject(message)
            });
        }
    }
}