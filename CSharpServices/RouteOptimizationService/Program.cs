// using System;
// using System.Collections.Generic;
// using System.Linq;
// using System.Net.Http;
// using System.Text.Json;
// using System.Threading.Tasks;
// using Confluent.Kafka;
// using StackExchange.Redis;
// using Newtonsoft.Json;
// using Polly;
//
// using SystemTextJson = System.Text.Json.JsonSerializer;
//
// namespace RouteOptimizationService
// {
//     class Program
//     {
//         // Kafka, Redis, and HttpClient.
//         static ConsumerConfig consumerConfig;
//         static IConsumer<Ignore, string> consumer;
//         static IProducer<string, string> producer;
//         static IDatabase redisDb;
//         static HttpClient httpClient = new HttpClient();
//         static string googleApiKey;
//
//         // Retry policy for the Google API.
//         static Polly.Retry.AsyncRetryPolicy retryPolicy = Policy
//             .Handle<HttpRequestException>()
//             .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));
//
//         // Buffer to accumulate coordinates.
//         static List<(double lat, double lng)> coordinateBuffer = new List<(double, double)>();
//
//         static async Task Main(string[] args)
//         {
//             googleApiKey = Environment.GetEnvironmentVariable("GOOGLE_API_KEY");
//             if (string.IsNullOrEmpty(googleApiKey))
//             {
//                 Console.WriteLine("❌ Google API Key is missing.");
//                 return;
//             }
//             
//             Console.WriteLine(Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS"));
//             // Kafka consumer.
//             consumerConfig = new ConsumerConfig
//             {
//                 BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS"),
//                 GroupId = "route-optimizer-group",
//                 AutoOffsetReset = AutoOffsetReset.Earliest
//             };
//
//             // Initialize Redis.
//             var redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTION") + ",abortConnect=false";
//             var redis = ConnectionMultiplexer.Connect(redisConnectionString);
//             redisDb = redis.GetDatabase();
//
//             // Initialize Kafka consumer and producer.
//             Console.WriteLine("building consumer");
//             consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
//             Console.WriteLine("building builder");
//             producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = consumerConfig.BootstrapServers }).Build();
//             Console.WriteLine("subscribing");
//             consumer.Subscribe("gps_data");
//
//             // self-test mode.
//             if (Environment.GetEnvironmentVariable("RUN_SAMPLE_TEST") == "true")
//             {
//                 Console.WriteLine("🚀 Running self-test...");
//                 Console.WriteLine($"GOOGLE_API_KEY: {googleApiKey}");
//                 await TestSnapToRoads(googleApiKey);
//                 Environment.Exit(0);
//             }
//             
//     while (true) 
//     {   
//     try
//     {
//         Console.WriteLine("---------------------------------------------");
//         var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
//         
//         if (consumeResult == null) continue;
//         
//         Console.WriteLine($"Received message: {consumeResult.Message.Value}");
//
//         try
//         {
//             var gpsData = JsonConvert.DeserializeObject<GpsData>(consumeResult.Message.Value);
//
//             if (gpsData?.Location == null || gpsData.Location.Length != 2)
//             {
//                 Console.WriteLine("Invalid location data - skipping");
//                 continue;
//             }
//
//             Console.WriteLine($"Received location: {gpsData.Location[0]}, {gpsData.Location[1]}");
//             
//             coordinateBuffer.Add((gpsData.Location[0], gpsData.Location[1]));
//             Console.WriteLine($"Buffer size: {coordinateBuffer.Count}");
//
//             if (coordinateBuffer.Count >= 3)
//             {
//                 var snappedPoints = await retryPolicy.ExecuteAsync(() => 
//                     SnapToRoads(googleApiKey, coordinateBuffer.ToArray()));
//                 
//                 coordinateBuffer.Clear();
//
//                 if (snappedPoints?.Length > 0)
//                 {
//                     var cacheKey = $"route:{gpsData.Location[0]}:{gpsData.Location[1]}";
//                     await redisDb.StringSetAsync(
//                         cacheKey, 
//                         JsonConvert.SerializeObject(snappedPoints[0]), 
//                         TimeSpan.FromHours(1)
//                     );
//                     
//                     await ProcessSnappedPoint(gpsData, snappedPoints[0]);
//                 }
//             }
//             
//             // consumer.StoreOffset(consumeResult);
//             consumer.Commit(consumeResult);
//         }
//         catch (System.Text.Json.JsonException ex)
//         {
//             Console.WriteLine($"JSON Parsing Error: {ex.Message}");
//         }
//     }
//     catch (ConsumeException e)
//     {
//         Console.WriteLine($"Kafka Error: {e.Error.Reason}");
//     }
//     catch (Exception e)
//     {
//         Console.WriteLine($"Critical Error: {e}");
//     }
// }
//         }
//
//         // Self-test method to call the API with a set of sample coordinates.
//         static async Task TestSnapToRoads(string apiKey)
//         {
//             try
//             {
//                 Console.WriteLine("Testing multiple coordinates...");
//                 var snappedPoints = await SnapToRoads(apiKey,
//                     (-35.27801, 149.12958),
//                     (-35.28032, 149.12907),
//                     (-35.28099, 149.12929),
//                     (-35.28144, 149.12984),
//                     (-35.28194, 149.13003),
//                     (-35.28282, 149.12956),
//                     (-35.28302, 149.12881),
//                     (-35.28473, 149.12836)
//                 );
//
//                 if (snappedPoints != null && snappedPoints.Length > 0)
//                 {
//                     foreach (var point in snappedPoints)
//                     {
//                         Console.WriteLine($"✅ Snapped: Latitude: {point.Location.Latitude}, Longitude: {point.Location.Longitude}, PlaceId: {point.PlaceId}");
//                     }
//                 }
//                 else
//                 {
//                     Console.WriteLine("❌ Null response");
//                 }
//             }
//             catch (Exception ex)
//             {
//                 Console.WriteLine($"❌ Error: {ex.Message}");
//             }
//         }
//
//         // Method to call Google Roads API's SnapToRoads endpoint with multiple coordinates.
//         static async Task<SnappedPoint[]> SnapToRoads(string apiKey, params (double lat, double lng)[] coordinates)
//         {
//             // Construct the path by joining coordinates with "%7C" (URL-encoded '|').
//             var path = string.Join("%7C", coordinates.Select(c => $"{c.lat},{c.lng}"));
//             var url = $"https://roads.googleapis.com/v1/snapToRoads?interpolate=true&path={path}&key={apiKey}";
//             var response = await httpClient.GetAsync(url);
//
//             if (!response.IsSuccessStatusCode)
//             {
//                 throw new HttpRequestException($"Failed to call Google Roads API: {response.StatusCode}");
//             }
//
//             var jsonResponse = await response.Content.ReadAsStringAsync();
//             Console.WriteLine("🔎 Raw JSON Response:");
//             Console.WriteLine(jsonResponse);
//
//             // Deserialize the JSON response with case-insensitive options.
//             var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
//             var snappedRoadResponse = SystemTextJson.Deserialize<SnapToRoadsResponse>(jsonResponse, options);
//
//             return snappedRoadResponse?.SnappedPoints;
//         }
//
//         // Method to process a snapped point (e.g., produce an optimized route message to Kafka).
//         static async Task ProcessSnappedPoint(GpsData originalData, SnappedPoint snappedPoint)
//         {
//             var optimizedRoute = new
//             {
//                 originalData.DeviceId,
//                 // OriginalLocation = new { originalData.Latitude, originalData.Longitude },
//                 // SnappedLocation = new { snappedPoint.Location.Latitude, snappedPoint.Lozcation.Longitude },
//                 snappedPoint.SpeedLimit,
//                 Timestamp = DateTime.UtcNow
//             };
//
//             await producer.ProduceAsync("optimized_routes", new Message<string, string>
//             {
//                 Key = originalData.DeviceId,
//                 Value = JsonConvert.SerializeObject(optimizedRoute)
//             });
//             
//             Console.WriteLine($"Processed: {originalData.DeviceId}");
//         }
//     }
// }

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using RouteOptimizationService.Services;

namespace RouteOptimizationService
{
    class Program
    {
        static async Task Main(string[] args)
        {
            await new KafkaService().StartAsync();
        }
    }
}

