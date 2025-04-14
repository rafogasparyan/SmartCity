using Newtonsoft.Json;
using System;
namespace RouteOptimizationService.Models
{
    public class GpsData
    {
        [JsonProperty("id")] public Guid Id { get; set; } = Guid.NewGuid(); // Unique identifier

        [JsonProperty("deviceID")] public string DeviceId { get; set; }

        [JsonProperty("timestamp")] public DateTime Timestamp { get; set; }

        [JsonProperty("speed")] public double Speed { get; set; } // Speed in km/h

        [JsonProperty("direction")] public string Direction { get; set; } // Example: "North-East"

        [JsonProperty("vehicle_type")] public string VehicleType { get; set; } = "private"; // Default value

        [JsonProperty("location")] public double[] Location { get; set; } // [latitude, longitude]
    }
}