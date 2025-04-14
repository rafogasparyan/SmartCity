using RouteOptimizationService.Models;
using System.Net.Http;
using System.Text.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace RouteOptimizationService.Services
{
    public class GoogleRoadsService
    {
        private readonly HttpClient httpClient = new();
        private readonly string apiKey = Environment.GetEnvironmentVariable("GOOGLE_API_KEY");

        public async Task<SnappedPoint[]> SnapToRoadsAsync((double lat, double lng)[] coords)
        {
            var path = string.Join("%7C", coords.Select(c => $"{c.lat},{c.lng}"));
            var url = $"https://roads.googleapis.com/v1/snapToRoads?interpolate=true&path={path}&key={apiKey}";

            var response = await httpClient.GetAsync(url);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            return JsonSerializer.Deserialize<SnapToRoadsResponse>(json, options)?.SnappedPoints;
        }
    }
}