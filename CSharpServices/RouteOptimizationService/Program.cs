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

