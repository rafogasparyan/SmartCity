using System;
using Polly;
using Polly.Retry;
using System.Net.Http;

namespace RouteOptimizationService.Utils
{
    public static class RetryPolicyFactory
    {
        public static AsyncRetryPolicy Create()
        {
            return Policy
                .Handle<HttpRequestException>()
                .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));
        }
    }
}