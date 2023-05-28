using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using CloudNative.CloudEvents;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;

namespace PintDrinker
{
    class Program
    {
        static async Task Main(string[] args)
        {


        var builder = new ConfigurationBuilder();
        builder.SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
        
        IConfiguration config = builder.Build();

        
        string connectionString = config.GetSection("ConnectionString").GetSection("namespace").Value;
        string eventHubName = config["ConnectionString:eventHub"];

            var enqueueOptions = new EnqueueEventOptions
                {
                    PartitionKey = "PintId"
                    
                };

            // Create an EventHubProducerClient
            await using var producer = new EventHubBufferedProducerClient(connectionString, eventHubName);

            producer.SendEventBatchFailedAsync += args =>
                {
                    Console.WriteLine($"Publishing failed for { args.EventBatch.Count } events.  Error: '{ args.Exception.Message }'");
                    return Task.CompletedTask;
                };


            // Cloud Event data
            var evt = new CloudEvent
            {
                Type = "hungovercoders.booze.cdc.pint",
                Id = Guid.NewGuid().ToString(),
                Time = DateTime.UtcNow,
                Source = new Uri("/events/pint-poured", UriKind.RelativeOrAbsolute),
                DataContentType = "application/json",
                Data = new
                {
                    PintId = "1",
                    State = "drunk"
                }
            };

            // Serialize the Cloud Event data to JSON
            var json = JsonConvert.SerializeObject(evt);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            var eventData = new EventData(Encoding.UTF8.GetBytes(json));

            await producer.EnqueueEventAsync(eventData, enqueueOptions);

            Console.WriteLine("Cloud Event sent successfully.");
            Console.WriteLine(json);
        

            // Close the producer client
            await producer.CloseAsync();
        }
    }
}
