using Microsoft.Azure.Cosmos;
using static Microsoft.Azure.Cosmos.Container;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.NewtonsoftJson;
using Newtonsoft.Json;
using CloudNative.CloudEvents.Http;
//using CloudNative.CloudEvents.SystemTextJson;

string endpoint = System.Environment.GetEnvironmentVariable("COSMOS_ENDPOINT");
string key = System.Environment.GetEnvironmentVariable("COSMOS_KEY");
string databaseName = "booze";
string containerName = "pints";
string containerLeaseName = "pints_lease";

CosmosClient client = new CosmosClient(endpoint, key);
Database database = await client.CreateDatabaseIfNotExistsAsync(databaseName);
Container sourceContainer = await database.CreateContainerIfNotExistsAsync(containerName, "/id");
Container leaseContainer = await database.CreateContainerIfNotExistsAsync(containerLeaseName, "/id");

ChangesHandler<Pint> handleChanges = async (
    IReadOnlyCollection<Pint> changes,
    CancellationToken cancellationToken
) =>
{
    Console.WriteLine($"START\tHandling batch of changes...");
    foreach (Pint pint in changes)
    {
        await Console.Out.WriteLineAsync($"Detected Operation:\t[{pint.id}]\t{pint.name}\t{pint.state}");

        var cloudEvent = new CloudEvent
        {
            Type = "hungovercoders.booze.cdc.pint",
            Id = Guid.NewGuid().ToString(),
            Time = DateTime.UtcNow,
            Source = new Uri("/events/pint-drunk", UriKind.RelativeOrAbsolute),
            DataContentType = "application/json",
            Data = pint
        };

        // var settings = new JsonSerializerSettings
        // {
        //     NullValueHandling = NullValueHandling.Ignore,
        //     Formatting = Formatting.Indented,
        //     ContractResolver = new ExcludeSpecVersionContractResolver()
        // };

        // var json = JsonConvert.SerializeObject(cloudEvent, settings);
        // await Console.Out.WriteLineAsync(json);
         // Create a JsonEventFormatter
        var content = cloudEvent.ToHttpContent(ContentMode.Structured, new JsonEventFormatter());
        // var json = cloudEvent.ToJsonContent(ContentMode.Structured, new JsonEventFormatter());
        // Console.WriteLine(json);

        var httpClient = new HttpClient();
        // Your application remains in charge of adding any further headers or 
        // other information required to authenticate/authorize or otherwise
        // dispatch the call at the server.
        var Url = "https://prod-08.northeurope.logic.azure.com:443/workflows/84f4d2800ebd49a3bbd5d9ca65988857/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=8PViliUNxGAx5Ec8DObZsUGOm4NFIwfTzv-vOwJD3IQ";
        var result = await httpClient.PostAsync(Url, content);

        Console.WriteLine(result.StatusCode);

    }
};

var builder = sourceContainer.GetChangeFeedProcessorBuilder<Pint>(
    processorName: "pintsProcessor",
    onChangesDelegate: handleChanges
);

ChangeFeedProcessor processor = builder
    .WithInstanceName("consoleApp")
    .WithLeaseContainer(leaseContainer)
    .Build();

await processor.StartAsync();

Console.WriteLine($"RUN\tListening for changes...");
Console.WriteLine("Press any key to stop");
Console.ReadKey();

await processor.StopAsync();

class ExcludeSpecVersionContractResolver : Newtonsoft.Json.Serialization.DefaultContractResolver
{
    protected override Newtonsoft.Json.Serialization.JsonProperty CreateProperty(
        System.Reflection.MemberInfo member,
        Newtonsoft.Json.MemberSerialization memberSerialization)
    {
        var property = base.CreateProperty(member, memberSerialization);
        if (property.PropertyName.Equals("specversion", StringComparison.OrdinalIgnoreCase))
        {
            property.ShouldSerialize = instance => false;
        }
        if (property.PropertyName.Equals("ExtensionAttributes", StringComparison.OrdinalIgnoreCase))
        {
            property.ShouldSerialize = instance => false;
        }
        if (property.PropertyName.Equals("IsValid", StringComparison.OrdinalIgnoreCase))
        {
            property.ShouldSerialize = instance => false;
        }
        return property;
    }
}
