using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace CosmosDBCrudExample
{

    class Program
    {
        private static string endpoint = Environment.GetEnvironmentVariable("COSMOS_ENDPOINT");
        private static string key = Environment.GetEnvironmentVariable("COSMOS_KEY");
        private static string databaseName = "booze";
        private static string containerName = "pints";

        public static async Task StockPints(string name, string brewery, string pub, double price, int quantity)
        {
            CosmosClientOptions options = new CosmosClientOptions
            {
                AllowBulkExecution = true
            };

            using (CosmosClient client = new CosmosClient(endpoint, key, options))
            {
                Database database = await client.CreateDatabaseIfNotExistsAsync(databaseName);
                Container container = await database.CreateContainerIfNotExistsAsync(containerName, "/pub");

                List<Pint> pintsToStock = new();

                for (int i = 0; i < quantity; i += 1)
                {
                    Pint pint = new Pint
                    {
                        id = Guid.NewGuid().ToString(),
                        name = name,
                        state = "Stocked",
                        brewery = brewery,
                        pub = pub,
                        price = price
                    };
                    pintsToStock.Add(pint);
                }

                List<Task> concurrentTasks = new List<Task>();

                Console.WriteLine($"Stocking pints for pub {pub} from brewery {brewery}...");
                foreach (Pint pintStock in pintsToStock)
                {
                    concurrentTasks.Add(container.CreateItemAsync(pintStock, new PartitionKey(pintStock.pub)));
                }

              
                await Task.WhenAll(concurrentTasks);
                Console.WriteLine("Pints stocked.");
            }
        }

        public static async Task PourPint(string pub, string name, int quantity)
        {
            using (CosmosClient client = new CosmosClient(endpoint, key))
            {
                Container container = client.GetContainer(databaseName, containerName);

                var queryText = $"SELECT TOP {quantity} * FROM c WHERE c.pub = '{pub}' AND c.name = '{name}' and c.state = 'Stocked' ORDER BY c._ts ASC";
                QueryDefinition queryDefinition = new QueryDefinition(queryText);

                FeedIterator<dynamic> queryResultSetIterator = container.GetItemQueryIterator<dynamic>(
                    queryDefinition,
                    requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(pub) }
                );

                List<Task> pourTasks = new List<Task>();

                Console.WriteLine($"Pouring {quantity} pint(s) of {name} in {pub}...");
                while (queryResultSetIterator.HasMoreResults && pourTasks.Count < quantity)
                {
                    FeedResponse<dynamic> feedResponse = await queryResultSetIterator.ReadNextAsync();
                    foreach (var pint in feedResponse)
                    {
                        pint.state = "Poured";
                        pourTasks.Add(container.UpsertItemAsync(pint, new PartitionKey(pint.pub.ToString())));
                    }
                }

                await Task.WhenAll(pourTasks);
                Console.WriteLine($"Poured {pourTasks.Count} pint(s) of {name} in {pub}.");
            }
        }

        static async Task Main(string[] args)
        {
            await StockPints("Stay Puft", "Tiny Rebel", "Head of Steam", 5.00, 100);
            await StockPints("Mike Rayer", "Crafty Devil", "Head of Steam", 5.00, 100);
            await StockPints("Yawn", "Flowerhorn", "Head of Steam", 5.00, 100);

            await StockPints("Stay Puft", "Tiny Rebel", "Brew Monster", 5.00, 100);
            await StockPints("Mike Rayer", "Crafty Devil", "Brew Monster", 5.00, 100);
            await StockPints("Yawn", "Flowerhorn", "Brew Monster", 5.00, 100);

            await PourPint("Head of Steam", "Stay Puft", 1);
        }
    }
}
