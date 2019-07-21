using System;
using System.Collections.Generic;
using System.Security;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace ChangeFeedFunction
{
    public static class Function1
    {

        private static readonly Lazy<DocumentClient> _cosmosClient = new Lazy<DocumentClient>(InitializeCosmosClient);
        private static readonly DocumentClient _client = _cosmosClient.Value;
        private static long _updatedcount = 0;
        //private static CosmosDatabase _cosmosDatabase => _cosmosClient.Value.Databases["ToDoList"];

        private static DocumentClient InitializeCosmosClient()
        {
            var connectionString =
                Environment.GetEnvironmentVariable("CosmosDBTriggerAttribute.ConnectionStringSetting");
            var endpointUrl = Environment.GetEnvironmentVariable("CosmosEndPoint");
            var authorizationKey = ToSecureString((Environment.GetEnvironmentVariable("AuthKey")));

            var cosmosClient = new DocumentClient(new Uri(endpointUrl), authorizationKey,
                    new ConnectionPolicy
                    {
                        ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Direct,
                        ConnectionProtocol = Protocol.Tcp,
                        RetryOptions = new RetryOptions { MaxRetryAttemptsOnThrottledRequests = 10, MaxRetryWaitTimeInSeconds = 30 }
                    });
            return cosmosClient;
        }

        public static SecureString ToSecureString(this string _self)
        {
            SecureString knox = new SecureString();
            char[] chars = _self.ToCharArray();
            foreach (char c in chars)
            {
                knox.AppendChar(c);
            }
            return knox;
        }

        //Microsoft.Azure.DocumentDB 2.5.1
        //Microsoft.Azure.WebJobs 3.0.10
        //Microsoft.Azure.WebJobs.Extensions.CosmosDB 3.0.3
        //Microsoft.NET.Sdk.Functions 1.0.29
        //.NET Core 2.1
        [FunctionName("Function1")]
        public static void Run([CosmosDBTrigger(
            databaseName: "samples",
            collectionName: "changefeed-samples",
            ConnectionStringSetting = "CosmosDBTriggerAttribute.ConnectionStringSetting",
            LeaseCollectionName = "leases",CreateLeaseCollectionIfNotExists =true, StartFromBeginning =true)]IReadOnlyList<Document> documents, ILogger log)
        {
            if (documents != null)
            {
                log.LogInformation($"{nameof(Function1)} received {documents.Count} document(s).");
                var targetDBId = Environment.GetEnvironmentVariable("TargetDBName");
                var targetCollectionId = (Environment.GetEnvironmentVariable("TargetCollectionName"));
                Uri collectionUri = UriFactory.CreateDocumentCollectionUri(targetDBId, targetCollectionId);
                Parallel.ForEach(documents, new ParallelOptions { MaxDegreeOfParallelism = 30 },
                    document => {
                        _client.UpsertDocumentAsync(collectionUri, document).Wait();
                        _updatedcount++;
                        log.LogInformation("Updated Document Count:" + _updatedcount);
                    });

            }
            else
            {
                log.LogInformation($"{nameof(Function1)} received 0 document(s).");
            }
        }
    }
}
