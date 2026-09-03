using Confluent.Kafka;
using SampleELK;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

/// <summary>
/// A .NET application that logs structured events to a Kafka topic.
/// </summary>
class Program
{
    /// <summary>
    /// The main entry point for the application.
    /// </summary>
    /// <param name="args">Command-line arguments.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static async Task Main(string[] args)
    {
        using IHost host = Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((context, config) =>
            {
                config.SetBasePath(Directory.GetCurrentDirectory())
                      .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                      .AddJsonFile($"appsettings.{context.HostingEnvironment.EnvironmentName}.json", optional: true, reloadOnChange: true)
                      .AddEnvironmentVariables();
            })
            .ConfigureLogging((context, logging) =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.AddConfiguration(context.Configuration.GetSection("Logging"));
            })
            .ConfigureServices((context, services) =>
            {
                // Configure Kafka producer with settings from appsettings
                var kafkaConfig = context.Configuration.GetSection("Kafka").Get<KafkaConfig>();
                services.Configure<KafkaConfig>(context.Configuration.GetSection("Kafka"));
                
                services.AddSingleton<IProducer<Null, string>>(provider =>
                {
                    var config = new ProducerConfig
                    {
                        BootstrapServers = kafkaConfig?.BootstrapServers ?? "localhost:9092",
                        ClientId = kafkaConfig?.ClientId ?? "ElasticLogEvent"
                    };
                    
                    return new ProducerBuilder<Null, string>(config).Build();
                });

                // Register the Worker service
                services.AddHostedService<Worker>();
            })
            .Build();

        // Run the host
        await host.RunAsync();
    }
}