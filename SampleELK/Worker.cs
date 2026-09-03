using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace SampleELK
{
    /// <summary>
    /// A background service that generates and sends log events to Kafka.
    /// </summary>
    public class Worker : BackgroundService
    {
        /// <summary>
        /// Logger instance for logging information.
        /// </summary>
        private readonly ILogger<Worker> _logger;

        /// <summary>
        /// Kafka producer for sending messages.
        /// </summary>
        private readonly IProducer<Null, string> _producer;

        /// <summary>
        /// Kafka configuration.
        /// </summary>
        private readonly KafkaConfig _kafkaConfig;

        /// <summary>
        /// Initializes a new instance of the <see cref="Worker"/> class.
        /// </summary>
        /// <param name="logger">The logger instance.</param>
        /// <param name="producer">The Kafka producer instance.</param>
        /// <param name="configuration">The configuration instance.</param>
        public Worker(ILogger<Worker> logger, IProducer<Null, string> producer, IConfiguration configuration)
        {
            _logger = logger;
            _producer = producer;
            _kafkaConfig = configuration.GetSection("Kafka").Get<KafkaConfig>() ?? new KafkaConfig();
            _logger.LogInformation("Worker initialized with Kafka bootstrap servers: {BootstrapServers}", _kafkaConfig.BootstrapServers);
        }

        /// <summary>
        /// Executes the background service.
        /// </summary>
        /// <param name="stoppingToken">A token to monitor for cancellation requests.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            int counter = 0;
            _logger.LogInformation("Worker service started. Sending logs to Kafka topic: {Topic}", _kafkaConfig.Topic);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var logEvent = new
                    {
                        EventType = "JobExecution",
                        Service = "KafkaLoggingApp",
                        MessageNumber = ++counter,
                        DurationSeconds = Math.Round(Random.Shared.NextDouble() * 10, 2),
                        Timestamp = DateTime.UtcNow
                    };

                    var json = JsonSerializer.Serialize(logEvent);

                    // Send to Kafka using topic from configuration
                    var deliveryResult = await _producer.ProduceAsync(
                        _kafkaConfig.Topic,
                        new Message<Null, string> { Value = json },
                        stoppingToken
                    );

                    _logger.LogInformation("Published log #{Counter} to Kafka topic {Topic} partition {Partition} offset {Offset}",
                        counter, _kafkaConfig.Topic, deliveryResult.Partition, deliveryResult.Offset);

                    await Task.Delay(5000, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    // Normal cancellation, exit gracefully
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred while processing log event #{Counter}", counter);

                    // Wait a bit before retrying
                    await Task.Delay(1000, stoppingToken);
                }
            }

            _logger.LogInformation("Worker service stopped.");
        }

        /// <summary>
        /// Disposes the worker service.
        /// </summary>
        public override void Dispose()
        {
            _producer?.Dispose();
            base.Dispose();
        }
    }

}
