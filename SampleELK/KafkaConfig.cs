using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SampleELK
{
    /// <summary>
    /// Configuration class for Kafka settings.
    /// </summary>
    public class KafkaConfig
    {
        /// <summary>
        /// Gets or sets the Kafka bootstrap servers.
        /// </summary>
        public string BootstrapServers { get; set; } = "localhost:9092";

        /// <summary>
        /// Gets or sets the Kafka topic to which logs will be sent.
        /// </summary>
        public string Topic { get; set; } = "app-logs";

        /// <summary>
        /// Gets or sets the client identifier for the Kafka producer.
        /// </summary>
        public string ClientId { get; set; } = "ElasticLogEvent";
    }
}
