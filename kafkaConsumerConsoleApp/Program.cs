using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace kafkaConsumerConsoleApp
{
    class Program
    {
        private static readonly CancellationTokenSource cancellatiokenSource = new();
        private static readonly string _bootstrapServers = "localhost:9092,localhost:9093,localhost:9094";

        static void Main(string[] args)
        {
            KafkaConsumer(new List<string>() { "my-topic-1", "my-topic-2", "my-topic-3", "my-topic-4" });

            Console.ReadLine();
        }

        private static async Task KafkaConsumer(IEnumerable<string> topicNames)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = "foo",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
            {
                consumer.Subscribe(topicNames);

                while (!cancellatiokenSource.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(cancellatiokenSource.Token);
                    Console.WriteLine($" {consumeResult.Topic} >> [{consumeResult.Message.Key}:{consumeResult.Message.Value}]");
                }
                consumer.Close();
            }
        }
    }
}
