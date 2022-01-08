using Confluent.Kafka;
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace consoleapp
{
    class Program
    {
        private static readonly CancellationTokenSource cancellatiokenSource = new();
        private static readonly string _bootstrapServers = "localhost:9092,localhost:9093,localhost:9094";

        static void Main(string[] args)
        {
            Task.WhenAll(KafkaProducer("my-topic-1"),
                         KafkaProducer("my-topic-2"),
                         KafkaProducer("my-topic-3"),
                         KafkaProducer("my-topic-4"));

            Console.ReadLine();
        }

        private static async Task KafkaProducer(string topicName)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                ClientId = Dns.GetHostName(),
            };

            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                int index = 1;
                while (!cancellatiokenSource.IsCancellationRequested)
                {
                    var t = producer.ProduceAsync(topicName, new Message<Null, string> { Value = $"hello world {index}" });
                    t.ContinueWith(task =>
                    {
                        if (task.IsFaulted)
                        {
                            Console.WriteLine($"Faulted: Wrote to offset: {task.Result.Offset} {topicName} >> {index}");
                        }
                        else
                        {
                            Console.WriteLine($"Success: Wrote to offset: {task.Result.Offset} {topicName} >> {index}");
                        }
                    });
                    await Task.Delay(5000);
                    index++;
                }

            }
        }
    }
}
