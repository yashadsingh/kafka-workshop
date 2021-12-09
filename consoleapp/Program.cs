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

        static void Main(string[] args)
        {
            Task.Run(() =>
            {
                KafkaProducer();
            });

            Task.Run(() =>
            {
                KafkaConsumer();
            });

            Console.ReadLine();
        }

        private static void KafkaConsumer()
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "foo",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
            {
                consumer.Subscribe("my-topic");

                while (!cancellatiokenSource.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(cancellatiokenSource.Token);
                    Console.WriteLine($" Consumed message [{consumeResult.Message.Key}:{consumeResult.Message.Value}]");
                }
                consumer.Close();
            }
        }

        private static async Task KafkaProducer()
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = Dns.GetHostName(),
            };

            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                int index = 1;
                while (!cancellatiokenSource.IsCancellationRequested)
                {
                    var t = producer.ProduceAsync("my-topic", new Message<Null, string> { Value = $"hello world {index}" });
                    t.ContinueWith(task =>
                    {
                        if (task.IsFaulted)
                        {
                            Console.WriteLine($"Faulted: Wrote to offset: {task.Result.Offset}");
                        }
                        else
                        {
                            Console.WriteLine($"Success: Wrote to offset: {task.Result.Offset}");
                        }
                    });
                    await Task.Delay(1000);
                    index++;
                }

            }
        }
    }
}
