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
        private static readonly string _bootstrapServers = "localhost:29092,localhost:39092";

        static void Main(string[] args)
        {
            Task.Run(() =>
            {
                KafkaProducer();
            });

            //Task.Run(() =>
            //{
            //    KafkaConsumer();
            //});

            Console.ReadLine();
        }

        private static void KafkaConsumer()
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
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
                BootstrapServers = _bootstrapServers,
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
                            Console.WriteLine($"Faulted: Wrote to offset: {task.Result.Offset} for message index {index}");
                        }
                        else
                        {
                            Console.WriteLine($"Success: Wrote to offset: {task.Result.Offset} for message index {index}");
                        }
                    });
                    await Task.Delay(1000);
                    index++;
                }

            }
        }
    }
}
