using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace kafkaConsumerConsoleApp
{
    class Program
    {
        private static readonly CancellationTokenSource cancellatiokenSource = new();
        private static readonly string _bootstrapServers = "localhost:29092,localhost:39092";

        static void Main(string[] args)
        {
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
    }
}
