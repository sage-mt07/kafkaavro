using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Producer.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Producer;

public class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("Avro形式のKafkaプロデューサーを開始します...");

        // Kafkaブローカーの設定
        var bootstrapServers = "localhost:9092";
        var schemaRegistryUrl = "http://localhost:8081";
        var sourceTopic = "avro-transactions-topic";

        // スキーマレジストリの設定
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = schemaRegistryUrl
        };

        // プロデューサー設定
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All
        };

        // スキーマレジストリクライアントの作成
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        // Avroシリアライザーの作成
        var keySerializer = new AvroSerializer<MessageKey>(schemaRegistry);
        var valueSerializer = new AvroSerializer<MessageValue>(schemaRegistry);

        // プロデューサーの作成
        using var producer = new ProducerBuilder<MessageKey, MessageValue>(producerConfig)
            .SetKeySerializer(keySerializer)
            .SetValueSerializer(valueSerializer)
            .Build();

        try
        {
            // テストデータの生成とプロデュース
            for (int i = 1; i <= 10; i++)
            {
                var transactionId = Guid.NewGuid().ToString();

                // キーの作成
                var key = new MessageKey
                {
                    PrimaryId = $"CUST-{i:D3}",
                    SecondaryId = $"ORD-{i:D5}",
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                // バリューの作成
                var value = new MessageValue
                {
                    TransactionId = transactionId,
                    ProductCode = $"PROD-{(i % 5) + 1:D3}",
                    Amount = decimal.Parse($"{i * 125}.{i:D2}"),
                    TransactionDate = DateTimeOffset.Now,
                    UnitPrice = decimal.Parse($"{(i % 5) * 10 + 5}.{i:D2}"),
                    Quantity = i * 2,
                    Metadata = new Dictionary<string, string>
                        {
                            { "channel", i % 2 == 0 ? "online" : "store" },
                            { "region", i % 3 == 0 ? "east" : (i % 3 == 1 ? "west" : "central") },
                            { "paymentMethod", i % 4 == 0 ? "credit" : (i % 4 == 1 ? "debit" : (i % 4 == 2 ? "cash" : "app")) }
                        }
                };

                // メッセージの送信
                var deliveryResult = await producer.ProduceAsync(sourceTopic, new Message<MessageKey, MessageValue> { Key = key, Value = value });

                Console.WriteLine($"メッセージを送信しました - Topic: {deliveryResult.Topic}, Partition: {deliveryResult.Partition}, Offset: {deliveryResult.Offset}");
                Console.WriteLine($"Key: PrimaryId={key.PrimaryId}, SecondaryId={key.SecondaryId}");
                Console.WriteLine($"Value: TransactionId={value.TransactionId}, Amount={value.Amount:N2}, Date={value.TransactionDate}");

                // 間隔をあける
                await Task.Delay(1000);
            }
        }
        catch (ProduceException<MessageKey, MessageValue> e)
        {
            Console.WriteLine($"配信エラー: {e.Error.Reason}");
        }

        Console.WriteLine("データの送信が完了しました。Enterキーを押して終了します...");
        Console.ReadLine();
    }
}
