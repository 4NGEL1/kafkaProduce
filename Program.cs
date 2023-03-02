using System;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace OssProducerWithKafkaApi
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string topicName = "TransactionAssnOrRnnn";

            using var adminClient = new AdminClientBuilder(
                new AdminClientConfig
                {
                    BootstrapServers = "r226nvs35y6q.streaming.us-ashburn-1.oci.oraclecloud.com:9092",
                    BrokerVersionFallback = "1.0.0",
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.Plain,
                    SaslUsername = "toksopc/loyalty_streaming_user/ocid1.streampool.oc1.iad.amaaaaaafcruwjya4jdiykjqjog53t2gmouajdwvinlf2xf7r226nvs35y6q",
                    SaslPassword = ">[JMQ6vg;iLzC1TeLM+-",
                }).Build();

            try
            {
                var of = new Offset().Value;

                Console.WriteLine($"{of}");
                Console.WriteLine($"Client Name {adminClient.Name}");

                var list = await adminClient.ListConsumerGroupsAsync();

                var p = new Partition();

                var a = new ConsumerGroupTopicPartitions[]
                {
                    new ConsumerGroupTopicPartitions("loyalty-microservice", new List<TopicPartition>{ new TopicPartition(topicName, p) })
                };

                var listTwo = await adminClient.ListConsumerGroupOffsetsAsync(a);

                foreach (var aTwo in listTwo)
                {
                    Console.WriteLine($"This is Capacity {aTwo.Partitions.Capacity}");
                    Console.WriteLine($"This is a Group {aTwo.Group}");
                    Console.WriteLine($"This is a Count {aTwo.Partitions.Count}");
                }

                foreach ( var group in list.Valid )
                {
                    Console.WriteLine($"Group List State {group.State}");
                    Console.WriteLine($"Group List Id {group.GroupId}");
                    Console.WriteLine($"Group List ConsumerGroups {group.IsSimpleConsumerGroup}");
                }

                /* await adminClient.DeleteTopicsAsync(new string[] { topicName });

                 Console.WriteLine("Topico eliminado.");*/

                /*await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                        new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = 1 } });*/

                /*var parti = new Partition();
                var topicP = new TopicPartition(topicName, parti);
                var offset = new Offset(500);
                var topicOffset = new List<TopicPartitionOffset>{
                    new TopicPartitionOffset(topicP, offset)
                };
                await adminClient.DeleteRecordsAsync(topicOffset);*/
            }
            catch (CreateTopicsException e)
            {
                var error = new Exception($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                Console.WriteLine($"Error to create topic: {error.Message}");
                throw error;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error to create topic: {e.Message}");
                throw;
            }

            var config = new ProducerConfig
            {
                BootstrapServers = "r226nvs35y6q.streaming.us-ashburn-1.oci.oraclecloud.com:9092", //usually of the form cell-1.streaming.[region code].oci.oraclecloud.com:9092
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "toksopc/loyalty_streaming_user/ocid1.streampool.oc1.iad.amaaaaaafcruwjya4jdiykjqjog53t2gmouajdwvinlf2xf7r226nvs35y6q",
                SaslPassword = ">[JMQ6vg;iLzC1TeLM+-", // use the auth-token you created step 5 of Prerequisites section
                AllowAutoCreateTopics = true,
            };

            // Produce(topicName, config); // use the name of the stream you created
        }

        static void Produce(string topic, ClientConfig config)
        {
            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                int numProduced = 0;
                int numMessages = 999;
                for (int i = 0; i < numMessages; ++i)
                {
                    var key = "messageKey" + i;
                    var val = "messageVal" + i;

                    Console.WriteLine($"Producing record: {key} {val}");

                    /*var task = await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = val });

                    Console.WriteLine($"This topic value for Task in produce message {task.Topic}");*/

                    producer.Produce(topic, new Message<string, string> { Key = key, Value = val },
                        (deliveryReport) =>
                        {
                            Console.WriteLine(deliveryReport.Topic);
                            Console.WriteLine(deliveryReport.Partition);
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                                Console.WriteLine($"Produced message to: {deliveryReport.TopicPartitionOffset}");
                                numProduced += 1;
                            }
                        });
                }

                producer.Flush(TimeSpan.FromSeconds(10));

                Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
            }
        }
    }
}