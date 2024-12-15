from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"

def create_topic(topic_name):
    """
    Create a Kafka topic with the given name.
    """
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    NUM_PARTITIONS = 4
    REPLICATION_FACTOR = 3

    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )

            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()  
                    print(f"Topic '{topic_name}' created successfully.")
                except Exception as e:
                    print(f"Failed to create topic '{topic_name}': {e}")
        else:
            print(f"Topic '{topic_name}' already exists.")

    except Exception as e:
        print(f"Error creating topic: {e}")


if __name__ == "__main__":
    TOPIC_NAME = "my_test_topic"  
    create_topic(TOPIC_NAME)
