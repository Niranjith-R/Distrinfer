from confluent_kafka.admin import AdminClient, NewPartitions


admin = AdminClient({
    'bootstrap.servers' : '192.168.1.11:9092',
})

admin.create_partitions([NewPartitions("input_prompts", 3)])

metadata = admin.list_topics(topic = "input_prompts")
topic = metadata.topics['input_prompts']
print(f"Partitions: {len(topic.partitions)}")