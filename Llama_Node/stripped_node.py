
from confluent_kafka import Consumer



conf = {
    'bootstrap.servers' : '0.0.0.0:9092',
    'group.id' : '1',
    'auto.offset.reset' : 'latest'
}

consumer = Consumer(conf)
consumer.subscribe(["input_prompts"])


print("============================================================================")

while True:
    data = consumer.poll(timeout=1.0)
    if not data:
        continue
    else:
        # chat(data.value().decode("utf-8"), True)
        print("Data Recieved, Go On Boiii")
        print(data)