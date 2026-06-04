from llama_cpp import Llama
from confluent_kafka import Consumer
import psycopg2
from psycopg2.extensions import AsIs
import json



def chat(prompt, stream):

    if stream == True:
        response = llm.create_chat_completion(
            messages=[
                {"role" : "system", "content" : "You are an assistants"},
                {"role" : "user", "content" : prompt},
            ],
            stream=True
        )

        for chunk in response:
            delta = chunk["choices"][0]["delta"]
            if "content" in delta : 
                print(delta["content"], end="", flush=True)

    else:
        response = llm.create_chat_completion(
            messages=[
                {"role" : "system", "content" : "You are an assistant"},
                {"role" : "user", "content" : prompt},
            ],
            stream=False
        )

        return response["choices"][0]["message"]["content"]



# Inference Backend should update the DB after infered. 


conf = {
    'bootstrap.servers' : '0.0.0.0:9092',
    'group.id' : '1',
    'auto.offset.reset' : 'latest'
}

conn = psycopg2.connect(host = "192.168.1.8", dbname = "Distrinfer", user = "postgres", password = 'sarangi')
curse = conn.cursor()

consumer = Consumer(conf)
consumer.subscribe(["input_prompts"])


llm = Llama.from_pretrained(
    repo_id="Qwen/Qwen2.5-1.5B-Instruct-GGUF",
    filename="*q8_0.gguf",
    verbose = False,
    n_ctx = 200000,
)

print("============================================================================")

while True:
    data = consumer.poll(timeout=1.0)
    if not data:
        continue
    else:
        print("Query Recieved")
        json_ = json.loads(data.value().decode("utf-8"))
        infer = chat(json_.get("prompt"), False)
        hex_value = json_.get("hex")
        Query = f"UPDATE DATA SET infer = %s, status = %s WHERE Hash = %s"
        curse.execute(Query, (infer, "Success", hex_value))
        conn.commit()
        print("Query Infered")