from llama_cpp import Llama
from confluent_kafka import Consumer



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

        print(response["choices"][0]["message"]["content"])


conf = {
    'bootstrap.servers' : '0.0.0.0:9092',
    'group.id' : '1',
    'auto.offset.reset' : 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(["input_prompts"])


llm = Llama.from_pretrained(
    repo_id="Qwen/Qwen2.5-1.5B-Instruct-GGUF",
    filename="*q8_0.gguf",
    verbose = False,
    n_ctx = 200000,
)



while True:
    data = consumer.poll()
    if not data:
        continue
    else:
        chat(data.value().decode("utf-8"), True)