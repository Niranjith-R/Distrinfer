from llama_cpp import Llama
from confluent_kafka import Consumer



def chat(prompt):
    response = llm.create_chat_completion(
        messages=[
            {"role" : "system", "content" : "You are an assistant who will always end with '\n'"},
            {"role" : "user", "content" : prompt},
        ],
        stream=True

    )

    for chunk in response:
        delta = chunk["choices"][0]["delta"]
        if "content" in delta : 
            print(delta["content"], end="", flush=True)


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


# output = llm(
#     prompt='''
# \
# ''',
#     stop=["Q:", "\n"],
#     max_tokens=1024,
#     echo=False
# )

# print(output["choices"][0]["text"])
while True:
    data = consumer.poll()
    if not data:
        continue
    else:
        chat(data.value().decode("utf-8"))