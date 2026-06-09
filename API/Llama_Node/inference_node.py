from celery import Celery
from celery.signals import worker_process_init
import psycopg2
from llama_cpp import Llama
import json
from socket import gethostname



conn = curse = llm = None
app = Celery("inference_node", broker="amqp://admin:admin@localhost:5672/")


@worker_process_init.connect
def init_worker(**kwargs):
    global conn, curse, llm
    conn = psycopg2.connect(host = "192.168.1.8", dbname = "Distrinfer", user = "postgres", password = 'sarangi')
    curse = conn.cursor()
    llm = Llama.from_pretrained(
        repo_id="Qwen/Qwen2.5-0.5B-Instruct-GGUF",
        filename="*q8_0.gguf",
        verbose = False,
        n_ctx = 32768,
    )


@app.task(name="infer")
def infer(input):
    json_data = json.loads(input)
    hash = json_data.get("hex")
    prompt = json_data.get("prompt")
    response = llm.create_chat_completion(
            messages=[
                {"role" : "system", "content" : "You are an assistant"},
                {"role" : "user", "content" : prompt},
            ],
            stream=False,
            max_tokens=2048
        )
    
    update_infer_sql = "UPDATE DATA SET infer = %s, status = %s, host = %s WHERE hash = %s"
    curse.execute(update_infer_sql, (response["choices"][0]["message"]["content"], "Success", gethostname(), hash))
    conn.commit()