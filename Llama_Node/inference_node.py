from celery import Celery
import psycopg2
from llama_cpp import Llama
import json
from socket import gethostname


app = Celery("inference_node", broker="amqp://admin:admin@localhost:5672/")
conn = psycopg2.connect(host = "192.168.1.8", dbname = "Distrinfer", user = "postgres", password = 'sarangi')
curse = conn.cursor()

llm = Llama.from_pretrained(
    repo_id="Qwen/Qwen2.5-0.5B-Instruct-GGUF",
    filename="*q8_0.gguf",
    verbose = False,
    n_ctx = 32768,
)


@app.task
def infer(input):

    update_sql = "UPDATE DATA SET host = %s where hash = %s"
    json_data = json.load(input)
    hash = json_data.get("hex")
    curse.execute(update_sql, (gethostname(), hash))
    conn.commit()
    prompt = json_data.get("prompt")
    response = llm.create_chat_completion(
            messages=[
                {"role" : "system", "content" : "You are an assistant"},
                {"role" : "user", "content" : prompt},
            ],
            stream=False,
            max_tokens=2048
        )
    
    update_infer_sql = "UPDATE DATA SET infer = %s WHERE hash = %s"
    curse.execute(update_infer_sql, (response["choices"][0]["message"]["content"], hash))