from openai import OpenAI


client = OpenAI(base_url="http://0.0.0.0:8000/v1", api_key="not-needed")
resp = client.chat.completions.create(
    model="qwen2.5-0.5b",
    messages=[{"role": "user", "content": "Hey there"}]
)
print(resp.choices[0].message.content)