# Distrinfer

Distrinfer is a distributed LLM inference engine built around a simple idea: scale the inference layer horizontally across whatever hardware you actually have — not a matched cluster, but heterogeneous, edge-class machines (old laptops, SBCs, a spare GPU) — by treating inference as a distributable, queueable workload rather than a single long-lived server process.

It was originally built for **batch / offline workloads** — large sets of prompts that need to run through a model with no particular urgency on any single one — and now also exposes a **standard, OpenAI-compatible endpoint** for low-volume interactive use.

## Core Idea

- Treat inference as a distributable, queueable workload rather than a single long-lived server process.
- Dispatch work dynamically across whatever hardware you have, not just identical clustered nodes.
- Use the result store (Postgres) as the single source of truth, not the task queue's transient state.

## Architecture

- **FastAPI** — the producer / API surface. Accepts inference requests, pre-inserts a "Pending" row into Postgres, and dispatches a Celery task.
- **Celery + RabbitMQ** — the task queue and broker. Workers pull tasks and run inference; results are written back to Postgres rather than relying on Celery's own result backend.
- **PostgreSQL** — the authoritative store for task state and results.
- **llama-cpp-python** — the inference runtime on each worker node, running a GGUF model (currently Qwen2.5-0.5B-Instruct, `q8_0`).

This replaced an earlier Kafka-based design, which struggled with heterogeneous hardware due to static partition assignment — partitions don't auto-rebalance toward faster nodes, so the cluster was bottlenecked by its slowest member. Celery's dynamic dispatch fixed this.

## Tech Stack

| Component        | Role                                                              |
| ---------------- | ------------------------------------------------------------------|
| FastAPI          | API layer / task producer                                         |
| Celery           | Distributed task execution                                        |
| RabbitMQ         | Message broker (pinned to `3.13-management`, 4.x is incompatible) |
| PostgreSQL       | Authoritative task + result store                                 |
| llama-cpp-python | Inference runtime                                                 |

## Quickstart

> This section assumes familiarity with running RabbitMQ and Postgres locally or via existing infra. Exact install steps depend on your OS/package manager.

1. Stand up RabbitMQ (`3.13-management`) and PostgreSQL.
2. Install Python dependencies (FastAPI, Celery, llama-cpp-python, SQLModel, etc.) — see `requirements.txt`.
3. Build/install `llama-cpp-python` against the backend matching your hardware — see <https://pypi.org/project/llama-cpp-python/>.
4. Set environment variables for your RabbitMQ, Postgres, and model path.
5. Start the FastAPI producer.
6. Start one or more Celery workers, on the same machine or on separate nodes pointed at the same broker/DB.

```bash
# producer
fastapi run api_main.py --host 0.0.0.0 --port 8000

# worker (run on each inference node)
celery -A inference_node worker --loglevel=info
```

## API Usage

Distrinfer exposes two separate interfaces depending on what you're doing.

### `POST /v1/chat/completions` — OpenAI-compatible (blocking)

Standard OpenAI chat completions request/response shape. Point the official OpenAI SDK, or any OpenAI-compatible client, at this with `base_url` set to your Distrinfer host + `/v1` — no adapter needed.

```python
from openai import OpenAI

client = OpenAI(base_url="http://your-host:8000/v1", api_key="not-needed")
resp = client.chat.completions.create(
    model="qwen2.5-0.5b",
    messages=[{"role": "user", "content": "How tall is Burj Khalifa"}]
)
print(resp.choices[0].message.content)
```

```bash
curl http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen2.5-0.5b",
    "messages": [{"role": "user", "content": "How tall is Burj Khalifa"}]
  }'
```

Dispatches to a Celery worker and blocks the request until the result is ready. Holds the connection open for the full inference duration — suited to low-volume/interactive use, not batch (see below and Known Limitations).

### `POST /query` + `GET /query/{id}` — Custom batch interface (async)

The intended entry point for batch/offline use. `POST /query` hashes the prompt (SHA-256, salted with a timestamp) to generate a unique reference, persists the request to Postgres with `status`, dispatches the task to Celery, and returns immediately without waiting for inference to complete.

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Hey there"}'
```

Response:

```json
{
  "data": {
    "prompt": "Hey there",
    "status": "pending"
  },
  "hash": "<sha256-hex>"
}
```

Poll `GET /query/{prompt_id}` by that hash to check status and retrieve the result once complete:

```bash
curl http://localhost:8000/query/<sha256-hex>
```

```json
{
  "id": "chatcmpl-781d2938-e963-4e8a-bdd4-5a181d2cd106",
  "object": "chat.completion",
  "created": 1789317630,
  "model": "/home/niranjithr/.cache/huggingface/hub/models--Qwen--Qwen2.5-0.5B-Instruct-GGUF/snapshots/9217f5db79a29953eb74d5343926648285ec7e67/./qwen2.5-0.5b-instruct-q8_0.gguf",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "Hello! How can I assist you today?"
      },
      "logprobs": null,
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 31,
    "completion_tokens": 9,
    "total_tokens": 40
  }
}
```

This submit-then-poll pattern is the supported way to run prompts through Distrinfer at volume — submit all prompts up front, then poll for results as workers complete them.

## Why Not Docker for Inference Nodes

Inference nodes are intentionally **not containerized**. `llama-cpp-python` needs to be compiled against the specific backend matching each node's hardware (CUDA, ROCm/HIP, Vulkan, OpenBLAS, plain CPU), and a single image either can't cover all of these or silently falls back to a slower backend with no clear signal. Since Distrinfer is explicitly designed to run across heterogeneous, edge-class hardware (SBCs, old laptops, desktop GPUs), native builds per node — documented below — were chosen over fighting that abstraction.


Build `llama-cpp-python` with the `CMAKE_ARGS` matching your target backend before installing on each node.

## Initial Benchmarks

100 prompts, same model (Qwen2.5-0.5B-Instruct), `max_tokens=512`, format `mm:ss:ms`:

| Configuration                           | Time       |
| ---------------------------------------- | ---------- |
| R5 3400G only (Celery)                   | 21:09:71   |
| R5 3400G + R5 7530U (Celery)              | 12:34:02   |
| R5 3400G + R5 7530U + i3 6006U (Celery)   | 8:55:02    |
| R5 3400G + i3 6006U (Kafka)               | 1:05:51:02 |

Adding a second, slower node still reduced total time by roughly 24%, since Celery dispatches dynamically rather than via static partitioning — the bottleneck node simply gets fewer tasks rather than stalling the whole batch.

## Known Limitations

- **OpenAI compatibility is partial.** `/v1/chat/completions` supports standard non-streaming chat completions and has been verified against the official OpenAI Python SDK. Streaming (`chat.completion.chunk` / SSE) and tool/function calling are **not** implemented yet.
- **No streaming on the batch path either.** Token-by-token delivery isn't supported there; results are retrieved via Postgres polling, which isn't suited to per-token streaming.
- **No auth yet.** JWT (or simple API key) auth for externally-facing endpoints is planned but not implemented — don't expose this beyond a trusted network as-is.

## License

GPL-3.0 license
