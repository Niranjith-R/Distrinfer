# Distrinfer

Distrinfer is a distributed LLM inference engine for **batch / offline workloads** — large bulk sets of prompts that need to be run through a model — built around the idea that scaling the inference layer horizontally, across heterogeneous hardware, is the most efficient way to chew through that volume.

It is **not** designed for real-time, latency-sensitive, single-request inference. If you need a fast round trip for one prompt at a time, this is the wrong tool. If you have thousands of prompts to process and don't care which node or how long any individual one takes, Distrinfer is built for exactly that.

> ⚠️ **Not an OpenAI API substitute.** Distrinfer uses its own custom API (`/`, `/query`, `/query/{id}`) — it does **not** implement the OpenAI API spec, request/response schema, streaming (SSE), or function/tool calling. **Do not point existing AI agent frameworks or SDKs built for the OpenAI API at this expecting plug-and-play compatibility** — they will not work without writing a custom adapter.

## Core Idea

* Treat inference as a distributable, queueable workload rather than a single long-lived server process.
* Dispatch work dynamically across whatever hardware you have, not just identical clustered nodes.
* Use the result store (Postgres) as the single source of truth, not the task queue's transient state.

## Architecture

<img width="700" height="600" alt="New Arch" src="https://github.com/user-attachments/assets/a6cbd069-7c4d-428e-9f65-be3f999114e4" />


At a high level:

- **FastAPI** — the producer / API surface. Accepts inference requests, pre-inserts a "Pending" row into Postgres, and dispatches a Celery task.
- **Celery + RabbitMQ** — the task queue and broker. Workers pull tasks and run inference; results are written back to Postgres rather than relying on Celery's own result backend.
- **PostgreSQL** — the authoritative store for task state and results.
- **llama-cpp-python** — the inference runtime on each worker node, running a GGUF model (currently Qwen2.5-0.5B-Instruct, `q8_0`).

This replaced an earlier Kafka-based design, which struggled with heterogeneous hardware due to static partition assignment — partitions don't auto-rebalance toward faster nodes, so the cluster was bottlenecked by its slowest member. Celery's dynamic dispatch fixed this.

## Tech Stack

| Component | Role |
|---|---|
| FastAPI | API layer / task producer |
| Celery | Distributed task execution |
| RabbitMQ | Message broker (pinned to `3.13-management`, 4.x is incompatible) |
| PostgreSQL | Authoritative task + result store |
| llama-cpp-python | Inference runtime |

## Quickstart

> This section assumes familiarity with running RabbitMQ and Postgres locally or via existing infra. Exact install steps depend on your OS/package manager.

1. Stand up RabbitMQ (`3.13-management`) and PostgreSQL.
2. Install Python dependencies (FastAPI, Celery, llama-cpp-python, SQLModel, etc.) — see `requirements.txt`.
3. Build/install `llama-cpp-python` against the backend matching your hardware — see https://pypi.org/project/llama-cpp-python/.
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

Request body, for all endpoints below:

```json
{
  "prompt": "How tall is Burj Khalifa"
}
```

### `POST /` — Live inference (blocking)

Dispatches the prompt to a Celery worker and blocks the request until the result is ready, polling internally every 100ms. Returns the inferred result directly in the response.

```bash
curl -X POST http://localhost:8000/ \
  -H "Content-Type: application/json" \
  -d '{"prompt": "How tall is Burj Khalifa"}'
```

Use this for quick, low-volume testing. It is **not** suited for batch workloads — each request holds the connection open for the full inference duration, which defeats the purpose of horizontal scaling at any real volume.

### `POST /query` — Queued inference (async)

The intended entry point for batch use. Hashes the prompt (SHA-256, salted with a timestamp) to generate a unique reference, persists the request to Postgres with `status`, dispatches the task to Celery, and returns immediately — it does not wait for inference to complete.

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"prompt": "How tall is Burj Khalifa"}'
```

Response:

```json
{
  "data": {
    "prompt": "How tall is Burj Khalifa",
    "status": "pending"
  },
  "hash": "<sha256-hex>"
}
```

### `GET /query/{prompt_id}` — Poll for result

Look up a previously submitted prompt by its hash to check status and, once complete, retrieve the result.

```bash
curl http://localhost:8000/query/<sha256-hex>
```

Response:

```json
{
  "id": "<sha256-hex>",
  "status": "complete",
  "Data": {
    "host": "<worker-host>",
    "prompt": "How tall is Burj Khalifa",
    "infered": "<model output>"
  }
}
```

This submit-then-poll pattern (`/query` + `/query/{prompt_id}`) is the supported way to run prompts through Distrinfer at volume — submit all prompts up front, then poll for results as workers complete them.

## Why Not Docker for Inference Nodes

Inference nodes are intentionally **not containerized**. `llama-cpp-python` needs to be compiled against the specific backend matching each node's hardware (CUDA, ROCm/HIP, Vulkan, OpenBLAS, plain CPU), and a single image either can't cover all of these or silently falls back to a slower backend with no clear signal. Since Distrinfer is explicitly designed to run across heterogeneous, edge-class hardware (SBCs, old laptops, desktop GPUs), native builds per node — documented below — were chosen over fighting that abstraction.

## Hardware & Build Matrix

| Node | Hardware | Backend |
|---|---|---|
| Primary | Ryzen 5 3400G | OpenBLAS / CPU |
| Laptop | i3 6006U | OpenBLAS |
| SBC | Radxa Rock 3A | CPU (RKNPU2 NPU backend planned, not yet implemented) |
| GPU node | RX 6500 XT (gfx1032) | Vulkan / ROCm-HIP (vLLM not viable — insufficient rocBLAS kernel support for gfx1032) |

Build `llama-cpp-python` with the `CMAKE_ARGS` matching your target backend before installing on each node.

## Initial Benchmarks

100 prompts, same model (Qwen2.5-0.5B-Instruct), `max_tokens=512`, format `mm:ss:ms`:

| Configuration | Time |
|---|---|
| R5 3400G only (Celery) | 15:21:71 |
| R5 3400G + i3 6006U (Celery, dynamic dispatch) | 11:43:02 |

Adding a second, slower node still reduced total time by roughly 24%, since Celery dispatches dynamically rather than via static partitioning — the bottleneck node simply gets fewer tasks rather than stalling the whole batch.

## Known Limitations

- **No streaming.** Token-by-token SSE delivery isn't supported; the current design relies on Postgres polling, which isn't suited to per-token streaming. A Redis pub/sub side channel is the planned fix.
- **No OpenAI API compatibility.** Distrinfer's API is custom (`/`, `/query`, `/query/{id}`) and does not follow the OpenAI request/response schema. No tool/function calling, no streaming. Integrating with agent frameworks expecting the OpenAI spec requires writing your own adapter.
- **No auth yet.** JWT auth for externally-facing endpoints is planned but not implemented — don't expose this beyond a trusted network as-is.
- **RKNPU2 NPU backend** for the Rock 3A is unimplemented; that node currently runs CPU inference only.

## License

TBD
