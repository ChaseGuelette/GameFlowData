# Local LLM Setup — llama.cpp + Vulkan on Windows + Hermes local-worker

> Updated May 12, 2026. This documents the working local Qwen setup and the Hermes `local-worker` profile created for implementation tasks.

## Goal

Run a local coding/implementation worker on the Windows GPU, accessed from Hermes in WSL.

The intended use is narrow:

- API/research/planning models handle broad reasoning and investigation.
- Local Qwen handles implementation handoffs: spec + source files + edits.
- The local profile keeps tools minimal so the model does not waste context on a huge full-agent startup prompt.

## Hardware

| Component | Spec |
|-----------|------|
| CPU | AMD Ryzen 7 9700X (8C/16T) |
| GPU | AMD Radeon RX 9060 XT (16 GB VRAM) |
| RAM | 32 GB DDR5-4800 |
| OS | Windows 10 + WSL2 Ubuntu |
| Inference backend | llama.cpp native Windows build, Vulkan backend |

## Architecture

```text
Windows host
  C:\Users\Chase\Projects\llama.cpp\build\bin\llama-server.exe
  GPU acceleration via Vulkan
  Model: C:\Users\Chase\Projects\models\Qwen3-14B-Q4_K_M.gguf
  Listens on: 0.0.0.0:8080
        ▲
        │ HTTP OpenAI-compatible API
        │ http://172.31.192.1:8080/v1
        ▼
WSL / Hermes
  Hermes custom provider
  Profile: local-worker
  Minimal tool surface
```

Why Windows instead of WSL:

AMD Vulkan GPU acceleration was not available/reliable inside WSL on this machine. Building and running llama.cpp natively on Windows gives the server direct Vulkan access, while Hermes talks to it from WSL over the Windows host IP.

## Current working model

| Item | Value |
|------|-------|
| Model file | `Qwen3-14B-Q4_K_M.gguf` |
| Model path | `C:\Users\Chase\Projects\models\Qwen3-14B-Q4_K_M.gguf` |
| Model size | ~8.4 GB |
| Parameters | ~14.8B |
| Original GGUF context metadata | `qwen3.context_length = 40960` |
| Working served context | `65536` |
| Server port | `8080` |
| WSL base URL | `http://172.31.192.1:8080/v1` |
| Verified dedicated GPU usage | ~13.91 GB |

## Key discovery: `--ctx-size 65536` alone was not enough

The first server command included:

```powershell
--ctx-size 65536
```

But the live server still reported:

```text
n_ctx = 40960
```

The error from Hermes was accurate:

```text
request (57972 tokens) exceeds the available context size (40960 tokens)
```

Root cause: the GGUF metadata advertised Qwen3 context as 40,960 tokens:

```text
qwen3.context_length = 40960
```

llama.cpp respected that unless we explicitly overrode the metadata and used RoPE scaling.

## Working server command

Use this PowerShell command path/script now:

```powershell
C:\Users\Chase\Projects\llama.cpp\restart-qwen-q4kv.ps1
```

Equivalent expanded command:

```powershell
Get-Process llama-server -ErrorAction SilentlyContinue | Stop-Process -Force

cd C:\Users\Chase\Projects\llama.cpp\build\bin

.\llama-server.exe `
  -m C:\Users\Chase\Projects\models\Qwen3-14B-Q4_K_M.gguf `
  --host 0.0.0.0 `
  --port 8080 `
  -ngl 99 `
  --parallel 1 `
  --ctx-size 65536 `
  --rope-scaling yarn `
  --yarn-orig-ctx 40960 `
  --override-kv qwen3.context_length=int:65536 `
  -ctk q4_0 `
  -ctv q4_0
```

What each important flag does:

| Flag | Why it matters |
|------|----------------|
| `--parallel 1` | Uses one server slot, so the full context budget goes to one request instead of being split across slots. |
| `--ctx-size 65536` | Requests a 65K context window. |
| `--rope-scaling yarn` | Enables context extension beyond the model's original metadata. |
| `--yarn-orig-ctx 40960` | Tells llama.cpp the original context length before extension. |
| `--override-kv qwen3.context_length=int:65536` | Overrides the GGUF metadata that capped context at 40,960. |
| `-ctk q4_0 -ctv q4_0` | Quantizes KV cache to q4_0, which is the key VRAM lever for large context. |

## Why q4 KV cache

KV cache memory was the limiting factor. Using q4 KV directly is the right first move for this setup.

Verified startup log:

```text
llama_context: n_ctx         = 65536
llama_context: n_ctx_seq     = 65536
llama_kv_cache: Vulkan0 KV buffer size = 2880.00 MiB
llama_kv_cache: size = 2880.00 MiB (65536 cells, 40 layers, 1/1 seqs), K (q4_0): 1440.00 MiB, V (q4_0): 1440.00 MiB
```

Observed dedicated GPU usage after launch:

```text
13.91 GB
```

This leaves enough headroom on the 16 GB card for a stable 65K context server.

## Verification commands

From WSL:

```bash
curl http://172.31.192.1:8080/health
```

Expected:

```json
{"status":"ok"}
```

Check model metadata:

```bash
curl -s http://172.31.192.1:8080/v1/models | python3 -m json.tool
```

Important verified fields:

```json
{
  "n_ctx": 65536,
  "n_ctx_train": 65536,
  "n_embd": 5120,
  "n_params": 14768307200,
  "size": 8995793920
}
```

Check server props:

```bash
curl -s http://172.31.192.1:8080/props | python3 -m json.tool
```

Important verified fields:

```json
{
  "default_generation_settings": {
    "n_ctx": 65536
  },
  "total_slots": 1,
  "model_alias": "Qwen3-14B-Q4_K_M.gguf"
}
```

Check slots:

```bash
curl -s http://172.31.192.1:8080/slots | python3 -m json.tool
```

Expected:

```json
[
  {
    "id": 0,
    "n_ctx": 65536
  }
]
```

Check GPU memory from Windows PowerShell:

```powershell
C:\Users\Chase\Projects\llama.cpp\check-gpu-memory.ps1
```

Equivalent PowerShell logic:

```powershell
Get-Counter '\GPU Adapter Memory(*)\Dedicated Usage' |
  Select-Object -ExpandProperty CounterSamples |
  Where-Object { $_.CookedValue -gt 0 } |
  Select-Object InstanceName,@{Name='DedicatedGB';Expression={[math]::Round($_.CookedValue / 1GB, 2)}}
```

## Hermes profile: local-worker

Created a dedicated Hermes profile:

```text
local-worker
```

Config path:

```text
/home/chase/.hermes/profiles/local-worker/config.yaml
```

Purpose:

- Keep local Qwen as an implementation worker.
- Avoid the huge default GameFlow/Hermes startup prompt.
- Avoid 100+ tool schemas and all MCP tools unless explicitly needed.
- Preserve the main/default Hermes profile for full project work.

Create command used:

```bash
hermes profile create local-worker --no-skills --no-alias
```

Core config commands:

```bash
hermes -p local-worker config set model.provider custom
hermes -p local-worker config set model.default Qwen3-14B-Q4_K_M
hermes -p local-worker config set model.base_url http://172.31.192.1:8080/v1
hermes -p local-worker config set model.api_key not-needed
hermes -p local-worker config set model.context_length 65536
hermes -p local-worker config set auxiliary.compression.context_length 65536
hermes -p local-worker config set terminal.cwd /home/chase
hermes -p local-worker config set display.personality concise
hermes -p local-worker config set compression.threshold 0.75
hermes -p local-worker config set agent.reasoning_effort none
```

Important config excerpt:

```yaml
model:
  provider: custom
  default: Qwen3-14B-Q4_K_M
  base_url: http://172.31.192.1:8080/v1
  api_key: not-needed
  context_length: 65536
auxiliary:
  compression:
    context_length: 65536
terminal:
  cwd: /home/chase
display:
  personality: concise
compression:
  threshold: 0.75
agent:
  reasoning_effort: none
```

## local-worker tool surface

Enabled only:

- terminal
- file
- code_execution
- skills
- todo

Disabled:

- web
- browser
- vision
- video
- image_gen
- tts
- memory
- session_search
- clarify
- delegation
- cronjob
- messaging
- rl
- homeassistant
- spotify
- yuanbao
- computer_use
- moa

MCP status for `local-worker`:

```text
No MCP servers configured.
```

Tool commands used:

```bash
for t in web browser vision video image_gen tts memory session_search delegation cronjob messaging clarify homeassistant spotify yuanbao computer_use moa rl; do
  hermes -p local-worker tools disable "$t"
done

for t in terminal file code_execution skills todo; do
  hermes -p local-worker tools enable "$t"
done
```

## Smoke tests

Run from WSL:

```bash
hermes -p local-worker chat --quiet -q 'Say exactly OK.'
```

Verified output:

```text
OK
```

Observed local-worker startup/request size:

```text
API call #1: model=Qwen3-14B-Q4_K_M provider=custom in=7951 out=159 total=8110 latency=15.0s cache=2971/7951 (37%)
```

This matters because the previous full-agent setup had a massive startup/tool/MCP surface. The constrained profile starts around 8K input tokens, leaving roughly 57K usable tokens inside the verified 65K context.

## The .thoughts.md failure that motivated this

File stats for `.thoughts.md`:

| Metric | Value |
|--------|-------|
| Bytes | 69,655 |
| Chars | 67,834 |
| Lines | 1,028 |
| Words | 8,019 |
| Qwen/llama token count | 32,652 |

With the old full-agent context and server actually capped at 40,960, reading `.thoughts.md` caused the request to exceed context:

```text
request (57972 tokens) exceeds the available context size (40960 tokens)
```

The fix had two parts:

1. Make the server actually serve 65K context.
2. Create a small local-worker Hermes profile so startup/tool overhead does not waste the local model's context.

## Daily usage

Start/restart the local server:

```powershell
C:\Users\Chase\Projects\llama.cpp\restart-qwen-q4kv.ps1
```

Verify from WSL:

```bash
curl http://172.31.192.1:8080/health
curl -s http://172.31.192.1:8080/v1/models | python3 -m json.tool | grep -A8 '"meta"'
```

Run local worker:

```bash
hermes -p local-worker chat
```

One-shot local worker:

```bash
hermes -p local-worker chat --quiet -q 'Say exactly READY.'
```

For implementation handoffs, prefer constrained task prompts like:

```bash
hermes -p local-worker chat --toolsets terminal,file,code_execution,skills,todo -q 'Implement the attached spec. Keep changes scoped. Run the relevant test.'
```

## When not to use local-worker

Do not use `local-worker` for broad project planning, research, or tasks requiring many MCP systems. Use the main/API model profile for that.

Use local-worker when:

- The task has a clear spec.
- The relevant files are known.
- The work is implementation/edit/test oriented.
- Minimal tools are enough.

Avoid local-worker when:

- You need Supabase/Railway/Vercel/GitHub/GBrain MCPs.
- You need heavy web research.
- You need long project memory or session search.
- You need the full GameFlowData AGENTS.md/invariant stack in context.

## Troubleshooting

| Problem | Check | Fix |
|---------|-------|-----|
| Server says 40,960 context | `/v1/models`, `/props` | Restart with `--rope-scaling yarn`, `--yarn-orig-ctx 40960`, `--override-kv qwen3.context_length=int:65536` |
| Server OOMs or crashes | Windows stderr log and GPU memory | Keep `--parallel 1`; use `-ctk q4_0 -ctv q4_0`; lower context only if needed |
| Hermes still overflows | Check profile/tool surface | Use `local-worker`; avoid main profile with 100+ tools/MCPs |
| WSL cannot connect | `curl http://172.31.192.1:8080/health` | Use Windows host IP from `ip route`, not WSL localhost |
| Very slow first launch | llama.cpp Vulkan shader warmup | Wait; first launch after changes can take minutes |
| Need exact server logs | `C:\Users\Chase\Projects\llama.cpp\logs\` | Check `llama-server-qwen3-14b-q4kv.err.log` |

## Persisted scripts

| Script | Purpose |
|--------|---------|
| `C:\Users\Chase\Projects\llama.cpp\restart-qwen-q4kv.ps1` | Stops old server and starts Qwen3-14B with 65K context + q4 KV |
| `C:\Users\Chase\Projects\llama.cpp\check-gpu-memory.ps1` | Reports dedicated GPU memory usage |

## Final state

Working state as of May 12, 2026:

- llama.cpp Windows Vulkan server is running Qwen3-14B-Q4_K_M.
- Context is verified at 65,536, not just configured.
- KV cache is q4_0 for both K and V.
- Server uses one slot so the whole context goes to one request.
- GPU dedicated usage is about 13.9 GB.
- Hermes `local-worker` profile is configured and smoke-tested.
- Local-worker request overhead is about 8K tokens, leaving useful context for implementation work.
