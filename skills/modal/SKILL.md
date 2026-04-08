---
name: modal
description: Guidance for writing and reviewing Modal.com Python applications. Use when implementing, debugging, or explaining Modal apps, functions, classes, images, volumes, secrets, schedules, web endpoints, or deployment workflows.
---

# Modal

Use this skill whenever the user is working with [Modal](references/modal-rules.md) code or asking for help with Modal architecture, APIs, deployment, or debugging.

## Core rules

- Always use `import modal` and qualified names such as `modal.App()` and `modal.Image.debian_slim()`.
- Prefer current, non-deprecated Modal APIs.
- Name Modal Apps, Volumes, and Secrets in kebab-case.
- Treat Modal as cloud execution by default, even during development.
- Put Modal-specific dependencies on `modal.Image` objects attached to functions or classes, not only in global project dependency files.
- Keep global-scope code safe to execute both locally and in Modal containers.
- For up-to-date syntax, examples, and edge cases, consult:
  - `https://modal.com/docs`
  - `https://modal.com/llms-full.txt`
  - `https://modal.com/docs/examples`
  - `https://github.com/modal-labs/modal-examples`
  - `https://modal.com/docs/reference`

## Implementation checklist

When creating or editing Modal code:

1. Identify the right primitive:
   - `modal.App()` for the app container
   - `@app.function(...)` for stateless remote work
   - `@app.cls(...)` with `@modal.enter()`, `@modal.method()`, `@modal.exit()` for stateful services
   - volumes, secrets, dicts, queues, and sandboxes only when they solve a real need
2. Define an appropriate image:
   - start with `modal.Image.debian_slim(...)`
   - attach Python, apt, and setup steps on the image
   - keep image dependencies scoped to the functions/classes that need them
3. Configure runtime needs explicitly:
   - CPU, memory, GPU, schedule, timeout, secrets, volumes
4. Choose invocation style intentionally:
   - `.remote()` for normal cloud execution
   - `.local()` for same-context execution
   - `.map()` for parallel fan-out
   - `.spawn()` for fire-and-forget background calls tied to app lifetime
5. For web apps/endpoints, use the correct decorator:
   - `@modal.fastapi_endpoint()` for simple HTTP endpoints
   - `@modal.asgi_app()` when returning an ASGI app such as FastAPI
6. For deploy/run instructions, prefer official CLI commands like:
   - `modal run path/to/app.py`
   - `modal serve path/to/app.py`
   - `modal deploy path/to/app.py`
7. If suggesting testing or debugging steps, mention:
   - `modal app logs <app_name>`
   - `with modal.enable_output():` around `app.deploy()` when extra deploy output helps

## Code patterns

### Minimal app

```python
import modal

app = modal.App("my-modal-app")

@app.function()
def square(x: int) -> int:
    return x * x
```

### Function with custom image and hardware

```python
import modal

app = modal.App("inference-app")

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("torch", "numpy", "transformers")
    .apt_install("ffmpeg")
    .run_commands("mkdir -p /models")
)

@app.function(image=image, gpu="A10G", memory=4096, cpu=2)
def inference(prompt: str) -> str:
    import transformers
    return prompt
```

### Stateful class

```python
import modal

app = modal.App("model-server")

@app.cls(gpu="A100")
class ModelServer:
    @modal.enter()
    def load_model(self):
        self.model = load_model()

    @modal.method()
    def predict(self, text: str) -> str:
        return self.model.generate(text)

    @modal.exit()
    def cleanup(self):
        cleanup()
```

### FastAPI endpoint

```python
import modal

app = modal.App("api-app")

@app.function()
@modal.fastapi_endpoint()
def healthcheck():
    return {"status": "ok"}
```

## Review heuristics

When reviewing Modal code, check for these common issues:

- Missing `import modal`
- Old or deprecated APIs
- Dependencies incorrectly assumed to exist outside the function image
- Heavy imports at module scope that should live inside the Modal function/class method
- Resource requirements not declared for GPU/CPU/memory intensive workloads
- Web endpoints using the wrong decorator
- Stateful workloads modeled as plain functions when `@app.cls` is a better fit
- Incorrect assumptions that code executes only on the local machine

## Reference

- Read `references/modal-rules.md` for the detailed rules this skill is based on.
