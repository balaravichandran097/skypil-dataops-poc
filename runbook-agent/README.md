# SkyPil Runbook Desk
A small Java 21 / Spring Boot application for learning retrieval and tool calling. All job data is synthetic. Open the service root for the UI. GET /health reports the active mode. POST /api/ask accepts {"question":"Can I rerun job 1043?"} and returns the answer, source passages, and tool trace.

## Run
With JDK 21 and Maven 3.9+, run `mvn test` and `mvn spring-boot:run`, then open http://localhost:8080. Alternatively run `docker build -t runbook-desk .` and `docker run --rm -p 8080:8080 runbook-desk`. The Docker build runs the tests. PORT defaults to 8080.

## Two modes
Guided mode is the keyless default. It matches question intent, retrieves version-filtered passages using keyword overlap, and returns extracts. Try CSV v1 versus v2, then jobs 1042 (invalid input), 1043 (ready for approval), and 1044 (busy). Readiness never changes any job.

Model mode calls an OpenAI-compatible chat completions endpoint with native function tools. To try it locally with Ollama, run Ollama, pull a tool-capable model, then launch Spring Boot:
```sh
ollama pull qwen3:4b
AGENT_MODEL=qwen3:4b mvn spring-boot:run
```
The default MODEL_CHAT_URL is http://localhost:11434/v1/chat/completions. For a hosted endpoint set MODEL_CHAT_URL and AGENT_MODEL, and supply MODEL_API_KEY as a server-side secret. Localhost inside a container refers to that container. Keep paid providers behind authenticated access and a budget before exposing model mode publicly.

## Follow a request
1. Application.java validates the request and selects the active mode.
2. Runbooks.java holds the sample corpus and synthetic job fixtures. Search filters explicit versions before ranking passages. Unversioned status-update questions use v2.
3. ModelAgent.java exposes search_runbooks, inspect_job and check_readiness. It relays tool results to the model and caps each question at four completions and six tool calls.
4. The UI displays the evidence and trace separately from the answer. Compare traces from guided and model mode. Model quality depends on the provider. No vector database, AWS credentials, production execution, or persistent job state is required.

## Render
Create a Docker web service from this project and select Free. If building from the skypil-dataops-poc repository, use the runbook-agent branch and the runbook-agent subdirectory as the Docker build context, with runbook-agent/Dockerfile as the repository-relative Dockerfile path. Health endpoint: /health.
