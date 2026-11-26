package dev.skypil;

import com.fasterxml.jackson.databind.*;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
class ModelAgent {
    private final ObjectMapper json;
    private final String model, key, endpoint;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    ModelAgent(ObjectMapper json, @Value("${AGENT_MODEL:}") String model,
               @Value("${MODEL_API_KEY:}") String key,
               @Value("${MODEL_CHAT_URL:http://localhost:11434/v1/chat/completions}") String endpoint) {
        this.json = json; this.model = model; this.key = key; this.endpoint = endpoint;
    }
    boolean enabled() { return !model.isBlank(); }

    private Map<String,Object> tool(String name, String description, String argument) {
        return Map.of("type","function","function",Map.of("name",name,"description",description,
            "parameters",Map.of("type","object","properties",Map.of(argument,Map.of("type","string")),
                "required",List.of(argument),"additionalProperties",false)));
    }
    Application.Answer ask(String question) {
        List<Object> messages = new ArrayList<>();
        messages.add(Map.of("role","system","content","You are a DataOps assistant. Use tools for factual answers. Answer only from their evidence. Ask for missing details or say when evidence is insufficient. Quote the relevant source IDs. You can only inspect and dry-run."));
        messages.add(Map.of("role","user","content",question));
        var tools = List.of(tool("search_runbooks","Find versioned passages. Include script, version and topic in query.","query"),
            tool("inspect_job","Read synthetic job status and its runbook.","id"),
            tool("check_readiness","Dry-run a synthetic job and return blockers.","id"));
        List<Runbooks.Evidence> evidence = new ArrayList<>();
        List<String> trace = new ArrayList<>();
        try {
            for (int round = 0; round < 4; round++) {
                var body = Map.of("model", model, "messages", messages, "tools", tools,
                    "tool_choice", round == 3 ? "none" : "auto", "max_tokens", 450);
                var req = HttpRequest.newBuilder(URI.create(endpoint)).timeout(Duration.ofSeconds(25))
                    .header("Content-Type","application/json");
                if (!key.isBlank()) req.header("Authorization","Bearer " + key);
                var response = http.send(req.POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body))).build(),
                    HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() / 100 != 2) throw new IllegalStateException("Provider unavailable");
                JsonNode message = json.readTree(response.body()).path("choices").path(0).path("message");
                var calls = message.path("tool_calls");
                if (!calls.isArray() || calls.isEmpty()) {
                    String content = message.path("content").asText("").strip();
                    if (content.isEmpty()) throw new IllegalStateException("Empty completion");
                    return new Application.Answer("model", content, evidence.stream().distinct().toList(), trace);
                }
                if (round == 3 || trace.size() + calls.size() > 6) break;
                messages.add(message);
                for (JsonNode call : calls) {
                    String name = call.path("function").path("name").asText();
                    var args = json.readTree(call.path("function").path("arguments").asText());
                    String id = args.path("id").asText("");
                    List<Runbooks.Evidence> found = switch (name) {
                        case "search_runbooks" -> Runbooks.search(args.path("query").asText(""));
                        case "inspect_job", "check_readiness" -> id.matches("[0-9]{4,12}")
                            ? Runbooks.jobEvidence(id, name.equals("check_readiness")) : List.of();
                        default -> List.of();
                    };
                    trace.add(name);
                    evidence.addAll(found);
                    messages.add(Map.of("role","tool","tool_call_id",call.path("id").asText(),
                        "content",json.writeValueAsString(found)));
                }
            }
            return new Application.Answer("model","Tool budget reached. Narrow the question and try again.", evidence, trace);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY,"Model request failed.");
        }
    }
}
