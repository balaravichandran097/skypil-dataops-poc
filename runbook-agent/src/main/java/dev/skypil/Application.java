package dev.skypil;

import java.util.*;
import java.util.regex.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

@SpringBootApplication
@RestController
public class Application {
    public static void main(String[] args) { SpringApplication.run(Application.class, args); }
    record Question(String question) {}
    record Answer(String mode, String answer, List<Runbooks.Evidence> evidence, List<String> trace) {}
    private final ModelAgent model;
    Application(ModelAgent model) { this.model = model; }

    @GetMapping("/health")
    Map<String,String> health() { return Map.of("status","ok","mode", model.enabled() ? "model" : "guided"); }

    @PostMapping("/api/ask")
    Answer ask(@RequestBody Question request) {
        String q = request.question();
        if (q == null || q.isBlank() || q.length() > 1200)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Enter 1 to 1200 characters.");
        return model.enabled() ? model.ask(q) : guided(q);
    }
    static Answer guided(String question) {
        String q = question.toLowerCase(Locale.ROOT);
        Matcher id = Pattern.compile("\\b(?:job\\s*#?\\s*)?(\\d{4,})\\b").matcher(q);
        List<Runbooks.Evidence> evidence;
        List<String> trace = new ArrayList<>();
        if (id.find()) {
            boolean readiness = q.matches("(?s).*(rerun|dry[ -]?run|readiness|retry|ready).*");
            trace.add(readiness ? "check_readiness" : "inspect_job");
            evidence = Runbooks.jobEvidence(id.group(1), readiness);
            if (evidence.size() > 1) trace.add("load_linked_runbook");
        } else if (q.matches("(?s).*\\b(job|rerun|dry.run)\\b.*")) {
            return new Answer("guided", "Which job? Try 1042, 1043 or 1044.", List.of(), List.of());
        } else {
            trace.add("search_runbooks");
            evidence = Runbooks.search(q).stream().limit(1).toList();
        }
        String answer = evidence.stream().map(Runbooks.Evidence::text).collect(java.util.stream.Collectors.joining(" "));
        return new Answer("guided", answer.isBlank() ? "No matching runbook passage. Try a script name or CSV version." : answer, evidence, trace);
    }
}
