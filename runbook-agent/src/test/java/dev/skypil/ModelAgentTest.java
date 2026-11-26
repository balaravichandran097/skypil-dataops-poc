package dev.skypil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ModelAgentTest {
    @Test void relaysToolEvidenceBeforeFinalAnswer() throws Exception {
        ObjectMapper json = new ObjectMapper();
        AtomicBoolean receivedEvidence = new AtomicBoolean();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0),0);
        server.createContext("/chat", exchange -> {
            var request = json.readTree(exchange.getRequestBody());
            var messages = request.path("messages");
            var last = messages.path(messages.size()-1);
            Map<String,Object> message;
            if ("tool".equals(last.path("role").asText())) {
                receivedEvidence.set(last.path("content").asText().contains("READY FOR APPROVAL"));
                message = Map.of("role","assistant","content","Job 1043 is ready for approval.");
            } else {
                var call = Map.of("id","call1","type","function","function",Map.of(
                    "name","check_readiness","arguments",json.writeValueAsString(Map.of("id","1043"))));
                message = Map.of("role","assistant","tool_calls",List.of(call));
            }
            byte[] bytes = json.writeValueAsBytes(Map.of("choices",List.of(Map.of("message",message))));
            exchange.sendResponseHeaders(200, bytes.length);
            try (var out = exchange.getResponseBody()) { out.write(bytes); }
        });
        server.start();
        try {
            var agent = new ModelAgent(json,"test","","http://127.0.0.1:" + server.getAddress().getPort() + "/chat");
            var result = agent.ask("Can I rerun job 1043?");
            assertTrue(receivedEvidence.get());
            assertEquals(List.of("check_readiness"),result.trace());
            assertEquals("job/1043",result.evidence().getFirst().source());
        } finally { server.stop(0); }
    }
}
