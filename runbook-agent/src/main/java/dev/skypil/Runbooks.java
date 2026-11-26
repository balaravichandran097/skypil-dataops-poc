package dev.skypil;

import java.util.*;
import java.util.regex.*;
import java.util.stream.*;

final class Runbooks {
    record Passage(String source, String keywords, String text) {}
    record Evidence(String source, String text) {}
    static final List<Passage> DOCS = List.of(
        new Passage("account-status-update/v1#csv","columns headers fields schema csv status","CSV v1 requires account_id and status. Allowed status values are ACTIVE, INACTIVE and SUSPENDED."),
        new Passage("account-status-update/v2#csv","columns headers fields schema csv status","CSV v2 requires account_id, status and reason. Include a nonblank reason on every row."),
        new Passage("account-extract/v1#input","extract export download columns fields csv","The account extract takes an account_id column and produces an account snapshot CSV."),
        new Passage("worker/v1#lease","executor busy lock lease running","A busy executor holds the single-execution lease. Wait until the active job finishes, then repeat the readiness check."),
        new Passage("worker/v1#retry","timeout upstream retry rerun approval","For an upstream timeout, validate the original input and check the executor lease. A passing dry run still requires operator approval before execution.")
    );
    private static final Pattern VERSION = Pattern.compile("\\b(?:v|version\\s*)(\\d+)\\b");
    static List<Evidence> search(String question) {
        String query = question.toLowerCase(Locale.ROOT);
        Matcher version = VERSION.matcher(query);
        String selected = version.find() ? "v" + version.group(1) : null;
        Set<String> terms = new HashSet<>(Arrays.asList(query.split("[^a-z0-9_]+")));
        record Match(Passage doc, long score) {}
        return DOCS.stream()
            .filter(d -> selected == null || d.source().contains("/" + selected + "#"))
            .filter(d -> selected != null || !d.source().contains("update/v1"))
            .filter(d -> !query.contains("extract") || d.source().startsWith("account-extract"))
            .filter(d -> !query.contains("update") || !d.source().startsWith("account-extract"))
            .map(d -> new Match(d, Arrays.stream(d.keywords().split(" ")).filter(terms::contains).count()))
            .filter(m -> m.score() > 0).sorted(Comparator.comparingLong(Match::score).reversed())
            .limit(2).map(m -> new Evidence(m.doc().source(), m.doc().text())).toList();
    }
    record Job(String status, String reason, String guide, boolean validInput, boolean busy) {}
    static final Map<String, Job> JOBS = Map.of(
        "1042", new Job("FAILED","Row 3 has no reason.","account-status-update/v2#csv",false,false),
        "1043", new Job("FAILED","Upstream timeout.","worker/v1#retry",true,false),
        "1044", new Job("WAITING","Executor lease held by another job.","worker/v1#lease",true,true));
    static Evidence inspect(String id, boolean dryRun) {
        Job job = JOBS.get(id);
        if (job == null) return new Evidence("job/" + id, "No matching demo job.");
        String result = "Job " + id + " is " + job.status() + ". " + job.reason();
        if (dryRun) {
            String readiness = job.busy() ? "BUSY" : job.validInput() ? "READY FOR APPROVAL" : "BLOCKED";
            result += " Dry run: " + readiness + ". No changes applied.";
        }
        return new Evidence("job/" + id, result);
    }
    static List<Evidence> jobEvidence(String id, boolean dryRun) {
        List<Evidence> evidence = new ArrayList<>();
        evidence.add(inspect(id, dryRun));
        Job job = JOBS.get(id);
        if (job != null) DOCS.stream().filter(d -> d.source().equals(job.guide()))
            .forEach(d -> evidence.add(new Evidence(d.source(), d.text())));
        return evidence;
    }
}
