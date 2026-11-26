package dev.skypil;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class WorkflowTest {
    @Test void selectsRequestedVersion() {
        assertTrue(Application.guided("columns for account-status-update v1").answer().contains("account_id and status"));
        assertTrue(Application.guided("columns for account-status-update v2").answer().contains("reason"));
        assertTrue(Application.guided("columns for v99").evidence().isEmpty());
    }
    @Test void checksEveryReadinessOutcome() {
        assertTrue(Application.guided("rerun 1042").answer().contains("BLOCKED"));
        assertTrue(Application.guided("rerun 1043").answer().contains("READY FOR APPROVAL"));
        assertTrue(Application.guided("rerun 1044").answer().contains("BUSY"));
    }
    @Test void handlesMissingEvidence() {
        assertTrue(Application.guided("inspect job 9999").answer().contains("No matching"));
        assertTrue(Application.guided("rerun my job").answer().contains("Which job"));
        assertTrue(Application.guided("what is the weather").evidence().isEmpty());
    }
}
