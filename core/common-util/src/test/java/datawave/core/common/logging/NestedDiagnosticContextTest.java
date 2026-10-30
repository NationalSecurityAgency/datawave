package datawave.core.common.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class NestedDiagnosticContextTest {

    @AfterEach
    void cleanUp() {
        while (!NestedDiagnosticContext.peek().isEmpty()) {
            NestedDiagnosticContext.pop();
        }
        MDC.clear();
    }

    @Test
    void restoresNestedContextAndLeavesUnrelatedMdcEntries() {
        MDC.put("request", "request-id");
        NestedDiagnosticContext.push("file");
        NestedDiagnosticContext.push("original-file");
        assertEquals("original-file", NestedDiagnosticContext.peek());
        assertEquals("file original-file", MDC.get("NDC"));

        assertEquals("original-file", NestedDiagnosticContext.pop());
        assertEquals("file", MDC.get("NDC"));
        assertEquals("file", NestedDiagnosticContext.pop());
        assertNull(MDC.get("NDC"));
        assertEquals("", NestedDiagnosticContext.peek());
        assertEquals("", NestedDiagnosticContext.pop());
        assertEquals("request-id", MDC.get("request"));
    }

    @Test
    void isolatesAndClearsContextOnReusedThreads() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            NestedDiagnosticContext.push("parent");
            executor.submit(() -> {
                assertEquals("", NestedDiagnosticContext.peek());
                assertNull(MDC.get("NDC"));
                NestedDiagnosticContext.push("worker");
                assertEquals("worker", MDC.get("NDC"));
                NestedDiagnosticContext.pop();
            }).get();
            assertEquals("", executor.submit(NestedDiagnosticContext::peek).get());
            assertNull(executor.submit(() -> MDC.get("NDC")).get());
            assertEquals("parent", MDC.get("NDC"));
        } finally {
            executor.shutdownNow();
        }
    }
}
