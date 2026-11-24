package example.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class MarkingDemoTest {
    @Test
    void combinedEnvelopeRetainsMetadataNotPresentInVisibility() {
        // Wire fixtures from the example.review v1 producer: TEAM/7 days and LEAD/30 days.
        // Both results project to the same visibility; only the envelope retains the different review interval.
        assertEquals(List.of("combined=example.review:v1:Ah4=", "visibility=TEAM&LEAD"), run("example.review", "1", "Ah4="));
        assertEquals(List.of("combined=example.review:v1:Agc=", "visibility=TEAM&LEAD"), run("example.review", "1", "AQc=", "Ah4="));
    }

    @Test
    void unsupportedVersionIsNotDowngradedToAVisibility() {
        assertThrows(IllegalArgumentException.class, () -> run("example.review", "2", "AQc="));
    }

    private static List<String> run(String... args) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(output, true, UTF_8)) {
            MarkingDemo.run(args, out);
        }
        return output.toString(UTF_8).lines().collect(Collectors.toList());
    }
}
