package example.consumer;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import example.marking.spi.MarkingEnvelope;
import example.marking.spi.MarkingProvider;

/** Example DataWave caller: accepts envelopes from a producer without importing its marking type, schema or codec. */
public final class MarkingDemo {
    private MarkingDemo() {}

    public static void main(String[] args) {
        run(args, System.out);
    }

    static void run(String[] args, PrintStream out) {
        if (args.length < 3) {
            throw new IllegalArgumentException("Usage: MarkingDemo SCHEMA VERSION BASE64_PAYLOAD [BASE64_PAYLOAD ...]");
        }
        String schema = args[0];
        int version = Integer.parseInt(args[1]);
        List<MarkingEnvelope> received = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            received.add(new MarkingEnvelope(schema, version, Base64.getDecoder().decode(args[i])));
        }

        MarkingProvider policy = MarkingProvider.load();
        MarkingEnvelope combined = policy.combine(received);
        String visibility = new String(policy.toColumnVisibility(combined).getExpression(), StandardCharsets.UTF_8);
        out.printf("combined=%s:v%d:%s%n", combined.schemaId(), combined.version(), Base64.getEncoder().encodeToString(combined.payload()));
        out.println("visibility=" + visibility);
    }
}
