package example.marking.spi;

import java.util.Arrays;
import java.util.Objects;

/** DataWave can copy, compare and transport this value without the provider. Only the provider interprets the payload. */
public final class MarkingEnvelope {`
    private final String schemaId;
    private final int version;
    private final byte[] payload;

    public MarkingEnvelope(String schemaId, int version, byte[] payload) {
        if (Objects.requireNonNull(schemaId, "schemaId").isBlank() || version < 1) {
            throw new IllegalArgumentException("A nonblank schema ID and positive version are required");
        }
        this.schemaId = schemaId;
        this.version = version;
        this.payload = Objects.requireNonNull(payload, "payload").clone();
    }

    public String schemaId() {
        return schemaId;
    }

    public int version() {
        return version;
    }

    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MarkingEnvelope)) {
            return false;
        }
        MarkingEnvelope that = (MarkingEnvelope) other;
        return version == that.version && schemaId.equals(that.schemaId) && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(schemaId, version) + Arrays.hashCode(payload);
    }
}
