package example.marking.provider;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

import org.apache.accumulo.core.security.ColumnVisibility;

import example.marking.spi.MarkingEnvelope;
import example.marking.spi.MarkingProvider;

/** The example.review v1 policy. Its two payload bytes are the tier code and unsigned review interval in days. */
public final class ReviewMarkingProvider implements MarkingProvider {
    private static final String SCHEMA = "example.review";
    private static final int VERSION = 1;

    @Override
    public MarkingEnvelope combine(Collection<MarkingEnvelope> inputs) {
        Iterator<MarkingEnvelope> it = Objects.requireNonNull(inputs, "inputs").iterator();
        if (!it.hasNext()) {
            throw new IllegalArgumentException("At least one review marking is required");
        }
        Review result = Review.decode(it.next());
        while (it.hasNext()) {
            result = result.combine(Review.decode(it.next()));
        }
        return result.encode();
    }

    @Override
    public ColumnVisibility toColumnVisibility(MarkingEnvelope input) {
        return new ColumnVisibility(Review.decode(input).tier.visibility);
    }

    private enum Tier {
        TEAM(1, "TEAM"), LEAD(2, "TEAM&LEAD");

        private final int code;
        private final String visibility;

        Tier(int code, String visibility) {
            this.code = code;
            this.visibility = visibility;
        }

        private static Tier decode(int code) {
            for (Tier tier : values()) {
                if (tier.code == code) {
                    return tier;
                }
            }
            throw new IllegalArgumentException("Unknown review tier: " + code);
        }
    }

    private static final class Review {
        private final Tier tier;
        private final int reviewWithinDays;

        private Review(Tier tier, int reviewWithinDays) {
            if (reviewWithinDays < 1 || reviewWithinDays > 255) {
                throw new IllegalArgumentException("Review interval must be between 1 and 255 days");
            }
            this.tier = tier;
            this.reviewWithinDays = reviewWithinDays;
        }

        private static Review decode(MarkingEnvelope envelope) {
            Objects.requireNonNull(envelope, "envelope");
            if (!SCHEMA.equals(envelope.schemaId()) || envelope.version() != VERSION) {
                throw new IllegalArgumentException("Unsupported review schema or version: " + envelope.schemaId() + " v" + envelope.version());
            }
            byte[] payload = envelope.payload();
            if (payload.length != 2) {
                throw new IllegalArgumentException("Review payload must have exactly two bytes");
            }
            return new Review(Tier.decode(Byte.toUnsignedInt(payload[0])), Byte.toUnsignedInt(payload[1]));
        }

        private Review combine(Review other) {
            Tier higher = tier.code >= other.tier.code ? tier : other.tier;
            return new Review(higher, Math.min(reviewWithinDays, other.reviewWithinDays));
        }

        private MarkingEnvelope encode() {
            return new MarkingEnvelope(SCHEMA, VERSION, new byte[] {(byte) tier.code, (byte) reviewWithinDays});
        }
    }
}
