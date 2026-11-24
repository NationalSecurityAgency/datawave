package example.marking.provider;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import example.marking.spi.MarkingEnvelope;
import example.marking.spi.MarkingProvider;

class ReviewMarkingProviderTest {
    private final MarkingProvider provider = MarkingProvider.load();

    private static MarkingEnvelope review(int tier, int days) {
        return new MarkingEnvelope("example.review", 1, new byte[] {(byte) tier, (byte) days});
    }

    @Test
    void projectsOnlyTheAccessConstraints() {
        assertArrayEquals("TEAM".getBytes(StandardCharsets.UTF_8), provider.toColumnVisibility(review(1, 255)).getExpression());
        assertArrayEquals("TEAM&LEAD".getBytes(StandardCharsets.UTF_8), provider.toColumnVisibility(review(2, 1)).getExpression());
    }

    @Test
    void combinesTheHighestTierAndShortestUnsignedIntervalIndependently() {
        MarkingEnvelope lead = review(2, 255);
        MarkingEnvelope team = review(1, 128);
        MarkingEnvelope urgent = review(1, 1);
        assertEquals(lead, provider.combine(List.of(lead)));
        assertEquals(review(2, 128), provider.combine(List.of(lead, team)));
        assertEquals(review(2, 1), provider.combine(List.of(lead, team, urgent)));
        assertEquals(review(2, 1), provider.combine(List.of(urgent, team, lead)));
        assertEquals(team, provider.combine(List.of(team, team)));
    }

    private static Stream<MarkingEnvelope> invalidEnvelopes() {
        return Stream.of(new MarkingEnvelope("other.review", 1, new byte[] {1, 1}), new MarkingEnvelope("example.review", 2, new byte[] {1, 1}),
                        new MarkingEnvelope("example.review", 1, new byte[0]), new MarkingEnvelope("example.review", 1, new byte[] {1}),
                        new MarkingEnvelope("example.review", 1, new byte[] {1, 1, 1}), review(0, 1), review(3, 1), review(255, 1), review(1, 0));
    }

    @ParameterizedTest
    @MethodSource("invalidEnvelopes")
    void rejectsInvalidInputEvenAfterTheStrictestMarking(MarkingEnvelope invalid) {
        assertThrows(IllegalArgumentException.class, () -> provider.toColumnVisibility(invalid));
        assertThrows(IllegalArgumentException.class, () -> provider.combine(List.of(invalid)));
        assertThrows(IllegalArgumentException.class, () -> provider.combine(List.of(review(2, 1), invalid)));
    }

    @Test
    void rejectsMissingInput() {
        assertThrows(IllegalArgumentException.class, () -> provider.combine(List.of()));
        assertThrows(NullPointerException.class, () -> provider.combine(null));
        assertThrows(NullPointerException.class, () -> provider.combine(Arrays.asList(review(2, 1), null)));
        assertThrows(NullPointerException.class, () -> provider.toColumnVisibility(null));
    }
}
