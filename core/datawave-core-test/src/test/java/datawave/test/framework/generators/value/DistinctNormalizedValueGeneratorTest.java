package datawave.test.framework.generators.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;

public class DistinctNormalizedValueGeneratorTest {

    @Test
    public void testSkipsValuesCollidingAfterNormalization() {
        ValueGenerator<String> generator = DistinctNormalizedValueGenerator.of(PresetGenerator.of(List.of("Tt", "tt", "TT", "ab")), new LcNoDiacriticsType());
        assertEquals("Tt", generator.next());
        assertEquals("ab", generator.next());
    }

    /**
     * A normalizer that preserves case keeps values that differ only by case.
     */
    @Test
    public void testDistinctnessFollowsTheNormalizer() {
        ValueGenerator<String> generator = DistinctNormalizedValueGenerator.of(PresetGenerator.of(List.of("Tt", "Tt", "tt")), new NoOpType());
        assertEquals("Tt", generator.next());
        assertEquals("tt", generator.next());
    }

    /**
     * A delegate that cannot produce another distinct value fails rather than looping forever.
     */
    @Test
    public void testExhaustedValueSpaceFails() {
        ValueGenerator<String> generator = DistinctNormalizedValueGenerator.of(PresetGenerator.of(List.of("a", "A")), new LcNoDiacriticsType());
        assertEquals("a", generator.next());

        Exception e = assertThrows(IllegalStateException.class, generator::next);
        assertEquals("could not generate a value distinct after normalization in 1000 attempts; 1 distinct values already generated", e.getMessage());
    }
}
