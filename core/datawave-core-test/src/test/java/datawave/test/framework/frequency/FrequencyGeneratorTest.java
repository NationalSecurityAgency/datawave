package datawave.test.framework.frequency;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.id.EventIdGenerator;

public interface FrequencyGeneratorTest {

    /**
     * Extending classes get their own specific implementations of a {@link EventIdGenerator}
     *
     * @return a FrequencyGenerator
     */
    EventIdGenerator getGenerator();

    /**
     * Exercise the {@link EventIdGenerator#generateCount(int)} method
     */
    void testGenerateCount();

    /**
     * Exercise the {@link EventIdGenerator#generateWithinBound(int)} method
     */
    void testGenerateWithinBound();

    /**
     * Exercise the {@link EventIdGenerator#generateCountWithinBound(int, int)} method
     */
    void testGenerateCountWithinBound();

    /**
     * A count of zero or less must produce no event ids. The default bound is {@link Integer#MAX_VALUE}, so a generator that adds an id before testing the
     * count runs until it exhausts the heap.
     */
    @Test
    default void testNonPositiveCount() {
        // assert against a small bound first so a regression fails here rather than exhausting the heap on the unbounded calls below
        assertEquals(List.of(), getGenerator().generateCountWithinBound(0, 100));
        assertEquals(List.of(), getGenerator().generateCount(0));
        assertEquals(List.of(), getGenerator().generateCount(-1));
    }
}
