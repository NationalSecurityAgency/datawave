package datawave.test.framework.frequency;

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
}
