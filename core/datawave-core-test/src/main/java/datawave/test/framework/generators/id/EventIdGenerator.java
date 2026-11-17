package datawave.test.framework.generators.id;

import java.util.List;

/**
 * Interface that allows callers to generate a list of event ids.
 * <p>
 * A starting offset can be provided.
 * <p>
 * Critically, all generators begin counting at integer value 1
 */
public interface EventIdGenerator {

    /**
     * Configure an offset. The meaning of the offset is specific to the implementation, so see the implementing class. For example, a
     * {@link ModuloEventIdGenerator} with modulo 2 and offset 1 generates the values 1, 3, 5, etc, while a {@link SequentialEventIdGenerator} with offset 1
     * generates the values 2, 3, 4, etc
     *
     * @param offset
     *            the offset
     */
    void setOffset(int offset);

    /**
     * Generate the first N event ids
     *
     * @param count
     *            the number of ids to generate
     * @return the first N event ids
     */
    List<Integer> generateCount(int count);

    /**
     * Generate all event ids that are less than or equal to the provided bound
     *
     * @param bound
     *            the upper bound
     * @return all event ids less than or equal to the bound
     */
    List<Integer> generateWithinBound(int bound);

    /**
     * Generate the first N event ids that are less than or equal to the provided bound
     *
     * @param count
     *            the number of event ids to create
     * @param bound
     *            the upper bound
     * @return the first N event ids within the bound
     */
    List<Integer> generateCountWithinBound(int count, int bound);
}
