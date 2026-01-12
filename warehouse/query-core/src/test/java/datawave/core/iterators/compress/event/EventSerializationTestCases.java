package datawave.core.iterators.compress.event;

import java.io.IOException;

/**
 * Ensure that all test cases are covered for the following
 * <ul>
 * <li>{@link EventSerializationTest}</li>
 * <li>{@link EventSerializationImaTest}</li>
 * <li>{@link EventSerializationMacTest}</li>
 * </ul>
 */
public interface EventSerializationTestCases {

    /**
     * Verify correct handling of field index keys, event keys, and term frequency keys.
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testOneOfEachKeyType() throws IOException;

    /**
     * Verify correct handling of different visibilities within the same event.
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testDifferentVisibilities() throws IOException;

    /**
     * Verify correct handling of different timestamps within the same event
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testDifferentTimestamps() throws IOException;

    /**
     * Verify correct handling of delete flags
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testDifferentDeleteFlags() throws IOException;

    /**
     * Verify full event is handled correctly
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testEvent() throws IOException;

    /**
     * Verify full event with grouping context is handled correctly
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testEventWithGroupingContext() throws IOException;

    /**
     * Verify full TLD event is handled correctly
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testTldEvent() throws IOException;

    /**
     * Verify full TLD event with grouping context is handled correctly
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testTldEventWithGroupingContext() throws IOException;

    /**
     * Simulate loading compressed data into an uncompressed key group. In this example data at rest is uncompressed and the compression iterator was added
     * later, resulting in compressed ingest.
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testLatentLoadOfCompressedData() throws IOException;

    /**
     * Simulate loading uncompressed data into a compressed key group. In this example data at rest was compressed and then the compression iterator was
     * removed, resulting in uncompressed ingest.
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testLatentLoadOfUncompressedData() throws IOException;

    /**
     * Simulate loading compressed data into an uncompressed key group with the same partition id.
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testLatentLoadOfCompressedDataWithSamePartitionId() throws IOException;

    /**
     * Simulate loading uncompressed data into a compressed key group with the same partition id
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testLatentLoadOfUncompressedDataWithSamePartitionId() throws IOException;

    /**
     * Simulate a large event that triggers the compression threshold. The resulting compressed marker should include the compression algorithm, the
     * serialization version and the number of keys serialized in the value.
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testLargeEventTriggersCompressionOfSerializedData() throws IOException;

    /**
     * An event is only compressed after lowing the compression threshold.
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testLargeEventCompressedAfterLoweringTheCompressionThreshold() throws IOException;

    /**
     * An event is compressed due to the compression threshold, then is uncompressed after increasing the threshold.
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testLargeEventUncompressedAfterIncreasingTheCompressionThreshold() throws IOException;

    /**
     * An event is compressed using one algorithm, then is compressed using a different algorithm.
     *
     * @throws IOException
     *             if something goes wrong
     */
    void testLargeEventCompressionAlgorithmChanges() throws IOException;
}
