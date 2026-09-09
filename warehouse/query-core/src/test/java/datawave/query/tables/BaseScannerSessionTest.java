package datawave.query.tables;

import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;

import datawave.microservice.query.Query;

/**
 * Interface for {@link ScannerSessionBuilder} tests, ensures that every implementation gets the same set of core tests
 */
public interface BaseScannerSessionTest<T> {

    /**
     * Verify an exception with appropriate message is thrown when a null client is passed to the builder
     */
    void testClientNotSet();

    /**
     * Verify an exception with appropriate message is thrown when a null table name is passed to the builder
     */
    void testTableNameNotSet();

    /**
     * Verify an exception with appropriate message is thrown when a null authorization set is passed to the builder
     */
    void testAuthorizationsNotSet();

    /**
     * Verify an exception with appropriate message is thrown when a zero size resource queue is specified
     */
    void testInvalidResourceQueueSize();

    /**
     * Verify an exception with appropriate message is thrown when a zero size result queue is specified
     */
    void testInvalidResultQueueSize();

    /**
     * Verify an exception with appropriate message is thrown when a null {@link Query} is passed to the builder
     */
    void testQueryNotSet();

    /**
     * Verify an exception with appropriate message is thrown when an invalid table name is passed to the builder
     */
    void testTableNotFound();

    /**
     * Verify default values for optional parameters
     */
    void testOptionalParameterDefaultValues();

    /**
     * Verify that the builder sets {@link ConsistencyLevel#IMMEDIATE}
     */
    void testConsistencyLevelImmediate();

    /**
     * Verify that the builder sets {@link ConsistencyLevel#EVENTUAL}
     */
    void testConsistencyLevelEventual();

    /**
     * Verify that the builder sets {@link ConsistencyLevel#IMMEDIATE} via the String value
     */
    void testConsistencyLevelImmediateViaString();

    /**
     * Verify that the builder sets {@link ConsistencyLevel#IMMEDIATE} via the String value
     */
    void testConsistencyLevelEventualViaString();

    /**
     * Verify that the builder throws an exception when an invalid {@link ConsistencyLevel} is provided
     */
    void testIncorrectConsistencyLevel();

    /**
     * Verify that the builder sets the <code>scan_type</code> execution hint
     */
    void testScanType();

    /**
     * Verify that the builder sets the <code>priority</code> execution hint
     */
    void testScanPriority();

    /**
     * Verify that the builder sets the <code>scan_type</code> and <code>priority</code> execution hint
     */
    void testScanTypeAndScanPriority();

    /**
     * Verify that the builder can enable stats
     */
    void testStatsEnabled();

    /**
     * Create a builder with the minimum required options
     *
     * @return a builder
     */
    T create();
}
