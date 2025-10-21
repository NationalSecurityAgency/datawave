package datawave.test.index;

import org.junit.jupiter.api.Test;

/**
 * This class contains tests that should be implemented by each extending class
 * <p>
 * {@link IndexConversionUtils} contains all boilerplate code to support declarative style tests
 */
public interface IndexConversionTests {

    /**
     * A test case that verifies duplicate keys are correctly collapsed into a single key
     */
    @Test
    void testDuplicateKeysCollapse();

    /**
     * A test case that verifies key order is preserved with variable values
     */
    @Test
    void testVariableValues();

    /**
     * A test case that verifies key order is preserved with variable fields
     */
    @Test
    void testVariableFields();

    /**
     * A test that verifies key order is preserved with variable days
     */
    @Test
    void testVariableDays();

    /**
     * A test that verifies key order is preserved with variable shards
     */
    @Test
    void testVariableShards();

    /**
     * A test that verifies key order is preserved with variable datatypes
     */
    @Test
    void testVariableDatatypes();

    /**
     * A test that verifies key order is preserved with variable visibilities
     */
    @Test
    void testVariableVisibilities();

    /**
     * A test that verifies key order is preserved with variable uids
     */
    @Test
    void testVariableUids();

    /**
     * A test that verifies key order is preserved with variable values, fields, days, shards, visibilities and uids.
     */
    @Test
    void testPermutationsAndCompareTables();

    /**
     * A test that verifies the iterator can handle both old and new key structures. This is a key requirement for a 'clone-and-compact' strategy.
     */
    @Test
    void testMixOfKeyStructures();

    /**
     * A test that verifies proper handling of keys with the delete flag set to true.
     */
    @Test
    void testDeletes();
}
