package datawave.test.framework.util;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryTableOperations;

/**
 * A collection of useful utilities for tests that rely on MiniAccumuloCluster.
 */
public class MacTestUtil {

    private static final Logger log = LoggerFactory.getLogger(MacTestUtil.class);

    /** How long a table property change is given to persist before the wait is abandoned */
    private static final Duration PROPERTY_WAIT_TIMEOUT = Duration.ofSeconds(30);

    /** How long to sleep between polls while waiting on a table property change */
    private static final Duration PROPERTY_POLL_INTERVAL = Duration.ofMillis(25);

    private MacTestUtil() {
        // enforce static access
    }

    /**
     * Create or recreate the given table. This ensures that each test method has a fresh table.
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     */
    public static void createOrRecreate(TableOperations tops, String tableName) {
        try {
            if (tops.exists(tableName)) {
                tops.delete(tableName);
            }
            tops.create(tableName);
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException e) {
            throw new RuntimeException("Failed to delete/create table", e);
        }
    }

    /**
     * Delete the given table.
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     */
    public static void deleteTable(TableOperations tops, String tableName) {
        if (tops.exists(tableName)) {
            try {
                tops.delete(tableName);
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
                throw new RuntimeException("Failed to delete table: " + tableName, e);
            }
        }
    }

    /**
     * Compact the given table
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table to compact
     */
    public static void compactTable(TableOperations tops, String tableName) {
        if (tops instanceof InMemoryTableOperations) {
            log.warn("cannot compact {} via InMemoryTableOperations", tableName);
            return;
        }

        if (tops.exists(tableName)) {
            try {
                Text start = new Text("");
                Text end = new Text("~");
                tops.compact(tableName, start, end, true, true);
                log.info("compacted table: {}", tableName);
            } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Remove a set of properties and wait for the changes to persist in ZooKeeper
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     * @param properties
     *            the set of property names to remove
     */
    public static void removePropertiesAndWait(TableOperations tops, String tableName, Set<String> properties) {
        for (String prop : properties) {
            try {
                tops.removeProperty(tableName, prop);
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException("Failed to add property", e);
            }
        }
        waitForPropertyRemoval(tops, tableName, properties);
    }

    /**
     * When a property is removed from accumulo the test must wait until the change is persisted in ZooKeeper. Otherwise, test may execute with an incorrect set
     * of assumptions.
     *
     * @param tops
     *            an instance of {@link TableOperations}
     * @param tableName
     *            the table name
     * @param properties
     *            the set of removed properties
     */
    private static void waitForPropertyRemoval(TableOperations tops, String tableName, Set<String> properties) {
        waitForProperties(tableName, properties, "removed", () -> {
            Set<String> outstanding = new HashSet<>();
            for (Map.Entry<String,String> prop : tops.getProperties(tableName)) {
                if (properties.contains(prop.getKey())) {
                    outstanding.add(prop.getKey());
                }
            }
            return outstanding;
        });
    }

    /**
     * Add a set of properties and wait until the changes are persisted in ZooKeeper.
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     * @param properties
     *            the map of option key-value pairs
     */
    public static void addPropertiesAndWait(TableOperations tops, String tableName, Map<String,String> properties) {
        for (Map.Entry<String,String> prop : properties.entrySet()) {
            try {
                tops.setProperty(tableName, prop.getKey(), prop.getValue());
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException("Failed to add property", e);
            }
        }
        waitForPropertyAddition(tops, tableName, properties);
    }

    /**
     * When a property is added to accumulo the test must wait until the change is persisted in ZooKeeper. Otherwise, test may execute with an incorrect set of
     * assumptions.
     * <p>
     * A property being updated is already present under its old value, so the observed value is compared against the requested one - waiting on key presence
     * alone would return as soon as the key exists and leave the test reading the previous value.
     *
     * @param tops
     *            an instance of {@link TableOperations}
     * @param tableName
     *            the table name
     * @param properties
     *            the map of added property key-value pairs
     */
    private static void waitForPropertyAddition(TableOperations tops, String tableName, Map<String,String> properties) {
        waitForProperties(tableName, properties.keySet(), "added", () -> {
            Set<String> outstanding = new HashSet<>(properties.keySet());
            for (Map.Entry<String,String> prop : tops.getProperties(tableName)) {
                if (properties.containsKey(prop.getKey()) && Objects.equals(properties.get(prop.getKey()), prop.getValue())) {
                    outstanding.remove(prop.getKey());
                }
            }
            return outstanding;
        });
    }

    /**
     * Resolves the properties that have not yet reached the desired state. Declared separately from {@link java.util.function.Supplier} so implementations can
     * propagate the checked exceptions thrown by {@link TableOperations#getProperties(String)}.
     */
    @FunctionalInterface
    private interface PropertyProbe {
        Set<String> outstanding() throws AccumuloException, TableNotFoundException;
    }

    /**
     * Poll until every property has reached the desired state, sleeping between attempts so a slow change does not spin a core.
     * <p>
     * A property change that never lands is a test bug (a malformed key, or a table that does not accept it) rather than something to wait out forever, so the
     * poll is bounded by {@link #PROPERTY_WAIT_TIMEOUT} and the resulting failure names the properties still outstanding.
     *
     * @param tableName
     *            the table name, used in log and failure messages
     * @param properties
     *            the properties being waited on
     * @param action
     *            a past-tense description of the change, e.g. {@code added}
     * @param probe
     *            resolves the properties that have not yet reached the desired state
     */
    private static void waitForProperties(String tableName, Set<String> properties, String action, PropertyProbe probe) {
        try {
            long start = System.nanoTime();
            long deadline = start + PROPERTY_WAIT_TIMEOUT.toNanos();
            int cycles = 1;

            Set<String> outstanding = probe.outstanding();
            while (!outstanding.isEmpty()) {
                if (System.nanoTime() - deadline >= 0) {
                    throw new IllegalStateException("Timed out after " + PROPERTY_WAIT_TIMEOUT.toMillis() + " ms waiting for " + properties.size()
                                    + " properties to be " + action + " on table " + tableName + "; still outstanding: " + new TreeSet<>(outstanding));
                }
                pausePolling();
                cycles++;
                outstanding = probe.outstanding();
            }

            long elapsed = (System.nanoTime() - start) / 1_000_000L;
            log.trace("{} {} properties in {} ms and {} cycles", action, properties.size(), elapsed, cycles);
        } catch (AccumuloException | TableNotFoundException e) {
            throw new RuntimeException("Exception while verifying properties were " + action, e);
        }
    }

    private static void pausePolling() {
        try {
            Thread.sleep(PROPERTY_POLL_INTERVAL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for a table property change", e);
        }
    }
}
