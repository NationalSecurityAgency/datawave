package datawave.test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of useful utilities for tests that rely on {@link MiniAccumuloCluster}.
 */
public class MacTestUtil {

    private static final Logger log = LoggerFactory.getLogger(MacTestUtil.class);

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
        try {
            int cycles = 0;
            boolean allRemoved = false;
            long start = System.currentTimeMillis();
            while (!allRemoved) {
                cycles++;
                allRemoved = true;
                Iterable<Map.Entry<String,String>> props = tops.getProperties(tableName);
                for (Map.Entry<String,String> prop : props) {
                    if (properties.contains(prop.getKey())) {
                        allRemoved = false;
                        break;
                    }
                }
            }
            long elapsed = System.currentTimeMillis() - start;
            log.trace("removed {} properties in {} ms and {} cycles", properties.size(), elapsed, cycles);
        } catch (AccumuloException | TableNotFoundException e) {
            throw new RuntimeException("Exception while verifying property removal", e);
        }
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
        waitForPropertyAddition(tops, tableName, properties.keySet());
    }

    /**
     * When a property is added to accumulo the test must wait until the change is persisted in ZooKeeper. Otherwise, test may execute with an incorrect set of
     * assumptions.
     *
     * @param tops
     *            an instance of {@link TableOperations}
     * @param tableName
     *            the table name
     * @param properties
     *            the set of removed properties
     */
    private static void waitForPropertyAddition(TableOperations tops, String tableName, Set<String> properties) {
        try {
            int cycles = 0;
            boolean allAdded = false;
            long start = System.currentTimeMillis();
            while (!allAdded) {
                cycles++;
                Set<String> additions = new HashSet<>();
                Iterable<Map.Entry<String,String>> props = tops.getProperties(tableName);
                for (Map.Entry<String,String> prop : props) {
                    if (properties.contains(prop.getKey())) {
                        additions.add(prop.getKey());
                    }
                }
                allAdded = additions.equals(properties);
            }
            long elapsed = System.currentTimeMillis() - start;
            log.trace("added {} properties in {} ms and {} cycles", properties.size(), elapsed, cycles);
        } catch (AccumuloException | TableNotFoundException e) {
            throw new RuntimeException("Exception while verifying property addition", e);
        }
    }
}
