package datawave.test;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
            fail("Failed to delete/create table");
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
                fail("Failed to delete: " + tableName, e);
                throw new RuntimeException("Failed to delete table: " + tableName, e);
            }
        }
    }

    /**
     * When a property is removed from accumulo the test must wait until the change is persisted in ZooKeeper. Otherwise, test may execute with an incorrect set
     * of assumptions.
     *
     * @param tops
     *            an instance of {@link TableOperations}
     * @param tableName
     *            the table name
     * @param property
     *            the property to remove
     */
    public static void waitForPropertyRemoval(TableOperations tops, String tableName, String property) {
        waitForPropertyRemoval(tops, tableName, Collections.singletonList(property));
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
     *            the list of removed properties
     */
    public static void waitForPropertyRemoval(TableOperations tops, String tableName, List<String> properties) {
        try {
            long start = System.currentTimeMillis();
            boolean allRemoved = false;
            while (!allRemoved) {
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
            log.trace("removed {} properties in {} ms", properties.size(), elapsed);
        } catch (AccumuloException | TableNotFoundException e) {
            fail("Exception while verifying property removal");
            throw new RuntimeException("Exception while verifying property removal", e);
        }
    }

    /**
     * When a property is added to accumulo the test must wait until the change is persisted in ZooKeeper. Otherwise, test may execute with an incorrect set of
     * assumptions.
     *
     * @param tops
     *            an instance of {@link TableOperations}
     * @param tableName
     *            the table name
     * @param property
     *            the property to remove
     */
    public static void waitForPropertyAddition(TableOperations tops, String tableName, String property) {
        waitForPropertyAddition(tops, tableName, Collections.singletonList(property));
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
     *            the list of removed properties
     */
    public static void waitForPropertyAddition(TableOperations tops, String tableName, List<String> properties) {
        try {
            long start = System.currentTimeMillis();
            boolean allAdded = false;
            while (!allAdded) {
                List<String> additions = new ArrayList<>();
                Iterable<Map.Entry<String,String>> props = tops.getProperties(tableName);
                for (Map.Entry<String,String> prop : props) {
                    if (properties.contains(prop.getKey())) {
                        additions.add(prop.getKey());
                    }
                }
                Collections.sort(additions);
                Collections.sort(properties);
                allAdded = additions.equals(properties);
            }
            long elapsed = System.currentTimeMillis() - start;
            log.trace("added {} properties in {} ms", properties.size(), elapsed);
        } catch (AccumuloException | TableNotFoundException e) {
            fail("Exception while verifying property addition");
            throw new RuntimeException("Exception while verifying property addition", e);
        }
    }
}
