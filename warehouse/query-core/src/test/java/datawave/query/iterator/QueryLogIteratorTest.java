package datawave.query.iterator;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.security.util.ScannerHelper;

/**
 * Unit test for {@link QueryLogIterator}.
 */
public class QueryLogIteratorTest {

    private static final String TABLE_NAME = "testTable";
    private static final String[] AUTHS = {"FOO", "BAR", "COB"};
    private final Value BLANK_VALUE = new Value(new byte[0]);
    private final Set<Authorizations> AUTHS_SET = Collections.singleton(new Authorizations(AUTHS));

    private AccumuloClient accumuloClient;

    @BeforeClass
    public static void beforeClass() throws Exception {
        File dir = new File(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource(".")).toURI());
        File targetDir = dir.getParentFile();
        System.setProperty("hadoop.home.dir", targetDir.getAbsolutePath());
    }

    @Before
    public void setUp() throws Exception {
        accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance(QueryLogIteratorTest.class.toString()));
        if (!accumuloClient.tableOperations().exists(TABLE_NAME)) {
            accumuloClient.tableOperations().create(TABLE_NAME);
        }

        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(1000L);
        try (BatchWriter writer = accumuloClient.createBatchWriter(TABLE_NAME, config)) {
            Mutation mutation = new Mutation("fieldA");
            mutation.put("boo", "a", BLANK_VALUE);
            mutation.put("boo", "b", BLANK_VALUE);
            mutation.put("foo", "a", BLANK_VALUE);
            mutation.put("foo", "c", BLANK_VALUE);
            writer.addMutation(mutation);
            writer.flush();
        } catch (MutationsRejectedException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        accumuloClient.tableOperations().deleteRows(TABLE_NAME, null, null);
    }

    @Test
    public void testLogging() throws TableNotFoundException {
        Logger.getRootLogger().setLevel(Level.INFO);

        Scanner scanner = ScannerHelper.createScanner(accumuloClient, TABLE_NAME, AUTHS_SET);
        scanner.setRange(new Range());

        IteratorSetting iteratorSetting = new IteratorSetting(10, QueryLogIterator.class);
        iteratorSetting.addOption(QueryOptions.QUERY_ID, "12345");
        scanner.addScanIterator(iteratorSetting);

        for (Map.Entry<Key,Value> entry : scanner) {
            // Do nothing here, we are examining logs only.
        }

        // Logs will print to console with a start/end for each method in the iterator.
    }
}
