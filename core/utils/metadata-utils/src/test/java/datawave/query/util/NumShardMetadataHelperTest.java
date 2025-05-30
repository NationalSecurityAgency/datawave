package datawave.query.util;


import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import java.util.List;
import java.util.Objects;


import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;

class NumShardMetadataHelperTest {

    public static final Text NUM_SHARDS = new Text("num_shards");
    public static final Text NUM_SHARDS_CF = new Text("ns");

    private static final String TABLE_METADATA = "testMetadataTable";
    private AccumuloClient accumuloClient;

    @BeforeAll
    static void beforeAll() throws URISyntaxException {
        File dir = new File(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource(".")).toURI());
        File targetDir = dir.getParentFile();
        System.setProperty("hadoop.home.dir", targetDir.getAbsolutePath());
    }

    @BeforeEach
    public void setup() throws TableNotFoundException, AccumuloException, TableExistsException, AccumuloSecurityException {
        accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance(MetadataHelperTest.class.toString()));
        if (!accumuloClient.tableOperations().exists(TABLE_METADATA)) {
            accumuloClient.tableOperations().create(TABLE_METADATA);
        }
    }

    @AfterEach
    void tearDown() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        accumuloClient.tableOperations().delete(TABLE_METADATA);
    }

    @Test
    public void testSingleEntry() throws TableNotFoundException, AccumuloException, AccumuloSecurityException, IOException {

        // write a couple of entries for multiple numshards
        Mutation m = new Mutation(NUM_SHARDS);
        m.put(NUM_SHARDS_CF.toString(), "20170101_13", "");

        writeMutation(m);

        List<String> nsEntries = NumShardMetadataHelper.getNumShardEntries(accumuloClient, TABLE_METADATA, NUM_SHARDS, NUM_SHARDS_CF);
        Assertions.assertEquals(1, nsEntries.size());

    }

    @Test
    public void testMultipleEntries() throws TableNotFoundException, AccumuloException, AccumuloSecurityException, IOException {

        // write a couple of entries for multiple numshards
        Mutation m = new Mutation(NUM_SHARDS);
        m.put(NUM_SHARDS_CF.toString(), "20170101_13", "");

        writeMutation(m);

        m = new Mutation(NUM_SHARDS);
        m.put(NUM_SHARDS_CF.toString(), "20171101_17", "");

        writeMutation(m);

        List<String> nsEntries = NumShardMetadataHelper.getNumShardEntries(accumuloClient, TABLE_METADATA, NUM_SHARDS, NUM_SHARDS_CF);
        Assertions.assertEquals(2, nsEntries.size());

    }

    @Test
    public void testMultipleWithInvalidEntries() throws TableNotFoundException, AccumuloException, AccumuloSecurityException, IOException {

        // write a couple of entries for multiple numshards
        Mutation m = new Mutation(NUM_SHARDS);
        m.put(NUM_SHARDS_CF.toString(), "20170101_13", "");

        writeMutation(m);

        m = new Mutation(NUM_SHARDS);
        m.put(NUM_SHARDS_CF.toString(), "20171101_17", "");

        writeMutation(m);

        // invalid entry and should be ignored
        m = new Mutation(NUM_SHARDS);
        m.put(NUM_SHARDS_CF + "blah", "20171102_19", "");

        writeMutation(m);

        List<String> nsEntries = NumShardMetadataHelper.getNumShardEntries(accumuloClient, TABLE_METADATA, NUM_SHARDS, NUM_SHARDS_CF);
        Assertions.assertEquals(2, nsEntries.size());

    }

    private void writeMutation(String row, String columnFamily, String columnQualifier, Value value) throws TableNotFoundException {
        Mutation mutation = new Mutation(row);
        mutation.put(columnFamily, columnQualifier, value);
        writeMutation(mutation);
    }

    private void writeMutation(Mutation m) throws TableNotFoundException {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(0);
        try (BatchWriter writer = accumuloClient.createBatchWriter(TABLE_METADATA, config)) {
            writer.addMutation(m);
            writer.flush();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

}
