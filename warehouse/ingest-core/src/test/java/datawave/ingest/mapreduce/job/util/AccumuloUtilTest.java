package datawave.ingest.mapreduce.job.util;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;

public class AccumuloUtilTest {
    private InMemoryInstance instance;
    private AccumuloClient accumuloClient;

    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        instance = new InMemoryInstance(this.getClass().toString());
        accumuloClient = new InMemoryAccumuloClient("root", instance);

        // test1 will act as a simulated metadata table
        accumuloClient.tableOperations().create("test1");
        // test2 is just another table
        accumuloClient.tableOperations().create("test2");
        String tableId = accumuloClient.tableOperations().tableIdMap().get("test1");
        String tableId2 = accumuloClient.tableOperations().tableIdMap().get("test2");
        BatchWriter writer = accumuloClient.createBatchWriter("test1");
        Mutation m = new Mutation(tableId + ";a");
        m.put("file", "rfile1", new Value());
        m.put("file", "rfile2", new Value());
        m.put("file", "rfile3", new Value());
        writer.addMutation(m);
        m = new Mutation(tableId + ";b");
        m.put("file", "rfile4", new Value());
        writer.addMutation(m);
        m = new Mutation(tableId + ";c");
        m.put("file", "rfile5", new Value());
        m.put("file", "rfile6", new Value());
        writer.addMutation(m);
        m = new Mutation(tableId2 + "<");
        m.put("file", "rfile7", new Value());
        writer.addMutation(m);

        accumuloClient.tableOperations().flush("test1");
    }

    @After
    public void shutdown() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        accumuloClient.tableOperations().delete("test1");
        accumuloClient.tableOperations().delete("test2");
        accumuloClient.close();
    }

    @Test
    public void getFilesFromMetadataBySplitTest_aOnly() throws AccumuloException {
        List<Map.Entry<String,List<String>>> splits = AccumuloUtil.getFilesFromMetadataBySplit(accumuloClient, "test1", "test1", "a", "a");
        Assert.assertTrue(splits.size() == 1);
        Assert.assertTrue(splits.get(0).getKey().equals("a"));
        Assert.assertTrue(splits.get(0).getValue().size() == 3);
    }

    @Test
    public void getFilesFromMetadataBySplitTest_nobound() throws AccumuloException {
        List<Map.Entry<String,List<String>>> splits = AccumuloUtil.getFilesFromMetadataBySplit(accumuloClient, "test1", "test1", null, null);
        Assert.assertTrue(splits.size() == 3);
        Assert.assertTrue(splits.get(0).getKey().equals("a"));
        Assert.assertTrue(splits.get(0).getValue().size() == 3);
        Assert.assertTrue(splits.get(1).getKey().equals("b"));
        Assert.assertTrue(splits.get(1).getValue().size() == 1);
        Assert.assertTrue(splits.get(2).getKey().equals("c"));
        Assert.assertTrue(splits.get(2).getValue().size() == 2);
    }

    @Test
    public void getFilesFromMetadataBySplitTest_defaultTablet() throws AccumuloException {
        List<Map.Entry<String,List<String>>> splits = AccumuloUtil.getFilesFromMetadataBySplit(accumuloClient, "test1", "test2", null, null);
        Assert.assertTrue(splits.size() == 1);
        Assert.assertTrue(splits.get(0).getKey(), splits.get(0).getKey().equals(""));
        Assert.assertTrue(splits.get(0).getValue().size() == 1);
    }

    @Test(expected = AccumuloException.class)
    public void getFilesFromMetadataBySplitTest_nonExistentMetadata() throws AccumuloException {
        AccumuloUtil.getFilesFromMetadataBySplit(accumuloClient, "test3", "test2", null, null);
    }

    @Test(expected = AccumuloException.class)
    public void getFilesFromMetadataBySplitTest_nonExistentTable() throws AccumuloException {
        AccumuloUtil.getFilesFromMetadataBySplit(accumuloClient, "test3", null, null);
    }
}
