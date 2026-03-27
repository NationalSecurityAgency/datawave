package datawave.core.common.connection;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.junit.Before;
import org.junit.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;

public class AccumuloTableInfoFetcherTest {

    private AccumuloTableInfoFetcher fetcher;

    @Before
    public void setup() throws Exception {
        InMemoryInstance instance = new InMemoryInstance();
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);
        fetcher = new AccumuloTableInfoFetcher(client);
    }

    @Test(expected = AccumuloException.class)
    public void testGetMajorCompactionCountThrowsWithoutCluster() throws Exception {
        // The Thrift-based implementation requires a live Accumulo cluster.
        // With InMemoryAccumuloClient, the underlying ClientContext cannot connect
        // to ZooKeeper, so this should throw AccumuloException.
        fetcher.getMajorCompactionCount();
    }
}
