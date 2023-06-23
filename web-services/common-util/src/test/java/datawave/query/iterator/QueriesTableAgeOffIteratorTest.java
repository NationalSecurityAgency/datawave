package datawave.query.iterator;

import java.util.EnumSet;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import org.apache.accumulo.core.client.AccumuloClient;
import org.junit.Assert;
import org.junit.Test;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;

public class QueriesTableAgeOffIteratorTest {

    private static final String TABLE_NAME = "test";

    @Test
    public void testAgeOffIterator() throws Exception {
        AccumuloClient client = new InMemoryAccumuloClient("root", new InMemoryInstance());

        client.tableOperations().create(TABLE_NAME);
        IteratorSetting iteratorCfg = new IteratorSetting(19, "ageoff", QueriesTableAgeOffIterator.class);
        client.tableOperations().attachIterator(TABLE_NAME, iteratorCfg, EnumSet.allOf(IteratorScope.class));

        long now = System.currentTimeMillis();
        // Write in a couple of keys with varying timestamps
        BatchWriter writer = client.createBatchWriter(TABLE_NAME,
                        new BatchWriterConfig().setMaxLatency(30, TimeUnit.MILLISECONDS).setMaxMemory(1024L).setMaxWriteThreads(1));

        Mutation m1 = new Mutation("row1");
        m1.put("colf1", "colq1", now, "");
        writer.addMutation(m1);

        Mutation m2 = new Mutation("row2");
        m2.put("colf2", "colq2", (now + 100000), "");
        writer.addMutation(m2);

        writer.close();

        // Scan the entire table, we should only see keys whose timestamps are greater than or equal to now.
        // Mutation 1 should be expired by now, we should only see Mutation 2;
        boolean sawRow2 = false;
        Scanner scanner = client.createScanner(TABLE_NAME, new Authorizations());
        for (Entry<Key,Value> entry : scanner) {
            if (entry.getKey().getRow().toString().equals("row1"))
                Assert.fail("We saw row1 when it should be expired.");
            if (entry.getKey().getRow().toString().equals("row2"))
                sawRow2 = true;
        }
        if (!sawRow2)
            Assert.fail("We did not see row2 and we should have");
    }

}
