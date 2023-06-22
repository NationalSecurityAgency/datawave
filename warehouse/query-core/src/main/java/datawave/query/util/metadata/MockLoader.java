package datawave.query.util.metadata;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;

/**
 *
 */
public class MockLoader extends CacheLoader<LoaderKey,InMemoryInstance> {

    public static final Logger log = Logger.getLogger(MockLoader.class);

    protected ListeningExecutorService executorService;

    public static final byte[] MOCK_PASSWORD = "".getBytes();

    public MockLoader(ListeningExecutorService listeningDecorator) {
        executorService = listeningDecorator;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.cache.CacheLoader#load(java.lang.Object)
     */
    @Override
    public InMemoryInstance load(LoaderKey key) throws Exception {
        TableCallable callable = new TableCallable(key);
        return callable.call();
    }

    public ListenableFuture<InMemoryInstance> reload(LoaderKey key, InMemoryInstance oldValue) throws Exception {

        ListenableFutureTask<InMemoryInstance> task = ListenableFutureTask.create(new TableCallable(key));

        executorService.execute(task);

        return task;

    }

    public static class TableCallable implements Callable<InMemoryInstance> {

        private LoaderKey key;

        public TableCallable(LoaderKey key) {
            this.key = key;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.util.concurrent.Callable#call()
         */
        @Override
        public InMemoryInstance call() throws Exception {

            InMemoryInstance instance = new InMemoryInstance(UUID.randomUUID() + key.table);
            Authorizations auths = key.client.securityOperations().getUserAuthorizations(key.user);

            if (log.isTraceEnabled()) {
                log.trace("Building mock instances, with auths " + auths + " from " + key);
            }

            try (AccumuloClient client = new InMemoryAccumuloClient(key.user, instance)) {
                client.securityOperations().changeUserAuthorizations(key.user, auths);
                if (client.tableOperations().exists(key.table))
                    client.tableOperations().delete(key.table);

                client.tableOperations().create(key.table);

                try (BatchScanner scanner = key.client.createBatchScanner(key.table, auths, 11);
                                BatchWriter writer = client.createBatchWriter(key.table, new BatchWriterConfig().setMaxMemory(100L * (1024L * 1024L))
                                                .setMaxLatency(100L, TimeUnit.MILLISECONDS).setMaxWriteThreads(1))) {

                    scanner.setRanges(Lists.newArrayList(new Range()));
                    Iterator<Entry<Key,Value>> iter = scanner.iterator();
                    while (iter.hasNext()) {

                        Entry<Key,Value> value = iter.next();

                        Key valueKey = value.getKey();

                        Mutation m = new Mutation(value.getKey().getRow());
                        m.put(valueKey.getColumnFamily(), valueKey.getColumnQualifier(), new ColumnVisibility(valueKey.getColumnVisibility()),
                                        valueKey.getTimestamp(), value.getValue());
                        writer.addMutation(m);

                    }
                }
            }
            if (log.isTraceEnabled())
                log.trace("Built new instance " + instance.hashCode() + " now returning for use");

            return instance;
        }

    }

}
