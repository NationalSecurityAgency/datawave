package datawave.experimental.executor;

import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.configuration.QueryData;

/**
 * Builds {@link QueryExecutor}s from a query plan
 */
public class QueryExecutorFactory {

    private final ShardQueryConfiguration config;
    private final MetadataHelper metadataHelper;
    private final BlockingQueue<Entry<Key,Value>> results;
    private final ExecutorService uidThreadPool;
    private final ExecutorService documentThreadPool;

    public QueryExecutorFactory(ShardQueryConfiguration config, MetadataHelper metadataHelper, LinkedBlockingQueue<Entry<Key,Value>> results,
                    ExecutorService uidThreadPool, ExecutorService documentThreadPool) {
        this.config = config;
        this.metadataHelper = metadataHelper;
        this.results = results;
        this.uidThreadPool = uidThreadPool;
        this.documentThreadPool = documentThreadPool;
    }

    public QueryExecutor createExecutor(QueryData queryData) {
        QueryExecutorOptions options = new QueryExecutorOptions();
        options.configureViaQueryData(queryData);
        options.setTableName(config.getShardTableName());
        options.setAuths(config.getAuthorizations().iterator().next());
        options.setClient(config.getClient());
        return new QueryExecutor(options, metadataHelper, results, uidThreadPool, documentThreadPool);
    }
}
