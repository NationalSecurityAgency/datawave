package datawave.query.tables.keyword;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.tables.chained.strategy.FullChainStrategy;

/**
 * Strategy for chaining UUID lookup and keyword extraction queries together, delegates to StatefulKeywordUUIDChainStrategy to handle individual batches of
 * lookups.
 */
public class KeywordUUIDChainStrategy extends FullChainStrategy<Entry<Key,Value>,Entry<Key,Value>> {

    /**
     * configurable batch size in the chain, -1 is no batching
     */
    private int batchSize = -1;

    @Override
    protected Query buildLatterQuery(Query initialQuery, Iterator<Entry<Key,Value>> initialQueryResults, String latterLogicName) {
        throw new UnsupportedOperationException("Should be delegating to StatefulKeywordUUIDChainStrategy");
    }

    @Override
    public Iterator<Entry<Key,Value>> runChainedQuery(AccumuloClient client, Query initialQuery, Set<Authorizations> auths,
                    Iterator<Entry<Key,Value>> initialQueryResults, QueryLogic<Entry<Key,Value>> latterQueryLogic) {

        Iterator<Entry<Key,Value>> wrapped = new Iterator<>() {
            private Iterator<Entry<Key,Value>> batchIterator;

            @Override
            public boolean hasNext() {
                while (batchIterator == null || (!batchIterator.hasNext() && initialQueryResults.hasNext())) {
                    try {
                        StatefulKeywordUUIDChainStrategy statefulChainStrategy = new StatefulKeywordUUIDChainStrategy(initialQuery);
                        statefulChainStrategy.setBatchSize(batchSize);
                        batchIterator = statefulChainStrategy.runChainedQuery(client, initialQuery, auths, initialQueryResults, latterQueryLogic);
                    } catch (Exception e) {
                        throw new DatawaveFatalQueryException("Failed to create next chained query: " + e.getMessage(), e);
                    }
                }

                // the iterator exists and has more, so always true
                return batchIterator.hasNext();
            }

            @Override
            public Entry<Key,Value> next() {
                return batchIterator.next();
            }
        };

        // prime the iterator to make sure latterQueryLogic is configured
        // noinspection ResultOfMethodCallIgnored
        wrapped.hasNext();

        return wrapped;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
