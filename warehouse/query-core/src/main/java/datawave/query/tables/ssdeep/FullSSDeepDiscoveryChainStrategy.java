package datawave.query.tables.ssdeep;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.tables.chained.strategy.FullChainStrategy;

/**
 * A chain strategy that is designed to first run a ssdeep similarity query and then run a subsequent discovery query for each matching ssdeep hash found by
 * that similarity query. Effectively allows the user to discover information related to hashes that are similar to one or more query hashes
 */
public class FullSSDeepDiscoveryChainStrategy extends FullChainStrategy<ScoredSSDeepPair,DiscoveredSSDeep> {
    private static final Logger log = Logger.getLogger(FullSSDeepDiscoveryChainStrategy.class);

    /**
     * configurable batch size in the chain, -1 is no batching
     */
    private int batchSize = -1;

    /**
     * should matchingHashes be deduped in the chain strategy?
     */
    private boolean dedupe = true;

    @Override
    protected Query buildLatterQuery(Query initialQuery, Iterator<ScoredSSDeepPair> initialQueryResults, String latterLogicName) {
        throw new UnsupportedOperationException("Should be delegating to StatefulSSDeepDiscoveryChainStrategy");
    }

    @Override
    public Iterator<DiscoveredSSDeep> runChainedQuery(AccumuloClient client, Query initialQuery, Set<Authorizations> auths,
                    Iterator<ScoredSSDeepPair> initialQueryResults, QueryLogic<DiscoveredSSDeep> latterQueryLogic) throws Exception {
        Iterator<DiscoveredSSDeep> wrapped = new Iterator<>() {
            private Iterator<DiscoveredSSDeep> batchIterator;
            /**
             * Keep track of what has already come off the Similarity query and prevent duplicate hashes from being used
             */
            private final Set<Integer> seenHashes = dedupe ? new HashSet<>() : null;

            @Override
            public boolean hasNext() {
                while (batchIterator == null || (!batchIterator.hasNext() && initialQueryResults.hasNext())) {
                    try {
                        StatefulSSDeepDiscoveryChainStrategy statefulChainStrategy = new StatefulSSDeepDiscoveryChainStrategy();
                        statefulChainStrategy.setBatchSize(batchSize);
                        statefulChainStrategy.setSeenHashes(seenHashes);
                        batchIterator = statefulChainStrategy.runChainedQuery(client, initialQuery, auths, initialQueryResults, latterQueryLogic);
                    } catch (Exception e) {
                        throw new DatawaveFatalQueryException("Failed to create next chained query", e);
                    }
                }

                // the iterator exists and has more, so always true
                return batchIterator.hasNext();
            }

            @Override
            public DiscoveredSSDeep next() {
                return batchIterator.next();
            }
        };

        // prime the iterator to make sure latterQueryLogic is configured
        wrapped.hasNext();

        return wrapped;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setDedupe(boolean dedupe) {
        this.dedupe = dedupe;
    }

    public boolean isDedupe() {
        return this.dedupe;
    }
}
