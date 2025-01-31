package datawave.query.tables.ssdeep;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import datawave.query.exceptions.DatawaveFatalQueryException;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.discovery.DiscoveredThing;
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
            private final HashSet<Integer> seenHashes = dedupe ? new HashSet<>() : null;

            @Override
            public boolean hasNext() {
                if (batchIterator == null || !batchIterator.hasNext()) {
                    try {
                        StatefulSSDeepDiscoveryChainStrategy statefulChainStrategy = new StatefulSSDeepDiscoveryChainStrategy();
                        statefulChainStrategy.setBatchSize(batchSize);
                        statefulChainStrategy.setSeenHashes(seenHashes);
                        batchIterator = statefulChainStrategy.runChainedQuery(client, initialQuery, auths, initialQueryResults, latterQueryLogic);

                        return batchIterator.hasNext();
                    } catch (Exception e) {
                        throw new DatawaveFatalQueryException("Failed to create next chained query", e);
                    }
                }

                // the iterator exists and has more, so always true
                return true;
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
