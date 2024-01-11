package datawave.query.tables.ssdeep;

import datawave.query.discovery.DiscoveredThing;
import datawave.query.tables.chained.iterators.ChainedQueryStreamingIterator;
import datawave.query.tables.chained.strategy.StreamedChainStrategy;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.logic.QueryLogic;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class StreamingSSDeepDiscoveryChainStrategy extends StreamedChainStrategy<Map.Entry<Key, Value>, DiscoveredThing> {

    protected final Logger log = Logger.getLogger(StreamingSSDeepDiscoveryChainStrategy.class);

    protected int maxResultsToBuffer = 5;

    @Override
    protected ChainedQueryStreamingIterator<Map.Entry<Key, Value>, DiscoveredThing> getStreamingIterator(AccumuloClient client, Query initialQuery, Set<Authorizations> auths, Iterator<Map.Entry<Key, Value>> initialQueryResults, QueryLogic<DiscoveredThing> latterQueryLogic) {
        ChainedQueryStreamingIterator<Map.Entry<Key, Value>, DiscoveredThing> chainedIterator = new SSDeepDiscoveryChainedIterator();
        chainedIterator.setMaxResultsToBuffer(maxResultsToBuffer);
        chainedIterator.setClient(client);
        chainedIterator.setInitialQuery(initialQuery);
        chainedIterator.setAuths(auths);
        chainedIterator.setInitialQueryResults(initialQueryResults);
        chainedIterator.setLatterQueryLogic(latterQueryLogic);
        return chainedIterator;
    }

    public int getMaxResultsToBuffer() {
        return maxResultsToBuffer;
    }

    public void setMaxResultsToBuffer(int maxResultsToBuffer) {
        this.maxResultsToBuffer = maxResultsToBuffer;
    }

    public static class SSDeepDiscoveryChainedIterator extends ChainedQueryStreamingIterator<Map.Entry<Key, Value>, DiscoveredThing> {

        protected static final Logger log = Logger.getLogger(SSDeepDiscoveryChainedIterator.class);

        protected final Set<String> ssdeepSeen = new HashSet<>();

        @Override
        protected Query buildNextQuery(Set<String> queryTerms) {
            StringBuilder b = new StringBuilder();
            for (String term: queryTerms) {
                if (b.length() > 0) {
                    b.append(" OR ");
                }
                b.append(term);
            }
            Query q = new QueryImpl(); // TODO, need to use a factory? don't hardcode this.
            q.setQuery(b.toString());
            q.setId(UUID.randomUUID());
            q.setPagesize(Integer.MAX_VALUE); // TODO: choose something reasonable.
            q.setQueryAuthorizations(auths.toString());
            q.setUserDN(initialQuery.getUserDN());
            return q;
        }

        @Override
        public Set<String> fetchQueryTerms(Map.Entry<Key, Value> initialResult) {
            log.debug("fetchQueryTerms() called...");
            Key key = initialResult.getKey();
            String ssdeep = key.getColumnQualifier().toString();
            if (ssdeepSeen.contains(ssdeep)) {
                return Collections.emptySet();
            }
            log.debug("Added new ssdeep " + ssdeep);
            ssdeepSeen.add(ssdeep);
            return Set.of("CHECKSUM_SSDEEP:\"" + ssdeep + "\"");
        }
    }


}
