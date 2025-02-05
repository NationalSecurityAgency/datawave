package datawave.query.tables.ssdeep;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
 * Converts batches of ScoredSSDeepPair into the latterQueryLogic in batches. Maintain batch scored state to enrich discovered results for each batch. A batch
 * size of -1 will force a single batch. When seenHashes is set ScoredSSDeepPair matchingHash will be deduped.
 */
public class StatefulSSDeepDiscoveryChainStrategy extends FullChainStrategy<ScoredSSDeepPair,DiscoveredSSDeep> {
    private static final Logger log = Logger.getLogger(StatefulSSDeepDiscoveryChainStrategy.class);

    private int batchSize = -1;
    private Set<Integer> seenHashes;
    private Multimap<String,ScoredSSDeepPair> scoredMatches;

    @Override
    protected Query buildLatterQuery(Query initialQuery, Iterator<ScoredSSDeepPair> initialQueryResults, String latterLogicName) {
        log.debug("buildLatterQuery() called...");

        // track the scored matches we've seen while traversing the initial query results.
        // this has to be case-insensitive because the CHECKSUM_SSDEEP index entries are most likely downcased.
        scoredMatches = TreeMultimap.create(String.CASE_INSENSITIVE_ORDER, ScoredSSDeepPair.NATURAL_ORDER);

        String queryString = captureScoredMatchesAndBuildQuery(initialQueryResults, scoredMatches, batchSize);

        if (scoredMatches.isEmpty()) {
            log.info("Did not receive scored matches from initial query, returning null latter query");
            return null;
        }

        Query q = new QueryImpl(); // TODO, need to use a factory? don't hardcode this.
        q.setQuery(queryString);
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE); // TODO: choose something reasonable.
        q.setQueryAuthorizations(initialQuery.getQueryAuthorizations());
        q.setUserDN(initialQuery.getUserDN());
        return q;
    }

    @Override
    public Iterator<DiscoveredSSDeep> runChainedQuery(AccumuloClient client, Query initialQuery, Set<Authorizations> auths,
                    Iterator<ScoredSSDeepPair> initialQueryResults, QueryLogic<DiscoveredSSDeep> latterQueryLogic) throws Exception {
        Iterator<DiscoveredSSDeep> itr = super.runChainedQuery(client, initialQuery, auths, initialQueryResults, latterQueryLogic);
        itr = getEnrichedDiscoveredSSDeepIterator(itr, scoredMatches);

        return itr;
    }

    public String captureScoredMatchesAndBuildQuery(Iterator<ScoredSSDeepPair> initialQueryResults, final Multimap<String,ScoredSSDeepPair> scoredMatches,
                    int batchSize) {
        int count = 0;
        Set<String> hashes = new HashSet<>();
        while (initialQueryResults.hasNext() && (batchSize == -1 || count < batchSize)) {
            ScoredSSDeepPair pair = initialQueryResults.next();
            // do hash checking if configured
            if (seenHashes != null) {
                int hashCode = pair.getMatchingHash().hashCode();
                if (seenHashes.contains(hashCode)) {
                    // already saw this hash, skip it
                    continue;
                }

                // add the hash so its never processed again
                seenHashes.add(hashCode);
            }

            scoredMatches.put(pair.getMatchingHash().toString(), pair);
            log.debug("Added new ssdeep " + pair.getMatchingHash());
            hashes.add("CHECKSUM_SSDEEP:\"" + pair.getMatchingHash().toString() + "\"");
            count++;
        }
        return StringUtils.join(hashes, " OR ");
    }

    /**
     * Given an iterator of DiscoveredSSDeep objects that have no matching query or weighted score, lookup the potential queries that returned them and the
     * weighted score associated with that query and use them to produce enriched results.
     *
     * @param resultsIterator
     *            an iterator of unenrched DiscoveredSSDeep's that don't have query or score info.
     * @param scoredMatches
     *            the colletion of matchin hashes and the original queries that lead them to be returned.
     * @return an iterator of DiscoveredSSDeep's enriched with the queries that returned them.
     */
    public Iterator<DiscoveredSSDeep> getEnrichedDiscoveredSSDeepIterator(Iterator<DiscoveredSSDeep> resultsIterator,
                    final Multimap<String,ScoredSSDeepPair> scoredMatches) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultsIterator, Spliterator.ORDERED), false)
                        .flatMap(discoveredSSdeep -> enrichDiscoveredSSDeep(discoveredSSdeep, scoredMatches)).iterator();
    }

    /**
     * Given a single discovered ssdeep, use the scoredMatches map to determine which queries it is related to. This will return zero to many new
     * DiscoveredSSDeep entries for each query that the matching ssdeep hash appeared in.
     *
     * @param discoveredSSDeep
     *            the ssdeep discovery information a single matched hash
     * @param scoredMatches
     *            the set of scored matches from the ssdeep similarity logic, used to look up score and query info for the matched hash.
     * @return a stream of DiscoveredSSDeep objects that align discovery information with the original query hashes.
     */
    public Stream<DiscoveredSSDeep> enrichDiscoveredSSDeep(DiscoveredSSDeep discoveredSSDeep, final Multimap<String,ScoredSSDeepPair> scoredMatches) {
        final DiscoveredThing discoveredThing = discoveredSSDeep.getDiscoveredThing();
        final String term = discoveredThing.getTerm();
        return scoredMatches.get(term).stream().map(scoredPair -> new DiscoveredSSDeep(scoredPair, discoveredThing));
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setSeenHashes(Set<Integer> seenHashes) {
        this.seenHashes = seenHashes;
    }
}
