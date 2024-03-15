package datawave.query.tables.ssdeep;

import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.query.discovery.DiscoveredThing;
import datawave.query.tables.chained.strategy.FullChainStrategy;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.logic.QueryLogic;

/**
 * A chain strategy that is designed to first run a ssdeep similarity query and then run a subsequent discovery query for each matching ssdeep hash found by
 * that similarity query. Effectively allows the user to discover information related to hashes that are similar to one or more query hashes
 */
public class FullSSDeepDiscoveryChainStrategy extends FullChainStrategy<ScoredSSDeepPair,DiscoveredSSDeep> {
    private static final Logger log = Logger.getLogger(FullSSDeepDiscoveryChainStrategy.class);

    private Multimap<String,ScoredSSDeepPair> scoredMatches;

    @Override
    protected Query buildLatterQuery(Query initialQuery, Iterator<ScoredSSDeepPair> initialQueryResults, String latterLogicName) {
        log.debug("buildLatterQuery() called...");

        // track the scored matches we've seen while traversing the initial query results.
        // this has to be case-insensitive because the CHECKSUM_SSDEEP index entries are most likely downcased.
        scoredMatches = TreeMultimap.create(String.CASE_INSENSITIVE_ORDER, ScoredSSDeepPair.NATURAL_ORDER);

        String queryString = captureScoredMatchesAndBuildQuery(initialQueryResults, scoredMatches);

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
        final Iterator<DiscoveredSSDeep> it = super.runChainedQuery(client, initialQuery, auths, initialQueryResults, latterQueryLogic);

        // Create a defensive copy of the score map because stream evaluation may be delayed.
        final Multimap<String,ScoredSSDeepPair> localScoredMatches = TreeMultimap.create(String.CASE_INSENSITIVE_ORDER, ScoredSSDeepPair.NATURAL_ORDER);
        localScoredMatches.putAll(scoredMatches);

        return getEnrichedDiscoveredSSDeepIterator(it, localScoredMatches);
    }

    /**
     *
     * @param initialQueryResults
     *            an iterator of scored ssdeep pairs that represent the results of the initial ssdeep similarity query.
     * @param scoredMatches
     *            used to capture the scored matches contained within the initialQueryResults
     * @return the query string for the next stage of the query.
     */
    public static String captureScoredMatchesAndBuildQuery(Iterator<ScoredSSDeepPair> initialQueryResults,
                    final Multimap<String,ScoredSSDeepPair> scoredMatches) {
        // extract the matched ssdeeps from the query results and generate the discovery query.
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(initialQueryResults, Spliterator.ORDERED), false)
                        .filter(queryResult -> scoredMatches.put(queryResult.getMatchingHash().toString(), queryResult))
                        .map(queryResult -> queryResult.getMatchingHash().toString()).distinct().peek(ssdeep -> log.debug("Added new ssdeep " + ssdeep))
                        .map(ssdeep -> "CHECKSUM_SSDEEP:\"" + ssdeep + "\"").collect(Collectors.joining(" OR ", "", ""));
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
    public static Iterator<DiscoveredSSDeep> getEnrichedDiscoveredSSDeepIterator(Iterator<DiscoveredSSDeep> resultsIterator,
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
    public static Stream<DiscoveredSSDeep> enrichDiscoveredSSDeep(DiscoveredSSDeep discoveredSSDeep, final Multimap<String,ScoredSSDeepPair> scoredMatches) {
        final DiscoveredThing discoveredThing = discoveredSSDeep.getDiscoveredThing();
        final String term = discoveredThing.getTerm();
        return scoredMatches.get(term).stream().map(scoredPair -> new DiscoveredSSDeep(scoredPair, discoveredThing));
    }
}
