package datawave.query.tables.ssdeep;

import datawave.query.tables.chained.strategy.FullChainStrategy;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class FullSSDeepEventChainStrategy extends FullChainStrategy<Map.Entry<Key, Value>, Map.Entry<Key, Value>> {
    @Override
    protected Query buildLatterQuery(Query initialQuery, Iterator<Map.Entry<Key, Value>> initialQueryResults, String latterLogicName) {
        log.debug("buildLatterQuery() called...");
        StringBuilder b = new StringBuilder();
        Set<String> ssdeepSeen = new HashSet<>();
        while (initialQueryResults.hasNext()) {
            Map.Entry<Key, Value> result = initialQueryResults.next();
            Key key = result.getKey();
            String ssdeep = key.getColumnQualifier().toString();
            if (ssdeepSeen.contains(ssdeep)) {
                continue;
            }
            log.debug("Added new ssdeep " + ssdeep);
            ssdeepSeen.add(ssdeep);
            if (b.length() > 0) {
                b.append(" OR ");
            }
            b.append("CHECKSUM_SSDEEP:\"").append(ssdeep).append("\"");
        }

        Query q = new QueryImpl(); // TODO, need to use a factory? don't hardcode this.
        q.setQuery(b.toString());
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE); // TODO: choose something reasonable.
        q.setQueryAuthorizations(initialQuery.getQueryAuthorizations());
        q.setUserDN(initialQuery.getUserDN());
        // TODO: set up a reasonable start and end date.
        return q;
    }
}
