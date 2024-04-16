package datawave.query.tables.ssdeep;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.QueryLogicTransformer;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import datawave.query.tables.chained.ChainedQueryTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

/**
 * Implements a ChainedQueryTable that will first use the SSDeepSimilarityQueryLogic to find similar hashes for a set of query hashes and then run the
 * ShardQueryLogic to retrieve discovery info for those matched hashes.
 */
public class SSDeepChainedEventQueryLogic extends ChainedQueryTable<ScoredSSDeepPair, Entry<Key, Value>> {
    private static final Logger log = Logger.getLogger(SSDeepChainedDiscoveryQueryLogic.class);

    private Query eventQuery = null;

    public SSDeepChainedEventQueryLogic() {
        super();
    }

    @SuppressWarnings("CopyConstructorMissesField")
    public SSDeepChainedEventQueryLogic(SSDeepChainedEventQueryLogic other) {
        super(other);
    }

    @Override
    public void close() {
        super.close();
    }

    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        super.initialize(client, settings, auths);
        this.eventQuery = settings.duplicate(settings.getQueryName() + "_event_query");

        log.debug("Initial settings parameters: " + settings.getParameters().toString());
        return this.logic1.initialize(client, settings, auths);
    }

    public void setupQuery(GenericQueryConfiguration config) throws Exception {
        if (null == this.getChainStrategy()) {
            final String error = "No transformed ChainStrategy provided for SSDeepChainedEventQueryLogic!";
            log.error(error);
            throw new RuntimeException(error);
        }

        log.info("Setting up ssdeep query using config");
        this.logic1.setupQuery(config);

        final Iterator<ScoredSSDeepPair> iter1 = this.logic1.iterator();

        log.info("Running chained discovery query");
        this.iterator = this.getChainStrategy().runChainedQuery(config.getClient(), this.eventQuery, config.getAuthorizations(), iter1, this.logic2);
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return this.logic2.getTransformer(settings);
    }

    @Override
    public SSDeepChainedEventQueryLogic clone() throws CloneNotSupportedException {
        return new SSDeepChainedEventQueryLogic(this);
    }

    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    }
}
