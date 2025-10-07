package datawave.query.tables.ssdeep;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.query.Query;
import datawave.query.tables.chained.ChainedQueryTable;

/**
 * Implements a ChainedQueryTable that will first use the SSDeepSimilarityQueryLogic to find similar hashes for a set of query hashes and then run the
 * SSDeepDiscoveryQueryLogic to retrieve discovery info for those matched hashes.
 */
public class SSDeepChainedDiscoveryQueryLogic extends ChainedQueryTable<ScoredSSDeepPair,DiscoveredSSDeep> {

    private static final Logger log = Logger.getLogger(SSDeepChainedDiscoveryQueryLogic.class);

    private Query discoveryQuery = null;

    public SSDeepChainedDiscoveryQueryLogic() {
        super();
    }

    @SuppressWarnings("CopyConstructorMissesField")
    public SSDeepChainedDiscoveryQueryLogic(SSDeepChainedDiscoveryQueryLogic other) {
        super(other);
    }

    @Override
    public void close() {
        super.close();
    }

    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        super.initialize(client, settings, auths);
        this.discoveryQuery = settings.duplicate(settings.getQueryName() + "_discovery_query");

        log.debug("Initial settings parameters: " + settings.getParameters().toString());
        GenericQueryConfiguration config = this.logic1.initialize(client, settings, auths);
        return config;
    }

    public void setupQuery(GenericQueryConfiguration config) throws Exception {
        if (null == this.getChainStrategy()) {
            final String error = "No transformed ChainStrategy provided for SSDeepChainedDiscoveryQueryLogic!";
            log.error(error);
            throw new RuntimeException(error);
        }

        log.info("Setting up ssdeep query using config");
        this.logic1.setupQuery(config);

        final Iterator<ScoredSSDeepPair> iter1 = this.logic1.iterator();

        log.info("Running chained discovery query");
        this.iterator = this.getChainStrategy().runChainedQuery(config.getClient(), this.discoveryQuery, config.getAuthorizations(), iter1, this.logic2);
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return this.logic2.getTransformer(settings);
    }

    @Override
    public SSDeepChainedDiscoveryQueryLogic clone() throws CloneNotSupportedException {
        return new SSDeepChainedDiscoveryQueryLogic(this);
    }

    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    }

}
