package datawave.query.tables.ssdeep;

import datawave.query.discovery.DiscoveredThing;
import datawave.query.tables.chained.ChainedQueryTable;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.QueryLogicTransformer;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Set;

public class SSDeepDiscoveryQueryTable extends ChainedQueryTable<Entry<Key, Value>, DiscoveredThing> {

    private static final Logger log = Logger.getLogger(SSDeepDiscoveryQueryTable.class);

    private Query q = null;

    public SSDeepDiscoveryQueryTable() { super(); }

    @SuppressWarnings("CopyConstructorMissesField")
    public SSDeepDiscoveryQueryTable(SSDeepDiscoveryQueryTable other) {
        super(other);
    }

    @Override
    public void close() {
        super.close();
    }

    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        super.initialize(client, settings, auths);
        this.q = settings.duplicate(settings.getQueryName() + "_discovery_query");

        log.debug("Initial settings parameters: " + settings.getParameters().toString());
        GenericQueryConfiguration config = this.logic1.initialize(client, settings, auths);
        return config;
    }

    public void setupQuery(GenericQueryConfiguration config) throws Exception {
        if (null == this.getChainStrategy()) {
            final String error = "No ChainStrategy provided for SSDeepDiscoveryQueryTable!";
            log.error(error);
            throw new RuntimeException(error);
        }

        log.info("Setting up ssdeep query using config");
        this.logic1.setupQuery(config);

        final Iterator<Entry<Key,Value>> iter1 = this.logic1.iterator();

        log.info("Running chained discovery query");
        this.iterator = this.getChainStrategy().runChainedQuery(config.getClient(), this.q, config.getAuthorizations(), iter1, this.logic2);
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return this.logic2.getTransformer(settings);
    }

    @Override
    public SSDeepDiscoveryQueryTable clone() throws CloneNotSupportedException {
        return new SSDeepDiscoveryQueryTable(this);
    }

    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    } 

}
