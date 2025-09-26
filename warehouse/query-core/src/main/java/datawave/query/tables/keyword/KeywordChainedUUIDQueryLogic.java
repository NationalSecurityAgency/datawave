package datawave.query.tables.keyword;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.tables.chained.ChainedQueryTable;

/**
 * Pairs the lookupUUID logic with the keyword extraction logic to create the ability to extract keywords given a set of document identifiers.
 */
public class KeywordChainedUUIDQueryLogic extends ChainedQueryTable<Entry<Key,Value>,Entry<Key,Value>> {

    private static final Logger log = Logger.getLogger(KeywordChainedUUIDQueryLogic.class);

    private Query keywordExtractionQuery = null;

    public KeywordChainedUUIDQueryLogic() {
        super();
    }

    public KeywordChainedUUIDQueryLogic(KeywordChainedUUIDQueryLogic other) {
        super(other);
        this.keywordExtractionQuery = other.keywordExtractionQuery;
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        super.initialize(client, settings, auths);
        this.keywordExtractionQuery = settings.duplicate(settings.getQueryName() + "_keyword_query");

        QueryImpl.Parameter hitListParam = settings.findParameter(QueryParameters.HIT_LIST);
        if (hitListParam.getParameterValue().isEmpty()) {
            settings.addParameter(QueryParameters.HIT_LIST, "true");
        } else {
            hitListParam.setParameterValue("true");
        }

        QueryImpl.Parameter returnFieldsParam = settings.findParameter(QueryParameters.RETURN_FIELDS);
        if (returnFieldsParam.getParameterValue().isEmpty()) {
            settings.addParameter(QueryParameters.RETURN_FIELDS, "LANGUAGE");
        } else {
            returnFieldsParam.setParameterValue("LANGUAGE");
        }

        QueryImpl.Parameter querySyntaxParam = settings.findParameter(QueryParameters.QUERY_SYNTAX);
        if (querySyntaxParam.getParameterValue().isEmpty()) {
            settings.addParameter(QueryParameters.QUERY_SYNTAX, "LUCENE-UUID");
        } else {
            querySyntaxParam.setParameterValue("LUCENE-UUID");
        }

        if (null == this.logic1) {
            final String error = "No initial logic (logic1) provided for KeywordChainedUUIDQueryLogic!";
            log.error(error);
            throw new RuntimeException(error);
        }

        if (null == this.logic2) {
            final String error = "No follow-on logic (logic2) provided for KeywordChainedUUIDQueryLogic!";
            log.error(error);
            throw new RuntimeException(error);
        }

        if (null == this.getChainStrategy()) {
            final String error = "No transformed ChainStrategy provided for KeywordChainedUUIDQueryLogic!";
            log.error(error);
            throw new RuntimeException(error);
        }

        log.debug("Initial settings parameters: " + settings.getParameters().toString());
        return this.logic1.initialize(client, settings, auths);
    }

    @Override
    public void setupQuery(GenericQueryConfiguration config) throws Exception {
        log.info("Setting lookup UUID via config");
        this.logic1.setupQuery(config);

        final Iterator<Entry<Key,Value>> iter1 = this.logic1.iterator();

        log.info("Running keyword extraction query");
        this.iterator = this.getChainStrategy().runChainedQuery(config.getClient(), keywordExtractionQuery, config.getAuthorizations(), iter1, this.logic2);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return this.logic2.getTransformer(settings);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new KeywordChainedUUIDQueryLogic(this);
    }

    @Override
    public Set<String> getExampleQueries() {
        return Set.of();
    }
}
