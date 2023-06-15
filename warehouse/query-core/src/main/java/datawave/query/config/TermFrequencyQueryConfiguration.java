package datawave.query.config;

import org.apache.accumulo.core.data.Range;

import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.BaseQueryLogic;

public class TermFrequencyQueryConfiguration extends GenericQueryConfiguration {

    private static final long serialVersionUID = 1L;

    private Range range = null;
    private Query query;

    public TermFrequencyQueryConfiguration(BaseQueryLogic<?> configuredLogic, Query query) {
        super(configuredLogic);
        setQuery(query);
    }

    public Range getRange() {
        return range;
    }

    public void setRange(Range range) {
        this.range = range;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }
}
