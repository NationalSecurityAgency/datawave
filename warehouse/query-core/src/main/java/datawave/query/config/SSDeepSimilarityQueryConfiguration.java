package datawave.query.config;

import java.util.Collection;

import org.apache.accumulo.core.data.Range;

import com.google.common.collect.Multimap;

import datawave.query.util.ssdeep.NGramTuple;
import datawave.query.util.ssdeep.SSDeepHash;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.BaseQueryLogic;

public class SSDeepSimilarityQueryConfiguration extends GenericQueryConfiguration {

    private Query query;

    private Collection<Range> ranges;

    private Multimap<NGramTuple,SSDeepHash> queryMap;

    public SSDeepSimilarityQueryConfiguration(BaseQueryLogic<?> configuredLogic, Query query) {
        super(configuredLogic);
        setQuery(query);
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public Collection<Range> getRanges() {
        return ranges;
    }

    public void setRanges(Collection<Range> ranges) {
        this.ranges = ranges;
    }

    public Multimap<NGramTuple,SSDeepHash> getQueryMap() {
        return queryMap;
    }

    public void setQueryMap(Multimap<NGramTuple,SSDeepHash> queryMap) {
        this.queryMap = queryMap;
    }
}
