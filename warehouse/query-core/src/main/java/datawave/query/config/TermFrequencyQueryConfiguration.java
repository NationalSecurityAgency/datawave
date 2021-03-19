package datawave.query.config;

import datawave.microservice.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.data.Range;

public class TermFrequencyQueryConfiguration extends GenericQueryConfiguration {
    
    private static final long serialVersionUID = 1L;
    
    private Range range = null;
    
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
}
