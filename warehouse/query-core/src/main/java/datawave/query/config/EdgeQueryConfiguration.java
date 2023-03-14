package datawave.query.config;

import java.util.List;

import datawave.data.type.Type;
import datawave.query.model.edge.EdgeQueryModel;
import datawave.query.tables.edge.EdgeQueryLogic;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;

/**
 * Created with IntelliJ IDEA. To change this template use File | Settings | File Templates.
 */
public class EdgeQueryConfiguration extends GenericQueryConfiguration {
    private static final long serialVersionUID = -2795330785878662313L;
    
    // We originally had the two choices:
    // EVENT => apply date range to edges generated using event date
    // LOAD => apply date range to load date in value of edges generated using event date
    // Then we added edges generated using activity date. So, we've added:
    // ACTIVITY => apply date range to edges generated using activity date
    // ACTIVITY_LOAD => apply date range to load date in value of edges generated using activity date
    // ANY => apply date range to regardless of date type
    // ANY_LOAD => apply date range to load date in value of edges generated using any date
    public enum dateType {
        EVENT, LOAD, ACTIVITY, ACTIVITY_LOAD, ANY, ANY_LOAD
    };
    
    public static final String INCLUDE_STATS = "stats";
    
    public static final String DATE_RANGE_TYPE = "date.type";
    
    public static final String LOAD = "LOAD", EVENT = "EVENT", ACTIVITY = "ACTIVITY", ACTIVITY_LOAD = "ACTIVITY_LOAD", ANY = "ANY", ANY_LOAD = "ANY_LOAD";
    
    public static final String SUMMARIZE = "summarize";
    
    // Query model defaults...
    private String modelName = "DATAWAVE_EDGE";
    private String modelTableName = "DatawaveMetadata";
    private EdgeQueryModel edgeQueryModel = null;
    
    private List<? extends Type<?>> dataTypes;
    private int numQueryThreads;
    private Query query;
    private boolean protobufEdgeFormat = true;
    
    // to be backwards compatible, by default we want to return
    protected boolean includeStats = true;
    private long maxQueryTerms = 10000;
    private long maxPrefilterValues = 100000;
    
    // default will be event date
    private dateType dateRangeType = dateType.EVENT;
    
    // Use to aggregate results will be false by default
    private boolean aggregateResults = false;
    
    public EdgeQueryConfiguration(EdgeQueryLogic configuredLogic, Query query) {
        super(configuredLogic);
        setDataTypes(configuredLogic.getDataTypes());
        setNumQueryThreads(configuredLogic.getQueryThreads());
        setQuery(query);
        setProtobufEdgeFormat(configuredLogic.isProtobufEdgeFormat());
        setModelName(configuredLogic.getModelName());
        setModelTableName(configuredLogic.getModelTableName());
        setEdgeQueryModel(configuredLogic.getEdgeQueryModel());
    }
    
    public List<? extends Type<?>> getDataTypes() {
        return dataTypes;
    }
    
    public void setDataTypes(List<? extends Type<?>> dataTypes) {
        this.dataTypes = dataTypes;
    }
    
    public int getNumQueryThreads() {
        return numQueryThreads;
    }
    
    public void setNumQueryThreads(int numQueryThreads) {
        this.numQueryThreads = numQueryThreads;
    }
    
    public boolean isProtobufEdgeFormat() {
        return protobufEdgeFormat;
    }
    
    public void setProtobufEdgeFormat(boolean protobufEdgeFormat) {
        this.protobufEdgeFormat = protobufEdgeFormat;
    }
    
    public boolean includeStats() {
        return includeStats;
    }
    
    public dateType getDateRangeType() {
        return dateRangeType;
    }
    
    public void setDateRangeType(dateType dateRangeType) {
        this.dateRangeType = dateRangeType;
    }
    
    /**
     * Fluent interface for parsing the parameters out of the Parameter set provided by the Query.
     * 
     * @param settings
     *            the settings
     * @return an edge query configuration
     */
    public EdgeQueryConfiguration parseParameters(Query settings) {
        if (settings.getParameters() != null) {
            QueryImpl.Parameter p = settings.findParameter(INCLUDE_STATS);
            if (p != null && !p.getParameterValue().isEmpty()) {
                this.includeStats = Boolean.parseBoolean(p.getParameterValue());
            }
            
            p = settings.findParameter(DATE_RANGE_TYPE);
            if (p != null && !p.getParameterValue().isEmpty()) {
                String paramVal = p.getParameterValue();
                switch (paramVal) {
                    case LOAD:
                        this.dateRangeType = dateType.LOAD;
                        break;
                    case EVENT:
                        this.dateRangeType = dateType.EVENT;
                        break;
                    case ACTIVITY:
                        this.dateRangeType = dateType.ACTIVITY;
                        break;
                    case ACTIVITY_LOAD:
                        this.dateRangeType = dateType.ACTIVITY_LOAD;
                        break;
                    case ANY:
                        this.dateRangeType = dateType.ANY;
                        break;
                    case ANY_LOAD:
                        this.dateRangeType = dateType.ANY_LOAD;
                        break;
                // @WARNING no default case, i.e. we are ignoring unexpected values for dateRangeType
                }
            }
            
            p = settings.findParameter(SUMMARIZE);
            if (p != null && !p.getParameterValue().isEmpty()) {
                this.aggregateResults = Boolean.parseBoolean(p.getParameterValue());
            }
        }
        return this;
    }
    
    public Query getQuery() {
        return query;
    }
    
    public void setQuery(Query query) {
        this.query = query;
    }
    
    public long getMaxQueryTerms() {
        return maxQueryTerms;
    }
    
    public void setMaxQueryTerms(long maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
    }
    
    public long getMaxPrefilterValues() {
        return maxPrefilterValues;
    }
    
    public void setMaxPrefilterValues(long maxPrefilterValues) {
        this.maxPrefilterValues = maxPrefilterValues;
    }
    
    public boolean isAggregateResults() {
        return aggregateResults;
    }
    
    public void setAggregateResults(boolean aggregateResults) {
        this.aggregateResults = aggregateResults;
    }
    
    public EdgeQueryModel getEdgeQueryModel() {
        return this.edgeQueryModel;
    }
    
    public void setEdgeQueryModel(EdgeQueryModel model) {
        this.edgeQueryModel = model;
    }
    
    public String getModelName() {
        return modelName;
    }
    
    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
    
    public String getModelTableName() {
        return modelTableName;
    }
    
    public void setModelTableName(String modelTableName) {
        this.modelTableName = modelTableName;
    }
}
