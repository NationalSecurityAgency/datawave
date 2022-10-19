package datawave.query.config;

import datawave.query.tables.edge.EdgeQueryLogic;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;

public class EdgeExtendedSummaryConfiguration extends EdgeQueryConfiguration {
    
    public static final String SUMMARIZE = "summarize";
    public static final String LIMITED_JEXL = "LIMITED_JEXL";
    public static final String LIST = "LIST";
    public static final String IGNORE_STATS_PARAM = "ignoreStats";
    public static final String IGNORE_RELATIONSHIPS_PARAM = "ignoreRelationships";
    public static final String QUERY_DELIMITER_PARAM = "delimiter";
    public static final String EDGE_TYPES_PARAM = "edgeTypes";
    
    public static final String querySyntax = "query.syntax";
    
    private char delimiter = '\0';
    
    private int scannerThreads = 10;
    
    private boolean includeRelationships = true;
    
    private String edgeTypes;
    
    private boolean summaryInputType = false;
    
    // Use to aggregate results will be false by default
    private boolean aggregateResults = false;
    
    private boolean overRideInput = false;
    private boolean overRideOutput = false;
    
    public EdgeExtendedSummaryConfiguration(EdgeQueryLogic configuredLogic, Query query) {
        super(configuredLogic, query);
    }
    
    @Override
    public EdgeQueryConfiguration parseParameters(Query settings) {
        super.parseParameters(settings);
        if (settings.getParameters() != null) {
            
            QueryImpl.Parameter p = settings.findParameter(SUMMARIZE);
            if (p != null && !p.getParameterValue().isEmpty()) {
                this.aggregateResults = Boolean.parseBoolean(p.getParameterValue());
                overRideOutput = true;
            }
            
            p = settings.findParameter(querySyntax);
            if (p != null && !p.getParameterValue().isEmpty()) {
                String paramVal = p.getParameterValue();
                if (paramVal.equals(LIST)) {
                    summaryInputType = true;
                } else {
                    summaryInputType = false;
                }
                overRideInput = true;
            }
            
            p = settings.findParameter(IGNORE_STATS_PARAM);
            if (p != null && !p.getParameterValue().isEmpty()) {
                if (Boolean.parseBoolean(p.getParameterValue())) {
                    includeStats = false;
                }
            }
            
            p = settings.findParameter(IGNORE_RELATIONSHIPS_PARAM);
            if (p != null && !p.getParameterValue().isEmpty()) {
                if (Boolean.parseBoolean(p.getParameterValue())) {
                    includeRelationships = false;
                }
            }
            
            p = settings.findParameter(QUERY_DELIMITER_PARAM);
            if (p != null && !p.getParameterValue().isEmpty()) {
                if (p.getParameterValue().length() != 1) {
                    throw new UnsupportedOperationException("The query parameter " + QUERY_DELIMITER_PARAM + " only accepts a single character.");
                }
                delimiter = p.getParameterValue().charAt(0);
            }
            
            p = settings.findParameter(EDGE_TYPES_PARAM);
            if (p != null && !p.getParameterValue().isEmpty()) {
                edgeTypes = p.getParameterValue();
            }
        }
        return this;
    }
    
    public char getDelimiter() {
        return delimiter;
    }
    
    public boolean isIncludeRelationships() {
        return includeRelationships;
    }
    
    public boolean isAggregateResults() {
        return aggregateResults;
    }
    
    public boolean isSummaryInputType() {
        return summaryInputType;
    }
    
    public String getEdgeTypes() {
        return edgeTypes;
    }
    
    public boolean isOverRideOutput() {
        return overRideOutput;
    }
    
    public boolean isOverRideInput() {
        return overRideInput;
    }
}
