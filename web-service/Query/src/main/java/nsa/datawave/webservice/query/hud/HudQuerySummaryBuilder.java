package nsa.datawave.webservice.query.hud;

import nsa.datawave.webservice.query.Query;

/**
 * 
 */
public class HudQuerySummaryBuilder {
    
    public HudQuerySummaryBuilder() {
        super();
    }
    
    public HudQuerySummary build(Query query) {
        HudQuerySummary summary = new HudQuerySummary();
        summary.setQueryLogicName(query.getQueryLogicName());
        summary.setId(query.getId().toString());
        summary.setQueryName(query.getQueryName());
        summary.setUserDN(query.getUserDN());
        summary.setQuery(query.getQuery());
        summary.setQueryAuthorizations(query.getQueryAuthorizations());
        summary.setExpirationDate(query.getExpirationDate().getTime());
        return summary;
    }
    
}
