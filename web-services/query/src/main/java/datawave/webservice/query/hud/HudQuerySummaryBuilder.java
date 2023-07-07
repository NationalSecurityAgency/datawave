package datawave.webservice.query.hud;

import datawave.webservice.query.Query;

/**
 *
 */
public class HudQuerySummaryBuilder {

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
