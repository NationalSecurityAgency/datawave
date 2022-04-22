package datawave.webservice.query.hud;

import datawave.microservice.querymetric.QueryMetricSummary;

/**
 * 
 */
public class HudMetricSummaryBuilder {
    
    public HudMetricSummary buildMetricsSummary(long hours, QueryMetricSummary qmSummary) {
        HudMetricSummary hmSummary = new HudMetricSummary();
        hmSummary.setHours(hours);
        hmSummary.setMaxPageResponseTime(qmSummary.getMaxPageResponseTime());
        hmSummary.setMaxPageResultSize(qmSummary.getMaxPageResultSize());
        hmSummary.setMinPageResponseTime(qmSummary.getMinPageResponseTime());
        hmSummary.setMinPageResultSize(qmSummary.getMinPageResultSize());
        hmSummary.setQueryCount(qmSummary.getQueryCount());
        hmSummary.setTotalPageResponseTime(qmSummary.getTotalPageResponseTime());
        hmSummary.setTotalPageResultSize(qmSummary.getTotalPageResultSize());
        hmSummary.setTotalPages(qmSummary.getTotalPages());
        return hmSummary;
    }
}
