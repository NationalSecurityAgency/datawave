package datawave.query.metrics;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.BaseQueryMetric.PageMetric;
import datawave.webservice.query.metric.QueryMetricHandler;
import datawave.webservice.query.metric.QueryMetricSummary;
import datawave.webservice.query.metric.QueryMetricsSummaryResponse;

import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

/**
 * 
 */
public abstract class BaseQueryMetricHandler<T extends BaseQueryMetric> implements QueryMetricHandler<T> {
    
    private Logger log = Logger.getLogger(BaseQueryMetricHandler.class);
    
    public void populateSummary(T metric, QueryMetricSummary bucket) {
        bucket.addQuery();
        for (PageMetric page : metric.getPageTimes()) {
            bucket.addPage(page.getPagesize(), page.getReturnTime());
        }
    }
    
    public QueryMetricsSummaryResponse processQueryMetricsSummary(List<T> queryMetrics) throws IOException {
        return processQueryMetricsSummary(queryMetrics, new Date(), new QueryMetricsSummaryResponse());
    }
    
    public QueryMetricsSummaryResponse processQueryMetricsSummary(List<T> queryMetrics, Date end, QueryMetricsSummaryResponse summary) throws IOException {
        
        Date hour1 = DateUtils.addHours(end, -1);
        Date hour6 = DateUtils.addHours(end, -6);
        Date hour12 = DateUtils.addHours(end, -12);
        Date day1 = DateUtils.addDays(end, -1);
        Date day7 = DateUtils.addDays(end, -7);
        Date day30 = DateUtils.addDays(end, -30);
        Date day60 = DateUtils.addDays(end, -60);
        Date day90 = DateUtils.addDays(end, -90);
        
        for (T metric : queryMetrics) {
            try {
                binSummary(metric, summary, hour1, hour6, hour12, day1, day7, day30, day60, day90);
            } catch (Exception e1) {
                log.error(e1.getMessage(), e1);
            }
        }
        
        return summary;
    }
    
    public void binSummary(T metric, QueryMetricsSummaryResponse summary, Date hour1, Date hour6, Date hour12, Date day1, Date day7, Date day30, Date day60,
                    Date day90) {
        Date d = metric.getCreateDate();
        // Find out which buckets this query belongs to based on query create date.
        // If a query's create date is within one hour of the endDate then it is also within the other ranges.
        if (d.after(hour1)) {
            populateSummary(metric, summary.getHour1());
        }
        if (d.after(hour6)) {
            populateSummary(metric, summary.getHour6());
        }
        if (d.after(hour12)) {
            populateSummary(metric, summary.getHour12());
        }
        if (d.after(day1)) {
            populateSummary(metric, summary.getDay1());
        }
        if (d.after(day7)) {
            populateSummary(metric, summary.getDay7());
        }
        if (d.after(day30)) {
            populateSummary(metric, summary.getDay30());
        }
        if (d.after(day60)) {
            populateSummary(metric, summary.getDay60());
        }
        if (d.after(day90)) {
            populateSummary(metric, summary.getDay90());
        }
        populateSummary(metric, summary.getAll());
    }
}
