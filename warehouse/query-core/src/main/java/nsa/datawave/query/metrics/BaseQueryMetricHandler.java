package nsa.datawave.query.metrics;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import nsa.datawave.webservice.query.metric.BaseQueryMetric;
import nsa.datawave.webservice.query.metric.BaseQueryMetric.PageMetric;
import nsa.datawave.webservice.query.metric.QueryMetricHandler;
import nsa.datawave.webservice.query.metric.QueryMetricSummary;
import nsa.datawave.webservice.query.metric.QueryMetricsSummaryHtmlResponse;
import nsa.datawave.webservice.query.metric.QueryMetricsSummaryResponse;

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
        
        QueryMetricsSummaryResponse summary = new QueryMetricsSummaryResponse();
        Date now = new Date();
        Date hour1 = DateUtils.addHours(now, -1);
        Date hour6 = DateUtils.addHours(now, -6);
        Date hour12 = DateUtils.addHours(now, -12);
        Date day1 = DateUtils.addDays(now, -1);
        Date day7 = DateUtils.addDays(now, -7);
        Date day30 = DateUtils.addDays(now, -30);
        Date day60 = DateUtils.addDays(now, -60);
        Date day90 = DateUtils.addDays(now, -90);
        
        for (T metric : queryMetrics) {
            try {
                binSummary(metric, summary, hour1, hour6, hour12, day1, day7, day30, day60, day90);
            } catch (Exception e1) {
                log.error(e1.getMessage());
            }
        }
        
        return summary;
    }
    
    public QueryMetricsSummaryHtmlResponse processQueryMetricsHtmlSummary(List<T> queryMetrics) throws IOException {
        
        QueryMetricsSummaryHtmlResponse summary = new QueryMetricsSummaryHtmlResponse();
        Date now = new Date();
        Date hour1 = DateUtils.addHours(now, -1);
        Date hour6 = DateUtils.addHours(now, -6);
        Date hour12 = DateUtils.addHours(now, -12);
        Date day1 = DateUtils.addDays(now, -1);
        Date day7 = DateUtils.addDays(now, -7);
        Date day30 = DateUtils.addDays(now, -30);
        Date day60 = DateUtils.addDays(now, -60);
        Date day90 = DateUtils.addDays(now, -90);
        
        for (T metric : queryMetrics) {
            try {
                binSummary(metric, summary, hour1, hour6, hour12, day1, day7, day30, day60, day90);
            } catch (Exception e1) {
                log.error(e1.getMessage());
            }
        }
        
        return summary;
    }
    
    public void binSummary(T metric, QueryMetricsSummaryResponse summary, Date hour1, Date hour6, Date hour12, Date day1, Date day7, Date day30, Date day60,
                    Date day90) {
        Date d = metric.getCreateDate();
        // Find out which bucket this query belongs to
        if (d.after(hour1)) {
            populateSummary(metric, summary.getHour1());
            populateSummary(metric, summary.getHour6());
            populateSummary(metric, summary.getHour12());
            populateSummary(metric, summary.getDay1());
            populateSummary(metric, summary.getDay7());
            populateSummary(metric, summary.getDay30());
            populateSummary(metric, summary.getDay60());
            populateSummary(metric, summary.getDay90());
            populateSummary(metric, summary.getAll());
        } else if (d.after(hour6)) {
            populateSummary(metric, summary.getHour6());
            populateSummary(metric, summary.getHour12());
            populateSummary(metric, summary.getDay1());
            populateSummary(metric, summary.getDay7());
            populateSummary(metric, summary.getDay30());
            populateSummary(metric, summary.getDay60());
            populateSummary(metric, summary.getDay90());
            populateSummary(metric, summary.getAll());
        } else if (d.after(hour12)) {
            populateSummary(metric, summary.getHour12());
            populateSummary(metric, summary.getDay1());
            populateSummary(metric, summary.getDay7());
            populateSummary(metric, summary.getDay30());
            populateSummary(metric, summary.getDay60());
            populateSummary(metric, summary.getDay90());
            populateSummary(metric, summary.getAll());
        } else if (d.after(day1)) {
            populateSummary(metric, summary.getDay1());
            populateSummary(metric, summary.getDay7());
            populateSummary(metric, summary.getDay30());
            populateSummary(metric, summary.getDay60());
            populateSummary(metric, summary.getDay90());
            populateSummary(metric, summary.getAll());
        } else if (d.after(day7)) {
            populateSummary(metric, summary.getDay7());
            populateSummary(metric, summary.getDay30());
            populateSummary(metric, summary.getDay60());
            populateSummary(metric, summary.getDay90());
            populateSummary(metric, summary.getAll());
        } else if (d.after(day30)) {
            populateSummary(metric, summary.getDay30());
            populateSummary(metric, summary.getDay60());
            populateSummary(metric, summary.getDay90());
            populateSummary(metric, summary.getAll());
        } else if (d.after(day60)) {
            populateSummary(metric, summary.getDay60());
            populateSummary(metric, summary.getDay90());
            populateSummary(metric, summary.getAll());
        } else if (d.after(day90)) {
            populateSummary(metric, summary.getDay90());
            populateSummary(metric, summary.getAll());
        } else {
            populateSummary(metric, summary.getAll());
        }
    }
}
