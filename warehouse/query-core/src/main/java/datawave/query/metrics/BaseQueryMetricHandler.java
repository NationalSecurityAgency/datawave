package datawave.query.metrics;

import datawave.core.query.metric.QueryMetricHandler;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;
import datawave.microservice.querymetric.QueryMetricSummary;
import datawave.microservice.querymetric.QueryMetricsSummaryResponse;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 *
 */
public abstract class BaseQueryMetricHandler<T extends BaseQueryMetric> implements QueryMetricHandler<T> {

    private Logger log = Logger.getLogger(BaseQueryMetricHandler.class);

    public void populateSummary(T metric, QueryMetricSummary bucket) {
        bucket.addQuery();
        for (BaseQueryMetric.PageMetric page : metric.getPageTimes()) {
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

    protected void binSummary(T metric, QueryMetricsSummaryResponse summary, Date hour1, Date hour6, Date hour12, Date day1, Date day7, Date day30, Date day60,
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

    protected void incrementNumUpdates(T updatedMetric, Collection<T> cachedMetrics) {
        long maxUpdates = cachedMetrics.stream().map(BaseQueryMetric::getNumUpdates).max(Long::compareTo).orElse(0l);
        updatedMetric.setNumUpdates(maxUpdates + 1);
    }

    protected void populateMetricSelectors(T queryMetric, LuceneToJexlQueryParser luceneToJexlQueryParser) {
        String type = queryMetric.getQueryType();
        Lifecycle lifecycle = queryMetric.getLifecycle();
        // this is time consuming - we only need to parse the query and write the selectors once
        if (lifecycle.equals(Lifecycle.DEFINED) && type != null && type.equalsIgnoreCase("RunningQuery") && queryMetric.getPositiveSelectors() == null
                        && queryMetric.getNegativeSelectors() == null) {
            try {
                String query = queryMetric.getQuery();
                if (query != null) {
                    ASTJexlScript jexlScript = null;
                    try {
                        // Parse and flatten here before visitors visit.
                        jexlScript = JexlASTHelper.parseAndFlattenJexlQuery(query);
                    } catch (Throwable t1) {
                        // not JEXL, try LUCENE
                        QueryNode node = luceneToJexlQueryParser.parse(query);
                        String jexlQuery = node.getOriginalQuery();
                        jexlScript = JexlASTHelper.parseAndFlattenJexlQuery(jexlQuery);
                    }

                    if (jexlScript != null) {
                        jexlScript = TreeFlatteningRebuildingVisitor.flatten(jexlScript);
                        List<ASTEQNode> positiveEQNodes = JexlASTHelper.getPositiveEQNodes(jexlScript);
                        List<String> positiveSelectors = new ArrayList<>();
                        for (ASTEQNode pos : positiveEQNodes) {
                            String identifier = JexlASTHelper.getIdentifier(pos);
                            Object literal = JexlASTHelper.getLiteralValue(pos);
                            if (identifier != null && literal != null) {
                                positiveSelectors.add(identifier + ":" + literal);
                            }
                        }
                        if (!positiveSelectors.isEmpty()) {
                            queryMetric.setPositiveSelectors(positiveSelectors);
                        }
                        List<ASTEQNode> negativeEQNodes = JexlASTHelper.getNegativeEQNodes(jexlScript);
                        List<String> negativeSelectors = new ArrayList<>();
                        for (ASTEQNode neg : negativeEQNodes) {
                            String identifier = JexlASTHelper.getIdentifier(neg);
                            Object literal = JexlASTHelper.getLiteralValue(neg);
                            if (identifier != null && literal != null) {
                                negativeSelectors.add(identifier + ":" + literal);
                            }
                        }
                        if (!negativeSelectors.isEmpty()) {
                            queryMetric.setNegativeSelectors(negativeSelectors);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("populateMetricSelectors: " + e.getMessage());
            }
        }
    }
}
