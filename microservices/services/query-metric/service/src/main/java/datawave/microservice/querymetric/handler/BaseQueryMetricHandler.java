package datawave.microservice.querymetric.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;
import datawave.microservice.querymetric.QueryMetricSummary;
import datawave.microservice.querymetric.QueryMetricsSummaryResponse;
import datawave.microservice.querymetric.factory.QueryMetricResponseFactory;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;

/**
 *
 */
public abstract class BaseQueryMetricHandler<T extends BaseQueryMetric> implements QueryMetricHandler<T> {
    
    private Logger log = Logger.getLogger(BaseQueryMetricHandler.class);
    protected LuceneToJexlQueryParser luceneToJexlQueryParser;
    protected QueryMetricResponseFactory queryMetricResponseFactory;
    
    public BaseQueryMetricHandler(LuceneToJexlQueryParser luceneToJexlQueryParser) {
        this.luceneToJexlQueryParser = luceneToJexlQueryParser;
    }
    
    protected QueryMetricsSummaryResponse createSummaryResponse() {
        return queryMetricResponseFactory.createSummaryResponse();
    }
    
    public void populateSummary(T metric, QueryMetricSummary bucket) {
        bucket.addQuery();
        for (BaseQueryMetric.PageMetric page : metric.getPageTimes()) {
            bucket.addPage(page.getPagesize(), page.getReturnTime());
        }
    }
    
    public QueryMetricsSummaryResponse processQueryMetricsSummary(List<T> queryMetrics, Date end) throws IOException {
        
        QueryMetricsSummaryResponse summary = createSummaryResponse();
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
                log.error(e1.getMessage());
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
    
    public void populateMetricSelectors(T queryMetric) {
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
                    } catch (Exception e) {
                        // not JEXL, try LUCENE
                        QueryNode node = this.luceneToJexlQueryParser.parse(query);
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
            } catch (JexlException e) {
                log.error("populateMetricSelectors: Failure parsing query");
            } catch (Exception e) {
                log.error("populateMetricSelectors: " + e.getMessage());
            }
        }
    }
    
    public void setQueryMetricResponseFactory(QueryMetricResponseFactory queryMetricResponseFactory) {
        this.queryMetricResponseFactory = queryMetricResponseFactory;
    }
}
