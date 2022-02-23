package datawave.query.metrics;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import datawave.ingest.data.config.ingest.TermFrequencyIngestHelperInterface;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;
import datawave.query.jexl.JexlASTHelper;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.BaseQueryMetric.PageMetric;
import datawave.webservice.query.metric.BaseQueryMetric.Prediction;
import datawave.webservice.query.util.QueryUtil;

public class ContentQueryMetricsIngestHelper extends CSVIngestHelper implements TermFrequencyIngestHelperInterface {
    
    /*
     * Field Name | Content | Update | Delete
     * 
     * ELAPSED_TIME X X LAST_UPDATED X X LIFECYCLE X X NUM_PAGES X X NUM_RESULTS X X PAGE_METRICS.X X X SETUP_TIME X X CREATE_CALL_TIME X X
     * 
     * AUTHORIZATIONS X BEGIN_DATE X END_DATE X ERROR_CODE X ERROR_MESSAGE X HOST X NEGATIVE_SELECTORS X POSITIVE_SELECTORS X PROXY_SERVERS QUERY X QUERY_ID X
     * QUERY_LOGIC X QUERY_TYPE X QUERY_NAME X X PARAMETERS X CREATE_DATE X USER X
     */
    
    private static final Logger log = Logger.getLogger(ContentQueryMetricsIngestHelper.class);
    
    private Set<String> contentIndexFields = new HashSet<>();
    private HelperDelegate<BaseQueryMetric> delegate = new HelperDelegate<>();
    
    public ContentQueryMetricsIngestHelper() {
        
    }
    
    public ContentQueryMetricsIngestHelper(boolean deleteMode) {
        this.deleteMode = deleteMode;
    }
    
    public Multimap<String,NormalizedContentInterface> getEventFieldsToDelete(BaseQueryMetric updatedQueryMetric, BaseQueryMetric storedQueryMetric) {
        return normalize(delegate.getEventFieldsToDelete(updatedQueryMetric, storedQueryMetric));
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields) {
        Multimap<String,NormalizedContentInterface> results = HashMultimap.create();
        
        for (Map.Entry<String,String> e : fields.entries()) {
            if (e.getValue() != null) {
                String field = e.getKey();
                NormalizedFieldAndValue nfv = null;
                int x = field.indexOf('.');
                if (x > -1) {
                    String baseFieldName = field.substring(0, x);
                    String group = field.substring(x + 1);
                    nfv = new NormalizedFieldAndValue(baseFieldName, e.getValue(), group, null);
                } else {
                    nfv = new NormalizedFieldAndValue(field, e.getValue());
                }
                applyNormalizationAndAddToResults(results, nfv);
            } else
                log.warn(this.getType().typeName() + " has key " + e.getKey() + " with a null value.");
        }
        return results;
    }
    
    public Multimap<String,NormalizedContentInterface> getEventFieldsToWrite(BaseQueryMetric updatedQueryMetric) {
        return normalize(delegate.getEventFieldsToWrite(updatedQueryMetric));
    }
    
    @Override
    public boolean isTermFrequencyField(String field) {
        return contentIndexFields.contains(field);
    }
    
    @Override
    public String getTokenFieldNameDesignator() {
        return "";
    }
    
    @Override
    public boolean isIndexOnlyField(String fieldName) {
        return false;
    }
    
    public int getFieldSizeThreshold() {
        return helper.getFieldSizeThreshold();
    }
    
    public static class HelperDelegate<T extends BaseQueryMetric> {
        
        public Multimap<String,String> getEventFieldsToWrite(T updatedQueryMetric) {
            
            HashMultimap<String,String> fields = HashMultimap.create();
            
            SimpleDateFormat sdf_date_time1 = new SimpleDateFormat("yyyyMMdd HHmmss");
            SimpleDateFormat sdf_date_time2 = new SimpleDateFormat("yyyyMMdd HHmmss");
            
            String type = updatedQueryMetric.getQueryType();
            // this is time consuming - we only need to parse the query and write the selectors once
            if (type.equalsIgnoreCase("RunningQuery") && updatedQueryMetric.getNumUpdates() == 0) {
                
                String query = updatedQueryMetric.getQuery();
                
                ASTJexlScript jexlScript = null;
                try {
                    // Parse and flatten here before visitors visit.
                    jexlScript = JexlASTHelper.parseAndFlattenJexlQuery(query);
                } catch (Throwable t1) {
                    // not JEXL, try LUCENE
                    try {
                        LuceneToJexlQueryParser luceneToJexlParser = new LuceneToJexlQueryParser();
                        QueryNode node = luceneToJexlParser.parse(query);
                        String jexlQuery = node.getOriginalQuery();
                        jexlScript = JexlASTHelper.parseAndFlattenJexlQuery(jexlQuery);
                    } catch (Throwable t2) {
                        
                    }
                }
                
                jexlScript = TreeFlatteningRebuildingVisitor.flatten(jexlScript);
                
                if (jexlScript != null) {
                    List<ASTEQNode> positiveEQNodes = JexlASTHelper.getPositiveEQNodes(jexlScript);
                    for (ASTEQNode pos : positiveEQNodes) {
                        String identifier = JexlASTHelper.getIdentifier(pos);
                        Object literal = JexlASTHelper.getLiteralValueSafely(pos);
                        if (identifier != null && literal != null) {
                            fields.put("POSITIVE_SELECTORS", identifier + ":" + literal);
                        }
                    }
                    List<ASTEQNode> negativeEQNodes = JexlASTHelper.getNegativeEQNodes(jexlScript);
                    for (ASTEQNode neg : negativeEQNodes) {
                        String identifier = JexlASTHelper.getIdentifier(neg);
                        Object literal = JexlASTHelper.getLiteralValueSafely(neg);
                        if (identifier != null && literal != null) {
                            fields.put("NEGATIVE_SELECTORS", identifier + ":" + literal);
                        }
                    }
                }
            }
            
            if (updatedQueryMetric.getQueryAuthorizations() != null) {
                fields.put("AUTHORIZATIONS", updatedQueryMetric.getQueryAuthorizations());
            }
            if (updatedQueryMetric.getBeginDate() != null) {
                fields.put("BEGIN_DATE", sdf_date_time1.format(updatedQueryMetric.getBeginDate()));
            }
            fields.put("LOGIN_TIME", Long.toString(updatedQueryMetric.getLoginTime()));
            fields.put("CREATE_CALL_TIME", Long.toString(updatedQueryMetric.getCreateCallTime()));
            if (updatedQueryMetric.getEndDate() != null) {
                fields.put("END_DATE", sdf_date_time1.format(updatedQueryMetric.getEndDate()));
            }
            if (updatedQueryMetric.getHost() != null) {
                fields.put("HOST", updatedQueryMetric.getHost());
            }
            if (updatedQueryMetric.getProxyServers() != null) {
                fields.put("PROXY_SERVERS", StringUtils.join(updatedQueryMetric.getProxyServers(), ","));
            }
            if (updatedQueryMetric.getQuery() != null) {
                fields.put("QUERY", updatedQueryMetric.getQuery());
            }
            if (updatedQueryMetric.getPlan() != null) {
                fields.put("PLAN", updatedQueryMetric.getPlan());
            }
            if (updatedQueryMetric.getQueryId() != null) {
                fields.put("QUERY_ID", updatedQueryMetric.getQueryId());
            }
            if (updatedQueryMetric.getQueryLogic() != null) {
                fields.put("QUERY_LOGIC", updatedQueryMetric.getQueryLogic());
            }
            if (updatedQueryMetric.getQueryType() != null) {
                fields.put("QUERY_TYPE", updatedQueryMetric.getQueryType());
            }
            if (updatedQueryMetric.getQueryName() != null) {
                fields.put("QUERY_NAME", updatedQueryMetric.getQueryName());
            }
            
            Set<Parameter> parameters = updatedQueryMetric.getParameters();
            if (parameters != null && !parameters.isEmpty()) {
                fields.put("PARAMETERS", QueryUtil.toParametersString(parameters));
            }
            if (updatedQueryMetric.getUser() != null) {
                fields.put("USER", updatedQueryMetric.getUser());
            }
            if (updatedQueryMetric.getUserDN() != null) {
                fields.put("USER_DN", updatedQueryMetric.getUserDN());
            }
            if (updatedQueryMetric.getCreateDate() != null) {
                fields.put("CREATE_DATE", sdf_date_time2.format(updatedQueryMetric.getCreateDate()));
            }
            if (updatedQueryMetric.getErrorCode() != null) {
                fields.put("ERROR_CODE", updatedQueryMetric.getErrorCode());
            }
            if (updatedQueryMetric.getErrorMessage() != null) {
                fields.put("ERROR_MESSAGE", updatedQueryMetric.getErrorMessage());
            }
            
            fields.put("SETUP_TIME", Long.toString(updatedQueryMetric.getSetupTime()));
            fields.put("NUM_RESULTS", Long.toString(updatedQueryMetric.getNumResults()));
            fields.put("NUM_PAGES", Long.toString(updatedQueryMetric.getNumPages()));
            if (updatedQueryMetric.getLifecycle() != null) {
                fields.put("LIFECYCLE", updatedQueryMetric.getLifecycle().toString());
            }
            fields.put("LAST_UPDATED", sdf_date_time2.format(updatedQueryMetric.getLastUpdated()));
            fields.put("NUM_UPDATES", Long.toString(updatedQueryMetric.getNumUpdates()));
            fields.put("ELAPSED_TIME", Long.toString(updatedQueryMetric.getLastUpdated().getTime() - updatedQueryMetric.getCreateDate().getTime()));
            List<PageMetric> pageMetrics = updatedQueryMetric.getPageTimes();
            if (pageMetrics != null && !pageMetrics.isEmpty()) {
                for (PageMetric p : pageMetrics) {
                    fields.put("PAGE_METRICS." + p.getPageNumber(), p.toEventString());
                }
            }
            fields.put("SOURCE_COUNT", Long.toString(updatedQueryMetric.getSourceCount()));
            fields.put("NEXT_COUNT", Long.toString(updatedQueryMetric.getNextCount()));
            fields.put("SEEK_COUNT", Long.toString(updatedQueryMetric.getSeekCount()));
            fields.put("YIELD_COUNT", Long.toString(updatedQueryMetric.getYieldCount()));
            fields.put("DOC_RANGES", Long.toString(updatedQueryMetric.getDocRanges()));
            fields.put("FI_RANGES", Long.toString(updatedQueryMetric.getFiRanges()));
            if (updatedQueryMetric.getVersion() != null) {
                fields.put("VERSION", updatedQueryMetric.getVersion());
            }
            Set<Prediction> predictions = updatedQueryMetric.getPredictions();
            if (predictions != null && !predictions.isEmpty()) {
                for (Prediction prediction : predictions) {
                    fields.put("PREDICTION", prediction.getName() + ":" + prediction.getPrediction());
                }
            }
            
            putExtendedFieldsToWrite(updatedQueryMetric, fields);
            
            return fields;
        }
        
        protected void putExtendedFieldsToWrite(T updatedQueryMetric, Multimap<String,String> fields) {
            
        }
        
        public Multimap<String,String> getEventFieldsToDelete(T updatedQueryMetric, T storedQueryMetric) {
            
            HashMultimap<String,String> fields = HashMultimap.create();
            
            SimpleDateFormat sdf_date_time2 = new SimpleDateFormat("yyyyMMdd HHmmss");
            
            if (updatedQueryMetric.getElapsedTime() != storedQueryMetric.getElapsedTime()) {
                fields.put("ELAPSED_TIME", Long.toString(storedQueryMetric.getElapsedTime()));
            }
            
            if (storedQueryMetric.getLastUpdated() != null && updatedQueryMetric.getLastUpdated() != null) {
                String storedValue = sdf_date_time2.format(storedQueryMetric.getLastUpdated());
                String updatedValue = sdf_date_time2.format(updatedQueryMetric.getLastUpdated());
                if (!updatedValue.equals(storedValue)) {
                    fields.put("LAST_UPDATED", storedValue);
                }
            }
            
            fields.put("NUM_UPDATES", Long.toString(storedQueryMetric.getNumUpdates()));
            
            if (!updatedQueryMetric.getLifecycle().equals(storedQueryMetric.getLifecycle())) {
                if (storedQueryMetric.getLifecycle() != null) {
                    fields.put("LIFECYCLE", storedQueryMetric.getLifecycle().toString());
                }
            }
            
            if (updatedQueryMetric.getNumPages() != storedQueryMetric.getNumPages()) {
                fields.put("NUM_PAGES", Long.toString(storedQueryMetric.getNumPages()));
            }
            
            if (updatedQueryMetric.getNumResults() != storedQueryMetric.getNumResults()) {
                fields.put("NUM_RESULTS", Long.toString(storedQueryMetric.getNumResults()));
            }
            
            if (updatedQueryMetric.getSetupTime() != storedQueryMetric.getSetupTime()) {
                fields.put("SETUP_TIME", Long.toString(storedQueryMetric.getSetupTime()));
            }
            
            if (updatedQueryMetric.getLoginTime() != storedQueryMetric.getLoginTime()) {
                fields.put("LOGIN_TIME", Long.toString(storedQueryMetric.getLoginTime()));
            }
            
            Map<Long,PageMetric> storedPageMetricMap = new HashMap<>();
            List<PageMetric> storedPageMetrics = storedQueryMetric.getPageTimes();
            if (storedPageMetrics != null) {
                for (PageMetric p : storedPageMetrics) {
                    storedPageMetricMap.put(p.getPageNumber(), p);
                }
            }
            
            List<PageMetric> updatedPageMetrics = updatedQueryMetric.getPageTimes();
            if (updatedPageMetrics != null) {
                for (PageMetric p : updatedPageMetrics) {
                    long pageNum = p.getPageNumber();
                    PageMetric storedPageMetric = storedPageMetricMap.get(pageNum);
                    if (storedPageMetric != null && !storedPageMetric.equals(p)) {
                        fields.put("PAGE_METRICS." + storedPageMetric.getPageNumber(), storedPageMetric.toEventString());
                    }
                }
            }
            
            if (updatedQueryMetric.getCreateCallTime() != storedQueryMetric.getCreateCallTime()) {
                fields.put("CREATE_CALL_TIME", Long.toString(storedQueryMetric.getCreateCallTime()));
            }
            
            if (updatedQueryMetric.getSourceCount() != storedQueryMetric.getSourceCount()) {
                fields.put("SOURCE_COUNT", Long.toString(storedQueryMetric.getSourceCount()));
            }
            if (updatedQueryMetric.getNextCount() != storedQueryMetric.getNextCount()) {
                fields.put("NEXT_COUNT", Long.toString(storedQueryMetric.getNextCount()));
            }
            if (updatedQueryMetric.getSeekCount() != storedQueryMetric.getSeekCount()) {
                fields.put("SEEK_COUNT", Long.toString(storedQueryMetric.getSeekCount()));
            }
            if (updatedQueryMetric.getYieldCount() != storedQueryMetric.getYieldCount()) {
                fields.put("YIELD_COUNT", Long.toString(storedQueryMetric.getYieldCount()));
            }
            if (updatedQueryMetric.getDocRanges() != storedQueryMetric.getDocRanges()) {
                fields.put("DOC_RANGES", Long.toString(storedQueryMetric.getDocRanges()));
            }
            if (updatedQueryMetric.getFiRanges() != storedQueryMetric.getFiRanges()) {
                fields.put("FI_RANGES", Long.toString(storedQueryMetric.getFiRanges()));
            }
            
            putExtendedFieldsToDelete(updatedQueryMetric, fields);
            
            return fields;
        }
        
        protected void putExtendedFieldsToDelete(T updatedQueryMetric, Multimap<String,String> fields) {
            
        }
    }
}
