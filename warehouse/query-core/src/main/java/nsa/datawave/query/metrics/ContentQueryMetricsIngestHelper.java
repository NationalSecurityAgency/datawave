package nsa.datawave.query.metrics;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nsa.datawave.ingest.data.config.NormalizedContentInterface;
import nsa.datawave.ingest.data.config.ingest.CSVIngestHelper;
import nsa.datawave.ingest.data.config.ingest.TermFrequencyIngestHelperInterface;
import nsa.datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import nsa.datawave.query.language.tree.QueryNode;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.webservice.query.QueryImpl.Parameter;
import nsa.datawave.webservice.query.metric.BaseQueryMetric;
import nsa.datawave.webservice.query.metric.BaseQueryMetric.PageMetric;
import nsa.datawave.webservice.query.metric.BaseQueryMetric.Prediction;
import nsa.datawave.webservice.query.util.QueryUtil;

import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

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
                    jexlScript = JexlASTHelper.parseJexlQuery(query);
                } catch (Throwable t1) {
                    // not JEXL, try LUCENE
                    try {
                        LuceneToJexlQueryParser luceneToJexlParser = new LuceneToJexlQueryParser();
                        QueryNode node = luceneToJexlParser.parse(query);
                        String jexlQuery = node.getOriginalQuery();
                        jexlScript = JexlASTHelper.parseJexlQuery(jexlQuery);
                    } catch (Throwable t2) {
                        
                    }
                }
                
                if (jexlScript != null) {
                    List<ASTEQNode> positiveEQNodes = JexlASTHelper.getPositiveEQNodes(jexlScript);
                    for (ASTEQNode pos : positiveEQNodes) {
                        String identifier = JexlASTHelper.getIdentifier(pos);
                        Object literal = JexlASTHelper.getLiteralValue(pos);
                        if (identifier != null && literal != null) {
                            fields.put("POSITIVE_SELECTORS", identifier + ":" + literal.toString());
                        }
                    }
                    List<ASTEQNode> negativeEQNodes = JexlASTHelper.getNegativeEQNodes(jexlScript);
                    for (ASTEQNode neg : negativeEQNodes) {
                        String identifier = JexlASTHelper.getIdentifier(neg);
                        Object literal = JexlASTHelper.getLiteralValue(neg);
                        if (identifier != null && literal != null) {
                            fields.put("NEGATIVE_SELECTORS", identifier + ":" + literal.toString());
                        }
                    }
                }
            }
            
            fields.put("AUTHORIZATIONS", updatedQueryMetric.getQueryAuthorizations());
            if (updatedQueryMetric.getBeginDate() != null) {
                fields.put("BEGIN_DATE", sdf_date_time1.format(updatedQueryMetric.getBeginDate()));
            }
            fields.put("LOGIN_TIME", Long.toString(updatedQueryMetric.getLoginTime()));
            fields.put("CREATE_CALL_TIME", Long.toString(updatedQueryMetric.getCreateCallTime()));
            if (updatedQueryMetric.getEndDate() != null) {
                fields.put("END_DATE", sdf_date_time1.format(updatedQueryMetric.getEndDate()));
            }
            fields.put("HOST", updatedQueryMetric.getHost());
            if (updatedQueryMetric.getProxyServers() != null) {
                fields.put("PROXY_SERVERS", StringUtils.join(updatedQueryMetric.getProxyServers(), ","));
            }
            fields.put("QUERY", updatedQueryMetric.getQuery());
            fields.put("PLAN", updatedQueryMetric.getPlan());
            fields.put("QUERY_ID", updatedQueryMetric.getQueryId());
            fields.put("QUERY_LOGIC", updatedQueryMetric.getQueryLogic());
            fields.put("QUERY_TYPE", updatedQueryMetric.getQueryType());
            fields.put("QUERY_NAME", updatedQueryMetric.getQueryName());
            
            Set<Parameter> parameters = updatedQueryMetric.getParameters();
            if (parameters != null && parameters.isEmpty() == false) {
                fields.put("PARAMETERS", QueryUtil.toParametersString(parameters));
            }
            fields.put("USER", updatedQueryMetric.getUser());
            fields.put("USER_DN", updatedQueryMetric.getUserDN());
            if (updatedQueryMetric.getCreateDate() != null) {
                fields.put("CREATE_DATE", sdf_date_time2.format(updatedQueryMetric.getCreateDate()));
            }
            fields.put("ERROR_CODE", updatedQueryMetric.getErrorCode());
            fields.put("ERROR_MESSAGE", updatedQueryMetric.getErrorMessage());
            
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
            if (pageMetrics != null && pageMetrics.isEmpty() == false) {
                for (PageMetric p : pageMetrics) {
                    fields.put("PAGE_METRICS." + p.getPageNumber(),
                                    p.getPagesize() + "/" + p.getReturnTime() + "/" + p.getCallTime() + "/" + p.getSerializationTime() + "/"
                                                    + p.getBytesWritten() + "/" + p.getPageRequested() + "/" + p.getPageReturned() + "/" + p.getLoginTime());
                }
            }
            fields.put("SOURCE_COUNT", Long.toString(updatedQueryMetric.getSourceCount()));
            fields.put("NEXT_COUNT", Long.toString(updatedQueryMetric.getNextCount()));
            fields.put("SEEK_COUNT", Long.toString(updatedQueryMetric.getSeekCount()));
            fields.put("DOC_RANGES", Long.toString(updatedQueryMetric.getDocRanges()));
            fields.put("FI_RANGES", Long.toString(updatedQueryMetric.getFiRanges()));
            Set<Prediction> predictions = updatedQueryMetric.getPredictions();
            if (predictions != null && predictions.isEmpty() == false) {
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
                if (updatedValue.equals(storedValue) == false) {
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
            
            Map<Long,PageMetric> storedPageMetricMap = new HashMap<Long,PageMetric>();
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
                    if (storedPageMetric != null && storedPageMetric.equals(p) == false) {
                        fields.put("PAGE_METRICS." + pageNum,
                                        storedPageMetric.getPagesize() + "/" + storedPageMetric.getReturnTime() + "/" + storedPageMetric.getCallTime() + "/"
                                                        + storedPageMetric.getSerializationTime() + "/" + storedPageMetric.getBytesWritten() + "/"
                                                        + storedPageMetric.getPageRequested() + "/" + storedPageMetric.getPageReturned() + "/"
                                                        + storedPageMetric.getLoginTime());
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
