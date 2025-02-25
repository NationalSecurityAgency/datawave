package datawave.microservice.querymetric.handler;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.core.query.util.QueryUtil;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import datawave.ingest.data.config.ingest.TermFrequencyIngestHelperInterface;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;

public class ContentQueryMetricsIngestHelper extends CSVIngestHelper implements TermFrequencyIngestHelperInterface {
    
    private static final Logger log = LoggerFactory.getLogger(ContentQueryMetricsIngestHelper.class);
    private static final Integer MAX_FIELD_VALUE_LENGTH = 500000;
    
    private Set<String> contentIndexFields = new HashSet<>();
    private HelperDelegate<BaseQueryMetric> delegate;
    
    public ContentQueryMetricsIngestHelper(boolean deleteMode, Collection<String> ignoredFields) {
        this(deleteMode, new HelperDelegate<>(ignoredFields));
    }
    
    public ContentQueryMetricsIngestHelper(boolean deleteMode, HelperDelegate<BaseQueryMetric> delegate) {
        this.deleteMode = deleteMode;
        this.delegate = delegate;
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
    
    public Multimap<String,NormalizedContentInterface> getEventFieldsToWrite(BaseQueryMetric updatedQueryMetric, BaseQueryMetric storedQueryMetric) {
        return normalize(delegate.getEventFieldsToWrite(updatedQueryMetric, storedQueryMetric));
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
        private Collection<String> ignoredFields = Collections.EMPTY_LIST;
        
        public HelperDelegate() {}
        
        public HelperDelegate(Collection<String> ignoredFields) {
            this.ignoredFields = ignoredFields;
        }
        
        protected boolean isChanged(String updated, String stored) {
            if ((StringUtils.isBlank(stored) && StringUtils.isNotBlank(updated)) || (stored != null && updated != null && !stored.equals(updated))) {
                return true;
            } else {
                return false;
            }
        }
        
        protected boolean isChanged(long updated, long stored) {
            if (updated != stored) {
                return true;
            } else {
                return false;
            }
        }
        
        protected boolean isChanged(BaseQueryMetric.Lifecycle updated, BaseQueryMetric.Lifecycle stored) {
            if ((stored == null && updated != null) || (stored != null && updated != null && (stored.ordinal() != updated.ordinal()))) {
                return true;
            } else {
                return false;
            }
        }
        
        protected boolean isFirstWrite(Collection<?> updated, Collection<?> stored) {
            if ((stored == null || stored.isEmpty()) && (updated != null && !updated.isEmpty())) {
                return true;
            } else {
                return false;
            }
        }
        
        protected boolean isFirstWrite(Map<?,?> updated, Map<?,?> stored) {
            if ((stored == null || stored.isEmpty()) && (updated != null && !updated.isEmpty())) {
                return true;
            } else {
                return false;
            }
        }
        
        protected boolean isFirstWrite(String updated, String stored) {
            if (stored == null && StringUtils.isNotBlank(updated)) {
                return true;
            } else {
                return false;
            }
        }
        
        protected boolean isFirstWrite(Object updated, Object stored) {
            if (stored == null && updated != null) {
                return true;
            } else {
                return false;
            }
        }
        
        protected boolean isFirstWrite(long updated, long stored, long initValue) {
            if (stored == initValue && updated != initValue) {
                return true;
            } else {
                return false;
            }
        }
        
        public Multimap<String,String> getEventFieldsToWrite(T updated, T stored) {
            
            HashMultimap<String,String> fields = HashMultimap.create();
            
            SimpleDateFormat sdf_date_time1 = new SimpleDateFormat("yyyyMMdd HHmmss");
            SimpleDateFormat sdf_date_time2 = new SimpleDateFormat("yyyyMMdd HHmmss");
            
            if (!ignoredFields.contains("POSITIVE_SELECTORS")) {
                if (isFirstWrite(updated.getPositiveSelectors(), stored == null ? null : stored.getPositiveSelectors())) {
                    fields.putAll("POSITIVE_SELECTORS", updated.getPositiveSelectors());
                }
            }
            if (!ignoredFields.contains("NEGATIVE_SELECTORS")) {
                if (isFirstWrite(updated.getNegativeSelectors(), stored == null ? null : stored.getNegativeSelectors())) {
                    fields.putAll("NEGATIVE_SELECTORS", updated.getNegativeSelectors());
                }
            }
            if (!ignoredFields.contains("AUTHORIZATIONS")) {
                if (isFirstWrite(updated.getQueryAuthorizations(), stored == null ? null : stored.getQueryAuthorizations())) {
                    fields.put("AUTHORIZATIONS", updated.getQueryAuthorizations());
                }
            }
            if (!ignoredFields.contains("BEGIN_DATE")) {
                if (isFirstWrite(updated.getBeginDate(), stored == null ? null : stored.getBeginDate())) {
                    fields.put("BEGIN_DATE", sdf_date_time1.format(updated.getBeginDate()));
                }
            }
            if (isChanged(updated.getCreateCallTime(), stored == null ? -1 : stored.getCreateCallTime())) {
                fields.put("CREATE_CALL_TIME", Long.toString(updated.getCreateCallTime()));
            }
            if (isFirstWrite(updated.getCreateDate(), stored == null ? null : stored.getCreateDate())) {
                fields.put("CREATE_DATE", sdf_date_time2.format(updated.getCreateDate()));
            }
            if (isChanged(updated.getDocRanges(), stored == null ? -1 : stored.getDocRanges())) {
                fields.put("DOC_RANGES", Long.toString(updated.getDocRanges()));
            }
            if (isChanged(updated.getDocSize(), stored == null ? -1 : stored.getDocSize())) {
                fields.put("DOC_SIZE", Long.toString(updated.getDocSize()));
            }
            if (isChanged(updated.getElapsedTime(), stored == null ? -1 : stored.getElapsedTime())) {
                fields.put("ELAPSED_TIME", Long.toString(updated.getElapsedTime()));
            }
            if (!ignoredFields.contains("END_DATE")) {
                if (isFirstWrite(updated.getEndDate(), stored == null ? null : stored.getEndDate())) {
                    fields.put("END_DATE", sdf_date_time1.format(updated.getEndDate()));
                }
            }
            if (isChanged(updated.getErrorCode(), stored == null ? null : stored.getErrorCode())) {
                fields.put("ERROR_CODE", updated.getErrorCode());
            }
            if (isChanged(updated.getErrorMessage(), stored == null ? null : stored.getErrorMessage())) {
                fields.put("ERROR_MESSAGE", updated.getErrorMessage());
            }
            if (isChanged(updated.getFiRanges(), stored == null ? -1 : stored.getFiRanges())) {
                fields.put("FI_RANGES", Long.toString(updated.getFiRanges()));
            }
            if (isFirstWrite(updated.getHost(), stored == null ? null : stored.getHost())) {
                fields.put("HOST", updated.getHost());
            }
            if (updated.getLastUpdated() != null) {
                try {
                    String storedValue = "";
                    if (stored != null && stored.getLastUpdated() != null) {
                        storedValue = sdf_date_time2.format(stored.getLastUpdated());
                    }
                    String updatedValue = sdf_date_time2.format(updated.getLastUpdated());
                    if (!updatedValue.isEmpty() && !updatedValue.equals(storedValue)) {
                        fields.put("LAST_UPDATED", updatedValue);
                    }
                } catch (Exception e) {
                    log.error("lastUpdated:" + e.getMessage());
                }
            }
            if (isChanged(updated.getLifecycle(), stored == null ? null : stored.getLifecycle())) {
                fields.put("LIFECYCLE", updated.getLifecycle().toString());
            }
            if (isChanged(updated.getLoginTime(), stored == null ? -1 : stored.getLoginTime())) {
                fields.put("LOGIN_TIME", Long.toString(updated.getLoginTime()));
            }
            if (isChanged(updated.getNextCount(), stored == null ? -1 : stored.getNextCount())) {
                fields.put("NEXT_COUNT", Long.toString(updated.getNextCount()));
            }
            if (isChanged(updated.getNumResults(), stored == null ? -1 : stored.getNumResults())) {
                fields.put("NUM_RESULTS", Long.toString(updated.getNumResults()));
            }
            if (isChanged(updated.getNumPages(), stored == null ? -1 : stored.getNumPages())) {
                fields.put("NUM_PAGES", Long.toString(updated.getNumPages()));
            }
            if (isChanged(updated.getNumUpdates(), stored == null ? -1 : stored.getNumUpdates())) {
                fields.put("NUM_UPDATES", Long.toString(updated.getNumUpdates()));
            }
            if (!ignoredFields.contains("PARAMETERS")) {
                if (isFirstWrite(updated.getParameters(), stored == null ? null : stored.getParameters())) {
                    fields.put("PARAMETERS", QueryUtil.toParametersString(updated.getParameters()));
                }
            }
            if (isChanged(updated.getPlan(), stored == null ? null : stored.getPlan())) {
                fields.put("PLAN", updated.getPlan());
            }
            if (!ignoredFields.contains("PROXY_SERVERS")) {
                if (isFirstWrite(updated.getProxyServers(), stored == null ? null : stored.getProxyServers())) {
                    fields.put("PROXY_SERVERS", StringUtils.join(updated.getProxyServers(), ","));
                }
            }
            
            Map<Long,PageMetric> storedPageMetricMap = new HashMap<>();
            if (stored != null) {
                List<PageMetric> storedPageMetrics = stored.getPageTimes();
                if (storedPageMetrics != null) {
                    for (PageMetric p : storedPageMetrics) {
                        storedPageMetricMap.put(p.getPageNumber(), p);
                    }
                }
            }
            if (updated != null) {
                List<PageMetric> updatedPageMetrics = updated.getPageTimes();
                if (updatedPageMetrics != null) {
                    for (PageMetric p : updatedPageMetrics) {
                        long pageNum = p.getPageNumber();
                        PageMetric storedPageMetric = storedPageMetricMap.get(pageNum);
                        if (storedPageMetric == null || !storedPageMetric.equals(p)) {
                            fields.put("PAGE_METRICS." + p.getPageNumber(), p.toEventString());
                        }
                    }
                }
            }
            if (isFirstWrite(updated.getPredictions(), stored == null ? null : stored.getPredictions())) {
                Set<Prediction> predictions = updated.getPredictions();
                if (predictions != null && !predictions.isEmpty()) {
                    for (Prediction prediction : predictions) {
                        fields.put("PREDICTION", prediction.getName() + ":" + prediction.getPrediction());
                    }
                }
            }
            if (!ignoredFields.contains("QUERY")) {
                if (isFirstWrite(updated.getQuery(), stored == null ? null : stored.getQuery())) {
                    fields.put("QUERY", updated.getQuery());
                }
            }
            if (isFirstWrite(updated.getQueryId(), stored == null ? null : stored.getQueryId())) {
                fields.put("QUERY_ID", updated.getQueryId());
            }
            if (!ignoredFields.contains("QUERY_LOGIC")) {
                if (isFirstWrite(updated.getQueryLogic(), stored == null ? null : stored.getQueryLogic())) {
                    fields.put("QUERY_LOGIC", updated.getQueryLogic());
                }
            }
            if (!ignoredFields.contains("QUERY_NAME")) {
                if (isFirstWrite(updated.getQueryName(), stored == null ? null : stored.getQueryName())) {
                    fields.put("QUERY_NAME", updated.getQueryName());
                }
            }
            if (!ignoredFields.contains("QUERY_TYPE")) {
                if (isFirstWrite(updated.getQueryType(), stored == null ? null : stored.getQueryType())) {
                    fields.put("QUERY_TYPE", updated.getQueryType());
                }
            }
            if (isFirstWrite(updated.getSetupTime(), stored == null ? -1 : stored.getSetupTime(), -1)) {
                fields.put("SETUP_TIME", Long.toString(updated.getSetupTime()));
            }
            if (isChanged(updated.getSeekCount(), stored == null ? -1 : stored.getSeekCount())) {
                fields.put("SEEK_COUNT", Long.toString(updated.getSeekCount()));
            }
            if (isChanged(updated.getSourceCount(), stored == null ? -1 : stored.getSourceCount())) {
                fields.put("SOURCE_COUNT", Long.toString(updated.getSourceCount()));
            }
            if (!ignoredFields.contains("USER")) {
                if (isFirstWrite(updated.getUser(), stored == null ? null : stored.getUser())) {
                    fields.put("USER", updated.getUser());
                }
            }
            if (!ignoredFields.contains("USER_DN")) {
                if (isFirstWrite(updated.getUserDN(), stored == null ? null : stored.getUserDN())) {
                    fields.put("USER_DN", updated.getUserDN());
                }
            }
            if (!ignoredFields.contains("VERSION")) {
                if (isFirstWrite(updated.getVersionMap(), stored == null ? null : stored.getVersionMap())) {
                    Map<String,String> versionMap = updated.getVersionMap();
                    if (versionMap != null) {
                        versionMap.entrySet().stream().forEach(e -> {
                            fields.put("VERSION." + e.getKey().toUpperCase(), e.getValue());
                        });
                    }
                }
            }
            if (isChanged(updated.getYieldCount(), stored == null ? -1 : stored.getYieldCount())) {
                fields.put("YIELD_COUNT", Long.toString(updated.getYieldCount()));
            }
            
            putExtendedFieldsToWrite(updated, stored, fields);
            
            HashMultimap<String,String> truncatedFields = HashMultimap.create();
            fields.entries().forEach(e -> {
                if (e.getValue().length() > MAX_FIELD_VALUE_LENGTH) {
                    truncatedFields.put(e.getKey(), e.getValue().substring(0, MAX_FIELD_VALUE_LENGTH) + "<truncated>");
                } else {
                    truncatedFields.put(e.getKey(), e.getValue());
                }
            });
            return truncatedFields;
        }
        
        protected void putExtendedFieldsToWrite(T updated, T stored, Multimap<String,String> fields) {
            
        }
        
        public Multimap<String,String> getEventFieldsToDelete(T updated, T stored) {
            
            HashMultimap<String,String> fields = HashMultimap.create();
            if (updated != null && stored != null) {
                
                SimpleDateFormat sdf_date_time2 = new SimpleDateFormat("yyyyMMdd HHmmss");
                
                if (isChanged(updated.getCreateCallTime(), stored.getCreateCallTime())) {
                    fields.put("CREATE_CALL_TIME", Long.toString(stored.getCreateCallTime()));
                }
                if (isChanged(updated.getDocRanges(), stored.getDocRanges())) {
                    fields.put("DOC_RANGES", Long.toString(stored.getDocRanges()));
                }
                if (isChanged(updated.getDocSize(), stored.getDocSize())) {
                    fields.put("DOC_SIZE", Long.toString(stored.getDocSize()));
                }
                if (isChanged(updated.getElapsedTime(), stored.getElapsedTime())) {
                    fields.put("ELAPSED_TIME", Long.toString(stored.getElapsedTime()));
                }
                if (isChanged(updated.getFiRanges(), stored.getFiRanges())) {
                    fields.put("FI_RANGES", Long.toString(stored.getFiRanges()));
                }
                if (stored.getLastUpdated() != null && updated.getLastUpdated() != null) {
                    try {
                        String storedValue = sdf_date_time2.format(stored.getLastUpdated());
                        String updatedValue = sdf_date_time2.format(updated.getLastUpdated());
                        if (!updatedValue.equals(storedValue)) {
                            fields.put("LAST_UPDATED", storedValue);
                        }
                    } catch (Exception e) {
                        log.error("lastUpdated:" + e.getMessage());
                    }
                }
                if (isChanged(updated.getLifecycle(), stored.getLifecycle())) {
                    fields.put("LIFECYCLE", stored.getLifecycle().toString());
                }
                if (isChanged(updated.getLoginTime(), stored.getLoginTime())) {
                    fields.put("LOGIN_TIME", Long.toString(stored.getLoginTime()));
                }
                if (isChanged(updated.getNumUpdates(), stored.getNumUpdates())) {
                    fields.put("NUM_UPDATES", Long.toString(stored.getNumUpdates()));
                }
                if (isChanged(updated.getNextCount(), stored.getNextCount())) {
                    fields.put("NEXT_COUNT", Long.toString(stored.getNextCount()));
                }
                if (isChanged(updated.getNumPages(), stored.getNumPages())) {
                    fields.put("NUM_PAGES", Long.toString(stored.getNumPages()));
                }
                if (isChanged(updated.getNumResults(), stored.getNumResults())) {
                    fields.put("NUM_RESULTS", Long.toString(stored.getNumResults()));
                }
                Map<Long,PageMetric> storedPageMetricMap = new HashMap<>();
                if (stored != null) {
                    List<PageMetric> storedPageMetrics = stored.getPageTimes();
                    if (storedPageMetrics != null) {
                        for (PageMetric p : storedPageMetrics) {
                            storedPageMetricMap.put(p.getPageNumber(), p);
                        }
                    }
                }
                if (updated != null) {
                    List<PageMetric> updatedPageMetrics = updated.getPageTimes();
                    if (updatedPageMetrics != null) {
                        for (PageMetric p : updatedPageMetrics) {
                            long pageNum = p.getPageNumber();
                            PageMetric storedPageMetric = storedPageMetricMap.get(pageNum);
                            if (storedPageMetric != null && !storedPageMetric.equals(p)) {
                                fields.put("PAGE_METRICS." + storedPageMetric.getPageNumber(), storedPageMetric.toEventString());
                            }
                        }
                    }
                }
                if (stored.getPlan() != null) {
                    if (stored.getPlan() != null && isChanged(updated.getPlan(), stored.getPlan())) {
                        fields.put("PLAN", stored.getPlan());
                    }
                }
                if (isChanged(updated.getSeekCount(), stored.getSeekCount())) {
                    fields.put("SEEK_COUNT", Long.toString(stored.getSeekCount()));
                }
                if (isChanged(updated.getSetupTime(), stored.getSetupTime())) {
                    fields.put("SETUP_TIME", Long.toString(stored.getSetupTime()));
                }
                if (isChanged(updated.getSourceCount(), stored.getSourceCount())) {
                    fields.put("SOURCE_COUNT", Long.toString(stored.getSourceCount()));
                }
                if (isChanged(updated.getYieldCount(), stored.getYieldCount())) {
                    fields.put("YIELD_COUNT", Long.toString(stored.getYieldCount()));
                }
                putExtendedFieldsToDelete(updated, stored, fields);
            }
            HashMultimap<String,String> truncatedFields = HashMultimap.create();
            fields.entries().forEach(e -> {
                if (e.getValue().length() > MAX_FIELD_VALUE_LENGTH) {
                    truncatedFields.put(e.getKey(), e.getValue().substring(0, MAX_FIELD_VALUE_LENGTH) + "<truncated>");
                } else {
                    truncatedFields.put(e.getKey(), e.getValue());
                }
            });
            return truncatedFields;
        }
        
        protected void putExtendedFieldsToDelete(T updated, T stored, Multimap<String,String> fields) {
            
        }
    }
}
