package datawave.microservice.querymetric.config;

import java.util.Collection;

import com.google.common.collect.Multimap;

import datawave.microservice.querymetric.handler.ContentQueryMetricsIngestHelper;

public class AlternateContentQueryMetricsIngestHelper extends ContentQueryMetricsIngestHelper {
    
    public AlternateContentQueryMetricsIngestHelper(boolean deleteMode, Collection<String> ignoredFields) {
        super(deleteMode, new HelperDelegate(ignoredFields));
    }
    
    private static class HelperDelegate<T extends AlternateQueryMetric> extends ContentQueryMetricsIngestHelper.HelperDelegate<AlternateQueryMetric> {
        
        public HelperDelegate(Collection<String> ignoredFields) {
            super(ignoredFields);
        }
        
        @Override
        protected void putExtendedFieldsToWrite(AlternateQueryMetric updated, AlternateQueryMetric stored, Multimap<String,String> fields) {
            if (isFirstWrite(updated.getExtraField(), stored == null ? null : stored.getExtraField())) {
                fields.put("EXTRA_FIELD", updated.getExtraField());
            }
        }
    }
}
