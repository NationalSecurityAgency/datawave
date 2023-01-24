package datawave.ingest.mapreduce.handler.shard;

import java.util.Map.Entry;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.webservice.common.logging.ThreadConfigurableLogger;

import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class AbstractColumnBasedHandler<KEYIN> extends ShardedDataTypeHandler<KEYIN> {
    
    public static final String INDEX_MISMATCH = "INDEX_MISMATCH";
    public static final String REVERSE_INDEX_MISMATCH = "REVERSE_INDEX_MISMATCH";
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(AbstractColumnBasedHandler.class);
    
    protected IngestHelperInterface helper = null;
    protected Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
    protected Multimap<String,NormalizedContentInterface> index = HashMultimap.create();
    protected Multimap<String,NormalizedContentInterface> reverse = HashMultimap.create();
    
    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
        TypeRegistry registry = TypeRegistry.getInstance(context.getConfiguration());
        Type type = registry.get(context.getConfiguration().get(DataTypeHelper.Properties.DATA_NAME));
        helper = type.getIngestHelper(context.getConfiguration());
        log.info(this.getClass().getSimpleName() + " configured.");
    }
    
    @Override
    protected Multimap<String,NormalizedContentInterface> getShardNamesAndValues(RawRecordContainer event,
                    Multimap<String,NormalizedContentInterface> eventFields, boolean createGlobalIndexTerms, boolean createGlobalReverseIndexTerms,
                    StatusReporter reporter) {
        
        // Reset
        fields = HashMultimap.create();
        index = HashMultimap.create();
        reverse = HashMultimap.create();
        
        for (Entry<String,NormalizedContentInterface> e : eventFields.entries()) {
            
            // Prune the fields to remove any fields which should not be included in
            // the shard table or shard Index tables
            if (helper.isShardExcluded(e.getValue().getIndexedFieldName())) {
                continue;
            }
            
            // Put the event field name and original value into the fields
            fields.put(e.getValue().getIndexedFieldName(), e.getValue());
            
            // Put the normalized field name and normalized value into the index
            if (createGlobalIndexTerms) {
                if (helper.isIndexedField(e.getValue().getIndexedFieldName())) {
                    index.put(e.getValue().getIndexedFieldName(), e.getValue());
                    
                    if (helper.isAliasedIndexField(e.getValue().getIndexedFieldName())) {
                        for (String alias : helper.getAliasesForIndexedField(e.getValue().getIndexedFieldName())) {
                            NormalizedContentInterface value = (NormalizedContentInterface) e.getValue().clone();
                            value.setFieldName(alias);
                            fields.put(alias, value);
                            index.put(alias, value);
                        }
                    }
                }
            }
            
            // Put the normalized field name and normalized value into the reverse
            if (createGlobalReverseIndexTerms) {
                if (helper.isReverseIndexedField(e.getValue().getIndexedFieldName())) {
                    NormalizedContentInterface rField = (NormalizedContentInterface) (e.getValue().clone());
                    rField.setEventFieldValue(new StringBuilder(rField.getEventFieldValue()).reverse().toString());
                    rField.setIndexedFieldValue(new StringBuilder(rField.getIndexedFieldValue()).reverse().toString());
                    reverse.put(e.getValue().getIndexedFieldName(), rField);
                }
            }
        }
        
        validateIndexedFields(createGlobalIndexTerms, createGlobalReverseIndexTerms, reporter);
        
        return fields;
    }
    
    /**
     * This routine will validate indexed fields in that it will check for fields that are not being indexed, but should have been per some other datasources
     * configuration.
     * 
     * @param checkIndex
     *            flag to check forward index
     * @param checkReverseIndex
     *            flag to check reverse index
     * @param reporter
     *            the status reporter
     */
    protected void validateIndexedFields(boolean checkIndex, boolean checkReverseIndex, StatusReporter reporter) {
        if ((checkIndex || checkReverseIndex) && (reporter != null)) {
            // broken out into separate loops in an effort to keep this as quick as possible
            if (!checkIndex) {
                for (String field : this.fields.keySet()) {
                    validateReverseIndex(field, reporter);
                }
            } else if (!checkReverseIndex) {
                for (String field : this.fields.keySet()) {
                    validateIndex(field, reporter);
                }
            } else {
                for (String field : this.fields.keySet()) {
                    validateIndex(field, reporter);
                    validateReverseIndex(field, reporter);
                }
            }
        }
    }
    
    protected String counterName(String field) {
        return helper.getType().typeName() + '.' + field;
    }
    
    protected void validateIndex(String field, StatusReporter reporter) {
        if (!index.containsKey(field) && helper.shouldHaveBeenIndexed(field)) {
            reporter.getCounter(INDEX_MISMATCH, counterName(field)).increment(1);
        }
    }
    
    protected void validateReverseIndex(String field, StatusReporter reporter) {
        if (!reverse.containsKey(field) && helper.shouldHaveBeenReverseIndexed(field)) {
            reporter.getCounter(REVERSE_INDEX_MISMATCH, counterName(field)).increment(1);
        }
    }
    
    @Override
    protected Multimap<String,NormalizedContentInterface> getGlobalIndexTerms() {
        return index;
    }
    
    @Override
    protected boolean hasIndexTerm(String fieldName) {
        return index.containsKey(fieldName);
    }
    
    @Override
    protected boolean hasReverseIndexTerm(String fieldName) {
        return reverse.containsKey(fieldName);
    }
    
    @Override
    protected Multimap<String,NormalizedContentInterface> getGlobalReverseIndexTerms() {
        return reverse;
    }
    
    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        // Type is ignored, return the configured helper
        return helper;
    }
}
