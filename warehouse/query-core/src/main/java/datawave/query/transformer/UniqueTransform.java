package datawave.query.transformer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;
import datawave.query.attributes.Document;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.unique.UniqueUtil;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.model.QueryModel;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This iterator will filter documents based on uniqueness across a set of configured fields. Only the first instance of an event with a unique set of those
 * fields will be returned. This transform is thread safe.
 */
public class UniqueTransform extends DocumentTransform.DefaultDocumentTransform {
    
    private static final Logger log = Logger.getLogger(UniqueTransform.class);
    
    private UniqueUtil uniqueUtil = new UniqueUtil();
    private BloomFilter<byte[]> bloom;
    private UniqueFields uniqueFields;
    private Multimap<String,String> modelMapping;
    
    /**
     * how long (in milliseconds) to let a page of results to collect before signaling to return a blank page to the client (which indicates the request is
     * still in process and all results will be returned at once at the end)
     */
    private final long queryExecutionForPageTimeout;
    
    public UniqueTransform(UniqueFields uniqueFields) {
        this(null, uniqueFields, 0L);
    }
    
    /**
     * Create a new {@link UniqueTransform} that will capture the reverse field mapping defined within the model being used by the logic (if present).
     * 
     * @param model
     *            the query model
     * @param uniqueFields
     *            the set of fields to find unique values for
     */
    public UniqueTransform(QueryModel model, UniqueFields uniqueFields, long queryExecutionForPageTimeout) {
        this.queryExecutionForPageTimeout = queryExecutionForPageTimeout;
        updateConfig(uniqueFields, model);
    }
    
    public void updateConfig(UniqueFields uniqueFields, QueryModel model) {
        this.uniqueFields = uniqueFields;
        this.uniqueFields.deconstructIdentifierFields();
        this.bloom = BloomFilter.create(new UniqueUtil.ByteFunnel(), 500000, 1e-15);
        if (log.isTraceEnabled()) {
            log.trace("unique fields: " + this.uniqueFields.getFields());
        }
        if (model != null) {
            modelMapping = HashMultimap.create();
            // reverse the reverse query mapping which will give us a mapping from the final field name to the original field name(s)
            for (Map.Entry<String,String> entry : model.getReverseQueryMapping().entrySet()) {
                modelMapping.put(entry.getValue(), entry.getKey());
            }
        }
    }
    
    /**
     * Apply uniqueness to a document.
     * 
     * @param keyDocumentEntry
     * @return The document if unique per the configured fields, null otherwise.
     */
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> keyDocumentEntry) {
        long elapsedExecutionTimeForCurrentPage = System.currentTimeMillis() - this.queryExecutionForPageStartTime;
        if (elapsedExecutionTimeForCurrentPage > this.queryExecutionForPageTimeout) {
            Document intermediateResult = new Document();
            intermediateResult.setIsIntermediateResult(true);
            return Maps.immutableEntry(new Key(), intermediateResult);
        }
        
        if (keyDocumentEntry != null) {
            if (FinalDocumentTrackingIterator.isFinalDocumentKey(keyDocumentEntry.getKey())) {
                return keyDocumentEntry;
            }
            
            try {
                if (uniqueUtil.isDuplicate(keyDocumentEntry.getValue(), uniqueFields, modelMapping)) {
                    keyDocumentEntry = null;
                }
            } catch (IOException ioe) {
                log.error("Failed to convert document to bytes.  Returning document as unique.", ioe);
            }
        }
        return keyDocumentEntry;
    }
    
}
