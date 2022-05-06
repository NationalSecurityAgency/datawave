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
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This transform will filter documents based on uniqueness across a set of configured fields. Only the first instance of an event with a unique set of those
 * fields will be returned. This transform is thread safe.
 */
public class UniqueTransform extends DocumentTransform.DefaultDocumentTransform {
    
    private static final Logger log = Logger.getLogger(UniqueTransform.class);
    
    private final UniqueUtil uniqueUtil = new UniqueUtil();
    private BloomFilter<byte[]> bloom;
    private UniqueFields uniqueFields;
    private Multimap<String,String> modelMapping;
    private LinkedList<Entry<Key,Document>> documents;
    
    /**
     * Length of time in milliseconds that a client will wait while results are collected. If a full page is not collected before the timeout, a blank page will
     * be returned to signal the request is still in progress.
     */
    private final long queryExecutionForPageTimeout;
    
    /**
     * Create a new {@link UniqueTransform} that will capture the reverse field mapping defined within the model being used by the logic (if present).
     *
     * @param logic
     *            the logic
     * @param uniqueFields
     *            the set of fields to find unique values for
     */
    public UniqueTransform(QueryModel model, UniqueFields uniqueFields, long queryExecutionForPageTimeout) {
        this.queryExecutionForPageTimeout = queryExecutionForPageTimeout;
        this.documents = new LinkedList<Entry<Key,Document>>();
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
    
    public void updateConfig(UniqueFields uniqueFields, QueryModel model) {
        this.uniqueFields = uniqueFields;
        this.uniqueFields.deconstructIdentifierFields();
        this.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
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
     * Apply uniqueness to a document and if it is unique, stores it in the internal list. Null is returned from this method. #flush() should be called in order
     * to retrieve the next .
     * 
     * @param keyDocumentEntry
     * @return null
     */
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> keyDocumentEntry) {
        if (keyDocumentEntry != null) {
            try {
                if (!FinalDocumentTrackingIterator.isFinalDocumentKey(keyDocumentEntry.getKey())
                                && uniqueUtil.isDuplicate(keyDocumentEntry.getValue(), uniqueFields, modelMapping)) {
                    return null;
                }
            } catch (IOException ioe) {
                log.error("Failed to convert document to bytes.  Returning document as unique.", ioe);
            }
            documents.add(keyDocumentEntry);
        }
        
        long elapsedExecutionTimeForCurrentPage = System.currentTimeMillis() - this.queryExecutionForPageStartTime;
        if (elapsedExecutionTimeForCurrentPage > this.queryExecutionForPageTimeout) {
            Document document = new Document();
            document.setIsIntermediateResult(true);
            return Maps.immutableEntry(new Key(), document);
        }
        
        return null;
    }
    
    /**
     * Returns the next unique document in the pipeline, if there is one. Also checks to see if the timeout for the page has exceeded and interjects an
     * intermediate result instead of hte next unique document if so.
     *
     * @return The next unique document per the configured fields, or an intermediate result (blank document) if the page execution time has elapsed, or null if
     *         there isn't one and the timeout hasn't elapsed.
     */
    @Override
    public Entry<Key,Document> flush() {
        Entry<Key,Document> entry = null;
        if (!documents.isEmpty()) {
            entry = documents.pop();
        }
        
        return entry;
    }
}
