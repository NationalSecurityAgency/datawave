package datawave.query.transformer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.attributes.Document;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.unique.UniqueUtil;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.model.QueryModel;
import datawave.query.tables.ShardQueryLogic;
import datawave.webservice.query.logic.BaseQueryLogic;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
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
    
    /**
     * how long (in milliseconds) to let a page of results to collect before signaling to return a blank page to the client (which indicates the request is
     * still in process and all results will be returned at once at the end)
     */
    private final long queryExecutionForPageTimeout;
    
    /**
     * whether this kind of query (a group by query) is allowed to exceed the standard timeout. If true, queryExecutionForPageTimeout will be used
     */
    private final boolean allowLongRunningQuery;
    
    // private final BloomFilter<byte[]> bloom;
    private UniqueFields uniqueFields;
    private Multimap<String,String> modelMapping;
    
    /**
     * Create a new {@link UniqueTransform} that will capture the reverse field mapping defined within the model being used by the logic (if present).
     * 
     * @param logic
     *            the logic
     * @param uniqueFields
     *            the set of fields to find unique values for
     */
    public UniqueTransform(BaseQueryLogic<Entry<Key,Value>> logic, UniqueFields uniqueFields, long queryExecutionForPageTimeout, boolean allowLongRunningQuery) {
        this.uniqueFields = uniqueFields;
        this.uniqueFields.deconstructIdentifierFields();
        // this.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
        if (log.isTraceEnabled()) {
            log.trace("unique fields: " + this.uniqueFields.getFields());
        }
        
        this.queryExecutionForPageTimeout = queryExecutionForPageTimeout;
        this.allowLongRunningQuery = allowLongRunningQuery;
        if (logic != null) {
            QueryModel model = ((ShardQueryLogic) logic).getQueryModel();
            if (model != null) {
                modelMapping = HashMultimap.create();
                // reverse the reverse query mapping which will give us a mapping from the final field name to the original field name(s)
                for (Map.Entry<String,String> entry : model.getReverseQueryMapping().entrySet()) {
                    modelMapping.put(entry.getValue(), entry.getKey());
                }
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
