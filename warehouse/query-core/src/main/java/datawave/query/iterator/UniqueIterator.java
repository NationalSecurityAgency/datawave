package datawave.query.iterator;

import com.google.common.base.Predicate;
import datawave.query.attributes.Document;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.unique.UniqueUtil;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.transformer.UniqueTransform;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

/**
 * Query iterator that runs on the t-server for unique queries. Most of hte logic is located in the #UniqueUtil, which essentially applies a bloom filter to
 * determine uniqueness.
 */
public class UniqueIterator {
    
    private static final Logger log = Logger.getLogger(UniqueTransform.class);
    
    private UniqueFields uniqueFields;
    private UniqueUtil uniqueUtil = new UniqueUtil();
    
    public UniqueIterator(UniqueFields uniqueFields) {
        this.uniqueFields = uniqueFields;
        this.uniqueFields.deconstructIdentifierFields();
        if (log.isTraceEnabled()) {
            log.trace("unique fields: " + this.uniqueFields.getFields());
        }
    }
    
    public Predicate<Map.Entry<Key,Document>> getPredicate() {
        return input -> UniqueIterator.this.apply(input) != null;
    }
    
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> keyDocumentEntry) {
        if (keyDocumentEntry != null) {
            if (FinalDocumentTrackingIterator.isFinalDocumentKey(keyDocumentEntry.getKey())) {
                return keyDocumentEntry;
            }
            
            try {
                if (uniqueUtil.isDuplicate(keyDocumentEntry.getValue(), uniqueFields, null)) {
                    keyDocumentEntry = null;
                }
            } catch (IOException ioe) {
                log.error("Failed to convert document to bytes.  Returning document as unique.", ioe);
            }
        }
        return keyDocumentEntry;
    }
    
}
