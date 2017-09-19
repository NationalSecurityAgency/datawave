package datawave.query.iterator.profile;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.function.serializer.ToStringDocumentSerializer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import datawave.query.function.LogTiming;
import datawave.query.function.serializer.WritableDocumentSerializer;

public class FinalDocumentTrackingIterator implements Iterator<Map.Entry<Key,Value>> {
    
    private Logger log = Logger.getLogger(FinalDocumentTrackingIterator.class);
    private Iterator<Map.Entry<Key,Value>> itr;
    private boolean itrIsDone = false;
    private boolean statsEntryReturned = false;
    private Key lastKey = null;
    
    private Range seekRange = null;
    private DocumentSerialization.ReturnType returnType = null;
    private boolean isReducedResponse = false;
    private boolean isCompressResults = false;
    private QuerySpanCollector querySpanCollector = null;
    private QuerySpan querySpan = null;
    
    public FinalDocumentTrackingIterator(QuerySpanCollector querySpanCollector, QuerySpan querySpan, Range seekRange, Iterator<Map.Entry<Key,Value>> itr,
                    DocumentSerialization.ReturnType returnType, boolean isReducedResponse, boolean isCompressResults) {
        this.itr = itr;
        this.seekRange = seekRange;
        this.returnType = returnType;
        this.isReducedResponse = isReducedResponse;
        this.isCompressResults = isCompressResults;
        this.querySpanCollector = querySpanCollector;
        this.querySpan = querySpan;
    }
    
    private Map.Entry<Key,Value> getStatsEntry(Key key) {
        
        Key statsKey = key;
        if (statsKey == null) {
            statsKey = seekRange.getStartKey();
            if (!seekRange.isStartKeyInclusive()) {
                statsKey = statsKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME);
            }
        }
        
        HashMap<Key,Document> documentMap = new HashMap();
        
        QuerySpan combinedQuerySpan = querySpanCollector.getCombinedQuerySpan(this.querySpan);
        if (combinedQuerySpan != null) {
            Document document = new Document();
            LogTiming.addTimingMetadata(document, combinedQuerySpan);
            documentMap.put(statsKey, document);
        }
        
        Iterator<Map.Entry<Key,Document>> emptyDocumentIterator = documentMap.entrySet().iterator();
        Iterator<Map.Entry<Key,Value>> serializedDocuments = null;
        
        if (returnType == DocumentSerialization.ReturnType.kryo) {
            // Serialize the Document using Kryo
            serializedDocuments = Iterators.transform(emptyDocumentIterator, new KryoDocumentSerializer(isReducedResponse, isCompressResults));
        } else if (returnType == DocumentSerialization.ReturnType.writable) {
            // Use the Writable interface to serialize the Document
            serializedDocuments = Iterators.transform(emptyDocumentIterator, new WritableDocumentSerializer(isReducedResponse));
        } else if (returnType == DocumentSerialization.ReturnType.tostring) {
            // Just return a toString() representation of the document
            serializedDocuments = Iterators.transform(emptyDocumentIterator, new ToStringDocumentSerializer(isReducedResponse));
        } else {
            throw new IllegalArgumentException("Unknown return type of: " + returnType);
        }
        
        return serializedDocuments.next();
    }
    
    @Override
    public boolean hasNext() {
        if (!itrIsDone && itr.hasNext()) {
            return true;
        } else {
            itrIsDone = true;
            if (!statsEntryReturned) {
                if (this.querySpan.hasEntries() || querySpanCollector.hasEntries()) {
                    return true;
                } else {
                    statsEntryReturned = true;
                    return false;
                }
            } else {
                return false;
            }
        }
    }
    
    @Override
    public Map.Entry<Key,Value> next() {
        
        Map.Entry<Key,Value> nextEntry = null;
        if (itrIsDone) {
            if (!statsEntryReturned) {
                nextEntry = getStatsEntry(this.lastKey);
                statsEntryReturned = true;
            }
        } else {
            nextEntry = this.itr.next();
            if (nextEntry != null) {
                this.lastKey = nextEntry.getKey();
            }
        }
        return nextEntry;
    }
    
    @Override
    public void remove() {
        this.itr.remove();
    }
}
