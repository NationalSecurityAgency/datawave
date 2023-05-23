package datawave.query.iterator.profile;

import com.google.common.collect.Iterators;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import datawave.query.function.LogTiming;
import datawave.query.function.serializer.JsonDocumentSerializer;
import datawave.query.function.serializer.JsonObjectSerializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.function.serializer.ToStringDocumentSerializer;
import datawave.query.function.serializer.WritableDocumentSerializer;
import datawave.query.iterator.Util;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FinalDocumentTrackingIterator implements Iterator<Map.Entry<Key,Value>> {
    
    private Logger log = Logger.getLogger(FinalDocumentTrackingIterator.class);
    private Iterator<Map.Entry<Key,Value>> itr;
    private boolean itrIsDone = false;
    private boolean statsEntryReturned = false;
    private Key lastKey = null;
    
    private static final Text MARKER_TEXT = new Text("\u2735FinalDocument\u2735");
    private static final ByteSequence MARKER_SEQUENCE = new ArrayByteSequence(MARKER_TEXT.getBytes(), 0, MARKER_TEXT.getLength());
    
    private Range seekRange = null;
    private DocumentSerialization.ReturnType returnType = null;
    private boolean isReducedResponse = false;
    private boolean isCompressResults = false;
    private QuerySpanCollector querySpanCollector = null;
    private QuerySpan querySpan = null;
    private YieldCallback yield = null;
    
    public FinalDocumentTrackingIterator(QuerySpanCollector querySpanCollector, QuerySpan querySpan, Range seekRange, Iterator<Map.Entry<Key,Value>> itr,
                    DocumentSerialization.ReturnType returnType, boolean isReducedResponse, boolean isCompressResults, YieldCallback<Key> yield) {
        this.itr = itr;
        this.seekRange = seekRange;
        this.returnType = returnType;
        this.isReducedResponse = isReducedResponse;
        this.isCompressResults = isCompressResults;
        this.querySpanCollector = querySpanCollector;
        this.querySpan = querySpan;
        this.yield = yield;
        
        // check for the special case where we were torn down just after returning the final document
        this.itrIsDone = this.statsEntryReturned = isStatsEntryReturned(seekRange);
    }
    
    private boolean isStatsEntryReturned(Range r) {
        // first check if this is a rebuild key (post teardown)
        if (!r.isStartKeyInclusive()) {
            // now check if the start key is a final document return
            return isFinalDocumentKey(r.getStartKey());
        }
        return false;
    }
    
    public static boolean isFinalDocumentKey(Key k) {
        if (k != null && k.getColumnQualifierData() != null) {
            ByteSequence bytes = k.getColumnQualifierData();
            if (bytes.length() >= MARKER_TEXT.getLength()) {
                return (bytes.subSequence(bytes.length() - MARKER_TEXT.getLength(), bytes.length()).compareTo(MARKER_SEQUENCE) == 0);
            }
        }
        return false;
    }
    
    private Map.Entry<Key,Value> getStatsEntry(Key statsKey) {
        
        // now add our marker
        statsKey = new Key(statsKey.getRow(), statsKey.getColumnFamily(), Util.appendText(statsKey.getColumnQualifier(), MARKER_TEXT),
                        statsKey.getColumnVisibility(), statsKey.getTimestamp());
        
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
        } else if (returnType == DocumentSerialization.ReturnType.json) {
            // Use the Writable interface to serialize the Document
            serializedDocuments = Iterators.transform(emptyDocumentIterator, new JsonObjectSerializer(isReducedResponse));
        } else if (returnType == DocumentSerialization.ReturnType.jsondocument) {
            // Use the Writable interface to serialize the Document
            serializedDocuments = Iterators.transform(emptyDocumentIterator, new JsonDocumentSerializer(isReducedResponse));
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
            // if we yielded, then leave gracefully
            // checking again as this is after the itr.hasNext() call above which may cause a yield
            if (yield != null && yield.hasYielded()) {
                return false;
            }
            
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
        if (yield == null || !yield.hasYielded()) {
            if (itrIsDone) {
                if (!statsEntryReturned) {
                    
                    // determine the key to append the stats entry to
                    Key statsKey = lastKey;
                    
                    // if no last key, then use the startkey of the range
                    if (statsKey == null) {
                        statsKey = seekRange.getStartKey();
                        if (!seekRange.isStartKeyInclusive()) {
                            statsKey = statsKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME);
                        }
                    }
                    
                    nextEntry = getStatsEntry(statsKey);
                    
                    statsEntryReturned = true;
                }
            } else {
                nextEntry = this.itr.next();
                if (nextEntry != null) {
                    this.lastKey = nextEntry.getKey();
                }
            }
        }
        return nextEntry;
    }
    
    @Override
    public void remove() {
        this.itr.remove();
    }
}
