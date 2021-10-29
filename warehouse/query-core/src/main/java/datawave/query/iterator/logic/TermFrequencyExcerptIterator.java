package datawave.query.iterator.logic;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.data.hash.UIDBuilder;
import datawave.ingest.protobuf.TermWeight;
import datawave.query.Constants;
import datawave.query.data.parsers.DatawaveKey;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * This iterator is intended to scan the term frequencies for a specified document, field, and offset range. The result will be excerpts for the field specified
 * for each document scanned.
 */
public class TermFrequencyExcerptIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    private static final Logger log = Logger.getLogger(TermFrequencyExcerptIterator.class);
    public static final String START_OFFSET = "start.offset";
    public static final String END_OFFSET = "end.offset";
    public static final String FIELD_NAME = "field.name";
    
    protected SortedKeyValueIterator<Key,Value> source;

    protected String fieldName;
    protected int startOffset;
    protected int endOffset;

    protected Range scanRange;
    
    protected Key tk;
    protected Value tv;

    public TermFrequencyExcerptIterator() {}
    
    @Override
    public boolean hasTop() {
        return tk != null;
    }
    
    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;
        
        if (!source.hasTop()) {
            return;
        }
        
        if (log.isTraceEnabled()) {
            log.trace(source.hasTop() + " nexting on " + scanRange);
        }
        
        String[] terms = new String[endOffset - startOffset - 1];
        
        // get the uid
        Key top = source.getTopKey();
        String dtUid = getDtUid(top);
        Text cv = top.getColumnVisibility();
        long ts = top.getTimestamp();
        Text row = top.getRow();
        
        while (source.hasTop() && tk == null) {
            top = source.getTopKey();
            
            if (dtUid.equals(getDtUid(top))) {
                
                String[] fieldAndValue = getFieldAndValue(top);
                if (fieldName.equals(fieldAndValue[0])) {
                    TermWeight.Info.Builder infoBuilder = TermWeight.Info.newBuilder();
                    
                    try {
                        TermWeight.Info info = TermWeight.Info.parseFrom(source.getTopValue().get());
                        
                        // Add each offset into the list maintaining sorted order
                        for (int i = 0; i < info.getTermOffsetCount(); i++) {
                            int offset = info.getTermOffset(i);
                            if (offset >= startOffset && offset <= endOffset) {
                                int index = offset - startOffset;
                                if (terms[index] == null || fieldAndValue[1].length() > terms[index].length()) {
                                    terms[index] = fieldAndValue[1];
                                }
                            }
                        }
                    } catch (InvalidProtocolBufferException e) {
                        log.error("Value found in tf column was not of type TermWeight.Info, skipping", e);
                    }
                    
                }
            } else {
                StringBuilder phrase = new StringBuilder();
                String separator = "";
                for (int i = 0; i < terms.length; i++) {
                    phrase.append(separator);
                    if (terms[i] != null) {
                        phrase.append(terms[i]);
                        separator = " ";
                    } else {
                        separator = "";
                    }
                }
                
                tk = new Key(row, new Text(dtUid), new Text(fieldName + Constants.NULL + phrase), cv, ts);
                tv = new Value();
            }
        }
    }
    
    /**
     * Get the field from the end of the column qualifier
     * 
     * @param tfKey
     * @return the field name
     */
    private String[] getFieldAndValue(Key tfKey) {
        String cq = tfKey.getColumnQualifier().toString();
        int index = cq.lastIndexOf(Constants.NULL);
        String fieldname = cq.substring(index + 1);
        int index2 = cq.lastIndexOf(Constants.NULL, index - 1);
        String fieldvalue = cq.substring(index2 + 1, index);
        return new String[] {fieldname, fieldvalue};
    }
    
    /**
     * get the dt and uid from a tf key
     * 
     * @param tfKey
     * @return the dt\x00uid
     */
    private String getDtUid(Key tfKey) {
        String cq = tfKey.getColumnQualifier().toString();
        int index = cq.indexOf(Constants.NULL);
        index = cq.indexOf(Constants.NULL, index + 1);
        return cq.substring(0, index);
    }
    
    @Override
    public Key getTopKey() {
        return tk;
    }
    
    @Override
    public Value getTopValue() {
        return tv;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        TermFrequencyExcerptIterator it = new TermFrequencyExcerptIterator();
        it.startOffset = startOffset;
        it.endOffset = endOffset;
        it.fieldName = fieldName;
        it.source = source.deepCopy(env);
        return it;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        this.startOffset = Integer.parseInt(options.get(START_OFFSET));
        this.endOffset = Integer.parseInt(options.get(END_OFFSET));
        this.fieldName = options.get(FIELD_NAME);
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        Key startKey;
        if (range.isStartKeyInclusive()) {
            startKey = new Key(range.getStartKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(range.getStartKey().getColumnFamily().toString() + Constants.NULL));
        } else {
            // need to start at the next UID
            startKey = new Key(range.getStartKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(range.getStartKey().getColumnFamily().toString() + '.'));
        }

        Key endKey;
        if (range.isEndKeyInclusive()) {
            endKey = new Key(range.getEndKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(range.getStartKey().getColumnFamily().toString() + '.'));
        } else {
            endKey = new Key(range.getEndKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(range.getStartKey().getColumnFamily().toString() + Constants.NULL));
        }

        this.scanRange = new Range(startKey, false, endKey, false);
        
        if (log.isTraceEnabled()) {
            log.trace(this + " seek'ing to: " + this.scanRange + " from requested range " + range);
        }
        
        source.seek(this.scanRange, Collections.emptyList(), false);
        next();
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TermFrequencyContextIterator: ");
        sb.append(this.fieldName);
        sb.append(", ");
        sb.append(this.startOffset);
        sb.append(", ");
        sb.append(this.endOffset);
        
        return sb.toString();
    }
    
    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = new IteratorOptions(TermFrequencyExcerptIterator.class.getSimpleName(),
                        "An iterator that returns excepts from the scanned documents", null, null);
        options.addNamedOption(FIELD_NAME, "The token field name for which to get excerpts (required)");
        options.addNamedOption(START_OFFSET, "The start offset for the excerpt (required)");
        options.addNamedOption(END_OFFSET, "The end offset for the excerpt (required)");
        return options;
    }
    
    @Override
    public boolean validateOptions(Map<String,String> map) {
        if (map.containsKey(FIELD_NAME)) {
            if (map.get(FIELD_NAME).isEmpty()) {
                return false;
            }
        } else {
            return false;
        }
        
        if (map.containsKey(START_OFFSET)) {
            try {
                Integer.parseInt(map.get(START_OFFSET));
            } catch (Exception e) {
                return false;
            }
        } else {
            return false;
        }
        
        if (map.containsKey(END_OFFSET)) {
            try {
                Integer.parseInt(map.get(END_OFFSET));
            } catch (Exception e) {
                return false;
            }
        } else {
            return false;
        }
        
        return true;
    }
}
