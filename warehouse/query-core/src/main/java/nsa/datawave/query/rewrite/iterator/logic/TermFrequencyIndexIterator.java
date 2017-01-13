package nsa.datawave.query.rewrite.iterator.logic;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

import nsa.datawave.query.data.parsers.DatawaveKey;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.attributes.PreNormalizedAttributeFactory;
import nsa.datawave.query.rewrite.iterator.DocumentIterator;
import nsa.datawave.query.rewrite.jexl.functions.FieldIndexAggregator;
import nsa.datawave.query.rewrite.jexl.functions.TermFrequencyAggregator;
import nsa.datawave.query.rewrite.predicate.TimeFilter;
import nsa.datawave.query.util.TypeMetadata;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

/**
 * Scans a bounds within a column qualifier. This iterator needs to: - 1) Be given a global Range (ie, [-inf,+inf]) - 2) Select an arbitrary column family (ie,
 * "fi\u0000FIELD") - 3) Given a prefix, scan all keys that have a column qualifer that has that prefix that occur in the column family for all rows in a tablet
 * 
 */
public class TermFrequencyIndexIterator implements SortedKeyValueIterator<Key,Value>, DocumentIterator {
    private static final Logger log = Logger.getLogger(TermFrequencyIndexIterator.class);
    
    public static final String INDEX_FILTERING_CLASSES = "indexfiltering.classes";
    
    protected SortedKeyValueIterator<Key,Value> source;
    
    protected final Text columnFamily;
    protected final Collection<ByteSequence> seekColumnFamilies;
    protected final boolean includeColumnFamilies;
    
    // used for managing parent calls to seek
    protected Range scanRange;
    
    protected Key tk;
    protected Value tv;
    
    protected Pattern pattern = null;
    
    protected boolean negated = false;
    
    protected final String field;
    
    protected PreNormalizedAttributeFactory attributeFactory;
    protected Document document;
    protected boolean buildDocument = false;
    protected Predicate<Key> datatypeFilter;
    protected final FieldIndexAggregator aggregation;
    protected TimeFilter timeFilter;
    
    private DatawaveKey startKeyParser;
    
    private DatawaveKey stopKeyParser;
    
    /**
     * A convenience constructor that allows all keys to pass through unmodified from the source.
     * 
     * @param source
     */
    public TermFrequencyIndexIterator(Key fiStartKey, Key fiEndKey, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter) {
        this(fiStartKey, fiEndKey, source, timeFilter, null, false, Predicates.<Key> alwaysTrue(), new TermFrequencyAggregator(null, null));
    }
    
    public TermFrequencyIndexIterator(Key fiStartKey, Key fiEndKey, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    boolean buildDocument, Predicate<Key> datatypeFilter, FieldIndexAggregator aggregator) {
        
        this.datatypeFilter = datatypeFilter;
        
        this.source = source;
        this.timeFilter = timeFilter;
        
        // Build the cf: fi\x00FIELD_NAME
        this.columnFamily = Constants.TERM_FREQUENCY_COLUMN_FAMILY;
        
        seekColumnFamilies = Lists.newArrayList();
        seekColumnFamilies.add(new ArrayByteSequence(columnFamily.getBytes()));
        
        startKeyParser = new DatawaveKey(fiStartKey);
        stopKeyParser = new DatawaveKey(fiEndKey);
        
        this.includeColumnFamilies = true;
        
        document = new Document();
        
        this.field = startKeyParser.getFieldName().toUpperCase();
        
        Key startKey = new Key(startKeyParser.getRow(), columnFamily, new Text(startKeyParser.getDataType() + Constants.NULL + startKeyParser.getUid()
                        + Constants.NULL + startKeyParser.getFieldValue()));
        Key endKey = new Key(startKeyParser.getRow(), columnFamily, new Text(startKeyParser.getDataType() + Constants.NULL + startKeyParser.getUid()
                        + Constants.NULL + stopKeyParser.getFieldValue() + Constants.MAX_UNICODE_STRING));
        scanRange = new Range(startKey, true, endKey, false);
        if (log.isTraceEnabled())
            log.trace("Adding scan range of " + scanRange);
        
        // Only when this source is running over an indexOnlyField
        // do we want to add it to the Document
        this.buildDocument = buildDocument;
        if (this.buildDocument) {
            if (typeMetadata == null) {
                typeMetadata = new TypeMetadata();
            }
            
            // Values coming from the field index are already normalized.
            // Create specialized attributes to encapsulate this and avoid
            // double normalization
            attributeFactory = new PreNormalizedAttributeFactory(typeMetadata);
        }
        
        this.aggregation = aggregator;
        
        this.timeFilter = timeFilter;
    }
    
    public void applyPattern(Pattern pattern) {
        this.pattern = pattern;
    }
    
    public void setNegated() {
        negated = true;
    }
    
    @Override
    public boolean hasTop() {
        return tk != null;
    }
    
    @Override
    public void next() throws IOException {
        // We need to null this every time even though our fieldname and fieldvalue won't
        // change, we have the potential for the column visibility to change
        document = new Document();
        
        tk = null;
        // reusable buffers
        Text row = new Text(), cf = new Text(), cq = new Text();
        
        if (log.isTraceEnabled()) {
            log.trace(source.hasTop() + " nexting on " + scanRange);
        }
        while (source.hasTop() && tk == null) {
            Key top = source.getTopKey();
            
            row = top.getRow(row);
            
            top.getColumnFamily(cf);
            top.getColumnQualifier(cq);
            
            if (!cq.toString().endsWith(field)) {
                if (log.isTraceEnabled()) {
                    log.trace(cq + " does not end with " + field);
                }
                source.next();
                continue;
            }
            
            DatawaveKey key = new DatawaveKey(top);
            
            for (int i = 0; i < 256 && source.hasTop() && key.getFieldName().compareTo(field) < 0; ++i) {
                source.next();
                key = new DatawaveKey(top);
                if (log.isTraceEnabled()) {
                    log.trace("Have key " + key + " < " + field);
                }
            }
            
            if (key.getFieldName().compareTo(field) < 0) {
                
                if (log.isTraceEnabled()) {
                    log.trace("Have key " + key + " is less than " + field);
                }
                
                StringBuilder builder = new StringBuilder(key.getDataType()).append(Constants.NULL).append(key.getUid()).append(Constants.NULL)
                                .append(key.getFieldValue()).append(field);
                Key nextKey = new Key(row, cf, new Text(builder.toString()));
                Range newRange = new Range(nextKey, true, scanRange.getEndKey(), scanRange.isEndKeyInclusive());
                source.seek(newRange, seekColumnFamilies, true);
                continue;
            }
            
            if (this.scanRange.isStartKeyInclusive()) {
                if (!this.scanRange.isInfiniteStartKey() && top.compareTo(this.scanRange.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL) < 0) {
                    if (log.isTraceEnabled()) {
                        log.trace("not inclusive " + top + " is before " + this.scanRange.getStartKey());
                    }
                    source.next();
                    continue;
                }
            } else {
                if (!this.scanRange.isInfiniteStartKey() && top.compareTo(this.scanRange.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL) <= 0) {
                    if (log.isTraceEnabled()) {
                        log.trace("inclusive " + top + " is before " + this.scanRange.getStartKey());
                    }
                    source.next();
                    continue;
                }
            }
            
            // Aggregate the document. NOTE: This will advance the source iterator
            tk = buildDocument ? aggregation.apply(source, document, attributeFactory) : aggregation.apply(source);
            if (log.isTraceEnabled()) {
                log.trace("Doc size: " + this.document.size());
                log.trace("Returning pointer " + tk);
            }
        }
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
        throw new UnsupportedOperationException("Cannot deep copy this iterator.");
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        throw new UnsupportedOperationException("This iterator cannot be init'd. Please use the constructor.");
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        
        if (log.isTraceEnabled()) {
            log.trace(this.toString() + " seek'ing to: " + this.scanRange + " from requested range " + range);
        }
        
        source.seek(this.scanRange, this.seekColumnFamilies, this.includeColumnFamilies);
        next();
    }
    
    @Override
    public void move(Key pointer) throws IOException {
        
        if (log.isTraceEnabled()) {
            log.trace("move pointer to " + pointer);
        }
        
        if (this.hasTop() && this.getTopKey().compareTo(pointer) >= 0) {
            throw new IllegalStateException("Tried to called move when we were already at or beyond where we were told to move to: topkey=" + this.getTopKey()
                            + ", movekey=" + pointer);
        }
        
        next();
    }
    
    protected void seek(SortedKeyValueIterator<Key,Value> source, Range r) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(this.toString() + " seek'ing to: " + r);
        }
        source.seek(r, this.seekColumnFamilies, true);
        next();
    }
    
    @Override
    public Document document() {
        return document;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexIterator: ");
        sb.append(this.columnFamily.toString().replace("\0", "\\x00"));
        sb.append(", ");
        sb.append(this.scanRange.toString().replace("\0", "\\x00"));
        
        return sb.toString();
    }
    
}
