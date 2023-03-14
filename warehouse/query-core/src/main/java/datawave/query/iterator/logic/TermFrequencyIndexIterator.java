package datawave.query.iterator.logic;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

import datawave.query.attributes.PreNormalizedAttributeFactory;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.data.parsers.TermFrequencyKey;
import datawave.query.iterator.DocumentIterator;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.jexl.functions.TermFrequencyAggregator;
import datawave.query.predicate.TimeFilter;
import datawave.query.attributes.Document;
import datawave.query.util.TypeMetadata;

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

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

import static datawave.query.Constants.NULL;
import static datawave.query.Constants.TERM_FREQUENCY_COLUMN_FAMILY;

/**
 * Scans a bounds within a column qualifier. This iterator needs to: - 1) Be given a global Range (ie, [-inf,+inf]) - 2) Select an arbitrary column family (ie,
 * "fi\u0000FIELD") - 3) Given a prefix, scan all keys that have a column qualifier that has that prefix that occur in the column family for all rows in a
 * tablet
 * 
 */
public class TermFrequencyIndexIterator implements SortedKeyValueIterator<Key,Value>, DocumentIterator {
    private static final Logger log = Logger.getLogger(TermFrequencyIndexIterator.class);
    
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
    protected boolean buildDocument;
    protected final FieldIndexAggregator aggregation;
    protected TimeFilter timeFilter;
    
    private final DatawaveKey startKeyParser;
    private final DatawaveKey stopKeyParser;
    
    // support for lazy building of scan range
    private final boolean startInclusive;
    private final boolean endInclusive;
    
    // reusable buffers
    private final Text row = new Text();
    private final Text cf = new Text();
    private final Text cq = new Text();
    
    private final TermFrequencyKey tfParser = new TermFrequencyKey();
    
    /**
     * A convenience constructor that allows all keys to pass through unmodified from the source.
     * 
     * @param source
     *            the source iterator
     * @param fiEndKey
     *            the index end key
     * @param fiStartKey
     *            the index start key
     * @param timeFilter
     *            a timefilter
     */
    public TermFrequencyIndexIterator(Key fiStartKey, Key fiEndKey, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter) {
        this(fiStartKey, fiEndKey, source, timeFilter, null, false, new TermFrequencyAggregator(null, null));
    }
    
    public TermFrequencyIndexIterator(Key fiStartKey, Key fiEndKey, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    boolean buildDocument, FieldIndexAggregator aggregator) {
        this(fiStartKey, true, fiEndKey, false, source, timeFilter, typeMetadata, buildDocument, aggregator);
    }
    
    public TermFrequencyIndexIterator(Range fiRange, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    boolean buildDocument, FieldIndexAggregator aggregator) {
        this(fiRange.getStartKey(), fiRange.isStartKeyInclusive(), fiRange.getEndKey(), fiRange.isEndKeyInclusive(), source, timeFilter, typeMetadata,
                        buildDocument, aggregator);
    }
    
    public TermFrequencyIndexIterator(Key fiStartKey, boolean startInclusive, Key fiEndKey, boolean endInclusive, SortedKeyValueIterator<Key,Value> source,
                    TimeFilter timeFilter, TypeMetadata typeMetadata, boolean buildDocument, FieldIndexAggregator aggregator) {
        
        this.source = source;
        this.timeFilter = timeFilter;
        
        // Build the cf: fi\x00FIELD_NAME
        this.columnFamily = TERM_FREQUENCY_COLUMN_FAMILY;
        
        this.seekColumnFamilies = Lists.newArrayList();
        this.seekColumnFamilies.add(new ArrayByteSequence(columnFamily.getBytes()));
        
        this.startKeyParser = new DatawaveKey(fiStartKey);
        this.stopKeyParser = new DatawaveKey(fiEndKey);
        
        this.includeColumnFamilies = true;
        
        this.field = startKeyParser.getFieldName().toUpperCase();
        
        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
        
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
    }
    
    public void buildScanRangeLazily() {
        Key startKey = new Key(startKeyParser.getRow(), columnFamily, new Text(startKeyParser.getDataType() + NULL + startKeyParser.getUid() + NULL
                        + startKeyParser.getFieldValue() + NULL + startKeyParser.getFieldName()));
        
        // must use the start key row so that the scan will be bounded by the specified cf/cq. All these scans are assumed to be across a single row
        Key endKey = new Key(stopKeyParser.getRow(), columnFamily, new Text(stopKeyParser.getDataType() + NULL + stopKeyParser.getUid() + NULL
                        + stopKeyParser.getFieldValue() + NULL + stopKeyParser.getFieldName()));
        
        this.scanRange = new Range(startKey, startInclusive, endKey, endInclusive);
        
        if (log.isTraceEnabled())
            log.trace("Adding scan range of " + scanRange);
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
        // We need to null this every time even though our fieldName and fieldValue won't
        // change, we have the potential for the column visibility to change
        document = new Document();
        
        tk = null;
        
        if (scanRange == null) {
            buildScanRangeLazily();
        }
        
        if (log.isTraceEnabled()) {
            log.trace(source.hasTop() + " nexting on " + scanRange);
        }
        
        Key top;
        while (source.hasTop() && tk == null) {
            top = source.getTopKey();
            
            // copy row, cf, cq into reusable buffers
            top.getRow(row);
            top.getColumnFamily(cf);
            top.getColumnQualifier(cq);
            
            tfParser.parse(top);
            
            if (!cq.toString().endsWith(field)) {
                if (log.isTraceEnabled()) {
                    log.trace(cq + " does not end with " + field);
                }
                source.next();
                continue;
            }
            
            if (tfParser.getField().compareTo(field) < 0) {
                
                if (log.isTraceEnabled()) {
                    log.trace("Have key " + top.toStringNoTime() + " is less than " + field);
                }
                
                StringBuilder builder = new StringBuilder(tfParser.getDatatype()).append(NULL).append(tfParser.getUid()).append(NULL)
                                .append(tfParser.getValue()).append(NULL).append(field);
                Key nextKey = new Key(row, cf, new Text(builder.toString()));
                Range newRange = new Range(nextKey, true, scanRange.getEndKey(), scanRange.isEndKeyInclusive());
                source.seek(newRange, seekColumnFamilies, true);
                continue;
            }
            
            // only inspect the values specified in the range since a broad row or uid range will potentially go cross document
            if (scanRange.isStartKeyInclusive() && tfParser.getValue().compareTo(startKeyParser.getFieldValue()) < 0) {
                source.next();
                continue;
            } else if (!scanRange.isStartKeyInclusive() && tfParser.getValue().compareTo(startKeyParser.getFieldValue()) <= 0) {
                source.next();
                continue;
            } else if (scanRange.isEndKeyInclusive() && tfParser.getValue().compareTo(stopKeyParser.getFieldValue()) > 0) {
                source.next();
                continue;
            } else if (!scanRange.isEndKeyInclusive() && tfParser.getValue().compareTo(stopKeyParser.getFieldValue()) >= 0) {
                source.next();
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
            tk = buildDocument ? aggregation.apply(source, document, attributeFactory) : aggregation.apply(source, scanRange, seekColumnFamilies,
                            includeColumnFamilies);
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
        
        if (scanRange == null) {
            buildScanRangeLazily();
        }
        
        if (log.isTraceEnabled()) {
            log.trace(this + " seek'ing to: " + this.scanRange + " from requested range " + range);
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
            throw new IllegalStateException("Tried to called move when we were already at or beyond where we were told to move to: topKey=" + this.getTopKey()
                            + ", moveKey=" + pointer);
        }
        
        next();
    }
    
    protected void seek(SortedKeyValueIterator<Key,Value> source, Range r) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(this + " seek'ing to: " + r);
        }
        source.seek(r, this.seekColumnFamilies, true);
        next();
    }
    
    @Override
    public Document document() {
        if (document == null) {
            // in case this method is called prior to a seek/next
            document = new Document();
        }
        return document;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexIterator: ");
        sb.append(this.columnFamily.toString().replace("\0", "\\x00"));
        // scanRange is now lazily set
        if (scanRange != null) {
            sb.append(", ");
            sb.append(this.scanRange.toString().replace("\0", "\\x00"));
        }
        
        return sb.toString();
    }
    
}
