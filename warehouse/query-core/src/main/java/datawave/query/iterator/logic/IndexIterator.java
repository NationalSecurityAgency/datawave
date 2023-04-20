package datawave.query.iterator.logic;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import datawave.query.Constants;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttributeFactory;
import datawave.query.iterator.DocumentIterator;
import datawave.query.iterator.LimitedSortedKeyValueIterator;
import datawave.query.iterator.Util;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.jexl.functions.IdentityAggregator;
import datawave.query.predicate.SeekingFilter;
import datawave.query.predicate.TimeFilter;
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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Scans a bounds within a column qualifier. This iterator needs to: - 1) Be given a global Range (ie, [-inf,+inf]) - 2) Select an arbitrary column family (ie,
 * "fi\u0000FIELD") - 3) Given a prefix, scan all keys that have a column qualifer that has that prefix that occur in the column family for all rows in a tablet
 * 
 */
public class IndexIterator implements SortedKeyValueIterator<Key,Value>, DocumentIterator {
    private static final Logger log = Logger.getLogger(IndexIterator.class);
    
    public static class Builder<B extends Builder<B>> {
        protected Text field;
        protected Text value;
        protected SortedKeyValueIterator<Key,Value> source;
        protected TimeFilter timeFilter = TimeFilter.alwaysTrue();
        protected boolean buildDocument = false;
        protected TypeMetadata typeMetadata;
        protected Predicate<Key> datatypeFilter = Predicates.alwaysTrue();
        protected FieldIndexAggregator aggregation = new IdentityAggregator(null, null);
        
        protected Builder(Text field, Text value, SortedKeyValueIterator<Key,Value> source) {
            this.field = field;
            this.value = value;
            this.source = source;
        }
        
        @SuppressWarnings("unchecked")
        protected B self() {
            return (B) this;
        }
        
        public B withTimeFilter(TimeFilter timeFilter) {
            this.timeFilter = timeFilter;
            return self();
        }
        
        public B shouldBuildDocument(boolean buildDocument) {
            this.buildDocument = buildDocument;
            return self();
        }
        
        public B withTypeMetadata(TypeMetadata typeMetadata) {
            this.typeMetadata = typeMetadata;
            return self();
        }
        
        public B withDatatypeFilter(Predicate<Key> datatypeFilter) {
            this.datatypeFilter = datatypeFilter;
            return self();
        }
        
        public B withAggregation(FieldIndexAggregator aggregation) {
            this.aggregation = aggregation;
            return self();
        }
        
        public IndexIterator build() {
            return new IndexIterator(this);
        }
        
    }
    
    public static Builder<?> builder(Text field, Text value, SortedKeyValueIterator<Key,Value> source) {
        return new Builder(field, value, source);
    }
    
    public static final String INDEX_FILTERING_CLASSES = "indexfiltering.classes";
    
    protected final SortedKeyValueIterator<Key,Value> source;
    protected final LimitedSortedKeyValueIterator limitedSource;
    protected final Text valueMinPrefix;
    protected final Text columnFamily;
    protected final Collection<ByteSequence> seekColumnFamilies;
    
    // used for managing parent calls to seek
    protected Range scanRange;
    
    protected Key tk;
    protected Value tv;
    
    protected final String field;
    protected final String value;
    
    protected Key limitKey;
    
    protected PreNormalizedAttributeFactory attributeFactory;
    protected Document document;
    protected boolean buildDocument = false;
    protected Predicate<Key> datatypeFilter;
    protected SeekingFilter dataTypeSeekingFilter;
    protected final FieldIndexAggregator aggregation;
    protected TimeFilter timeFilter;
    protected SeekingFilter timeSeekingFilter;
    
    protected IndexIterator(Builder builder) {
        this(builder.field, builder.value, builder.source, builder.timeFilter, builder.typeMetadata, builder.buildDocument, builder.datatypeFilter,
                        builder.aggregation);
    }
    
    private IndexIterator(Text field, Text value, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    boolean buildDocument, Predicate<Key> datatypeFilter, FieldIndexAggregator aggregator) {
        
        this.valueMinPrefix = Util.minPrefix(value);
        
        this.datatypeFilter = datatypeFilter;
        if (datatypeFilter instanceof SeekingFilter) {
            dataTypeSeekingFilter = (SeekingFilter) datatypeFilter;
        }
        
        this.source = source;
        // wrap the source with a limit
        this.limitedSource = new LimitedSortedKeyValueIterator(source);
        if (timeFilter instanceof SeekingFilter) {
            timeSeekingFilter = (SeekingFilter) timeFilter;
        }
        
        // Build the cf: fi\x00FIELD_NAME
        this.columnFamily = new Text(Constants.FI_PREFIX);
        this.columnFamily.append(Constants.TEXT_NULL.getBytes(), 0, Constants.TEXT_NULL.getLength());
        this.columnFamily.append(field.getBytes(), 0, field.getLength());
        
        // Copy the byte[] by hand because ArrayByteSequence doesn't
        // The underlying bytes could be modified even though columnFamily is final
        byte[] columnFamilyBytes = new byte[this.columnFamily.getLength()];
        System.arraycopy(this.columnFamily.getBytes(), 0, columnFamilyBytes, 0, this.columnFamily.getLength());
        
        // Make sure we properly set the ColumnFamilies when calling seek() to avoid
        // opening readers to locality groups we don't care about
        this.seekColumnFamilies = Collections.singleton(new ArrayByteSequence(columnFamilyBytes));
        
        this.field = field.toString();
        this.value = value.toString();
        
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
        Text row = new Text();
        Text cf = new Text();
        Text cq = new Text();
        
        while (source.hasTop() && tk == null) {
            Key top = source.getTopKey();
            
            row = top.getRow(row);
            
            // Compare the current topKey's columnFamily against what we expect to receive
            cf = top.getColumnFamily(cf);
            int cfDiff = columnFamily.compareTo(cf);
            
            // check value, type, uid (bar\x00type\x00uid)
            cq = top.getColumnQualifier(cq);
            int cqDiff = Util.prefixDiff(valueMinPrefix, cq);
            
            if (cfDiff > 0) {
                // need try and find our columnFamily
                Key newStart = new Key(row, columnFamily, valueMinPrefix);
                Range newRange = new Range(newStart, false, scanRange.getEndKey(), scanRange.isEndKeyInclusive());
                
                if (log.isTraceEnabled()) {
                    log.trace("topkey: " + top);
                    log.trace("cfDiff > 0, seeking to range: " + newRange);
                }
                
                source.seek(newRange, seekColumnFamilies, true);
                continue;
            } else if (cfDiff < 0) {
                // need to move to the next row and try again
                // this op is destructive on row, but iz ok 'cause the continue will reset it
                //
                // We can provide the columnFamily to avoid an additional iteration that would just call seek
                // with the given columnFamily for this Iterator
                Key newStart = new Key(top.followingKey(PartialKey.ROW).getRow(row), this.columnFamily);
                
                // If we try to seek to a Key that it outside of our Range, we're done
                if (scanRange.afterEndKey(newStart)) {
                    return;
                }
                
                Range newRange = new Range(newStart, false, scanRange.getEndKey(), scanRange.isEndKeyInclusive());
                
                if (log.isTraceEnabled()) {
                    log.trace("topkey: " + top);
                    log.trace("cfDiff < 0, seeking to range: " + newRange);
                }
                
                source.seek(newRange, seekColumnFamilies, true);
                continue;
            }
            
            if (cqDiff > 0) {
                // need try and find our columnFamily
                Key newStart = new Key(row, columnFamily, valueMinPrefix);
                Range newRange = new Range(newStart, false, scanRange.getEndKey(), scanRange.isEndKeyInclusive());
                
                if (log.isTraceEnabled()) {
                    log.trace("topkey: " + top);
                    log.trace("cqDiff > 0, seeking to range: " + newRange);
                }
                
                source.seek(newRange, seekColumnFamilies, true);
                continue;
            } else if (cqDiff < 0) {
                // need to move to the next row and try again
                // this op is destructive on row, but iz ok 'cause the continue will reset it
                //
                // We can provide the columnFamily to avoid an additional iteration that would just call seek
                // with the given columnFamily for this Iterator
                Key newStart = new Key(top.followingKey(PartialKey.ROW).getRow(row), this.columnFamily);
                
                // If we try to seek to a Key that it outside of our Range, we're done
                if (scanRange.afterEndKey(newStart)) {
                    return;
                }
                
                Range newRange = new Range(newStart, false, scanRange.getEndKey(), scanRange.isEndKeyInclusive());
                
                if (log.isTraceEnabled()) {
                    log.trace("topkey: " + top);
                    log.trace("cqDiff < 0, seeking to range: " + newRange);
                }
                
                source.seek(newRange, seekColumnFamilies, true);
                continue;
            }
            
            if (this.scanRange.isStartKeyInclusive()) {
                if (!this.scanRange.isInfiniteStartKey() && top.compareTo(this.scanRange.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL) < 0) {
                    source.next();
                    continue;
                }
            } else {
                if (!this.scanRange.isInfiniteStartKey() && top.compareTo(this.scanRange.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL) <= 0) {
                    source.next();
                    continue;
                }
            }
            
            // A field index key's timestamp is accurate to the millisecond, so we can observe this
            // and remove all keys which don't satisfy the intra-day time range.
            if (!this.timeFilter.apply(top)) {
                if (log.isTraceEnabled()) {
                    log.trace("Ignoring key due to not occuring within time filter: " + top);
                }
                Range newRange;
                if (timeSeekingFilter != null
                                && (newRange = timeSeekingFilter.getSeekRange(top, this.scanRange.getEndKey(), this.scanRange.isEndKeyInclusive())) != null) {
                    source.seek(newRange, seekColumnFamilies, true);
                } else {
                    source.next();
                }
                continue;
            }
            
            if (!this.datatypeFilter.apply(top)) {
                if (log.isTraceEnabled()) {
                    log.trace("Ignoring key due to not occuring within datatype filter: " + top);
                }
                Range newRange;
                if (dataTypeSeekingFilter != null
                                && (newRange = dataTypeSeekingFilter.getSeekRange(top, this.scanRange.getEndKey(), this.scanRange.isEndKeyInclusive())) != null) {
                    source.seek(newRange, seekColumnFamilies, true);
                } else {
                    source.next();
                }
                continue;
            }
            
            // restrict the aggregation to the current target value within the document
            limitedSource.setLimit(getLimitKey(top.getRow()));
            
            // Aggregate the document. NOTE: This will advance the source iterator
            if (buildDocument) {
                tk = aggregation.apply(limitedSource, document, attributeFactory);
            } else {
                tk = aggregation.apply(limitedSource, scanRange, seekColumnFamilies, true);
            }
            
            if (log.isTraceEnabled()) {
                log.trace("Doc size: " + this.document.size());
                log.trace("Returning pointer " + tk.toStringNoTime());
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
        this.scanRange = buildIndexRange(range);
        
        if (log.isTraceEnabled()) {
            log.trace(this + " seek'ing to: " + this.scanRange + " from " + range);
        }
        
        source.seek(this.scanRange, this.seekColumnFamilies, true);
        next();
    }
    
    private final Text newColumnQualifier = new Text(new byte[128]);
    
    /**
     * Advance the source to the Key specified by pointer then fetch the next tk/tv from that point
     * 
     * @param pointer
     *            the minimum point to advance source to
     * @throws IOException
     *             if something goes wrong accessing accumulo
     * @throws IllegalStateException
     *             if getTopKey() is greater than or equal to pointer
     */
    @Override
    public void move(Key pointer) throws IOException {
        if (this.hasTop() && this.getTopKey().compareTo(pointer) >= 0) {
            throw new IllegalStateException("Tried to called move when we were already at or beyond where we were told to move to: topkey=" + this.getTopKey()
                            + ", movekey=" + pointer);
        }
        
        newColumnQualifier.set(valueMinPrefix);
        Text id = pointer.getColumnFamily();
        newColumnQualifier.append(id.getBytes(), 0, id.getLength());
        
        Key nextKey = new Key(pointer.getRow(), columnFamily, newColumnQualifier);
        Range r = new Range(nextKey, true, scanRange.getEndKey(), scanRange.isEndKeyInclusive());
        
        if (log.isTraceEnabled()) {
            log.trace(this + " moving to: " + r);
        }
        
        source.seek(r, seekColumnFamilies, true);
        
        if (log.isTraceEnabled()) {
            log.trace(this + " finished move. Now at " + (source.hasTop() ? source.getTopKey() : "null") + ", calling next()");
        }
        
        next();
    }
    
    protected void seek(SortedKeyValueIterator<Key,Value> source, Range r) throws IOException {
        source.seek(r, this.seekColumnFamilies, true);
    }
    
    /**
     * Permute a "Document" Range to the equivalent "Field Index" Range for a Field:Term
     * 
     * @param r
     *            a range formatted like a document key
     * @return the field index range
     */
    protected Range buildIndexRange(Range r) {
        Key startKey = permuteRangeKey(r.getStartKey(), r.isStartKeyInclusive());
        Key endKey = permuteRangeKey(r.getEndKey(), r.isEndKeyInclusive());
        
        return new Range(startKey, r.isStartKeyInclusive(), endKey, r.isEndKeyInclusive());
    }
    
    /**
     * Permute a "Document" Key to an equivalent "Field Index" key for a Field:Term
     * 
     * @param rangeKey
     *            a key formatted like a document key
     * @param inclusive
     *            flag to determine to add a null bye
     * @return a key formatted for a field index range
     */
    protected Key permuteRangeKey(Key rangeKey, boolean inclusive) {
        Key key = null;
        
        if (null != rangeKey) {
            // The term for this index iterator
            Text term = new Text(valueMinPrefix);
            
            // Build up term\x00type\x00uid for the new columnqualifier
            term = Util.appendText(term, rangeKey.getColumnFamily());
            
            // if not inclusive, then add a null byte to the end of the UID to ensure we go to the next one
            if (!inclusive) {
                term = Util.appendSuffix(term, (byte) 0);
            }
            
            key = new Key(rangeKey.getRow(), this.columnFamily, term);
        }
        
        return key;
    }
    
    /**
     * Get a key to limit the scan range of this iterator.
     * <p>
     * Practically the row will not change, so this 'get or build' pattern is safe.
     *
     * @param row
     *            the row a shard index key (the partition in YYYYmmdd_n format)
     * @return the limit key
     */
    public Key getLimitKey(Text row) {
        if (limitKey == null) {
            limitKey = new Key(row, columnFamily, new Text(valueMinPrefix + Constants.MAX_UNICODE_STRING));
        }
        return limitKey;
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
        sb.append(this.valueMinPrefix.toString().replace("\0", "\\x00"));
        return sb.toString();
    }
}
