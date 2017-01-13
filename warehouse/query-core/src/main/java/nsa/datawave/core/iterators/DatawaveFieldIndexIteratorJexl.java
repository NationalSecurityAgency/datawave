package nsa.datawave.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import nsa.datawave.query.iterators.JumpingIterator;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.predicate.TimeFilter;
import nsa.datawave.util.StringUtils;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;

/**
 * 
 * An iterator for the Datawave shard table, it searches FieldIndex keys and returns Event keys (its topKey must be an Event key).
 * 
 * FieldIndex keys: {shardId}:fi\0{fieldName}:{fieldValue}\0datatype\0uid
 * 
 * Event key: {shardId}:{datatype}\0{UID}
 * 
 * Note that if the range is broader that for a specific fieldname and fieldvalue, then the keys would be unsorted. Use the extended
 * DatawaveFieldIndexCachingIteratorJexl for that case.
 * 
 */
@SuppressWarnings("rawtypes")
public class DatawaveFieldIndexIteratorJexl extends WrappingIterator implements JumpingIterator<Key,Value> {
    
    protected Key topKey = null;
    protected Value topValue = null;
    protected final List<Range> boundingRanges = new ArrayList<Range>();
    protected Text fieldName = null;
    protected Text fiName = null; // part of the datawave shard structure: fi\0fieldname
    protected Text fieldValue = null; // part of the datawave shard structure
    protected Predicate<Key> datatypeFilter;
    protected TimeFilter timeFilter;
    
    public static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
    public static final Logger log = Logger.getLogger(DatawaveFieldIndexIteratorJexl.class);
    public static final String NULL_BYTE = Constants.NULL_BYTE_STRING;
    public static final String ONE_BYTE = "\u0001";
    
    private volatile boolean negated = false;
    protected Range initialSeekRange;
    protected Range parentRange;
    
    // This iterator should have no seek column families. This is because all filtering is done by the boundingbox range
    // and we do not want the underlying iterators to filter keys so that we can check the bounds in this iterator as quickly
    // as possible.
    protected Collection<ByteSequence> seekColumnFamilies = Collections.EMPTY_LIST;
    protected boolean seekColumnFamiliesInclusive = false;
    
    protected Text dataType = null; // Important safety tip: This is NOT a datatype filter. This is used to determine where
                                    // to start the scan within EACH shard that is part of the overall range as an optimization.
    protected Text myColFam = null;
    protected StringBuilder jumpKeyStringBuilder; // heavy optimization, for use by jump method only!
    protected StringBuilder boundingRangeStringBuilder; // heavy optimization, for use by buildBoundingRange method only!
    
    // the number of underlying keys scanned (used by ivarators for example to determine when we should force persistence of the results)
    protected AtomicLong scannedKeys = new AtomicLong(0);
    
    public static final PartialKey DEFAULT_RETURN_KEY_TYPE = PartialKey.ROW_COLFAM;
    protected PartialKey returnKeyType = DEFAULT_RETURN_KEY_TYPE;
    
    // Wrapping iterator only accesses its private source in setSource and getSource
    // Since this class overrides these methods, it's safest to keep the source declaration here
    protected SortedKeyValueIterator<Key,Value> source;
    
    // -------------------------------------------------------------------------
    // ------------- Constructors
    public DatawaveFieldIndexIteratorJexl() {}
    
    public DatawaveFieldIndexIteratorJexl(Text fieldName, Text fieldValue, TimeFilter timeFilter, Predicate<Key> datatypeFilter) {
        this(fieldName, fieldValue, timeFilter, datatypeFilter, false);
    }
    
    public DatawaveFieldIndexIteratorJexl(Text fieldName, Text fieldValue, TimeFilter timeFilter, Predicate<Key> datatypeFilter, boolean neg) {
        this(fieldName, fieldValue, timeFilter, datatypeFilter, neg, DEFAULT_RETURN_KEY_TYPE);
    }
    
    protected DatawaveFieldIndexIteratorJexl(Text fieldName, Text fieldValue, TimeFilter timeFilter, Predicate<Key> datatypeFilter, boolean neg,
                    PartialKey returnKeyType) {
        if (fieldName.toString().startsWith("fi" + NULL_BYTE)) {
            this.fieldName = new Text(fieldName.toString().substring(3));
            this.fiName = fieldName;
        } else {
            this.fieldName = fieldName;
            this.fiName = new Text("fi" + NULL_BYTE + fieldName.toString());
        }
        log.trace("fName : " + fiName.toString().replaceAll(NULL_BYTE, "%00"));
        this.fieldValue = fieldValue;
        this.negated = neg;
        this.myColFam = new Text(this.fiName);
        this.jumpKeyStringBuilder = new StringBuilder();
        this.boundingRangeStringBuilder = new StringBuilder();
        this.returnKeyType = returnKeyType;
        this.timeFilter = timeFilter;
        this.datatypeFilter = datatypeFilter;
    }
    
    public DatawaveFieldIndexIteratorJexl(DatawaveFieldIndexIteratorJexl other, IteratorEnvironment env) {
        this.source = other.getSource().deepCopy(env);
        this.fieldName = other.fieldName;
        this.fiName = other.fiName;
        this.returnKeyType = other.returnKeyType;
        this.timeFilter = other.timeFilter;
        this.datatypeFilter = other.datatypeFilter;
        this.fieldValue = other.fieldValue;
        this.boundingRanges.addAll(other.boundingRanges);
        this.negated = other.negated;
        this.myColFam = other.myColFam;
        this.jumpKeyStringBuilder = new StringBuilder();
        this.boundingRangeStringBuilder = new StringBuilder();
        this.seekColumnFamilies = other.seekColumnFamilies;
        this.seekColumnFamiliesInclusive = other.seekColumnFamiliesInclusive;
        if (log.isTraceEnabled()) {
            for (ByteSequence bs : this.seekColumnFamilies) {
                log.trace("seekColumnFamilies for this iterator: " + (new String(bs.toArray())).replaceAll(NULL_BYTE, "%00"));
            }
        }
    }
    
    // -------------------------------------------------------------------------
    // ------------- Overrides
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
    }
    
    @Override
    protected void setSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }
    
    @Override
    protected SortedKeyValueIterator<Key,Value> getSource() {
        return source;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new DatawaveFieldIndexIteratorJexl(this, env);
    }
    
    @Override
    public Key getTopKey() {
        return topKey;
    }
    
    @Override
    public Value getTopValue() {
        return topValue;
    }
    
    @Override
    public boolean hasTop() {
        return (topKey != null);
    }
    
    @Override
    public void next() throws IOException {
        log.trace("next() called");
        if (!source.hasTop()) {
            this.topKey = null;
            return;
        }
        source.next();
        scannedKeys.incrementAndGet();
        findTop();
        
    }
    
    /*
     * Seek: conforms to how the BooleanLogicIterator calls it
     * 
     * POSITIVE We are usually given a row, or row:datatype as the starting key of the seek range for positive nodes. We are responsible for constructing the
     * proper FIELDNAME -> FIELDVALUE range.
     * 
     * NEGATED If we are negated, we will receive a properly formated range so we don't need to mess with it.
     * 
     * NOTE: boundingRanges - Since we know the beginning and ending of our FieldName->FieldValue pair we can construct a range and use it for testing i.e.
     * boundingRange.contains(key) versus parsing each key for its FN,FV I'm not sure which is more efficient when you have a lot of matches.
     */
    @Override
    public void seek(Range r, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.parentRange = this.initialSeekRange = new Range(r);
        
        this.topKey = null;
        this.topValue = null;
        this.boundingRanges.clear();
        
        if (log.isTraceEnabled()) {
            log.trace("begin seek, range: " + parentRange);
            for (ByteSequence bs : this.seekColumnFamilies) {
                log.trace("seekColumnFamilies for this iterator: " + (new String(bs.toArray())).replaceAll(NULL_BYTE, "%00"));
            }
        }
        
        // ---------------------------------------------------------------------
        // if no bounding ranges are required, then assume we have seeked to a matching key
        if (!requiresBoundingRange()) {
            if (log.isTraceEnabled()) {
                log.trace("seek, but I'm negated, go directly to key:range: " + parentRange);
            }
            boundingRanges.add(parentRange);
            source.seek(parentRange, this.seekColumnFamilies, seekColumnFamiliesInclusive);
            scannedKeys.incrementAndGet();
            if (source.hasTop() && parentRange.contains(source.getTopKey())) {
                this.topKey = buildEventKey(source.getTopKey(), returnKeyType);
                this.topValue = source.getTopValue();
            }
            if (log.isTraceEnabled()) {
                log.trace("Negated top key: " + (this.topKey != null ? topKey : "null"));
            }
            return;
        }
        
        // ---------------------------------------------------------------------
        // Positive node -build our internal bounding range based on the incoming seek range.
        
        // If we have a null start key or empty starting row, lets seek our underlying source
        // to the start of the incoming range
        if (null == parentRange.getStartKey() || null == parentRange.getStartKey().getRow() || parentRange.getStartKey().getRow().getLength() <= 0) {
            
            source.seek(parentRange, this.seekColumnFamilies, seekColumnFamiliesInclusive);
            scannedKeys.incrementAndGet();
            
            if (!source.hasTop()) {
                return;
            }
            
            // Assume the real row does not need to be shardified
            this.boundingRanges.addAll(buildBoundingRanges(source.getTopKey().getRow(), fiName, fieldValue));
            if (log.isTraceEnabled()) {
                log.trace("startKey null or row was empty, parentRange: " + parentRange + "  source.getTopKey(): " + source.getTopKey());
            }
            
        } else {
            
            // Check if a datatype was passed to us in
            // the column family of the range's starting key.
            // We do NOT want to remember it if it contains a uid because when we change rows we will presumably miss keys.
            Key pStartKey = parentRange.getStartKey();
            String cFam = null;
            if (null != pStartKey && null != pStartKey.getRow() && null != pStartKey.getColumnFamily() && !pStartKey.getColumnFamily().toString().isEmpty()) {
                cFam = pStartKey.getColumnFamily().toString();
                String[] cFamParts = StringUtils.split(cFam, NULL_BYTE);
                boolean hasUid = false;
                if (cFamParts.length > 1) {
                    hasUid = !cFamParts[1].trim().isEmpty(); // if it is empty, we do NOT have a uid.
                }
                
                // if we were passed a data type as an optimization, lets save it for later use
                // but only if it's not a datatype\x00uid
                if (!hasUid) {
                    this.dataType = pStartKey.getColumnFamily();
                } else {
                    // if we have a uid and it's non-inclusive add an extra null byte to the datatype\x00uid
                    if (!parentRange.isStartKeyInclusive()) {
                        cFam += NULL_BYTE;
                    }
                    // we want to pass this to the first buildBoundingRange, but not preserve it for when we skip rows.
                }
            }
            
            this.boundingRanges.addAll(buildBoundingRanges((pStartKey != null ? pStartKey.getRow() : null), fiName, fieldValue, ((null == cFam) ? null
                            : new Text(cFam))));
        }
        
        // If a datatype was passed in, and it is also in the end key, the range
        // may not encompass the field index keys, we need to make sure it does.
        // TODO: need to confirm the format of ranges that are passed in,
        // i.e. [Key(ROW,datatype) - Key(EndROW, datatype) ) or [Key(ROW,datatype) - Key(EndROW) )
        // if it is the latter then we can get rid of this block
        if (null != parentRange.getEndKey() && null != parentRange.getEndKey().getColumnFamily()
                        && !parentRange.getEndKey().getColumnFamily().toString().isEmpty()) {
            
            Key endKey = new Key(parentRange.getEndKey().followingKey(PartialKey.ROW));
            parentRange = new Range(parentRange.getStartKey(), parentRange.isStartKeyInclusive(), endKey, parentRange.isEndKeyInclusive());
        }
        
        // if the start key of our bounding range > parentKey.endKey we can stop
        // This can happen in cases of an -infinite start key and a declared end key
        if (areBoundingRangesPastParentRange()) {
            return;
        }
        
        // When we seek the source, we want to update the range's starting key,
        // and maintain its ending key.
        parentRange = new Range(boundingRanges.get(0).getStartKey(), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
        if (log.isTraceEnabled()) {
            log.trace("seek, boundingRange: " + boundingRanges);
            log.trace("seek, seek Range: " + parentRange);
        }
        source.seek(parentRange, this.seekColumnFamilies, seekColumnFamiliesInclusive);
        scannedKeys.incrementAndGet();
        if (log.isTraceEnabled()) {
            log.trace(source.hasTop() ? "source top key: " + source.getTopKey() : "source has no top.");
        }
        findTop();
        
        if (log.isTraceEnabled()) {
            log.trace("seek, topKey : " + ((null == topKey) ? "null" : topKey));
        }
        
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DatawaveFieldIndexIteratorJexl{fName=").append(fiName).append(", fValue=").append(fieldValue).append(", negated=").append(negated)
                        .append("}");
        return builder.toString();
    }
    
    // -------------------------------------------------------------------------
    // ------------- Public stuff
    public boolean isNegated() {
        return negated;
    }
    
    public Text getFieldName() {
        return fieldName;
    }
    
    public Text getFieldValue() {
        return fieldValue;
    }
    
    /**
     * @return the field index column family (fi\0fieldname)
     */
    public Text getFiName() {
        return fiName;
    }
    
    // backward compatibility
    public Text getfName() {
        return fiName;
    }
    
    // backward compatibility
    public Text getfValue() {
        return fieldValue;
    }
    
    public PartialKey getReturnKeyType() {
        return returnKeyType;
    }
    
    public void setReturnKeyType(PartialKey returnKeyType) {
        this.returnKeyType = returnKeyType;
    }
    
    /**
     * jumpKey is an Event Key and our topKey is also an event key. This is basically a seek except we don't do it if we are already at/past the jumpKey (note
     * you need to convert the jump key to an index key for proper comparison). This avoids rewinding an iterator
     * 
     * @param jumpKey
     * @return
     * @throws IOException
     */
    @Override
    public boolean jump(Key jumpKey) throws IOException {
        jumpKeyStringBuilder.delete(0, jumpKeyStringBuilder.length());
        if (log.isTraceEnabled()) {
            log.trace("JUMP, jumpKey: " + jumpKey + "  topKey: " + topKey);
        }
        
        // If I have top and it's less then the jumpKey, we need to move, otherwise stay where we are.
        if (this.topKey != null && (jumpKey.compareTo(topKey) > 0)) {
            
            // turn the jumpKey into a local key
            jumpKeyStringBuilder.append(this.fieldValue).append(NULL_BYTE).append(jumpKey.getColumnFamily());
            Key localJumpKey = new Key(jumpKey.getRow(), this.myColFam, new Text(jumpKeyStringBuilder.toString()));
            if (log.isTraceEnabled()) {
                log.trace("jumpKey as local Key: " + localJumpKey);
            }
            
            // make sure our parentRange contains it
            if (null != parentRange && parentRange.contains(localJumpKey)) {
                
                // generate a new range which we will seek to.
                Range jumpRange = new Range(localJumpKey, true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
                if (log.isTraceEnabled()) {
                    log.trace("jumpRange; " + jumpRange);
                }
                
                parentRange = jumpRange; // update our parent/overall range
                
                // construct our boundingRanges used to test the top key
                boundingRanges.clear();
                boundingRanges.addAll(this.buildBoundingRanges(jumpRange.getStartKey().getRow(), fiName, fieldValue, this.dataType));
                
                // seek the source
                source.seek(jumpRange, this.seekColumnFamilies, seekColumnFamiliesInclusive);
                scannedKeys.incrementAndGet();
                
                // find the topKey
                findTop();
                
            } else {
                topKey = null;
                topValue = Constants.NULL_VALUE;
            }
        }
        return this.topKey != null;
    }
    
    /**
     * From a field index key, this builds row=shardId, cf=datatype\0UID, cq=fieldname\0fieldvalue
     * 
     * @param key
     * @return Key(shardId, datatype\0UID)
     */
    public static Key buildEventKey(Key key, PartialKey keyType) {
        // field index key is shardId : fi\0fieldName : fieldValue\0datatype\0uid
        // event key is shardId : dataType\0uid : fieldName\0fieldValue
        String cf = key.getColumnFamily().toString();
        String cq = key.getColumnQualifier().toString();
        int cqNullIndex = cq.indexOf('\0');
        switch (keyType) {
            case ROW:
                return new Key(key.getRow());
            case ROW_COLFAM:
                return new Key(key.getRow(), new Text(cq.substring(cqNullIndex + 1)));
            case ROW_COLFAM_COLQUAL:
                return new Key(key.getRow(), new Text(cq.substring(cqNullIndex + 1)), new Text(cf.substring(3) + '\0' + cq.substring(0, cqNullIndex)));
            case ROW_COLFAM_COLQUAL_COLVIS:
                return new Key(key.getRow(), new Text(cq.substring(cqNullIndex + 1)), new Text(cf.substring(3) + '\0' + cq.substring(0, cqNullIndex)),
                                key.getColumnVisibility());
            default:
                return new Key(key.getRow(), new Text(cq.substring(cqNullIndex + 1)), new Text(cf.substring(3) + '\0' + cq.substring(0, cqNullIndex)),
                                key.getColumnVisibility(), key.getTimestamp());
        }
    }
    
    // -------------------------------------------------------------------------
    // ------------- other stuff
    
    /**
     * Do we require computing bounding ranges? By default we do, provided we are not negated. Hence negated nodes should only ever be seeked to a specific
     * field index key.
     * 
     * @return true is bounding ranges are required to be computed.
     */
    protected boolean requiresBoundingRange() {
        return !isNegated();
    }
    
    /**
     * Build the bounding ranges. Normally this returns only one range, but it could return multiple (@see DatawaveFieldIndexRegex/Range/ListIteratorJexl
     * superclasses). If multiple are returned, then they must be sorted
     * 
     * @param rowId
     * @return
     */
    protected List<Range> buildBoundingRanges(Text rowId, Text fiName, Text fieldValue) {
        return buildBoundingRanges(rowId, fiName, fieldValue, null);
    }
    
    /**
     * Build the bounding ranges. Normally this returns only one range, but it could return multiple (@see DatawaveFieldIndexRegex/Range/ListIteratorJexl
     * superclasses). If multiple are returned, then they must be sorted
     * 
     * @param rowId
     * @return
     */
    protected List<Range> buildBoundingRanges(Text rowId, Text fiName, Text fieldValue, Text startingColumnFamily) {
        // construct new range
        this.boundingRangeStringBuilder.delete(0, this.boundingRangeStringBuilder.length());
        this.boundingRangeStringBuilder.append(fieldValue).append(NULL_BYTE);
        if (null != startingColumnFamily) {
            this.boundingRangeStringBuilder.append(startingColumnFamily);
            // The incoming range may have contained just a datatype or a datatype\x00uid.
            // For cases where it is only datatype, we want to append a null byte for our bounding range, but leave it
            // alone if it contains a uid.
            if (startingColumnFamily.toString().indexOf('\0') < 0) {
                this.boundingRangeStringBuilder.append(NULL_BYTE);
            }
        }
        Key startKey = new Key(rowId, fiName, new Text(this.boundingRangeStringBuilder.toString()));
        
        this.boundingRangeStringBuilder.delete(0, this.boundingRangeStringBuilder.length());
        this.boundingRangeStringBuilder.append(fieldValue);
        this.boundingRangeStringBuilder.append(ONE_BYTE);
        Key endKey = new Key(rowId, fiName, new Text(this.boundingRangeStringBuilder.toString()));
        return Collections.singletonList(new Range(startKey, true, endKey, false));
    }
    
    // need to build a range starting at the end of current row and seek the
    // source to it. If we get an IOException, that means we hit the end of the tablet.
    protected Text moveToNextRow(Key k) throws IOException {
        log.trace("moveToNextRow()");
        
        // Make sure the source iterator's key didn't seek past the end
        // of our starting row and get into the next row. It can happen if your
        // fi keys are on a row boundary.
        Text nextRow = k.getRow();
        if (nextRow.equals(boundingRanges.get(0).getStartKey().getRow())) {
            
            if (parentRange.getEndKey() != null && !parentRange.contains(k.followingKey(PartialKey.ROW))) {
                // we are trying to seek out of the range, stop!
                // force the source to be exhausted
                this.source.seek(new Range(k, false, k.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), false), this.seekColumnFamilies,
                                this.seekColumnFamiliesInclusive);
                while (this.source.hasTop()) {
                    this.source.next();
                }
                nextRow = null;
            } else {
                Range followingRowRange = new Range(k.followingKey(PartialKey.ROW), parentRange.getEndKey());
                if (log.isTraceEnabled()) {
                    log.trace("moveToNextRow(Key k), followingRowRange: " + followingRowRange);
                }
                // do an initial seek to determine the next row (needed to calculate bounding ranges below)
                source.seek(followingRowRange, this.seekColumnFamilies, seekColumnFamiliesInclusive);
                scannedKeys.incrementAndGet();
                if (source.hasTop()) {
                    nextRow = source.getTopKey().getRow();
                } else {
                    nextRow = null;
                }
            }
        }
        
        if (log.isTraceEnabled()) {
            log.trace("moveToNextRow, nextRow: " + nextRow);
        }
        
        // The boundingRange is used to test that we have the right fieldName->fieldValue pairing.
        boundingRanges.clear();
        
        if (nextRow != null) {
            boundingRanges.addAll(this.buildBoundingRanges(nextRow, fiName, fieldValue, this.dataType));
            
            if (log.isTraceEnabled()) {
                log.trace("findTop() boundingRange: " + boundingRanges);
            }
            
            // build range does not check anything again the parentRange, so let's check it ourselves.
            // if our boundingRange is greater than the end key of the Parent range, then we are done.
            if (areBoundingRangesPastParentRange()) {
                boundingRanges.clear();
                nextRow = null;
            } else {
                // When we seek the source, we want to update the starting key
                // and hang onto the parentRange's end key
                Range seekRange = new Range(boundingRanges.get(0).getStartKey(), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
                this.parentRange = seekRange;
                if (log.isTraceEnabled()) {
                    log.trace("findTop() seeking to: " + seekRange);
                }
                
                // If we've moved to the next row and landed on a valid key in
                // our range, we can simply continue, otherwise seek the source.
                if (!source.hasTop() || !seekRange.contains(source.getTopKey())) {
                    source.seek(seekRange, this.seekColumnFamilies, seekColumnFamiliesInclusive);
                    scannedKeys.incrementAndGet();
                }
            }
        }
        return nextRow;
    }
    
    /**
     * Make sure our boundingRange startKey is not past the parent range
     */
    protected boolean areBoundingRangesPastParentRange() {
        for (Range boundingRange : boundingRanges) {
            if (null != parentRange.getEndKey() && boundingRange.getStartKey().compareTo(parentRange.getEndKey()) > 0) {
                return true;
            }
        }
        return false;
    }
    
    protected boolean boundingRangesContainKey(Key key) {
        for (Range boundingRange : boundingRanges) {
            if (boundingRange.contains(key)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Should we move to the next field index? The key presented is outside the bounding box (wrong fieldname or value) or has the wrong datatype. Hence we
     * should move to the next field index (may be overridden by other classes requiring more searching in the current field index).
     */
    protected boolean shouldMoveToNextFieldIndex(Key k) {
        return true;
    }
    
    /**
     * Basic method to find our topKey which matches our given FieldName,FieldValue.
     * 
     * @throws IOException
     */
    protected void findTop() throws IOException {
        log.trace("findTop()");
        while (true) {
            if (!source.hasTop()) {
                log.trace("Source does not have top");
                break;
            }
            Key k = source.getTopKey();
            if (!parentRange.contains(k)) {
                log.trace("Source is out of the parentRange");
                break;
            }
            // ensure that we are still within range
            if (boundingRangesContainKey(k)) {
                // check that this key is valid per our time and datatype filters
                if ((timeFilter != null && !timeFilter.apply(k)) || (datatypeFilter != null && !datatypeFilter.apply(k))) {
                    source.next();
                    scannedKeys.incrementAndGet();
                } else {
                    topKey = buildEventKey(k, returnKeyType);
                    topValue = source.getTopValue();
                    // final check to ensure all keys are contained by initial seek
                    if (initialSeekRange.contains(topKey)) {
                        if (log.isDebugEnabled()) {
                            log.debug("boundingRange contains key " + k);
                            log.debug("setting as topKey " + topKey);
                        }
                        // this should be the only way out of this method with topKey and topValue set
                        return;
                    } else {
                        source.next();
                        scannedKeys.incrementAndGet();
                    }
                }
            } else if (shouldMoveToNextFieldIndex(k)) {
                if (log.isTraceEnabled()) {
                    log.trace("findTop, top out of local range: " + k);
                }
                
                Text nextRow = moveToNextRow(k);
                
                if (log.isTraceEnabled()) {
                    log.trace("findTop, nextRow: " + nextRow);
                }
                
                // NOTE: the moveToNextRow method takes care of checking the nextRow against
                // the parentRange's end key and returns null if it's beyond it.
                if (nextRow == null) {
                    // end of tablet or the next row is past our end key, stop
                    break;
                }
                
            } else {
                source.next();
                scannedKeys.incrementAndGet();
            }
        }
        
        // We didn't find a top key and hit an ending condition.
        topKey = null;
        topValue = null;
    }
    
}
