package nsa.datawave.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import nsa.datawave.query.iterators.JumpingIterator;

import nsa.datawave.util.StringUtils;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

@Deprecated
public class IntersectingIteratorJexl implements JumpingIterator<Key,Value> {
    
    protected static final Logger log = Logger.getLogger(IntersectingIteratorJexl.class);
    private TermSource[] sources;
    private int sourcesCount = 0;
    protected Text nullText = new Text();
    protected final byte[] emptyByteArray = new byte[0];
    private Key topKey = null;
    protected Value value = new Value(emptyByteArray);
    private Range overallRange;
    private Text currentRow = null;
    private Text currentDocID = new Text(emptyByteArray);
    private Text parentEndRow;
    private static final String NULL_BYTE = EvaluatingIterator.NULL_BYTE_STRING;
    private static final boolean SEEK_INCLUSIVE = true;
    
    /**
     * Used in representing a Term that is intersected on.
     */
    protected static class TermSource {
        
        public SortedKeyValueIterator<Key,Value> iter;
        public Text dataLocation;
        public Text term;
        public boolean notFlag;
        public Collection<ByteSequence> seekColumnFamily;
        
        public TermSource(TermSource other) {
            this(other.iter, other.dataLocation, other.term, other.notFlag);
        }
        
        public TermSource(SortedKeyValueIterator<Key,Value> iter, Text dataLocation, Text term, boolean notFlag) {
            this.iter = iter;
            this.dataLocation = dataLocation;
            this.term = term;
            this.notFlag = notFlag;
            this.seekColumnFamily = Collections.singleton((ByteSequence) new ArrayByteSequence(dataLocation.getBytes(), 0, dataLocation.getLength()));
        }
        
    }
    
    protected Text getPartition(Key key) {
        return key.getRow();
    }
    
    /**
     * Returns the given key's dataLocation
     *
     * @param key
     * @return
     */
    protected Text getDataLocation(Key key) {
        return key.getColumnFamily();
    }
    
    /**
     * Returns the given key's term
     *
     * @param key
     * @return
     */
    protected Text getTerm(Key key) {
        int idx = 0;
        String sKey = key.getColumnQualifier().toString();
        
        idx = sKey.indexOf(NULL_BYTE);
        return new Text(sKey.substring(0, idx));
    }
    
    /**
     * Returns the given key's DocID
     *
     * @param key
     * @return
     */
    protected String getDatatypeUid(Key key) {
        int idx = 0;
        String sKey = key.getColumnQualifier().toString();
        
        idx = sKey.indexOf(NULL_BYTE);
        return sKey.substring(idx + 1);
    }
    
    /**
     * Build a key from the given row and dataLocation
     *
     * @param row
     *            The desired row
     * @param dataLocation
     *            The desired dataLocation
     * @return
     */
    protected Key buildKey(Text row, Text dataLocation) {
        return new Key(row, (dataLocation == null) ? nullText : dataLocation);
    }
    
    /**
     * Build a key from the given row, dataLocation, and term
     *
     * @param row
     *            The desired row
     * @param dataLocation
     *            The desired dataLocation
     * @param term
     *            The desired term
     * @return
     */
    protected Key buildKey(Text row, Text dataLocation, Text term) {
        log.debug(row + ", " + dataLocation + ", " + term);
        return new Key(row, (dataLocation == null) ? nullText : dataLocation, (term == null) ? nullText : term);
    }
    
    /**
     * Return the key that directly follows the given key
     *
     * @param key
     *            The key who will be directly before the returned key
     * @return
     */
    protected Key buildFollowingPartitionKey(Key key) {
        return key.followingKey(PartialKey.ROW);
    }
    
    /**
     * Empty default constructor
     */
    public IntersectingIteratorJexl() {}
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new IntersectingIteratorJexl(this, env);
    }
    
    public IntersectingIteratorJexl(IntersectingIteratorJexl other, IteratorEnvironment env) {
        if (other.sources != null) {
            sourcesCount = other.sourcesCount;
            sources = new TermSource[sourcesCount];
            for (int i = 0; i < sourcesCount; i++) {
                sources[i] = new TermSource(other.sources[i].iter.deepCopy(env), other.sources[i].dataLocation, other.sources[i].term, other.sources[i].notFlag);
            }
        }
        this.overallRange = other.overallRange;
        this.parentEndRow = other.parentEndRow;
    }
    
    @Override
    public Key getTopKey() {
        return topKey;
    }
    
    @Override
    public Value getTopValue() {
        return value;
    }
    
    @Override
    public boolean hasTop() {
        return currentRow != null;
    }
    
    /**
     * Find the next key in the current TermSource that is at or beyond the cursor (currentRow, currentTerm, currentDocID).
     *
     * @param ts
     *            The index of the current source in <code>sources</code>
     * @return True if the source advanced beyond the cursor
     * @throws IOException
     */
    private boolean seekOneSource(TermSource ts) throws IOException {
        /*
         * Within this loop progress must be made in one of the following forms: - currentRow, currentTerm, or curretDocID must be increased - the given source
         * must advance its iterator This loop will end when any of the following criteria are met - the iterator for the given source is pointing to the key
         * (currentRow, columnFamilies[sourceID], currentTerm, currentDocID) - the given source is out of data and currentRow is set to null - the given source
         * has advanced beyond the endRow and currentRow is set to null
         */
        
        // precondition: currentRow is not null
        boolean advancedCursor = false;
        
        while (true) {
            if (!ts.iter.hasTop()) {
                if (log.isDebugEnabled()) {
                    log.debug("The current iterator no longer has a top");
                }
                
                // If we got to the end of an iterator, found a Match if it's a NOT
                if (ts.notFlag) {
                    break;
                }
                
                currentRow = null;
                // setting currentRow to null counts as advancing the cursor
                return true;
            }
            
            // check if we're past the end key
            int endCompare = -1;
            
            if (log.isDebugEnabled()) {
                log.debug("Current topKey = " + ts.iter.getTopKey());
            }
            
            // we should compare the row to the end of the range
            if (overallRange.getEndKey() != null) {
                if (log.isDebugEnabled()) {
                    log.debug("II.seekOneSource overallRange.getEndKey() != null");
                    log.debug("II.seekOneSource comparing " + overallRange.getEndKey().getRow() + " to " + ts.iter.getTopKey().getRow());
                }
                
                endCompare = overallRange.getEndKey().getRow().compareTo(ts.iter.getTopKey().getRow());
                
                log.debug("II.seekOneSource endCompare = " + endCompare);
                log.debug("II.seekOneSource endkeyInclusive = " + overallRange.isEndKeyInclusive());
                
                if (endCompare < 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("II.seekOneSource at the end of the tablet server");
                    }
                    
                    currentRow = null;
                    
                    // setting currentRow to null counts as advancing the cursor
                    return true;
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("II.seekOneSource overallRange.getEndKey() == null");
                }
            }
            
            // Compare the Row IDs
            int partitionCompare = currentRow.compareTo(getPartition(ts.iter.getTopKey()));
            if (log.isDebugEnabled()) {
                log.debug("Current partition: " + currentRow);
            }
            
            // check if this source is already at or beyond currentRow
            // if not, then seek to at least the current row
            if (partitionCompare > 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Need to seek to the current row");
                    
                    // seek to at least the currentRow
                    log.debug("ts.dataLocation = " + ts.dataLocation.toString());
                    log.debug("Term = " + new Text(ts.term + "\0" + currentDocID).toString());
                }
                
                Key seekKey = buildKey(currentRow, ts.dataLocation, nullText);
                
                if (log.isDebugEnabled()) {
                    log.debug("Seeking to: " + seekKey);
                }
                
                ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamily, SEEK_INCLUSIVE);
                continue;
            }
            
            // check if this source has gone beyond currentRow
            // if so, advance currentRow
            if (partitionCompare < 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Went too far beyond the currentRow");
                }
                
                if (ts.notFlag) {
                    break;
                }
                
                currentRow.set(getPartition(ts.iter.getTopKey()));
                currentDocID.set(emptyByteArray);
                
                advancedCursor = true;
                continue;
            }
            
            // we have verified that the current source is positioned in currentRow
            // now we must make sure we're in the right columnFamily in the current row
            if (ts.dataLocation != null) {
                int dataLocationCompare = ts.dataLocation.compareTo(getDataLocation(ts.iter.getTopKey()));
                
                if (log.isDebugEnabled()) {
                    log.debug("Comparing dataLocations");
                    log.debug("dataLocation = " + ts.dataLocation);
                    log.debug("newDataLocation = " + getDataLocation(ts.iter.getTopKey()));
                }
                
                // check if this source is already on the right columnFamily
                // if not, then seek forwards to the right columnFamily
                if (dataLocationCompare > 0) {
                    
                    Key seekKey = buildKey(currentRow, ts.dataLocation, nullText);
                    
                    if (log.isDebugEnabled()) {
                        log.debug("Need to seek to the right dataLocation, seeking to: " + seekKey);
                    }
                    
                    ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamily, SEEK_INCLUSIVE);
                    if (!ts.iter.hasTop()) {
                        currentRow = null;
                        return true;
                    }
                    
                    continue;
                }
                // check if this source is beyond the right columnFamily
                // if so, then seek to the next row
                if (dataLocationCompare < 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("Went too far beyond the dataLocation");
                    }
                    
                    if (endCompare == 0) {
                        // we're done
                        currentRow = null;
                        
                        // setting currentRow to null counts as advancing the cursor
                        return true;
                    }
                    
                    // Seeking beyond the current dataLocation gives a valid negated result
                    if (ts.notFlag) {
                        break;
                    }
                    
                    Key seekKey = buildFollowingPartitionKey(ts.iter.getTopKey());
                    
                    if (log.isDebugEnabled()) {
                        log.debug("Seeking to: " + seekKey);
                    }
                    
                    ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamily, SEEK_INCLUSIVE);
                    if (!ts.iter.hasTop()) {
                        currentRow = null;
                        return true;
                    }
                    continue;
                }
            }
            
            // Compare the Terms
            int termCompare = ts.term.compareTo(getTerm(ts.iter.getTopKey()));
            if (log.isDebugEnabled()) {
                log.debug("term = " + ts.term);
                log.debug("newTerm = " + getTerm(ts.iter.getTopKey()));
            }
            
            // We need to seek down farther into the data
            if (termCompare > 0) {
                
                Key seekKey = buildKey(currentRow, ts.dataLocation, new Text(ts.term + NULL_BYTE));
                
                if (log.isDebugEnabled()) {
                    log.debug("Need to seek to the right term, seeking to: " + seekKey);
                }
                
                ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamily, SEEK_INCLUSIVE);
                if (!ts.iter.hasTop()) {
                    currentRow = null;
                    return true;
                }
                
                if (log.isDebugEnabled()) {
                    log.debug("topKey after seeking to correct term: " + ts.iter.getTopKey());
                }
                
                continue;
            }
            
            // We've jumped out of the current term, set the new term as currentTerm and start looking again
            if (termCompare < 0) {
                if (log.isDebugEnabled()) {
                    log.debug("TERM: Need to jump to the next row");
                }
                
                if (endCompare == 0) {
                    currentRow = null;
                    
                    return true;
                }
                
                if (ts.notFlag) {
                    break;
                }
                
                Key seekKey = buildFollowingPartitionKey(ts.iter.getTopKey());
                if (log.isDebugEnabled()) {
                    log.debug("Using this key to find the next key: " + ts.iter.getTopKey());
                    log.debug("Seeking to: " + seekKey);
                }
                
                ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamily, SEEK_INCLUSIVE);
                
                if (!ts.iter.hasTop()) {
                    currentRow = null;
                    return true;
                }
                
                continue;
            }
            
            // Compare the DocIDs
            Text docid = new Text(getDatatypeUid(ts.iter.getTopKey()));
            int docidCompare = currentDocID.compareTo(docid);
            
            if (log.isDebugEnabled()) {
                log.debug("Comparing DocIDs");
                log.debug("currentDocID = " + currentDocID);
                log.debug("docid = " + docid);
            }
            
            // The source isn't at the right DOC
            if (docidCompare > 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Need to seek to the correct docid");
                }
                
                // seek forwards
                Key seekKey = buildKey(currentRow, ts.dataLocation, new Text(ts.term + NULL_BYTE + currentDocID));
                
                if (log.isDebugEnabled()) {
                    log.debug("Seeking to: " + seekKey);
                }
                
                ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamily, SEEK_INCLUSIVE);
                
                continue;
            }
            
            // if this source has advanced beyond the current column qualifier then advance currentCQ and return true
            if (docidCompare < 0) {
                if (ts.notFlag) {
                    break;
                }
                
                if (log.isDebugEnabled()) {
                    log.debug("We went too far, update the currentDocID to be the location of where were seek'ed to");
                }
                
                currentDocID.set(docid);
                advancedCursor = true;
                break;
            }
            
            // Set the term as currentTerm (in case we found this record on the first try)
            Text currentTerm = getTerm(ts.iter.getTopKey());
            
            if (log.isDebugEnabled()) {
                log.debug("currentTerm = " + currentTerm);
            }
            
            // If we're negated, next() the first TermSource since we guaranteed it was not a NOT term
            if (ts.notFlag) {
                sources[0].iter.next();
                advancedCursor = true;
            }
            
            // If we got here, we have a match
            break;
        }
        
        return advancedCursor;
    }
    
    @Override
    public void next() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("In ModifiedIntersectingIterator.next()");
        }
        
        if (currentRow == null) {
            return;
        }
        
        // precondition: the current row is set up and the sources all have the same column qualifier
        // while we don't have a match, seek in the source with the smallest column qualifier
        sources[0].iter.next();
        
        advanceToIntersection();
        
        if (hasTop()) {
            if (overallRange != null && !overallRange.contains(topKey)) {
                topKey = null;
            }
        }
    }
    
    protected void advanceToIntersection() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("In ModifiedIntersectingIterator.advanceToIntersection()");
        }
        
        boolean cursorChanged = true;
        int numSeeks = 0;
        while (cursorChanged) {
            // seek all of the sources to at least the highest seen column qualifier in the current row
            cursorChanged = false;
            for (TermSource ts : sources) {
                if (currentRow == null) {
                    topKey = null;
                    return;
                }
                numSeeks++;
                if (seekOneSource(ts)) {
                    cursorChanged = true;
                    break;
                }
            }
        }
        
        topKey = buildKey(currentRow, this.currentDocID);
        
        if (log.isDebugEnabled()) {
            log.debug("ModifiedIntersectingIterator: Got a match: " + topKey);
        }
    }
    
    public static String stringTopKey(SortedKeyValueIterator<Key,Value> iter) {
        if (iter.hasTop()) {
            return iter.getTopKey().toString();
        }
        return "";
    }
    
    public static final String columnFamiliesOptionName = "columnFamilies";
    public static final String termValuesOptionName = "termValues";
    public static final String notFlagsOptionName = "notFlags";
    
    /**
     * Encode a <code>Text</code> array of all the columns to intersect on
     *
     * @param columns
     *            The columns to be encoded
     * @return
     */
    public static String encodeColumns(Text[] columns) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            sb.append(new String(Base64.encodeBase64(TextUtil.getBytes(columns[i]))));
            sb.append('\n');
        }
        return sb.toString();
    }
    
    /**
     * Encode a <code>Text</code> array of all of the terms to intersect on. The terms should match the columns in a one-to-one manner
     *
     * @param terms
     *            The terms to be encoded
     * @return
     */
    public static String encodeTermValues(Text[] terms) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < terms.length; i++) {
            sb.append(new String(Base64.encodeBase64(TextUtil.getBytes(terms[i]))));
            sb.append('\n');
        }
        
        return sb.toString();
    }
    
    /**
     * Encode an array of <code>booleans</code> denoted which columns are NOT'ed
     *
     * @param flags
     *            The array of NOTs
     * @return
     */
    public static String encodeBooleans(boolean[] flags) {
        byte[] bytes = new byte[flags.length];
        for (int i = 0; i < flags.length; i++) {
            if (flags[i]) {
                bytes[i] = 1;
            } else {
                bytes[i] = 0;
            }
        }
        return new String(Base64.encodeBase64(bytes));
    }
    
    /**
     * Decode the encoded columns into a <code>Text</code> array
     *
     * @param columns
     *            The Base64 encoded String of the columns
     * @return
     */
    public static Text[] decodeColumns(String columns) {
        String[] columnStrings = StringUtils.split(columns, "\n");
        Text[] columnTexts = new Text[columnStrings.length];
        for (int i = 0; i < columnStrings.length; i++) {
            columnTexts[i] = new Text(Base64.decodeBase64(columnStrings[i].getBytes()));
        }
        
        return columnTexts;
    }
    
    /**
     * Decode the encoded terms into a <code>Text</code> array
     *
     * @param terms
     *            The Base64 encoded String of the terms
     * @return
     */
    public static Text[] decodeTermValues(String terms) {
        String[] termStrings = StringUtils.split(terms, "\n");
        Text[] termTexts = new Text[termStrings.length];
        for (int i = 0; i < termStrings.length; i++) {
            termTexts[i] = new Text(Base64.decodeBase64(termStrings[i].getBytes()));
        }
        
        return termTexts;
    }
    
    /**
     * Decode the encoded NOT flags into a <code>boolean</code> array
     *
     * @param flags
     * @return
     */
    public static boolean[] decodeBooleans(String flags) {
        // return null of there were no flags
        if (flags == null) {
            return null;
        }
        
        byte[] bytes = Base64.decodeBase64(flags.getBytes());
        boolean[] bFlags = new boolean[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            bFlags[i] = (bytes[i] == 1);
        }
        
        return bFlags;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("IntersectingIteratorJexl.init()");
        }
        
        Text[] dataLocations = decodeColumns(options.get(columnFamiliesOptionName));
        Text[] terms = decodeTermValues(options.get(termValuesOptionName));
        boolean[] notFlags = decodeBooleans(options.get(notFlagsOptionName));
        
        if (terms.length < 2) {
            throw new IOException("IntersectingIterator requires two or more columns families");
        }
        
        // Scan the not flags.
        // There must be at least one term that isn't negated
        // And we are going to re-order such that the first term is not a ! term
        if (notFlags == null) {
            notFlags = new boolean[terms.length];
            for (int i = 0; i < terms.length; i++) {
                notFlags[i] = false;
            }
        }
        
        // Make sure that the first dataLocation/Term is not a NOT by swapping it with a later dataLocation/Term
        if (notFlags[0]) {
            for (int i = 1; i < notFlags.length; i++) {
                if (!notFlags[i]) {
                    // Swap the terms
                    Text swap = new Text(terms[0]);
                    terms[0].set(terms[i]);
                    terms[i].set(swap);
                    
                    // Swap the dataLocations
                    swap.set(dataLocations[0]);
                    dataLocations[0].set(dataLocations[i]);
                    dataLocations[i].set(swap);
                    
                    // Flip the notFlags
                    notFlags[0] = false;
                    notFlags[i] = true;
                    break;
                }
            }
            
            if (notFlags[0]) {
                throw new IOException("IntersectionIterator requires at least one column family without not");
            }
        }
        
        // Build up the array of sources that are to be intersected
        sources = new TermSource[dataLocations.length];
        for (int i = 0; i < dataLocations.length; i++) {
            sources[i] = new TermSource(source.deepCopy(env), dataLocations[i], terms[i], notFlags[i]);
        }
        
        sourcesCount = dataLocations.length;
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> seekColFams, boolean inclusive) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("In ModifiedIntersectingIterator.seek()");
            log.debug("ModifiedIntersectingIterator.seek Given range => " + range);
        }
        
        if (null == range.getStartKey()) {
            currentRow = new Text();
        } else {
            currentRow = range.getStartKey().getRow();
        }
        
        currentDocID.set(emptyByteArray);
        
        // NOTE the seekColFams and inlusive parameters are ignored in favor of
        // each TermSource using its own seekColFam filter which is its dataLocation i.e. fi\x00FIELDNAME
        doSeek(range);
    }
    
    /**
     * Checks whether or not we are at the end of the range for this particular seek We will later use this to short circuit
     *
     * @param range
     * @return
     */
    protected boolean isEndOfRange(Range range) {
        if (range.getStartKey() != null && range.getEndKey() != null && !range.isStartKeyInclusive()) {
            final Text startKeyCf = range.getStartKey().followingKey(PartialKey.ROW_COLFAM).getColumnFamily();
            final Text endKeyCf = range.getEndKey().getColumnFamily();
            
            // initially I simply checked if startKeyCf was < endKey; however
            // we're checking whether or not endKey begins with the same
            // combination of fv, data type, and uid
            
            return startKeyCf.equals(endKeyCf);
        }
        return false;
        
    }
    
    private void doSeek(Range range) throws IOException {
        
        overallRange = new Range(range);
        
        if (range.getEndKey() != null && range.getEndKey().getRow() != null) {
            this.parentEndRow = range.getEndKey().getRow();
        }
        
        /**
         * Assuming that don't have a simple column family to search through. If we are dealing with a single family, then we can short circuit. note that we're
         * setting topKey to null, which is sufficient to short circuit
         */
        
        if (isEndOfRange(range)) {
            currentRow = null;
            topKey = null;
            return;
        }
        
        // seek each of the sources to the right column family within the row given by key
        for (int i = 0; i < sourcesCount; i++) {
            Key startKey = null;
            Key endKey = null;
            if (range.getStartKey() != null) {
                // Build a key with the DocID if one is given
                if (range.getStartKey().getColumnFamily() != null) {
                    if (range.isStartKeyInclusive()) {
                        startKey = buildKey(getPartition(range.getStartKey()), (sources[i].dataLocation == null) ? nullText : sources[i].dataLocation,
                                        (sources[i].term == null) ? nullText : new Text(sources[i].term + NULL_BYTE + range.getStartKey().getColumnFamily()));
                    } else {
                        startKey = buildKey(getPartition(range.getStartKey()), (sources[i].dataLocation == null) ? nullText : sources[i].dataLocation,
                                        (sources[i].term == null) ? nullText : new Text(sources[i].term + NULL_BYTE + range.getStartKey().getColumnFamily()
                                                        + NULL_BYTE));
                    }
                } // Build a key with just the term.
                else {
                    startKey = buildKey(getPartition(range.getStartKey()), (sources[i].dataLocation == null) ? nullText : sources[i].dataLocation,
                                    (sources[i].term == null) ? nullText : sources[i].term);
                }
                
                // Adhere to the endKey if one was given
                if (range.getEndKey() != null) {
                    if (range.getEndKey().getColumnFamily() != null) {
                        endKey = buildKey(getPartition(range.getEndKey()), (sources[i].dataLocation == null) ? nullText : sources[i].dataLocation,
                                        (sources[i].term == null) ? nullText : new Text(sources[i].term + NULL_BYTE + range.getEndKey().getColumnFamily()));
                    } else {
                        endKey = buildKey(getPartition(range.getEndKey()), (sources[i].dataLocation == null) ? nullText : sources[i].dataLocation,
                                        (sources[i].term == null) ? nullText : sources[i].term);
                    }
                }
                
                sources[i].iter.seek(new Range(startKey, range.isStartKeyInclusive(), endKey, range.isEndKeyInclusive()), sources[i].seekColumnFamily,
                                SEEK_INCLUSIVE);
            } else {
                // The range is essentially (inf-, inf+)
                sources[i].iter.seek(range, sources[i].seekColumnFamily, SEEK_INCLUSIVE);
            }
        }
        
        advanceToIntersection();
        
        if (hasTop()) {
            if (overallRange != null && !overallRange.contains(topKey)) {
                
                if (log.isDebugEnabled()) {
                    log.debug("doSeek, topKey " + topKey + " is outside of overall range: " + overallRange);
                }
                
                topKey = null;
            }
        }
    }
    
    public void addSource(SortedKeyValueIterator<Key,Value> source, IteratorEnvironment env, Text dataLocation, Text term, boolean notFlag) {
        // Check if we have space for the added Source
        if (sources == null) {
            sources = new TermSource[1];
        } else {
            // allocate space for node, and copy current tree.
            // TODO: Should we change this to an ArrayList so that we can just add() ?
            TermSource[] localSources = new TermSource[sources.length + 1];
            int currSource = 0;
            for (TermSource myTerm : sources) {
                // TODO: Do I need to call new here? or can I just re-use the term?
                localSources[currSource] = new TermSource(myTerm);
                currSource++;
            }
            sources = localSources;
        }
        
        sources[sourcesCount] = new TermSource(source.deepCopy(env), dataLocation, term, notFlag);
        sourcesCount++;
    }
    
    /*
     * The topKey and jumpKey are in different formats. The topKey returned from this iterator has the docId as the entirety of the qualifier. This should
     * probably be changed at some point as it's different than everything else. Luckily the BLTreeNode handles this in how it parses for docId.
     * ------------------------- IntersectingIterator's topKey is in this format: ShardId | fieldValue | datatype\0uid Jump Key is in this format: ShardId |
     * datatype\0uid | <empty>
     */
    @Override
    public boolean jump(Key jumpKey) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("jump: " + jumpKey);
        }
        
        // is the jumpKey outside my overall range?
        if (parentEndRow != null && parentEndRow.compareTo(jumpKey.getRow()) < 0) {
            // can't go there.
            if (log.isDebugEnabled()) {
                log.debug("jumpRow: " + jumpKey.getRow() + " is greater than my parentEndRow: " + parentEndRow);
            }
            return false;
        }
        
        if (!hasTop()) {
            if (log.isDebugEnabled()) {
                log.debug("jump called, but topKey is null, must need to move to next row");
            }
            return false;
        }
        
        int comp = this.topKey.getRow().compareTo(jumpKey.getRow());
        // compare rows
        if (comp > 0) {
            if (log.isDebugEnabled()) {
                log.debug("jump, our row is ahead of jumpKey.");
                log.debug("jumpRow: " + jumpKey.getRow() + " myRow: " + topKey.getRow() + " parentEndRow" + parentEndRow);
            }
            return hasTop(); // do nothing, we're ahead of jumpKey row
        } else if (comp < 0) { // a row behind jump key, need to move forward
        
            if (log.isDebugEnabled()) {
                log.debug("II jump, row jump");
            }
            
            Range fake = new Range(jumpKey, true, overallRange.getEndKey(), true);
            // note: the seekColFam and inclusive bit of each termsource is used in seek
            seek(fake, null, true);
            return hasTop();
        } else {
            String myRowUid = BooleanLogicIteratorJexl.getEventKeyRowDatatypeUid(this.topKey);
            String jumpRowUid = BooleanLogicIteratorJexl.getEventKeyRowDatatypeUid(jumpKey);
            if (log.isDebugEnabled()) {
                log.debug("myUid: " + myRowUid + "  current topKey: " + this.topKey);
                
                if (jumpRowUid == null) {
                    log.debug("jumpUid is null");
                } else {
                    log.debug("jumpUid: " + jumpRowUid + "  current jumpKey: " + jumpKey);
                }
            }
            
            int ucomp = (null == jumpRowUid) ? 1 : myRowUid.compareTo(jumpRowUid);
            if (ucomp < 0) { // need to move all sources forward
                if (log.isDebugEnabled()) {
                    log.debug("jump, uid jump");
                }
                
                Range range = new Range(jumpKey, true, overallRange.getEndKey(), true);
                
                // We want to set the current row and target docId from what the jump key gives us
                this.currentRow = jumpKey.getRow();
                this.currentDocID = jumpKey.getColumnFamily();
                
                // Seek each term source to the "currentDocID"
                // note: the seekColFam and inclusive bit of each termsource is used in doSeek
                doSeek(range);
                
                // make sure it is in the range if we have one.
                if (hasTop() && parentEndRow != null && topKey.getRow().compareTo(parentEndRow) > 0) {
                    topKey = null;
                }
                if (log.isDebugEnabled() && hasTop()) {
                    log.debug("jump, topKey is now: " + topKey);
                }
                return hasTop();
                
            }// else do nothing
            if (hasTop() && parentEndRow != null && topKey.getRow().compareTo(parentEndRow) > 0) {
                topKey = null;
            }
            return hasTop();
        }
    }
}
