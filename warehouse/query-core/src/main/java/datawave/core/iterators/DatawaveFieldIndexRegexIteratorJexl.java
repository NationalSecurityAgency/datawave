package datawave.core.iterators;

import com.google.common.base.Predicate;
import datawave.core.iterators.querylock.QueryLock;
import datawave.query.Constants;
import datawave.query.iterator.filter.field.index.FieldIndexFilter;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import datawave.query.predicate.TimeFilter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 
 * An iterator for the Datawave shard table, it searches FieldIndex keys and returns Event keys (its topKey must be an Event key).
 *
 * This version takes a regex and will return sorted UIDs that match the supplied regex
 * 
 * FieldIndex keys: fi\0{fieldName}:{fieldValue}\0datatype\0uid
 * 
 * Event key: CF, {datatype}\0{UID}
 * 
 */
public class DatawaveFieldIndexRegexIteratorJexl extends DatawaveFieldIndexCachingIteratorJexl {
    private String regex = null;
    private ThreadLocal<Pattern> pattern = new ThreadLocal<Pattern>() {
        @Override
        protected Pattern initialValue() {
            return Pattern.compile(regex);
        }
    };
    
    // -------------------------------------------------------------------------
    // ------------- Constructors
    public DatawaveFieldIndexRegexIteratorJexl() {
        super();
    }
    
    public DatawaveFieldIndexRegexIteratorJexl(Text fieldName, Text fieldRegex, TimeFilter timeFilter, Predicate<Key> datatypeFilter, long scanThreshold,
                    long scanTimeout, int bufferSize, int maxRangeSplits, int maxOpenFiles, FileSystem fs, Path uniqueDir, QueryLock queryLock,
                    boolean allowDirReuse) throws JavaRegexParseException {
        this(fieldName, fieldRegex, timeFilter, datatypeFilter, false, scanThreshold, scanTimeout, bufferSize, maxRangeSplits, maxOpenFiles, fs, uniqueDir,
                        queryLock, allowDirReuse);
    }
    
    public DatawaveFieldIndexRegexIteratorJexl(Text fieldName, Text fieldRegex, TimeFilter timeFilter, Predicate<Key> datatypeFilter, boolean neg,
                    long scanThreshold, long scanTimeout, int bufferSize, int maxRangeSplits, int maxOpenFiles, FileSystem fs, Path uniqueDir,
                    QueryLock queryLock, boolean allowDirReuse) throws JavaRegexParseException {
        this(fieldName, fieldRegex, timeFilter, datatypeFilter, neg, scanThreshold, scanTimeout, bufferSize, maxRangeSplits, maxOpenFiles, fs, uniqueDir,
                        queryLock, allowDirReuse, DEFAULT_RETURN_KEY_TYPE, true, null);
    }
    
    public DatawaveFieldIndexRegexIteratorJexl(Text fieldName, Text fieldRegex, TimeFilter timeFilter, Predicate<Key> datatypeFilter, boolean neg,
                    long scanThreshold, long scanTimeout, int bufferSize, int maxRangeSplits, int maxOpenFiles, FileSystem fs, Path uniqueDir,
                    QueryLock queryLock, boolean allowDirReuse, PartialKey returnKeyType, boolean sortedUIDs, FieldIndexFilter fieldIndexFilter)
                    throws JavaRegexParseException {
        super(fieldName, fieldRegex, timeFilter, datatypeFilter, neg, scanThreshold, scanTimeout, bufferSize, maxRangeSplits, maxOpenFiles, fs, uniqueDir,
                        queryLock, allowDirReuse, returnKeyType, sortedUIDs, fieldIndexFilter);
        this.regex = fieldRegex.toString();
        // now fix the fValue to be the part we use for ranges
        JavaRegexAnalyzer analyzer = new JavaRegexAnalyzer(this.regex);
        if (analyzer.isLeadingLiteral()) {
            setFieldValue(new Text(analyzer.getLeadingLiteral()));
        } else {
            setFieldValue(new Text(""));
        }
    }
    
    public DatawaveFieldIndexRegexIteratorJexl(DatawaveFieldIndexRegexIteratorJexl other, IteratorEnvironment env) {
        super(other, env);
        this.regex = other.regex;
    }
    
    // -------------------------------------------------------------------------
    // ------------- Overrides
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new DatawaveFieldIndexRegexIteratorJexl(this, env);
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DatawaveFieldIndexRegexIteratorJexl{fName=").append(getFieldName()).append(", fValue=").append(getFieldValue()).append(", regex=")
                        .append(regex).append(", negated=").append(isNegated()).append("}");
        return builder.toString();
    }
    
    @Override
    protected List<Range> buildBoundingFiRanges(Text rowId, Text fiName, Text fieldValue) {
        Key startKey = null;
        Key endKey = null;
        if (isNegated()) {
            startKey = new Key(rowId, fiName);
            endKey = new Key(rowId, new Text(fiName.toString() + '\0'));
            return new RangeSplitter(new Range(startKey, true, endKey, true), getMaxRangeSplit());
        } else {
            // construct new range
            this.boundingFiRangeStringBuilder.setLength(0);
            this.boundingFiRangeStringBuilder.append(fieldValue);
            startKey = new Key(rowId, fiName, new Text(boundingFiRangeStringBuilder.toString()));
            
            this.boundingFiRangeStringBuilder.append(Constants.MAX_UNICODE_STRING);
            endKey = new Key(rowId, fiName, new Text(boundingFiRangeStringBuilder.toString()));
            return new RangeSplitter(new Range(startKey, true, endKey, true), getMaxRangeSplit());
        }
    }
    
    // -------------------------------------------------------------------------
    // ------------- Other stuff
    
    /**
     * Does this key match our regex. Note we are not overriding the super.isMatchingKey() as we need that to work as is NOTE: This method must be thread safe
     * NOTE: The caller takes care of the negation
     *
     * @param k
     * @return
     */
    @Override
    protected boolean matches(Key k) throws IOException {
        boolean matches = false;
        String colq = k.getColumnQualifier().toString();
        
        // search backwards for the null bytes to expose the value in value\0datatype\0UID
        int index = colq.lastIndexOf('\0');
        index = colq.lastIndexOf('\0', index - 1);
        matches = (pattern.get().matcher(colq.substring(0, index)).matches());
        
        return matches;
    }
    
}
