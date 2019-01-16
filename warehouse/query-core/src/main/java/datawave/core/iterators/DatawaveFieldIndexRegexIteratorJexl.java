package datawave.core.iterators;

import datawave.query.Constants;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
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
    
    public static class Builder<B extends Builder<B>> extends DatawaveFieldIndexCachingIteratorJexl.Builder<B> {
        
        public DatawaveFieldIndexRegexIteratorJexl build() {
            return new DatawaveFieldIndexRegexIteratorJexl(this);
        }
        
    }
    
    public static Builder<?> builder() {
        return new Builder();
    }
    
    protected DatawaveFieldIndexRegexIteratorJexl(Builder builder) {
        super(builder);
        this.regex = builder.fieldValue.toString();
        try {
            // now fix the fValue to be the part we use for ranges
            JavaRegexAnalyzer analyzer = new JavaRegexAnalyzer(this.regex);
            if (analyzer.isLeadingLiteral()) {
                setFieldValue(new Text(analyzer.getLeadingLiteral()));
            } else {
                setFieldValue(new Text(""));
            }
        } catch (JavaRegexParseException ex) {
            throw new IllegalStateException("Unable to parse regex " + regex, ex);
        }
        
    }
    
    private String regex = null;
    private ThreadLocal<Pattern> pattern = ThreadLocal.withInitial(() -> Pattern.compile(regex));
    
    // -------------------------------------------------------------------------
    // ------------- Constructors
    public DatawaveFieldIndexRegexIteratorJexl() {
        super();
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
