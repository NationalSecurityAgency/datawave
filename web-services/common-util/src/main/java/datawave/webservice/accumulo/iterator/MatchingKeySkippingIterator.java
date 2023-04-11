package datawave.webservice.accumulo.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.StringUtils;

/**
 * Iterator that skips over matching keys. By default, it will skip over matching rows. There is currently an option (rowDelimeter) that will configure this
 * iterator to skip over rows that match up to the delimiter (prefix matching).
 */
public class MatchingKeySkippingIterator extends SkippingIterator implements OptionDescriber {
    // options
    public static final String NUM_SCANS_STRING_NAME = "scansBeforeSeek";
    public static final String ROW_DELIMITER_OPTION = "rowDelimiter"; // will compare rows from start to the first occurrence of this delimeter
    
    // iterator predecessor seek options to pass through
    private Range latestRange;
    private Collection<ByteSequence> latestColumnFamilies;
    private boolean latestInclusive;
    
    // private fields
    private Key lastKeyFound;
    private int numscans;
    private String delimiter;
    protected Comparator<Key> comparator = new RowEqualsComparator();
    
    /*
     * convenience method to set the option to optimize the frequency of scans vs. seeks
     */
    public static void setNumScansBeforeSeek(IteratorSetting cfg, int num) {
        cfg.addOption(NUM_SCANS_STRING_NAME, Integer.toString(num));
    }
    
    // this must be public for OptionsDescriber
    public MatchingKeySkippingIterator() {
        super();
    }
    
    public MatchingKeySkippingIterator(MatchingKeySkippingIterator other, IteratorEnvironment env) {
        super();
        setSource(other.getSource().deepCopy(env));
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new MatchingKeySkippingIterator(this, env);
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        String o = options.get(NUM_SCANS_STRING_NAME);
        numscans = o == null ? 10 : Integer.parseInt(o);
        delimiter = options.get(ROW_DELIMITER_OPTION);
        if (null != delimiter) {
            comparator = new RowPrefixComparator(delimiter);
        }
    }
    
    // this is only ever called immediately after getting "next" entry
    @Override
    protected void consume() throws IOException {
        if (finished == true || lastKeyFound == null)
            return;
        int count = 0;
        while (getSource().hasTop() && (comparator.compare(lastKeyFound, getSource().getTopKey()) == 0)) {
            
            // try to efficiently jump to the next matching key
            if (count < numscans) {
                ++count;
                getSource().next(); // scan
            } else {
                // too many scans, just seek
                count = 0;
                
                // determine where to seek to, but don't go beyond the user-specified range
                Key nextKey = getSource().getTopKey().followingKey(PartialKey.ROW);
                if (!latestRange.afterEndKey(nextKey))
                    getSource().seek(new Range(nextKey, true, latestRange.getEndKey(), latestRange.isEndKeyInclusive()), latestColumnFamilies, latestInclusive);
                else {
                    finished = true;
                    break;
                }
            }
        }
        lastKeyFound = getSource().hasTop() ? getSource().getTopKey() : null;
    }
    
    private boolean finished = true;
    
    @Override
    public boolean hasTop() {
        return !finished && getSource().hasTop();
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // save parameters for future internal seeks
        latestRange = range;
        latestColumnFamilies = columnFamilies;
        latestInclusive = inclusive;
        lastKeyFound = null;
        
        Key startKey = range.getStartKey();
        Range seekRange = new Range(startKey == null ? null : new Key(startKey.getRow()), true, range.getEndKey(), range.isEndKeyInclusive());
        super.seek(seekRange, columnFamilies, inclusive);
        finished = false;
        
        if (getSource().hasTop()) {
            lastKeyFound = getSource().getTopKey();
            if (range.beforeStartKey(getSource().getTopKey()))
                consume();
        }
    }
    
    @Override
    public IteratorOptions describeOptions() {
        String name = "firstEntry";
        String desc = "Only allows iteration over the first entry per row";
        HashMap<String,String> namedOptions = new HashMap<>();
        namedOptions.put(NUM_SCANS_STRING_NAME, "Number of scans to try before seeking [10]");
        namedOptions.put(ROW_DELIMITER_OPTION, "Delimiter for comparing rows. Will compare from start of row to the first occurrence of this delimeter");
        return new IteratorOptions(name, desc, namedOptions, null);
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        try {
            String o = options.get(NUM_SCANS_STRING_NAME);
            Integer i = o == null ? 10 : Integer.parseInt(o);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    public static class RowEqualsComparator implements Comparator<Key> {
        @Override
        public int compare(Key k1, Key k2) {
            return (k1.getRow().compareTo(k2.getRow()));
        }
    }
    
    public static class RowPrefixComparator implements Comparator<Key> {
        private String delimiter;
        
        public RowPrefixComparator(String delimiter) {
            this.delimiter = delimiter;
        }
        
        @Override
        public int compare(Key k1, Key k2) {
            return StringUtils.substringBefore(k1.getRow().toString(), this.delimiter)
                            .compareTo(StringUtils.substringBefore(k2.getRow().toString(), this.delimiter));
        }
        
    }
}
