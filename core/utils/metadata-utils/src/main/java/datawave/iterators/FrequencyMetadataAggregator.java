package datawave.iterators;

import java.io.IOException;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iteratorsImpl.conf.ColumnSet;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;

import datawave.marking.MarkingFunctions;
import datawave.query.model.DateFrequencyMap;
import datawave.util.StringUtils;
import datawave.util.time.DateHelper;

/**
 * Aggregates entries in the metadata table for the "f", "i", and "ri" columns. When initially ingested, entries for these columns have a column qualifier with
 * the format {@code <datatype>\0<yyyyMMdd>}, and a value containing a possibly partial frequency count for the date in the column qualifier. Entries with the
 * same row, column family, datatype, and column family will be aggregated into a single entry where the column qualifier consists of the datatype and the value
 * consists of an encoded {@link DateFrequencyMap} with the dates and counts seen. Additionally, this aggregator will handle the case where we have a previously
 * aggregated entry and freshly ingested rows that need to be aggregated together.
 */
public class FrequencyMetadataAggregator extends WrappingIterator implements OptionDescriber {
    
    public static final String COMBINE_VISIBILITIES_OPTION = "COMBINE_VISIBILITIES";
    public static final String COLUMNS_OPTION = "columns";
    public static final String AGGREGATED = "AGGREGATED";
    
    private static final Logger log = Logger.getLogger(FrequencyMetadataAggregator.class);
    private static final String NULL_BYTE = "\0";
    private static final MarkingFunctions markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
    
    private boolean combineVisibilities;
    private ColumnSet columns;
    
    private Key topKey;
    private Value topValue;
    
    private final TreeMap<Key,Value> cache;
    private final Map<ColumnVisibility,DateFrequencyMap> visibilityToDateFrequencies;
    private final Map<ColumnVisibility,Long> visibilityToMaxTimestamp;
    
    private final Key workKey = new Key();
    private final Text currentRow = new Text();
    private final Text currentColumnFamily = new Text();
    private String currentDatatype;
    private String currentDate;
    private ColumnVisibility currentVisibility;
    private long currentTimestamp;
    private boolean isCurrentAggregated;
    
    public FrequencyMetadataAggregator() {
        cache = new TreeMap<>();
        visibilityToDateFrequencies = new HashMap<>();
        visibilityToMaxTimestamp = new HashMap<>();
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        FrequencyMetadataAggregator copy = new FrequencyMetadataAggregator();
        copy.setSource(getSource().deepCopy(env));
        copy.combineVisibilities = combineVisibilities;
        return copy;
    }
    
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(COMBINE_VISIBILITIES_OPTION, "Boolean value denoting whether to combine entries with different visibilities. Defaults to false.");
        options.put(COLUMNS_OPTION, "<col fam>[:<col qual>]{,<col fam>[:<col qual>]} escape non-alphanum chars using %<hex>.");
        return new IteratorOptions(getClass().getSimpleName(), "An iterator used to collapse frequency columns in the metadata table", options, null);
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (options.containsKey(COMBINE_VISIBILITIES_OPTION)) {
            try {
                // noinspection ResultOfMethodCallIgnored
                Boolean.parseBoolean(options.get(COMBINE_VISIBILITIES_OPTION));
            } catch (Exception e) {
                throw new IllegalArgumentException("Bad boolean for " + COMBINE_VISIBILITIES_OPTION + " option: " + options.get(COMBINE_VISIBILITIES_OPTION));
            }
        }
        
        if (!options.containsKey(COLUMNS_OPTION)) {
            throw new IllegalArgumentException("Options must include " + COLUMNS_OPTION);
        }
        
        String encodedColumns = options.get(COLUMNS_OPTION);
        if (encodedColumns.isEmpty()) {
            throw new IllegalArgumentException("Empty columns specified for " + COLUMNS_OPTION);
        }
        
        for (String columns : Splitter.on(",").split(encodedColumns)) {
            if (!ColumnSet.isValidEncoding(columns)) {
                throw new IllegalArgumentException("invalid column encoding " + encodedColumns);
            }
        }
        
        return true;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        
        combineVisibilities = options.containsKey(COMBINE_VISIBILITIES_OPTION) && Boolean.parseBoolean(options.get(COMBINE_VISIBILITIES_OPTION));
        columns = new ColumnSet(List.of(StringUtils.split(options.get(COLUMNS_OPTION), ",")));
        
        if (log.isTraceEnabled()) {
            log.trace("Option " + COMBINE_VISIBILITIES_OPTION + ": " + combineVisibilities);
            log.trace("Option " + COLUMNS_OPTION + ": " + columns);
        }
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // Do not seek to the middle of a value that should be combined.
        Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
        
        super.seek(seekRange, columnFamilies, inclusive);
        findTop();
        
        if (range.getStartKey() != null) {
            while (hasTop() && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
                            && getTopKey().getTimestamp() > range.getStartKey().getTimestamp()) {
                // Value has a more recent timestamp, pass it up.
                next();
            }
        }
        
        while (hasTop() && range.beforeStartKey(getTopKey())) {
            next();
        }
    }
    
    @Override
    public Key getTopKey() {
        return topKey == null ? super.getTopKey() : topKey;
    }
    
    @Override
    public Value getTopValue() {
        return topValue == null ? super.getTopValue() : topValue;
    }
    
    @Override
    public boolean hasTop() {
        return topKey != null || super.hasTop();
    }
    
    @Override
    public void next() throws IOException {
        log.trace("Fetching next");
        // If topKey is not null, the last call to next() popped an entry from the cache. Reset to null. If any more entries remain in the cache, they will be
        // popped in findTop().
        if (topKey != null) {
            topKey = null;
            topValue = null;
        } else {
            // If topKey is null, the last call to next() did not pop an entry from the cache. Advance to the next from the source. We will determine if
            // aggregation is needed in findTop().
            super.next();
        }
        
        findTop();
    }
    
    private void findTop() throws IOException {
        log.trace("Finding top");
        // Attempt to pop an entry from the cache. If no entries remain, evaluate the next key for potential aggregation.
        if (!popCache()) {
            if (super.hasTop()) {
                workKey.set(super.getTopKey());
                // Check if the current key contains a column marked for aggregation, and is not deleted. If so, rebuild the cache with the relevant aggregated
                // entries.
                if (columns.contains(workKey) && !workKey.isDeleted()) {
                    updateCache();
                    popCache();
                }
            }
        }
    }
    
    /**
     * Set {@link #topKey} and {@link #topValue} to the next available entry in the cache. Returns true if the cache was not empty, or false otherwise.
     */
    private boolean popCache() {
        log.trace("Popping cache");
        if (!cache.isEmpty()) {
            Map.Entry<Key,Value> entry = cache.pollFirstEntry();
            topKey = entry.getKey();
            topValue = entry.getValue();
            return true;
        }
        return false;
    }
    
    /**
     * Reset all current tracking variables.
     */
    private void resetCurrent() {
        currentRow.clear();
        currentColumnFamily.clear();
        currentDatatype = null;
        currentDate = null;
        currentVisibility = null;
        currentTimestamp = 0L;
        isCurrentAggregated = false;
        visibilityToDateFrequencies.clear();
        visibilityToMaxTimestamp.clear();
    }
    
    /**
     * Iterate over the source entries, aggregate all entries for the next row/column family/datatype combination, and add them to the cache.
     */
    private void updateCache() throws IOException {
        log.trace("Updating cache");
        
        resetCurrent();
        
        while (true) {
            // If the source does not have any more entries, wrap up the last batch of entries.
            if (!super.hasTop()) {
                log.trace("Source does not have top");
                wrapUpCurrent();
                return;
            }
            
            workKey.set(super.getTopKey());
            if (log.isTraceEnabled()) {
                log.trace("updateCache examining key " + workKey);
            }
            
            // If the current entry has a different row, column family, or datatype from the previous entry, wrap up and return the current
            // batch of entries.
            if (!partOfCurrentAggregation(workKey)) {
                wrapUpCurrent();
                return;
            }
            
            // Aggregate the current entry only if it is not deleted.
            if (!workKey.isDeleted()) {
                // Aggregate the current entry.
                aggregateCurrent();
            } else {
                // Add the deleted entry to the cache so that it is available for scanning, but do not include it as part of the aggregation.
                cache.put(super.getTopKey(), super.getTopValue());
            }
            
            // Advance to the next entry from the source.
            super.next();
        }
    }
    
    /**
     * Return true if the current entry has the same row, column family, and datatype from the previous entry, or false otherwise.
     */
    private boolean partOfCurrentAggregation(Key key) {
        // Update the current row if null.
        if (currentRow.getLength() == 0) {
            currentRow.set(key.getRow());
            if (log.isTraceEnabled()) {
                log.trace("Set current row to " + currentRow);
            }
            // Check if we're on a new field.
        } else if (!currentRow.equals(key.getRow())) {
            if (log.isTraceEnabled()) {
                log.trace("Next row " + key.getRow() + " differs from prev " + currentRow);
            }
            return false;
        }
        
        // Update the current column family if null.
        if (currentColumnFamily.getLength() == 0) {
            currentColumnFamily.set(key.getColumnFamily());
            if (log.isTraceEnabled()) {
                log.trace("Set current column family to " + currentColumnFamily);
            }
            // Check if we're on a new column family.
        } else if (!currentColumnFamily.equals(key.getColumnFamily())) {
            if (log.isTraceEnabled()) {
                log.trace("Next column family " + key.getColumnFamily() + " differs from prev " + currentColumnFamily);
            }
            return false;
        }
        
        String columnQualifier = key.getColumnQualifier().toString();
        int separatorPos = columnQualifier.indexOf(NULL_BYTE);
        
        // If a null byte is not present, this is an entry with a legacy format and should not be aggregated.
        if (separatorPos == -1) {
            if (log.isTraceEnabled()) {
                log.trace("Found column qualifier that does not contain null byte: " + columnQualifier);
            }
            return false;
        }
        
        String datatype = columnQualifier.substring(0, separatorPos);
        String remainder = columnQualifier.substring((separatorPos + 1));
        
        // If a second null byte is present, this is an entry with an index boundary marker in the format <datatype>\0<date>\0<true|false> and should not be
        // aggregated.
        if (remainder.contains(NULL_BYTE)) {
            if (log.isTraceEnabled()) {
                log.trace("Found index boundary marker: " + columnQualifier);
            }
            return false;
        }
        
        // This is an aggregated entry.
        if (remainder.equals(AGGREGATED)) {
            isCurrentAggregated = true;
        } else {
            // The remainder should typically be a date, but in rare cases may be a legacy format with the type class name instead of the date, and cannot be
            // aggregated if so. Check if the remainder can be parsed as a date.
            try {
                DateHelper.parse(remainder);
            } catch (DateTimeParseException e) {
                if (log.isTraceEnabled()) {
                    log.trace("Found unparseable date: " + columnQualifier);
                }
                return false;
            }
            currentDate = columnQualifier.substring((separatorPos + 1));
            if (log.isTraceEnabled()) {
                log.trace("Set current date to " + currentDate);
            }
        }
        
        // Update the current datatype if null.
        if (currentDatatype == null) {
            currentDatatype = datatype;
            if (log.isTraceEnabled()) {
                log.trace("Set current datatype to " + currentDatatype);
            }
            // Check if we're on a new datatype.
        } else if (!currentDatatype.equals(datatype)) {
            if (log.isTraceEnabled()) {
                log.trace("Next datatype " + datatype + " differs from prev " + currentDatatype);
            }
            return false;
        }
        
        // Update the current visibility and timestamp.
        currentVisibility = new ColumnVisibility(key.getColumnVisibility());
        currentTimestamp = key.getTimestamp();
        return true;
    }
    
    /**
     * Aggregate the current entry.
     */
    private void aggregateCurrent() {
        Value value = super.getTopValue();
        // Fetch the date-frequency map for the current column visibility, creating one if not present.
        DateFrequencyMap dateFrequencies = visibilityToDateFrequencies.computeIfAbsent(currentVisibility, (k) -> new DateFrequencyMap());
        
        // If the current entry has an aggregated value, parse it as such and merge it with the date-frequency map.
        if (isCurrentAggregated) {
            try {
                DateFrequencyMap entryMap = new DateFrequencyMap(value.get());
                dateFrequencies.incrementAll(entryMap);
            } catch (IOException e) {
                Key key = super.getTopKey();
                log.error("Failed to parse date frequency map from value for key " + key, e);
                throw new IllegalArgumentException("Failed to parse date frequency map from value for key " + key, e);
            }
        } else {
            // If the current entry does not have an aggregated value, it has a count for a specific date. Increment the count for the date in the map.
            long count = LongCombiner.VAR_LEN_ENCODER.decode(value.get());
            dateFrequencies.increment(currentDate, count);
        }
        
        // If the current timestamp is later than the previously tracked timestamp for the current column visibility, update the tracked timestamp.
        if (visibilityToMaxTimestamp.containsKey(currentVisibility)) {
            long prevTimestamp = visibilityToMaxTimestamp.get(currentVisibility);
            if (prevTimestamp < currentTimestamp) {
                visibilityToMaxTimestamp.put(currentVisibility, currentTimestamp);
            }
        } else {
            visibilityToMaxTimestamp.put(currentVisibility, currentTimestamp);
        }
    }
    
    /**
     * Create the entries to be returned by {@link #next()} and add them to the cache.
     */
    private void wrapUpCurrent() {
        if (log.isTraceEnabled()) {
            log.trace("Wrapping up for row: " + currentRow + ", cf: " + currentColumnFamily + ", cq: " + currentDatatype);
        }
        
        cache.putAll(buildCacheEntries());
        resetCurrent();
    }
    
    /**
     * Build and return a sorted map of the key-value entries that should be made available to be returned by {@link #next()}.
     */
    private Map<Key,Value> buildCacheEntries() {
        if (log.isTraceEnabled()) {
            log.trace("buildTopKeys, currentRow: " + currentRow);
            log.trace("buildTopKeys, currentColumnFamily: " + currentColumnFamily);
            log.trace("buildTopKeys, currentDatatype: " + currentDatatype);
        }
        
        Text columnQualifier = new Text(currentDatatype + NULL_BYTE + AGGREGATED);
        
        // If we are combining all entries regardless of column visibility, we will end up with one entry to return.
        if (combineVisibilities) {
            // Combine the visibilities and frequencies, and find the latest timestamp.
            ColumnVisibility combined = combineAllVisibilities();
            long latestTimestamp = getLatestTimestamp();
            DateFrequencyMap combinedFrequencies = combineAllDateFrequencies();
            
            // Return the single key-value pair.
            Key key = new Key(currentRow, currentColumnFamily, columnQualifier, combined, latestTimestamp);
            Value value = new Value(WritableUtils.toByteArray(combinedFrequencies));
            return Collections.singletonMap(key, value);
        } else {
            Map<Key,Value> entries = new HashMap<>();
            // Create a key-value pair for each distinct column visibility.
            for (Map.Entry<ColumnVisibility,DateFrequencyMap> entry : visibilityToDateFrequencies.entrySet()) {
                ColumnVisibility visibility = entry.getKey();
                long timestamp = visibilityToMaxTimestamp.get(visibility);
                Key key = new Key(currentRow, currentColumnFamily, columnQualifier, visibility, timestamp);
                Value value = new Value(WritableUtils.toByteArray(entry.getValue()));
                entries.put(key, value);
            }
            return entries;
        }
    }
    
    /**
     * Return a {@link ColumnVisibility} that is the combination of all visibilities present in {@link #visibilityToDateFrequencies}.
     */
    private ColumnVisibility combineAllVisibilities() {
        Set<ColumnVisibility> visibilities = visibilityToDateFrequencies.keySet();
        try {
            return markingFunctions.combine(visibilities);
        } catch (MarkingFunctions.Exception e) {
            log.error("Failed to combine visibilities " + visibilities);
            throw new IllegalArgumentException("Failed to combine visibilities " + visibilities, e);
        }
    }
    
    /**
     * Return the latest timestamp present in {@link #visibilityToMaxTimestamp}.
     */
    private long getLatestTimestamp() {
        long max = 0L;
        for (long timestamp : visibilityToMaxTimestamp.values()) {
            max = Math.max(max, timestamp);
        }
        return max;
    }
    
    /**
     * Return a {@link DateFrequencyMap} that contains all date counts present in {@link #visibilityToDateFrequencies}.
     */
    private DateFrequencyMap combineAllDateFrequencies() {
        DateFrequencyMap combined = new DateFrequencyMap();
        for (DateFrequencyMap map : visibilityToDateFrequencies.values()) {
            combined.incrementAll(map);
        }
        return combined;
        
    }
}
