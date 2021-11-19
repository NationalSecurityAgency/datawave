package datawave.iterators.filter;

import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterOptions;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Test class used for the purpose of providing an alternative matchPattern format from TokenSpecParser with some overlap.
 *
 * The matchPattern format is expected to contain multiple newline-separated lines, with each line conforming to the following format:
 * "<descriptiveLabel> <pattern>=<number><duration units>" where "descriptiveLabel" and "pattern" each contain one or more alphanumeric characters. "number"
 * contains one or more digits, representing an Integer "duration units" contains exactly one of the following: ms, s, or d Leading and trailing spaces are
 * ignored.
 *
 * Example: foobar xyz=5d
 *
 * Each line must have the above pattern - no empty lines.
 */
public class ColumnVisibilityLabeledFilter extends AppliedRule {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ColumnVisibilityLabeledFilter.class);
    
    private Map<String,Long> patternToTtl;
    private boolean filterRuleApplied;
    
    /**
     * Used to initialize the {@code FilterRule} implementation
     *
     * @param options
     *            {@code Map} object containing the MATCHPATTERN for the filter rule.
     * @param iterEnv
     * @see datawave.iterators.filter.AgeOffConfigParams
     */
    @Override
    public void init(FilterOptions options, IteratorEnvironment iterEnv) {
        super.init(options, iterEnv);
        if (options == null) {
            throw new IllegalArgumentException("options must be set for FilterRule implementation");
        }
        
        if (options.getOption(AgeOffConfigParams.MATCHPATTERN) != null) {
            String[] lines = StringUtils.split(options.getOption(AgeOffConfigParams.MATCHPATTERN), '\n');
            patternToTtl = new HashMap(lines.length);
            for (String line : lines) {
                populateMapWithTimeToLiveValue(patternToTtl, line);
            }
        }
    }
    
    /**
     * @param map
     *            maps pattern to a time to live (in millis)
     * @param line
     *            contains one line of matchPattern, in format described above
     */
    private void populateMapWithTimeToLiveValue(Map<String,Long> map, String line) {
        String trimmedLine = line.trim();
        
        int indexOfFirstSpace = trimmedLine.indexOf(' ');
        String descriptiveLabel = trimmedLine.substring(0, indexOfFirstSpace);
        
        String remainder = trimmedLine.substring(indexOfFirstSpace).trim();
        String[] parts = remainder.split("=");
        String pattern = parts[0];
        String ttlString = parts[1];
        
        map.put(pattern, convertTtlStringToMillis(ttlString));
        LOG.debug("Added {} -> {} to map for {}.", pattern, map.get(pattern), descriptiveLabel);
    }
    
    private Long convertTtlStringToMillis(String ttlString) {
        if (ttlString.endsWith("ms")) {
            return Long.parseLong(ttlString.substring(0, ttlString.length() - 2));
        } else if (ttlString.endsWith("s")) {
            return 1000 * Long.parseLong(ttlString.substring(0, ttlString.length() - 1));
        } else if (ttlString.endsWith("d")) {
            return 1000 * 60 * 60 * 24 * Long.parseLong(ttlString.substring(0, ttlString.length() - 1));
        }
        throw new IllegalStateException(ttlString + " does not conform to specified format.");
    }
    
    @Override
    public boolean isFilterRuleApplied() {
        return this.filterRuleApplied;
    }
    
    @Override
    public boolean accept(AgeOffPeriod ageOffPeriod, Key k, Value V) {
        // ignore default ageOffPeriod
        String columnVisibilityStr = k.getColumnVisibility().toString();
        for (Map.Entry<String,Long> entry : this.patternToTtl.entrySet()) {
            if (columnVisibilityStr.contains(entry.getKey())) {
                Long timeToLive = entry.getValue();
                long cutOff = ageOffPeriod.getCutOffMilliseconds();
                // move cut-off back by the timeToLive
                if (timeToLive > 0) {
                    // remove offset for default TTL
                    cutOff += ageOffPeriod.getTtl() * ageOffPeriod.getTtlUnitsFactor();
                    
                    // deduct TTL for this key
                    cutOff -= timeToLive;
                }
                this.filterRuleApplied = true;
                return k.getTimestamp() > cutOff;
            }
        }
        return true;
    }
}
