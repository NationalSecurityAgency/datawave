package datawave.iterators.filter;

import java.util.Objects;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import datawave.iterators.filter.TokenTtlTrie.Builder.MERGE_MODE;
import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterOptions;
import datawave.util.CompositeTimestamp;

/**
 * TokenizingAgeoffFilter cuts a field into tokens (splitting at a specified set of delimiters), and makes ageoff decisions based on whether or not any of the
 * supplied tokens are in its configuration. If multiple of the configured tokens are found among the field's tokens, the TTL of the token appearing first in
 * the configuration is applied.
 * <p>
 * This can be used to specify ageoff based on a field with, for example, comma separated values.
 * <p>
 * A sample configuration might look like:
 *
 * <pre>
 * {@code
 *
 * <rule>
 *   <filterClass>datawave.iterators.filter.ColumnQualifierTokenFilter</filterClass>
 *   <!-- Any tokens without specified TTLs will ageoff after 3000ms --<
 *   <ttl units="ms">3000</ttl>
 *
 *   <!-- Field is comma separated -->
 *   <delimiters>,</delimiters>
 *
 *   <matchPattern>
 *     "foo": 5d,
 *     "bar": 300ms,
 *     "baz", <!-- use rule's default TTL -->
 *   </matchPattern>
 * </rule>
 * }
 * </pre>
 *
 * With such a configuration:
 * <ul>
 * <li>a cell with column qualifier foo,bar would be aged off after 5 days ('foo' wins, since it appears first in configuration),</li>
 * <li>a cell with column qualifier foobar,baz would be aged off after 3000ms (only the token 'baz' appears in configuration),</li>
 * <li>a cell with column qualifier baz,bar would be aged off after 300 ms ('bar' wins, since it appears first in configuration), and</li>
 * <li>a cell with column qualifier foobar,barbaz would not be assigned a TTL by this filter.
 * </ul>
 *
 */
public abstract class TokenizingFilterBase extends AppliedRule {
    public static final String DELIMITERS_TAG = "delimiters";
    private String matchPattern = null;
    private TokenTtlTrie scanTrie = null;
    private boolean ruleApplied;

    public abstract byte[] getKeyField(Key k, Value V);

    /**
     * Return a list of delimiters for scans. While the default is to pull this information out of the {@code &lt;delimiters&gt;} tag in the configuration,
     * subclasses may wish to override this to provide fixed delimiter sets.
     *
     * @param options
     *            filter options
     *
     * @return list of delimiters for scans
     */
    public byte[] getDelimiters(FilterOptions options) {
        String delimiters = options.getOption(DELIMITERS_TAG);
        if (delimiters == null) {
            throw new IllegalArgumentException("A set of delimiters must be specified");
        }
        return delimiters.getBytes();
    }

    @Override
    public void init(FilterOptions options) {
        init(options, null);
    }

    @Override
    public void init(FilterOptions options, IteratorEnvironment iterEnv) {
        super.init(options, iterEnv);
        ruleApplied = false;
        String confPattern = options.getOption(AgeOffConfigParams.MATCHPATTERN);
        MERGE_MODE mode = getMergeMode(options);
        if (!Objects.equals(matchPattern, confPattern)) {
            this.scanTrie = new TokenTtlTrie.Builder(mode).setDelimiters(getDelimiters(options)).parse(confPattern).build();
            this.matchPattern = confPattern;
        }
    }

    @Override
    public void deepCopyInit(FilterOptions newOptions, AppliedRule parentCopy) {
        TokenizingFilterBase parent = (TokenizingFilterBase) parentCopy;
        this.matchPattern = parent.matchPattern;
        this.scanTrie = parent.scanTrie;
        super.deepCopyInit(newOptions, parentCopy);
    }

    @Override
    public boolean accept(AgeOffPeriod period, Key k, Value V) {
        Long calculatedTTL = scanTrie.scan(getKeyField(k, V));
        // no match found
        if (calculatedTTL == null) {
            ruleApplied = false;
            return true;
        }
        // cutoffTimestamp includes the default TTL
        long cutoffTimestamp = period.getCutOffMilliseconds();

        // If there is a TTL for this key:
        if (calculatedTTL > 0) {
            // cutoffTimestamp is currently offset by the default TTL. Start by undoing that default offset
            cutoffTimestamp += period.getTtl() * period.getTtlUnitsFactor();

            // Subtract the key's TTL from the cut-off timestamp
            cutoffTimestamp -= calculatedTTL;
        }
        ruleApplied = true;
        return CompositeTimestamp.getAgeOffDate(k.getTimestamp()) > cutoffTimestamp;
    }

    @Override
    public boolean isFilterRuleApplied() {
        return ruleApplied;
    }

    private MERGE_MODE getMergeMode(FilterOptions options) {
        String isMergeStr = options.getOption(AgeOffConfigParams.IS_MERGE);
        if (null == isMergeStr) {
            return MERGE_MODE.OFF;
        } else {
            boolean isMerge = Boolean.parseBoolean(isMergeStr);
            return isMerge ? MERGE_MODE.ON : MERGE_MODE.OFF;
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[size=" + (scanTrie == null ? null : scanTrie.size()) + "]";
    }
}
