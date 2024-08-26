package datawave.query.function;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import datawave.data.hash.UID;
import datawave.data.hash.UIDConstants;
import datawave.query.Constants;
import datawave.query.iterator.QueryOptions;
import datawave.query.util.Tuple3;

/**
 * Count and output the number of descendant events, immediate child events, or both based on the specified tuple elements, including:
 * <ul>
 * <li>Range - the shard range within which the caller used to scan for events, including the Key specified below
 * <li>Key - the top key of some event column, which should contain a valid row, column family, and column qualifier that respectively identify its shard ID,
 * data type, and UID
 * <li>List&lt;Entry&lt;Key,Value&gt;&gt; - a list of document attributes, especially those generated by KeyToDocumentData
 * </ul>
 * The output is returned as a DescendantCount containing a non-null list of Keys based on three possible types allowed via configuration, including:
 * CHILD_COUNT (the default), HAS_CHILDREN, and DESCENDANT_COUNT. If the counted value is 0 for CHILD_COUNT or DESCENDANT_COUNT, or false for HAS_CHILDREN,
 * neither Key will be returned. In other words, one or more Keys will be generated only if the DescendantCount returns a positive integer value for the
 * respective count.
 * <p>
 * Child counts are calculated in one of two ways depending on the configuration and information, if any, passed via the document attributes. The original, or
 * "traditional," implementation tries to derive counts by seeking through the event section of the shard table. The other implementation, having more options
 * at its disposal, tries to get counts by iterating through the field index (fi) section of the shard.
 * <p>
 * The fi-iterating implementation is attempted only if the initialized options, at a minimum, contain a field name that is also contained in the applied
 * document attributes. The field named by the options must correlate to entries in the fi with properly formatted column families, such as "fi\x00FIELD_NAME".
 * It must also have column qualifiers conforming to a pattern that the "source" iterator can scan, iterate, and recognize nested children. The default pattern
 * assumes the count can be determined from a column qualifier having an "-att-#" identifier, such as "00000000-1111-2222-aaaa-bbbbbbbbbbbb-att-9-att-2-att-1",
 * which would be a parent UUID containing at least 3 generations of descendants. Other patterns/delimiters can be configured, as applicable.
 * <p>
 * If fi-iterating is found to be too inefficient, two options can help speed up performance. First, adding a "childcount.index.skip.threshold" property tells
 * the DescendantCountFunction to skip over less important "grandchild" fi entries and seek directly to the next immediate child once an exceptionally large
 * number of iterations has occurred (e.g., 200). Although the DESCENDANT_COUNT key will contain an imprecise, smaller value appended a "+" character, the
 * CHILD_COUNT value should be calculated much faster for events having extremely deep trees of descendants. The second option speeds up performance even more
 * by turning off CHILD_COUNT and DESCENDANT_COUNT in favor of outputting only the HAS_CHILDREN key. When only this key is turned on, the
 * DescendantCountFunction stops iterating once it hits the first matching child.
 * <p>
 * The event-seeking legacy implementation is a fallback if the fi count can't be attempted at all, either by virtue of the configuration or document
 * attributes. Potentially less efficient that the fi-iterating implementation, this logic tries to count children by seeking to each parent event's child in
 * the shard. Because it seeks directly to 1st generation children, it does not have the option of outputting a DESCENDANT_COUNT key at all.
 *
 * @see KeyToDocumentData
 */
public class DescendantCountFunction implements SourcedFunction<Tuple3<Range,Key,List<Entry<Key,Value>>>,DescendantCount> {
    private static final Logger LOG = LogManager.getLogger(DescendantCountFunction.class);

    private static final String DEFAULT_DELIMITER_PATTERN = "-att-\\d*";

    private static final CountResult ZERO_COUNT = new CountResult(0);

    private Collection<ByteSequence> columnFamilies = KeyToDocumentData.columnFamilies;

    private boolean inclusive = KeyToDocumentData.inclusive;

    private Text indexCf;

    private ByteSequence indexComparator;

    private Pattern indexDelimiterPattern;

    private boolean outputChildCount = true;

    private boolean outputDescendantCount = false;

    private boolean outputHasChildren = false;

    private int skipThreshold = -1;

    private SortedKeyValueIterator<Key,Value> source;

    @Override
    public DescendantCount apply(final Tuple3<Range,Key,List<Entry<Key,Value>>> tuple) {
        // Extract the key and document attributes from which the child count will be determined
        final Range range;
        final Key key;
        final List<Entry<Key,Value>> documentAttributes;
        if (null != tuple) {
            range = tuple.first();
            key = tuple.second();
            documentAttributes = tuple.third();
        } else {
            range = null;
            key = null;
            documentAttributes = null;
        }

        // Get the descendant count(s), including applicable keys
        DescendantCount finalCount = ZERO_COUNT;
        if ((null != range) && (null != key) && (this.outputChildCount || this.outputHasChildren || this.outputDescendantCount)) {
            // Get the raw count
            CountResult count = null;
            try {
                count = this.countDescendants(range, key, documentAttributes);
            } catch (final IOException e) {
                final String message = "Unable to count child events";
                LOG.error(message, e);
            }

            // Create a list of key entries based on tallied counts, if any
            if (null != count) {
                // Create keys
                if (count.hasDescendants()) {
                    final List<Key> countKeys = new LinkedList<>();
                    if (this.outputHasChildren && count.hasDescendants()) {
                        final ColumnVisibility visibility = key.getColumnVisibilityParsed();
                        long timestamp = key.getTimestamp();
                        boolean hasChildren = count.hasDescendants();
                        final Key hasChildrenKey = new Key(key.getRow(), key.getColumnFamily(),
                                        new Text(QueryOptions.DEFAULT_HAS_CHILDREN_FIELDNAME + '\0' + Boolean.toString(hasChildren)), visibility, timestamp);
                        countKeys.add(hasChildrenKey);
                    }

                    if (this.outputChildCount && count.hasDescendants()) {
                        final ColumnVisibility visibility = key.getColumnVisibilityParsed();
                        long timestamp = key.getTimestamp();
                        int numChildren = count.getFirstGenerationCount();
                        final Key childCountKey = new Key(key.getRow(), key.getColumnFamily(),
                                        new Text(QueryOptions.DEFAULT_CHILD_COUNT_FIELDNAME + '\0' + Integer.toString(numChildren)), visibility, timestamp);
                        countKeys.add(childCountKey);
                    }

                    if (this.outputDescendantCount && (count.getAllGenerationsCount() > 0)) {
                        final ColumnVisibility visibility = key.getColumnVisibilityParsed();
                        long timestamp = key.getTimestamp();
                        int numDescendants = count.getAllGenerationsCount();
                        final Text text;
                        if (count.skippedDescendants()) {
                            text = new Text(QueryOptions.DEFAULT_DESCENDANT_COUNT_FIELDNAME + '\0' + Integer.toString(numDescendants - 1) + '+');
                        } else {
                            text = new Text(QueryOptions.DEFAULT_DESCENDANT_COUNT_FIELDNAME + '\0' + Integer.toString(numDescendants));
                        }

                        final Key descendantCountKey = new Key(key.getRow(), key.getColumnFamily(), text, visibility, timestamp);
                        countKeys.add(descendantCountKey);
                    }

                    if (!countKeys.isEmpty()) {
                        count.setKeys(countKeys);
                    }
                }

                // Re-assign the return value;
                finalCount = count;
            }
        }

        return finalCount;
    }

    protected CountResult countDescendants(final Range range, final Key key, final List<Entry<Key,Value>> documentAttributes) throws IOException {
        final CountResult childCount;

        if ((null != range) && (null != key)) {
            // Get the range and key information needed for child count determination
            final Text row = key.getRow();
            final String cf = key.getColumnFamily().toString();

            int index = cf.indexOf('\0');
            final String dataType = cf.substring(0, index);
            final String uidString = cf.substring(index + 1);
            final UID uid = UID.parse(uidString);

            // Validate marker
            Key marker = null;
            if (this.source.hasTop()) {
                marker = new Key(this.source.getTopKey());
                if (!range.contains(marker)) {
                    // TODO: Remove this when and if we figure out how this could happen
                    LOG.error("Source returned a key outside of the seek range: " + marker + " not in " + range + " using " + source.getClass());
                    marker = null;
                }
            }

            // Get the root value to use for field-indexed child counts, if applicable
            String fiRootValue = null;
            if ((null != this.indexComparator) && (null != documentAttributes)) {
                for (final Entry<Key,Value> entry : documentAttributes) {
                    final ByteSequence cqSequence = entry.getKey().getColumnQualifierData();
                    if (cqSequence.length() > (this.indexComparator.length())) {
                        final ByteSequence subsequence = cqSequence.subSequence(0, this.indexComparator.length());
                        if (this.indexComparator.compareTo(subsequence) == 0) {
                            fiRootValue = new String(cqSequence.subSequence(subsequence.length(), cqSequence.length()).toArray());
                            break;
                        }
                    }
                }
            }

            // If the root value is defined, determine the child count using the field index
            long startTime = (LOG.isTraceEnabled()) ? System.currentTimeMillis() : 0;
            if ((null != fiRootValue) && !fiRootValue.isEmpty()) {
                childCount = this.getCountByFieldIndexScan(range, row, dataType, uid, fiRootValue, marker);
            }
            // Otherwise, determine the child count using traditional event scanning
            else {
                childCount = new CountResult(this.getCountByEventScan(range, row, dataType, uid, marker));
            }

            // trace it
            if (LOG.isTraceEnabled()) {
                int numChildren = childCount.getFirstGenerationCount();
                int numDescendants = childCount.getAllGenerationsCount();
                long elapsedTime = System.currentTimeMillis() - startTime;
                String method = ((null != fiRootValue) && !fiRootValue.isEmpty()) ? "field index" : "traditional scanning";

                final String message = elapsedTime + " ms to count " + numChildren + " children and " + numDescendants + " descendants for " + row + '\\'
                                + dataType + '\\' + uid + " using " + method;
                if (childCount.skippedDescendants()) {
                    LOG.trace(message + " (some descendants were skipped)");
                } else {
                    LOG.trace(message);
                }
            }
        } else if (LOG.isTraceEnabled()) {
            final String message = "Unable to count child events";
            LOG.trace(message, new IllegalArgumentException("Key is null"));
            childCount = new CountResult(0, 0);
        } else {
            childCount = new CountResult(0, 0);
        }

        return childCount;
    }

    public Collection<ByteSequence> getColumnFamilies() {
        return columnFamilies;
    }

    private int getCountByEventScan(final Range seekRange, final Text row, final String dataType, final UID uid, Key marker) throws IOException {

        try {
            final Text colqT = new Text();
            final Text colfT = new Text();
            Key tk;

            // create the children range
            String baseUid = uid.getBaseUid();
            Key startKey = new Key(row, new Text(dataType + '\0' + baseUid + UIDConstants.DEFAULT_SEPARATOR));
            Key endKey = new Key(row, new Text(dataType + '\0' + baseUid + Constants.MAX_UNICODE_STRING));
            Range range = new Range(startKey, true, endKey, false);

            // seek too the new range
            Set<ByteSequence> emptyCfs = Collections.emptySet();
            this.source.seek(range, emptyCfs, false);

            // the list of children uids
            Set<String> uids = new HashSet<>();

            // now iterator through the keys and gather up the children uids
            String parentUid = uid.toString() + UIDConstants.DEFAULT_SEPARATOR;
            while (this.source.hasTop()) {
                tk = this.source.getTopKey();
                tk.getColumnFamily(colfT);
                tk.getColumnQualifier(colqT);

                String cf = this.source.getTopKey().getColumnFamily().toString();
                int index = cf.indexOf('\0');
                String uidString = cf.substring(index + 1);
                if (uidString.startsWith(parentUid)) {
                    int nextIndex = uidString.indexOf(UIDConstants.DEFAULT_SEPARATOR, parentUid.length());
                    // if no more separators past the parentUid, then this is an immediate child
                    if (nextIndex < 0) {
                        uids.add(uidString);
                        if (LOG.isTraceEnabled())
                            LOG.trace("Found a child UID: " + parentUid + " <= " + uidString);
                        startKey = new Key(row, new Text(dataType + '\0' + uidString + '0'));
                    }
                    // we found another separator, so this is a descendant....try to seek directly to the next immediate child
                    else {
                        if (LOG.isTraceEnabled())
                            LOG.trace("Passing by a descendant UID: " + parentUid + " <= " + uidString);
                        startKey = new Key(row, new Text(dataType + '\0' + uidString.substring(0, nextIndex) + '0'));
                    }
                } else {

                    if (LOG.isTraceEnabled())
                        LOG.trace("Passing by a non-child UID: " + parentUid + " <= " + uidString);
                    /**
                     * We've made a minor change, so that if our uid string is actually greater than the parent UID, we should break as we don't want to
                     * endlessly seek ( and ), we've moved beyond uidString. Otherwise, let's seek to the parent UID and get the children for it
                     */
                    if (uidString.compareTo(parentUid) > 0) {
                        break;
                    } else
                        startKey = new Key(row, new Text(dataType + '\0' + parentUid + UIDConstants.DEFAULT_SEPARATOR));
                }
                if (LOG.isTraceEnabled())
                    LOG.trace("Seeking to " + startKey);
                range = new Range(startKey, true, endKey, false);
                this.source.seek(range, emptyCfs, false);
            }

            // now reset the iterator back to where it was
            // if the iterator was done when we started, no need to seek anywhere as we are in that state again
            if (marker != null) {
                this.source.seek(new Range(marker, true, seekRange.getEndKey(), seekRange.isEndKeyInclusive()), columnFamilies, inclusive);
            }

            // and return the count
            return uids.size();
        } catch (IterationInterruptedException e) {
            // Re-throw iteration interrupted as-is since this is an expected event from
            // a client going away. Re-throwing as an IOException will cause the tserver
            // to catch the exception and log a warning. Re-throwing as-is will let the
            // tserver catch and ignore it as intended.
            throw e;
        } catch (Exception e) {
            throw new IOException("Error aggregating event", e);
        }
    }

    private CountResult getCountByFieldIndexScan(final Range seekRange, final Text row, final String dataType, final UID uid, final String fiRootValue,
                    Key marker) throws IOException {
        try {
            final Text colqT = new Text();
            Key tk;

            // Alternately assign marker
            if ((null == marker) && this.source.hasTop()) {
                marker = this.source.getTopKey();
            }

            // create the children range
            final Key startKey = new Key(row, this.indexCf, new Text(fiRootValue + '-'));
            final Key endKey = new Key(row, this.indexCf, new Text(fiRootValue + '.'));
            final Range range = new Range(startKey, true, endKey, false);

            // seek to the first possible child
            final Set<ByteSequence> emptyCfs = Collections.emptySet();
            this.source.seek(range, emptyCfs, false);

            // now iterate through the keys and count matching column qualifiers
            boolean countImmediateChildren = this.outputChildCount;
            boolean breakLoopIfAnyChildrenExist = (!this.outputChildCount && !this.outputDescendantCount);
            int numberOfImmediateChildren = 0;
            int numberOfDescendants = 0;
            int nonMatchingDescendants = 0;
            boolean skippedSomeDescendants = false;
            while (this.source.hasTop()) {
                // This is at least a child or grandchild, so increment the descendant count
                numberOfDescendants++;

                // Evaluate to see if the current key qualifies as an immediate child
                //
                // Note: Normally, the first item should normally always qualify. Exceptional cases might
                // occur if a child has been removed via age-off, purged, hasn't been ingested yet, etc.
                boolean getNext = true;
                if (countImmediateChildren) {
                    // Get the key
                    tk = this.source.getTopKey();
                    tk.getColumnQualifier(colqT);

                    final String cq = colqT.toString();
                    int nullIndex = cq.indexOf('\0');
                    if ((nullIndex > 0) && (nullIndex < cq.length())) {
                        final String childSuffix = cq.substring(fiRootValue.length(), nullIndex);
                        final Matcher matcher = this.indexDelimiterPattern.matcher(childSuffix);
                        if (matcher.matches()) {
                            numberOfImmediateChildren++;
                            nonMatchingDescendants = 0;
                        } else {
                            // If configured, past an exceptionally large number of irrelevant grandchildren.
                            // Although this would potentially throw off the descendant count, it may be necessary
                            // if a given event has thousands (or millions) of grandchildren and we're mainly interested
                            // in the number of 1st generation children.
                            nonMatchingDescendants++;
                            if ((this.skipThreshold > 0) && (nonMatchingDescendants >= this.skipThreshold)) {
                                if (this.skipExcessiveNumberOfDescendants(childSuffix, matcher, row, fiRootValue, endKey)) {
                                    getNext = false;
                                    skippedSomeDescendants = true;
                                    nonMatchingDescendants = 0;
                                }
                            }
                        }
                    }
                }

                // Break the loop immediately if we're only interested in hasChildren == true
                if (breakLoopIfAnyChildrenExist) {
                    break;
                }

                // Advance to the next item
                if (getNext) {
                    this.source.next();
                }
            }

            // now reset the iterator back to where it was
            // if the iterator was done when we started, no need to seek anywhere as we are in that state again
            if (marker != null) {
                this.source.seek(new Range(marker, true, seekRange.getEndKey(), seekRange.isEndKeyInclusive()), columnFamilies, inclusive);
            }

            // and return the count
            final CountResult result = new CountResult(numberOfImmediateChildren, numberOfDescendants);
            result.setSkippedDescendants(skippedSomeDescendants);
            return result;
        } catch (IterationInterruptedException e) {
            // Re-throw iteration interrupted as-is since this is an expected event from
            // a client going away. Re-throwing as an IOException will cause the tserver
            // to catch the exception and log a warning. Re-throwing as-is will let the
            // tserver catch and ignore it as intended.
            throw e;
        } catch (Exception e) {
            throw new IOException("Error aggregating event", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K extends WritableComparable<?>,V extends Writable> void init(final SortedKeyValueIterator<K,V> source, final Map<String,String> options,
                    final IteratorEnvironment env) throws IOException {
        if (null != source) {
            // If configured, set the fieldname for the fi lookup implementation
            if (null != options) {
                if (options.containsKey(QueryOptions.CHILD_COUNT_INDEX_FIELDNAME)) {
                    final String fieldname = options.get(QueryOptions.CHILD_COUNT_INDEX_FIELDNAME);
                    if (!fieldname.isEmpty()) {
                        // Assign the reusable Text and ByteSequences
                        this.indexCf = new Text(Constants.FI_PREFIX_WITH_NULL + fieldname);
                        this.indexComparator = new ArrayByteSequence((fieldname + '\0').getBytes());

                        // Assign the child delimiter pattern based on a configured pattern
                        if (options.containsKey(QueryOptions.CHILD_COUNT_INDEX_PATTERN)) {
                            final String pattern = options.get(QueryOptions.CHILD_COUNT_INDEX_PATTERN);
                            if (!pattern.isEmpty()) {
                                this.indexDelimiterPattern = Pattern.compile(pattern);
                            }
                        }

                        // If still undefined, assign the child delimiter pattern based on a configured delimiter appended with "\d*"
                        if ((null == this.indexDelimiterPattern) && options.containsKey(QueryOptions.CHILD_COUNT_INDEX_DELIMITER)) {
                            final String delimiter = options.get(QueryOptions.CHILD_COUNT_INDEX_DELIMITER);
                            if (!delimiter.isEmpty()) {
                                this.indexDelimiterPattern = Pattern.compile(delimiter + "\\d*");
                            }
                        }

                        // If still undefined, assign the child delimiter pattern based on the default value
                        if (null == this.indexDelimiterPattern) {
                            this.indexDelimiterPattern = Pattern.compile(DEFAULT_DELIMITER_PATTERN);
                        }

                        // Determine whether or not to output the DESCENDANT_COUNT field
                        // Note: A complete DESCENDANT_COUNT is currently available only using field-index scanning
                        if (options.containsKey(QueryOptions.CHILD_COUNT_OUTPUT_ALL_DESCDENDANTS)) {
                            final String value = options.get(QueryOptions.CHILD_COUNT_OUTPUT_ALL_DESCDENDANTS);
                            this.outputDescendantCount = Boolean.valueOf(value);
                        }

                        // Determine whether or not to specify a "skip threshold" for events with excessively deep descendant counts
                        if (options.containsKey(QueryOptions.CHILD_COUNT_INDEX_SKIP_THRESHOLD)) {
                            final String value = options.get(QueryOptions.CHILD_COUNT_INDEX_SKIP_THRESHOLD);
                            try {
                                int threshold = Integer.parseInt(value);
                                this.skipThreshold = (threshold > 1) ? threshold : this.skipThreshold;
                            } catch (NumberFormatException e) {
                                final String message = "Unable to configure " + QueryOptions.CHILD_COUNT_INDEX_SKIP_THRESHOLD;
                                LOG.trace(message, e);
                            }
                        }
                    }
                }

                // Determine whether or not to output the CHILD_COUNT field
                if (options.containsKey(QueryOptions.CHILD_COUNT_OUTPUT_IMMEDIATE_CHILDREN)) {
                    final String value = options.get(QueryOptions.CHILD_COUNT_OUTPUT_IMMEDIATE_CHILDREN);
                    this.outputChildCount = Boolean.valueOf(value);
                }

                // Determine whether or not to output the HAS_CHILDREN field
                if (options.containsKey(QueryOptions.CHILD_COUNT_OUTPUT_HASCHILDREN)) {
                    final String value = options.get(QueryOptions.CHILD_COUNT_OUTPUT_HASCHILDREN);
                    this.outputHasChildren = Boolean.valueOf(value);
                }
            }

            this.source = (SortedKeyValueIterator<Key,Value>) source;
        } else {
            final String message = "Unable to initialize " + this.getClass().getSimpleName();
            LOG.error(message, new IllegalArgumentException("Iterator source is null"));
        }
    }

    public boolean isInclusive() {
        return inclusive;
    }

    public void setColumnFamilies(final Collection<ByteSequence> columnFamilies) {
        if (null != columnFamilies) {
            this.columnFamilies = columnFamilies;
        } else {
            this.columnFamilies = Collections.emptyList();
        }
    }

    public void setInclusive(boolean inclusive) {
        this.inclusive = inclusive;
    }

    private boolean skipExcessiveNumberOfDescendants(final String childSuffix, final Matcher matcher, final Text row, final String fiRootValue,
                    final Key endKey) throws IOException {
        boolean skipped;
        if (matcher.find(0) && (matcher.start() < childSuffix.length())) {
            // Get the base matching child suffix
            final String baseMatch = childSuffix.substring(matcher.start(), matcher.end());

            // create the skipping range
            final Key skipStartKey = new Key(row, this.indexCf, new Text(fiRootValue + baseMatch + '0'));
            final Range skipRange = new Range(skipStartKey, true, endKey, false);

            // seek to the next first-generation child, if one exists
            final Set<ByteSequence> emptyCfs = Collections.emptySet();
            this.source.seek(skipRange, emptyCfs, false);

            // Assign the return value
            skipped = true;
        } else {
            skipped = false;
        }

        return skipped;
    }

    /**
     * Provides the tallies of descendant events
     */
    protected static class CountResult implements DescendantCount {
        private List<Key> keys;
        private final int numberOfChildren;
        private final int numberOfDescendants;
        private boolean skippedDescendants;

        public CountResult(int numberOfChildren) {
            this(numberOfChildren, -1);
        }

        public CountResult(int numberOfChildren, int numberOfDescendants) {
            this.keys = Collections.emptyList();
            this.numberOfChildren = numberOfChildren;
            this.numberOfDescendants = numberOfDescendants;
        }

        @Override
        public int getAllGenerationsCount() {
            return numberOfDescendants;
        }

        @Override
        public int getFirstGenerationCount() {
            return numberOfChildren;
        }

        @Override
        public List<Key> getKeys() {
            return Collections.unmodifiableList(this.keys);
        }

        @Override
        public boolean hasDescendants() {
            return ((numberOfChildren > 0) || (numberOfDescendants > 0));
        }

        public void setKeys(final List<Key> keys) {
            if (null != keys) {
                this.keys = keys;
            } else {
                this.keys = Collections.emptyList();
            }
        }

        public void setSkippedDescendants(boolean skippedDescendants) {
            this.skippedDescendants = skippedDescendants;
        }

        public boolean skippedDescendants() {
            return this.skippedDescendants;
        }
    }
}
