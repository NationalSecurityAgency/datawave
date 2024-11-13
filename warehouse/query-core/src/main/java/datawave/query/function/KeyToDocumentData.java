package datawave.query.function;

import static datawave.query.Constants.EMPTY_VALUE;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import datawave.data.hash.UID;
import datawave.data.hash.UIDConstants;
import datawave.query.attributes.Document;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.aggregation.DocumentData;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.Tuple3;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * This class aggregates all event data for a given 'document key'.
 */
public class KeyToDocumentData implements Function<Entry<Key,Document>,Entry<DocumentData,Document>> {

    private static final Logger log = Logger.getLogger(KeyToDocumentData.class);

    protected SortedKeyValueIterator<Key,Value> source;

    private static final ByteSequence EMPTY_BYTE_SEQUENCE = new ArrayByteSequence(new byte[] {});

    protected final Collection<ByteSequence> columnFamilies = Lists.newArrayList(new ArrayByteSequence("tf"), new ArrayByteSequence("d"));

    private final DescendantCountFunction countFunction;

    protected Equality equality;

    private final EventDataQueryFilter filter;

    // default implementation
    protected RangeProvider rangeProvider = new DocumentRangeProvider();

    private boolean includeParent = false;

    // track aggregation threshold and the time
    private long aggregationStart;
    private long aggregationStop;
    private int aggregationThreshold;

    public KeyToDocumentData(SortedKeyValueIterator<Key,Value> source) {
        this(source, new PrefixEquality(PartialKey.ROW_COLFAM), false, false);
    }

    public KeyToDocumentData(SortedKeyValueIterator<Key,Value> source, Equality equality, boolean includeChildCount, boolean includeParent) {
        this(source, equality, null, includeChildCount, includeParent);
    }

    public KeyToDocumentData(SortedKeyValueIterator<Key,Value> source, Equality equality, EventDataQueryFilter filter, boolean includeChildCount,
                    boolean includeParent) {
        this(source, null, null, equality, filter, includeChildCount, includeParent);
    }

    public KeyToDocumentData(final SortedKeyValueIterator<Key,Value> source, final IteratorEnvironment env, final Map<String,String> options,
                    final Equality equality, final EventDataQueryFilter filter, boolean includeChildCount, boolean includeParent) {
        // Initialize primary instance variables
        this.source = source;
        this.equality = equality;
        this.filter = filter;
        this.includeParent = includeParent;

        // Conditionally create and initialize the child count function
        if (includeChildCount) {
            this.countFunction = new DescendantCountFunction();
            try {
                this.countFunction.init(source, options, env);
            } catch (IOException e) {
                final String message = "Unable to initialize child count function for " + this.getClass().getSimpleName();
                log.trace(message, e);
            }
        }
        // Child counting is desired, so a function is not needed
        else {
            this.countFunction = null;
        }
    }

    /**
     * Builder-style method for setting a non-default implementation of a {@link RangeProvider}
     *
     * @param rangeProvider
     *            a {@link RangeProvider}
     * @return this object
     */
    public KeyToDocumentData withRangeProvider(RangeProvider rangeProvider) {
        this.rangeProvider = rangeProvider;
        return this;
    }

    /**
     * Builder-style method for setting the aggregation threshold
     *
     * @param aggregationThreshold
     *            a time in milliseconds
     * @return this object
     */
    public KeyToDocumentData withAggregationThreshold(int aggregationThreshold) {
        this.aggregationThreshold = aggregationThreshold;
        return this;
    }

    /**
     * Append hierarchy fields, including parent and descendant counts, based on the specified range and key
     *
     * @param documentAttributes
     *            the attributes to update (and output)
     * @param range
     *            the boundary within which to compute hierarchical field
     * @param key
     *            the reference event for generating hierarchical fields
     * @return the modified list of document attributes
     * @throws IOException
     *             for issues with read/write
     */
    public List<Entry<Key,Value>> appendHierarchyFields(final List<Entry<Key,Value>> documentAttributes, final Range range, final Key key) throws IOException {
        return appendHierarchyFields(documentAttributes, key, range, countFunction, includeParent);
    }

    @Override
    public Entry<DocumentData,Document> apply(Entry<Key,Document> from) {
        // We want to ensure that we have a non-empty column qualifier
        if (null == from || null == from.getKey() || null == from.getValue()) {
            return null;
        }

        Range keyRange = rangeProvider.getRange(from.getKey());

        try {
            logStart();
            source.seek(keyRange, columnFamilies, false);

            if (log.isDebugEnabled())
                log.debug(source.hasTop() + " Key range is " + keyRange);

            final List<Entry<Key,Value>> attrs; // Assign only once for
            // efficiency
            final Set<Key> docKeys = new HashSet<>();
            if (source.hasTop()) {
                attrs = this.collectDocumentAttributes(from.getKey(), docKeys, keyRange);
                this.appendHierarchyFields(attrs, keyRange, from.getKey());
            } else {
                attrs = Collections.emptyList();
            }

            logStop(keyRange.getStartKey());
            return Maps.immutableEntry(new DocumentData(from.getKey(), docKeys, attrs, false), from.getValue());
        } catch (IOException e) {
            log.error("Unable to collection document attributes for evaluation: " + keyRange, e);
            QueryException qe = new QueryException(DatawaveErrorCode.DOCUMENT_EVALUATION_ERROR, e);
            throw new DatawaveFatalQueryException(qe);
        }
    }

    /**
     * Given a Key pointing to the start of a document to aggregate, construct a list of attributes, adding the names of the attributes to the specified set of
     * "docKeys".
     *
     * @param documentStartKey
     *            A Key of the form "bucket type\x00uid: "
     * @param docKeys
     *            the names of generated attributes
     * @param keyRange
     *            the Range used to initialize source with seek()
     * @return list of entries
     * @throws IOException
     *             for issues with read/write
     */
    public List<Entry<Key,Value>> collectDocumentAttributes(final Key documentStartKey, final Set<Key> docKeys, final Range keyRange) throws IOException {
        // set up the document key we are filtering for on the EventDataQueryFilter
        if (filter != null) {
            filter.startNewDocument(documentStartKey);
        }

        final List<Entry<Key,Value>> documentAttributes;
        if (null == documentStartKey) {
            documentAttributes = Collections.emptyList();
        } else {
            documentAttributes = new ArrayList<>(256);
            WeakReference<Key> docAttrKey = new WeakReference<>(source.getTopKey());

            while (docAttrKey != null) {
                boolean seeked = false;
                if (equality.partOf(documentStartKey, docAttrKey.get())) {
                    if (filter == null || filter.keep(docAttrKey.get())) {
                        docKeys.add(getDocKey(docAttrKey.get()));
                    }

                    if (filter == null || filter.apply(Maps.immutableEntry(docAttrKey.get(), StringUtils.EMPTY))) {
                        documentAttributes.add(Maps.immutableEntry(docAttrKey.get(), source.getTopValue()));
                    } else {
                        Key limitKey = filter.transform(docAttrKey.get());
                        if (limitKey != null) {
                            documentAttributes.add(Maps.immutableEntry(limitKey, EMPTY_VALUE));
                        }
                        // request a seek range from the filter
                        Range seekRange = filter.getSeekRange(docAttrKey.get(), keyRange.getEndKey(), keyRange.isEndKeyInclusive());
                        if (seekRange != null) {
                            source.seek(seekRange, columnFamilies, false);
                            seeked = true;
                        }
                    }
                }

                // only call next if this wasn't a fresh seek()
                if (!seeked) {
                    source.next();
                }

                if (source.hasTop()) {
                    docAttrKey = new WeakReference<>(source.getTopKey());
                } else {
                    docAttrKey = null;
                }

            }
        }
        if (log.isTraceEnabled()) {
            log.trace("Document attributes: " + documentAttributes);
        }
        return documentAttributes;
    }

    // map the key to the dockey (only shard, datatype, uid)
    public static Key getDocKey(Key key) {
        final ByteSequence row = key.getRowData();
        final ByteSequence cf = key.getColumnFamilyData();
        final ByteSequence cv = key.getColumnVisibilityData();
        return new Key(row.getBackingArray(), row.offset(), row.length(), cf.getBackingArray(), cf.offset(), cf.length(), EMPTY_BYTE_SEQUENCE.getBackingArray(),
                        EMPTY_BYTE_SEQUENCE.offset(), EMPTY_BYTE_SEQUENCE.length(), cv.getBackingArray(), cv.offset(), cv.length(), key.getTimestamp());
    }

    private static List<Entry<Key,Value>> appendHierarchyFields(List<Entry<Key,Value>> documentAttributes, Key key, Range seekRange,
                    DescendantCountFunction function, boolean includeParent) {
        if (function != null || includeParent) {

            // get the minimal timestamp and majority visibility from the
            // attributes
            Map<String,MutableInt> visibilityCounts = new HashMap<>();
            long minTimestamp = Long.MAX_VALUE;
            for (Entry<Key,Value> attr : documentAttributes) {
                Key attrKey = attr.getKey();
                minTimestamp = Math.min(attrKey.getTimestamp(), minTimestamp);
                String attrVis = attrKey.getColumnVisibility().toString();
                MutableInt count = visibilityCounts.get(attrVis);
                if (count == null) {
                    count = new MutableInt(1);
                    visibilityCounts.put(attrVis, count);
                } else {
                    count.increment();
                }
            }
            String visibility = key.getColumnVisibility().toString();
            int count = 0;
            for (Entry<String,MutableInt> visibilityCount : visibilityCounts.entrySet()) {
                if (visibilityCount.getValue().intValue() > count) {
                    visibility = visibilityCount.getKey();
                    count = visibilityCount.getValue().intValue();
                }
            }

            // parse out the datatype and uid
            String cf = key.getColumnFamily().toString();
            int index = cf.indexOf('\0');
            String uidString = cf.substring(index + 1);
            UID uid = UID.parse(uidString);

            // Conditionally include the count(s) of descendants
            applyDescendantCounts(function, seekRange, key, documentAttributes, visibility, minTimestamp);

            // include the parent uid
            if (includeParent && uid.getExtra() != null && !uid.getExtra().isEmpty()) {
                String parentUid = uidString.substring(0, uidString.lastIndexOf(UIDConstants.DEFAULT_SEPARATOR));
                Key parentUidKey = new Key(key.getRow(), key.getColumnFamily(), new Text(QueryOptions.DEFAULT_PARENT_UID_FIELDNAME + '\0' + parentUid),
                                new ColumnVisibility(visibility), minTimestamp);
                documentAttributes.add(Maps.immutableEntry(parentUidKey, EMPTY_VALUE));
            }
        }

        return documentAttributes;
    }

    private static int applyDescendantCounts(final DescendantCountFunction function, final Range range, final Key key,
                    final List<Entry<Key,Value>> documentAttributes, final String visibility, long timestamp) {
        int basicChildCount = 0;
        if (null == function || null == key) {
            return basicChildCount;
        }

        // Count the descendants, generating keys based on query options and
        // document attributes
        final Tuple3<Range,Key,List<Entry<Key,Value>>> tuple = new Tuple3<>(range, key, documentAttributes);
        final DescendantCount count = function.apply(tuple);

        // No need to do any more work if there aren't any descendants
        if (count != null && count.hasDescendants()) {
            // Extract the basic, first-generation count
            basicChildCount = count.getFirstGenerationCount();

            // Get any generated keys, apply any specified visibility, and
            // add to the document attributes
            final List<Key> keys = count.getKeys();
            if (documentAttributes != null && !documentAttributes.isEmpty() && !keys.isEmpty()) {
                // Create a Text for the Keys' visibility
                Text appliedVis;
                if (visibility != null && !visibility.isEmpty()) {
                    appliedVis = new Text(visibility);
                } else {
                    appliedVis = new Text();
                }

                // Conditionally adjust visibility and timestamp
                for (final Key childCountKey : keys) {
                    final Text appliedRow = childCountKey.getRow();
                    final Text appliedCf = childCountKey.getColumnFamily();
                    final Text appliedCq = childCountKey.getColumnQualifier();
                    if (visibility == null || visibility.isEmpty()) {
                        childCountKey.getColumnVisibility(appliedVis);
                    }
                    if (timestamp <= 0) {
                        timestamp = childCountKey.getTimestamp();
                    }

                    final Key appliedKey = new Key(appliedRow, appliedCf, appliedCq, appliedVis, timestamp);
                    documentAttributes.add(Maps.immutableEntry(appliedKey, EMPTY_VALUE));
                }
            }
        }

        return basicChildCount;
    }

    /**
     * Mark the aggregation start time.
     */
    private void logStart() {
        if (aggregationThreshold == -1) {
            return;
        }
        aggregationStart = System.currentTimeMillis();
    }

    /**
     * Mark the aggregation stop time.
     * <p>
     * Logs the total aggregation time if the {@link #aggregationThreshold} is exceeded.
     *
     * @param k
     *            the aggregation range's start key
     */
    private void logStop(Key k) {
        if (aggregationThreshold == -1) {
            return;
        }

        aggregationStop = System.currentTimeMillis();

        if (aggregationThreshold > 0 && (aggregationStop - aggregationStart) > aggregationThreshold) {
            log.warn("time to aggregate document " + k.getRow() + " " + k.getColumnFamily().toString().replace("\0", "0x00") + " was "
                            + (aggregationStop - aggregationStart));
        }
    }
}
