package datawave.query.function;

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

import static datawave.query.Constants.EMPTY_VALUE;

public class KeyToDocumentData implements Function<Entry<Key,Document>,Entry<DocumentData,Document>> {
    
    private static final Logger log = Logger.getLogger(KeyToDocumentData.class);
    
    protected SortedKeyValueIterator<Key,Value> source;
    
    private static final ByteSequence EMPTY_BYTE_SEQUENCE = new ArrayByteSequence(new byte[] {});
    
    protected static final Collection<ByteSequence> columnFamilies = Lists.<ByteSequence> newArrayList(new ArrayByteSequence("tf"), new ArrayByteSequence("d"));
    protected static final boolean inclusive = false;
    
    private final DescendantCountFunction countFunction;
    
    protected Equality equality;
    
    private EventDataQueryFilter filter;
    
    private boolean includeParent = false;
    
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
     */
    public List<Entry<Key,Value>> appendHierarchyFields(final List<Entry<Key,Value>> documentAttributes, final Range range, final Key key) throws IOException {
        return appendHierarchyFields(documentAttributes, key, source, range, countFunction, includeParent);
    }
    
    @Override
    public Entry<DocumentData,Document> apply(Entry<Key,Document> from) {
        // We want to ensure that we have a non-empty colqual
        if (null == from || null == from.getKey() || null == from.getValue())
            return null;
        Range keyRange = getKeyRange(from);
        
        try {
            
            source.seek(keyRange, columnFamilies, inclusive);
            
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
            
            return Maps.immutableEntry(new DocumentData(from.getKey(), docKeys, attrs, false), from.getValue());
        } catch (IOException e) {
            log.error("Unable to collection document attributes for evaluation: " + keyRange, e);
            QueryException qe = new QueryException(DatawaveErrorCode.DOCUMENT_EVALUATION_ERROR, e);
            throw new DatawaveFatalQueryException(qe);
        }
        
    }
    
    /**
     * Given a Key pointing to the start of an document to aggregate, construct a list of attributes, adding the names of the attributes to the specified set of
     * "docKeys".
     * 
     * @param documentStartKey
     *            A Key of the form "bucket type\x00uid: "
     * @param docKeys
     *            the names of generated generated attributes
     * @param keyRange
     *            the Range used to initialize source with seek()
     * @return
     */
    public List<Entry<Key,Value>> collectDocumentAttributes(final Key documentStartKey, final Set<Key> docKeys, final Range keyRange) throws IOException {
        return collectAttributesForDocumentKey(documentStartKey, source, equality, filter, docKeys, keyRange);
    }
    
    /**
     * Given a Key pointing to the start of an document to aggregate, construct a Range that should encapsulate the "document" to be aggregated together. Also
     * checks to see if data was found for the constructed Range before returning.
     * 
     * @param documentStartKey
     *            A Key of the form "bucket type\x00uid: "
     * @param keyRange
     *            the Range used to initialize source with seek()
     * @return the attributes
     */
    private static List<Entry<Key,Value>> collectAttributesForDocumentKey(Key documentStartKey, SortedKeyValueIterator<Key,Value> source, Equality equality,
                    EventDataQueryFilter filter, Set<Key> docKeys, Range keyRange) throws IOException {
        
        // setup the document key we are filtering for on the EventDataQueryFilter
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
                    } else if (filter != null) {
                        Key limitKey = filter.transform(docAttrKey.get());
                        if (limitKey != null) {
                            documentAttributes.add(Maps.immutableEntry(limitKey, EMPTY_VALUE));
                        }
                        // request a seek range from the filter
                        Range seekRange = filter.getSeekRange(docAttrKey.get(), keyRange.getEndKey(), keyRange.isEndKeyInclusive());
                        if (seekRange != null) {
                            source.seek(seekRange, columnFamilies, inclusive);
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
        final ByteSequence row = key.getRowData(), cf = key.getColumnFamilyData(), cv = key.getColumnVisibilityData();
        return new Key(row.getBackingArray(), row.offset(), row.length(), cf.getBackingArray(), cf.offset(), cf.length(),
                        EMPTY_BYTE_SEQUENCE.getBackingArray(), EMPTY_BYTE_SEQUENCE.offset(), EMPTY_BYTE_SEQUENCE.length(), cv.getBackingArray(), cv.offset(),
                        cv.length(), key.getTimestamp());
    }
    
    private static List<Entry<Key,Value>> appendHierarchyFields(List<Entry<Key,Value>> documentAttributes, Key key, SortedKeyValueIterator<Key,Value> source,
                    Range seekRange, DescendantCountFunction function, boolean includeParent) throws IOException {
        if ((null != function) || includeParent) {
            
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
            if (includeParent) {
                if (uid.getExtra() != null && !uid.getExtra().equals("")) {
                    String parentUid = uidString.substring(0, uidString.lastIndexOf(UIDConstants.DEFAULT_SEPARATOR));
                    Key parentUidKey = new Key(key.getRow(), key.getColumnFamily(), new Text(QueryOptions.DEFAULT_PARENT_UID_FIELDNAME + '\0' + parentUid),
                                    new ColumnVisibility(visibility), minTimestamp);
                    documentAttributes.add(Maps.immutableEntry(parentUidKey, new Value()));
                }
            }
        }
        
        return documentAttributes;
    }
    
    private static int applyDescendantCounts(final DescendantCountFunction function, final Range range, final Key key,
                    final List<Entry<Key,Value>> documentAttributes, final String visibility, long timestamp) {
        int basicChildCount = 0;
        if ((null != function) && (null != key)) {
            // Count the descendants, generating keys based on query options and
            // document attributes
            final Tuple3<Range,Key,List<Entry<Key,Value>>> tuple = new Tuple3<>(range, key, documentAttributes);
            final DescendantCount count = function.apply(tuple);
            
            // No need to do any more work if there aren't any descendants
            if ((null != count) && count.hasDescendants()) {
                // Extract the basic, first-generation count
                basicChildCount = count.getFirstGenerationCount();
                
                // Get any generated keys, apply any specified visibility, and
                // add to the document attributes
                final List<Key> keys = count.getKeys();
                if ((null != documentAttributes) && !documentAttributes.isEmpty() && !keys.isEmpty()) {
                    // Create a Text for the Keys' visibility
                    Text appliedVis;
                    if ((null != visibility) && !visibility.isEmpty()) {
                        appliedVis = new Text(visibility);
                    } else {
                        appliedVis = new Text();
                    }
                    
                    // Conditionally adjust visibility and timestamp
                    for (final Key childCountKey : keys) {
                        final Text appliedRow = childCountKey.getRow();
                        final Text appliedCf = childCountKey.getColumnFamily();
                        final Text appliedCq = childCountKey.getColumnQualifier();
                        if ((null == visibility) || visibility.isEmpty()) {
                            childCountKey.getColumnVisibility(appliedVis);
                        }
                        if (!(timestamp > 0)) {
                            timestamp = childCountKey.getTimestamp();
                        }
                        
                        final Key appliedKey = new Key(appliedRow, appliedCf, appliedCq, appliedVis, timestamp);
                        documentAttributes.add(Maps.immutableEntry(appliedKey, new Value()));
                    }
                }
            }
        }
        
        return basicChildCount;
    }
    
    /**
     * Define the start key given the from condition.
     *
     * @param from
     * @return
     */
    protected Key getStartKey(Map.Entry<Key,Document> from) {
        return new Key(from.getKey().getRow(), from.getKey().getColumnFamily());
    }
    
    /**
     * Define the end key given the from condition.
     *
     * @param from
     * @return
     */
    protected Key getStopKey(Map.Entry<Key,Document> from) {
        return filter == null ? from.getKey().followingKey(PartialKey.ROW_COLFAM) : filter.getStopKey(from.getKey());
    }
    
    /**
     * Get the key range that covers the complete document specified by the input key range
     *
     * @param from
     * @return
     */
    protected Range getKeyRange(Map.Entry<Key,Document> from) {
        if (filter != null) {
            return filter.getKeyRange(from);
        }
        return new Range(getStartKey(from), true, getStopKey(from), false);
    }
}
