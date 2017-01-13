package nsa.datawave.core.iterators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import nsa.datawave.core.iterators.uid.ShardUidMappingIterator;
import nsa.datawave.core.iterators.uid.UidMapper;
import nsa.datawave.core.iterators.uid.UidMappingIterator;
import nsa.datawave.data.hash.UID;
import nsa.datawave.data.hash.UIDConstants;
import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.marking.MarkingFunctionsFactory;
import nsa.datawave.query.QueryParameters;
import nsa.datawave.query.config.GenericShardQueryConfiguration;
import nsa.datawave.query.filter.FilteringMaster;
import nsa.datawave.query.parser.EventFields;
import nsa.datawave.query.parser.EventFields.FieldValue;
import nsa.datawave.query.parser.QueryEvaluator;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.iterator.QueryOptions;
import nsa.datawave.query.util.StringTuple;
import nsa.datawave.util.StringUtils;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 *
 * This iterator aggregates rows together using the specified key comparator. Subclasses will provide their own implementation of fillMap which will fill the
 * supplied EventFields object with field names (key) and field values (value). After all fields have been put into the aggregated object (by aggregating all
 * columns with the same key), the EventFields object will be compared against the supplied expression. If the expression returns true, then the return key and
 * return value can be retrieved via getTopKey() and getTopValue().
 *
 * By default this iterator will return all Events in the shard. If the START_DATE and END_DATE are specified, then this iterator will evaluate the timestamp of
 * the key against the start and end dates. If the event date is not within the range of start to end, then it is skipped.
 *
 * This iterator will return up the stack an EventFields object serialized using Kryo in the cell Value. If the RETURN_FIELDS parameter is set, then all
 * non-matching fields will be removed from the EventFields object before it is serialized and returned.
 *
 */
public class EvaluatingIterator extends ShardUidMappingIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    
    private static Logger log = Logger.getLogger(EvaluatingIterator.class);
    public static final String QUERY_OPTION = "expr";
    public static final String QUERY_EVALUATOR = "query.evaluator";
    
    public static final String NORMALIZER_LIST = "normalizer.list";
    
    public static final String NULL_BYTE_STRING = "\u0000";
    
    public static final String EVENT_DATATYPE = QueryOptions.DEFAULT_DATATYPE_FIELDNAME;
    
    public static final String FI_PREFIX = "fi" + NULL_BYTE_STRING;
    public static final String MAX_SKIPS_OPT = "shard.evaluating.iterator.max.skips";
    
    private static final Text NEXT_FI_PREFIX = new Text("fi\u0001");
    
    protected static final MarkingFunctions markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
    
    private static final TreeSet<String> CONTAINS_ALL = new TreeSet<String>() {
        private static final long serialVersionUID = 1L;
        
        @Override
        public boolean contains(Object o) {
            return true;
        }
    };
    
    protected SortedKeyValueIterator<Key,Value> iterator;
    protected Key currentKey = new Key();
    protected Key returnKey;
    protected Value returnValue;
    protected String expression;
    protected QueryEvaluator evaluator;
    private EventFields event = null;
    protected Kryo kryo = new Kryo();
    protected Range seekRange = null;
    protected LongRange dateRange = null;
    protected Set<String> unevaluatedFields = new HashSet<>();
    protected FilteringMaster filters = null;
    protected Set<String> returnFields = null;
    protected Set<String> blacklistedFields = null;
    protected NavigableSet<String> requiredFields = null;
    protected String normalizerListString = "";
    protected boolean includeDataTypeAsEventField = false;
    protected boolean includeGroupingContext = false;
    protected boolean includeChildCount = false;
    protected boolean includeParent = false;
    protected UidMapper returnUidMapper = null;
    
    protected Collection<ByteSequence> seekingCFs;
    protected boolean seekingCFsInclusive;
    
    protected Set<String> colFamsToIgnore;
    protected Multimap<String,String> validUnevaluatedFields;
    
    protected Set<ColumnVisibility> columnVisibilities = Sets.newHashSet();
    
    protected int maxSkips = 5;
    
    protected LRUMap cvCache = new LRUMap();
    
    protected Set<String> alreadyReturned = new HashSet<>();
    protected String alreadyReturnedBaseUid = null;
    
    protected EvaluatingIterator(EvaluatingIterator other, IteratorEnvironment env) {
        super(other, env);
        this.iterator = other.iterator.deepCopy(env);
        this.event = new EventFields();
        this.event.putAll(other.event);
        this.filters = other.filters;
        this.dateRange = other.dateRange;
        this.expression = other.expression;
        this.unevaluatedFields = other.unevaluatedFields;
        this.blacklistedFields = other.blacklistedFields;
        this.requiredFields = other.requiredFields;
        this.normalizerListString = other.normalizerListString;
        this.includeDataTypeAsEventField = other.includeDataTypeAsEventField;
        this.seekRange = other.seekRange;
        this.returnFields = other.returnFields;
        this.colFamsToIgnore = other.colFamsToIgnore;
        this.includeGroupingContext = other.includeGroupingContext;
        this.includeChildCount = other.includeChildCount;
        this.includeParent = other.includeParent;
        this.returnUidMapper = other.returnUidMapper;
        this.alreadyReturned.addAll(other.alreadyReturned);
        this.alreadyReturnedBaseUid = other.alreadyReturnedBaseUid;
        
        Class<? extends QueryEvaluator> evaluatorClass = other.evaluator.getClass();
        try {
            this.evaluator = evaluatorClass.newInstance();
            this.evaluator.setQuery(this.expression);
            
            if (!this.normalizerListString.isEmpty()) {
                this.evaluator.configureNormalizers(normalizerListString);
            }
        } catch (ParseException e) {
            throw new RuntimeException("Failed to parse query: " + this.expression, e);
        } catch (InstantiationException e) {
            throw new RuntimeException("Failed to instantiate query evaluator " + evaluatorClass, e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to access query evaluator class " + evaluatorClass, e);
        }
        
        setUpRequiredFields();
    }
    
    public EvaluatingIterator() {
        super();
    }
    
    /**
     * Implementations will return the PartialKey value to use for comparing keys for aggregating events
     *
     * @return the type of comparator to use
     */
    public PartialKey getKeyComparator() {
        return PartialKey.ROW_COLFAM;
    }
    
    /**
     * When the query expression evaluates to true against the event, the event fields will be serialized into the Value and returned up the iterator stack.
     * Implemenations will need to provide a key to be used with the event.
     *
     * @param k
     * @return the key that should be returned with the map of values.
     */
    public Key getReturnKey(Key k) throws Exception {
        ColumnVisibility cv = markingFunctions.combine(columnVisibilities);
        return getMappedKey(k, cv.flatten());
    }
    
    /**
     * This routine is used to hold a key that is unique per UID (i.e. drop the COLQ)
     *
     * @param k
     * @return the key sans COLQ
     */
    public Key getMappedKey(Key k, byte[] cv) {
        // Remove the COLQ from the key and use the provided visibility
        return new Key(k.getRowData().getBackingArray(), k.getColumnFamilyData().getBackingArray(), EMPTY_BYTES, cv, k.getTimestamp(), k.isDeleted(), false);
    }
    
    /**
     * Reset state.
     */
    public void reset() {
        event.clear();
        columnVisibilities.clear();
    }
    
    /**
     * This is the method by which field names and values are added into the current event
     *
     * @param fieldName
     * @param fieldValue
     */
    protected void addEventField(EventFields event, String fieldName, FieldValue fieldValue) {
        // We will need to preserve the grouping specific stuff to return the event with its context,
        // so when we have groups, use FieldValueAndName instead of FieldValue
        int period = fieldName.indexOf('.');
        if (period != -1) {
            fieldValue = new FieldValueAndName(fieldName, fieldValue);
            fieldName = fieldName.substring(0, period);
        }
        event.put(fieldName, fieldValue);
    }
    
    /**
     * Determine whether a key falls into the same event as the currentKey taking uidMapping into account as appropriate
     * 
     * @param key2
     * @param comparator
     * @param ignoreUidMapping
     * @return true if the same key
     */
    protected boolean sameAsCurrentKey(Key key2, PartialKey comparator, boolean ignoreUidMapping) {
        if (uidMapper != null && !ignoreUidMapping) {
            key2 = mapUid(key2, false, false, false, false);
        }
        return currentKey.equals(key2, comparator);
    }
    
    /**
     * Aggregate the fields for an event, taking uidMapping into account as appropriate This will also apply filters for all appropriate keys deriving extra
     * parameters such as term offsets
     * 
     * @param event
     *            : the event being built
     * @param fieldsToAggregate
     *            : the fields to aggregate
     * @param ignoreUidMapping
     *            : true if uid mapping is to be ignored altogether
     * @param extraParameters
     *            : if not null, and there are filters, then fill with the extra parameters as created by the filters for query evaluation
     * @throws IOException
     */
    protected void aggregateRowColumn(EventFields event, NavigableSet<String> fieldsToAggregate, boolean ignoreUidMapping, Map<String,Object> extraParameters)
                    throws IOException {
        currentKey.set(iterator.getTopKey());
        if (uidMapper != null && !ignoreUidMapping) {
            currentKey = mapUid(currentKey, false, false, false, false);
        }
        
        boolean tracing = log.isTraceEnabled();
        
        try {
            Text colqT = new Text();
            Text colfT = new Text();
            int skips = 0;
            Key tk;
            Key lastMappedKey = null;
            Value tv;
            while (iteratorHasTop(ignoreUidMapping) && sameAsCurrentKey(iterator.getTopKey(), getKeyComparator(), ignoreUidMapping)) {
                
                // if we have extraParameters with filters to fill them...
                if (extraParameters != null && this.filters != null) {
                    // get the mapped key for this key
                    Key mappedKey = getMappedKey(iterator.getTopKey(), iterator.getTopKey().getColumnVisibilityData().getBackingArray());
                    
                    // if this is a new key, then get extra parameters for it
                    if (lastMappedKey == null || !lastMappedKey.equals(mappedKey, getKeyComparator())) {
                        lastMappedKey = mappedKey;
                        calculateExtraParameters(mappedKey, extraParameters);
                    }
                }
                
                tk = iterator.getTopKey();
                tv = iterator.getTopValue();
                
                if (this.filters != null && !this.filters.accept(tk, tv)) {
                    if (tracing)
                        log.trace("Skipping since filters denied this key/value");
                    iterator.next();
                    continue;
                }
                
                tk.getColumnFamily(colfT);
                tk.getColumnQualifier(colqT);
                
                String colq = colqT.toString();
                if (tracing)
                    log.trace("aggregateRowColumn, colq: " + colq);
                int idx = colq.indexOf(NULL_BYTE_STRING);
                String orgField = colq.substring(0, idx);
                String value = colq.substring(idx + 1);
                
                // remove any field grouping information
                String field = orgField;
                String context = null;
                int period = orgField.indexOf('.');
                if (period != -1) {
                    context = field.substring(period + 1);
                    field = field.substring(0, period);
                }
                
                if (!fieldsToAggregate.contains(field)) {
                    if (tracing)
                        log.trace("not a required field");
                    if (skips < maxSkips) {
                        ++skips;
                        if (tracing)
                            log.trace("Skipping");
                        iterator.next();
                    } else {
                        skips = 0;
                        String nextField = getNextRequiredField(field, fieldsToAggregate);
                        // if we have no next field, we can break out of this loop early unless we are uidMapping in which case we may have other events to look
                        // at
                        if (nextField == null && (uidMapper == null || ignoreUidMapping)) {
                            if (tracing)
                                log.trace("Returning early from loop.");
                            Range seekTo = new Range(tk.followingKey(PartialKey.ROW_COLFAM), true, seekRange.getEndKey(), seekRange.isEndKeyInclusive());
                            if (tracing)
                                log.trace("Range to seek source: " + seekTo);
                            iterator.seek(seekTo, seekingCFs, seekingCFsInclusive);
                            break;
                        }
                        // if we have a next field, then seek to it within the current event
                        else if (nextField != null) {
                            if (tracing)
                                log.trace("Seeking to next field");
                            Key nextKey = new Key(tk.getRow(), colfT, new Text(nextField));
                            Range seekTo = new Range(nextKey, true, seekRange.getEndKey(), seekRange.isEndKeyInclusive());
                            iterator.seek(seekTo, seekingCFs, seekingCFsInclusive);
                        }
                        // we have depleted the fields to aggregate in the current event, seek to the first field of the next event
                        else if (uidMapper != null && !ignoreUidMapping) {
                            if (tracing)
                                log.trace("Seeking to next event");
                            Range seekTo = new Range(tk.followingKey(PartialKey.ROW_COLFAM), true, seekRange.getEndKey(), seekRange.isEndKeyInclusive());
                            if (tracing)
                                log.trace("Range to seek source: " + seekTo);
                            iterator.seek(seekTo, seekingCFs, seekingCFsInclusive);
                        }
                    }
                } else {
                    if (tracing)
                        log.trace("found required field: " + field);
                    
                    // Add the visibility for this K,V pair to the combiner
                    ColumnVisibility columnVisibility = getColumnVisibility(tk);
                    if (columnVisibility.getExpression() != null && columnVisibility.getExpression().length != 0) {
                        this.columnVisibilities.add(columnVisibility);
                    }
                    
                    addEventField(event, orgField, new FieldValue(columnVisibility, value.getBytes(), context));
                    iterator.next();
                }
            }
            // Get the return key
            returnKey = getReturnKey(currentKey);
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
    
    /**
     * Aggregate the fields for an event, taking uidMapping into account as appropriate. This uses the aggregateRowColumn method for the majority of the work,
     * but also adds the EVENT_DATATYPE, CHILD_COUNT, and PARENT_ID as required.
     * 
     * @param event
     *            : the event being built
     * @param fieldsToAggregate
     *            : the fields to aggregate
     * @param ignoreUidMapping
     *            : true if uid mapping is to be ignored altogether
     * @throws IOException
     */
    protected void aggregateEvent(EventFields event, NavigableSet<String> fieldsToAggregate, boolean ignoreUidMapping, Map<String,Object> extraParameters)
                    throws IOException {
        if (includeDataTypeAsEventField && fieldsToAggregate.contains(EVENT_DATATYPE)) {
            String colfam = iterator.getTopKey().getColumnFamily().toString();
            int idx = colfam.indexOf(NULL_BYTE_STRING);
            String dataType = colfam.substring(0, idx);
            FieldValue dataTypeField = new FieldValue(new ColumnVisibility(iterator.getTopKey().getColumnVisibilityData().getBackingArray()),
                            dataType.getBytes());
            addEventField(event, EVENT_DATATYPE, dataTypeField);
        }
        
        aggregateRowColumn(event, fieldsToAggregate, ignoreUidMapping, extraParameters);
        
        if (includeChildCount && fieldsToAggregate.contains(Constants.CHILD_COUNT)) {
            String cf = returnKey.getColumnFamily().toString();
            int index = cf.indexOf(NULL_BYTE_STRING);
            String dataType = cf.substring(0, index);
            String uidString = cf.substring(index + 1);
            UID uid = UID.parse(uidString);
            int childCount = getChildCount(returnKey.getRow(), dataType, uid);
            if (childCount > 0) {
                FieldValue childCountField = new FieldValue(new ColumnVisibility(returnKey.getColumnVisibility()), Integer.toString(childCount).getBytes());
                addEventField(event, Constants.CHILD_COUNT, childCountField);
            }
        }
        if (includeParent && fieldsToAggregate.contains(Constants.PARENT_UID)) {
            String cf = returnKey.getColumnFamily().toString();
            String uidString = cf.substring(cf.indexOf('\0') + 1);
            UID uid = UID.parse(uidString);
            if (uid.getExtra() != null && !uid.getExtra().equals("")) {
                String parentUid = uidString.substring(0, uidString.lastIndexOf(UIDConstants.DEFAULT_SEPARATOR));
                FieldValue parentField = new FieldValue(new ColumnVisibility(returnKey.getColumnVisibility()), parentUid.getBytes());
                addEventField(event, Constants.PARENT_UID, parentField);
            }
        }
    }
    
    /**
     * Aggregate an alternative event. This is used when we are returning an event other than the one that was evaluated.
     * 
     * @param shardId
     * @param dataType
     * @param uid
     * @param event
     * @param fieldsToAggregate
     * @throws IOException
     */
    protected boolean aggregateAltEvent(String shardId, String dataType, String uid, EventFields event, NavigableSet<String> fieldsToAggregate)
                    throws IOException {
        boolean found = false;
        
        Key marker = null;
        if (iteratorHasTop(true)) {
            marker = new Key(iterator.getTopKey());
        }
        
        Key startKey = new Key(new Text(shardId), new Text(dataType + NULL_BYTE_STRING + uid));
        Key endKey = new Key(new Text(shardId), new Text(dataType + NULL_BYTE_STRING + uid + NULL_BYTE_STRING));
        Range range = new Range(startKey, true, endKey, false);
        
        // seek too the new range
        Set<ByteSequence> emptyCfs = Collections.emptySet();
        iterator.seek(range, emptyCfs, false);
        
        // Only aggregate if we actually found the event
        if (iterator.hasTop()) {
            
            // remember the original return key
            Key orgReturnKey = returnKey;
            
            // now aggregate the event
            aggregateEvent(event, fieldsToAggregate, true, null);
            
            // now return the original return key (which falls in the requested range),
            // but also returns the actual cf containing the alternate UID in the columnQualifier
            returnKey = new Key(orgReturnKey.getRowData().getBackingArray(), orgReturnKey.getColumnFamilyData().getBackingArray(), returnKey
                            .getColumnFamilyData().getBackingArray(), returnKey.getColumnVisibilityData().getBackingArray(), returnKey.getTimestamp(),
                            returnKey.isDeleted(), false);
            
            found = true;
        }
        
        // now reset the iterator back to where it was
        // if the iterator was done when we started, no need to seek anywhere as we are in that state again
        if (marker != null) {
            iterator.seek(new Range(marker, true, seekRange.getEndKey(), seekRange.isEndKeyInclusive()), seekingCFs, seekingCFsInclusive);
        }
        return found;
    }
    
    /**
     * Get the child count for a document.
     * 
     * @param row
     * @param dataType
     * @param uid
     * @return
     * @throws IOException
     */
    protected int getChildCount(Text row, String dataType, UID uid) throws IOException {
        Key marker = null;
        if (iteratorHasTop(true)) {
            marker = new Key(iterator.getTopKey());
        }
        
        try {
            Text colqT = new Text();
            Text colfT = new Text();
            Key tk;
            
            // now if the sub-iterator is the DescendentFilterIterator, then we may be here for a "children" query in which case the
            // children of the children are being filtered out. Temporarily disable this.
            boolean disabledDescendantIterator = false;
            if (iterator instanceof DescendentFilterIterator) {
                if (((DescendentFilterIterator) iterator).isChildrenOnly()) {
                    ((DescendentFilterIterator) iterator).setChildrenOnly(false);
                    disabledDescendantIterator = true;
                }
            }
            
            // create the children range
            String baseUid = uid.getBaseUid();
            Key startKey = new Key(row, new Text(dataType + NULL_BYTE_STRING + baseUid + UIDConstants.DEFAULT_SEPARATOR));
            Key endKey = new Key(row, new Text(dataType + NULL_BYTE_STRING + baseUid + Constants.MAX_UNICODE_STRING));
            Range range = new Range(startKey, true, endKey, false);
            
            // seek too the new range
            Set<ByteSequence> emptyCfs = Collections.emptySet();
            iterator.seek(range, emptyCfs, false);
            
            // the list of children uids
            Set<String> uids = new HashSet<>();
            
            // now iterator through the keys and gather up the children uids
            String parentUid = uid.toString() + UIDConstants.DEFAULT_SEPARATOR;
            while (iteratorHasTop(true)) {
                tk = iterator.getTopKey();
                tk.getColumnFamily(colfT);
                tk.getColumnQualifier(colqT);
                
                String cf = iterator.getTopKey().getColumnFamily().toString();
                int index = cf.indexOf(NULL_BYTE_STRING);
                String uidString = cf.substring(index + 1);
                if (uidString.startsWith(parentUid)) {
                    int nextIndex = uidString.indexOf(UIDConstants.DEFAULT_SEPARATOR, parentUid.length());
                    // if no more separators past the parentUid, then this is an immediate child
                    if (nextIndex < 0) {
                        uids.add(uidString);
                        log.debug("Found a child UID: " + parentUid + " <= " + uidString);
                        startKey = new Key(row, new Text(dataType + NULL_BYTE_STRING + uidString + '0'));
                    }
                    // we found another separator, so this is a descendant....try to seek directly to the next immediate child
                    else {
                        log.debug("Passing by a descendant UID: " + parentUid + " <= " + uidString);
                        startKey = new Key(row, new Text(dataType + NULL_BYTE_STRING + uidString.substring(0, nextIndex) + '0'));
                    }
                } else {
                    log.debug("Passing by a non-child UID: " + parentUid + " <= " + uidString);
                    startKey = new Key(row, new Text(dataType + NULL_BYTE_STRING + uidString + UIDConstants.DEFAULT_SEPARATOR));
                }
                log.debug("Seeking to " + startKey);
                range = new Range(startKey, true, endKey, false);
                iterator.seek(range, emptyCfs, false);
            }
            
            // now reset the descendant filter iterator if it was disabled
            if (disabledDescendantIterator) {
                ((DescendentFilterIterator) iterator).setChildrenOnly(true);
            }
            
            // now reset the iterator back to where it was
            // if the iterator was done when we started, no need to seek anywhere as we are in that state again
            if (marker != null) {
                iterator.seek(new Range(marker, true, seekRange.getEndKey(), seekRange.isEndKeyInclusive()), seekingCFs, seekingCFsInclusive);
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
    
    /**
     * Determine whether a uid was already returned by this instance of the EvaluatingIterator. This will only keep state within the context of one base uid and
     * is only used when we have a returnUidMapper in which case we could have multiple evaluated events mapped to the same event.
     *
     * @param uid
     * @return true if we had already returned this uid.
     */
    protected boolean alreadyReturned(String uid) {
        String baseUid = UID.parseBase(uid).getBaseUid();
        
        // first lets determine whether the set of already returned uids should be cleared
        if ((alreadyReturnedBaseUid != null) && (!alreadyReturnedBaseUid.equals(baseUid))) {
            alreadyReturned.clear();
            alreadyReturnedBaseUid = baseUid;
        }
        
        alreadyReturnedBaseUid = baseUid;
        return !alreadyReturned.add(uid);
    }
    
    /**
     * A method to be used instead of iterator.hasTop() as we need to apply the post CF filter if set (see super.mapSeek())
     * 
     * @return
     * @throws IOException
     */
    protected boolean iteratorHasTop() throws IOException {
        return iteratorHasTop(false);
    }
    
    /**
     * A method to be used instead of iterator.hasTop() as we need to apply the post CF filter if set (see super.mapSeek())
     * 
     * @return
     * @throws IOException
     */
    protected boolean iteratorHasTop(boolean ignoreUidMapping) throws IOException {
        if (uidMapper != null && !ignoreUidMapping) {
            while (iterator.hasTop() && !containedByFilter(mapUid(iterator.getTopKey(), false, false, false, false))) {
                iterator.next();
            }
        }
        return iterator.hasTop();
    }
    
    /**
     * Find the top
     */
    @Override
    protected void findTop() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("findTop");
        }
        do {
            reset();
            // check if aggregation is needed
            if (iteratorHasTop()) {
                if (log.isDebugEnabled()) {
                    log.debug("iterator has top: " + iterator.getTopKey());
                }
                
                // search for the next event key
                findEventKey(iterator.getTopKey());
                
                if (iteratorHasTop()) {
                    
                    // determine what set of fields are required
                    NavigableSet<String> fieldsToAggregate = requiredFields;
                    if (returnUidMapper != null) {
                        fieldsToAggregate = new TreeSet<>(evaluator.getFieldNames());
                    }
                    
                    Map<String,Object> extraParameters = new HashMap<>();
                    
                    aggregateEvent(event, fieldsToAggregate, false, extraParameters);
                    
                    // Check to see if the date fits within the range. Compare against the timestamp of the return key
                    if (!dateRange.containsLong(returnKey.getTimestamp())) {
                        if (log.isDebugEnabled()) {
                            log.debug("Event does not fit into query time range, skipping: " + returnKey);
                        }
                        event.clear();
                    }
                    
                    if (this.validUnevaluatedFields != null) {
                        extraParameters.put("validUnevaluatedFields", this.validUnevaluatedFields);
                    }
                    
                    // Evaluate the event against the expression
                    try {
                        if (this.evaluator.evaluate(event, extraParameters)) {
                            if (log.isDebugEnabled()) {
                                log.debug("Event evaluated to true, key = " + returnKey);
                            }
                            
                            // Perform filtering if filters have been defined.
                            if (this.filters != null && !this.filters.accept(event)) {
                                event.clear();
                            } else {
                                // if a return uid mapper is set, then map the uid and return that event instead.
                                // if the event is supposed to be filtered out, then we skip this step.
                                if (returnUidMapper != null) {
                                    // cache the return key as we will use it for the returnKey and simply return an alternate returnValue
                                    String cf = returnKey.getColumnFamily().toString();
                                    int index = cf.indexOf(NULL_BYTE_STRING);
                                    String shardId = returnKey.getRow().toString();
                                    String dataType = cf.substring(0, index);
                                    String uid = cf.substring(index + 1);
                                    String mappedUid = returnUidMapper.getUidMapping(uid);
                                    if (mappedUid == null)
                                        mappedUid = uid;
                                    reset();
                                    // attempt to load the mapped Uid
                                    if (!alreadyReturned(mappedUid)) {
                                        if (!aggregateAltEvent(shardId, dataType, mappedUid, event, (returnFields == null ? CONTAINS_ALL : new TreeSet<>(
                                                        returnFields)))) {
                                            reset();
                                            if (mappedUid.equals(uid)) {
                                                log.error("Unable to aggregate the event we just evaluated: " + shardId + '/' + dataType + '/' + uid);
                                            } else {
                                                log.warn("Returning the matched event " + uid + " as we were unable to find the " + returnUidMapper
                                                                + " mapped event " + shardId + '/' + dataType + '/' + mappedUid);
                                                // we need to re-aggregate the original event using the return fields
                                                if (!alreadyReturned(uid)) {
                                                    if (!aggregateAltEvent(shardId, dataType, uid, event, (returnFields == null ? CONTAINS_ALL : new TreeSet<>(
                                                                    returnFields)))) {
                                                        reset();
                                                        log.error("Unable to aggregate the event we just evaluated: " + shardId + '/' + dataType + '/' + uid);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                
                                // If RETURN_FIELDS is set, remove all non-matching fields from the EventFields object.
                                if (null != this.returnFields) {
                                    // Copy the field names from the EventFields object so we don't run into a concurrent
                                    // modification exception
                                    Set<String> fieldNames = new HashSet<>(event.keySet());
                                    for (String field : fieldNames) {
                                        if (!this.returnFields.contains(field))
                                            event.removeAll(field);
                                    }
                                }
                                
                                // If BLACKLISTED_FIELDS is set, remove all non-matching fields from the EventFields object.
                                if (null != this.blacklistedFields) {
                                    // Copy the field names from the EventFields object so we don't run into a concurrent
                                    // modification exception
                                    Set<String> fields = new HashSet<>(event.keySet());
                                    for (String field : fields) {
                                        if (this.blacklistedFields.contains(field))
                                            event.removeAll(field);
                                    }
                                }
                            }
                            
                            // The combination of whitelist and blacklist could lead to an empty event.
                            if (event.size() > 0) {
                                // TODO This should be moved to a method that can be overridden so the default behavior can be
                                // changed by the implementer. The getTopValue() method is a likely candidate.
                                // Create a byte array
                                
                                // if we are including the group context, then we need to unwrap any FieldValues that are
                                // FieldValueAndName objects containing a different field name
                                EventFields eventToRtrn = event;
                                if (includeGroupingContext) {
                                    eventToRtrn = new EventFields();
                                    for (Map.Entry<String,FieldValue> entry : event.entries()) {
                                        if (entry.getValue() instanceof FieldValueAndName) {
                                            eventToRtrn.put(((FieldValueAndName) entry.getValue()).getFieldName(), entry.getValue());
                                        } else {
                                            eventToRtrn.put(entry.getKey(), entry.getValue());
                                        }
                                    }
                                }
                                
                                ByteArrayOutputStream baos = new ByteArrayOutputStream(10 + eventToRtrn.getByteSize() + (eventToRtrn.size() * 20));
                                Output output = new Output(baos);
                                
                                // Serialize the EventFields object
                                kryo.writeObject(output, eventToRtrn);
                                
                                output.close();
                                
                                // Truncate array to the used size.
                                returnValue = new Value(baos.toByteArray());
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("The whitelist or blacklist excluded all fields from the event with key = " + returnKey);
                                }
                                
                                returnKey = null;
                                returnValue = null;
                            }
                        } else {
                            log.debug("event evaluated to false or was empty");
                            if (log.isTraceEnabled()) {
                                if (event.size() > 0) {
                                    log.trace("Event evaluated to false");
                                    log.trace("Event:\n" + event);
                                    log.trace("Query: " + expression);
                                } else {
                                    log.trace("event was empty");
                                }
                            }
                            returnKey = null;
                            returnValue = null;
                        }
                    } catch (ParseException e) {
                        throw new IOException("Failed to evaluate event", e);
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Iterator no longer has top.");
                    }
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Iterator.hasTop() == false");
                }
                returnKey = null;
                returnValue = null;
            }
        } while (returnValue == null && iteratorHasTop());
        
        // Sanity check. Make sure both returnValue and returnKey are null or both are not null
        if (!((returnKey == null && returnValue == null) || (returnKey != null && returnValue != null))) {
            log.warn("Key: " + ((returnKey == null) ? "null" : returnKey.toString()));
            log.warn("Value: " + ((returnValue == null) ? "null" : returnValue.toString()));
            throw new IOException("Return values are inconsistent");
        }
    }
    
    /**
     * Method used to find the first key at or after the supplied key.
     *
     * This method has specific behavior when certain conditions are met:
     *
     * 1) When <code>key</code>'s column family begins with 'fi\u0000', we know this is field index key. We should seek to the first key after the field event
     * keys. Then we should recursively keep searching until there are no keys left or condition 4 is met.
     *
     * 2) When the column family is in the <code>colFamsToIgnore</code> set we should seek to the first key after the column family ends. Then we should
     * recursively keep searching until there are no keys left or condition 4 is met.
     *
     * 3) If the column family does not contain a null byte string, it's not an event key we reserve the right to put in partition level keys
     *
     * 4) If neither 1 nor 2 are true and the cf contains a null byte string, we are at an event key.
     *
     * @param key
     *            starting point to check if it is a valid key
     */
    void findEventKey(Key key) throws IOException {
        String cf = key.getColumnFamily().toString();
        if (cf.startsWith(FI_PREFIX)) {
            Key seekTo = new Key(key.getRow(), NEXT_FI_PREFIX);
            Range r = new Range(seekTo, false, seekRange.getEndKey(), seekRange.isEndKeyInclusive());
            this.iterator.seek(r, this.seekingCFs, this.seekingCFsInclusive);
            if (iteratorHasTop()) {
                findEventKey(this.iterator.getTopKey());
            }
        } else if (colFamsToIgnore.contains(cf)) {
            Key seekTo = key.followingKey(PartialKey.ROW_COLFAM);
            Range r = new Range(seekTo, true, seekRange.getEndKey(), seekRange.isEndKeyInclusive());
            this.iterator.seek(r, this.seekingCFs, this.seekingCFsInclusive);
            if (iteratorHasTop()) {
                findEventKey(this.iterator.getTopKey());
            }
        } else if (!cf.contains(NULL_BYTE_STRING)) { // this is a partition key, skip it
            this.iterator.next();
            if (iteratorHasTop()) {
                findEventKey(this.iterator.getTopKey());
            }
        }
        // if we're down here, we're at an event key
    }
    
    /**
     * Calculate the extra parameters for a given document/key.
     * 
     * @param returnKey
     * @param extraParameters
     */
    protected void calculateExtraParameters(Key returnKey, Map<String,Object> extraParameters) {
        if (log.isDebugEnabled()) {
            log.debug("Fetching the extra parameters (e.g. phrases for the content functions)");
        }
        
        Map<String,Object> docExtraParameters = filters.getContextMap(returnKey, null);
        
        // if no uid mapping, then assume there will be only one document
        if (uidMapper == null) {
            extraParameters.putAll(docExtraParameters);
        } else {
            // if uid mapping, then we may have multiple mapped keys, so create a set per parameter
            for (Map.Entry<String,Object> entry : docExtraParameters.entrySet()) {
                @SuppressWarnings("unchecked")
                Set<Object> set = (Set<Object>) (extraParameters.get(entry.getKey()));
                if (set == null) {
                    set = new HashSet<>();
                    extraParameters.put(entry.getKey(), set);
                }
                set.add(entry.getValue());
            }
        }
    }
    
    @Override
    public Key getTopKey() {
        return returnKey;
    }
    
    @Override
    public Value getTopValue() {
        return returnValue;
    }
    
    @Override
    public boolean hasTop() {
        return returnKey != null;
    }
    
    @Override
    public void next() throws IOException {
        log.debug("next");
        returnKey = null;
        returnValue = null;
        findTop();
    }
    
    /**
     * Copy of IteratorUtil.maximizeStartKeyTimeStamp due to IllegalAccessError
     *
     * @param range
     * @return
     */
    static Range maximizeStartKeyTimeStamp(Range range) {
        Range seekRange = range;
        
        if (range.getStartKey() != null && range.getStartKey().getTimestamp() != Long.MAX_VALUE) {
            Key seekKey = new Key(seekRange.getStartKey());
            seekKey.setTimestamp(Long.MAX_VALUE);
            seekRange = new Range(seekKey, range.isStartKeyInclusive(), range.getEndKey(), range.isEndKeyInclusive());
        }
        
        return seekRange;
    }
    
    /**
     * seek to the event specified by the key using the data in the value to validate index-only fields in the query tree.
     *
     * @param eventKey
     * @param eventValue
     * @throws IOException
     */
    public void seekEvent(Key eventKey, Value eventValue) throws IOException {
        Key endKey = eventKey.followingKey(PartialKey.ROW_COLFAM);
        Key startKey = new Key(eventKey.getRow(), eventKey.getColumnFamily());
        Range eventRange = new Range(startKey, endKey);
        HashSet<ByteSequence> cf = new HashSet<>();
        cf.add(eventKey.getColumnFamilyData());
        if (eventValue != null) {
            StringTuple st = new StringTuple(0);
            st.readFields(new DataInputStream(new ByteArrayInputStream(eventValue.get())));
            this.validUnevaluatedFields = BooleanLogicIteratorJexl.deserializeMatchingQueryNodes(st);
            if (log.isDebugEnabled()) {
                log.debug("validUnevaluatedFields: " + this.validUnevaluatedFields);
            }
        }
        this.seek(eventRange, cf, true);
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // do not want to seek to the middle of a value that should be
        // aggregated...
        seekRange = maximizeStartKeyTimeStamp(range);
        seekingCFs = columnFamilies;
        seekingCFsInclusive = inclusive;
        
        if (uidMapper != null) {
            // map the range to one that includes everything mapped to the required uid
            SeekParams mappedRangeParams = mapSeek(range, columnFamilies, inclusive);
            seekRange = mappedRangeParams.range;
            seekingCFs = mappedRangeParams.columnFamilies;
            seekingCFsInclusive = mappedRangeParams.inclusive;
        }
        
        if (log.isDebugEnabled()) {
            log.debug("seek, range: " + range);
            log.debug("seek, maxStartKeyRange: " + seekRange);
        }
        
        seekRange = handleReseek(seekRange);
        
        if (log.isDebugEnabled()) {
            log.debug("seek, postCheckForReseek:" + seekRange);
        }
        
        iterator.seek(seekRange, seekingCFs, seekingCFsInclusive);
        if (iteratorHasTop()) {
            findTop();
        } else {
            returnKey = null;
        }
        
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        validateOptions(options);
        
        event = new EventFields();
        
        this.iterator = source;
        
        String queryEvaluatorClass = options.get(QUERY_EVALUATOR);
        if (queryEvaluatorClass == null) {
            queryEvaluatorClass = QueryEvaluator.class.getName();
        }
        try {
            Class<?> clz = Class.forName(queryEvaluatorClass);
            if (!QueryEvaluator.class.isAssignableFrom(clz)) {
                throw new RuntimeException("Configured query evaluator is not a QueryEvaluator: " + queryEvaluatorClass);
            }
            @SuppressWarnings("unchecked")
            Class<? extends QueryEvaluator> evaluatorClass = (Class<? extends QueryEvaluator>) clz;
            
            this.evaluator = evaluatorClass.newInstance();
            this.evaluator.setQuery(this.expression);
            
            if (!this.normalizerListString.isEmpty()) {
                this.evaluator.configureNormalizers(normalizerListString);
            }
        } catch (ParseException e) {
            throw new RuntimeException("Failed to parse query: " + this.expression, e);
        } catch (InstantiationException e) {
            throw new RuntimeException("Failed to instantiate query evaluator " + queryEvaluatorClass, e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to access query evaluator class " + queryEvaluatorClass, e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to find query evaluator class " + queryEvaluatorClass, e);
        }
        
        setUpRequiredFields();
        
        // If we want to enable any filters
        if (options.containsKey(QueryParameters.FILTERING_ENABLED) && options.containsKey(QueryParameters.FILTERING_CLASSES)) {
            if (log.isDebugEnabled()) {
                log.debug("Filtering enabled");
            }
            
            String filteringClasses = options.get(QueryParameters.FILTERING_CLASSES);
            
            String[] classNames = StringUtils.split(filteringClasses, GenericShardQueryConfiguration.PARAM_VALUE_SEP);
            
            // Warn if no classes were provided (doesn't *need* to fail)
            if (classNames.length == 0) {
                log.debug("No filtering will occur because no class names were provided. At least one class name should be provided if filtering is enabled.");
            }
            
            Map<String,Object> filterOptions = new HashMap<>();
            
            filterOptions.put("query", this.expression);
            filterOptions.put("unevaluatedFields", this.unevaluatedFields.toArray(new String[this.unevaluatedFields.size()]));
            
            filters = new FilteringMaster(source.deepCopy(env), env, classNames, filterOptions, this.evaluator);
        }
        
        String opt = options.get(MAX_SKIPS_OPT);
        if (opt != null) {
            try {
                int maxSkips = Integer.parseInt(options.get(MAX_SKIPS_OPT));
                if (maxSkips > 0) {
                    this.maxSkips = maxSkips;
                } else {
                    log.warn("Max skips will default to 5. Max skips must be > 0.");
                }
            } catch (Throwable t) {
                log.error("Max skips will default to 5.", t);
            }
        }
    }
    
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        
        options.put(QUERY_OPTION, "query expression");
        options.put(Constants.START_DATE, "start date from query in ms");
        options.put(Constants.END_DATE, "end date from query in ms");
        options.put(QueryParameters.UNEVALUATED_FIELDS, "a pristine copy of the unevaluated fields");
        options.put(QueryParameters.RETURN_FIELDS, "'" + GenericShardQueryConfiguration.PARAM_VALUE_SEP + "' separated list of fields that we want returned");
        options.put(QueryParameters.BLACKLISTED_FIELDS, "'" + GenericShardQueryConfiguration.PARAM_VALUE_SEP
                        + "' separated list of fields that shouldn't be returned");
        options.put(NORMALIZER_LIST, "';' separated list of fieldName:normalizerClass values");
        options.put(QueryParameters.NON_EVENT_KEY_PREFIXES, "'" + GenericShardQueryConfiguration.PARAM_VALUE_SEP
                        + "' separated list of column families that should be ignored");
        options.put(QueryParameters.INCLUDE_DATATYPE_AS_FIELD, "include the datatype as an event field");
        options.put(QueryParameters.INCLUDE_GROUPING_CONTEXT, "include field with the extended grouping context on the field names");
        options.put(QueryParameters.INCLUDE_CHILD_COUNT, "include the count of document children as a field");
        options.put(QueryParameters.INCLUDE_PARENT, "include the uid of the parent document as a field");
        options.put(QUERY_EVALUATOR, "overide the query evaluator to be used for query evaluation (must extend QueryEvaluator)");
        options.put(QueryParameters.FILTERING_ENABLED, "boolean value whether or not to filter events");
        options.put(QueryParameters.FILTERING_CLASSES, "'" + GenericShardQueryConfiguration.PARAM_VALUE_SEP + "' separated list of DataFilter classes");
        options.put(MAX_SKIPS_OPT, "The max number of non-required fields skipped over before attempting a seek");
        options.put(QueryParameters.RETURN_UID_MAPPER, "A uid mapper which if set will specify the uid to return in lieu of the one that evaluated to true");
        
        options.putAll(new ShardUidMappingIterator().describeOptions().getNamedOptions());
        
        return new IteratorOptions(getClass().getSimpleName(), "evaluates event objects against an expression", options, null);
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (!options.containsKey(QUERY_OPTION)) {
            log.warn("QUERY_OPTION in EvaluatingIterator was not set.");
            return false;
        } else {
            this.expression = options.get(QUERY_OPTION);
        }
        
        long startDate = 0L;
        if (options.containsKey(Constants.START_DATE))
            startDate = Long.parseLong(options.get(Constants.START_DATE));
        
        long endDate = Long.MAX_VALUE;
        if (options.containsKey(Constants.END_DATE))
            endDate = Long.parseLong(options.get(Constants.END_DATE));
        
        this.dateRange = new LongRange(startDate, endDate);
        
        if (options.containsKey(QueryParameters.UNEVALUATED_FIELDS)) {
            String unevalFields = options.get(QueryParameters.UNEVALUATED_FIELDS);
            if (unevalFields != null && !unevalFields.trim().equals("")) {
                Collections.addAll(this.unevaluatedFields, StringUtils.split(unevalFields, GenericShardQueryConfiguration.PARAM_VALUE_SEP));
            }
        } else {
            // Warn if no unevaluatedFields were provided (doesn't *need* to fail)
            log.debug("No unevaluated fields were provided to the filters");
        }
        
        if (options.containsKey(QueryParameters.RETURN_FIELDS)) {
            String fieldList = options.get(QueryParameters.RETURN_FIELDS);
            if (fieldList != null && !fieldList.trim().equals("")) {
                this.returnFields = new HashSet<>();
                Collections.addAll(this.returnFields, StringUtils.split(fieldList, GenericShardQueryConfiguration.PARAM_VALUE_SEP));
            }
        }
        
        if (options.containsKey(QueryParameters.BLACKLISTED_FIELDS)) {
            String fieldList = options.get(QueryParameters.BLACKLISTED_FIELDS);
            if (fieldList != null && !fieldList.trim().equals("")) {
                this.blacklistedFields = new HashSet<>();
                Collections.addAll(this.blacklistedFields, StringUtils.split(fieldList, GenericShardQueryConfiguration.PARAM_VALUE_SEP));
            }
        }
        
        if (options.containsKey(NORMALIZER_LIST)) {
            this.normalizerListString = options.get(NORMALIZER_LIST);
            if (log.isDebugEnabled()) {
                log.debug("validateOptions, normalizerListString: " + this.normalizerListString);
            }
        }
        
        if (options.containsKey(QueryParameters.NON_EVENT_KEY_PREFIXES)) {
            String[] ignoreCfs = StringUtils.split(options.get(QueryParameters.NON_EVENT_KEY_PREFIXES), GenericShardQueryConfiguration.PARAM_VALUE_SEP);
            colFamsToIgnore = new HashSet<>(ignoreCfs.length);
            
            Collections.addAll(colFamsToIgnore, ignoreCfs);
        } else {
            colFamsToIgnore = new HashSet<>();
        }
        
        if (options.containsKey(QueryParameters.INCLUDE_DATATYPE_AS_FIELD) && Boolean.valueOf(options.get(QueryParameters.INCLUDE_DATATYPE_AS_FIELD))) {
            log.debug("INCLUDE_DATATYPE_AS_FIELD is enabled");
            includeDataTypeAsEventField = true;
        }
        
        if (options.containsKey(QueryParameters.INCLUDE_GROUPING_CONTEXT) && Boolean.valueOf(options.get(QueryParameters.INCLUDE_GROUPING_CONTEXT))) {
            log.debug("INCLUDE_GROUPING_CONTEXT is enabled");
            this.includeGroupingContext = true;
        }
        
        if (options.containsKey(QueryParameters.INCLUDE_CHILD_COUNT) && Boolean.valueOf(options.get(QueryParameters.INCLUDE_CHILD_COUNT))) {
            log.debug("INCLUDE_CHILD_COUNT is enabled");
            includeChildCount = true;
        }
        
        if (options.containsKey(QueryParameters.INCLUDE_PARENT) && Boolean.valueOf(options.get(QueryParameters.INCLUDE_PARENT))) {
            log.debug("INCLUDE_PARENT is enabled");
            includeParent = true;
        }
        
        // Uid mapping is optional
        if (options.containsKey(UidMappingIterator.UID_MAPPER)) {
            this.uidMapper = getUidMapper(options, UidMappingIterator.UID_MAPPER);
        }
        
        if (options.containsKey(QueryParameters.RETURN_UID_MAPPER)) {
            this.returnUidMapper = getUidMapper(options, QueryParameters.RETURN_UID_MAPPER);
        }
        
        return true;
    }
    
    protected UidMapper getUidMapper(Map<String,String> options, String uidMapperOption) {
        Class<?> uidMapperClass;
        try {
            uidMapperClass = Class.forName(options.get(uidMapperOption));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Cannot find class for " + uidMapperOption + " option: " + options.get(uidMapperOption), e);
        }
        if (!UidMapper.class.isAssignableFrom(uidMapperClass)) {
            throw new IllegalArgumentException(uidMapperOption + " option does not implement " + UidMapper.class + ": " + uidMapperClass);
        }
        try {
            return (UidMapper) (uidMapperClass.newInstance());
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Cannot instantiate class for " + uidMapperOption + " option: " + uidMapperClass, e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Cannot access constructor for " + uidMapperOption + " option: " + uidMapperClass, e);
        }
    }
    
    public String getQueryExpression() {
        return this.expression;
    }
    
    /**
     * Returns the field that occurs after currentField in the requiredFields set.
     *
     * @param currentField
     * @return field occuring after currentField in requiredFields; null if there is none
     */
    public String getNextRequiredField(String currentField, NavigableSet<String> fieldsToAggregate) {
        NavigableSet<String> tailSet = fieldsToAggregate.tailSet(currentField, false);
        if (tailSet.isEmpty()) {
            return null;
        } else {
            return tailSet.first();
        }
    }
    
    protected SortedKeyValueIterator<Key,Value> getSource() {
        return this.iterator;
    }
    
    /**
     * If projection is enabled, fill required fields with the union of the projected fields and the queried terms. This will allow the event aggregator to skip
     * fields in events that are unused in either process.
     *
     * If projection is disabled, then create a stub set that returns true no matter what object is passed to it.
     */
    protected void setUpRequiredFields() {
        if (returnFields != null) {
            requiredFields = new TreeSet<>();
            for (String fname : evaluator.getFieldNames()) {
                requiredFields.add(fname.toUpperCase());
            }
            for (String fname : returnFields) {
                requiredFields.add(fname.toUpperCase());
            }
            log.debug("requiredFields: " + requiredFields);
        } else {
            requiredFields = CONTAINS_ALL;
        }
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new EvaluatingIterator(this, env);
    }
    
    public ColumnVisibility getColumnVisibility(Key k) {
        Text cv = k.getColumnVisibility();
        ColumnVisibility result = (ColumnVisibility) cvCache.get(cv);
        if (result == null) {
            result = new ColumnVisibility(cv.getBytes());
            cvCache.put(cv, result);
        }
        return result;
    }
    
    /**
     * This subclass of FieldValue is used to hold a potentially different field name when the real name differs from the one being evaluated against in the
     * EventFields map.
     *
     * 
     *
     */
    public static class FieldValueAndName extends FieldValue {
        private String fieldName;
        
        public FieldValueAndName(String name, FieldValue value) {
            super(value.getVisibility(), value.getValue(), value.getContext());
            setHit(value.isHit());
            this.fieldName = name;
        }
        
        public String getFieldName() {
            return fieldName;
        }
        
        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }
        
        public FieldValue getFieldValue() {
            return this;
        }
        
        public void setFieldValue(FieldValue fieldValue) {
            setVisibility(fieldValue.getVisibility());
            setValue(fieldValue.getValue());
        }
        
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append('(').append(this.fieldName).append(") ").append(super.toString());
            return builder.toString();
        }
    }
    
    /**
     * When this iterator returns an entry, the key is actually *before* the last key this iterator actually read. Upon reseek, we'll receive a range that's of
     * the form:
     *
     * (<last returned key>, <scan range last key><)|]>
     *
     * Since the last returned key is before the last key we actually read, we can definitely reperform work and potentially infinite loop if that key/value
     * pair will always cause a buffer flush by Accumulo.
     *
     * This method catches that case and corrects the range to be after the last read key returned.
     */
    public static Range handleReseek(Range range) {
        final Key start = range.getStartKey();
        if (!range.isStartKeyInclusive() && start.getRowData().length() > 0 && start.getColumnFamilyData().length() > 0
                        && start.getColumnQualifierData().length() == 0) {
            Range newRange = new Range(start.followingKey(PartialKey.ROW_COLFAM), true, range.getEndKey(), range.isEndKeyInclusive());
            return newRange;
        } else {
            return range;
        }
    }
    
}
