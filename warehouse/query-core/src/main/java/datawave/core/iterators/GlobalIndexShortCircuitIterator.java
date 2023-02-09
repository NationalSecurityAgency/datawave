package datawave.core.iterators;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import datawave.ingest.protobuf.Uid;
import datawave.query.Constants;
import datawave.util.TextUtil;

import datawave.util.time.DateHelper;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * <p>
 * Iterator used for global index lookups in ShardQueryLogic. This iterator will aggregate range information for a query term so that we don't pass back too
 * many ranges to the web service. This iterator is constructed with ranges that contain the start and end term, field name, and start and end dates.
 *
 * <p>
 * This iterator has two required properties (SHARDS_PER_DAY and EVENTS_PER_DAY) that must be set and one optional property (DATA_TYPES). This iterator uses
 * these properties to determine what type of ranges to pass up to the range calculator.
 *
 * <p>
 * As this iterator is consuming K/V from the global index for a term, it will return a range that corresponds to the entire day (all shards for that day) if
 * there are more events per day in the global index than EVENTS_PER_DAY or if the number of shards that contain entries for the term is more than
 * SHARDS_PER_DAY. If neither of these thresholds is hit for the day, then this iterator will return a set of ranges that will either be event specific or shard
 * and datatype specific (if there are no UIDs in the Uid.List). If the cardinality exceeds {@link #EVENTS_PER_DAY}, the code will return {@link Long#MAX_VALUE}
 * as the cardinality for that day.
 *
 * <p>
 * NOTE: This iterator assumes that it will be receiving key/values pairs from the global index for one term in a sorted order.
 *
 */
public class GlobalIndexShortCircuitIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    /**
     * When the number of shards that contain data for the given term passes this property value, then a range for the entire day will be returned
     */
    public static final String SHARDS_PER_DAY = "shards.per.day";
    
    /**
     * When the number of events that contain data for the given term passes this property value, then a range for the entire day will be returned
     */
    public static final String EVENTS_PER_DAY = "events.per.day";
    
    /**
     * Comma separated list of data types to return results for, optional.
     */
    public static final String DATA_TYPES = "data.types";
    
    private SortedKeyValueIterator<Key,Value> iterator;
    private Key returnKey = null;
    private Value returnValue = null;
    private long shardsPerDay = 0L;
    private long eventsPerDay = 0L;
    private Set<Range> ranges = new HashSet<>();
    private Set<String> types = new HashSet<>();
    private Range masterRange = null;
    private Collection<ByteSequence> masterColumnFamilies = null;
    private boolean masterColumnFamiliesInclusive = false;
    protected static final Logger log = Logger.getLogger(GlobalIndexShortCircuitIterator.class);
    
    public GlobalIndexShortCircuitIterator() {}
    
    public GlobalIndexShortCircuitIterator(GlobalIndexShortCircuitIterator iter, IteratorEnvironment env) {
        this.iterator = iter.iterator.deepCopy(env);
        this.ranges = iter.ranges;
        this.types = iter.types;
        this.shardsPerDay = iter.shardsPerDay;
        this.eventsPerDay = iter.eventsPerDay;
        this.masterRange = iter.masterRange;
        this.masterColumnFamilies = iter.masterColumnFamilies;
        this.masterColumnFamiliesInclusive = iter.masterColumnFamiliesInclusive;
        
    }
    
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (!validateOptions(options))
            throw new IOException("Iterator options are not correct");
        this.iterator = source;
    }
    
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(DATA_TYPES, "comma separated list of types of data to return");
        options.put(SHARDS_PER_DAY, "when the number of shards that contain data for the term is passed, then a range for the entire day is created");
        options.put(EVENTS_PER_DAY, "when the number of events per day is passed, then a range for the entire day is created");
        return new IteratorOptions(getClass().getSimpleName(), "returns aggregated index ranges for each day", options, null);
    }
    
    public boolean validateOptions(Map<String,String> options) {
        if (options.containsKey(SHARDS_PER_DAY)) {
            this.shardsPerDay = Long.parseLong(options.get(SHARDS_PER_DAY));
        } else {
            return false;
        }
        if (options.containsKey(EVENTS_PER_DAY)) {
            this.eventsPerDay = Long.parseLong(options.get(EVENTS_PER_DAY));
        } else {
            return false;
        }
        if (options.containsKey(DATA_TYPES) && !StringUtils.isEmpty(options.get(DATA_TYPES))) {
            types.addAll(Arrays.asList(options.get(DATA_TYPES).split(",")));
        }
        return true;
    }
    
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new GlobalIndexShortCircuitIterator(this, env);
    }
    
    public boolean hasTop() {
        return (returnValue != null);
    }
    
    public void next() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("next called");
        }
        
        // Return K/V from the range map
        returnKey = null;
        returnValue = null;
        if (this.iterator.hasTop())
            findTop();
    }
    
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("seek called: " + range);
        }
        
        this.iterator.seek(range, columnFamilies, inclusive);
        this.masterRange = range;
        this.masterColumnFamilies = columnFamilies;
        this.masterColumnFamiliesInclusive = inclusive;
        returnValue = null;
        if (this.iterator.hasTop())
            findTop();
    }
    
    public Key getTopKey() {
        if (log.isDebugEnabled()) {
            log.debug("topKey: " + returnKey);
        }
        
        return returnKey;
    }
    
    public Value getTopValue() {
        return returnValue;
    }
    
    /**
     * This method aggregates all information from the global index for a term for one day. It will return when the day boundary has passed so that the ranges
     * for that term for that day can be retrieved via the getTopKey and getTopValue methods.
     * 
     * @throws IOException
     *             for issues with read/write
     */
    private void findTop() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("findTop called");
        }
        
        Key startingRange = this.iterator.getTopKey();
        Date startDate = parseDateFromColQual(startingRange);
        long dayCount = 0;
        Set<String> shardsSeen = new HashSet<>();
        Set<Entry<String,String>> shardDatatypeRanges = new HashSet<>();
        boolean done = false;
        
        if (log.isDebugEnabled()) {
            log.debug("MasterRange: " + this.masterRange);
        }
        
        do {
            if (log.isDebugEnabled()) {
                log.debug("top of do");
            }
            
            // Check for hasTop again because we are in a loop and we are calling next() and seek()
            // on the source iterator inside this loop
            if (!this.iterator.hasTop()) {
                if (log.isDebugEnabled()) {
                    log.debug("source iterator no longer has top, should be done.");
                }
                
                done = true;
                continue;
            }
            
            Key currentKey = this.iterator.getTopKey();
            Value currentValue = this.iterator.getTopValue();
            
            if (log.isDebugEnabled()) {
                log.debug("currentKey: " + currentKey);
            }
            
            // If we have passed the day boundary and we have ranges to pass back, then lets break out
            // of this loop.
            if (!startDate.equals(parseDateFromColQual(currentKey)) && !ranges.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("Not on the desired startDate and have found ranges to return");
                }
                
                done = true;
                continue;
            } else if (!this.masterRange.contains(currentKey)) {
                if (log.isDebugEnabled()) {
                    log.debug("currentKey not contained in the masterRange");
                }
                
                done = true;
                continue;
            } else if (!startDate.equals(parseDateFromColQual(currentKey))) {
                if (log.isDebugEnabled()) {
                    log.debug("Not on the desired startDate and have no ranges to return");
                }
                
                // We have passed the date boundary but didn't have any ranges. In this case, we don't
                // want to return, we want to reset the state for a new day and continue.
                done = false;
                startingRange = currentKey;
                startDate = parseDateFromColQual(startingRange);
                dayCount = 0;
                shardsSeen.clear();
                this.ranges.clear();
            }
            
            // Get the shard id and datatype from the colq
            String colq = currentKey.getColumnQualifier().toString();
            int separator = colq.indexOf(Constants.NULL_BYTE_STRING);
            String shardId = null;
            String datatype = null;
            if (separator != -1) {
                shardId = colq.substring(0, separator);
                datatype = colq.substring(separator + 1);
            } else {
                throw new IOException("Malformed global index qualifier: " + colq);
            }
            
            // Keep track of the number of shards we have seen for this day
            shardsSeen.add(shardId);
            
            // If the types have been set, then determine if we need to skip or continue
            if (!types.isEmpty() && !types.contains(datatype)) {
                this.iterator.next();
                continue;
            }
            
            // Parse the UID.List object from the value
            Uid.List uidList = null;
            boolean forcedDayRange = false;
            try {
                uidList = Uid.List.parseFrom(currentValue.get());
                
                if (log.isDebugEnabled()) {
                    log.debug("UidCOUNT for this key: " + uidList.getCOUNT());
                }
                
                // Add the count for this shard to the total count for the term.
                dayCount += uidList.getCOUNT();
            } catch (InvalidProtocolBufferException e) {
                // Not an event specific range if we cannot decode the protobuf
                forcedDayRange = true;
            }
            
            // Lets create a range for the entire day if any of the following are true:
            // a - forcedDayRange is true; when we cant unpack a Uid.List object from one of the global index entries for that day
            // b - when the number of events for that term in the Uid.List passes the EVENTS_PER_DAY threshold
            // c - when the number of shards for that day passes the SHARDS_PER_DAY threshold
            if (dayCount >= this.eventsPerDay || shardsSeen.size() > this.shardsPerDay || forcedDayRange) {
                if (log.isDebugEnabled()) {
                    log.debug("Removing existing ranges and calculating a new one");
                }
                
                // Empty the range set
                this.ranges.clear();
                shardDatatypeRanges.clear();
                // Add in a range for the whole day
                Key k = new Key(DateHelper.format(startDate));
                
                Date endDate = DateUtils.addDays(startDate, 1);
                Key e = new Key(DateHelper.format(endDate));
                
                Range r = new Range(k, true, e, false);
                this.ranges.add(r);
                
                // We want to seek over everything for this date since we just returned the entire day
                Key masterStartKey = this.masterRange.getStartKey();
                Key seekKey = new Key(masterStartKey.getRow(), masterStartKey.getColumnFamily(), new Text(DateHelper.format(endDate)));
                
                Range seekRange = new Range(seekKey, true, this.masterRange.getEndKey(), this.masterRange.isEndKeyInclusive());
                
                if (log.isDebugEnabled()) {
                    log.debug("Exhausted this date, seeking to: " + seekRange);
                }
                
                this.iterator.seek(seekRange, this.masterColumnFamilies, this.masterColumnFamiliesInclusive);
                this.masterRange = seekRange;
                
                // We don't know what the actual cardinality is, just that it's more than what we want
                dayCount = Long.MAX_VALUE;
                
                done = true;
                continue;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Handling the UID list");
                }
                
                Text shard = new Text(shardId);
                if (uidList.getIGNORE()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Adding range for the shard & datatype");
                    }
                    
                    // Add a Range for the entire shard and datatype.
                    Text cf = new Text(datatype);
                    Key startKey = new Key(shard, cf);
                    Key endKey = new Key(shard, new Text(cf + Constants.MAX_UNICODE_STRING + Constants.NULL_BYTE_STRING));
                    Range shardDatatypeRange = new Range(startKey, true, endKey, false);
                    
                    // Remove all event-specific ranges for this shard/datatype
                    Iterator<Range> rangeIter = this.ranges.iterator();
                    while (rangeIter.hasNext()) {
                        Range r = rangeIter.next();
                        if (shardDatatypeRange.contains(r.getStartKey())) {
                            rangeIter.remove();
                        }
                    }
                    
                    this.ranges.add(shardDatatypeRange);
                    
                    // Track that we already added a range for this shard+datatype
                    shardDatatypeRanges.add(new AbstractMap.SimpleEntry<>(shard.toString(), datatype));
                } else {
                    if (shardDatatypeRanges.contains(new AbstractMap.SimpleEntry<>(shard.toString(), datatype))) {
                        if (log.isDebugEnabled()) {
                            log.debug("Do not need to add event-specific ranges, have already added a range for this shard and datatype");
                        }
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Adding ranges for each shard, datatype, UID");
                        }
                        
                        // Add Event specific ranges
                        for (String uuid : uidList.getUIDList()) {
                            Text cf = new Text(datatype);
                            TextUtil.textAppend(cf, uuid);
                            Key startKey = new Key(shard, cf);
                            Key endKey = new Key(shard, new Text(cf + Constants.NULL_BYTE_STRING));
                            Range eventRange = new Range(startKey, true, endKey, false);
                            this.ranges.add(eventRange);
                        }
                    }
                }
            }
            
            this.iterator.next();
            
        } while (!done);
        
        if (!this.ranges.isEmpty()) {
            // Set the returnKey with the following structure
            // row = original row
            // colf = original colf
            // colq = original colq + cardinality of term
            Text colq = new Text(startingRange.getColumnQualifier());
            TextUtil.textAppend(colq, Long.toString(dayCount));
            this.returnKey = new Key(startingRange.getRow(), startingRange.getColumnFamily(), colq);
            ArrayWritable aw = new ArrayWritable(Range.class);
            aw.set(this.ranges.toArray(new Range[this.ranges.size()]));
            returnValue = new Value(WritableUtils.toByteArray(aw));
            this.ranges.clear();
        }
    }
    
    private Date parseDateFromColQual(Key k) throws IOException {
        try {
            return DateHelper.parse(Text.decode(k.getColumnQualifierData().getBackingArray(), 0, 8));
        } catch (Exception e) {
            throw new IOException("Index entry column qualifier is not correct format: " + k);
        }
    }
    
}
