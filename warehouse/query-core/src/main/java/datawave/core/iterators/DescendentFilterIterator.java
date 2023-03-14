package datawave.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import datawave.data.hash.UIDConstants;
import datawave.util.StringUtils;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * <p>
 * Iterator that can efficiently return a filtered set of event keys, specified by the document tree descendents of a uid, from a DATAWAVE "sharded" event
 * table.
 * </p>
 * 
 * <p>
 * Takes a uid via the option {@code DescendentFilterIterator.PARENT_UIDS}.
 * </p>
 * <p>
 * Optionally takes a flag via the option {@code DescendentFilterIterator.CHILDREN_ONLY} meaning to return only the immediate children of the specified parent
 * uid.
 * </p>
 * 
 * <p>
 * Will automatically skip over in-partition index keys (fi, tf, d)
 * </p>
 * 
 * 
 */
public class DescendentFilterIterator extends SkippingIterator implements OptionDescriber {
    public static final String PARENT_UIDS = "descendentfilter.parent.uids";
    public static final String CHILDREN_ONLY = "descendentfilter.children.only";
    public static final String INCLUDE_PARENT = "descendentfilter.include.parent";
    private static final String NULL = "\0";
    private static final byte[] UID_SEPARATOR_BYTES = Character.toString(UIDConstants.DEFAULT_SEPARATOR).getBytes();
    private final Logger log = Logger.getLogger(DescendentFilterIterator.class);
    
    private Range prevRange = new Range();
    @SuppressWarnings("unchecked")
    private Collection<ByteSequence> columnFamilies = (Set<ByteSequence>) Collections.EMPTY_SET;
    private boolean colfInclusive = false;
    
    private final Text prevRow = new Text();
    private final Text prevCf = new Text();
    private final Text row = new Text();
    private final Text cf = new Text();
    
    private boolean childrenOnly = false;
    private boolean includeParent = false;
    private String parentUids = null;
    private Map<String,Map<String,Set<String>>> parentUidMap = new HashMap<>();
    
    public DescendentFilterIterator() {
        
    }
    
    public DescendentFilterIterator(DescendentFilterIterator other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
        
        this.childrenOnly = other.childrenOnly;
        this.includeParent = other.includeParent;
        this.setParentUids(other.parentUids);
    }
    
    public boolean isChildrenOnly() {
        return childrenOnly;
    }
    
    public void setChildrenOnly(boolean childrenOnly) {
        this.childrenOnly = childrenOnly;
    }
    
    public boolean isIncludeParent() {
        return includeParent;
    }
    
    public void setIncludeParent(boolean includeParent) {
        this.includeParent = includeParent;
    }
    
    public String getParentUids() {
        return parentUids;
    }
    
    public void setParentUids(String parentUids) {
        this.parentUids = parentUids;
        // now parse the parentUids into the map
        this.parentUidMap.clear();
        for (String shardTypeUid : StringUtils.split(parentUids, ' ')) {
            String[] parts = StringUtils.split(shardTypeUid, '/');
            Map<String,Set<String>> dataTypes = this.parentUidMap.get(parts[0]);
            if (dataTypes == null) {
                dataTypes = new HashMap<>();
                this.parentUidMap.put(parts[0], dataTypes);
            }
            Set<String> uids = dataTypes.get(parts[1]);
            if (uids == null) {
                uids = new HashSet<>();
                dataTypes.put(parts[1], uids);
            }
            uids.add(parts[2]);
        }
    }
    
    public DescendentFilterIterator deepCopy(IteratorEnvironment env) {
        return new DescendentFilterIterator(this, env);
    }
    
    /**
     * Determine if the specified uid is of the appropriate relationship to the parent uid
     * 
     * @param uid
     *            the uid
     * @param shardId
     *            the shard id
     * @param dataType
     *            the data type
     * @param parentUids
     *            the parent uids
     * @return true if accepted, false if not
     */
    protected boolean acceptUid(String shardId, String dataType, String uid, Set<String> parentUids) {
        boolean accepted = false;
        
        // determine if this uid is a parent uid
        boolean parent = parentUids.contains(uid);
        if (includeParent && parent) {
            accepted = true;
        } else {
            // determine if this uid is a descendent of a one of the parentUids
            for (String parentUid : parentUids) {
                if (uid.startsWith(parentUid + UIDConstants.DEFAULT_SEPARATOR)) {
                    // if children only, then check level
                    if (childrenOnly) {
                        // if we have a separator past the initial separator following the parent uid, then not immediate child
                        int index = uid.indexOf(UIDConstants.DEFAULT_SEPARATOR, parentUid.length() + 1);
                        if (index < 0) {
                            accepted = true;
                            break;
                        }
                    } else {
                        accepted = true;
                        break;
                    }
                }
            }
        }
        return accepted;
    }
    
    @Override
    protected void consume() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("---:> consume()");
        }
        
        // We only want to loop as long as there is more data
        while (getSource().hasTop() && prevRange.contains(getSource().getTopKey())) {
            Key topKey = getTopKey();
            this.row.set(topKey.getRow().getBytes(), 0, topKey.getRow().getLength());
            this.cf.set(topKey.getColumnFamily().getBytes(), 0, topKey.getColumnFamily().getLength());
            
            // If it's the same cf, we don't need to re-check
            if (this.row.compareTo(prevRow) == 0 && this.cf.compareTo(prevCf) == 0) {
                return;
            }
            
            int index = this.cf.find(NULL);
            
            if (log.isTraceEnabled()) {
                log.trace("---:> index = " + index);
            }
            
            // No null byte in cf ('tf' or 'd' column family)
            if (index == -1) {
                if (log.isTraceEnabled()) {
                    log.trace("---:> no null byte");
                }
                
                Range r = new Range(topKey.followingKey(PartialKey.ROW_COLFAM), prevRange.isStartKeyInclusive(), prevRange.getEndKey(),
                                prevRange.isEndKeyInclusive());
                
                if (log.isTraceEnabled()) {
                    log.trace("---:> seeking to: " + r);
                }
                
                getSource().seek(r, this.columnFamilies, this.colfInclusive);
            } else {
                // Is an event or field index key
                String prefix = Text.decode(this.cf.getBytes(), 0, index);
                if (log.isTraceEnabled()) {
                    log.trace("---:> prefix: " + prefix);
                }
                
                if (prefix.equals("fi")) {
                    if (log.isTraceEnabled()) {
                        log.trace("---:> field index key");
                    }
                    
                    // Otherwise, seek past it
                    Key k = new Key(this.row, new Text(prefix + "\1"));
                    Range r;
                    
                    if (log.isTraceEnabled()) {
                        log.trace("---:> Want to seek to: " + k);
                    }
                    
                    if (prevRange.afterEndKey(k)) {
                        // Seek to the end of the Range
                        r = new Range(prevRange.getEndKey(), true, prevRange.getEndKey(), prevRange.isEndKeyInclusive());
                    } else {
                        r = new Range(k, prevRange.isStartKeyInclusive(), prevRange.getEndKey(), prevRange.isEndKeyInclusive());
                    }
                    
                    if (log.isTraceEnabled()) {
                        log.trace("---:> seeking to: " + r);
                    }
                    
                    getSource().seek(r, this.columnFamilies, this.colfInclusive);
                } else {
                    // Is an event or field index key
                    String shardId = this.row.toString();
                    String dataType = Text.decode(this.cf.getBytes(), 0, index);
                    String uid = Text.decode(this.cf.getBytes(), index + 1, this.cf.getLength() - (index + 1));
                    
                    if (parentUidMap.containsKey(shardId) && parentUidMap.get(shardId).containsKey(dataType)
                                    && acceptUid(shardId, dataType, uid, parentUidMap.get(shardId).get(dataType))) {
                        if (log.isTraceEnabled()) {
                            log.trace("---:> accepting key");
                        }
                        
                        prevRow.set(this.row.getBytes(), 0, this.row.getLength());
                        prevCf.set(this.cf.getBytes(), 0, this.cf.getLength());
                        
                        return;
                    } else {
                        if (log.isTraceEnabled()) {
                            if (childrenOnly) {
                                log.trace("---:> Not a child uid: " + uid);
                            } else {
                                log.trace("---:> Not a descendent uid: " + uid);
                            }
                        }
                        
                        // Otherwise, seek past it
                        Text followingCf = new Text(cf);
                        followingCf.append(UID_SEPARATOR_BYTES, 0, UID_SEPARATOR_BYTES.length);
                        Key k = new Key(this.row, followingCf);
                        Range r;
                        
                        if (log.isTraceEnabled()) {
                            log.trace("---:> Want to seek to: " + k);
                        }
                        
                        if (prevRange.afterEndKey(k)) {
                            // Seek to the end of the Range
                            r = new Range(prevRange.getEndKey(), true, prevRange.getEndKey(), prevRange.isEndKeyInclusive());
                        } else {
                            r = new Range(k, prevRange.isStartKeyInclusive(), prevRange.getEndKey(), prevRange.isEndKeyInclusive());
                        }
                        
                        if (log.isTraceEnabled()) {
                            log.trace("---:> seeking to: " + r);
                        }
                        
                        getSource().seek(r, this.columnFamilies, this.colfInclusive);
                    }
                }
            }
        }
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        validateOptions(options);
        
        super.init(source, options, env);
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.prevRange = range;
        this.columnFamilies = columnFamilies;
        this.colfInclusive = inclusive;
        
        super.seek(range, columnFamilies, inclusive);
    }
    
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        
        options.put(DescendentFilterIterator.PARENT_UIDS, "parent uids for which to pass through descendents thereof");
        options.put(DescendentFilterIterator.CHILDREN_ONLY, "if set then only immediate children are passed through (default is false)");
        options.put(DescendentFilterIterator.INCLUDE_PARENT, "if set then the parent keyars are included (default is false)");
        
        return new IteratorOptions(getClass().getSimpleName(), "Filters out all keys except for descendents of the parent uid", options, null);
        
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (!options.containsKey(PARENT_UIDS) || null == options.get(PARENT_UIDS)) {
            log.error("Options did not contain: " + PARENT_UIDS);
            return false;
        }
        
        setParentUids(options.get(PARENT_UIDS));
        
        if (options.containsKey(CHILDREN_ONLY) && null != options.get(CHILDREN_ONLY)) {
            this.childrenOnly = Boolean.parseBoolean(options.get(CHILDREN_ONLY));
        }
        
        if (options.containsKey(INCLUDE_PARENT) && null != options.get(INCLUDE_PARENT)) {
            this.includeParent = Boolean.parseBoolean(options.get(INCLUDE_PARENT));
        }
        
        if (log.isDebugEnabled()) {
            StringBuilder buffer = new StringBuilder();
            buffer.append("Configured to pull back ");
            if (this.includeParent) {
                buffer.append("event and ");
            }
            if (this.childrenOnly) {
                buffer.append("immediate children of ");
            } else {
                buffer.append("all descendents of ");
            }
            buffer.append(this.parentUids);
            log.debug(buffer.toString());
        }
        
        return true;
    }
}
