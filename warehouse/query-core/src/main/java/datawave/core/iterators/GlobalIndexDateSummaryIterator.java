package datawave.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;
import datawave.ingest.protobuf.Uid.List.Builder;
import datawave.marking.MarkingFunctions;
import datawave.query.Constants;

/**
 * <p>
 * Iterator used for global index lookups in ShardIndexQueryTable. This iterator will aggregate information by date for a fieldValue, fieldName, and datatype.
 * If there are multiple sets of column visibilities for a given combination, then the count appears for the combined visibilities. This iterator is set up in
 * the ShardIndexQueryTable and is constructed with a source iterator which is already filtering the requested datatypes (GlobalIndexDataTypeFilter), term(s)
 * (GlobalIndexTermMatchingFilter), and date range (GlobalIndexDateRangeFilter) as desired by the client.
 * </p>
 */
public class GlobalIndexDateSummaryIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    protected static final Logger log = Logger.getLogger(GlobalIndexDateSummaryIterator.class);
    protected SortedKeyValueIterator<Key,Value> iterator;
    protected Key returnKey = null;
    protected Value returnValue = null;
    protected SortedMap<Key,Value> returnCache = new TreeMap<>();
    protected Set<ColumnVisibility> columnVisibilities = Sets.newHashSet();

    public GlobalIndexDateSummaryIterator() {}

    public GlobalIndexDateSummaryIterator(GlobalIndexDateSummaryIterator iter, IteratorEnvironment env) {
        this();
        this.iterator = iter.iterator.deepCopy(env);
        this.returnCache.putAll(iter.returnCache);
    }

    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new GlobalIndexDateSummaryIterator(this, env);
    }

    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (!validateOptions(options))
            throw new IOException("Iterator options are not correct");
        this.iterator = source;
    }

    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        return new IteratorOptions(getClass().getSimpleName(), "returns global index keys aggregating shard_ids into a single date", options, null);
    }

    public boolean validateOptions(Map<String,String> options) {
        return true;
    }

    public boolean hasTop() {
        boolean hasTop = (returnValue != null);
        if (log.isDebugEnabled()) {
            log.debug("hasTop: " + hasTop);
        }
        return hasTop;
    }

    public Key getTopKey() {
        return returnKey;
    }

    public Value getTopValue() {
        return returnValue;
    }

    public void next() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("next called");
        }

        returnKey = null;
        returnValue = null;

        // ensure we have something if there is anything to get
        findTop();

        // if we got something, then stage the first one to return
        if (!returnCache.isEmpty()) {
            returnKey = returnCache.firstKey();
            returnValue = returnCache.remove(returnKey);
        }
    }

    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("seek called: " + range);
        }

        this.iterator.seek(range, columnFamilies, inclusive);
        next();
    }

    /**
     * This method aggregates all information from the global index for a term for one day, type, and fieldname.
     *
     * @throws IOException
     *             for issues with read/write
     */
    protected void findTop() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("findTop called");
        }

        // if we already have something cached, then simply return
        if (!returnCache.isEmpty()) {
            return;
        }

        // Get the next valid term info
        TermInfo termInfo = getNextValidTermInfo();

        // while we have a term info but nothing in the cache
        while (termInfo != null && returnCache.isEmpty()) {
            // start a summary
            TermInfoSummary summary = new TermInfoSummary(termInfo.fieldValue, termInfo.fieldName, termInfo.date);
            summary.addTermInfo(termInfo);

            // get the next valid term info
            this.iterator.next();
            termInfo = getNextValidTermInfo();
            while (termInfo != null && summary.isCompatible(termInfo)) {
                summary.addTermInfo(termInfo);
                this.iterator.next();
                termInfo = getNextValidTermInfo();
            }

            // now turn the summary into a set of key, value pairs
            returnCache.putAll(summary.getKeyValues());

            // if we did not get any key values out of that summary, then loop on the next term info
            if (returnCache.isEmpty()) {
                termInfo = getNextValidTermInfo();
            }
        }
    }

    /**
     * Get the next TermInfo.valid term info.
     *
     * @return The next valid term info.
     * @throws IOException
     *             for issues with read/write
     */
    protected TermInfo getNextValidTermInfo() throws IOException {
        while (this.iterator.hasTop()) {
            TermInfo info = getNextTermInfo();
            if (info.valid) {
                return info;
            }
            this.iterator.next();
        }
        return null;
    }

    /**
     * Get the next term info
     *
     * @return The next term info
     * @throws IOException
     *             for issues with read/write
     */
    protected TermInfo getNextTermInfo() throws IOException {
        return new TermInfo(this.iterator.getTopKey(), this.iterator.getTopValue());
    }

    /**
     * This class is used to summarize the info for a given fieldname
     *
     *
     *
     */
    protected static class TermInfoSummary {
        private String fieldValue = null;
        private String fieldName = null;
        private String date = null;
        private Map<String,MutableLong> summary = new HashMap<>();
        private Map<String,Set<ColumnVisibility>> columnVisibilitiesMap = Maps.newHashMap();

        public TermInfoSummary(String fieldValue, String fieldName, String date) {
            this.fieldValue = fieldValue;
            this.fieldName = fieldName;
            this.date = date;
        }

        public boolean isCompatible(TermInfo info) {
            return info.fieldValue.equals(fieldValue) && info.fieldName.equals(fieldName) && info.date.equals(date);
        }

        public void addTermInfo(TermInfo info/* , Set<ColumnVisibility> columnVisibilities */) throws IOException {
            if (!isCompatible(info)) {
                throw new IllegalArgumentException("Attempting to add term info for " + info.fieldName + "=" + info.fieldValue + ", " + info.date
                                + " to the summary for " + fieldName + "=" + fieldValue + ", " + date);
            }
            // Merge the columnVisibilities
            // Do not count the record if we can't parse its ColumnVisibility
            Set<ColumnVisibility> columnVisibilities = columnVisibilitiesMap.get(info.datatype);
            if (columnVisibilities == null) {
                columnVisibilities = Sets.newHashSet();
            }
            try {
                if (info.vis.getExpression().length != 0) {
                    columnVisibilities.add(info.vis);
                }

                MutableLong count = summary.get(info.datatype);
                if (count == null) {
                    summary.put(info.datatype, new MutableLong(info.count));
                    columnVisibilitiesMap.put(info.datatype, columnVisibilities);
                } else {
                    count.add(info.count);
                }
            } catch (Exception e) {
                // We want to stop the scan when we cannot properly combine ColumnVisibility
                String message = "Error parsing ColumnVisibility of key";
                log.error(message, e);
                throw new IOException(message, e);
            }
        }

        public Map<Key,Value> getKeyValues() throws IOException {
            Map<Key,Value> resultsMap = new HashMap<>();
            for (Entry<String,MutableLong> entry : summary.entrySet()) {
                // Key: row = fieldName, colf = datatype, colq = date
                // Value: count
                String datatype = entry.getKey();
                long count = entry.getValue().longValue();

                try {
                    // Calculate the ColumnVisibility for this key from the combiner.
                    Set<ColumnVisibility> columnVisibilities = this.columnVisibilitiesMap.get(datatype);

                    // Note that the access controls found in the combined ColumnVisibility will be pulled out appropriately here
                    ColumnVisibility cv = MarkingFunctions.Factory.createMarkingFunctions().combine(columnVisibilities);

                    // Create a new Key compatible with the shardIndex key format
                    Key k = new Key(this.fieldValue, this.fieldName, this.date + '\0' + datatype, new String(cv.getExpression()));

                    // Create a UID object with just the count for the Value
                    Builder uidBuilder = Uid.List.newBuilder();
                    uidBuilder.setIGNORE(false);
                    uidBuilder.setCOUNT(count);
                    Uid.List uidList = uidBuilder.build();
                    org.apache.accumulo.core.data.Value v = new org.apache.accumulo.core.data.Value(uidList.toByteArray());
                    resultsMap.put(k, v);
                } catch (Exception e) {
                    // We want to stop the scan when we cannot properly combine ColumnVisibility
                    String message = "Could not create combined ColumnVisibility";
                    log.error(message, e);
                    throw new IOException(message, e);
                }
            }
            return resultsMap;
        }
    }

    /**
     * This class is used to get the info for a specified global index entry
     *
     *
     *
     */
    protected static class TermInfo {
        protected long count = 0;
        protected String fieldValue = null;
        protected String fieldName = null;
        protected String date = null;
        protected String datatype = null;
        protected ColumnVisibility vis = null;
        protected boolean valid = false;

        public TermInfo(Key key, Value value) {
            // Get the shard id and datatype from the colq
            fieldValue = key.getRow().toString();
            fieldName = key.getColumnFamily().toString();
            String colq = key.getColumnQualifier().toString();

            int separator = colq.indexOf(Constants.NULL_BYTE_STRING);
            if (separator != -1) {
                int end_separator = colq.lastIndexOf(Constants.NULL_BYTE_STRING);
                // if we have multiple separators, then we must have a tasking data type entry.
                if (separator != end_separator) {
                    // ensure we at least have yyyyMMdd
                    if ((end_separator - separator) < 9) {
                        return;
                    }
                    // in this case the form is datatype\0date\0task status (old knowledge entry)
                    date = colq.substring(separator + 1, separator + 9);
                    datatype = colq.substring(0, separator);
                } else {
                    // ensure we at least have yyyyMMdd
                    if (separator < 8) {
                        return;
                    }
                    // in this case the form is shardid\0datatype
                    date = colq.substring(0, 8);
                    datatype = colq.substring(separator + 1);
                }

                // Parse the UID.List object from the value
                Uid.List uidList = null;
                try {
                    uidList = Uid.List.parseFrom(value.get());
                    if (null != uidList) {
                        count = uidList.getCOUNT();
                    }
                } catch (InvalidProtocolBufferException e) {
                    // Don't add UID information, at least we know what shard
                    // it is located in.
                }

                Text tvis = key.getColumnVisibility();
                vis = new ColumnVisibility(tvis);

                // we now have a valid info
                valid = true;
            }
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(fieldName).append(" = ").append(fieldValue).append(" : ");
            builder.append(date).append(':').append(datatype).append(" = ").append(count).append(" : ");
            builder.append(vis);
            return builder.toString();
        }
    }
}
