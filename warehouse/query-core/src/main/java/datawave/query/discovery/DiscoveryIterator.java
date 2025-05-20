package datawave.query.discovery;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.query.Constants;

public class DiscoveryIterator implements SortedKeyValueIterator<Key,Value> {

    private static final Logger log = Logger.getLogger(DiscoveryIterator.class);
    private static final MarkingFunctions markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();

    private Key key;
    private Value value;
    private SortedKeyValueIterator<Key,Value> iterator;
    private boolean separateCountsByColVis = false;
    private boolean showReferenceCount = false;
    private boolean reverseIndex = false;
    private boolean sumCounts = false;

    @Override
    public DiscoveryIterator deepCopy(IteratorEnvironment env) {
        DiscoveryIterator copy = new DiscoveryIterator();
        copy.iterator = iterator.deepCopy(env);
        return copy;
    }

    @Override
    public void next() throws IOException {
        this.key = null;
        this.value = null;

        while (iterator.hasTop() && key == null) {
            // Get the entries to aggregate.
            Multimap<String,TermEntry> terms = getTermsByDatatype();
            if (terms.isEmpty()) {
                log.trace("Couldn't aggregate index info; moving onto next date/field/term if data is available.");
            } else {
                // Aggregate the entries.
                List<DiscoveredThing> things = terms.asMap().values().stream().map(this::aggregate).filter(Objects::nonNull).collect(Collectors.toList());
                // Establish the next top of this iterator.
                if (!things.isEmpty()) {
                    setTop(things);
                    return;
                }
            }
        }
        log.trace("No data found.");
    }

    /**
     * Return a multimap containing mappings of datatypes to term entries that should be aggregated.
     */
    private Multimap<String,TermEntry> getTermsByDatatype() throws IOException {
        Multimap<String,TermEntry> terms = ArrayListMultimap.create();
        Key start = new Key(iterator.getTopKey());
        Key key;
        // If we should sum up counts, we want to collect the term entries for each date seen for the current field and term of start. Otherwise, we only want
        // to collect the term entries for the current field, term, and date of start.
        BiFunction<Key,Key,Boolean> dateMatchingFunction = sumCounts ? (first, second) -> true : this::datesMatch;
        // Find all matching entries and parse term entries from them.
        while (iterator.hasTop() && start.equals((key = iterator.getTopKey()), PartialKey.ROW_COLFAM) && dateMatchingFunction.apply(start, key)) {
            TermEntry termEntry = new TermEntry(key, iterator.getTopValue());
            if (termEntry.isValid())
                terms.put(termEntry.getDatatype(), termEntry);
            else {
                if (log.isTraceEnabled()) {
                    log.trace("Received invalid term entry from key: " + key);
                }
            }
            iterator.next();
        }
        return terms;
    }

    /**
     * Return true if the dates for the two keys match, or false otherwise.
     */
    private boolean datesMatch(Key left, Key right) {
        ByteSequence leftBytes = left.getColumnQualifierData();
        ByteSequence rightBytes = right.getColumnQualifierData();
        for (int i = 0; i < 8; i++) {
            if (leftBytes.byteAt(i) != rightBytes.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return the given term entries aggregated into a single {@link DiscoveredThing} if possible, or return null if any issues occurred.
     */
    private DiscoveredThing aggregate(Collection<TermEntry> termEntries) {
        if (termEntries.isEmpty()) {
            return null;
        } else {
            TermEntry first = termEntries.iterator().next();
            String term = reverseIndex ? new StringBuilder(first.getTerm()).reverse().toString() : first.getTerm();
            String date = sumCounts ? "" : first.date;

            Set<ColumnVisibility> visibilities = new HashSet<>();
            Map<String,Long> visibilityToCounts = new HashMap<>();
            long count = 0L;

            // Aggregate the counts and visibilities from each entry.
            for (TermEntry termEntry : termEntries) {
                // Fetch the count to aggregate based of whether we should show the term count or the reference count.
                long currentCount = this.showReferenceCount ? termEntry.getUidListSize() : termEntry.getUidCount();
                try {
                    // Track the distinct visibilities seen.
                    visibilities.add(termEntry.getVisibility());
                    // If counts by visibility should be tracked, do so.
                    if (this.separateCountsByColVis) {
                        String visibility = new String(termEntry.getVisibility().flatten());
                        visibilityToCounts.compute(visibility, (k, v) -> v == null ? currentCount : v + currentCount);
                    }
                } catch (Exception e) {
                    // If an exception occurs, skip to the next entry.
                    log.trace(e);
                    continue;
                }
                // Increment the overall count.
                count += currentCount;
            }

            // If we do not have a count greater than 0, return null.
            if (count <= 0) {
                if (log.isTraceEnabled()) {
                    log.trace("Did not aggregate any counts for [" + first.getTerm() + "][" + first.getField() + "][" + first.getDatatype() + "]["
                                    + first.getDate() + "]. Returning null");
                }
                return null;
            } else {
                // Otherwise, combine the visibilities, and return the aggregated result.
                try {
                    ColumnVisibility visibility = markingFunctions.combine(visibilities);
                    MapWritable countsByVis = new MapWritable();
                    visibilityToCounts.forEach((key, value) -> countsByVis.put(new Text(key), new LongWritable(value)));
                    return new DiscoveredThing(term, first.getField(), first.getDatatype(), date, new String(visibility.flatten()), count, countsByVis);
                } catch (Exception e) {
                    if (log.isTraceEnabled()) {
                        log.warn("Invalid column visibilities after combining " + visibilities);
                    }
                    return null;
                }
            }
        }
    }

    /**
     * Set the top {@link Key} and {@link Value} of this iterator, created from the given list of {@link DiscoveredThing} instances.
     */
    private void setTop(List<DiscoveredThing> things) {
        // We want the key to be the last possible key for this date. Return the key as it is in the index (reversed if necessary) to ensure the keys are
        // consistent with the initial seek range.
        DiscoveredThing thing = things.get(0);
        String row = (this.reverseIndex ? new StringBuilder().append(thing.getTerm()).reverse().toString() : thing.getTerm());
        Key newKey = new Key(row, thing.getField(), thing.getDate() + "\uffff");

        // Create a value from the list of things.
        ArrayWritable thingArray = new ArrayWritable(DiscoveredThing.class, things.toArray(new DiscoveredThing[0]));
        Value newValue = new Value(WritableUtils.toByteArray(thingArray));

        this.key = newKey;
        this.value = newValue;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.iterator.seek(range, columnFamilies, inclusive);
        if (log.isTraceEnabled()) {
            log.trace("My source " + (this.iterator.hasTop() ? "does" : "does not") + " have a top.");
        }
        next();
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.iterator = source;
        this.separateCountsByColVis = Boolean.parseBoolean(options.get(DiscoveryLogic.SEPARATE_COUNTS_BY_COLVIS));
        this.showReferenceCount = Boolean.parseBoolean(options.get(DiscoveryLogic.SHOW_REFERENCE_COUNT));
        this.reverseIndex = Boolean.parseBoolean(options.get(DiscoveryLogic.REVERSE_INDEX));
        this.sumCounts = Boolean.parseBoolean(options.get(DiscoveryLogic.SUM_COUNTS));

        if (log.isTraceEnabled()) {
            log.trace("Source: " + source.getClass().getName());
            log.trace("Separate counts by column visibility: " + this.separateCountsByColVis);
            log.trace("Show reference counts only: " + this.showReferenceCount);
            log.trace("Reverse index: " + this.reverseIndex);
            log.trace("Sum counts: " + this.sumCounts);
        }
    }

    @Override
    public boolean hasTop() {
        return key != null;
    }

    @Override
    public Key getTopKey() {
        return key;
    }

    @Override
    public Value getTopValue() {
        return value;
    }

    /**
     * Represents term information parsed from a {@link Key}, {@link Value} pair.
     */
    private static class TermEntry {

        private final String term;
        private final String field;
        private String date;
        private String datatype;
        private ColumnVisibility visibility;
        private long uidCount;
        private long uidListSize;
        private boolean valid;

        public TermEntry(Key key, Value value) {
            term = key.getRow().toString();
            field = key.getColumnFamily().toString();

            String colq = key.getColumnQualifier().toString();
            int firstSeparatorPos = colq.indexOf(Constants.NULL_BYTE_STRING);
            if (firstSeparatorPos != -1) {
                int lastSeparatorPos = colq.lastIndexOf(Constants.NULL_BYTE_STRING);
                // If multiple separators are present, this is a task datatype entry.
                if (firstSeparatorPos != lastSeparatorPos) {
                    // Ensure that we at least have yyyyMMdd.
                    if ((lastSeparatorPos - firstSeparatorPos) < 9) {
                        return;
                    }
                    // The form is datatype\0date\0task status (old knowledge entry).
                    date = colq.substring(firstSeparatorPos + 1, firstSeparatorPos + 9);
                    datatype = colq.substring(0, firstSeparatorPos);
                } else {
                    // Ensure that we at least have yyyyMMdd.
                    if (firstSeparatorPos < 8) {
                        return;
                    }
                    // The form is shardId\0datatype.
                    date = colq.substring(0, 8);
                    datatype = colq.substring(firstSeparatorPos + 1);
                }

                // Parse the UID.List object from the value.
                try {
                    Uid.List uidList = Uid.List.parseFrom(value.get());
                    if (uidList != null) {
                        uidCount = uidList.getCOUNT();
                        uidListSize = uidList.getUIDList().size();
                    }
                } catch (InvalidProtocolBufferException e) {
                    // Don't add UID information. At least we know what shard it's located in.
                }

                visibility = new ColumnVisibility(key.getColumnVisibility());

                // This is now considered a valid entry for aggregation.
                valid = true;
            }
        }

        public String getTerm() {
            return term;
        }

        public String getField() {
            return field;
        }

        public String getDate() {
            return date;
        }

        public String getDatatype() {
            return datatype;
        }

        public ColumnVisibility getVisibility() {
            return visibility;
        }

        public long getUidCount() {
            return uidCount;
        }

        public long getUidListSize() {
            return uidListSize;
        }

        public boolean isValid() {
            return valid;
        }
    }
}
