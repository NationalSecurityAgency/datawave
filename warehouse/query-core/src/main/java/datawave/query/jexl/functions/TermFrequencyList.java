package datawave.query.jexl.functions;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import datawave.ingest.protobuf.TermWeightPosition;
import org.apache.accumulo.core.data.Key;

import com.google.common.base.Predicate;
import com.google.common.collect.Multimaps;
import com.google.common.collect.TreeMultimap;

/**
 * Represents a List of integer offsets, corresponding to word positions, in a given field, for a term. This is intended to be used in the scope of a Map from
 * term to {@link TermFrequencyList}
 */
public class TermFrequencyList {
    
    /**
     * A zone is actually a combination of the zone and the event id (uid or record id)
     */
    public static class Zone implements Comparable<Zone> {
        private String eventId;
        private String zone;
        // is this field a content expansion field meaning it is used for unfielded content functions
        private boolean contentExpansionField;
        
        public Zone(String zone, boolean contentExpansionField, String eventId) {
            this.zone = zone;
            this.contentExpansionField = contentExpansionField;
            this.eventId = eventId;
        }
        
        public String getEventId() {
            return eventId;
        }
        
        public String getZone() {
            return zone;
        }
        
        public boolean isContentExpansionField() {
            return contentExpansionField;
        }
        
        @Override
        public int hashCode() {
            return eventId.hashCode() + zone.hashCode() + (contentExpansionField ? 1 : 0);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Zone) {
                Zone other = (Zone) obj;
                return zone.equals(other.getZone()) && eventId.equals(other.getEventId()) && contentExpansionField == other.contentExpansionField;
            }
            return false;
        }
        
        @Override
        public int compareTo(Zone o) {
            int comparison = zone.compareTo(o.getZone());
            if (comparison == 0) {
                comparison = eventId.compareTo(o.getEventId());
            }
            if (comparison == 0) {
                comparison = Boolean.valueOf(contentExpansionField).compareTo(Boolean.valueOf(o.isContentExpansionField()));
            }
            return comparison;
        }
        
        @Override
        public String toString() {
            return zone + '(' + eventId + ") ->" + contentExpansionField;
        }
    }
    
    /**
     * Get an event id from the term frequency key
     * 
     * @param key
     */
    public static String getEventId(Key key) {
        StringBuilder eventId = new StringBuilder();
        eventId.append(key.getRow()).append('\0');
        String cq = key.getColumnQualifier().toString();
        int index = cq.indexOf('\0');
        index = cq.indexOf('\0', index + 1);
        eventId.append(cq, 0, index);
        return eventId.toString();
    }
    
    protected final TreeMultimap<Zone,TermWeightPosition> offsetsPerField;
    
    public TermFrequencyList(TreeMultimap<Zone,TermWeightPosition> offsetsByField) {
        checkNotNull(offsetsByField);
        
        this.offsetsPerField = offsetsByField;
    }
    
    public TermFrequencyList(Entry<Zone,Iterable<TermWeightPosition>> offsetsPerField) {
        this(Collections.singleton(offsetsPerField));
    }
    
    public TermFrequencyList(Entry<Zone,Iterable<TermWeightPosition>>... offsetsPerField) {
        this(Arrays.asList(offsetsPerField));
    }
    
    public TermFrequencyList(Iterable<Entry<Zone,Iterable<TermWeightPosition>>> offsetsPerField) {
        checkNotNull(offsetsPerField);
        
        this.offsetsPerField = TreeMultimap.create();
        
        addOffsets(offsetsPerField);
    }
    
    public static TermFrequencyList merge(TermFrequencyList list1, TermFrequencyList list2) {
        TreeMultimap<Zone,TermWeightPosition> offsetsPerField = TreeMultimap.create();
        offsetsPerField.putAll(list1.offsetsPerField);
        offsetsPerField.putAll(list2.offsetsPerField);
        return new TermFrequencyList(offsetsPerField);
    }
    
    public void addOffsets(Zone field, Iterable<TermWeightPosition> offsets) {
        checkNotNull(field);
        checkNotNull(offsets);
        
        this.offsetsPerField.putAll(field, offsets);
    }
    
    public void addOffsets(Entry<Zone,Iterable<TermWeightPosition>> offsetForField) {
        checkNotNull(offsetForField);
        
        addOffsets(offsetForField.getKey(), offsetForField.getValue());
    }
    
    public void addOffsets(Iterable<Entry<Zone,Iterable<TermWeightPosition>>> offsetsForFields) {
        checkNotNull(offsetsForFields);
        
        for (Entry<Zone,Iterable<TermWeightPosition>> offsetForField : offsetsForFields) {
            addOffsets(offsetForField);
        }
    }
    
    public void addOffsets(TreeMultimap<Zone,TermWeightPosition> offsetsForFields) {
        checkNotNull(offsetsForFields);
        
        for (Zone field : offsetsForFields.keySet()) {
            addOffsets(field, offsetsForFields.get(field));
        }
    }
    
    /**
     * Return an <code>Immutable</code> copy of the entire mapping
     * 
     * @return
     */
    public TreeMultimap<Zone,TermWeightPosition> fetchOffsets() {
        return this.offsetsPerField;
    }
    
    /**
     * Return only offsets for a limited set of fields
     * 
     * @param fields
     * @return
     */
    public TreeMultimap<Zone,TermWeightPosition> fetchOffsets(Set<Zone> fields) {
        checkNotNull(fields);
        
        return (TreeMultimap<Zone,TermWeightPosition>) Multimaps.filterKeys(this.offsetsPerField, new FieldFilterPredicate(fields));
    }
    
    /**
     * Let clients ask what fields we are currently tracking.
     */
    public Set<String> fields() {
        if (this.offsetsPerField.isEmpty()) {
            return Collections.<String> emptySet();
        } else {
            Set<String> fields = new HashSet<>();
            for (Zone zone : this.offsetsPerField.keySet()) {
                fields.add(zone.getZone());
            }
            return fields;
        }
    }
    
    /**
     * Let clients ask what event ids we are currently tracking.
     */
    public Set<String> eventIds() {
        if (this.offsetsPerField.isEmpty()) {
            return Collections.<String> emptySet();
        } else {
            Set<String> eventIds = new HashSet<>();
            for (Zone zone : this.offsetsPerField.keySet()) {
                eventIds.add(zone.getEventId());
            }
            return eventIds;
        }
    }
    
    /**
     * Let clients ask what zones we are currently tracking.
     */
    public Set<Zone> zones() {
        return this.offsetsPerField.isEmpty() ? Collections.<Zone> emptySet() : Collections.unmodifiableSet(this.offsetsPerField.keySet());
    }
    
    @Override
    public String toString() {
        return this.offsetsPerField.toString();
    }
    
    @Override
    public int hashCode() {
        return this.offsetsPerField.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof TermFrequencyList) {
            TermFrequencyList other = (TermFrequencyList) o;
            
            return this.offsetsPerField.equals(other.offsetsPerField);
        }
        
        return false;
    }
    
    private class FieldFilterPredicate implements Predicate<Zone> {
        private final Set<Zone> fieldNames;
        
        public FieldFilterPredicate(Set<Zone> fieldNames) {
            this.fieldNames = fieldNames;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.google.common.base.Predicate#apply(java.lang.Object)
         */
        @Override
        public boolean apply(Zone input) {
            // Retain results which have the given field name
            return this.fieldNames.contains(input);
        }
        
    }
}
