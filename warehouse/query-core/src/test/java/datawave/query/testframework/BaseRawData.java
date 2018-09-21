package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;
import datawave.data.normalizer.NumberNormalizer;
import datawave.ingest.data.config.NormalizedContentInterface;
import org.apache.log4j.Logger;
import org.bouncycastle.util.Strings;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a single entry of raw data read from an ingest file. This is a generic POJO that can be used for any data entry. This class is immutable.
 */
public abstract class BaseRawData implements RawData {
    private static final Logger log = Logger.getLogger(BaseRawData.class);
    
    /**
     * Field name for the datatype for each entry. This field is added to every event.
     */
    public static final String EVENT_DATATYPE = Normalizer.LC_NO_DIACRITICS_NORMALIZER.normalize("EVENT_DATATYPE");
    
    protected static final RawMetaData DATATYPE_METADATA = new RawMetaData(EVENT_DATATYPE, Normalizer.LC_NO_DIACRITICS_NORMALIZER, false);
    
    // =============================
    // instance members
    /** mapping of field to data for each entry */
    protected final Map<String,Set<String>> entry = new HashMap<>();
    
    // needed for some extending classes
    public BaseRawData() {}
    
    /**
     * Creates a POJO for a raw data entry. The values in the <code>fields</code> must match the corresponding entries specified in the <code>headers</code>.
     * The fields and values will be normalized.
     *
     * @param fields
     *            raw data fields
     */
    public BaseRawData(final String datatype, final String[] fields) {
        processFields(datatype, fields);
    }
    
    public void processFields(final String datatype, final String[] fields) {
        // add event datatype entry to event
        // this will be used when filtering by datatype
        final Set<String> eventDt = new HashSet<>();
        eventDt.add(DATATYPE_METADATA.normalizer.normalize(datatype));
        this.entry.put(EVENT_DATATYPE, eventDt);
        
        // add each header event
        final List<String> hdrs = getHeaders();
        // ensure headers match field input
        Assert.assertEquals(hdrs.size(), fields.length);
        
        for (int n = 0; n < hdrs.size(); n++) {
            String header = hdrs.get(n);
            final Normalizer<?> norm = getNormalizer(header);
            final Set<String> values = new HashSet<>();
            // convert multi-value fields into a set of values
            if (isMultiValueField(header)) {
                String[] multi = Strings.split(fields[n], RawDataManager.MULTIVALUE_SEP_CHAR);
                for (String s : multi) {
                    if (norm instanceof NumberNormalizer) {
                        values.add(s);
                    } else {
                        values.add(norm.normalize(s));
                    }
                }
            } else if (isTokenizedField(header)) {
                // convert field to a list of tokens that include the complete field
                String[] multi = Strings.split(fields[n], ' ');
                // add full field as an entry
                values.add(fields[n]);
                for (String s : multi) {
                    if (norm instanceof NumberNormalizer) {
                        values.add(s);
                    } else {
                        values.add(norm.normalize(s));
                    }
                }
            } else {
                if (norm instanceof NumberNormalizer) {
                    values.add(fields[n]);
                } else {
                    values.add(norm.normalize(fields[n]));
                }
            }
            String key = getKey(header).toLowerCase();
            if (this.entry.containsKey(key)) {
                Set<String> curr = this.entry.get(key);
                curr.addAll(values);
            } else {
                this.entry.put(key, values);
            }
        }
    }
    
    /**
     * Converts data into a raw data entry.
     * 
     * @param datatype
     *            datatype for data
     * @param fields
     *            mapping of fields to a collection of values
     */
    protected void processNormalizedContent(final String datatype, Map<String,Collection<NormalizedContentInterface>> fields) {
        // add event datatype entry to event
        // this will be used when filtering by datatype
        final Set<String> eventDt = new HashSet<>();
        eventDt.add(DATATYPE_METADATA.normalizer.normalize(datatype));
        this.entry.put(EVENT_DATATYPE, eventDt);
        
        for (Map.Entry<String,Collection<NormalizedContentInterface>> fld : fields.entrySet()) {
            final Normalizer<?> norm = getNormalizer(fld.getKey());
            final Set<String> values = new HashSet<>();
            for (final NormalizedContentInterface normValue : fld.getValue()) {
                if (norm instanceof NumberNormalizer) {
                    values.add(normValue.getEventFieldValue());
                } else {
                    values.add(norm.normalize(normValue.getEventFieldValue()));
                }
            }
            this.entry.put(fld.getKey().toLowerCase(), values);
        }
    }
    
    void processMapFormat(final String datatype, Map<String,Collection<String>> data) {
        // add event datatype entry to event
        // this will be used when filtering by datatype
        final Set<String> eventDt = new HashSet<>();
        eventDt.add(DATATYPE_METADATA.normalizer.normalize(datatype));
        this.entry.put(EVENT_DATATYPE, eventDt);
        
        for (Map.Entry<String,Collection<String>> entry : data.entrySet()) {
            Normalizer<?> norm = getNormalizer(entry.getKey());
            Set<String> fieldVals = new HashSet<>();
            for (String val : entry.getValue()) {
                if (norm instanceof NumberNormalizer) {
                    fieldVals.add(val);
                } else {
                    fieldVals.add(norm.normalize(val));
                }
            }
            this.entry.put(entry.getKey().toLowerCase(), fieldVals);
        }
    }
    
    // ================================
    // implemented interface methods
    
    @Override
    public Set<Map<String,String>> getMapping() {
        // convert multi-value fields into separate entries
        // each multi value entry will create N number of additional entries
        final Set<Map<String,String>> expanded = new HashSet<>();
        Map<String,String> current = new HashMap<>();
        expanded.add(current);
        for (final Map.Entry<String,Set<String>> entry : this.entry.entrySet()) {
            final List<String> values = new ArrayList<>(entry.getValue());
            String val = values.get(0);
            // add field to all current entries
            for (final Map<String,String> add : expanded) {
                add.put(entry.getKey(), val);
            }
            values.remove(val);
            if (!values.isEmpty()) {
                // create a duplicate of the current entries and reset current key for each value
                // thus ("a", "b") would duplicate all of the existing entries and overwrite the value
                final Set<Map<String,String>> dup = new HashSet<>(expanded);
                final Set<Map<String,String>> multiAdd = new HashSet<>();
                for (final String multi : values) {
                    for (final Map<String,String> me : dup) {
                        Map<String,String> copy = new HashMap<>(me);
                        copy.put(entry.getKey(), multi);
                        multiAdd.add(copy);
                    }
                }
                expanded.addAll(multiAdd);
            }
        }
        
        return expanded;
    }
    
    @Override
    public String getValue(final String field) {
        Set<String> val = this.entry.get(field);
        return (val == null ? null : val.iterator().next());
    }
    
    @Override
    public Set<String> getAllValues(final String field) {
        return this.entry.get(field);
    }
    
    @Override
    public String getKey(String field) {
        // by default the query field is the same as the header field
        return field;
    }
    
    @Override
    public boolean isTokenizedField(String field) {
        // by default all fields are not tokenized
        return false;
    }
    
    // ================================
    // abstract methods
    abstract protected List<String> getHeaders();
    
    abstract protected boolean containsField(String field);
    
    abstract protected Normalizer<?> getNormalizer(String field);
    
    // ================================
    // base override methods
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        BaseRawData data = (BaseRawData) o;
        return Objects.equals(entry, data.entry);
    }
    
    @Override
    public int hashCode() {
        return this.entry.hashCode();
    }
    
    /**
     * Defines the metadata for an individual field.
     */
    public static class RawMetaData {
        final String name;
        final Normalizer normalizer;
        final boolean multiValue;
        
        public RawMetaData(final String fieldName, final Normalizer norm, final boolean multi) {
            this.name = fieldName;
            this.normalizer = norm;
            this.multiValue = multi;
        }
        
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder(this.getClass().getSimpleName());
            buf.append(": name(").append(this.name).append(") normailizer(");
            buf.append(this.normalizer.getClass().getSimpleName()).append(") multi(");
            buf.append(this.multiValue).append(")");
            return buf.toString();
        }
    }
}
