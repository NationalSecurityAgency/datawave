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
 * Represents a single event of raw data read from an ingest file. This is a generic POJO that can be used for any data event. This class is immutable.
 */
public abstract class BaseRawData implements RawData {
    private static final Logger log = Logger.getLogger(BaseRawData.class);
    
    /**
     * Field name for the datatype for each event. This field is added to every event.
     */
    public static final String EVENT_DATATYPE = Normalizer.LC_NO_DIACRITICS_NORMALIZER.normalize("EVENT_DATATYPE");
    
    private static final RawMetaData DATATYPE_METADATA = new RawMetaData(EVENT_DATATYPE, Normalizer.LC_NO_DIACRITICS_NORMALIZER, false);
    
    // =============================
    // instance members
    /** mapping of field to data for each event */
    protected final Map<String,Set<String>> event = new HashMap<>();
    
    protected final Map<String,RawMetaData> metaDataMap;
    protected final List<String> headers;
    
    /**
     *
     * @param datatype
     *            name of datatype
     * @param fieldHeaders
     *            ordered list of field names
     * @param metadata
     *            field metadata
     */
    public BaseRawData(final String datatype, final List<String> fieldHeaders, final Map<String,RawMetaData> metadata) {
        this.metaDataMap = metadata;
        this.headers = fieldHeaders;
        // may already have event datatype added
        if (!this.metaDataMap.containsKey(EVENT_DATATYPE)) {
            this.metaDataMap.put(EVENT_DATATYPE, DATATYPE_METADATA);
        }
        
        // add event datatype to event data
        // this will be used when filtering by datatype
        final Set<String> eventDt = new HashSet<>();
        eventDt.add(DATATYPE_METADATA.normalizer.normalize(datatype));
        this.event.put(EVENT_DATATYPE, eventDt);
    }
    
    /**
     * Creates a POJO for a raw data event. The values in the <code>fields</code> must match the corresponding entries specified in the <code>headers</code>.
     * The fields and values will be normalized.
     *
     * @param fields
     *            raw data fields
     * @param fieldHeaders
     *            ordered list of field names
     * @param metadata
     *            field metadata
     * */
    public BaseRawData(final String datatype, final String[] fields, final List<String> fieldHeaders, final Map<String,RawMetaData> metadata) {
        this(datatype, fieldHeaders, metadata);
        processFields(datatype, fields);
    }
    
    public void processFields(final String datatype, final String[] fields) {
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
                // add full field as an event
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
            if (this.event.containsKey(key)) {
                Set<String> curr = this.event.get(key);
                curr.addAll(values);
            } else {
                this.event.put(key, values);
            }
        }
    }
    
    /**
     * Converts data from a {@link NormalizedContentInterface} into a raw data entry.
     *
     * @param datatype
     *            datatype for data
     * @param fields
     *            mapping of fields to a collection of normalized values
     */
    protected void processNormalizedContent(final String datatype, Map<String,Collection<NormalizedContentInterface>> fields) {
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
            this.event.put(fld.getKey().toLowerCase(), values);
        }
    }
    
    /**
     * Converts a map of fields to values to a raw entry.
     *
     * @param datatype
     *            datatype name
     * @param data
     *            map of field to values
     */
    void processMapFormat(final String datatype, Map<String,Collection<String>> data) {
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
            this.event.put(entry.getKey().toLowerCase(), fieldVals);
        }
    }
    
    // ================================
    // implemented interface methods
    
    @Override
    public Set<Map<String,String>> getMapping() {
        // convert multi-value fields into separate entries
        // each multi value event will create N number of additional entries
        final Set<Map<String,String>> expanded = new HashSet<>();
        Map<String,String> current = new HashMap<>();
        expanded.add(current);
        for (final Map.Entry<String,Set<String>> entry : this.event.entrySet()) {
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
        Set<String> val = this.event.get(field);
        return (val == null ? null : val.iterator().next());
    }
    
    @Override
    public Set<String> getAllValues(final String field) {
        return this.event.get(field);
    }
    
    @Override
    public String getKey(String field) {
        // by default the query field is the same as the header field
        // some datatypes may alter the query field
        return field;
    }
    
    @Override
    public List<String> getHeaders() {
        return this.headers;
    }
    
    @Override
    public boolean containsField(String field) {
        return this.metaDataMap.keySet().contains(field.toLowerCase());
    }
    
    @Override
    public boolean isTokenizedField(String field) {
        // by default all fields are not tokenized - see groups data
        return false;
    }
    
    @Override
    public boolean isMultiValueField(final String field) {
        return this.metaDataMap.get(field.toLowerCase()).multiValue;
    }
    
    @Override
    public Normalizer<?> getNormalizer(String field) {
        Assert.assertTrue(containsField(field));
        return this.metaDataMap.get(field.toLowerCase()).normalizer;
    }
    
    // ================================
    // base override methods
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        BaseRawData data = (BaseRawData) o;
        return Objects.equals(event, data.event);
    }
    
    @Override
    public int hashCode() {
        return this.event.hashCode();
    }
}
