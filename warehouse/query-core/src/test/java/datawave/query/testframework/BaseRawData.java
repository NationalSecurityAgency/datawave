package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;
import datawave.data.normalizer.NumberNormalizer;
import org.apache.log4j.Logger;
import org.bouncycastle.util.Strings;

import java.util.ArrayList;
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
    
    // =============================
    // instance members
    /** mapping of field to data for each entry */
    private final Map<String,Set<String>> entry;
    
    /**
     * Creates a POJO for a raw data entry. The values in the <code>fields</code> must match the corresponding entries specified in the <code>headers</code>.
     * The fields and values will be normalized.
     *
     * @param fields
     *            raw data fields
     */
    public BaseRawData(final String[] fields) {
        this.entry = new HashMap<>();
        for (int n = 0; n < getHeaders().size(); n++) {
            String header = getHeaders().get(n);
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
            } else {
                if (norm instanceof NumberNormalizer) {
                    values.add(fields[n]);
                } else {
                    values.add(norm.normalize(fields[n]));
                }
            }
            this.entry.put(header.toLowerCase(), values);
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
        
        RawMetaData(final String fieldName, final Normalizer norm, final boolean multi) {
            this.name = fieldName;
            this.normalizer = norm;
            this.multiValue = multi;
        }
    }
}
