package datawave.query.testframework;

import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single entry of raw data read from an ingest file. This is a generic POJO that can be used for any data entry. This class is immutable.
 */
public abstract class BaseRawData implements IRawData {
    private static final Logger log = Logger.getLogger(BaseRawData.class);
    
    // =============================
    // instance members
    private final Map<String,String> entry;
    
    /**
     * Creates a POJO for a raw data entry. The values in the <code>fields</code> must match the corresponding entries specified in the <code>headers</code>.
     *
     * @param fields
     *            raw data fields
     */
    public BaseRawData(String[] fields) {
        this.entry = new HashMap<>();
        for (int n = 0; n < getHeaders().size(); n++) {
            this.entry.put(getHeaders().get(n).toLowerCase(), fields[n]);
        }
    }
    
    // ================================
    // implemented interface methods
    @Override
    public String getValue(final String field) {
        return this.entry.get(field.toLowerCase());
    }
    
    // ================================
    // abstract methods
    abstract protected List<String> getHeaders();
    
    abstract protected boolean containsField(final String field);
    
    // ================================
    // base override methods
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        BaseRawData cityPOJO = (BaseRawData) o;
        return Objects.equals(entry, cityPOJO.entry);
    }
    
    @Override
    public int hashCode() {
        return this.entry.hashCode();
    }
    
    public static class RawMetaData {
        final String name;
        final Type type;
        final boolean multiValue;
        
        RawMetaData(final String fieldName, final Type metaType, final boolean multi) {
            this.name = fieldName;
            this.type = metaType;
            this.multiValue = multi;
        }
    }
}
