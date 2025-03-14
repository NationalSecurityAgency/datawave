package datawave.ingest.metadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.google.common.base.Objects;

public class MetadataWithEarliestDate {

    public static final String IGNORED_NORMALIZER_CLASS = null;
    private final Text columnFamily;
    private Map<String,Components> earliestDates = new HashMap<>();

    public MetadataWithEarliestDate(Text columnFamily) {
        this.columnFamily = columnFamily;
    }

    private static String createKey(String fieldName, String dataTypeOutputName) {
        String key = fieldName + dataTypeOutputName;
        return key;
    }

    public void createOrUpdate(String fieldName, String dataTypeOutputName, long eventDate) {
        String identifier = createKey(fieldName, dataTypeOutputName);
        Components value = earliestDates.get(identifier);
        if (null == value) {
            earliestDates.put(identifier, new Components(fieldName, dataTypeOutputName, eventDate));
        } else if (eventDate < value.getEarliestDate()) {
            value.setEarliestDate(eventDate);
        }
    }

    public Collection<Components> entries() {
        return earliestDates.values();
    }

    public void clear() {
        earliestDates.clear();
    }

    public Text getColumnFamily() {
        return columnFamily;
    }

    static class Components {
        private final String fieldName;
        private final String dataTypeOutputName;
        private long earliestDate;

        public Components(String fieldName, String dataTypeOutputName, long eventDate) {
            this.fieldName = fieldName;
            this.dataTypeOutputName = dataTypeOutputName;
            this.earliestDate = eventDate;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String getDataType() {
            return dataTypeOutputName;
        }

        public long getEarliestDate() {
            return earliestDate;
        }

        public void setEarliestDate(long dateToUse) {
            this.earliestDate = dateToUse;
        }

        @Override
        public int hashCode() {
            int hashcode = (fieldName + dataTypeOutputName).hashCode();
            hashcode += (int) (earliestDate ^ (earliestDate >>> 32));
            return hashcode;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Components)) {
                return false;
            }
            Components other = (Components) o;
            return Objects.equal(dataTypeOutputName, other.dataTypeOutputName) && Objects.equal(fieldName, other.fieldName)
                            && this.earliestDate == other.earliestDate;
        }
    }
}
