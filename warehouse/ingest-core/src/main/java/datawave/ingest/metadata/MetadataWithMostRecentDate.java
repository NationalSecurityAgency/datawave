package datawave.ingest.metadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.google.common.base.Objects;

public class MetadataWithMostRecentDate {
    public static final String IGNORED_NORMALIZER_CLASS = null;
    private final Text columnFamily;
    private Map<String,MostRecentEventDateAndKeyComponents> mostRecentDates = new HashMap<>();

    public MetadataWithMostRecentDate(Text columnFamily) {
        this.columnFamily = columnFamily;
    }

    private static String createKey(String fieldName, String dataTypeOutputName, String normalizerClassName) {
        String key = fieldName + dataTypeOutputName;
        if (null != normalizerClassName) {
            key += normalizerClassName;
        }
        return key;
    }

    public void createOrUpdate(String fieldName, String dataTypeOutputName, String normalizerClassName, long eventDate) {
        String identifier = createKey(fieldName, dataTypeOutputName, normalizerClassName);
        MostRecentEventDateAndKeyComponents value = mostRecentDates.get(identifier);
        if (null == value) {
            mostRecentDates.put(identifier, new MostRecentEventDateAndKeyComponents(fieldName, dataTypeOutputName, normalizerClassName, eventDate));
        } else if (eventDate > value.getMostRecentDate()) {
            value.setMostRecentDate(eventDate);
        }
    }

    public Collection<MostRecentEventDateAndKeyComponents> entries() {
        return mostRecentDates.values();
    }

    public void clear() {
        mostRecentDates.clear();
    }

    public Text getColumnFamily() {
        return columnFamily;
    }

    class MostRecentEventDateAndKeyComponents {
        private final String fieldName;
        private final String dataTypeOutputName;
        private final String normalizerClassName;
        private long mostRecentDate;

        public MostRecentEventDateAndKeyComponents(String fieldName, String dataTypeOutputName, String normalizerClassName, long eventDate) {
            this.fieldName = fieldName;
            this.dataTypeOutputName = dataTypeOutputName;
            this.normalizerClassName = normalizerClassName;
            this.mostRecentDate = eventDate;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String getDataType() {
            return dataTypeOutputName;
        }

        public String getNormalizerClassName() {
            return normalizerClassName;
        }

        public long getMostRecentDate() {
            return mostRecentDate;
        }

        public void setMostRecentDate(long dateToUse) {
            this.mostRecentDate = dateToUse;
        }

        @Override
        public int hashCode() {
            int hashcode = (fieldName + dataTypeOutputName + (null != normalizerClassName ? normalizerClassName : "")).hashCode();
            hashcode += (int) (mostRecentDate ^ (mostRecentDate >>> 32));
            return hashcode;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof MostRecentEventDateAndKeyComponents)) {
                return false;
            }
            MostRecentEventDateAndKeyComponents other = (MostRecentEventDateAndKeyComponents) o;
            return Objects.equal(dataTypeOutputName, other.dataTypeOutputName) && Objects.equal(fieldName, other.fieldName)
                            && Objects.equal(normalizerClassName, other.normalizerClassName) && this.mostRecentDate == other.mostRecentDate;
        }
    }
}
