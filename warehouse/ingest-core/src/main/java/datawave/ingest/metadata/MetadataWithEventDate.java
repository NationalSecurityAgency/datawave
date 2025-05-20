package datawave.ingest.metadata;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.io.Text;

public class MetadataWithEventDate {

    private final Text columnFamily;
    private final Set<Components> entries = new HashSet<>();

    public MetadataWithEventDate(Text columnFamily) {
        this.columnFamily = columnFamily;
    }

    public Text getColumnFamily() {
        return columnFamily;
    }

    public void put(String fieldName, String dataTypeName, long date) {
        entries.add(new Components(fieldName, dataTypeName, date));
    }

    public void clear() {
        entries.clear();
    }

    public Set<Components> getEntries() {
        return entries;
    }

    public static class Components {
        private final String fieldName;
        private final String dataType;
        private final long date;

        public Components(String fieldName, String dataType, long date) {
            this.fieldName = fieldName;
            this.dataType = dataType;
            this.date = date;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String getDataType() {
            return dataType;
        }

        public long getDate() {
            return date;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Components that = (Components) o;
            return Objects.equals(fieldName, that.fieldName) && Objects.equals(dataType, that.dataType) && Objects.equals(date, that.date);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, dataType, date);
        }
    }
}
