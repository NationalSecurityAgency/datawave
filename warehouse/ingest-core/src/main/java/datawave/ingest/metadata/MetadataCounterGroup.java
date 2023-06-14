package datawave.ingest.metadata;

import java.util.Collection;
import java.util.HashMap;

import com.google.common.base.Objects;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class MetadataCounterGroup {
    private static final Logger log = Logger.getLogger(MetadataCounterGroup.class);
    private final Text columnFamily;
    private HashMap<String,CountAndKeyComponents> counts = new HashMap<>();

    public MetadataCounterGroup(String groupName, Text tableName) {
        this.columnFamily = new Text(groupName + RawRecordMetadata.DELIMITER + tableName);
    }

    public MetadataCounterGroup(Text counterIdentifier) {
        this.columnFamily = counterIdentifier;
    }

    private static String createKey(String dataType, String rowid, String date) {
        return dataType + rowid + date;
    }

    /* rowId is either the fieldName or the Lac */
    public void addToCount(long countDelta, String dataType, String rowId, String date) {
        String hashMapKey = createKey(dataType, rowId, date);
        CountAndKeyComponents value = counts.get(hashMapKey);
        if (null == value) {
            counts.put(hashMapKey, new CountAndKeyComponents(dataType, rowId, date, countDelta));
        } else {
            value.incrementCount(countDelta);
        }
    }

    public void clear() {
        counts.clear();
    }

    public Text getColumnFamily() {
        return columnFamily;
    }

    public Collection<CountAndKeyComponents> getEntries() {
        return counts.values();
    }

    public class CountAndKeyComponents {
        private final String dataType;
        private final String rowId;
        private final String date;
        private long count;

        public CountAndKeyComponents(String dataType, String rowId, String date, long countDelta) {
            this.dataType = dataType;
            this.rowId = rowId;
            this.date = date;
            this.count = countDelta;
        }

        public String getDataType() {
            return dataType;
        }

        public String getDate() {
            return date;
        }

        public String getRowId() {
            return rowId;
        }

        public void incrementCount(long countDelta) {
            this.count += countDelta;
        }

        @Override
        public int hashCode() {
            int hashCode = (dataType + rowId + date).hashCode();
            hashCode = hashCode * 31 + (int) (count ^ (count >>> 32));
            return hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CountAndKeyComponents)) {
                return false;
            }
            CountAndKeyComponents other = (CountAndKeyComponents) o;
            return Objects.equal(this.dataType, other.dataType) && Objects.equal(this.rowId, other.rowId) && Objects.equal(this.date, other.date)
                            && count == other.count;
        }

        public long getCount() {
            return count;
        }
    }
}
