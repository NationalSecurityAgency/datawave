package datawave.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * This class can be used to handle the selectivity entries in the metadata table.
 *
 * 1) fieldValueCount: The number of tuple instances this field/value. 2) fieldAllValueCount: The number of tuple instances of this field for all values. 3)
 * uniqueFieldAllValueCount: The number of unique values for this field. 4) totalAllFieldAllValueCount: The total number of tuple instances for all fields and
 * all values. 5) totalUniqueAllFieldAllValueCount: The number of unique tuples for all fields and all values. 6) totalUniqueAllFieldCount: The number of unique
 * fields.
 */
public class MetadataCardinalityCounts implements Serializable {
    public static String[] COUNT_HEADERS = new String[] {"fieldValueCount", "fieldAllValueCount", "uniqueFieldAllValueCount", "totalAllFieldAllValueCount",
            "totalUniqueAllFieldAllValueCount", "totalUniqueAllFieldCount"};
    
    private final String field;
    private final String value;
    private long fieldValueCount;
    private long fieldAllValueCount;
    private long uniqueFieldAllValueCount;
    private long totalAllFieldAllValueCount;
    private long totalUniqueAllFieldAllValueCount;
    private long totalUniqueAllFieldCount;
    
    public MetadataCardinalityCounts(String field, String value, long fieldValueCount, long fieldAllValueCount, long uniqueFieldAllValueCount,
                    long totalAllFieldAllValueCount, long totalUniqueAllFieldAllValueCount, long totalUniqueAllFieldCount) {
        this.field = field;
        this.value = value;
        this.fieldValueCount = fieldValueCount;
        this.fieldAllValueCount = fieldAllValueCount;
        this.uniqueFieldAllValueCount = uniqueFieldAllValueCount;
        this.totalAllFieldAllValueCount = totalAllFieldAllValueCount;
        this.totalUniqueAllFieldAllValueCount = totalUniqueAllFieldAllValueCount;
        this.totalUniqueAllFieldCount = totalUniqueAllFieldCount;
    }
    
    public MetadataCardinalityCounts(String field, String value, long[] counts) {
        this(field, value, counts[0], counts[1], counts[2], counts[3], counts[4], counts[5]);
    }
    
    public MetadataCardinalityCounts(Key key, Value value) {
        this(key.getRow().toString(), key.getColumnQualifier().toString(), getLongs(value, 6));
    }
    
    /**
     * This method can be used to merge multiple cardinality counts.
     * 
     * @param counts
     */
    public void merge(MetadataCardinalityCounts counts) {
        // average the instance counts together avoiding overflows
        fieldValueCount = fieldValueCount / 2 + counts.fieldValueCount / 2;
        fieldAllValueCount = fieldAllValueCount / 2 + counts.fieldAllValueCount / 2;
        totalAllFieldAllValueCount = totalAllFieldAllValueCount / 2 + counts.totalAllFieldAllValueCount / 2;
        
        // max the unique value/field counts
        uniqueFieldAllValueCount = Math.max(uniqueFieldAllValueCount, counts.uniqueFieldAllValueCount);
        totalUniqueAllFieldAllValueCount = Math.max(totalUniqueAllFieldAllValueCount, counts.totalUniqueAllFieldAllValueCount);
        totalUniqueAllFieldCount = Math.max(totalUniqueAllFieldCount, counts.totalUniqueAllFieldCount);
    }
    
    public String[] getCountHeaders() {
        return COUNT_HEADERS;
    }
    
    public long[] getCountArray() {
        return new long[] {fieldValueCount, fieldAllValueCount, uniqueFieldAllValueCount, totalAllFieldAllValueCount, totalUniqueAllFieldAllValueCount,
                totalUniqueAllFieldCount};
    }
    
    public Key getKey(ColumnVisibility vis, long timeStamp) {
        return new Key(field, ColumnFamilyConstants.COLF_COUNT.toString(), value, vis, timeStamp);
    }
    
    public Value getValue() {
        return new Value(getBytes(getCountArray()));
    }
    
    private static byte[] getBytes(long... values) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Kryo kryo = new Kryo();
        
        baos.reset();
        Output output = new Output(baos);
        for (long value : values) {
            kryo.writeObject(output, value);
        }
        output.close();
        return baos.toByteArray();
    }
    
    private static long[] getLongs(Value value, int expected) {
        long[] values = new long[expected];
        Kryo kryo = new Kryo();
        ByteArrayInputStream bais = new ByteArrayInputStream(value.get(), 0, value.getSize());
        Input input = new Input(bais);
        for (int i = 0; i < expected; i++) {
            values[i] = kryo.readObject(input, Long.class);
        }
        return values;
    }
    
    public String getField() {
        return field;
    }
    
    public String getFieldValue() {
        return value;
    }
    
    public long getFieldValueCount() {
        return fieldValueCount;
    }
    
    public long getFieldAllValueCount() {
        return fieldAllValueCount;
    }
    
    public long getUniqueFieldAllValueCount() {
        return uniqueFieldAllValueCount;
    }
    
    public long getTotalAllFieldAllValueCount() {
        return totalAllFieldAllValueCount;
    }
    
    public long getTotalUniqueAllFieldAllValueCount() {
        return totalUniqueAllFieldAllValueCount;
    }
    
    public long getTotalUniqueAllFieldCount() {
        return totalUniqueAllFieldCount;
    }
    
    @Override
    public int hashCode() {
        return field.hashCode() + value.hashCode() + new Long(fieldValueCount).hashCode() + new Long(fieldAllValueCount).hashCode()
                        + new Long(uniqueFieldAllValueCount).hashCode() + new Long(totalAllFieldAllValueCount).hashCode()
                        + new Long(totalUniqueAllFieldAllValueCount).hashCode() + new Long(totalUniqueAllFieldCount).hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MetadataCardinalityCounts) {
            MetadataCardinalityCounts other = (MetadataCardinalityCounts) obj;
            return value.equals(other.value) && field.equals(other.field) && fieldValueCount == other.fieldValueCount
                            && fieldAllValueCount == other.fieldAllValueCount && uniqueFieldAllValueCount == other.uniqueFieldAllValueCount
                            && totalAllFieldAllValueCount == other.totalAllFieldAllValueCount && totalUniqueAllFieldCount == other.totalUniqueAllFieldCount
                            && totalUniqueAllFieldAllValueCount == other.totalUniqueAllFieldAllValueCount;
        }
        return false;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.field).append(" / ").append(this.value);
        long[] counts = getCountArray();
        for (int i = 0; i < COUNT_HEADERS.length; i++) {
            builder.append("\n").append(COUNT_HEADERS[i]).append(" = ").append(counts[i]);
        }
        return builder.toString();
    }
}
