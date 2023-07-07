package datawave.metrics.mapreduce.util;

import org.apache.accumulo.core.data.Value;

public class EmptyValue extends Value {
    private static byte[] emptyBytes;
    private static EmptyValue value;

    static {
        emptyBytes = new byte[0];
        value = new EmptyValue();
    }

    private EmptyValue() {
        super(emptyBytes);
    }

    public static EmptyValue getInstance() {
        return value;
    }

    public static byte[] getEmptyBytes() {
        return emptyBytes;
    }
}
