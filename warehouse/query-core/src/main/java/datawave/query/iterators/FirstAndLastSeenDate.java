package datawave.query.iterators;

import java.util.AbstractMap;

import org.apache.accumulo.core.data.Value;

public class FirstAndLastSeenDate extends AbstractMap.SimpleEntry<String,String> {
    public FirstAndLastSeenDate(String f, String s) {
        super(f, s);
    }

    public FirstAndLastSeenDate(Value value) {
        this(value.toString().split(","));
    }

    private FirstAndLastSeenDate(String[] split) {
        super(split[0], split[1]);
    }

    @Override
    public String toString() {
        return getKey() + "," + getValue();
    }
}
