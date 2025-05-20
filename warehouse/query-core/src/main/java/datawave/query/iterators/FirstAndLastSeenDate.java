package datawave.query.iterators;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;

public class FirstAndLastSeenDate extends Pair<String,String> {
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
        return getFirst() + "," + getSecond();
    }
}
