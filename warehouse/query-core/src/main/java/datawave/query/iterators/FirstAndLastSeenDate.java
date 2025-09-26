package datawave.query.iterators;

import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.tuple.Pair;

public class FirstAndLastSeenDate extends Pair<String,String> {

    private final String first;
    private final String last;

    public FirstAndLastSeenDate(String first, String last) {
        this.first = first;
        this.last = last;
    }

    public FirstAndLastSeenDate(Value value) {
        this(value.toString().split(","));
    }

    private FirstAndLastSeenDate(String[] split) {
        this(split[0], split[1]);
    }

    @Override
    public String getLeft() {
        return first;
    }

    @Override
    public String getRight() {
        return last;
    }

    @Override
    public String toString() {
        return getLeft() + "," + getRight();
    }

    @Override
    public String setValue(String field) {
        throw new UnsupportedOperationException();
    }
}
