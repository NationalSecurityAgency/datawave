package datawave.query.util;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class Tuple2<A,B> extends Tuple1<A> {
    private B second;

    public Tuple2(A first, B second) {
        super(first);
        this.second = second;
    }

    public B second() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Tuple2) {
            Tuple2<?,?> tup = (Tuple2<?,?>) o;
            return first().equals(tup.first()) && second().equals(tup.second());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hc = new HashCodeBuilder();
        hc.append(first());
        hc.append(second());
        return hc.toHashCode();
    }

    @Override
    public String toString() {
        return "[" + first() + "," + second + "]";
    }
}
